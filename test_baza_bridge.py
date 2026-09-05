"""Hermetic signed Baza bridge tests: temporary SQLite, no live network."""

import concurrent.futures
import hashlib
import hmac
import json
import os
import secrets
import unittest
from io import BytesIO
from unittest.mock import patch

from test_app import ClaimWorkflowTestCase, HandlerHarness, TemporaryStateTestCase, app, _network_is_forbidden, _TEST_ENV
from state_store import StateStore
import baza_bridge


SECRET = "test-baza-bridge-secret-" + "x" * 32
NOW = 1_800_000_000
_ENV_PATCH = patch.dict(os.environ, _TEST_ENV)
_NETWORK_PATCH = patch.object(app.urllib.request, "urlopen", _network_is_forbidden)


def setUpModule():
    _ENV_PATCH.start()
    _NETWORK_PATCH.start()


def tearDownModule():
    _NETWORK_PATCH.stop()
    _ENV_PATCH.stop()


def signed_headers(path, body, *, timestamp=NOW, nonce=None):
    timestamp = str(timestamp)
    nonce = nonce or secrets.token_hex(16)
    canonical = "\n".join((timestamp, nonce, "POST", path, hashlib.sha256(body).hexdigest())).encode("utf-8")
    return {
        "Content-Type": "application/json",
        "X-Krugosvet-Key-Id": "baza-picker-v1",
        "X-Krugosvet-Timestamp": timestamp,
        "X-Krugosvet-Nonce": nonce,
        "X-Krugosvet-Signature": hmac.new(SECRET.encode("utf-8"), canonical, hashlib.sha256).hexdigest(),
    }


class TestBazaBridgeAuthentication(TemporaryStateTestCase):
    def setUp(self):
        super().setUp()
        self.patches = [
            patch.object(app, "BAZA_PICKER_BRIDGE_SECRET", SECRET),
            patch.object(app, "readiness_state", return_value={"ok": True}),
            patch.object(app, "rate_limit_allowed", return_value=True),
            patch.object(baza_bridge.time, "time", return_value=NOW),
            patch.object(app.urllib.request, "urlopen", _network_is_forbidden),
        ]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def handler(self, action="next", payload=None, **kwargs):
        path = "/integrations/baza/v1/" + action
        body = json.dumps(payload or {"bitrixUserId": "42"}).encode("utf-8")
        return HandlerHarness.make("POST", path, body, headers=signed_headers(path, body, **kwargs))

    def test_unsigned_and_tampered_requests_never_reach_allocator(self):
        for mode in ("unsigned", "body", "path", "key"):
            with self.subTest(mode=mode):
                handler = self.handler()
                if mode == "unsigned":
                    del handler.headers["X-Krugosvet-Signature"]
                elif mode == "body":
                    handler.rfile = BytesIO(b'{"bitrixUserId": "99"}')
                elif mode == "path":
                    handler.path = "/integrations/baza/v1/claim"
                else:
                    handler.headers["X-Krugosvet-Key-Id"] = "picker-v1"
                with patch.object(app, "baza_picker_action") as action:
                    handler.do_POST()
                self.assertEqual(HandlerHarness.status(handler), 401)
                action.assert_not_called()

    def test_old_and_future_signatures_fail(self):
        for offset in (-301, 301):
            handler = self.handler(timestamp=NOW + offset)
            handler.do_POST()
            self.assertEqual(HandlerHarness.status(handler), 401)
            self.assertEqual(HandlerHarness.json(handler)["error"], "expired_signature")

    def test_replay_is_rejected_after_reopening_same_database(self):
        nonce = "n" * 32
        first = self.handler(nonce=nonce)
        second = self.handler(nonce=nonce)
        with patch.object(app, "baza_picker_action", return_value=({"deal": None}, 200)) as action:
            first.do_POST()
            with patch.object(app, "STATE_STORE", StateStore(self.data_dir)):
                second.do_POST()
        self.assertEqual(HandlerHarness.status(first), 200)
        self.assertEqual(HandlerHarness.status(second), 409)
        self.assertEqual(HandlerHarness.json(second)["error"], "replay_detected")
        action.assert_called_once()

    def test_concurrent_nonce_is_reserved_once(self):
        path = "/integrations/baza/v1/status"
        body = b'{"bitrixUserId":"42"}'
        headers = signed_headers(path, body)
        def verify(_):
            try:
                baza_bridge.authenticate(path, body, headers, SECRET, self.store, now=NOW)
                return "accepted"
            except baza_bridge.BridgeError as error:
                return error.code
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(verify, range(8)))
        self.assertEqual(results.count("accepted"), 1)
        self.assertEqual(results.count("replay_detected"), 7)

    def test_unsigned_dev_mode_cannot_enable_bridge(self):
        handler = self.handler()
        with patch.multiple(app, BAZA_PICKER_BRIDGE_SECRET="", DRY_RUN=True, ALLOW_UNVERIFIED_USERS=True):
            handler.do_POST()
        self.assertEqual(HandlerHarness.status(handler), 503)

    def test_browser_origin_and_unknown_or_query_routes_are_denied(self):
        for kind, expected in (("origin", 403), ("unknown", 404), ("query", 404)):
            with self.subTest(kind=kind):
                handler = self.handler()
                if kind == "origin":
                    handler.headers["Origin"] = "https://picker.example.test"
                else:
                    handler.path += "/other" if kind == "unknown" else "?managerId=99"
                with patch.object(app, "baza_picker_action") as action:
                    handler.do_POST()
                self.assertEqual(HandlerHarness.status(handler), expected)
                action.assert_not_called()

    def test_body_limit_and_duplicate_json_keys_are_rejected(self):
        oversized = self.handler()
        oversized.headers["Content-Length"] = str(baza_bridge.MAX_BODY_BYTES + 1)
        oversized.do_POST()
        self.assertEqual(HandlerHarness.status(oversized), 413)
        path = "/integrations/baza/v1/status"
        for body in (b'[]', b'{"bitrixUserId":"42","bitrixUserId":"99"}'):
            handler = HandlerHarness.make("POST", path, body, headers=signed_headers(path, body))
            handler.do_POST()
            self.assertEqual(HandlerHarness.status(handler), 400)

    def test_next_uses_signed_actor_and_original_continuation_only(self):
        payload = {"bitrixUserId": "42", "managerId": "999", "auth": {"AUTH_ID": "spoof"}, "continuationToken": "cursor"}
        handler = self.handler(payload=payload)
        with (
            patch.object(app, "get_next_deal_for_manager", return_value={"deal": None}) as get_next,
            patch.object(app, "verify_bitrix_user") as oauth,
        ):
            handler.do_POST()
        self.assertEqual(HandlerHarness.status(handler), 200)
        get_next.assert_called_once_with("42", "cursor")
        oauth.assert_not_called()

    def test_claim_uses_canonical_greeting_and_ignores_browser_actor(self):
        handler = self.handler("claim", {"bitrixUserId": "42", "managerId": "999", "auth": {"AUTH_ID": "spoof"}, "dealId": "100", "selectionToken": "selection"})
        with patch.object(app, "preview_claim", return_value={"ok": True}) as claim:
            handler.do_POST()
        self.assertEqual(HandlerHarness.status(handler), 200)
        claim.assert_called_once_with("100", "42", selection_token="selection")

    def test_greeting_status_requires_same_manager_and_succeeded_operation(self):
        self.store.begin_claim_operation("100", "99", operation_key="foreign")
        self.store.finalize_claim_operation("foreign", claim={})
        self.store.begin_claim_operation("101", "42", operation_key="pending")
        self.store.begin_claim_operation("102", "42", operation_key="failed")
        self.store.fail_claim_operation("failed", "proven no write")
        for operation_key in ("unknown", "foreign", "pending", "failed"):
            with self.subTest(operation_key=operation_key):
                handler = self.handler("greeting-status", {
                    "bitrixUserId": "42", "managerId": "99", "operationKey": operation_key,
                })
                with patch.object(self.store, "get_greeting_outbox") as get_job:
                    handler.do_POST()
                self.assertEqual(HandlerHarness.status(handler), 404)
                self.assertEqual(HandlerHarness.json(handler), {"ok": False, "error": "not_found"})
                get_job.assert_not_called()

    def test_greeting_status_rejects_missing_or_unbounded_operation_key(self):
        for value in (None, "", "  ", "x" * 513, 42, {}):
            with self.subTest(value_type=type(value).__name__):
                handler = self.handler("greeting-status", {"bitrixUserId": "42", "operationKey": value})
                with patch.object(self.store, "get_claim_operation") as get_operation:
                    handler.do_POST()
                self.assertEqual(HandlerHarness.status(handler), 400)
                get_operation.assert_not_called()

    def test_greeting_status_reads_pending_and_terminal_manual_without_work(self):
        operation_key = "claim:status-test"
        self.store.begin_claim_operation("100", "42", operation_key=operation_key, request={
            "greetingRequested": True, "greetingContext": {"sessionId": "321", "direction": "Турция"},
        })
        self.store.finalize_claim_operation(operation_key, claim={})
        for expected_status in ("queued", "manual"):
            if expected_status == "manual":
                self.store.lease_greeting_outbox("prepare-test")
                self.store.mark_greeting_outbox_manual(operation_key, "prepare-test", error_code="private-error")
            before = self.store.get_greeting_outbox(operation_key)
            with (
                patch.object(self.store, "_transaction", side_effect=AssertionError("status read must not write")),
                patch.object(app, "bitrix_call") as bitrix,
                patch.object(app, "preview_claim") as claim,
                patch.object(app, "attach_greeting_to_claim") as attach,
                patch.object(app, "GREETING_WAKE_EVENT") as wake,
            ):
                for _ in range(2):
                    response, status = app.baza_picker_action("greeting-status", {
                        "bitrixUserId": "42", "operationKey": operation_key,
                    })
            self.assertEqual(status, 200)
            self.assertEqual(set(response), {"ok", "greeting"})
            self.assertEqual(set(response["greeting"]), {"ok", "status", "autoSent", "text", "message"})
            self.assertEqual(response["greeting"]["status"], expected_status)
            self.assertFalse(response["greeting"]["autoSent"])
            self.assertNotIn("private-error", json.dumps(response))
            if expected_status == "manual":
                self.assertIn("Проверьте чат", response["greeting"]["message"])
            self.assertEqual(before, self.store.get_greeting_outbox(operation_key))
            bitrix.assert_not_called()
            claim.assert_not_called()
            attach.assert_not_called()
            wake.set.assert_not_called()

    def test_greeting_status_without_historical_outbox_returns_manual_without_backfill(self):
        self.store.begin_claim_operation("100", "42", operation_key="historical")
        self.store.finalize_claim_operation("historical", claim={})
        handler = self.handler("greeting-status", {"bitrixUserId": "42", "operationKey": "historical"})
        with patch.object(app, "bitrix_call") as bitrix, patch.object(app, "GREETING_WAKE_EVENT") as wake:
            handler.do_POST()
        self.assertEqual(HandlerHarness.status(handler), 200)
        self.assertEqual(HandlerHarness.json(handler)["greeting"]["status"], "manual")
        self.assertIsNone(self.store.get_greeting_outbox("historical"))
        bitrix.assert_not_called()
        wake.set.assert_not_called()

    def test_browser_manager_id_cannot_replace_missing_signed_actor(self):
        handler = self.handler(payload={"managerId": "42", "auth": {"AUTH_ID": "spoof"}})
        with patch.object(app, "get_next_deal_for_manager") as get_next:
            handler.do_POST()
        self.assertEqual(HandlerHarness.status(handler), 400)
        self.assertEqual(HandlerHarness.json(handler)["error"], "invalid_actor")
        get_next.assert_not_called()

    def test_unexpected_claim_failure_keeps_recovery_mode_without_exposing_exception(self):
        handler = self.handler("claim", {"bitrixUserId": "42", "dealId": "100", "selectionToken": "signed"})
        with patch.object(app, "baza_picker_action", side_effect=RuntimeError("private internal details")):
            handler.do_POST()
        self.assertEqual(HandlerHarness.status(handler), 503)
        result = HandlerHarness.json(handler)
        self.assertTrue(result["recoveryPending"])
        self.assertNotIn("private internal details", str(result))

    def test_refresh_failure_preserves_same_guard_before_search_or_claim(self):
        for action, expected_status in (("next", 200), ("claim", 503)):
            with self.subTest(action=action):
                handler = self.handler(action)
                with (
                    patch.object(app, "EXTRA_CLAIM_REQUESTS_ENABLED", True),
                    patch.object(app, "extra_claim_limit_state", return_value={"enabled": True, "limitReached": True, "integrationUnavailable": True}) as refresh,
                    patch.object(app, "get_next_deal_for_manager") as get_next,
                    patch.object(app, "preview_claim") as claim,
                ):
                    handler.do_POST()
                self.assertEqual(HandlerHarness.status(handler), expected_status)
                refresh.assert_called_once_with("42", refresh=True)
                get_next.assert_not_called()
                claim.assert_not_called()

    def test_status_uses_current_profile_access_and_original_daily_state(self):
        handler = self.handler("status")
        manager = {"id": "42", "active": True, "intranet": True, "name": "Manager"}
        extra = {"takenToday": 5, "dailyLimit": 8, "businessDate": "2026-09-05"}
        with (
            patch.object(app, "get_manager_profile", return_value=manager) as profile,
            patch.object(app, "check_manager_access", return_value={"ok": True}),
            patch.object(app, "extra_claim_limit_state", return_value=extra) as daily,
        ):
            handler.do_POST()
        result = HandlerHarness.json(handler)
        self.assertEqual(result["stats"], extra)
        self.assertEqual(result["manager"], manager)
        profile.assert_called_once_with("42")
        daily.assert_called_once_with("42", refresh=True)

    def test_reject_and_extra_request_delegate_to_original_commands(self):
        for action in ("reject", "extra-request"):
            with self.subTest(action=action):
                payload = {"bitrixUserId": "42", "dealId": "100", "selectionToken": "selection", "reason": "other", "managerId": "999"}
                handler = self.handler(action, payload)
                with (
                    patch.object(app, "record_rejection", return_value={"ok": True}) as reject,
                    patch.object(app, "request_extra_claim", return_value={"ok": True}) as request,
                ):
                    handler.do_POST()
                self.assertEqual(HandlerHarness.status(handler), 200)
                if action == "reject":
                    reject.assert_called_once_with("42", {"dealId": "100", "selectionToken": "selection", "reason": "other"})
                else:
                    request.assert_called_once_with("42", "other")


class TestBazaBridgeClaimSemantics(ClaimWorkflowTestCase):
    def test_expired_claim_and_reject_selection_has_explicit_restart_flag(self):
        with self.common_claim_context(dry_run=True), patch.object(app, "bitrix_call") as bitrix:
            claim = app.preview_claim(self.deal_id, self.manager_id, selection_token="expired", send_greeting=False)
            rejection = app.record_rejection(self.manager_id, {"dealId": self.deal_id, "selectionToken": "expired", "reason": "other"})
        self.assertTrue(claim["selectionStale"])
        self.assertTrue(rejection["selectionStale"])
        self.assertFalse(claim["ok"])
        self.assertFalse(rejection["ok"])
        bitrix.assert_not_called()

    def test_pending_operation_has_recovery_flag_for_claim_and_reject(self):
        self.begin_operation()
        with self.common_claim_context(), patch.object(app, "bitrix_call") as bitrix:
            claim = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token(), send_greeting=False)
            rejection = app.record_rejection(self.manager_id, {"dealId": self.deal_id, "selectionToken": self.token(), "reason": "other"})
        self.assertTrue(claim["recoveryPending"])
        self.assertTrue(rejection["recoveryPending"])
        self.assertNotIn("selectionStale", claim)
        self.assertNotIn("selectionStale", rejection)
        bitrix.assert_not_called()

    def test_changed_deal_has_restart_flag_without_mutating_it(self):
        changed = {**self.source_deal(), "DATE_MODIFY": "new-version"}
        with self.common_claim_context(dry_run=True), patch.object(app, "bitrix_call", return_value=changed) as bitrix:
            claim = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token(), send_greeting=False)
            rejection = app.record_rejection(self.manager_id, {"dealId": self.deal_id, "selectionToken": self.token(), "reason": "other"})
        self.assertTrue(claim["selectionStale"])
        self.assertTrue(rejection["selectionStale"])
        self.assertNotIn("crm.deal.update", [item.args[0] for item in bitrix.call_args_list])
        self.assertEqual(self.store.count_claims(self.manager_id), 0)

    def test_failed_audit_recovery_remains_ambiguous(self):
        self.begin_operation()
        self.store.fail_claim_operation(self.operation_key(), "audit_finalize_failed_after_remote_update", result={"remoteUpdated": True})
        with (
            self.common_claim_context(),
            patch.object(app, "bitrix_call", return_value=self.claimed_deal()) as bitrix,
            patch.object(self.store, "finalize_claim_operation", side_effect=RuntimeError("disk unavailable")),
        ):
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token(), send_greeting=False)
        self.assertFalse(result["ok"])
        self.assertTrue(result["recoveryPending"])
        self.assertEqual(result["_httpStatus"], 503)
        self.assertNotIn("selectionStale", result)
        self.assertNotIn("crm.deal.update", [item.args[0] for item in bitrix.call_args_list])

    def test_recovery_from_old_oauth_attempt_does_not_queue_a_new_greeting_in_baza(self):
        self.store.begin_claim_operation(self.deal_id, self.manager_id, operation_key=self.operation_key(), request={
            "claimMarker": self.attempt_marker(), "dealVersion": self.version,
            "greetingRequested": True, "greetingContext": {"sessionId": "321", "direction": "Турция"},
        })
        self.store.fail_claim_operation(self.operation_key(), "audit_finalize_failed_after_remote_update", result={"remoteUpdated": True})
        with (
            self.common_claim_context(greeting=False),
            patch.multiple(app, GREETING_AUTO_SEND=True, GREETING_AUTO_SEND_SUPPORTED=True),
            patch.object(app, "bitrix_call", return_value=self.claimed_deal()) as bitrix,
        ):
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token(), send_greeting=False)
        self.assertTrue(result["ok"])
        self.assertTrue(result["auditRecorded"])
        self.assertIsNone(self.store.get_greeting_outbox(self.operation_key()))
        self.assertNotIn("crm.deal.update", [item.args[0] for item in bitrix.call_args_list])

    def test_baza_dry_run_preserves_original_no_write_guards(self):
        with (
            self.common_claim_context(dry_run=True),
            patch.object(app, "bitrix_call", return_value=self.source_deal()) as bitrix,
        ):
            result, status = app.baza_picker_action("claim", {"bitrixUserId": self.manager_id, "dealId": self.deal_id, "selectionToken": self.token()})
        self.assertEqual(status, 200)
        self.assertTrue(result["ok"])
        self.assertTrue(result["dryRun"])
        self.assertEqual(self.store.count_claims(self.manager_id), 0)
        self.assertIsNone(self.store.get_claim_operation(self.operation_key()))
        self.assertNotIn("crm.deal.update", [item.args[0] for item in bitrix.call_args_list])

    def test_baza_claim_queues_canonical_greeting_once_and_replay_keeps_original_identity(self):
        side_effect = self.claim_side_effect([self.source_deal(), self.source_deal(), self.claimed_deal()])
        payload = {"bitrixUserId": self.manager_id, "dealId": self.deal_id, "selectionToken": self.token()}
        with (
            self.common_claim_context(greeting=False),
            patch.multiple(app, GREETING_AUTO_SEND=True, GREETING_AUTO_SEND_SUPPORTED=True),
            patch.object(app, "cached_greeting_context", return_value={"openlineSessionIds": ["321"], "classification": {"direction": "Турция"}}),
            patch.object(app, "start_actor_greeting_delivery") as actor_delivery,
            patch.object(app, "bitrix_call", side_effect=side_effect),
        ):
            result, status = app.baza_picker_action("claim", payload)
        actor_delivery.assert_not_called()
        self.assertEqual(status, 200)
        self.assertTrue(result["ok"])
        self.assertTrue(result["auditRecorded"])
        self.assertEqual(result["greeting"]["status"], "queued")
        self.assertEqual(result["operationKey"], self.operation_key())
        self.assertEqual(result["claimedAt"], self.store.list_claims()[0]["timestamp"])
        self.assertEqual(self.store.get_greeting_outbox(self.operation_key())["status"], "pending")

        with self.common_claim_context(), patch.object(app, "bitrix_call", return_value=self.claimed_deal()):
            replay, status = app.baza_picker_action("claim", payload)
        self.assertTrue(replay["idempotentReplay"])
        self.assertEqual(replay["operationKey"], result["operationKey"])
        self.assertEqual(replay["claimedAt"], result["claimedAt"])
        self.assertEqual(self.store.count_claims(self.manager_id), 1)
        self.assertEqual(len(self.store.list_greeting_outbox()), 1)

        def worker_bitrix(method, params=None, timeout=None):
            if method == "crm.deal.get":
                return self.claimed_deal()
            if method == "imopenlines.crm.message.add":
                return "85851"
            raise AssertionError(f"unexpected method: {method}")

        with (
            patch.multiple(app, DRY_RUN=False, GREETING_AUTO_SEND=True, GREETING_AUTO_SEND_SUPPORTED=True),
            patch.object(app, "get_greeting_manager_profile", return_value={"id": self.manager_id, "name": "Айгуль Менеджер", "active": True, "intranet": True}),
            patch.object(app, "resolve_greeting_target", return_value={"chatId": "1763", "sessionId": "321"}),
            patch.object(app, "bitrix_call", side_effect=worker_bitrix) as bitrix,
        ):
            self.assertEqual(app.process_greeting_outbox_once("bridge-worker"), {"leased": 1, "processed": 1})
            self.assertEqual(app.process_greeting_outbox_once("bridge-worker"), {"leased": 0, "processed": 0})
        sends = [call for call in bitrix.call_args_list if call.args[0] == "imopenlines.crm.message.add"]
        self.assertEqual(len(sends), 1)
        self.assertEqual(sends[0].args[1], {
            "CRM_ENTITY_TYPE": "deal", "CRM_ENTITY": self.deal_id,
            "USER_ID": self.manager_id, "CHAT_ID": "1763",
            "MESSAGE": "Здравствуйте! Меня зовут Айгуль. Я эксперт по направлению Турция. Сейчас посмотрю варианты и посчитаю для вас стоимость тура.",
        })

    def test_old_successful_claim_replay_exposes_original_audit_identity_without_new_greeting(self):
        self.begin_operation()
        timestamp = "2026-09-05T23:58:00+06:00"
        self.store.finalize_claim_operation(
            self.operation_key(), claim={"timestamp": timestamp},
            result={"ok": True, "dryRun": False, "dealId": self.deal_id, "auditRecorded": True},
        )
        # Older deployed policy allowed several lifecycles. Their historical
        # events must not invalidate an exact, already-succeeded operation.
        self.store.append_claim({"dealId": self.deal_id, "managerId": "historical-manager"})
        with self.common_claim_context(), patch.object(app, "bitrix_call", return_value=self.claimed_deal()) as bitrix:
            result, status = app.baza_picker_action("claim", {"bitrixUserId": self.manager_id, "dealId": self.deal_id, "selectionToken": self.token()})
        self.assertEqual(status, 200)
        self.assertTrue(result["idempotentReplay"])
        self.assertEqual(result["operationKey"], self.operation_key())
        self.assertEqual(result["claimedAt"], self.store.list_claims()[0]["timestamp"])
        self.assertEqual([call.args[0] for call in bitrix.call_args_list], ["crm.deal.get"])
        self.assertEqual(self.store.list_greeting_outbox(), [])

    def test_audit_recovery_keeps_the_confirmed_response_identity(self):
        side_effect = self.claim_side_effect([self.source_deal(), self.source_deal(), self.claimed_deal()])
        payload = {"bitrixUserId": self.manager_id, "dealId": self.deal_id, "selectionToken": self.token()}
        with (
            self.common_claim_context(),
            patch.multiple(app, GREETING_AUTO_SEND=True, GREETING_AUTO_SEND_SUPPORTED=True),
            patch.object(app, "cached_greeting_context", return_value={"openlineSessionIds": ["321"], "classification": {"direction": "Турция"}}),
            patch.object(app, "bitrix_call", side_effect=side_effect),
            patch.object(self.store, "finalize_claim_operation", side_effect=RuntimeError("disk failure")),
        ):
            result, _ = app.baza_picker_action("claim", payload)
        self.assertTrue(result["ok"])
        self.assertFalse(result["auditRecorded"])
        # Simulate older imported allocation evidence on a deal that had already
        # been reclaimed under the previous policy before this release.
        self.store.append_claim({"dealId": self.deal_id, "managerId": "historical-manager"})
        with (
            self.common_claim_context(),
            patch.multiple(app, GREETING_AUTO_SEND=True, GREETING_AUTO_SEND_SUPPORTED=True),
            patch.object(app, "bitrix_call", return_value=self.claimed_deal()) as bitrix,
        ):
            recovered, _ = app.baza_picker_action("claim", payload)
        self.assertTrue(recovered["ok"])
        self.assertTrue(recovered["auditRecorded"])
        self.assertEqual(recovered["operationKey"], result["operationKey"])
        self.assertEqual(recovered["claimedAt"], result["claimedAt"])
        self.assertEqual(self.store.get_claim_by_operation_key(self.operation_key())["timestamp"], result["claimedAt"])
        self.assertNotIn("crm.deal.update", [call.args[0] for call in bitrix.call_args_list])
        self.assertEqual(recovered["greeting"]["status"], "queued")
        self.assertEqual(self.store.get_greeting_outbox(self.operation_key())["sessionId"], "321")
        self.assertEqual(len(self.store.list_greeting_outbox()), 1)


if __name__ == "__main__":
    unittest.main()
