"""Private provenance reads use temporary SQLite and never call Bitrix."""

import json
import os
import sqlite3
from contextlib import contextmanager
from io import BytesIO
from unittest.mock import patch

import baza_bridge
from state_store import MESSAGE_ORIGIN_COVERAGE, StateStore
from test_app import TemporaryStateTestCase, HandlerHarness, app, _network_is_forbidden, _TEST_ENV
from test_baza_bridge import NOW, SECRET, signed_headers


HORIZON = "2026-10-17T00:00:00+00:00"
BEFORE = "2026-10-16T23:00:00+00:00"
AFTER = "2026-10-17T01:00:00+00:00"


class TestMessageOrigins(TemporaryStateTestCase):
    def setUp(self):
        super().setUp()
        for item in (
            patch.dict(os.environ, _TEST_ENV),
            patch.object(app, "BAZA_PICKER_BRIDGE_SECRET", SECRET),
            patch.object(app, "readiness_state", return_value={"ok": True}),
            patch.object(app, "rate_limit_allowed", return_value=True),
            patch.object(baza_bridge.time, "time", return_value=NOW),
            patch.object(app.urllib.request, "urlopen", _network_is_forbidden),
        ):
            item.start()
            self.addCleanup(item.stop)
        with self.store._transaction(immediate=True) as connection:
            connection.execute("UPDATE meta SET value=? WHERE key=?", (HORIZON, MESSAGE_ORIGIN_COVERAGE))

    def query(self, **kwargs):
        return self.store.message_origins(**{
            "deal_id": "100", "session_id": "321", "chat_id": "1763", "message_ids": ["800", "801"], **kwargs,
        })

    def handler(self, payload=None, **signature):
        path = "/integrations/baza/v1/message-origins"
        body = json.dumps(payload if payload is not None else {
            "bitrixDealId": "100", "sessionId": "321", "chatId": "1763", "messageIds": ["800", "801"],
        }).encode()
        return HandlerHarness.make("POST", path, body, headers=signed_headers(path, body, **signature))

    def job(self, *, deal="100", session="321", chat="1763", status="sent", message="800", finalized=AFTER, key="operation"):
        self.store.begin_claim_operation(deal, "42", operation_key=key, request={
            "greetingRequested": True, "greetingContext": {"sessionId": session or "321", "direction": "Test"},
        })
        self.store.finalize_claim_operation(key, claim={})
        with self.store._transaction(immediate=True) as connection:
            connection.execute(
                "UPDATE greeting_outbox SET status=?, message_id=?, session_id=?, chat_id=?, finalized_at=? WHERE operation_key=?",
                (status, message, session, chat, finalized, key),
            )

    def event(self, **fields):
        self.store.append_greeting({
            "dealId": "100", "managerId": "42", "timestamp": AFTER,
            "status": "sent", "autoSent": True, "autoAttempted": True,
            "operationKey": "legacy", **fields,
        })

    def database(self):
        with sqlite3.connect(self.store.db_path) as connection:
            return [line for line in connection.iterdump() if not line.startswith('INSERT INTO "baza_bridge_nonces"')]

    def test_empty_database_supports_only_new_messages_and_boundary_survives_restart(self):
        result = self.query()
        self.assertEqual(result, {
            "ok": True, "bitrixDealId": "100", "sessionId": "321", "chatId": "1763",
            "checkedIds": ["800", "801"], "automaticMessageIds": [],
            "complete": True, "coveredSince": HORIZON,
        })
        reopened = StateStore(self.data_dir)
        self.assertEqual(reopened.message_origins(deal_id="100", session_id="321", chat_id="1763", message_ids=["800"])["coveredSince"], HORIZON)

    def test_schema_bootstrap_does_not_arm_greetings_or_backfill_delivery(self):
        self.assertEqual(self.store.list_greeting_outbox(), [])
        self.assertEqual(self.store.list_greetings(), [])
        with self.store._connect() as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM meta WHERE key=?", (MESSAGE_ORIGIN_COVERAGE,)).fetchone()[0], 1)

    def test_old_schema_preserves_legacy_ids_and_marks_unknown_targets(self):
        self.job(deal="101", session="998", message="801", status="uncertain")
        self.event(dealId="102", sendResult={"messageId": 800})
        with self.store._transaction(immediate=True) as connection:
            connection.execute("DROP INDEX idx_greeting_events_provider_message")
            connection.execute("ALTER TABLE greeting_events DROP COLUMN provider_message_id")
            connection.execute("DROP INDEX idx_greeting_outbox_chat_status")
            connection.execute("DROP INDEX idx_greeting_outbox_origin_recovery")
            connection.execute("ALTER TABLE greeting_outbox DROP COLUMN chat_id")
        reopened = StateStore(self.data_dir)
        result = reopened.message_origins(deal_id="100", session_id="321", chat_id="1763", message_ids=["800", "801"])
        self.assertEqual(result["automaticMessageIds"], ["800", "801"])
        self.assertFalse(result["complete"])
        self.assertEqual(reopened.get_greeting_outbox("operation")["chatId"], "")

    def test_exact_outbox_id_is_automatic_without_exposing_private_fields(self):
        self.job()
        result = self.query()
        self.assertEqual(result["automaticMessageIds"], ["800"])
        self.assertTrue(result["complete"])
        self.assertNotIn("42", json.dumps(result))
        self.assertNotIn("operation", json.dumps(result))

    def test_exact_requested_id_is_automatic_even_after_deal_session_and_chat_rebinding(self):
        self.job(deal="101", session="998", chat="777", key="other-deal")
        result = self.query()
        self.assertEqual(result["automaticMessageIds"], ["800"])
        self.assertTrue(result["complete"])

    def test_known_unrelated_chat_dispatch_does_not_block(self):
        self.job(deal="101", session="998", chat="777", status="dispatching", message="")
        self.assertTrue(self.query()["complete"])

    def test_unknown_target_dispatch_blocks_globally(self):
        self.job(deal="101", session="998", chat="", status="dispatching", message="")
        self.assertFalse(self.query()["complete"])

    def test_same_chat_dispatch_blocks_after_both_deal_and_session_change(self):
        self.job(deal="101", session="998", status="dispatching", message="")
        self.assertFalse(self.query()["complete"])

    def test_legacy_exact_ids_are_matched_globally(self):
        self.event(dealId="101", sendResult={"messageId": "800"})
        self.assertEqual(self.query()["automaticMessageIds"], ["800"])

    def test_unresolved_legacy_attempt_without_chat_blocks_globally(self):
        self.event(dealId="101", status="sending", autoSent=False, timestamp=BEFORE)
        self.assertFalse(self.query()["complete"])

    def test_same_deal_exact_provider_id_survives_session_rollover(self):
        self.job(session="999", message="800")
        result = self.query()
        self.assertEqual(result["automaticMessageIds"], ["800"])
        self.assertTrue(result["complete"])

    def test_same_deal_other_saved_session_dispatch_blocks_negatives(self):
        self.job(session="999", status="dispatching", message="")
        self.assertFalse(self.query()["complete"])

    def test_same_session_automatic_id_survives_rebinding_to_another_deal(self):
        self.job(deal="101", key="previous-deal")
        result = self.query()
        self.assertEqual(result["automaticMessageIds"], ["800"])
        self.assertTrue(result["complete"])
        self.assertEqual(result["bitrixDealId"], "100")

    def test_same_session_dispatch_for_another_deal_blocks_negatives(self):
        self.job(deal="101", status="dispatching", message="", key="previous-deal")
        self.assertFalse(self.query()["complete"])

    def test_all_inflight_states_block_negatives_even_when_created_before_boundary(self):
        self.job(status="pending", message="", finalized=None)
        for state in ("pending", "checking", "dispatching", "uncertain"):
            with self.subTest(state=state):
                with self.store._transaction(immediate=True) as connection:
                    connection.execute("UPDATE greeting_outbox SET status=?, created_at=?", (state, BEFORE))
                self.assertFalse(self.query()["complete"])

    def test_unknown_session_for_relevant_deal_blocks_negatives(self):
        self.job(session="", status="dispatching", message="")
        self.assertFalse(self.query()["complete"])

    def test_known_id_remains_automatic_while_other_evidence_is_uncertain(self):
        self.job(status="uncertain")
        result = self.query()
        self.assertEqual(result["automaticMessageIds"], ["800"])
        self.assertFalse(result["complete"])

    def test_concurrent_dispatch_completion_cannot_mix_read_snapshots(self):
        self.job(status="dispatching", message="", finalized=None)
        transaction = self.store._transaction

        @contextmanager
        def finish_during_read(*, immediate=False):
            with transaction(immediate=immediate) as connection:
                class Snapshot:
                    def execute(inner, sql, parameters=()):
                        cursor = connection.execute(sql, parameters)
                        if sql.startswith("SELECT value FROM meta"):
                            with transaction(immediate=True) as writer:
                                writer.execute(
                                    "UPDATE greeting_outbox SET status='sent', message_id='800', finalized_at=?",
                                    (AFTER,),
                                )
                        return cursor
                yield Snapshot()

        with patch.object(self.store, "_transaction", finish_during_read):
            during_dispatch = self.query()
        self.assertFalse(during_dispatch["complete"])
        self.assertEqual(during_dispatch["automaticMessageIds"], [])
        confirmed = self.query()
        self.assertTrue(confirmed["complete"])
        self.assertEqual(confirmed["automaticMessageIds"], ["800"])

    def test_sent_without_exact_provider_id_is_incomplete(self):
        self.job(message="")
        self.assertFalse(self.query()["complete"])

    def test_confirmed_manual_non_send_does_not_block(self):
        self.job(status="manual", message="")
        self.assertTrue(self.query()["complete"])

    def test_old_timeout_still_blocks_because_provider_may_finish_later(self):
        self.job(status="uncertain", message="", finalized=BEFORE)
        result = self.query()
        self.assertFalse(result["complete"])
        self.assertEqual(result["coveredSince"], HORIZON)

    def test_confirmed_old_send_without_retained_id_does_not_block_new_messages(self):
        self.job(status="sent", message="", finalized=BEFORE)
        self.assertTrue(self.query()["complete"])

    def test_uncertain_missing_or_malformed_terminal_time_fails_closed(self):
        self.job(status="uncertain", message="")
        for timestamp in (None, "", "bad", "2026-10-16T23:00:00"):
            with self.store._transaction(immediate=True) as connection:
                connection.execute("UPDATE greeting_outbox SET finalized_at=?", (timestamp,))
            self.assertFalse(self.query()["complete"])

    def test_legacy_exact_id_and_reservation_finalization(self):
        self.event(status="sending", autoSent=False)
        self.event(sendResult={"messageId": 800, "ok": True})
        result = self.query()
        self.assertEqual(result["automaticMessageIds"], ["800"])
        self.assertTrue(result["complete"])

    def test_legacy_unkeyed_attempts_cannot_be_paired(self):
        self.event(operationKey="", status="sending", autoSent=False, timestamp=BEFORE)
        self.event(operationKey="", sendResult={"messageId": "800"})
        result = self.query()
        self.assertEqual(result["automaticMessageIds"], ["800"])
        self.assertFalse(result["complete"])

    def test_current_audit_without_id_uses_canonical_job(self):
        self.job()
        self.event(operationKey="operation")
        self.assertTrue(self.query()["complete"])

    def test_old_legacy_missing_id_is_not_retrospective_negative_proof(self):
        self.event(timestamp=BEFORE)
        result = self.query()
        self.assertTrue(result["complete"])
        self.assertEqual(result["coveredSince"], HORIZON)
        self.event(operationKey="new-unknown")
        self.assertFalse(self.query()["complete"])

    def legacy_error(self, errors, **fields):
        self.event(status="manual", autoSent=False, autoAttempted=None,
                   sendResult={"ok": False, "method": "imopenlines.operator.answer + im.message.add", "errors": errors}, **fields)

    @staticmethod
    def history(session="998", chat="777"):
        return {"sessionId": int(session), "chatId": int(chat), "userId": f"chat{chat}",
                "chat": {chat: {"id": chat, "entityType": "LINES"}},
                "message": {"999": {"text": "Private history must never be saved"}}}

    def test_legacy_provider_failure_scopes_every_attempt_without_claiming_non_delivery(self):
        self.legacy_error([
            {"sessionId": "998", "chatId": "777", "error": "ACCESS_DENIED: webhook fallback rejected"},
            {"sessionId": "999", "chatId": "778", "error": "timed out"},
        ], openlineSessionIds=["123"])
        # Old retry copied openlineSessionIds from the earlier attempt. Actual
        # errors are exhaustive; that stale field is not target evidence.
        self.assertTrue(self.query()["complete"])
        for chat in ("777", "778"):
            self.assertFalse(self.query(chat_id=chat)["complete"])

    def test_exact_legacy_pre_send_branches_do_not_block(self):
        self.legacy_error([{"sessionId": "998", "error": "Не удалось получить chatId открытой линии."}], operationKey="lookup")
        self.legacy_error([{"error": "У сделки не найдена открытая линия для автоотправки."}], operationKey="empty")
        self.assertTrue(self.query()["complete"])

    def test_mixed_known_and_unknown_legacy_targets_keep_global_block(self):
        self.legacy_error([
            {"sessionId": "998", "chatId": "777", "error": "timed out"},
            {"sessionId": "999", "error": "unrecognized failure"},
        ])
        self.assertFalse(self.query()["complete"])

    def test_malformed_legacy_scope_is_not_negative_proof(self):
        for errors in ([], None, [{"sessionId": "998", "chatId": "0", "error": "failed"}],
                       [{"chatId": "777", "error": "failed"}],
                       [{"sessionId": "998", "error": "Не удалось получить chatId открытой линии. ",}],
                       [{"error": "У сделки не найдена открытая линия для автоотправки.", "chatId": "777"}]):
            with self.subTest(errors=errors):
                self.legacy_error(errors)
                self.assertFalse(self.query()["complete"])

    def test_failed_legacy_method_cannot_impersonate_a_known_no_send_branch(self):
        self.event(status="manual", autoSent=False, autoAttempted=None, sendResult={
            "ok": False, "method": "other sender", "errors": [{"error": "У сделки не найдена открытая линия для автоотправки."}],
        })
        self.assertFalse(self.query()["complete"])

    def test_historical_session_proof_scopes_uncertain_job_without_changing_send_state(self):
        self.job(deal="101", session="998", chat="", status="uncertain", message="")
        before = self.store.get_greeting_outbox("operation")
        self.assertFalse(self.query()["complete"])
        with patch.object(app, "bitrix_call", return_value=self.history()) as provider:
            self.assertEqual(app.reconcile_message_origin_scope_once(), {"checked": True, "resolved": True})
        provider.assert_called_once_with("imopenlines.session.history.get", {"SESSION_ID": "998"}, timeout=min(5, app.BITRIX_FAST_TIMEOUT_SECONDS))
        self.assertEqual(self.store.get_greeting_outbox("operation"), before)
        self.assertTrue(self.query()["complete"])
        self.assertFalse(self.query(chat_id="777")["complete"])
        # Exact target wins over a stale deal or saved session relationship.
        self.assertTrue(self.query(deal_id="101", session_id="998")["complete"])
        reopened = StateStore(self.data_dir)
        self.assertTrue(reopened.message_origins(deal_id="100", session_id="321", chat_id="1763", message_ids=["800"])["complete"])
        with patch.object(app, "bitrix_call") as provider:
            self.assertEqual(app.reconcile_message_origin_scope_once(), {"checked": False, "resolved": False})
        provider.assert_not_called()
        with self.store._connect() as connection:
            cached = dict(connection.execute("SELECT * FROM message_origin_session_chats").fetchone())
        self.assertNotIn("Private history", json.dumps(cached))

    def test_scope_recovery_does_not_run_under_a_database_transaction(self):
        self.job(session="998", chat="", status="uncertain", message="")
        def read(method, params, **kwargs):
            with self.store._transaction(immediate=True) as connection:
                connection.execute("INSERT INTO meta(key,value,updated_at) VALUES('independent-read','ok','now')")
            return self.history()
        with patch.object(app, "bitrix_call", side_effect=read):
            self.assertTrue(app.reconcile_message_origin_scope_once()["resolved"])

    def test_missing_malformed_and_mismatched_native_proof_remains_global(self):
        self.job(session="998", chat="", status="uncertain", message="")
        bad = [None, {}, self.history(session="999"), {**self.history(), "sessionId": None},
               {**self.history(), "chatId": "0777"}, {**self.history(), "chat": {"777": {"id": "778", "entityType": "LINES"}}},
               {**self.history(), "chat": {"777": {"id": "777", "entityType": "OTHER"}}}]
        for proof in bad:
            with self.subTest(proof=proof):
                self.assertFalse(self.store.record_message_origin_session_history("998", proof))
                self.assertFalse(self.query()["complete"])

    def test_flat_native_chat_shape_requires_the_same_explicit_identity(self):
        self.job(session="998", chat="", status="uncertain", message="")
        proof = {"sessionId": "998", "chatId": "777", "chat": {"id": "777", "entityType": "LINES"}}
        self.assertTrue(self.store.record_message_origin_session_history("998", proof))
        self.assertTrue(self.query()["complete"])
        self.assertFalse(self.query(chat_id="777")["complete"])

    def test_contradictory_session_scope_is_quarantined_and_cannot_be_overwritten(self):
        self.job(session="998", chat="", status="uncertain", message="")
        self.assertTrue(self.store.record_message_origin_session_history("998", self.history()))
        self.assertTrue(self.query()["complete"])
        self.assertFalse(self.store.record_message_origin_session_history("998", self.history(chat="778")))
        self.assertFalse(self.query()["complete"])
        self.assertFalse(self.store.record_message_origin_session_history("998", self.history()))
        self.assertFalse(self.query()["complete"])
        self.assertIsNone(self.store.reserve_message_origin_session(now=10_000))

    def test_new_actual_chat_conflict_invalidates_older_session_proof(self):
        self.job(session="998", chat="", status="uncertain", message="")
        self.assertTrue(self.store.record_message_origin_session_history("998", self.history()))
        self.job(deal="101", session="998", chat="778", status="sent", key="new-job")
        self.assertFalse(self.query()["complete"])
        self.assertEqual(self.query()["automaticMessageIds"], ["800"])

    def test_worker_retries_failed_history_with_persistent_backoff_and_fairness(self):
        self.job(session="998", chat="", status="uncertain", message="")
        self.job(deal="101", session="999", chat="", status="uncertain", message="", key="second")
        with patch.object(app.time, "time", return_value=100), patch.object(app, "bitrix_call", side_effect=TimeoutError) as provider:
            self.assertEqual(app.reconcile_message_origin_scope_once(), {"checked": True, "resolved": False})
            provider.assert_called_once()
        # The failed session cannot starve another old attempt or retry early.
        self.assertEqual(self.store.reserve_message_origin_session(now=110), "999")
        reopened = StateStore(self.data_dir)
        self.assertIsNone(reopened.reserve_message_origin_session(now=159))
        self.assertEqual(reopened.reserve_message_origin_session(now=160), "998")
        self.assertIsNone(reopened.reserve_message_origin_session(now=169))
        self.assertEqual(reopened.reserve_message_origin_session(now=170), "999")
        self.assertIsNone(reopened.reserve_message_origin_session(now=279))
        self.assertEqual(reopened.reserve_message_origin_session(now=280), "998")

    def test_worker_never_recovers_inflight_or_invalid_saved_sessions(self):
        for index, status in enumerate(("pending", "checking", "dispatching", "sent", "manual")):
            self.job(deal=str(101 + index), chat="", status=status, message="", key=f"job-{index}")
        self.job(deal="200", session="", chat="", status="uncertain", message="", key="invalid")
        with patch.object(app, "bitrix_call") as provider:
            self.assertEqual(app.reconcile_message_origin_scope_once(), {"checked": False, "resolved": False})
        provider.assert_not_called()

    def test_background_loop_has_one_bounded_read_per_ten_second_turn(self):
        with patch.object(app, "reconcile_message_origin_scope_once") as recover, patch.object(app.time, "sleep", side_effect=KeyboardInterrupt) as sleep:
            with self.assertRaises(KeyboardInterrupt):
                app.message_origin_scope_loop()
        recover.assert_called_once_with()
        sleep.assert_called_once_with(10)

    def test_49_historical_blockers_only_block_their_proven_target_chats(self):
        for index in range(13):
            self.job(deal=str(1000 + index), session=str(2000 + index), chat="", status="uncertain", message="", key=f"job-{index}")
        for index in range(30):
            self.legacy_error([{"sessionId": str(3000 + index), "chatId": str(4000 + index), "error": "provider error"}], operationKey=f"legacy-{index}")
        for index in range(5):
            self.legacy_error([{"sessionId": str(5000 + index), "error": "Не удалось получить chatId открытой линии."}], operationKey=f"preflight-{index}")
        self.legacy_error([{"error": "У сделки не найдена открытая линия для автоотправки."}], operationKey="no-session")
        original_jobs, original_events = self.store.list_greeting_outbox(), self.store.list_greetings()
        self.assertFalse(self.query()["complete"])
        with patch.object(app, "bitrix_call", side_effect=lambda method, params, **kwargs: self.history(params["SESSION_ID"], str(6000 + (int(params["SESSION_ID"]) - 2000) % 12))) as provider:
            for _ in range(13):
                self.assertTrue(app.reconcile_message_origin_scope_once()["resolved"])
            self.assertEqual(provider.call_count, 13)
        self.assertTrue(self.query()["complete"])
        for chat in [str(4000 + index) for index in range(30)] + [str(6000 + index) for index in range(12)]:
            self.assertFalse(self.query(chat_id=chat)["complete"])
        self.assertEqual(self.store.list_greeting_outbox(), original_jobs)
        self.assertEqual(self.store.list_greetings(), original_events)

    def test_corrupt_boundary_fails_closed(self):
        with self.store._transaction(immediate=True) as connection:
            connection.execute("UPDATE meta SET value='invalid' WHERE key=?", (MESSAGE_ORIGIN_COVERAGE,))
        handler = self.handler()
        handler.do_POST()
        self.assertEqual(HandlerHarness.status(handler), 503)

    def test_invalid_payloads_are_rejected_without_provider_calls(self):
        valid = {"bitrixDealId": "100", "sessionId": "321", "chatId": "1763", "messageIds": ["800"]}
        invalid = [
            {**valid, "sessionId": 321}, {**valid, "sessionId": "0"}, {**valid, "sessionId": "321' OR 1=1"},
            {**valid, "chatId": 1763}, {**valid, "chatId": ""},
            {**valid, "messageIds": []}, {**valid, "messageIds": ["800", "800"]},
            {**valid, "messageIds": [800]}, {**valid, "messageIds": [True]},
            {**valid, "messageIds": ["01"]}, {**valid, "messageIds": "800"},
            {**valid, "messageIds": [str(i + 1) for i in range(201)]},
            {**valid, "bitrixDealId": "1" * 21}, {**valid, "auth": "spoof"},
            {"sessionId": "321", "messageIds": ["800"]},
        ]
        for payload in invalid:
            with self.subTest(payload=payload):
                handler = self.handler(payload)
                handler.do_POST()
                self.assertEqual(HandlerHarness.status(handler), 400)

    def test_two_hundred_exact_ids_are_supported(self):
        ids = [str(10**18 + i) for i in range(200)]
        self.assertEqual(self.query(message_ids=ids)["checkedIds"], ids)

    def test_authenticated_route_only_mutates_replay_nonce(self):
        self.job()
        before = self.database()
        handler = self.handler()
        with (
            patch.object(app, "bitrix_call", side_effect=AssertionError("no Bitrix")),
            patch.object(app, "preview_claim", side_effect=AssertionError("no claim")),
            patch.object(app, "process_greeting_outbox_once", side_effect=AssertionError("no greeting")),
        ):
            handler.do_POST()
        self.assertEqual(HandlerHarness.status(handler), 200)
        self.assertEqual(before, self.database())
        with self.store._connect() as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM baza_bridge_nonces").fetchone()[0], 1)

    def test_authentication_binds_body_path_and_rejects_replay(self):
        for mode in ("unsigned", "body", "path", "origin"):
            with self.subTest(mode=mode):
                handler = self.handler()
                if mode == "unsigned":
                    del handler.headers["X-Krugosvet-Signature"]
                elif mode == "body":
                    handler.rfile = BytesIO(b'{"bitrixDealId":"101","sessionId":"321","messageIds":["800","801"]}')
                elif mode == "path":
                    handler.path = "/integrations/baza/v1/claim-history"
                else:
                    handler.headers["Origin"] = "https://picker.example.test"
                with patch.object(app, "baza_picker_action") as action:
                    handler.do_POST()
                self.assertNotEqual(HandlerHarness.status(handler), 200)
                action.assert_not_called()
        first = self.handler(nonce="a" * 32)
        second = self.handler(nonce="a" * 32)
        first.do_POST()
        second.do_POST()
        self.assertEqual(HandlerHarness.status(first), 200)
        self.assertEqual(HandlerHarness.status(second), 409)

    def test_rate_limit_has_separate_namespace_from_claims_and_history(self):
        handler = self.handler()
        with patch.object(app, "rate_limit_allowed", return_value=False) as rate:
            handler.do_POST()
        self.assertEqual(HandlerHarness.status(handler), 429)
        rate.assert_called_once_with(f"baza-origins:{handler.client_key()}")
