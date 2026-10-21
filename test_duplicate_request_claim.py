"""Request-level checks around the real claim workflow; no provider writes."""
from datetime import timedelta
from unittest.mock import patch

import test_app as fixtures
from request_claim_guard import RequestClaimConflictError, RequestClaimEvidenceError

app = fixtures.app


class TestDuplicateRequestClaim(fixtures.ClaimWorkflowTestCase):
    def setUp(self):
        super().setUp()
        self._request_guard_patch.stop()
        signing = patch.object(app, "selection_signing_key", return_value=b"request-claim-test-key")
        signing.start()
        self.addCleanup(signing.stop)

    def evidence(self, *, renewed=False):
        now = app.local_now()
        return {
            "identityKeys": ["contact:100"],
            "inboundEventKey": "a" * 64 if renewed else "",
            "inboundAt": (now - timedelta(seconds=30)).isoformat() if renewed else "",
            "checkedPrior": [{"dealId": "99", "closedAt": (now - timedelta(minutes=1)).isoformat()}] if renewed else [],
            "observedAt": now.isoformat(),
        }

    def previous_claim(self):
        self.store.begin_claim_operation("99", "33", operation_key="previous-request", request={
            "requestClaimContext": self.evidence(), "claimMarker": "previous-marker",
        })
        self.store.finalize_claim_operation("previous-request", claim={"dealId": "99", "managerId": "33"},
                                            result={"ok": True})
        # The original claim happened before the terminal transition.
        with self.store._transaction(immediate=True) as db:
            db.execute("UPDATE request_claim_allocations SET created_at=? WHERE operation_key=?",
                       ((app.local_now() - timedelta(minutes=2)).isoformat(), "previous-request"))

    def assert_blocked_without_effects(self, result, remote, status):
        self.assertFalse(result["ok"])
        self.assertEqual(result["_httpStatus"], status)
        self.assertFalse(any("update" in c.args[0] or "message.add" in c.args[0] for c in remote.call_args_list))
        self.assertIsNone(self.store.get_claim_operation(self.operation_key()))
        self.assertIsNone(self.store.get_greeting_outbox(self.operation_key()))

    def test_new_card_without_new_message_is_rejected_before_assignment_or_greeting(self):
        with self.common_claim_context(), patch.object(app, "request_claim_context", side_effect=RequestClaimConflictError(deal_id="99")), \
                patch.object(app, "bitrix_call", return_value=self.source_deal()) as remote:
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token())
        self.assert_blocked_without_effects(result, remote, 409)

    def test_unavailable_message_origin_does_not_spend_quota_or_send(self):
        with self.common_claim_context(), patch.object(app, "request_claim_context", side_effect=RequestClaimEvidenceError()), \
                patch.object(app, "bitrix_call", return_value=self.source_deal()) as remote:
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token())
        self.assert_blocked_without_effects(result, remote, 503)

    def test_concurrent_claim_after_provider_read_is_still_blocked_atomically(self):
        self.previous_claim()
        with self.common_claim_context(), patch.object(app, "request_claim_context", return_value=self.evidence()), \
                patch.object(app, "bitrix_call", return_value=self.source_deal()) as remote:
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token())
        self.assert_blocked_without_effects(result, remote, 409)

    def test_completed_request_plus_new_inbound_preserves_owner_and_in_work_update(self):
        self.previous_claim()
        with self.common_claim_context(), patch.object(app, "request_claim_context", return_value=self.evidence(renewed=True)), \
                patch.object(app, "bitrix_call", side_effect=self.claim_side_effect([
                    self.source_deal(), self.source_deal(), self.claimed_deal(),
                ])) as remote:
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token())
        self.assertTrue(result["ok"], result)
        updates = [c.args[1] for c in remote.call_args_list if c.args[0] == "crm.deal.update"]
        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0]["fields[STAGE_ID]"], "NEW")
        self.assertEqual(updates[0]["fields[ASSIGNED_BY_ID]"], self.manager_id)

    def test_confirmed_replay_does_not_depend_on_new_history_evidence(self):
        with self.common_claim_context(), patch.object(app, "request_claim_context", return_value=self.evidence()), \
                patch.object(app, "bitrix_call", side_effect=self.claim_side_effect([
                    self.source_deal(), self.source_deal(), self.claimed_deal(),
                ])):
            first = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token())
        self.assertTrue(first["ok"], first)
        with self.common_claim_context(), patch.object(app, "request_claim_context", side_effect=AssertionError("replay queried evidence")), \
                patch.object(app, "bitrix_call", return_value=self.claimed_deal()):
            replay = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token())
        self.assertTrue(replay["ok"], replay)
        self.assertTrue(replay["idempotentReplay"])

    def test_contact_identity_change_without_deal_version_change_releases_without_write(self):
        first = self.evidence()
        changed = {**first, "identityKeys": ["contact:100", "phone:" + "b" * 64]}
        with self.common_claim_context(), patch.object(app, "request_claim_context", side_effect=[first, changed]), \
                patch.object(app, "bitrix_call", return_value=self.source_deal()) as remote:
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token())
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "request_identity_changed")
        self.assertFalse(any(c.args[0] == "crm.deal.update" for c in remote.call_args_list))
        self.assertEqual(self.store.get_claim_operation(self.operation_key())["result"], {"remoteUpdated": False})
        self.assertEqual(self.store.list_request_claim_conflicts(first["identityKeys"]), [])

    def test_stage_change_during_final_evidence_read_is_checked_before_write(self):
        current_deal = self.source_deal()
        reads = []
        def evidence(*args, **kwargs):
            reads.append(1)
            if len(reads) == 2:
                current_deal["STAGE_ID"] = "WON"
            return self.evidence()
        with self.common_claim_context(), patch.object(app, "request_claim_context", side_effect=evidence), \
                patch.object(app, "bitrix_call", side_effect=lambda *args, **kwargs: dict(current_deal)) as remote:
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token())
        self.assertFalse(result["ok"])
        self.assertTrue(result["selectionStale"])
        self.assertFalse(any(c.args[0] == "crm.deal.update" for c in remote.call_args_list))
        self.assertEqual(self.store.get_claim_operation(self.operation_key())["result"], {"remoteUpdated": False})

    def test_search_skips_duplicate_card_and_offers_next_customer(self):
        headers = [{"ID": str(i), "STAGE_ID": next(iter(app.SOURCE_STAGES)), "DATE_MODIFY": "v1"} for i in (100, 101)]
        analyzed = {h["ID"]: {"id": h["ID"], "version": "v1", "messages": [],
                              "classification": {"direction": "Не определено"}} for h in headers}
        manager = {"id": "42", "active": True, "intranet": True, "competencies": []}
        with patch.object(app, "get_manager_profile", return_value=manager), \
                patch.object(app, "check_manager_access", return_value={"ok": True, "rule": {}}), \
                patch.object(app, "list_allowed_deal_headers", return_value=headers), \
                patch.object(app, "iter_analyzed_deal_headers", side_effect=lambda rows: fixtures.analysis_fixture_rows(rows, analyzed, {})), \
                patch.object(app, "request_claim_context", side_effect=[RequestClaimConflictError(), self.evidence()]):
            result = app._get_next_deal_for_manager("42")
        self.assertEqual(result["deal"]["id"], "101")
