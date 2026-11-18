"""Synthetic Bitrix claims with a real SQLite outbox and a controlled remote pin."""
import contextlib
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import app
from test_app import ClaimWorkflowTestCase


class TestClaimGreetingReadiness(ClaimWorkflowTestCase):
    def setUp(self):
        super().setUp()
        self.deal = dict(self.claimed_deal(), CATEGORY_ID="0", CLOSED="N", STAGE_SEMANTIC_ID="P")
        self.store.set_rule(self.manager_id, enabled=True)
        self.receipt = "ready"
        self.readiness_checks = []
        self.sent = []
        self.session = "321"
        self.target = {"sessionId": self.session, "chatId": "1763"}
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.multiple(app, DRY_RUN=False, GREETING_AUTO_SEND=True, GREETING_AUTO_SEND_SUPPORTED=True))
            stack.enter_context(patch.object(app, "baza_integration_configured", return_value=True))
            stack.enter_context(patch.object(app, "claim_event_delivery_enabled", return_value=True))
            stack.enter_context(patch.object(app, "baza_post", side_effect=self.baza_readiness))
            stack.enter_context(patch.object(app, "get_greeting_manager_profile", return_value={
                "id": self.manager_id, "name": "Test", "active": True, "intranet": True,
            }))
            stack.enter_context(patch.object(app, "bitrix_call", side_effect=self.crm_get))
            stack.enter_context(patch.object(app, "require_available_greeting_chat"))
            stack.enter_context(patch.object(app, "resolve_greeting_target", return_value=self.target))
            self.quiet = stack.enter_context(patch.object(app, "require_quiet_claim_dialog"))
            stack.enter_context(patch.object(app, "send_greeting_message", side_effect=self.send))
            self.addCleanup(stack.pop_all().close)

    def baza_readiness(self, path, payload, *, timeout):
        self.assertEqual(path, "/integrations/deal-picker/v1/claim-dialog-readiness")
        self.assertEqual(payload, {"operationKey": self.operation_key(), "bitrixDealId": self.deal_id,
                                  "bitrixUserId": self.manager_id, **self.target})
        self.readiness_checks.append(payload)
        return {"schemaVersion": 1, "status": self.receipt}

    def crm_get(self, method, params=None, timeout=None):
        self.assertEqual((method, params), ("crm.deal.get", {"id": self.deal_id}))
        return dict(self.deal)

    def send(self, deal_id, manager_id, text, context, *, target):
        self.assertEqual((deal_id, manager_id, target), (self.deal_id, self.manager_id, self.target))
        self.sent.append(self.store.get_greeting_outbox(self.operation_key())["status"])
        return {"messageId": "42"}

    def seed(self, age=0):
        self.start = (datetime.now(timezone.utc) - timedelta(seconds=age)).isoformat()
        self.store.begin_claim_operation(self.deal_id, self.manager_id, operation_key=self.operation_key(), request={
            "claimMarker": self.attempt_marker(), "attemptStartedAt": self.start, "greetingRequested": True,
            "greetingContext": {"sessionId": self.session, "direction": "Турция", "deliveryVersion": 2, "categoryId": "0"},
        })
        result = self.store.finalize_claim_operation(self.operation_key(),
            claim=app.claim_log_entry(self.manager_id, self.deal, timestamp=self.start),
            result={"ok": True, "auditRecorded": True}, expected_claim_marker=self.attempt_marker())
        self.assertTrue(result["greetingQueued"])

    def run_job(self):
        app.process_greeting_outbox_once("synthetic-worker")
        return self.store.get_greeting_outbox(self.operation_key())

    def recorded_activation_wait(self):
        with self.store._connect() as db:
            db.execute("UPDATE greeting_outbox SET error_code='chat_activation_pending',next_attempt_at=NULL,attempt_count=4 WHERE operation_key=?",
                       (self.operation_key(),))

    def test_pending_pin_then_exact_pin_on_open_progressed_deal(self):
        self.seed()
        self.receipt = "pending"
        self.assertEqual(self.run_job()["errorCode"], "chat_activation_pending")
        self.assertEqual(self.sent, [])
        self.recorded_activation_wait()
        self.deal["STAGE_ID"] = "EXECUTING"
        self.receipt = "ready"
        self.assertEqual(self.run_job()["status"], "sent")
        self.assertEqual(self.run_job()["messageId"], "42")
        self.assertEqual(self.sent, ["dispatching"])
        self.quiet.assert_called_once()

    def test_nine_minutes_only_for_recorded_wait_and_unanswered_pinned_session(self):
        self.seed(age=540)
        self.recorded_activation_wait()
        self.assertEqual(self.run_job()["status"], "sent")
        self.assertEqual(self.quiet.call_args.kwargs["claimed_at"], self.start)
        self.assertEqual(self.sent, ["dispatching"])

    def test_late_first_attempt_stays_closed(self):
        self.seed(age=122)
        self.assertEqual(self.run_job()["errorCode"], "greeting_window_expired")
        self.assertEqual(self.readiness_checks, [])

    def test_ten_minutes_is_a_hard_limit_after_restart(self):
        self.seed(age=601)
        self.recorded_activation_wait()
        self.assertEqual(self.run_job()["errorCode"], "greeting_window_expired")
        self.assertEqual(self.readiness_checks, [])

    def test_existing_staff_reply_on_local_channel_blocks_greeting(self):
        self.seed()
        self.receipt = "alreadyReplied"
        job = self.run_job()
        self.assertEqual((job["status"], job["errorCode"]), ("manual", "greeting_staff_message_seen"))
        self.assertEqual(self.sent, [])

    def test_native_staff_reply_on_advanced_stage_blocks_greeting(self):
        self.seed()
        self.deal["STAGE_ID"] = "EXECUTING"
        self.quiet.side_effect = RuntimeError("greeting_staff_message_seen")
        self.assertEqual(self.run_job()["errorCode"], "greeting_staff_message_seen")
        self.assertEqual(self.sent, [])

    def test_advanced_deal_still_requires_current_owner_at_last_read(self):
        self.seed()
        self.deal["STAGE_ID"] = "EXECUTING"
        self.quiet.side_effect = lambda *args, **kwargs: self.deal.update(ASSIGNED_BY_ID="77777")
        self.assertEqual(self.run_job()["errorCode"], "claim_marker_mismatch")
        self.assertEqual(self.sent, [])

    def test_unmapped_service_worker_cannot_pin_as_employee(self):
        self.seed()
        self.receipt = "unmapped"
        self.assertEqual(self.run_job()["errorCode"], "greeting_manager_unmapped")
        self.assertEqual(self.sent, [])

    def test_previous_baza_cannot_enable_a_stage_that_was_previously_blocked(self):
        self.seed()
        self.deal["STAGE_ID"] = "EXECUTING"
        with patch.object(app, "baza_post", side_effect=app.BazaIntegrationHttpError(404)):
            self.assertEqual(self.run_job()["errorCode"], "greeting_guard_unavailable")
        self.assertEqual(self.sent, [])

    def test_terminal_requeued_other_pipeline_or_unrelated_stage_never_passes(self):
        self.seed()
        operation = self.store.get_claim_operation(self.operation_key(), for_greeting=True)
        job = self.store.get_greeting_outbox(self.operation_key())
        for changes in ({"STAGE_ID": "LOSE", "STAGE_SEMANTIC_ID": "F"}, {"STAGE_ID": "EXECUTING", "CLOSED": "Y"},
                        {"STAGE_ID": "EXECUTING", "CATEGORY_ID": "3"}, {"STAGE_ID": "EXECUTING", "STAGE_SEMANTIC_ID": "S"},
                        {"STAGE_ID": "UC_PUCAAQ"}, {"STAGE_ID": "PREPAYMENT_INVOICE"}):
            with self.subTest(changes=changes), self.assertRaisesRegex(RuntimeError, "claim_marker_mismatch"):
                app.require_greeting_deal(job, operation, dict(self.deal, **changes))

    def test_legacy_endpoint_fallback_rechecks_owner_after_the_http_call(self):
        self.seed()
        def old_server(*args, **kwargs):
            self.deal.update(STAGE_ID="EXECUTING")
            raise app.BazaIntegrationHttpError(404)
        with patch.object(app, "baza_post", side_effect=old_server):
            self.assertEqual(self.run_job()["errorCode"], "greeting_guard_unavailable")
        self.assertEqual(self.sent, [])

    def test_regular_pin_check_during_stage_change_requires_timestamped_history(self):
        self.seed()
        def ready_after_stage_change(*unused, **kwargs):
            self.deal.update(STAGE_ID="EXECUTING")
            return {"schemaVersion": 1, "status": "ready"}
        self.quiet.side_effect = RuntimeError("greeting_staff_message_seen")
        with patch.object(app, "baza_post", side_effect=ready_after_stage_change):
            self.assertEqual(self.run_job()["errorCode"], "greeting_staff_message_seen")
        self.assertEqual(self.sent, [])

    def test_unverified_readiness_cannot_extend_the_activation_window(self):
        self.seed()
        self.receipt = "unknown"
        job = self.run_job()
        self.assertEqual(job["errorCode"], "greeting_guard_unavailable")
        self.assertEqual(app.greeting_check_limit(job["errorCode"], self.store.get_claim_operation(
            self.operation_key(), for_greeting=True)), 3)
        self.assertEqual(self.sent, [])

    def test_private_greeting_policy_survives_restart_and_remains_redacted_on_status(self):
        self.seed()
        self.assertNotIn("greetingContext", self.store.get_claim_operation(self.operation_key())["request"])
        internal = self.store.get_claim_operation(self.operation_key(), for_greeting=True)
        self.assertEqual(internal["request"]["greetingContext"]["categoryId"], "0")
        self.assertEqual(internal["request"]["greetingContext"]["deliveryVersion"], 2)

    def test_personal_sender_does_not_accept_a_progressed_chat_to_send_a_late_greeting(self):
        self.seed()
        self.deal["STAGE_ID"] = "EXECUTING"
        self.receipt = "alreadyReplied"
        leased = self.store.lease_exact_greeting_outbox(self.operation_key(), "personal")
        with (patch.object(app, "extract_auth_credentials", return_value=("ephemeral", "fake.example")),
              patch.object(app, "actor_greeting_chat_context", return_value=self.target),
              patch.object(app, "answer_openline_for_actor") as answer,
              patch.object(app, "send_chat_message_for_actor") as send):
            job = app.process_actor_greeting_outbox_job(leased, "personal", {"fake": True}, {
                "id": self.manager_id, "active": True, "intranet": True,
            })
        self.assertEqual(job["errorCode"], "greeting_staff_message_seen")
        answer.assert_not_called()
        send.assert_not_called()

    def test_native_sender_observes_a_late_export_before_handing_off_the_personal_token(self):
        self.seed()
        internal = self.store.get_claim_operation(self.operation_key(), for_greeting=True)
        internal["request"]["greetingContext"]["entrypoint"] = "bitrix"
        job = self.store.get_greeting_outbox(self.operation_key())
        with (patch.object(app, "greeting_pin_receipt", side_effect=["unknown", "unmapped"]),
              patch.object(app, "cached_verified_actor_auth", return_value={"access_token": "fake"}),
              patch.object(app.time, "sleep") as wait,
              patch.object(app, "ensure_unmapped_actor_pin") as pin):
            app.check_claim_greeting_before_send(job, internal, self.target, actor_auth={"token": "fake"}, worker_token="personal")
        wait.assert_called_once_with(2)
        self.assertEqual(pin.call_args.kwargs["receipt"], "unmapped")
