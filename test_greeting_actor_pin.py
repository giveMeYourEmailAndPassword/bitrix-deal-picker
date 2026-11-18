from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import Mock

from greeting_actor_pin import GreetingActorPinError, ensure_unmapped_actor_pin
from test_state_store import StateStoreTestCase


class TestGreetingActorPin(StateStoreTestCase):
    def setUp(self):
        super().setUp()
        self.store = self.make_store()
        operation, deal, self.manager, self.session, self.chat = "canonical", "101", "20", "700", "800"
        self.store.begin_claim_operation(deal, self.manager, operation_key=operation,
            request={"claimMarker": "marker", "greetingRequested": True,
                     "greetingContext": {"sessionId": self.session, "direction": "Турция"}})
        self.store.finalize_claim_operation(operation, claim={"dealId": deal, "managerId": self.manager},
                                           expected_claim_marker="marker")
        self.lease = "worker"
        self.job = self.store.lease_exact_greeting_outbox(operation, self.lease)
        self.assertTrue(self.job["transitioned"])
        self.auth = {"access_token": "test-only-secret", "domain": "example.test"}
        self.verify = Mock(return_value=self.auth)
        self.check = Mock(return_value="unmapped")
        self.actor = Mock(return_value=True)

    def run_pin(self, store=None, job=None, worker=None, chat=None, **changes):
        args = dict(receipt="unmapped", verify_actor=self.verify, revalidate=self.check, actor_call=self.actor)
        return ensure_unmapped_actor_pin(store or self.store, job or self.job, worker or self.lease,
                                         self.auth, chat or self.chat, **{**args, **changes})

    def refused(self, reason, **changes):
        with self.assertRaises(GreetingActorPinError) as raised:
            self.run_pin(**changes)
        self.assertEqual(str(raised.exception), reason)

    def test_success_token_not_stored_and_replay_with_original_checked_identity(self):
        self.assertTrue(self.run_pin())
        self.actor.assert_called_once_with(self.auth, "imopenlines.session.mode.pin", {"CHAT_ID":"800","ACTIVATE":"Y"})
        self.assertEqual(self.check.call_count, 2)
        self.assertTrue(self.run_pin(store=self.make_store()))
        self.assertEqual(self.actor.call_count, 1)
        self.assertEqual(self.check.call_count, 3)
        self.verify.assert_called_with(self.auth, self.manager)
        with self.store._connect() as db:
            fact = dict(db.execute("SELECT * FROM greeting_actor_pins").fetchone())
        self.assertTrue(fact["acknowledged_at"])
        self.assertNotIn(self.auth["access_token"], str(fact))

    def test_explicit_unmapped_only_and_cached_recipient_required(self):
        for status in ("ready", "unknown", None):
            self.refused("greeting_pin_receipt_changed", receipt=status)
        self.refused("greeting_staff_message_seen", receipt="alreadyReplied")
        self.verify.return_value = None
        self.refused("greeting_actor_pin_unavailable")
        self.verify.return_value = {}
        self.refused("greeting_actor_pin_unavailable")
        self.actor.assert_not_called()

    def test_changed_before_pin_rejects_without_reserving(self):
        self.check.side_effect = RuntimeError("claim_marker_mismatch")
        with self.assertRaisesRegex(RuntimeError, "^claim_marker_mismatch$"):
            self.run_pin()
        self.actor.assert_not_called()
        with self.store._connect() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM greeting_actor_pins").fetchone()[0], 0)

    def test_expired_or_wrong_lease_and_replaced_manager_or_session_cannot_pin(self):
        self.refused("greeting_actor_pin_unavailable", worker="foreign")
        for changed in ({"sessionId": "701"}, {"dealId": "102"}, {"managerId": "21"}, {"operationKey": "foreign"}):
            self.refused("greeting_actor_pin_unavailable", job={**self.job, **changed})
        with self.store._transaction(immediate=True) as db:
            db.execute("UPDATE greeting_outbox SET lease_expires_at='2000-01-01'")
        self.refused("greeting_actor_pin_unavailable")
        self.actor.assert_not_called()

    def test_false_truthy_non_boolean_and_lost_responses_are_never_retried(self):
        for result in (False, 1, "true", TimeoutError("test-only secret")):
            with self.subTest(result=type(result).__name__):
                if isinstance(result, Exception):
                    self.actor.side_effect = result
                else:
                    self.actor.side_effect = None
                    self.actor.return_value = result
                self.refused("greeting_actor_pin_uncertain")
                self.refused("greeting_actor_pin_uncertain", store=self.make_store())
                self.assertEqual(self.actor.call_count, 1)
                # Independent synthetic operation scenarios use the temp database.
                with self.store._transaction(immediate=True) as db:
                    db.execute("DELETE FROM greeting_actor_pins")
                self.actor.reset_mock()

    def test_crash_after_reservation_is_uncertain_after_restart(self):
        def crash(*args):
            raise SystemExit("simulated abrupt process death")

        self.actor.side_effect = crash
        with self.assertRaises(SystemExit):
            self.run_pin()
        self.refused("greeting_actor_pin_uncertain", store=self.make_store())
        self.assertEqual(self.actor.call_count, 1)

    def test_mapping_or_reply_changed_during_rpc_blocks_greeting(self):
        self.check.side_effect = ["unmapped", "alreadyReplied"]
        self.refused("greeting_staff_message_seen")
        self.actor.assert_called_once()
        self.check.side_effect = ["alreadyReplied"]
        self.refused("greeting_staff_message_seen", store=self.make_store())
        self.assertEqual(self.actor.call_count, 1)

    def test_changed_crm_during_pin_prevents_send_even_if_acknowledged(self):
        self.check.side_effect = ["unmapped", RuntimeError("claim_marker_mismatch")]
        with self.assertRaisesRegex(RuntimeError, "^claim_marker_mismatch$"):
            self.run_pin()
        self.actor.assert_called_once()
        self.check.side_effect = None
        self.refused("greeting_actor_pin_unavailable", chat="999")
        self.assertEqual(self.actor.call_count, 1)

    def test_concurrent_observer_and_a_newer_canonical_assignment(self):
        entered, release = Event(), Event()
        def blocking(*args):
            entered.set()
            self.assertTrue(release.wait(5))
            return True
        self.actor.side_effect = blocking
        with ThreadPoolExecutor(max_workers=2) as pool:
            writer = pool.submit(self.run_pin)
            self.assertTrue(entered.wait(5))
            try:
                self.refused("greeting_actor_pin_uncertain", store=self.make_store())
            finally:
                release.set()
            self.assertTrue(writer.result(5))
        self.assertEqual(self.actor.call_count, 1)
        self.store.append_claim({"dealId": "101", "managerId": "21"}, operation_key="historical-newer")
        self.refused("greeting_actor_pin_unavailable")
        self.assertEqual(self.actor.call_count, 1)
