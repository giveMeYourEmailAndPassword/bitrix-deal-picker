"""A physical Bitrix session may cross the greeting dispatch boundary only once."""
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from test_state_store import StateStoreTestCase


class TestGreetingSessionDispatch(StateStoreTestCase):
    def checking(self, store, key, deal, session):
        request = {"claimMarker": key, "greetingRequested": True,
                   "greetingContext": {"sessionId": session, "direction": "Турция"}}
        store.begin_claim_operation(deal, "20", operation_key=key, request=request)
        store.finalize_claim_operation(key, claim={"dealId": deal, "managerId": "20"}, expected_claim_marker=key)
        self.assertTrue(store.lease_exact_greeting_outbox(key, key)["transitioned"])
        self.assertTrue(store.update_greeting_outbox_check(key, key, text="Проверочное приветствие")["transitioned"])

    def test_two_processes_racing_on_one_session_allow_one_send_after_restart(self):
        first_store = self.make_store()
        second_store = self.make_store()
        self.checking(first_store, "one", "101", "700")
        self.checking(second_store, "two", "102", "700")
        barrier = Barrier(2)

        def dispatch(store, key):
            barrier.wait(timeout=5)
            return store.mark_greeting_outbox_dispatching(key, key, chat_id="800")

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(dispatch, store, key) for store, key in
                       ((first_store, "one"), (second_store, "two"))]
            outcomes = [f.result(timeout=8) for f in futures]
        winner, = [r for r in outcomes if r["transitioned"]]
        stopped, = [r for r in outcomes if not r["transitioned"]]
        self.assertEqual(winner["status"], "dispatching")
        self.assertIsNotNone(winner["dispatchingAt"])
        self.assertEqual((stopped["status"], stopped["errorCode"], stopped["dispatchingAt"]),
                         ("manual", "greeting_session_already_contacted", None))
        restarted = self.make_store()
        stopped_key = stopped["operationKey"]
        self.assertFalse(restarted.mark_greeting_outbox_dispatching(stopped_key, stopped_key)["transitioned"])
        self.assertFalse(restarted.lease_exact_greeting_outbox(stopped_key, "retry")["transitioned"])
        self.assertEqual(restarted.lease_greeting_outbox("worker"), [])

    def test_legacy_sent_uncertain_and_partially_recorded_dispatches_block_the_session(self):
        store = self.make_store()
        for n, (old_status, boundary) in enumerate([("sent", None), ("uncertain", None), ("dispatching", None),
                                                   ("manual", "2026-11-18T18:00:00+00:00")]):
            with self.subTest(old_status=old_status):
                session, old, new = str(1000 + n), f"old-{n}", f"new-{n}"
                self.checking(store, old, str(2000 + n * 2), session)
                with store._transaction(immediate=True) as db:
                    db.execute("UPDATE greeting_outbox SET status=?,dispatching_at=? WHERE operation_key=?",
                               (old_status, boundary, old))
                self.checking(store, new, str(2001 + n * 2), session)
                result = store.mark_greeting_outbox_dispatching(new, new, chat_id="800")
                self.assertFalse(result["transitioned"])
                self.assertEqual(result["errorCode"], "greeting_session_already_contacted")
                self.assertEqual(store.get_greeting_outbox(old)["status"], old_status)

    def test_new_session_on_same_chat_and_old_unsent_manual_decision_stay_independent(self):
        store = self.make_store()
        self.checking(store, "first", "101", "700")
        self.assertTrue(store.mark_greeting_outbox_dispatching("first", "first", chat_id="800")["transitioned"])
        self.checking(store, "manual", "102", "701")
        self.assertTrue(store.mark_greeting_outbox_manual("manual", "manual", error_code="chat_entity_mismatch")["transitioned"])
        self.assertFalse(store.mark_greeting_outbox_dispatching("manual", "manual", chat_id="800")["transitioned"])
        self.checking(store, "second", "103", "701")
        self.assertTrue(store.mark_greeting_outbox_dispatching("second", "second", chat_id="800")["transitioned"])
