"""Real thread overlap with isolated SQLite and synthetic provider responses."""
from contextlib import contextmanager
import os
import threading
import unittest
from unittest.mock import patch

from claim_locks import ClaimLocks, hold_claim_operation
import test_app as fixtures

app = fixtures.app


class TestClaimLockRegistry(unittest.TestCase):
    def test_waiters_share_the_same_entry_and_cleanup_after_exception(self):
        locks = ClaimLocks()
        waiting = threading.Event()
        acquired = threading.Event()

        def worker():
            waiting.set()
            with locks.hold("42", "100"):
                acquired.set()

        with locks.hold("42", "100"):
            thread = threading.Thread(target=worker)
            thread.start()
            self.assertTrue(waiting.wait(1))
            self.assertFalse(acquired.wait(0.05))
            self.assertEqual(len(locks._entries), 2)
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertTrue(acquired.is_set())
        self.assertEqual(locks._entries, {})
        with self.assertRaises(RuntimeError):
            with locks.hold("42", "100", additional_manager_ids=["43", "42"]):
                self.assertEqual(len(locks._entries), 3)
                raise RuntimeError("synthetic failure")
        self.assertEqual(locks._entries, {})

    def test_changed_operation_owner_relocks_before_entering(self):
        locks = ClaimLocks()
        reads = iter(["42", "43", "43"])
        with hold_claim_operation(locks, "44", "100", lambda: next(reads)):
            self.assertEqual(set(locks._entries), {("manager", "43"), ("manager", "44"), ("deal", "100")})
        self.assertEqual(locks._entries, {})

    def test_empty_identity_is_rejected_without_retaining_entries(self):
        locks = ClaimLocks()
        for manager, deal in [("", None), ("42", "")]:
            with self.assertRaises(ValueError):
                with locks.hold(manager, deal):
                    self.fail("invalid identity acquired a fence")
        self.assertEqual(locks._entries, {})


class TestConcurrentClaims(fixtures.ClaimWorkflowTestCase):
    def setUp(self):
        environment = patch.dict(os.environ, fixtures._TEST_ENV)
        environment.start()
        self.addCleanup(environment.stop)
        network = patch.object(app.urllib.request, "urlopen", fixtures._network_is_forbidden)
        network.start()
        self.addCleanup(network.stop)
        super().setUp()
        self.locks = ClaimLocks()
        self.results = []
        self.errors = []
        self.remote_lock = threading.Lock()
        self.remote = {}
        self.updates = []
        self.reads = []

    @contextmanager
    def context(self, on_read=None):
        def call(method, params=None, timeout=None):
            deal_id = str(params["id"])
            if method == "crm.deal.get":
                with self.remote_lock:
                    self.reads.append(deal_id)
                if on_read:
                    on_read(deal_id)
                with self.remote_lock:
                    return dict(self.remote.get(deal_id) or {
                        "ID": deal_id, "TITLE": "Synthetic request",
                        "STAGE_ID": next(iter(app.SOURCE_STAGES)),
                        "ASSIGNED_BY_ID": "9", "DATE_MODIFY": self.version,
                    })
            if method == "crm.deal.update":
                fields = {key[7:-1]: value for key, value in params.items() if key.startswith("fields[")}
                with self.remote_lock:
                    self.updates.append((deal_id, str(fields["ASSIGNED_BY_ID"])))
                    self.remote[deal_id] = {"ID": deal_id, "TITLE": "Synthetic request", "DATE_MODIFY": "after", **fields}
                return True
            raise AssertionError("Unexpected provider method " + method)

        with (
            self.common_claim_context(),
            patch.object(app, "CLAIM_LOCKS", self.locks),
            patch.object(app, "get_manager_profile", side_effect=lambda manager: {
                "id": manager, "active": True, "intranet": True, "competencies": ["Турция"],
            }),
            patch.object(app, "is_limit_bypassed_now", return_value=False),
            patch.object(app, "bitrix_call", side_effect=call),
        ):
            yield

    def start_claim(self, deal_id, manager_id):
        policy = fixtures.test_manager_policy(rule=self.store.get_rule(manager_id))
        token = app.issue_selection_token(deal_id, manager_id, self.version, policy, now=1_000)

        def run():
            try:
                result = app.preview_claim(deal_id, manager_id, selection_token=token, send_greeting=False)
                self.results.append((deal_id, manager_id, result))
            except BaseException as error:
                self.errors.append(error)

        thread = threading.Thread(target=run)
        thread.start()
        return thread

    def join(self, threads):
        for thread in threads:
            thread.join(3)
            self.assertFalse(thread.is_alive(), "claim worker did not release its fences")
        self.assertEqual(self.errors, [])
        self.assertEqual(self.locks._entries, {})

    def test_independent_managers_and_deals_overlap_provider_reads(self):
        barrier = threading.Barrier(2)
        first_reads = set()

        def read(deal_id):
            with self.remote_lock:
                first = deal_id not in first_reads
                first_reads.add(deal_id)
            if first:
                barrier.wait(timeout=2)

        with self.context(read):
            self.join([self.start_claim("100", "42"), self.start_claim("101", "43")])
        self.assertEqual(len(self.updates), 2)
        self.assertTrue(all(result["ok"] for _, _, result in self.results))
        self.assertEqual(len(self.store.list_claims()), 2)

    def test_same_manager_cannot_exceed_one_claim_while_first_network_read_waits(self):
        self.store.set_rule("42", enabled=True, daily_limit=1)
        entered = threading.Event()
        release = threading.Event()

        def read(deal_id):
            if deal_id == "100":
                entered.set()
                self.assertTrue(release.wait(2))

        with self.context(read):
            first = self.start_claim("100", "42")
            try:
                self.assertTrue(entered.wait(1))
                second = self.start_claim("101", "42")
                self.assertTrue(second.is_alive())
            finally:
                release.set()
            self.join([first, second])
        self.assertEqual(self.updates, [("100", "42")])
        denied = next(result for deal, _, result in self.results if deal == "101")
        self.assertFalse(denied["ok"])
        self.assertTrue(denied["limitReached"])
        self.assertNotIn("101", self.reads)
        self.assertEqual(self.store.count_claims("42"), 1)

    def test_two_managers_claiming_one_deal_have_exactly_one_remote_write(self):
        entered = threading.Event()
        release = threading.Event()

        def read(_deal_id):
            entered.set()
            self.assertTrue(release.wait(2))

        with self.context(read):
            first = self.start_claim("100", "42")
            try:
                self.assertTrue(entered.wait(1))
                second = self.start_claim("100", "43")
            finally:
                release.set()
            self.join([first, second])
        self.assertEqual(self.updates, [("100", "42")])
        self.assertEqual(sum(bool(result["ok"]) for _, _, result in self.results), 1)
        self.assertEqual(len(self.store.list_claims()), 1)

    def test_admin_disable_waits_for_same_manager_but_unrelated_claim_finishes(self):
        entered = threading.Event()
        release = threading.Event()
        admin_done = threading.Event()
        admin_results = []

        def read(deal_id):
            if deal_id == "100":
                entered.set()
                self.assertTrue(release.wait(2))

        def disable():
            admin_results.append(app.update_admin_rule({"managerId": "42", "enabled": False}))
            admin_done.set()

        with self.context(read), patch.object(app, "require_admin", return_value={"id": "1"}):
            first = self.start_claim("100", "42")
            try:
                self.assertTrue(entered.wait(1))
                admin = threading.Thread(target=disable)
                admin.start()
                other = self.start_claim("101", "43")
                other.join(1)
                self.assertFalse(other.is_alive(), "an unrelated claim must not wait for the slow owner")
                self.assertFalse(admin_done.wait(0.05))
            finally:
                release.set()
            self.join([first, other, admin])
            after_disable = self.start_claim("102", "42")
            self.join([after_disable])
        self.assertTrue(admin_results[0]["ok"])
        self.assertEqual(set(self.updates), {("100", "42"), ("101", "43")})
        denied = next(result for deal, _, result in self.results if deal == "102")
        self.assertEqual(denied["_httpStatus"], 403)

    def test_recovery_by_another_manager_fences_original_owners_quota(self):
        self.store.set_rule("42", enabled=True, daily_limit=1)
        entered = threading.Event()
        release = threading.Event()

        def read(deal_id):
            if deal_id == "100":
                entered.set()
                self.assertTrue(release.wait(2))

        with self.context(read):
            self.begin_operation("42")
            self.store.fail_claim_operation(self.operation_key(), "unknown", result={"remoteUpdateUncertain": True})
            self.remote["100"] = self.claimed_deal(manager="42")
            recovery = self.start_claim("100", "43")
            try:
                self.assertTrue(entered.wait(1))
                next_claim = self.start_claim("101", "42")
                next_claim.join(0.05)
                self.assertTrue(next_claim.is_alive(), "original owner's quota must wait for audit recovery")
            finally:
                release.set()
            self.join([recovery, next_claim])
        self.assertEqual(self.updates, [])
        self.assertEqual(self.store.count_claims("42"), 1)
        denied = next(result for deal, _, result in self.results if deal == "101")
        self.assertTrue(denied["limitReached"])
        self.assertNotIn("101", self.reads)
        self.assertEqual(self.store.get_claim_operation(self.operation_key())["status"], "succeeded")

    def test_reconciler_does_not_use_a_listed_former_manager_fence(self):
        self.begin_operation()
        self.store.fail_claim_operation(self.operation_key(), "unknown", result={"remoteUpdateUncertain": True})
        original_get = self.store.get_claim_operation

        def changed_owner(key):
            return {**original_get(key), "managerId": "43"}

        with (
            patch.object(app, "CLAIM_LOCKS", self.locks),
            patch.object(self.store, "get_claim_operation", side_effect=changed_owner),
            patch.object(app, "bitrix_call") as remote,
        ):
            result = app.reconcile_stale_claim_operations()
        self.assertEqual(result["checked"], 0)
        remote.assert_not_called()
        self.assertEqual(self.store.list_claims(), [])
        self.assertEqual(self.locks._entries, {})


if __name__ == "__main__":
    unittest.main()
