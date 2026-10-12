"""Contact responsibility with no network: durable claims, fences and lost replies."""

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from contact_owner_sync import ContactOwnerSync
from state_store import StateStore


class ContactOwnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = StateStore(Path(self.tmp.name))
        self.remote = {"owner": "9", "deals": {}, "writes": [], "calls": []}
        self.before_write = None
        self.fail_reads = False
        self.fail_update = False
        self.apply_before_timeout = False
        self.sync = ContactOwnerSync(self.store, self.call, "UF_MARKER")

    def call(self, method, params, timeout=None):
        self.remote["calls"].append(method)
        if self.fail_reads and method.endswith(".get"):
            raise TimeoutError()
        if method == "crm.deal.get":
            return dict(self.remote["deals"][str(params["id"])])
        if method == "crm.contact.get":
            return {"ID": str(params["id"]), "ASSIGNED_BY_ID": self.remote["owner"]}
        self.assertEqual(method, "crm.contact.update")
        self.assertEqual(set(params), {"id", "fields[ASSIGNED_BY_ID]"})
        self.remote["writes"].append(dict(params))
        if self.before_write:
            self.before_write()
        if not self.fail_update or self.apply_before_timeout:
            self.remote["owner"] = params["fields[ASSIGNED_BY_ID]"]
        if self.fail_update:
            raise TimeoutError()
        return True

    def claim(self, deal="100", manager="42", *, confirm=True, context=True):
        key, marker = "claim:" + deal, "marker:" + deal
        request = {"claimMarker": marker}
        if context:
            request["contactOwnerContext"] = {"contactId": "7", "previousOwner": self.remote["owner"]}
        self.store.begin_claim_operation(deal, manager, operation_key=key, request=request)
        self.remote["deals"][deal] = {"ID": deal, "CONTACT_ID": "7", "ASSIGNED_BY_ID": manager, "UF_MARKER": marker}
        if confirm:
            self.confirm(key, deal, manager, marker)
        return key

    def confirm(self, key, deal, manager, marker):
        return self.store.finalize_claim_operation(key,
            claim={"dealId": deal, "managerId": manager}, expected_claim_marker=marker)

    def due(self):
        with self.store._transaction(immediate=True) as db:
            db.execute("UPDATE contact_owner_jobs SET next_attempt_at='2000-01-01',lease_expires_at='2000-01-01'")

    def test_claim_and_contact_job_commit_together_and_replay_does_not_write_again(self):
        key = self.claim()
        self.assertEqual(self.sync.status(key), {"status": "pending"})
        self.sync.process(key)
        self.assertEqual(self.remote["owner"], "42")
        self.assertEqual(self.sync.status(key), {"status": "ready"})
        self.confirm(key, "100", "42", "marker:100")
        self.sync.process(key)
        self.assertEqual(len(self.remote["writes"]), 1)

    def test_reservation_does_not_assign_contact_before_claim_confirmed(self):
        key = self.claim(confirm=False)
        self.assertFalse(self.sync.process(key))
        self.assertEqual(self.remote["calls"], [])

    def test_old_claim_without_snapshot_never_backfills(self):
        key = self.claim(context=False)
        self.assertIsNone(self.sync.status(key))
        self.assertFalse(self.sync.process(key))
        self.assertEqual(self.remote["calls"], [])

    def test_manual_contact_change_after_claim_is_preserved(self):
        key = self.claim()
        self.remote["owner"] = "99"
        self.sync.process(key)
        self.assertEqual(self.sync.status(key), {"status": "conflict", "code": "contact_owner_changed"})
        self.assertEqual(self.remote["writes"], [])

    def test_deal_owner_marker_and_binding_changes_each_stop_write(self):
        for field, value in (("ASSIGNED_BY_ID", "99"), ("UF_MARKER", "new"), ("CONTACT_ID", "99")):
            with self.subTest(field=field):
                deal = str(100 + len(self.remote["deals"]))
                key = self.claim(deal)
                self.remote["deals"][deal][field] = value
                self.sync.process(key)
                self.assertEqual(self.sync.status(key)["status"], "conflict")
        self.assertEqual(self.remote["writes"], [])

    def test_newer_claim_on_another_deal_supersedes_old_retry(self):
        old = self.claim()
        new = self.claim("101", "43")
        self.sync.process(old)
        self.sync.process(new)
        self.assertEqual(self.remote["writes"], [{"id": "7", "fields[ASSIGNED_BY_ID]": "43"}])
        self.assertEqual(self.sync.status(old)["code"], "newer_contact_claim")

    def test_recovery_does_not_make_an_older_claim_the_newest(self):
        old = self.claim(confirm=False)
        new = self.claim("101", "43")
        self.confirm(old, "100", "42", "marker:100")
        self.sync.process(new)
        self.sync.process(old)
        self.assertEqual(self.remote["owner"], "43")
        self.assertEqual(len(self.remote["writes"]), 1)

    def test_newer_reservation_waits_then_cancelled_reservation_releases_old(self):
        old = self.claim()
        newer = self.claim("101", "43", confirm=False)
        self.sync.process(old)
        self.assertEqual(self.sync.status(old)["code"], "newer_contact_claim_pending")
        self.store.fail_claim_operation(newer, "not_applied", result={"remoteUpdated": False})
        self.due()
        self.sync.process(old)
        self.assertEqual(self.remote["owner"], "42")

    def test_later_claim_can_follow_known_inflight_prior_contact_write(self):
        old = self.claim()
        self.before_write = lambda: self.claim("101", "43")
        self.sync.process(old)
        self.before_write = None
        self.sync.process("claim:101")
        self.assertEqual(self.remote["owner"], "43")
        self.assertEqual(len(self.remote["writes"]), 2)

    def test_prior_write_can_finish_between_later_snapshot_and_reservation(self):
        old = self.claim()
        started = self.store._now_iso()
        observed_owner = self.remote["owner"]
        self.sync.process(old)
        generation = self.sync.reserve_native("native:session:2", "101", "43", "7",
            observed_owner, "", snapshot_started_at=started)
        self.remote["deals"]["101"] = {"ID": "101", "CONTACT_ID": "7", "ASSIGNED_BY_ID": "43"}
        self.sync.confirm_native(generation)
        self.sync.process("native:session:2")
        self.assertEqual(self.remote["owner"], "43")

    def test_unknown_write_after_known_prior_transition_keeps_contact_fenced(self):
        old = self.claim()
        self.before_write = lambda: self.claim("101", "43")
        self.sync.process(old)
        self.before_write = None
        self.fail_update = True
        self.sync.process("claim:101")
        self.assertEqual(self.sync.status("claim:101")["code"], "contact_update_uncertain")
        newer = self.claim("102", "44")
        self.due()
        self.sync.process("claim:101")
        self.assertFalse(self.sync.process(newer))
        self.assertEqual(self.remote["owner"], "42")
        self.assertEqual(len(self.remote["writes"]), 2)

    def test_unknown_write_with_third_party_owner_still_keeps_fence(self):
        key = self.claim()
        self.fail_update = True
        self.sync.process(key)
        self.remote["owner"] = "99"
        self.due()
        self.sync.process(key)
        self.assertEqual(self.sync.status(key)["code"], "contact_update_uncertain")
        self.assertEqual(len(self.remote["writes"]), 1)

    def test_manual_return_to_historical_owner_is_not_a_known_new_transition(self):
        old = self.claim()
        self.sync.process(old)
        self.remote["owner"] = "9"
        new = self.claim("101", "43")
        self.remote["owner"] = "42"  # manual reassignment after the new claim
        self.sync.process(new)
        self.assertEqual(self.sync.status(new)["code"], "contact_owner_changed")
        self.assertEqual(len(self.remote["writes"]), 1)

    def test_read_failure_retries_safely_after_restart(self):
        key = self.claim()
        self.fail_reads = True
        self.sync.process(key)
        self.fail_reads = False
        self.due()
        restarted = ContactOwnerSync(StateStore(Path(self.tmp.name)), self.call, "UF_MARKER")
        restarted.process(key)
        self.assertEqual(restarted.status(key), {"status": "ready"})

    def test_lost_update_reply_is_read_back_without_duplicate_write(self):
        key = self.claim()
        self.fail_update = self.apply_before_timeout = True
        self.sync.process(key)
        self.assertEqual(self.sync.status(key), {"status": "ready"})
        self.due()
        self.sync.process(key)
        self.assertEqual(len(self.remote["writes"]), 1)

    def test_uncertain_dispatch_never_repeats_and_blocks_newer_write(self):
        old = self.claim()
        self.fail_update = True
        self.sync.process(old)
        new = self.claim("101", "43")
        self.due()
        self.sync.process(old)
        self.assertFalse(self.sync.process(new))
        self.assertEqual(len(self.remote["writes"]), 1)
        self.assertEqual(self.sync.status(old)["code"], "contact_update_uncertain")

    def test_confirmed_rejection_is_terminal_and_does_not_block_newer_claim(self):
        old = self.claim()
        original = self.sync.call
        self.sync.call = lambda method, params, timeout=None: False if method == "crm.contact.update" else original(method, params, timeout)
        self.sync.process(old)
        self.assertEqual(self.sync.status(old)["code"], "contact_update_rejected")
        self.sync.call = original
        new = self.claim("101", "43")
        self.sync.process(new)
        self.assertEqual(self.remote["owner"], "43")

    def test_expired_dispatch_lease_recovers_by_read_only(self):
        key = self.claim()
        job = self.sync.lease(key)
        self.assertEqual(self.sync.current(job, dispatch=True, dispatch_owner="9"), "current")
        self.remote["owner"] = "42"
        self.due()
        self.sync.process(key)
        self.assertEqual(self.sync.status(key), {"status": "ready"})
        self.assertEqual(self.remote["writes"], [])

    def test_shared_contact_cannot_be_leased_by_two_workers(self):
        old = self.claim()
        self.assertIsNotNone(self.sync.lease(old))
        new = self.claim("101", "43")
        other = ContactOwnerSync(StateStore(Path(self.tmp.name)), self.call, "UF_MARKER")
        self.assertIsNone(other.lease(new))

    def test_already_assigned_contact_needs_no_update(self):
        key = self.claim()
        self.remote["owner"] = "42"
        self.sync.process(key)
        self.assertEqual(self.sync.status(key), {"status": "ready"})
        self.assertEqual(self.remote["writes"], [])

    def test_native_and_picker_share_ordering_and_allow_empty_preserved_marker(self):
        old = self.claim()
        self.remote["deals"]["101"] = {"ID": "101", "CONTACT_ID": "7", "ASSIGNED_BY_ID": "43"}
        generation = self.sync.reserve_native("native:session:1", "101", "43", "7", "9", "")
        self.assertEqual(self.sync.reserve_native("native:session:1", "101", "43", "7", "9", ""), generation)
        self.sync.confirm_native(generation)
        self.sync.process(old)
        self.sync.process("native:session:1")
        self.assertEqual(self.remote["owner"], "43")
        self.assertEqual(len(self.remote["writes"]), 1)

    def test_delayed_native_acceptance_cannot_supersede_later_picker_claim(self):
        observed_at = self.store._now_iso()
        self.claim()
        with self.assertRaisesRegex(ValueError, "newer_contact_claim"):
            self.sync.reserve_native("native:old:1", "101", "43", "7", "9", "", observed_at=observed_at)

    def test_conflicted_native_write_keeps_terminal_generation_fence(self):
        old = self.claim()
        generation = self.sync.reserve_native("native:session:1", "101", "43", "7", "9", "")
        self.sync.conflict_native(generation)
        self.sync.process(old)
        self.assertEqual(self.sync.status(old)["code"], "newer_contact_claim")
        self.assertEqual(self.remote["writes"], [])

    def test_native_crash_cleanup_cancels_only_exact_unconfirmed_reservation(self):
        self.sync.reserve_native("native:crashed:1", "101", "43", "7", "9", "")
        confirmed = self.sync.reserve_native("native:confirmed:1", "102", "44", "8", "9", "")
        self.sync.confirm_native(confirmed)
        self.sync.reserve_native("native:other:1", "103", "45", "9", "9", "")
        self.claim("104", confirm=False)
        self.sync.cancel_native_operation("native:crashed:1")
        self.sync.cancel_native_operation("native:confirmed:1")
        self.assertEqual(self.sync.status("native:crashed:1"), {"status": "cancelled"})
        self.assertEqual(self.sync.status("native:confirmed:1"), {"status": "pending"})
        self.assertEqual(self.sync.status("native:other:1"), {"status": "pending"})
        with self.assertRaisesRegex(ValueError, "invalid_native_contact_operation"):
            self.sync.cancel_native_operation("claim:104")
        self.assertEqual(self.sync.status("claim:104"), {"status": "pending"})

    def test_retrying_contacts_do_not_starve_a_fresh_contact(self):
        for offset in range(4):
            deal = str(200 + offset)
            key = "claim:" + deal
            marker = "marker:" + deal
            self.store.begin_claim_operation(deal, "42", operation_key=key, request={
                "claimMarker": marker, "contactOwnerContext": {
                    "contactId": str(7 + offset), "previousOwner": "9"}})
            self.confirm(key, deal, "42", marker)
        with self.store._transaction(immediate=True) as db:
            db.execute("UPDATE contact_owner_jobs SET next_attempt_at='2000-01-01'")
            db.execute("UPDATE contact_owner_jobs SET dispatched=1,dispatch_owner='9' WHERE generation<=3")
        original = self.store._outbox_datetime
        clock = [datetime.now(self.store.local_timezone)]
        seen = []
        with patch.object(self.store, "_outbox_datetime", side_effect=lambda value=None: clock[0] if value is None else original(value)):
            for _ in range(4):
                job = self.sync.lease()
                seen.append(job["deal_id"])
                self.sync.finish(job, "pending", "contact_update_uncertain")
                # Retry jobs become due again before the next tick. The fresh
                # contact's older due time must still advance through the queue.
                clock[0] += timedelta(seconds=61)
        self.assertEqual(seen, ["200", "201", "202", "203"])

# Exercise the actual picker workflow; all provider methods remain in-memory.
from unittest.mock import patch
from test_app import ClaimWorkflowTestCase, app


class ContactClaimWorkflowTests(ClaimWorkflowTestCase):
    def setUp(self):
        super().setUp()
        signing = patch.object(app, "selection_signing_key", return_value=b"contact-claim-test-key")
        signing.start()
        self.addCleanup(signing.stop)

    def test_claim_assigns_deal_and_primary_contact_then_replay_is_read_only(self):
        remote = {**self.source_deal(), "CONTACT_ID": "7"}
        contact = {"ID": "7", "ASSIGNED_BY_ID": "9"}
        writes = []

        def call(method, params=None, timeout=None):
            if method == "crm.deal.get":
                return dict(remote)
            if method == "crm.contact.get":
                return dict(contact)
            writes.append(method)
            if method == "crm.deal.update":
                for name, value in params.items():
                    if name.startswith("fields["):
                        remote[name[7:-1]] = value
                return True
            if method == "crm.contact.update":
                contact["ASSIGNED_BY_ID"] = params["fields[ASSIGNED_BY_ID]"]
                return True
            raise AssertionError(method)

        with self.common_claim_context(), patch.object(app, "bitrix_call", side_effect=call):
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token(), send_greeting=False)
            replay = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token(), send_greeting=False)
        self.assertTrue(result["ok"])
        self.assertEqual(result["contactAssignment"], {"status": "ready"})
        self.assertTrue(replay["ok"])
        self.assertEqual(contact["ASSIGNED_BY_ID"], self.manager_id)
        self.assertEqual(writes, ["crm.deal.update", "crm.contact.update"])

    def test_contact_read_failure_does_not_partially_claim_deal(self):
        remote = {**self.source_deal(), "CONTACT_ID": "7"}
        def call(method, params=None, timeout=None):
            if method == "crm.deal.get":
                return dict(remote)
            self.assertEqual(method, "crm.contact.get")
            raise TimeoutError()
        with self.common_claim_context(), patch.object(app, "bitrix_call", side_effect=call):
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token(), send_greeting=False)
        self.assertFalse(result["ok"])
        self.assertEqual(result["_httpStatus"], 503)
        self.assertIsNone(self.store.get_claim_operation(self.operation_key()))


if __name__ == "__main__":
    unittest.main()
