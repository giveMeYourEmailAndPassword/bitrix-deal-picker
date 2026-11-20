"""No-network scanner tests: durable windows, recovery, and shared writer intents."""
import copy
import json
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from crm_contact_owner_sync import CrmContactOwnerSync, iso, timestamp
from state_store import StateStore


class Contacts:
    def __init__(self):
        self.jobs = {}
        self.calls = []

    def reserve_crm(self, key, deal, manager, contact, previous, marker, *, snapshot_started_at=None):
        values = (deal, manager, contact, previous, marker)
        self.calls.append((key, values, snapshot_started_at))
        if key in self.jobs and self.jobs[key] != values:
            raise ValueError("crm_contact_intent_conflict")
        self.jobs.setdefault(key, values)
        return len(self.jobs)


class CrmContactScannerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        self.store = StateStore(self.directory)
        self.start = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)
        self.clock = self.start
        self.deals, self.bindings, self.owners = {}, {}, {}
        self.calls, self.page_calls = [], []
        self.fail_get, self.fail_contacts = set(), set()
        self.bad_page = None
        self.page_failure = False
        self.contacts = Contacts()
        self.patches = [
            patch("crm_contact_owner_sync.eligible_deal", side_effect=self.eligible),
            patch("crm_contact_owner_sync.contact_ids", side_effect=lambda call, d: list(self.bindings[d["ID"]])),
            patch("crm_contact_owner_sync.latest_eligible_deal", side_effect=self.latest),
        ]
        for stub in self.patches:
            stub.start()
            self.addCleanup(stub.stop)
        self.scanner = self.create()

    def create(self):
        return CrmContactOwnerSync(self.store, self.call, self.contacts, "UF_MARKER", now=lambda: self.clock)

    @staticmethod
    def eligible(d):
        return (d.get("CLOSED") == "N" and d.get("STAGE_SEMANTIC_ID") == "P"
                and d.get("STAGE_ID", "").split(":")[-1] not in {"UC_ZJ55BR", "UC_PUCAAQ"})

    def latest(self, call, cid):
        candidates = [d for d in self.deals.values() if self.eligible(d) and cid in self.bindings[d["ID"]]]
        return copy.deepcopy(max(candidates, key=lambda d: (timestamp(d["DATE_CREATE"]), int(d["ID"])))) if candidates else None

    def add(self, did="100", *, cid="7", manager="42", modified=1, created=None, stage="NEW", closed="N", semantic="P"):
        did, cid = str(did), str(cid)
        self.deals[did] = {"ID": did, "ASSIGNED_BY_ID": str(manager), "CONTACT_ID": cid,
                           "DATE_MODIFY": (self.start + timedelta(seconds=modified)).isoformat(),
                           "DATE_CREATE": (self.start + timedelta(seconds=modified if created is None else created)).isoformat(),
                           "STAGE_ID": stage, "CLOSED": closed, "STAGE_SEMANTIC_ID": semantic,
                           "UF_MARKER": "marker-" + did}
        self.bindings[did] = [cid]
        self.owners.setdefault(cid, "9")
        return self.deals[did]

    def advance(self, seconds=10):
        self.clock += timedelta(seconds=seconds)

    def call(self, method, params, timeout=None):
        self.assertFalse(method.endswith(".update"), "scanner must never write CRM")
        self.calls.append((method, copy.deepcopy(params)))
        if method == "crm.deal.list":
            self.page_calls.append(copy.deepcopy(params))
            if self.page_failure:
                raise TimeoutError("synthetic")
            if self.bad_page is not None:
                return copy.deepcopy(self.bad_page)
            lower = timestamp(params["filter[>=DATE_MODIFY]"])
            upper = timestamp(params["filter[<=DATE_MODIFY]"])
            after = int(params["filter[>ID]"])
            rows = [d for d in self.deals.values() if lower <= timestamp(d["DATE_MODIFY"]) <= upper and int(d["ID"]) > after]
            return copy.deepcopy(sorted(rows, key=lambda d: int(d["ID"]))[:50])
        if method == "crm.deal.get":
            did = str(params["id"])
            if did in self.fail_get:
                raise TimeoutError("synthetic")
            return copy.deepcopy(self.deals[did])
        if method == "crm.contact.get":
            cid = str(params["id"])
            if cid in self.fail_contacts:
                raise TimeoutError("synthetic")
            return {"ID": cid, "ASSIGNED_BY_ID": self.owners[cid]}
        self.fail("Unexpected method: " + method)

    def observations(self):
        with self.store._connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM crm_contact_owner_observations ORDER BY CAST(deal_id AS INTEGER)")]

    def test_activation_ignores_old_records_and_new_active_deal_queues_shared_writer(self):
        self.add("90", modified=-86400)
        self.add()
        self.advance()
        result = self.scanner.run()
        self.assertEqual(result["observed"], 1)
        self.assertEqual(result["queued"], 1)
        self.assertEqual(len(self.contacts.jobs), 1)
        key, values, started = self.contacts.calls[0]
        self.assertTrue(key.startswith("crm:"))
        self.assertEqual(values, ("100", "42", "7", "9", "marker-100"))
        self.assertEqual(started, self.clock.isoformat())
        self.assertEqual(timestamp(self.page_calls[0]["filter[>=DATE_MODIFY]"]), self.start.timestamp())

    def test_owner_change_even_in_same_second_is_a_new_observation(self):
        deal = self.add()
        self.advance()
        self.scanner.run()
        self.owners["7"] = "42"
        deal["ASSIGNED_BY_ID"] = "43"
        self.advance()
        self.scanner.run()
        self.assertEqual(len(self.contacts.jobs), 2)
        self.assertEqual(self.contacts.calls[-1][1][1:4], ("43", "7", "42"))
        self.assertEqual(self.observations()[0]["revision"], 2)

    def test_both_excluded_stages_and_closed_deal_create_no_contact_intents(self):
        self.add("100", stage="UC_ZJ55BR")
        self.add("101", stage="C15:UC_PUCAAQ", cid="8")
        self.add("102", closed="Y", semantic="F", cid="9")
        self.advance()
        self.scanner.run()
        self.assertEqual(self.contacts.jobs, {})
        self.assertEqual(self.scanner.status()["pending"], 0)

    def test_transition_from_excluded_into_work_is_observed_without_owner_change(self):
        deal = self.add(stage="UC_PUCAAQ")
        self.advance()
        self.scanner.run()
        self.assertEqual(self.contacts.jobs, {})
        deal["STAGE_ID"] = "NEW"
        deal["DATE_MODIFY"] = (self.start + timedelta(seconds=11)).isoformat()
        self.advance()
        self.scanner.run()
        self.assertEqual(len(self.contacts.jobs), 1)

    def test_newest_active_deal_wins_even_when_older_deal_triggered_poll(self):
        self.add("100", created=-200, manager="42")
        self.add("99", created=-100, modified=-90, manager="43")
        self.advance()
        self.scanner.run()
        self.assertEqual(next(iter(self.contacts.jobs.values())), ("99", "43", "7", "9", "marker-99"))

    def test_creation_date_tie_uses_larger_id_from_policy(self):
        self.add("100", created=-100, manager="42")
        self.add("101", created=-100, modified=-90, manager="43")
        self.advance()
        self.scanner.run()
        self.assertEqual(next(iter(self.contacts.jobs.values()))[0:2], ("101", "43"))

    def test_every_actual_contact_binding_gets_its_own_idempotent_intent(self):
        self.add()
        self.bindings["100"] = ["7", "8"]
        self.owners["8"] = "10"
        self.advance()
        self.scanner.run()
        self.assertEqual({value[2] for value in self.contacts.jobs.values()}, {"7", "8"})

    def test_already_matching_contact_never_creates_a_job(self):
        self.add()
        self.owners["7"] = "42"
        self.advance()
        self.scanner.run()
        self.assertEqual(self.contacts.jobs, {})
        self.assertEqual(self.scanner.status()["pending"], 0)

    def test_overlapping_windows_do_not_repeat_observation_or_generation(self):
        self.add()
        self.advance()
        self.scanner.run()
        self.advance()
        self.scanner.run()
        self.assertEqual(len(self.contacts.calls), 1)
        self.assertEqual(self.observations()[0]["revision"], 1)
        self.assertEqual(self.page_calls[-1]["filter[>=DATE_MODIFY]"], self.start.isoformat())

    def test_restart_resumes_exact_fixed_window_keyset_before_new_window(self):
        for did in range(1, 52):
            self.add(did, cid=did)
        self.advance()
        first = self.scanner.poll()
        self.assertEqual(first["read"], 50)
        cursor = json.loads(self.scanner.meta("cursor"))
        self.assertEqual(cursor["after"], 50)
        self.assertFalse(cursor["complete"])
        self.advance(3600)
        self.store = StateStore(self.directory)
        restarted = self.create()
        self.assertEqual(restarted.poll()["read"], 1)
        self.assertEqual(self.page_calls[-1]["filter[>ID]"], 50)
        self.assertEqual(timestamp(self.page_calls[-1]["filter[<=DATE_MODIFY]"]), cursor["to"])
        self.assertEqual(len(self.observations()), 51)
        self.assertEqual(restarted.status()["activatedAt"], self.start.isoformat())

    def test_restart_catches_modifications_during_downtime_without_skipping_window(self):
        self.advance()
        self.scanner.run()
        self.add(modified=600)
        self.advance(3600)
        self.store = StateStore(self.directory)
        restarted = self.create()
        restarted.run()
        self.assertEqual(len(self.contacts.jobs), 1)
        self.assertLessEqual(timestamp(self.page_calls[-1]["filter[>=DATE_MODIFY]"]), (self.start + timedelta(seconds=600)).timestamp())

    def test_network_page_failure_preserves_cursor_and_existing_work_still_processes(self):
        self.add()
        self.advance()
        self.scanner.poll()
        self.advance()
        self.page_failure = True
        result = self.scanner.run()
        cursor = json.loads(self.scanner.meta("cursor"))
        self.assertEqual(cursor["after"], 0)
        self.assertFalse(cursor["complete"])
        self.assertEqual(result["errors"], 1)
        self.assertEqual(len(self.contacts.jobs), 1)
        self.page_failure = False
        self.scanner.poll()
        self.assertEqual(self.observations()[0]["revision"], 1)

    def test_invalid_filter_page_is_rejected_whole_without_cursor_advance(self):
        valid = self.add()
        invalid = self.add("101", modified=-100)
        self.bad_page = [valid, invalid]
        self.advance()
        with self.assertRaisesRegex(ValueError, "filter"):
            self.scanner.poll()
        self.assertEqual(self.observations(), [])
        self.assertEqual(json.loads(self.scanner.meta("cursor"))["after"], 0)

    def test_queue_and_cursor_commit_together_when_local_storage_fails(self):
        self.add()
        self.advance()
        original = self.store._transaction

        class BrokenInsert:
            def __init__(self, db):
                self.db = db

            def execute(self, sql, params=()):
                if "INSERT INTO crm_contact_owner_observations" in sql:
                    raise RuntimeError("synthetic local storage failure")
                return self.db.execute(sql, params)

        @contextmanager
        def transaction(*, immediate=False):
            with original(immediate=immediate) as db:
                yield BrokenInsert(db)

        with patch.object(self.store, "_transaction", transaction):
            with self.assertRaises(RuntimeError):
                self.scanner.poll()
        self.assertEqual(self.observations(), [])
        self.assertEqual(json.loads(self.scanner.meta("cursor"))["after"], 0)
        self.assertEqual(self.scanner.poll()["queued"], 1)

    def test_one_failing_source_does_not_starve_other_deals_and_retries_after_restart(self):
        self.add()
        self.add("101", cid="8")
        self.fail_get.add("100")
        self.advance()
        self.scanner.run()
        self.assertEqual(len(self.contacts.jobs), 1)
        self.assertEqual(next(iter(self.contacts.jobs.values()))[0], "101")
        self.assertEqual(self.scanner.status()["retrying"], 1)
        self.fail_get.clear()
        self.advance(16)
        self.store = StateStore(self.directory)
        self.create().run()
        self.assertEqual(len(self.contacts.jobs), 2)
        self.assertEqual(self.scanner.status()["pending"], 0)

    def test_partial_contact_failure_replays_exact_intent_without_new_generation(self):
        self.add()
        self.bindings["100"] = ["7", "8"]
        self.owners["8"] = "10"
        self.fail_contacts.add("8")
        self.advance()
        self.scanner.run()
        self.assertEqual(len(self.contacts.jobs), 1)
        self.fail_contacts.clear()
        self.advance(16)
        self.scanner.run()
        self.assertEqual(len(self.contacts.jobs), 2)
        self.assertEqual(self.contacts.calls[0][0], self.contacts.calls[1][0])

    def test_replayed_partial_observation_cannot_reauthorize_manual_contact_change(self):
        self.add()
        self.bindings["100"] = ["7", "8"]
        self.owners["8"] = "10"
        self.fail_contacts.add("8")
        self.advance()
        self.scanner.run()
        self.owners["7"] = "999"
        self.fail_contacts.clear()
        self.advance(16)
        self.scanner.run()
        first = [values for values in self.contacts.jobs.values() if values[2] == "7"]
        self.assertEqual(first, [("100", "42", "7", "9", "marker-100")])
        self.assertEqual(self.scanner.status()["pending"], 0)

    def test_closed_after_observation_is_ignored_at_processing(self):
        deal = self.add()
        self.advance()
        self.scanner.poll()
        deal.update(CLOSED="Y", STAGE_SEMANTIC_ID="F")
        self.scanner.process()
        self.assertEqual(self.contacts.jobs, {})
        self.assertEqual(self.scanner.status()["pending"], 0)

    def test_bulk_edit_of_closed_deals_does_not_delay_a_fresh_manager_change(self):
        # 20.11.2026: about 1700 old deals were closed in bulk and a manager
        # change made minutes later waited for hours behind them.
        for did in range(1, 61):
            self.add(did, cid=1000 + did, closed="Y", semantic="F", stage="LOSE")
        self.add("100")
        self.advance()
        result = self.scanner.run()
        self.assertEqual(result["queued"], 1)
        self.assertEqual(next(iter(self.contacts.jobs.values())), ("100", "42", "7", "9", "marker-100"))
        self.assertEqual(self.scanner.status()["pending"], 0)

    def test_closed_and_intake_rows_are_settled_without_reading_each_deal(self):
        self.add("100", stage="UC_ZJ55BR")
        self.add("101", stage="C15:UC_PUCAAQ", cid="8")
        self.add("102", closed="Y", semantic="F", cid="9")
        self.add("103", semantic="S", cid="10")
        self.advance()
        self.assertEqual(self.scanner.poll(), {"read": 4, "queued": 0})
        self.assertEqual(self.scanner.status()["pending"], 0)
        self.assertIsNone(self.scanner.process())
        self.assertNotIn("crm.deal.get", [method for method, _ in self.calls])
        self.assertEqual({row["error_code"] for row in self.observations()}, {"ineligible"})

    def test_row_without_explicit_proof_still_gets_the_full_deal_read(self):
        self.add("100", closed="", semantic="")
        self.advance()
        self.assertEqual(self.scanner.poll()["queued"], 1)
        self.scanner.process()
        self.assertIn(("crm.deal.get", {"id": "100"}), self.calls)
        self.assertEqual(self.contacts.jobs, {})
        self.assertEqual(self.scanner.status()["pending"], 0)

    def test_pending_change_is_dropped_when_the_deal_closes_before_processing(self):
        deal = self.add()
        self.advance()
        self.assertEqual(self.scanner.poll()["queued"], 1)
        deal.update(CLOSED="Y", STAGE_SEMANTIC_ID="F",
                    DATE_MODIFY=(self.start + timedelta(seconds=11)).isoformat())
        self.advance()
        self.assertEqual(self.scanner.poll()["queued"], 0)
        self.assertEqual(self.scanner.status()["pending"], 0)
        self.assertIsNone(self.scanner.process())
        self.assertEqual(self.contacts.jobs, {})

    def test_deal_settled_as_closed_is_picked_up_again_when_it_returns_to_work(self):
        deal = self.add(closed="Y", semantic="F")
        self.advance()
        self.scanner.run()
        self.assertEqual(self.contacts.jobs, {})
        deal.update(CLOSED="N", STAGE_SEMANTIC_ID="P",
                    DATE_MODIFY=(self.start + timedelta(seconds=11)).isoformat())
        self.advance()
        self.scanner.run()
        self.assertEqual(len(self.contacts.jobs), 1)

    def test_scanner_rpc_refuses_any_crm_mutation(self):
        with self.assertRaisesRegex(ValueError, "write_forbidden"):
            self.scanner.rpc("crm.contact.update", {"id": "7"})
        self.assertEqual(self.calls, [])

    def test_real_policy_and_shared_writer_apply_newest_owner_to_all_bindings(self):
        # Exercise the actual three modules together, with only the HTTP
        # transport replaced; the scanner itself still cannot call update.
        for stub in self.patches:
            stub.stop()
        from contact_owner_sync import ContactOwnerSync
        from contact_owner_policy import ITEM_FIELDS
        writes = []

        def transport(method, params, timeout=None):
            if method == "crm.deal.contact.items.get":
                return [{"CONTACT_ID": cid} for cid in self.bindings[str(params["id"])]]
            if method == "crm.item.list":
                cid = str(params["filter[contactIds]"])
                candidates = [d for d in self.deals.values() if cid in self.bindings[d["ID"]]
                              and d["CLOSED"] == "N" and d["STAGE_SEMANTIC_ID"] == "P"]
                candidates.sort(key=lambda d: (timestamp(d["DATE_CREATE"]), int(d["ID"])), reverse=True)
                start = int(params["start"])
                return {"items": [{**{src: d[dst] for src, dst in ITEM_FIELDS.items() if dst in d},
                                   "contactIds": self.bindings[d["ID"]]}
                                  for d in candidates[start:start + 50]]}
            if method == "crm.contact.update":
                self.assertEqual(set(params), {"id", "fields[ASSIGNED_BY_ID]"})
                writes.append(dict(params))
                self.owners[str(params["id"])] = params["fields[ASSIGNED_BY_ID]"]
                return True
            return self.call(method, params, timeout)

        source = self.add("100", created=-200, manager="42")
        self.add("99", created=-100, modified=-90, manager="43")
        self.bindings["100"] = ["7", "8"]
        self.owners["8"] = "10"
        contacts = ContactOwnerSync(self.store, transport, "UF_MARKER", enforce_active_policy=True)
        scanner = CrmContactOwnerSync(self.store, transport, contacts, "UF_MARKER", now=lambda: self.clock)
        self.advance()
        self.assertEqual(scanner.run()["queued"], 2)
        self.assertEqual(writes, [], "observation alone never writes CRM")
        contacts.process()
        contacts.process()
        self.assertEqual(self.owners, {"7": "43", "8": "42"})
        self.assertEqual(len(writes), 2)
        source.update(ASSIGNED_BY_ID="44", DATE_MODIFY=(self.start + timedelta(seconds=11)).isoformat())
        self.advance()
        scanner.run()
        contacts.process()
        self.assertEqual(self.owners, {"7": "43", "8": "44"})
        self.assertEqual(len(writes), 3)


if __name__ == "__main__":
    unittest.main()
