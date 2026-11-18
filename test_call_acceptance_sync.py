"""Synthetic telephony statistics with real SQLite and claim-lock fences."""
import copy
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from call_acceptance_sync import CallAcceptanceSync, call_deal_unresolved
from claim_locks import ClaimLocks
from native_acceptance_sync import iso
from state_store import StateStore

LINE = "reg000001"
BOT, ANSWERER, OTHER = "900", "7", "8"


class CallAcceptanceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="call-acceptance-")
        self.addCleanup(self.temp.cleanup)
        self.store = StateStore(Path(self.temp.name))
        self.now = datetime(2026, 11, 18, 17, tzinfo=timezone.utc).timestamp()
        self.origin = self.now
        # One old finished call exists before the feature is enabled.
        self.records = [self.record(40, start=self.origin - 600)]
        self.deal = {"ID": "101", "ASSIGNED_BY_ID": BOT, "CONTACT_ID": "10", "CREATED_BY_ID": BOT,
                     "DATE_MODIFY": iso(self.origin - 3600), "STAGE_SEMANTIC_ID": "P", "CLOSED": "N",
                     "STAGE_ID": "UC_PUCAAQ", "SOURCE_ID": "94", "LEAD_ID": None, "UF_CRM_CLAIM": ""}
        self.items = [{"id": 101, "stageId": "UC_PUCAAQ", "contactIds": [10]}]
        self.user = {"ID": ANSWERER, "ACTIVE": True, "UF_DEPARTMENT": [1]}
        self.calls, self.writes, self.confirmed = [], [], []
        self.before_call = self.update = self.guard_error = None
        self.occupied, self.bound = False, [10]
        self.locks = ClaimLocks()
        self.service = self.make_service()
        self.service.poll()  # establishes the baseline at record 40
        self.now += 60

    def record(self, statistic_id, *, start=None, **changes):
        row = {"ID": str(statistic_id), "CALL_ID": f"externalCall.{statistic_id}", "CALL_TYPE": "2",
               "CALL_FAILED_CODE": "200", "PORTAL_NUMBER": LINE, "PORTAL_USER_ID": ANSWERER,
               "CALL_START_DATE": iso(self.origin + 5 if start is None else start),
               "CALL_DURATION": "20", "CRM_ENTITY_TYPE": "CONTACT", "CRM_ENTITY_ID": "10"}
        row.update(changes)
        return row

    def make_service(self):
        return CallAcceptanceSync(
            self.store, self.call, self.locks, "UF_CRM_CLAIM", lines=[LINE],
            queue_stages=("UC_ZJ55BR", "UC_PUCAAQ"), target_stage="NEW",
            chat_guard=self.guard, now=lambda: self.now, after_confirmed=self.confirmed.append)

    def guard(self, deal_id, manager_id, call):
        self.assertEqual((deal_id, manager_id), ("101", ANSWERER))
        if self.guard_error:
            raise self.guard_error
        return self.occupied

    def call(self, method, params, timeout=8):
        self.calls.append((method, copy.deepcopy(params)))
        if self.before_call:
            self.before_call(method, params)
        if method == "voximplant.statistic.get":
            rows = sorted(self.records, key=lambda row: int(row["ID"]))
            if "FILTER[ID]" in params:
                return [copy.deepcopy(row) for row in rows if row["ID"] == str(params["FILTER[ID]"])]
            if params.get("ORDER") == "DESC":
                return [copy.deepcopy(row) for row in reversed(rows)][:50]
            self.assertEqual((params["FILTER[CALL_TYPE]"], params["SORT"], params["ORDER"]), ("2", "ID", "ASC"))
            return [copy.deepcopy(row) for row in rows if int(row["ID"]) > int(params["FILTER[>ID]"])][:50]
        if method == "crm.item.list":
            self.assertEqual((params["filter[contactIds]"], params["filter[closed]"]), ("10", "N"))
            return {"items": copy.deepcopy(self.items)}
        if method == "user.get":
            return [copy.deepcopy(self.user)]
        if method == "crm.deal.get":
            return copy.deepcopy(self.deal)
        if method == "crm.deal.contact.items.get":
            return [{"CONTACT_ID": value} for value in self.bound]
        if method == "im.user.get":
            return {"id": int(params["ID"]), "bot": str(params["ID"]) == BOT}
        if method == "crm.deal.update":
            self.writes.append(copy.deepcopy(params))
            self.assertEqual(self.jobs()[-1]["dispatched"], 1, "intent is durable before transport")
            if self.update:
                return self.update(params)
            self.apply(params)
            return True
        self.fail("unexpected endpoint: " + method)

    def apply(self, params):
        self.deal["STAGE_ID"] = params["fields[STAGE_ID]"]
        self.deal["ASSIGNED_BY_ID"] = params.get("fields[ASSIGNED_BY_ID]", self.deal["ASSIGNED_BY_ID"])
        self.deal["DATE_MODIFY"] = iso(self.now)

    def jobs(self):
        with self.store._connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM call_acceptance_jobs ORDER BY CAST(statistic_id AS INTEGER)")]

    def answered(self, statistic_id=41, **changes):
        self.records.append(self.record(statistic_id, **changes))
        self.service.poll()

    def canonical(self, *, status="succeeded", at=None):
        with self.store._transaction(immediate=True) as db:
            db.execute("INSERT INTO claim_operations(operation_key,deal_id,manager_id,status,request_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                       ("canonical-1", "101", "44", status, "{}", iso(at or self.now), iso(at or self.now)))

    def assert_job(self, status, code=""):
        job = self.jobs()[-1]
        self.assertEqual((job["status"], job["error_code"]), (status, code))
        return job

    def test_answered_call_gives_queued_deal_to_answerer_and_moves_it_to_work(self):
        self.answered()
        self.service.process()
        self.assertEqual(self.writes, [{"id": "101", "fields[STAGE_ID]": "NEW", "fields[ASSIGNED_BY_ID]": ANSWERER}])
        self.assert_job("ready")
        self.assertEqual(self.confirmed, ["101"])
        self.assertFalse(call_deal_unresolved(self.store, "101"))
        # A restart and further polls never replay the same call.
        self.service = self.make_service()
        self.now += 100
        self.service.run()
        self.assertEqual((len(self.writes), len(self.jobs())), (1, 1))
        self.assertFalse(any(method == "crm.contact.update" for method, _ in self.calls))

    def test_new_caller_deal_already_owned_by_answerer_only_changes_stage(self):
        self.deal.update(ASSIGNED_BY_ID=ANSWERER, CREATED_BY_ID="30", SOURCE_ID="CALL", LEAD_ID="500",
                         STAGE_ID="UC_ZJ55BR")
        self.items[0]["stageId"] = "UC_ZJ55BR"
        self.answered()
        self.service.process()
        self.assertEqual(self.writes, [{"id": "101", "fields[STAGE_ID]": "NEW"}])
        self.assert_job("ready")

    def test_calls_finished_before_activation_are_never_replayed(self):
        self.assertEqual(self.service.meta("cursor"), "40")
        # A late statistic row of a call that ended before activation.
        self.answered(41, start=self.origin - 300)
        self.service.process()
        self.assertEqual((self.jobs(), self.writes), ([], []))
        self.assertEqual(self.service.meta("cursor"), "41")
        self.service = self.make_service()
        self.assertEqual(float(self.service.meta("activated_at")), self.origin)

    def test_missed_foreign_line_and_unbound_calls_authorize_nothing(self):
        self.records += [
            self.record(41, CALL_FAILED_CODE="304", PORTAL_USER_ID="0"),
            self.record(42, PORTAL_NUMBER="reg999"),
            self.record(43, PORTAL_USER_ID="0"),
            self.record(44, CRM_ENTITY_TYPE="LEAD"),
            self.record(45, CRM_ENTITY_TYPE=None, CRM_ENTITY_ID=None),
            self.record(46, CALL_START_DATE="yesterday"),
        ]
        self.service.poll()
        # None of them is a job, and none of them blocks later calls.
        self.assertEqual((self.jobs(), self.service.meta("cursor")), ([], "46"))
        self.answered(47)
        self.assertEqual([job["statistic_id"] for job in self.jobs()], ["47"])

    def test_page_violating_the_inbound_filter_fails_closed_without_moving_cursor(self):
        self.records += [self.record(41), self.record(42, CALL_TYPE="1")]
        with self.assertRaises(ValueError):
            self.service.poll()
        self.assertEqual((self.jobs(), self.service.meta("cursor")), ([], "40"))

    def test_failed_first_read_is_not_an_empty_baseline(self):
        store = StateStore(Path(tempfile.mkdtemp(dir=self.temp.name)))
        self.assertFalse(call_deal_unresolved(store, "101"))
        service = CallAcceptanceSync(store, self.failing, self.locks, "UF_CRM_CLAIM", lines=[LINE],
                                     queue_stages=("UC_ZJ55BR",), target_stage="NEW", chat_guard=self.guard,
                                     now=lambda: self.now)
        service.run()
        self.assertIsNone(service.meta("cursor"))

    def failing(self, method, params, timeout=8):
        raise TimeoutError()

    def test_client_with_a_deal_in_work_keeps_its_manager(self):
        self.items.append({"id": 55, "stageId": "EXECUTING", "contactIds": [10]})
        self.answered()
        self.service.process()
        self.assert_job("skipped", "call_contact_has_working_deal")
        self.assertEqual(self.writes, [])

    def test_no_queued_deal_is_quiet_and_two_queued_deals_are_never_guessed(self):
        self.items = []
        self.answered(41)
        self.service.process()
        self.assert_job("skipped", "call_no_queued_deal")
        self.items = [{"id": 101, "stageId": "UC_PUCAAQ", "contactIds": [10]},
                      {"id": 102, "stageId": "UC_ZJ55BR", "contactIds": [10]}]
        self.answered(42)
        self.service.process()
        self.assert_job("conflict", "call_deal_ambiguous")
        self.assertEqual(self.writes, [])

    def test_incomplete_open_deal_list_never_authorizes(self):
        self.items = [{"id": 1000 + n, "stageId": "UC_PUCAAQ", "contactIds": [10]} for n in range(50)]
        self.answered()
        self.service.process()
        self.assert_job("pending", "call_sync_unavailable")
        self.assertEqual(self.writes, [])

    def test_owner_chosen_by_a_person_is_not_overwritten(self):
        self.deal["ASSIGNED_BY_ID"] = OTHER
        self.answered()
        self.service.process()
        self.assert_job("conflict", "call_deal_owned_by_employee")
        self.assertEqual(self.writes, [])

    def test_placeholder_owner_of_a_missed_call_is_replaced_on_callback(self):
        self.deal.update(ASSIGNED_BY_ID="30", CREATED_BY_ID="30", SOURCE_ID="CALL", LEAD_ID="500")
        self.answered()
        self.service.process()
        self.assertEqual(self.writes, [{"id": "101", "fields[STAGE_ID]": "NEW", "fields[ASSIGNED_BY_ID]": ANSWERER}])
        self.assert_job("ready")

    def test_placeholder_rule_needs_telephony_origin_and_no_earlier_picker_claim(self):
        self.deal.update(ASSIGNED_BY_ID="30", CREATED_BY_ID="30", SOURCE_ID="CALL", LEAD_ID=None)
        self.answered(41)
        self.service.process()
        self.assert_job("conflict", "call_deal_owned_by_employee")
        self.deal["LEAD_ID"] = "500"
        self.canonical(at=self.origin - 60)
        self.answered(42)
        self.service.process()
        self.assert_job("conflict", "call_deal_owned_by_employee")
        self.assertEqual(self.writes, [])

    def test_dialog_accepted_by_another_employee_blocks_and_unknown_guard_retries(self):
        self.guard_error = RuntimeError("guard")
        self.answered()
        self.service.process()
        self.assert_job("pending", "call_sync_unavailable")
        self.guard_error, self.occupied = None, True
        self.now += 31
        self.service.process()
        self.assert_job("conflict", "call_dialog_owned_by_employee")
        self.assertEqual(self.writes, [])

    def test_inactive_employee_and_changed_call_record_are_terminal(self):
        self.user["ACTIVE"] = False
        self.answered(41)
        self.service.process()
        self.assert_job("conflict", "call_employee_inactive")
        self.user["ACTIVE"] = True
        self.answered(42)
        self.records[-1]["PORTAL_USER_ID"] = OTHER
        self.service.process()
        self.assert_job("conflict", "call_record_changed")
        self.assertEqual(self.writes, [])

    def test_deal_taken_meanwhile_is_left_alone_and_manual_result_sends_nothing(self):
        self.answered(41)
        self.deal.update(STAGE_ID="NEW", ASSIGNED_BY_ID=OTHER)
        self.service.process()
        self.assert_job("skipped", "call_deal_left_queue")
        self.deal.update(STAGE_ID="UC_PUCAAQ", ASSIGNED_BY_ID=BOT)
        self.answered(42)
        self.deal.update(STAGE_ID="NEW", ASSIGNED_BY_ID=ANSWERER)
        self.service.process()
        self.assert_job("ready")
        self.assertEqual((self.writes, self.confirmed), ([], ["101"]))
        self.assertFalse(call_deal_unresolved(self.store, "101"))

    def test_pending_picker_claim_blocks_the_call(self):
        self.answered()
        self.canonical(status="pending")
        self.service.process()
        self.assert_job("conflict", "picker_claim_pending")
        self.assertEqual(self.writes, [])

    def test_picker_attempt_appearing_during_proof_supersedes_the_call(self):
        self.answered()

        def attempt(method, params):
            if method == "user.get":
                self.before_call = None
                self.canonical(at=self.origin - 60)
        self.before_call = attempt
        self.service.process()
        self.assert_job("conflict", "call_acceptance_superseded")
        self.assertEqual(self.writes, [])

    def test_picker_claim_completed_after_the_call_wins_even_if_seen_first(self):
        self.records.append(self.record(41))
        self.canonical(at=self.now)  # the call ended earlier, the claim is newer
        self.service.poll()
        self.service.process()
        self.assert_job("conflict", "call_newer_picker_claim")
        self.assertEqual(self.writes, [])

    def test_crm_change_during_preparation_is_not_overwritten(self):
        self.answered()
        reads = []

        def change(method, params):
            if method == "crm.deal.get":
                reads.append(1)
                if len(reads) == 2:
                    self.deal["DATE_MODIFY"] = iso(self.now + 1)
        self.before_call = change
        self.service.process()
        self.assert_job("conflict", "call_deal_changed_before_dispatch")
        self.assertEqual(self.writes, [])

    def test_unknown_write_is_read_back_after_restart_never_sent_again(self):
        def lost(params):
            self.apply(params)
            raise TimeoutError()
        self.update = lost
        self.answered()
        self.service.process()
        self.assert_job("ready")
        self.assertEqual(len(self.writes), 1)

        self.deal.update(STAGE_ID="UC_PUCAAQ", ASSIGNED_BY_ID=BOT)
        self.update = lambda params: (_ for _ in ()).throw(TimeoutError())
        self.answered(42)
        self.service.process()
        job = self.assert_job("pending", "call_deal_update_uncertain")
        self.assertEqual((job["dispatched"], job["acknowledged"]), (1, 0))
        self.assertTrue(call_deal_unresolved(self.store, "101"))
        # The delayed request lands; a new process only reads.
        self.apply({"fields[STAGE_ID]": "NEW", "fields[ASSIGNED_BY_ID]": ANSWERER})
        self.service = self.make_service()
        self.now += 31
        self.service.process()
        self.assert_job("ready")
        self.assertEqual(len(self.writes), 2)
        self.assertFalse(call_deal_unresolved(self.store, "101"))

    def test_definite_rejection_releases_the_fence(self):
        def denied(params):
            error = RuntimeError("denied")
            error.bitrix_error_code = "ACCESS_DENIED"
            raise error
        self.update = denied
        self.answered()
        self.service.process()
        job = self.assert_job("conflict", "call_deal_update_rejected")
        self.assertEqual(job["dispatched"], 0)
        self.assertFalse(call_deal_unresolved(self.store, "101"))
        self.assertEqual(self.deal["STAGE_ID"], "UC_PUCAAQ")

    def test_uncertain_dispatch_blocks_a_second_call_for_the_same_deal(self):
        self.update = lambda params: (_ for _ in ()).throw(TimeoutError())
        self.answered(41)
        self.service.process()
        self.update = None
        self.answered(42, PORTAL_USER_ID=OTHER)
        self.user["ID"] = OTHER
        self.guard = lambda deal_id, manager_id, call: False
        self.service = self.make_service()
        self.now += 31
        self.service.drain()
        first, second = self.jobs()
        self.assertEqual((first["status"], first["dispatched"]), ("pending", 1))
        self.assertEqual((second["status"], second["error_code"]), ("pending", "call_previous_update_uncertain"))
        self.assertEqual(len(self.writes), 1)

    def test_stale_call_is_closed_without_a_write(self):
        self.answered()
        self.now += 1000
        self.service.process()
        self.assert_job("conflict", "call_acceptance_expired")
        self.assertEqual(self.writes, [])

    def test_unreadable_binding_is_retried_and_changed_binding_is_terminal(self):
        self.answered()

        def unavailable(method, params):
            if method == "crm.deal.contact.items.get":
                raise ValueError("contact_owner_bindings_unavailable")
        self.before_call = unavailable
        self.service.process()
        self.assert_job("pending", "call_sync_unavailable")
        self.before_call, self.bound = None, [11]
        self.now += 31
        self.service.process()
        self.assert_job("conflict", "call_deal_binding_changed")
        self.assertEqual(self.writes, [])

    def test_explicit_lines_and_distinct_stages_are_required(self):
        for options in ({"lines": []}, {"lines": "reg1"}, {"lines": ["reg 1"]}, {"lines": [LINE, LINE]},
                        {"queue_stages": ()}, {"target_stage": "UC_PUCAAQ"}):
            settings = {"lines": [LINE], "queue_stages": ("UC_PUCAAQ",), "target_stage": "NEW", **options}
            with self.assertRaises(ValueError):
                CallAcceptanceSync(self.store, self.call, self.locks, "UF_CRM_CLAIM",
                                   chat_guard=self.guard, **settings)


if __name__ == "__main__":
    unittest.main()
