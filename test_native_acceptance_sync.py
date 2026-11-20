"""Synthetic Open Lines transitions and real SQLite/claim-lock fences."""
import copy
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from claim_locks import ClaimLocks
from native_acceptance_sync import NativeAcceptanceSync, iso, native_deal_unresolved
from state_store import StateStore


class Contacts:
    def __init__(self):
        self.reservations, self.confirmations, self.cancellations, self.conflicts = [], [], [], []
        self.cancelled_operations = []

    def reserve_native(self, *args, **kwargs):
        self.reservations.append((args, kwargs))
        return len(self.reservations)

    def confirm_native(self, generation):
        self.confirmations.append(generation)

    def cancel_native(self, generation):
        self.cancellations.append(generation)

    def conflict_native(self, generation):
        self.conflicts.append(generation)

    def cancel_native_operation(self, operation_key):
        self.cancelled_operations.append(operation_key)


class NativeAcceptanceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="native-acceptance-")
        self.addCleanup(self.temp.cleanup)
        self.store = StateStore(Path(self.temp.name))
        self.now = datetime(2026, 10, 12, 10, tzinfo=timezone.utc).timestamp()
        self.origin = self.now
        self.source = "wz_whatsapp_cf47040c2d7948c6b261851eedc08348f"
        self.session = {"id": 900, "chatId": 123, "configId": 33, "source": self.source,
                        "dateCreate": iso(self.now + 1), "operatorId": 7, "status": "paused",
                        "crmEntityType": "deal", "crmEntityId": 101}
        self.deal = {"ID": "101", "ASSIGNED_BY_ID": "8", "CONTACT_ID": "10",
                     "DATE_MODIFY": iso(self.now - 86400), "STAGE_SEMANTIC_ID": "P", "CLOSED": "N",
                     "STAGE_ID": "NEW", "UF_CRM_CLAIM": ""}
        self.contact = {"ID": "10", "ASSIGNED_BY_ID": "9"}
        self.user = {"ID": "7", "ACTIVE": True, "UF_DEPARTMENT": [1], "USER_TYPE": "employee"}
        self.calls, self.writes = [], []
        self.before_call = None
        self.update = None
        self.list_response = None
        self.binding_extra = ""
        self.contacts, self.locks = Contacts(), ClaimLocks()
        self.service = self.make_service()
        self.now += 10

    def make_service(self):
        return NativeAcceptanceSync(self.store, self.call, self.locks, self.contacts,
                                    "UF_CRM_CLAIM", now=lambda: self.now)

    def call(self, method, params, timeout=8):
        self.calls.append((method, copy.deepcopy(params)))
        if self.before_call:
            self.before_call(method, params)
        if method == "imopenlines.v2.Session.list":
            if self.list_response:
                return self.list_response(params)
            rows = [copy.deepcopy(self.session)]
            if params.get("status") and self.session["status"] != params["status"]:
                rows = []
            return {"sessions": rows, "hasNextPage": False}
        if method == "imopenlines.dialog.get":
            return {"id": self.session["chatId"], "owner": self.session["operatorId"],
                    "entity_type": "LINES", "entity_id": f'{self.source}|{self.session["configId"]}|external',
                    "entity_data_1": f'Y|DEAL|101|N|N|{self.session["id"]}',
                    "entity_data_2": "CONTACT|10|DEAL|101" + self.binding_extra}
        if method == "imopenlines.crm.chat.get":
            return [{"CHAT_ID": 123, "CONNECTOR_ID": self.source}]
        if method == "user.get":
            return [copy.deepcopy(self.user)]
        if method == "crm.deal.get":
            return copy.deepcopy(self.deal)
        if method == "crm.contact.get":
            return copy.deepcopy(self.contact)
        if method == "crm.deal.update":
            self.writes.append(copy.deepcopy(params))
            self.assertTrue(self.contacts.reservations, "contact generation precedes remote mutation")
            with self.store._connect() as db:
                self.assertEqual(db.execute("SELECT dispatched FROM native_acceptance_jobs").fetchone()[0], 1)
            if self.update:
                return self.update()
            for field in ("ASSIGNED_BY_ID", "STAGE_ID"):
                if f"fields[{field}]" in params:
                    self.deal[field] = params[f"fields[{field}]"]
            self.deal["DATE_MODIFY"] = iso(self.now)
            return True
        self.fail("unexpected endpoint: " + method)

    def jobs(self):
        with self.store._connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM native_acceptance_jobs ORDER BY revision")]

    def observe(self, **changes):
        self.session.update(changes)
        self.service.observe(copy.deepcopy(self.session))

    def canonical(self, *, pending=False, at=None):
        recorded = iso(self.now if at is None else at)
        with self.store._transaction(immediate=True) as db:
            db.execute("INSERT INTO claim_operations(operation_key,deal_id,manager_id,status,request_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                       ("canonical-1", "101", "44", "pending" if pending else "succeeded", '{}', recorded, recorded))

    def historical_legacy_claim(self):
        self.store.append_claim({"timestamp": iso(self.origin - 3600), "dealId": "101", "managerId": "8"})

    def assert_picker_owner_protected(self, *, dispatched):
        job, = self.jobs()
        self.assertEqual(job["status"], "conflict")
        self.assertEqual(job["error_code"], "native_picker_owner_protected")
        self.assertEqual(job["dispatched"], dispatched)
        self.assertEqual(self.contacts.confirmations, [])

    def test_new_paused_acceptance_updates_only_owner_and_confirms_contact_once(self):
        self.observe()
        self.service.process()
        self.assertEqual(self.writes, [{"id": "101", "fields[ASSIGNED_BY_ID]": "7"}])
        self.assertEqual(self.deal["STAGE_ID"], "NEW")
        self.assertEqual(self.contacts.confirmations, [1])
        self.assertEqual(self.jobs()[0]["status"], "ready")
        self.assertEqual(self.contacts.reservations[0][1]["observed_at"], self.session["dateCreate"])
        self.service = self.make_service()
        self.now += 100
        self.observe()
        self.service.process()
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(len(self.jobs()), 1)
        self.assertFalse(any("history" in method for method, _ in self.calls))

    def test_old_accepted_first_snapshot_is_baseline_after_restart_too(self):
        self.observe(dateCreate=iso(self.origin - 86400))
        self.assertEqual(self.jobs(), [])
        self.now += 100
        self.service = self.make_service()
        self.observe(operatorId=11)
        self.assertEqual(self.jobs(), [])
        self.assertEqual(float(self.service.meta("activated_at")), self.origin)

    def test_accepting_an_unprocessed_deal_assigns_employee_and_sets_work_stage_in_one_update(self):
        self.deal.update(CATEGORY_ID="0", STAGE_ID="UC_ZJ55BR")
        self.observe(status="answered", dateOperatorAnswer=iso(self.now))
        self.service.process()
        self.assertEqual(self.writes, [
            {"id": "101", "fields[ASSIGNED_BY_ID]": "7", "fields[STAGE_ID]": "NEW"},
        ])
        self.assertEqual(self.jobs()[0]["status"], "ready")
        self.assertEqual(self.contacts.confirmations, [1])

    def test_accepting_a_waiting_deal_already_assigned_to_employee_sets_work_stage(self):
        self.deal.update(CATEGORY_ID=0, STAGE_ID="UC_PUCAAQ", ASSIGNED_BY_ID="7")
        self.observe(status="answered", dateOperatorAnswer=iso(self.now))
        self.service.process()
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.writes[0]["fields[STAGE_ID]"], "NEW")
        self.assertEqual(self.deal["ASSIGNED_BY_ID"], "7")
        self.assertEqual(self.jobs()[0]["status"], "ready")
        self.assertEqual(self.contacts.confirmations, [1])

    def test_acceptance_preserves_later_stage_and_foreign_pipeline(self):
        for category, stage in (("0", "PREPARATION"), ("8", "C8:UC_PUCAAQ"), ("8", "UC_PUCAAQ")):
            with self.subTest(category=category, stage=stage):
                self.setUp_case_for_loop()
                self.deal.update(CATEGORY_ID=category, STAGE_ID=stage, ASSIGNED_BY_ID="8",
                                 DATE_MODIFY=iso(self.origin))
                self.writes.clear()
                self.observe(status="answered", dateOperatorAnswer=iso(self.now))
                self.service.process()
                self.assertEqual(self.writes, [{"id": "101", "fields[ASSIGNED_BY_ID]": "7"}])
                self.assertEqual(self.deal["STAGE_ID"], stage)
                self.assertEqual(self.jobs()[0]["status"], "ready")

    def test_stage_advance_during_contact_reservation_is_preserved_even_in_same_second(self):
        self.deal.update(CATEGORY_ID="0", STAGE_ID="UC_PUCAAQ")

        def manual_advance(method, _params):
            if method == "crm.contact.get":
                self.deal["STAGE_ID"] = "PREPARATION"

        self.before_call = manual_advance
        self.observe(status="answered", dateOperatorAnswer=iso(self.now))
        self.service.process()
        self.assertEqual(self.writes, [])
        self.assertEqual(self.deal["STAGE_ID"], "PREPARATION")
        self.assertEqual(self.jobs()[0]["status"], "conflict")
        self.assertEqual(self.contacts.confirmations, [])

    def test_stage_update_timeout_with_matching_owner_needs_stage_confirmation_after_restart(self):
        self.deal.update(CATEGORY_ID="0", STAGE_ID="UC_PUCAAQ", ASSIGNED_BY_ID="7")

        def timeout():
            raise TimeoutError("synthetic timeout while setting stage")

        self.update = timeout
        self.observe(status="answered", dateOperatorAnswer=iso(self.now))
        self.service.process()
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.writes[0]["fields[STAGE_ID]"], "NEW")
        self.assertEqual(self.jobs()[0]["status"], "pending")
        self.assertEqual(self.contacts.confirmations, [])

        self.now += 100
        self.service = self.make_service()
        self.service.process()
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.jobs()[0]["status"], "pending")
        self.assertEqual(self.contacts.confirmations, [])

        self.deal["STAGE_ID"] = "NEW"
        self.now += 100
        self.service.process()
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.jobs()[0]["status"], "ready")
        self.assertEqual(self.contacts.confirmations, [1])

    def test_native_owner_already_updated_after_answer_but_intake_stage_predates_answer(self):
        answered_at = self.origin + 3
        self.deal.update(CATEGORY_ID="0", STAGE_ID="UC_PUCAAQ", ASSIGNED_BY_ID="7",
                         DATE_MODIFY=iso(answered_at + 4), MOVED_TIME=iso(answered_at - 10))
        self.observe(status="answered", dateOperatorAnswer=iso(answered_at))
        self.service.process()
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.writes[0]["fields[STAGE_ID]"], "NEW")
        self.assertEqual(self.jobs()[0]["status"], "ready")

    def test_stage_moved_back_to_queue_after_answer_is_preserved(self):
        answered_at = self.origin + 3
        self.deal.update(CATEGORY_ID="0", STAGE_ID="UC_PUCAAQ", ASSIGNED_BY_ID="7",
                         DATE_MODIFY=iso(answered_at + 4), MOVED_TIME=iso(answered_at + 4))
        self.observe(status="answered", dateOperatorAnswer=iso(answered_at))
        self.service.process()
        self.assertEqual(self.writes, [])
        self.assertEqual(self.jobs()[0]["status"], "conflict")
        self.assertEqual(self.contacts.confirmations, [])

    def test_stage_change_before_final_confirmation_never_confirms_or_replays_assignment(self):
        self.deal.update(CATEGORY_ID="0", STAGE_ID="UC_PUCAAQ")
        reads_after_write = 0

        def advance_after_initial_readback(method, _params):
            nonlocal reads_after_write
            if method == "crm.deal.get" and self.writes:
                reads_after_write += 1
                if reads_after_write == 2:
                    self.deal["STAGE_ID"] = "PREPARATION"

        self.before_call = advance_after_initial_readback
        self.observe(status="answered", dateOperatorAnswer=iso(self.now))
        self.service.process()
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.deal["STAGE_ID"], "PREPARATION")
        self.assertEqual(self.jobs()[0]["status"], "conflict")
        self.assertEqual(self.contacts.confirmations, [])
        self.now += 100
        self.service.process()
        self.assertEqual(len(self.writes), 1)

    def test_existing_queued_session_can_be_accepted_after_baseline(self):
        self.observe(dateCreate=iso(self.origin - 86400), status="new", operatorId=7)
        self.assertEqual(self.jobs(), [])
        self.observe(status="answered")
        self.service.process()
        self.assertEqual(len(self.writes), 1)

    def test_cloud_whatsapp_and_instagram_promote_once_on_acceptance(self):
        for line, source in ((103, "thoth_waba"), (45, "fbinstagramdirect")):
            with self.subTest(line=line):
                self.source = source
                self.setUp_case_for_loop()
                self.writes.clear()
                self.deal.update(CATEGORY_ID="0", STAGE_ID="UC_PUCAAQ", ASSIGNED_BY_ID="7",
                                 DATE_MODIFY=iso(self.now), MOVED_TIME=iso(self.origin - 60))
                self.observe(source=self.source, configId=line, status="new", operatorId=7)
                self.assertFalse(self.service.process())
                self.observe(status="answered", dateOperatorAnswer=iso(self.now))
                self.assertTrue(self.service.process())
                self.assertEqual(self.jobs()[0]["status"], "ready")
                self.assertEqual(self.writes, [{"id": "101", "fields[STAGE_ID]": "NEW"}])
                self.observe(status="paused")
                self.assertFalse(self.service.process())

    def test_cloud_whatsapp_skips_other_lines_and_preexisting_acceptances(self):
        self.source = "thoth_waba"
        with self.store._transaction(immediate=True) as db:
            db.execute("UPDATE native_acceptance_meta SET value=? WHERE key='activated_at'",
                       (str(self.origin - 86400),))
        self.observe(source=self.source, configId=7, status="answered")
        self.observe(source=self.source, configId=103, status="answered",
                     dateCreate=iso(self.origin - 60), dateOperatorAnswer=iso(self.origin - 30))
        self.service = self.make_service()
        self.observe()
        self.assertEqual(self.jobs(), [])
        self.assertFalse(self.service.process())

    def test_cloud_whatsapp_acceptance_after_rollout_of_older_queued_deal(self):
        self.source = "thoth_waba"
        self.deal.update(CATEGORY_ID="0", STAGE_ID="UC_PUCAAQ")
        self.observe(source=self.source, configId=103, status="answered",
                     dateCreate=iso(self.origin - 60), dateOperatorAnswer=iso(self.now - 1))
        self.assertTrue(self.service.process())
        self.assertEqual(self.deal["STAGE_ID"], "NEW")

    def test_intake_stage_can_change_between_queue_observation_and_acceptance(self):
        self.observe(status="new")
        self.now += 20
        self.deal.update(CATEGORY_ID="0", STAGE_ID="UC_PUCAAQ",
                         MOVED_TIME=iso(self.now), DATE_MODIFY=iso(self.now))
        self.now += 20
        self.observe(status="answered", dateOperatorAnswer=iso(self.now))
        self.assertTrue(self.service.process())
        self.assertEqual(self.deal["STAGE_ID"], "NEW")
        self.assertEqual(self.deal["ASSIGNED_BY_ID"], "7")

    def test_intake_promotion_requires_employee_profile(self):
        self.user["USER_TYPE"] = "bot"
        self.deal.update(CATEGORY_ID="0", STAGE_ID="UC_PUCAAQ")
        self.observe(status="answered")
        self.service.process()
        self.assertEqual(self.writes, [])
        self.assertEqual(self.jobs()[0]["error_code"], "native_employee_unconfirmed")

    def test_upgrade_keeps_pending_owner_update_from_the_previous_version(self):
        self.observe(status="answered")
        with self.store._transaction(immediate=True) as db:
            db.execute("DELETE FROM native_acceptance_meta WHERE key='stage_activated_at'")
            for column in ("promote_stage", "promotion_from_stage", "promotion_moved_time"):
                db.execute(f"ALTER TABLE native_acceptance_jobs DROP COLUMN {column}")
        self.deal.update(CATEGORY_ID="0", STAGE_ID="UC_PUCAAQ")
        self.service = self.make_service()
        self.assertTrue(self.service.process())
        self.assertEqual(self.jobs()[0]["status"], "ready")
        self.assertEqual(self.writes, [{"id": "101", "fields[ASSIGNED_BY_ID]": "7"}])

    def test_stage_activation_keeps_earlier_instagram_acceptance_owner_only(self):
        self.source = "fbinstagramdirect"
        with self.store._transaction(immediate=True) as db:
            db.execute("UPDATE native_acceptance_meta SET value=? WHERE key='activated_at'",
                       (str(self.origin - 86400),))
        self.deal.update(CATEGORY_ID="0", STAGE_ID="UC_PUCAAQ")
        self.observe(source=self.source, configId=45, status="answered",
                     dateCreate=iso(self.origin - 60), dateOperatorAnswer=iso(self.origin - 30))
        self.assertTrue(self.service.process())
        self.assertEqual(self.jobs()[0]["status"], "ready")
        self.assertEqual(self.writes, [{"id": "101", "fields[ASSIGNED_BY_ID]": "7"}])

    def test_offered_operator_is_not_acceptance(self):
        self.observe(status="new", operatorId=7)
        self.service.process()
        self.assertEqual(self.jobs(), [])
        self.assertEqual(self.writes, [])

    def test_accepted_owner_change_requires_observed_queue_transition(self):
        self.observe(dateCreate=iso(self.origin - 86400), operatorId=8)
        self.observe(operatorId=7)
        self.assertEqual(self.jobs(), [])
        self.observe(status="new")
        self.observe(status="paused")
        self.assertEqual(len(self.jobs()), 1)

    def test_ambiguous_crm_binding_is_terminal_without_writes(self):
        self.observe()
        self.binding_extra = "|DEAL|102"
        self.service.process()
        self.assertEqual(self.writes, [])
        self.assertEqual(self.jobs()[0]["error_code"], "native_dialog_binding_changed")

    def test_inactive_employee_and_closed_deal_are_preserved(self):
        for invalid in ("inactive", "closed", "archived", "won", "unavailable_semantics"):
            with self.subTest(invalid=invalid):
                self.setUp_case_for_loop()
                if invalid == "inactive":
                    self.user["ACTIVE"] = False
                elif invalid == "closed":
                    self.deal["CLOSED"] = "Y"
                elif invalid == "archived":
                    self.deal["IS_ARCHIVED"] = "Y"
                else:
                    self.deal["STAGE_SEMANTIC_ID"] = "S" if invalid == "won" else None
                self.observe()
                self.service.process()
                self.assertEqual(self.writes, [])
                self.assertEqual(self.jobs()[0]["status"], "conflict")

    def setUp_case_for_loop(self):
        with self.store._transaction(immediate=True) as db:
            db.execute("DELETE FROM native_acceptance_jobs")
            db.execute("DELETE FROM native_acceptance_sessions")
        self.user["ACTIVE"] = True
        self.deal.update(CLOSED="N", IS_ARCHIVED="N", STAGE_SEMANTIC_ID="P")

    def test_newer_canonical_claim_supersedes_native_intent(self):
        self.observe()
        self.canonical()
        self.service.process()
        self.assertEqual(self.jobs()[0]["error_code"], "native_acceptance_superseded")
        self.assertEqual(self.writes, [])

    def test_old_successful_picker_claim_protects_a_third_live_owner(self):
        # Picker originally assigned 44. The current CRM owner is 8; Open Lines reports 7.
        self.canonical(at=self.origin - 3600)
        self.observe()
        self.service.process()
        self.assert_picker_owner_protected(dispatched=0)
        self.assertEqual(self.deal["ASSIGNED_BY_ID"], "8")
        self.assertEqual(self.writes, [])
        self.assertEqual(self.contacts.reservations, [])

    def test_legacy_picker_event_without_operation_key_protects_current_owner(self):
        self.historical_legacy_claim()
        with self.store._connect() as db:
            self.assertIsNone(db.execute("SELECT operation_key FROM claim_events").fetchone()[0])
        self.observe()
        self.service.process()
        self.assert_picker_owner_protected(dispatched=0)
        self.assertEqual(self.writes, [])
        self.assertEqual(self.contacts.reservations, [])

    def test_native_acceptance_confirms_an_already_matching_picker_deal_owner(self):
        self.historical_legacy_claim()
        self.deal["ASSIGNED_BY_ID"] = "7"
        self.observe()
        self.service.process()
        self.assertEqual(self.jobs()[0]["status"], "ready")
        self.assertEqual(self.writes, [])
        self.assertEqual(self.contacts.confirmations, [1])

    def test_picker_event_after_contact_reservation_releases_it_without_dispatch(self):
        reserve = self.contacts.reserve_native

        def reserve_then_record(*args, **kwargs):
            generation = reserve(*args, **kwargs)
            self.historical_legacy_claim()
            return generation

        self.contacts.reserve_native = reserve_then_record
        self.observe()
        self.service.process()
        self.assert_picker_owner_protected(dispatched=0)
        self.assertEqual(self.writes, [])
        self.assertEqual(self.contacts.cancellations, [1])

    def test_picker_event_at_dispatch_releases_reservation_and_sends_nothing(self):
        mark_dispatched = self.service.mark_dispatched

        def record_then_dispatch(job):
            self.assertEqual(job["contact_generation"], 1)
            self.historical_legacy_claim()
            return mark_dispatched(job)

        self.service.mark_dispatched = record_then_dispatch
        self.observe()
        self.service.process()
        self.assert_picker_owner_protected(dispatched=0)
        self.assertEqual(self.writes, [])
        self.assertEqual(self.contacts.cancellations, [1])

    def test_restart_reconciles_an_already_sent_native_write_without_confirming_contact(self):
        def timeout():
            raise TimeoutError("synthetic unknown outcome")

        self.update = timeout
        self.observe()
        self.service.process()
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.jobs()[0]["error_code"], "native_deal_update_uncertain")
        self.historical_legacy_claim()
        self.deal["ASSIGNED_BY_ID"] = "7"
        self.now += 100
        self.service = self.make_service()
        self.service.process()
        self.assert_picker_owner_protected(dispatched=1)
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.contacts.conflicts, [1])
        self.assertFalse(native_deal_unresolved(self.store, "101"))

    def test_unresolved_canonical_attempt_blocks_native_even_when_observed_first(self):
        self.canonical(pending=True)
        self.observe()
        self.service.process()
        self.assertEqual(self.jobs()[0]["error_code"], "picker_claim_pending")
        self.assertEqual(self.writes, [])

    def test_crm_change_during_preparation_is_not_overwritten(self):
        self.observe()
        def change(method, _params):
            if method == "crm.contact.get":
                self.deal["ASSIGNED_BY_ID"] = "44"
        self.before_call = change
        self.service.process()
        self.assertEqual(self.jobs()[0]["error_code"], "native_deal_changed_before_dispatch")
        self.assertEqual(self.writes, [])
        self.assertEqual(self.contacts.cancellations, [1])

    def test_unknown_write_is_read_back_after_restart_never_sent_again(self):
        self.observe()
        def timeout():
            raise TimeoutError("synthetic timeout")
        self.update = timeout
        self.service.process()
        self.assertEqual(self.jobs()[0]["error_code"], "native_deal_update_uncertain")
        self.assertEqual(len(self.writes), 1)
        self.service = self.make_service()
        self.now += 100
        self.service.process()
        self.assertEqual(len(self.writes), 1)
        self.deal["ASSIGNED_BY_ID"] = "7"
        self.now += 100
        self.service.process()
        self.assertEqual(self.jobs()[0]["status"], "ready")
        self.assertEqual(len(self.writes), 1)

    def test_shared_claim_lock_waits_and_rechecks_canonical_fence(self):
        self.observe()
        started, done = threading.Event(), threading.Event()
        def work():
            started.set()
            self.service.process()
            done.set()
        with self.locks.hold("44", "101"):
            thread = threading.Thread(target=work)
            thread.start()
            self.assertTrue(started.wait(1))
            self.assertFalse(done.wait(.05))
            self.canonical()
        thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertEqual(self.writes, [])
        self.assertEqual(self.jobs()[0]["error_code"], "native_acceptance_superseded")

    def test_parallel_processors_do_not_replay_completed_job(self):
        self.observe()
        threads = [threading.Thread(target=self.service.process) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(3)
            self.assertFalse(thread.is_alive())
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.jobs()[0]["status"], "ready")

    def test_poll_failure_does_not_advance_baseline_or_cursor(self):
        before = self.service.meta("cursor")
        self.list_response = lambda _params: {"error": "permission"}
        with self.assertRaises(ValueError):
            self.service.poll()
        self.assertEqual(self.service.meta("cursor"), before)
        self.assertEqual(self.jobs(), [])

    def test_discovery_is_bounded_and_recheck_can_find_old_queue_acceptance(self):
        self.observe(dateCreate=iso(self.origin - 86400), status="new")
        self.now += 40
        self.session["status"] = "paused"
        self.service.recheck()
        self.assertEqual(len(self.jobs()), 1)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0][1]["limit"], 200)
        self.assertLess(datetime.fromisoformat(self.calls[0][1]["dateCreateTo"]).timestamp() - datetime.fromisoformat(self.calls[0][1]["dateCreateFrom"]).timestamp(), 3)

    def test_accepted_session_change_after_dispatch_leaves_ordering_tombstone(self):
        self.observe()
        def update_and_transfer():
            self.deal["ASSIGNED_BY_ID"] = "7"
            self.session["operatorId"] = 44
            return True
        self.update = update_and_transfer
        self.service.process()
        self.assertEqual(self.jobs()[0]["status"], "conflict")
        self.assertEqual(self.contacts.conflicts, [1])
        self.assertEqual(self.contacts.cancellations, [])

    def test_definite_rejection_releases_fence_and_cancels_unapplied_contact(self):
        self.observe()
        self.update = lambda: False
        self.service.process()
        self.assertEqual(self.jobs()[0]["error_code"], "native_deal_update_rejected")
        self.assertFalse(native_deal_unresolved(self.store, "101"))
        self.assertEqual(self.contacts.cancellations, [1])
        self.now += 100
        self.service.process()
        self.assertEqual(len(self.writes), 1)

    def test_known_access_denied_is_not_an_uncertain_dispatch(self):
        self.observe()
        def reject():
            error = RuntimeError("synthetic")
            error.bitrix_error_code = "ACCESS_DENIED"
            raise error
        self.update = reject
        self.service.process()
        self.assertEqual(self.jobs()[0]["error_code"], "native_deal_update_rejected")
        self.assertFalse(native_deal_unresolved(self.store, "101"))

    def test_uncertain_dispatch_blocks_newer_native_session_on_same_deal(self):
        self.observe()
        def timeout():
            raise TimeoutError("synthetic")
        self.update = timeout
        self.service.process()
        self.assertTrue(native_deal_unresolved(self.store, "101"))
        self.now += 1
        self.observe(id=901, dateCreate=iso(self.now), operatorId=44)
        self.assertFalse(self.service.process())
        self.assertEqual(len(self.writes), 1)
        self.now += 100
        self.service.process()
        self.assertEqual(len(self.writes), 1)
        self.assertTrue(native_deal_unresolved(self.store, "101"))

    def test_reverse_discovery_does_not_make_older_session_supersede_current(self):
        self.observe(id=901, dateCreate=iso(self.origin + 2))
        self.observe(id=900, dateCreate=iso(self.origin + 1))
        self.session.update(id=901, dateCreate=iso(self.origin + 2))
        self.service.process()
        self.service.process()
        self.assertEqual(len(self.writes), 1)
        self.assertEqual([j["status"] for j in self.jobs()].count("ready"), 1)

    def test_already_current_owner_does_not_create_false_inflight_fence(self):
        self.deal["ASSIGNED_BY_ID"] = "7"
        self.observe()
        reads = 0
        def change_on_readback(method, _params):
            nonlocal reads
            if method == "crm.deal.get":
                reads += 1
                if reads == 3:
                    self.deal["ASSIGNED_BY_ID"] = "44"
        self.before_call = change_on_readback
        self.service.process()
        self.assertEqual(self.writes, [])
        self.assertEqual(self.jobs()[0]["error_code"], "native_deal_owner_changed")
        self.assertFalse(native_deal_unresolved(self.store, "101"))

    def test_crashed_contact_reservation_conflict_is_terminal_and_released(self):
        self.observe()
        def conflicting_reservation(*_args, **_kwargs):
            raise ValueError("native_contact_intent_conflict")
        self.contacts.reserve_native = conflicting_reservation
        self.service.process()
        self.assertEqual(self.writes, [])
        self.assertEqual(self.jobs()[0]["status"], "conflict")
        self.assertEqual(self.jobs()[0]["error_code"], "native_contact_intent_conflict")
        self.assertEqual(self.contacts.cancelled_operations, [self.jobs()[0]["operation_key"]])

    def test_canonical_completed_before_first_poll_still_beats_older_acceptance(self):
        self.session["dateOperatorAnswer"] = iso(self.origin + 3)
        self.canonical()
        self.now += 5
        self.observe()
        self.service.process()
        self.assertEqual(self.writes, [])
        self.assertEqual(self.jobs()[0]["observed_at"], self.origin + 3)
        self.assertEqual(self.jobs()[0]["error_code"], "native_newer_picker_claim")

    def test_queue_event_bound_survives_delayed_binding_and_defers_to_canonical(self):
        self.observe(dateCreate=iso(self.origin - 86400), status="new")
        bound = self.now
        self.now += 2
        self.observe(status="answered", crmEntityId=None)
        self.now += 2
        self.canonical()
        self.now += 2
        self.observe(crmEntityId=101)
        self.service.process()
        self.assertEqual(self.writes, [])
        self.assertEqual(self.jobs()[0]["observed_at"], bound)
        self.assertEqual(self.jobs()[0]["error_code"], "native_newer_picker_claim")


if __name__ == "__main__":
    unittest.main()
