"""Synthetic native API and real SQLite; never sends Bitrix requests."""
import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from claim_locks import ClaimLocks
from contact_owner_policy import ITEM_FIELDS
from lost_session_restart import Decision, LostSessionRestart, iso, stamp, source_witness
from state_store import StateStore


class LostSessionRestartTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="lost-session-restart-")
        self.addCleanup(self.temp.cleanup)
        self.store = StateStore(Path(self.temp.name))
        self.now = 1794996000.5
        self.origin = self.now
        self.config = {"ID": "103", "ACTIVE": "Y", "CRM_FORWARD": "N", "QUEUE": [41]}
        self.old = {"id": 900, "chatId": 123, "configId": 103, "source": "thoth_waba",
                    "dateCreate": iso(self.now - 100), "operatorId": 7, "status": "answered",
                    "crmEntityType": "deal", "crmEntityId": 101, "userId": 500000,
                    "dateOperatorAnswer": iso(self.now - 80), "dateClose": None}
        self.sessions, self.current = [self.old], self.old
        self.deals = {"101": {"ID": "101", "ASSIGNED_BY_ID": "7", "CONTACT_ID": "10",
                      "CATEGORY_ID": "0", "DATE_CREATE": iso(self.now - 1000), "DATE_MODIFY": iso(self.now - 60),
                      "SOURCE_ID": "103|OPENLINE",
                      "STAGE_ID": "LOSE", "STAGE_SEMANTIC_ID": "F", "CLOSED": "Y",
                      "MOVED_TIME": iso(self.now - 60)}}
        self.bindings = {"101": ["10"]}
        self.transition = {"dealId": "101", "transitionId": "70", "fromSemantic": "P",
                           "toSemantic": "F", "transitionTime": iso(self.now - 60)}
        self.messages = {"1": self.message("1", "7", self.now - 80),
                         "2": self.message("2", "500000", self.now + 2)}
        self.users = {"500000": {"id": "500000", "connector": True, "bot": False},
                      "7": {"id": "7", "connector": False, "bot": False},
                      "262867": {"id": "262867", "connector": False, "bot": True}}
        self.calls, self.writes = [], []
        self.add_writes, self.queue_writes = [], []
        self.owner, self.managers = 7, [7]
        self.dialog_contact = '10'
        self.add_hook, self.queue_hook = None, None
        self.hook, self.restart_hook, self.history_hook = None, None, None
        self.proofs = 0
        self.service = self.make_service()
        self.now += 40

    @staticmethod
    def message(mid, sender, when):
        return {"id": mid, "chatid": "123", "senderid": sender, "date": iso(when),
                "text": "synthetic", "textlegacy": "synthetic", "params": {"connectorMid": ['external-' + mid]}}

    def make_service(self):
        return LostSessionRestart(self.store, self.call, ClaimLocks(),
            allowed_lines={"103": ["thoth_waba"]}, prove_failed=self.prove,
            bot_id="262867", bot_client_id="synthetic-client-secret",
            now=lambda: self.now)

    def prove(self, did):
        self.assertEqual(did, "101")
        self.proofs += 1
        return copy.deepcopy(self.transition)

    def call(self, method, params, timeout=8):
        self.calls.append((method, copy.deepcopy(params)))
        if self.hook:
            self.hook(method, params)
        if method == "imopenlines.config.get":
            return copy.deepcopy(self.config)
        if method == "imopenlines.v2.Session.list":
            rows = [copy.deepcopy(row) for row in self.sessions
                if str(row["configId"]) == params["configId"] and row["source"] == params["source"]
                and ("status" not in params or row["status"] == params["status"])
                and ("dateCreateFrom" not in params or stamp(row["dateCreate"]) >= stamp(params["dateCreateFrom"]))
                and stamp(row["dateCreate"]) <= stamp(params["dateCreateTo"])]
            rows.sort(key=lambda row: stamp(row["dateCreate"]))
            offset, limit = params["offset"], params["limit"]
            return {"sessions": rows[offset:offset + limit], "hasNextPage": offset + limit < len(rows)}
        if method == "crm.deal.list":
            if "filter[@ID]" in params:
                return [copy.deepcopy(self.deals[did]) for did in params["filter[@ID]"] if did in self.deals]
            return [copy.deepcopy(deal) for deal in self.deals.values()
                    if deal.get("ORIGINATOR_ID") == params["filter[ORIGINATOR_ID]"]
                    and deal.get("ORIGIN_ID") == params["filter[ORIGIN_ID]"]]
        if method == "imopenlines.dialog.get":
            did, sid = str(self.current["crmEntityId"]), str(self.current["id"])
            kind = self.current["crmEntityType"].upper()
            lost = "101" if kind == "CONTACT" else did
            return {"id": 123, "type": "lines", "owner": self.owner,
                    "entity_id": "thoth_waba|103|external|address",
                    "entity_data_1": f"Y|{kind}|{did}|N|N|{sid}",
                    "entity_data_2": f"CONTACT|{self.dialog_contact}|DEAL|{lost}",
                    "last_message_id": max(self.messages, key=int), "manager_list": copy.deepcopy(self.managers)}
        if method == "crm.deal.get":
            return copy.deepcopy(self.deals[params["id"]])
        if method == "crm.deal.contact.items.get":
            return [{"CONTACT_ID": cid} for cid in self.bindings[params["id"]]]
        if method == "crm.item.list":
            rows = []
            for deal in self.deals.values():
                ids = self.bindings[deal["ID"]]
                if params["filter[contactIds]"] in ids and deal["CLOSED"] == "N" and deal["STAGE_SEMANTIC_ID"] == "P":
                    rows.append({**{key: deal[value] for key, value in ITEM_FIELDS.items() if value in deal}, "contactIds": ids})
            return {"items": rows[params["start"]:params["start"] + 50]}
        if method == "imopenlines.session.history.get":
            value = {"chatId": 123, "sessionId": int(params["SESSION_ID"]), "userId": "chat123",
                     "message": copy.deepcopy(self.messages), "users": copy.deepcopy(self.users),
                     "hasNextPage": False, "nextId": None, "nextDate": None}
            if params["SESSION_ID"] == "901":
                value["message"] = {mid: row for mid, row in value["message"].items() if int(mid) >= 2}
            return self.history_hook(params, value) if self.history_hook else value
        if method == "imopenlines.message.session.start":
            self.assertEqual(set(params), {"CHAT_ID", "MESSAGE_ID"})
            self.assertEqual(params, {"CHAT_ID": "123", "MESSAGE_ID": "2"})
            self.assertEqual(self.jobs()[0]["dispatched"], 1)
            self.assertEqual(self.jobs()[0]["state"], "dispatching")
            self.writes.append(copy.deepcopy(params))
            if self.restart_hook:
                return self.restart_hook()
            self.native_restart()
            return True
        if method == "crm.deal.add":
            job = self.jobs()[0]
            self.assertEqual(job["create_dispatched"], 1)
            self.assertEqual(job["reserved"], 1)
            self.add_writes.append(copy.deepcopy(params))
            if self.add_hook:
                return self.add_hook(params)
            return self.native_add(params)
        if method == "imopenlines.bot.session.transfer":
            self.assertEqual(set(params), {"CHAT_ID", "QUEUE_ID", "CLIENT_ID"})
            self.assertEqual(params["QUEUE_ID"], "103")
            self.assertEqual(self.jobs()[0]["queue_dispatched"], 1)
            self.assertNotIn("synthetic-client-secret", str(self.jobs()))
            self.queue_writes.append(copy.deepcopy(params))
            if self.queue_hook:
                return self.queue_hook()
            self.native_queue()
            return True
        self.fail("unexpected endpoint: " + method)

    def native_restart(self):
        self.old.update(status="closed", dateClose=iso(self.now))
        self.current = {**self.old, "id": 901, "crmEntityId": 102, "crmEntityType": "deal", "status": "answered",
                        "dateCreate": iso(self.now), "dateClose": None, "dateOperatorAnswer": None, "operatorId": 41}
        self.owner, self.managers = 41, [41]
        self.sessions.append(self.current)

    def native_add(self, params):
        fields = {key[len("fields["):key.index("]")]: copy.deepcopy(value) for key, value in params.items()}
        self.deals["102"] = {**self.deals["101"], **fields,
            "ID": "102", "CATEGORY_ID": "0", "STAGE_SEMANTIC_ID": "P", "CLOSED": "N",
            "DATE_CREATE": iso(self.now), "DATE_MODIFY": iso(self.now)}
        self.bindings["102"] = fields["CONTACT_IDS"]
        return "102"

    def native_queue(self):
        self.current.update(status="new", operatorId=None, dateOperatorAnswer=None)
        self.owner, self.managers = 0, []

    def jobs(self):
        with self.store._connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM lost_session_restart_watches")]

    def discover(self):
        self.service.poll()  # new lane
        self.service.poll()  # answered lane, including old session
        self.assertEqual(len(self.jobs()), 1)

    def run_one(self):
        self.discover()
        return self.service.process()

    def test_old_open_session_new_return_restarts_once_after_durable_intent(self):
        result = self.run_one()
        self.assertEqual(result["state"], "restarted")
        self.assertEqual(result["new_deal_id"], "102")
        self.assertEqual(len(self.writes), 1)
        self.assertGreaterEqual(self.proofs, 2)
        self.assertEqual(len(self.add_writes), 1)
        self.assertEqual(len(self.queue_writes), 1)
        self.now += 1000
        self.service = self.make_service()
        self.assertIsNone(self.service.process())
        self.assertEqual(len(self.writes), 1)
        scans = [p for method, p in self.calls if method == "imopenlines.v2.Session.list" and "status" in p]
        self.assertTrue(all("dateCreateFrom" not in p for p in scans))

    def test_activation_survives_service_restart_and_no_old_message_replay(self):
        self.messages["2"]["date"] = iso(self.origin)
        activation = self.service.meta("activated_at")
        self.now += 100
        self.service = self.make_service()
        self.assertEqual(self.service.meta("activated_at"), activation)
        self.assertEqual(self.run_one()["code"], "restart_no_new_customer_message")
        self.assertFalse(self.writes)

    def test_inherited_precreation_message_before_cutoff_does_not_block_fresh_return(self):
        self.messages["1"]["date"] = iso(self.origin - 200)
        self.assertLess(stamp(self.messages["1"]["date"]), stamp(self.old["dateCreate"]))
        self.assertEqual(self.run_one()["state"], "restarted")
        self.assertEqual(len(self.writes), 1)

    def test_same_input_cannot_restart_new_native_session_still_bound_to_lost_deal(self):
        def restart_still_lost():
            self.native_restart()
            self.current["crmEntityId"] = 101
            self.current["status"] = "new"
            return True
        self.restart_hook = restart_still_lost
        self.assertEqual(self.run_one()["state"], "uncertain")
        self.service.poll()  # paused lane
        self.now += 3
        self.service.poll()  # new lane discovers new session with old binding
        self.assertEqual(len(self.jobs()), 2)
        result = self.service.process()
        self.assertEqual(result["session_id"], "901")
        self.assertEqual(result["code"], "restart_message_already_dispatched")
        self.assertEqual(len(self.writes), 1)
        self.now += 1000
        self.service = self.make_service()
        self.service.process()  # old uncertain job can only read back
        self.assertEqual(len(self.writes), 1)
        # The database also protects competing processes which race between
        # their read and dispatch transaction: the input cannot have two owners.
        with self.assertRaises(sqlite3.IntegrityError):
            with self.store._transaction(immediate=True) as db:
                db.execute("UPDATE lost_session_restart_watches SET dispatched=1,message_id='2' WHERE session_id='901'")

    def copied_return(self, *, lost=False, wrong_key=False, wrong_body=False, duplicate=False):
        original = copy.deepcopy(self.messages['2'])
        self.native_restart()
        copied = {**original, 'id': '22', 'date': iso(self.now)}
        if wrong_key:
            copied['params']['connectorMid'] = ['different-external-origin']
        if wrong_body:
            copied['text'] = 'another authored body'
        self.messages = {'22': copied}
        if duplicate:
            self.messages['23'] = {**copy.deepcopy(copied), 'id': '23'}
        if lost:
            self.current.update(crmEntityId=101, status='new')
        return True

    def test_native_copy_with_new_id_and_date_uses_exact_connector_witness(self):
        self.restart_hook = self.copied_return
        result = self.run_one()
        self.assertEqual(result['state'], 'restarted')
        self.assertEqual(result['target_message_id'], '22')
        self.assertEqual(self.jobs()[0]['message_id'], '2')
        self.assertEqual(self.jobs()[0]['target_message_id'], '22')
        self.assertEqual(self.writes, [{'CHAT_ID': '123', 'MESSAGE_ID': '2'}])
        self.assertEqual(len(self.queue_writes), 1)

    def test_human_after_original_return_before_copy_date_prevents_queue(self):
        def copy_with_intervening_reply():
            self.copied_return()
            self.messages['21'] = self.message('21', '7', self.now - 1)
            return True
        self.restart_hook = copy_with_intervening_reply
        result = self.run_one()
        self.assertEqual(result['code'], 'restart_later_human_or_unknown_message')
        self.assertEqual(len(self.writes), 1)
        self.assertFalse(self.queue_writes)
        self.now += 100
        self.service = self.make_service()
        self.service.process()
        self.assertEqual(len(self.writes), 1)
        self.assertFalse(self.queue_writes)

    def test_human_at_original_return_timestamp_before_copy_is_ambiguous(self):
        original_time = stamp(self.messages['2']['date'])
        def copy_with_same_second_reply():
            self.copied_return()
            self.messages['21'] = self.message('21', '7', original_time)
            return True
        self.restart_hook = copy_with_same_second_reply
        self.assertEqual(self.run_one()['code'], 'restart_later_human_or_unknown_message')
        self.assertFalse(self.queue_writes)

    def test_old_human_before_original_return_does_not_block_native_copy(self):
        old_time = stamp(self.messages['2']['date']) - 1
        def copy_with_old_reply():
            self.copied_return()
            self.messages['21'] = self.message('21', '7', old_time)
            return True
        self.restart_hook = copy_with_old_reply
        self.assertEqual(self.run_one()['state'], 'restarted')
        self.assertEqual(len(self.queue_writes), 1)

    def test_native_copy_same_text_wrong_connector_key_never_queues(self):
        self.restart_hook = lambda: self.copied_return(wrong_key=True)
        self.assertEqual(self.run_one()['state'], 'uncertain')
        self.assertFalse(self.queue_writes)

    def test_native_copy_same_connector_but_changed_body_never_queues(self):
        self.restart_hook = lambda: self.copied_return(wrong_body=True)
        self.assertEqual(self.run_one()['state'], 'uncertain')
        self.assertFalse(self.queue_writes)

    def test_duplicate_connector_witness_in_new_history_is_ambiguous(self):
        self.restart_hook = lambda: self.copied_return(duplicate=True)
        self.assertEqual(self.run_one()['state'], 'uncertain')
        self.assertFalse(self.queue_writes)

    def test_copied_new_native_id_cannot_reuse_consumed_external_source(self):
        self.restart_hook = lambda: self.copied_return(lost=True)
        self.assertEqual(self.run_one()['state'], 'uncertain')
        original_key = self.jobs()[0]['source_identity_key']
        self.service.poll()  # paused
        self.now += 3
        self.service.poll()  # new; copied native input now has ID22
        result = self.service.process()
        self.assertEqual(result['session_id'], '901')
        self.assertEqual(result['code'], 'restart_message_already_dispatched')
        self.assertEqual(len(self.add_writes), 1)
        self.assertEqual(len(self.writes), 1)
        with self.assertRaises(sqlite3.IntegrityError):
            with self.store._transaction(immediate=True) as db:
                db.execute("""UPDATE lost_session_restart_watches SET reserved=1,message_id='22',
                    source_identity_key=? WHERE session_id='901'""", (original_key,))

    def test_connector_alias_must_be_present_and_agree(self):
        self.messages['2']['params']['CONNECTOR_MID'] = ['contradicting-origin']
        self.assertEqual(self.run_one()['state'], 'watching')
        self.assertFalse(self.add_writes)

    def test_consistent_uppercase_connector_alias_is_supported(self):
        self.messages['2']['params'] = {'CONNECTOR_MID': ['external-2']}
        self.restart_hook = self.copied_return
        self.assertEqual(self.run_one()['state'], 'restarted')

    def test_tick_returns_only_integer_counters_for_parent_logging(self):
        result = self.service.tick()
        self.assertTrue(result)
        self.assertTrue(all(type(value) is int for value in result.values()))
        self.assertEqual(result["processed"], 0)

    def test_waits_thirty_seconds_for_native_creator_before_add(self):
        self.now = self.origin + 20
        self.assertEqual(self.run_one()["code"], "restart_waiting_native_creator")
        self.assertFalse(self.add_writes)
        self.assertFalse(self.writes)
        self.assertEqual(self.jobs()[0]["reserved"], 0)

    def test_reuses_single_older_intake_with_old_contact_owner(self):
        self.deals["102"] = {**self.deals["101"], "ID": "102", "STAGE_ID": "UC_PUCAAQ",
                             "CLOSED": "N", "STAGE_SEMANTIC_ID": "P", "SOURCE_ID": "OTHER"}
        self.bindings["102"] = ["10"]
        self.assertEqual(self.run_one()["state"], "restarted")
        self.assertFalse(self.add_writes)
        self.assertEqual(self.deals["102"]["ASSIGNED_BY_ID"], "7")
        self.assertEqual(self.deals["101"]["CLOSED"], "Y")
        self.assertEqual(self.deals["101"]["ASSIGNED_BY_ID"], "7")

    def test_multiple_intakes_never_create_or_restart(self):
        for did in ("102", "103"):
            self.deals[did] = {**self.deals["101"], "ID": did, "STAGE_ID": "UC_ZJ55BR",
                               "CLOSED": "N", "STAGE_SEMANTIC_ID": "P"}
            self.bindings[did] = ["10"]
        self.assertEqual(self.run_one()["code"], "restart_multiple_intakes")
        self.assertFalse(self.add_writes)
        self.assertFalse(self.writes)

    def test_contact_bound_native_session_resolves_related_old_lost_deal(self):
        self.old.update(crmEntityType="contact", crmEntityId=10)
        self.assertEqual(self.run_one()["state"], "restarted")
        self.assertEqual(self.jobs()[0]["deal_id"], "101")
        self.assertEqual(self.jobs()[0]["crm_type"], "contact")

    def test_deal_bound_native_contact_must_be_an_actual_old_deal_contact(self):
        self.dialog_contact = '11'
        self.assertEqual(self.run_one()['code'], 'restart_native_contact_not_related_to_lost_deal')
        self.assertFalse(self.add_writes)
        self.assertFalse(self.writes)

    def test_safe_customer_burst_after_add_keeps_original_trigger_and_finishes(self):
        def add_with_customer_burst(params):
            result = self.native_add(params)
            self.messages['3'] = self.message('3', '500000', self.origin + 3)
            return result
        self.add_hook = add_with_customer_burst
        self.assertEqual(self.run_one()['state'], 'restarted')
        self.assertEqual(len(self.add_writes), 1)
        self.assertEqual(self.writes, [{'CHAT_ID': '123', 'MESSAGE_ID': '2'}])
        self.assertEqual(len(self.queue_writes), 1)

    def test_original_source_edit_after_add_blocks_restart(self):
        def add_then_edit(params):
            result = self.native_add(params)
            self.messages['2']['text'] = 'edited after reservation'
            return result
        self.add_hook = add_then_edit
        self.assertEqual(self.run_one()['code'], 'restart_source_changed_after_reservation')
        self.assertFalse(self.writes)
        self.assertFalse(self.queue_writes)

    def test_add_timeout_is_recovered_by_exact_origin_without_second_add(self):
        def uncertain_add(params):
            self.native_add(params)
            raise TimeoutError("synthetic")
        self.add_hook = uncertain_add
        self.assertEqual(self.run_one()["state"], "restarted")
        self.assertEqual(len(self.add_writes), 1)
        fields = self.add_writes[0]
        self.assertEqual(fields["fields[ASSIGNED_BY_ID]"], "41")
        self.assertEqual(fields["fields[CONTACT_IDS][]"], ["10"])
        self.assertLessEqual(len(fields["fields[ORIGIN_ID]"]), 128)
        self.assertTrue(any(method == "crm.deal.list" and "filter[ORIGIN_ID]" in params for method, params in self.calls))

    def test_unobserved_add_keeps_contact_fence_and_never_retries(self):
        def uncertain_add(_params):
            raise TimeoutError("synthetic")
        self.add_hook = uncertain_add
        self.assertEqual(self.run_one()["code"], "restart_add_result_uncertain")
        self.assertFalse(self.writes)
        self.now += 100
        self.service = self.make_service()
        self.assertEqual(self.service.process()["code"], "restart_add_result_uncertain")
        self.assertEqual(len(self.add_writes), 1)
        with self.store._connect() as db:
            self.assertEqual(db.execute("SELECT session_id FROM lost_session_restart_contact_fences WHERE contact_id='10'").fetchone()[0], "900")

    def test_committed_add_claimed_before_timeout_lookup_is_known_and_unfenced(self):
        def committed_then_claimed(params):
            self.native_add(params)
            self.deals['102'].update(ASSIGNED_BY_ID='9', STAGE_ID='NEW',
                                     DATE_MODIFY=iso(self.now + 1), UF_CRM_CLAIM='native-claim')
            raise TimeoutError('synthetic lost acknowledgement')
        self.add_hook = committed_then_claimed
        self.assertEqual(self.run_one()['code'], 'restart_created_deal_claimed')
        job = self.jobs()[0]
        self.assertEqual(job['state'], 'blocked')
        self.assertEqual(job['target_deal_id'], '102')
        self.assertEqual(json.loads(job['target_identity_json'])['ID'], '102')
        self.assertEqual(job['reserved'], 1)
        self.assertEqual(len(self.add_writes), 1)
        self.assertFalse(self.writes)
        self.assertFalse(self.queue_writes)
        with self.store._connect() as db:
            self.assertIsNone(db.execute("SELECT * FROM lost_session_restart_contact_fences WHERE contact_id='10'").fetchone())
        self.now += 1000
        self.service = self.make_service()
        self.assertIsNone(self.service.process())
        self.assertEqual(len(self.add_writes), 1)

    def test_delayed_add_visibility_resumes_from_marker_only(self):
        self.add_hook = lambda _params: None
        self.assertEqual(self.run_one()["state"], "uncertain")
        self.now += 100
        self.native_add(self.add_writes[0])
        self.service = self.make_service()
        self.assertEqual(self.service.process()["state"], "restarted")
        self.assertEqual(len(self.add_writes), 1)

    def test_target_claim_before_restart_aborts_without_hijacking(self):
        def claim(method, params):
            if method == "crm.deal.get" and params["id"] == "102" and self.jobs()[0]["target_deal_id"]:
                self.deals["102"]["UF_CRM_CLAIM"] = "claimed-by-9"
                self.deals["102"]["ASSIGNED_BY_ID"] = "9"
        self.hook = claim
        self.assertEqual(self.run_one()["state"], "blocked")
        self.assertFalse(self.writes)
        self.assertFalse(self.queue_writes)

    def test_terminal_claim_releases_contact_but_never_releases_consumed_input(self):
        def claim(method, params):
            if method == "crm.deal.get" and params["id"] == "102" and self.jobs()[0]["target_deal_id"]:
                self.deals["102"]["UF_CRM_CLAIM"] = "claimed-by-9"
        self.hook = claim
        self.assertEqual(self.run_one()["state"], "blocked")
        first = self.jobs()[0]
        proof = json.loads(first["source_proof_json"])
        with self.store._connect() as db:
            self.assertIsNone(db.execute("SELECT * FROM lost_session_restart_contact_fences WHERE contact_id='10'").fetchone())

        def leased(sid):
            with self.store._transaction(immediate=True) as db:
                db.execute("""INSERT INTO lost_session_restart_watches
                    (session_id,chat_id,line_id,source,date_create,deal_id,state,lease_token,lease_until)
                    VALUES(?,'123','103','thoth_waba',?,'101','checking',?,?)""",
                    (sid, self.old["dateCreate"], 'lease-' + sid, self.now + 180))
                return dict(db.execute("SELECT * FROM lost_session_restart_watches WHERE session_id=?", (sid,)).fetchone())

        # A future, independently proven customer input may acquire the contact.
        future = leased('902')
        witness = source_witness(self.message('3', '500000', self.now), '500000')
        self.service.reserve(future, {**proof, 'message_id': '3', 'message_at': self.now,
                                     'source_witness': witness, 'source_identity_key': witness['identity_key']}, None)
        with self.store._connect() as db:
            self.assertEqual(db.execute("SELECT session_id FROM lost_session_restart_contact_fences WHERE contact_id='10'").fetchone()[0], '902')
        # The earlier input remains consumed even in another newly created session.
        old_input = leased('903')
        with self.assertRaisesRegex(Decision, 'restart_message_already_dispatched'):
            self.service.reserve(old_input, proof, None)

    def test_native_operator_acceptance_before_queue_blocks_without_message(self):
        def accepted():
            self.native_restart()
            self.current['dateOperatorAnswer'] = iso(self.now)
            return True
        self.restart_hook = accepted
        self.assertEqual(self.run_one()['code'], 'restart_human_accepted_before_queue')
        self.assertFalse(self.queue_writes)

    def test_source_acceptance_after_return_before_first_proof_never_mutates(self):
        self.old['dateOperatorAnswer'] = iso(self.origin + 3)
        self.assertEqual(self.run_one()['code'], 'restart_source_human_accepted')
        self.assertEqual(self.jobs()[0]['reserved'], 0)
        self.assertFalse(self.add_writes)
        self.assertFalse(self.writes)
        self.assertFalse(self.queue_writes)

    def test_equal_second_source_acceptance_is_ambiguous_and_never_mutates(self):
        self.old['dateOperatorAnswer'] = self.messages['2']['date']
        self.assertEqual(self.run_one()['code'], 'restart_source_human_accepted')
        self.assertFalse(self.add_writes)
        self.assertFalse(self.writes)
        self.assertFalse(self.queue_writes)

    def test_source_answer_field_must_be_present(self):
        self.old.pop('dateOperatorAnswer')
        with self.assertRaises(ValueError):
            self.discover()
        self.assertFalse(self.add_writes)
        self.assertFalse(self.writes)

    def test_human_reply_after_restart_prevents_queue_transfer(self):
        def reply():
            self.native_restart()
            self.messages["3"] = self.message("3", "7", self.now)
            return True
        self.restart_hook = reply
        self.assertEqual(self.run_one()["code"], "restart_later_human_or_unknown_message")
        self.assertFalse(self.queue_writes)

    def test_queue_timeout_is_never_retried(self):
        def uncertain_queue():
            raise TimeoutError("synthetic")
        self.queue_hook = uncertain_queue
        self.assertEqual(self.run_one()["state"], "uncertain")
        self.now += 100
        self.service = self.make_service()
        self.assertEqual(self.service.process()["state"], "uncertain")
        self.assertEqual(len(self.add_writes), 1)
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(len(self.queue_writes), 1)

    def test_queue_status_new_with_retained_answer_date_is_not_success(self):
        def queue_with_stale_answer():
            self.native_queue()
            self.current["dateOperatorAnswer"] = iso(self.now)
            return True
        self.queue_hook = queue_with_stale_answer
        self.assertEqual(self.run_one()["state"], "uncertain")
        self.assertEqual(self.jobs()[0]["state"], "uncertain")
        self.assertEqual(len(self.queue_writes), 1)

    def test_message_must_be_strictly_after_latest_loss(self):
        self.transition["transitionTime"] = self.messages["2"]["date"]
        self.assertEqual(self.run_one()["code"], "restart_no_new_customer_message")
        self.assertFalse(self.writes)

    def test_later_human_reply_prevents_restart_even_when_newest_is_customer(self):
        self.messages["3"] = self.message("3", "7", self.origin + 3)
        self.messages["4"] = self.message("4", "500000", self.origin + 4)
        self.assertEqual(self.run_one()["code"], "restart_later_human_or_unknown_message")
        self.assertFalse(self.writes)

    def test_unknown_later_author_blocks_but_system_and_known_bot_do_not(self):
        self.messages["3"] = self.message("3", "999", self.origin + 3)
        self.assertEqual(self.run_one()["code"], "restart_later_human_or_unknown_message")
        self.assertFalse(self.writes)

    def test_system_and_bot_after_customer_allowed(self):
        self.messages["3"] = self.message("3", "0", self.origin + 3)
        self.messages["4"] = self.message("4", "262867", self.origin + 4)
        self.assertEqual(self.run_one()["state"], "restarted")

    def test_customer_requires_exact_session_user_and_connector_proof(self):
        self.users["500000"]["connector"] = False
        self.assertEqual(self.run_one()["code"], "restart_no_new_customer_message")
        self.assertFalse(self.writes)

    def test_secondary_contact_active_deal_blocks_restart(self):
        self.bindings["101"].append("11")
        self.deals["102"] = {**self.deals["101"], "ID": "102", "STAGE_ID": "NEW", "CLOSED": "N", "STAGE_SEMANTIC_ID": "P"}
        self.bindings["102"] = ["11"]
        self.assertEqual(self.run_one()["code"], "restart_active_deal_exists")
        self.assertFalse(self.writes)

    def test_config_must_disable_contact_stickiness(self):
        self.config["CRM_FORWARD"] = "Y"
        self.assertEqual(self.run_one()["state"], "watching")
        self.assertFalse(self.writes)

    def test_native_closed_or_rebound_session_is_retired(self):
        self.discover()
        self.old.update(status="closed", dateClose=iso(self.now))
        self.assertEqual(self.service.process()["code"], "restart_session_closed")
        self.assertFalse(self.writes)

    def test_per_message_chat_identity_is_required(self):
        self.messages["2"]["chatid"] = "999"
        self.assertEqual(self.run_one()["state"], "watching")
        self.assertFalse(self.writes)

    def test_current_dialog_rebound_before_dispatch_is_retired(self):
        self.discover()
        self.current = {**self.old, "id": 901, "crmEntityId": 102}
        self.assertEqual(self.service.process()["code"], "restart_current_binding_changed")
        self.assertFalse(self.writes)

    def test_native_last_message_must_match_history(self):
        def stale(_params, value):
            value["message"]["3"] = self.message("3", "0", self.origin + 3)
            return value
        self.history_hook = stale
        self.assertEqual(self.run_one()["state"], "watching")
        self.assertFalse(self.writes)

    def test_second_proof_change_prevents_dispatch(self):
        def hook(method, _params):
            if method == "imopenlines.session.history.get" and self.proofs == 2:
                self.messages["3"] = self.message("3", "0", self.origin + 3)
        self.hook = hook
        self.assertEqual(self.run_one()["state"], "watching")
        self.assertFalse(self.writes)

    def test_history_suffix_is_paginated_and_later_human_on_next_page_blocks(self):
        self.messages = {"2": self.message("2", "500000", self.origin + 2),
                         "3": self.message("3", "7", self.origin + 3),
                         "4": self.message("4", "500000", self.origin + 4)}
        def pages(params, value):
            if "LAST_ID" not in params:
                value.update(message={"4": self.messages["4"]}, hasNextPage=True,
                             nextId=4, nextDate=int(self.origin + 4))
            else:
                self.assertEqual(params["LAST_ID"], 4)
                value.update(message={mid: row for mid, row in self.messages.items() if mid != "4"})
            return value
        self.history_hook = pages
        self.assertEqual(self.run_one()["code"], "restart_later_human_or_unknown_message")
        self.assertFalse(self.writes)

    def test_incomplete_history_without_cursor_never_authorizes(self):
        self.messages.pop("1")
        def broken(_params, value):
            value["hasNextPage"] = True
            return value
        self.history_hook = broken
        self.assertEqual(self.run_one()["state"], "watching")
        self.assertFalse(self.writes)

    def test_timeout_does_not_repeat_even_after_restart_and_lease_expiry(self):
        def timeout():
            raise TimeoutError("synthetic")
        self.restart_hook = timeout
        self.assertEqual(self.run_one()["state"], "uncertain")
        self.now += 1000
        self.service = self.make_service()
        self.assertEqual(self.service.process()["state"], "uncertain")
        self.assertEqual(len(self.writes), 1)

    def test_uncertain_native_success_is_read_back_without_second_write(self):
        def timeout_after_commit():
            self.native_restart()
            raise TimeoutError("synthetic")
        self.restart_hook = timeout_after_commit
        self.assertEqual(self.run_one()["state"], "restarted")
        self.assertEqual(len(self.writes), 1)

    def test_true_without_new_native_deal_remains_uncertain(self):
        self.restart_hook = lambda: True
        self.assertEqual(self.run_one()["state"], "uncertain")
        self.now += 100
        self.assertEqual(self.service.process()["state"], "uncertain")
        self.assertEqual(len(self.writes), 1)

    def test_write_rpc_allowlist_blocks_any_unintended_mutation(self):
        with self.assertRaisesRegex(ValueError, "read_only"):
            self.service.rpc("crm.contact.update", {"id": "10"})
        self.assertFalse(self.writes)


if __name__ == "__main__":
    unittest.main()
