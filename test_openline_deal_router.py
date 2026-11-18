"""Synthetic native sessions and real SQLite; no Bitrix requests are sent."""
import copy
import json
import tempfile
import unittest
from pathlib import Path

from claim_locks import ClaimLocks
from contact_owner_policy import ITEM_FIELDS
from openline_deal_router import OpenlineDealRouter, iso, timestamp, validate_router_settings
from state_store import StateStore


class OpenlineDealRouterTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="openline-deal-router-")
        self.addCleanup(self.temp.cleanup)
        self.store = StateStore(Path(self.temp.name))
        self.now = 1794996000.5
        self.origin = self.now
        self.config = {"ID": "103", "ACTIVE": "Y", "CRM_FORWARD": "N", "QUEUE": ["41"],
                       "WELCOME_BOT_ENABLE": "Y", "WELCOME_BOT_JOIN": "always", "WELCOME_BOT_ID": "262867"}
        self.session = {"id": 900, "chatId": 123, "configId": 103, "source": "thoth_waba",
                        "dateCreate": iso(self.now + 1.5), "operatorId": 41, "status": "new",
                        "crmEntityType": "deal", "crmEntityId": 101,
                        "dateOperatorAnswer": None, "dateFirstAnswer": None, "dateClose": None}
        self.sessions = [self.session]
        self.owner = 0
        self.managers = []
        self.participants = [262867, 41, 500000]
        self.deals = {"101": {"ID": "101", "ASSIGNED_BY_ID": "7", "CONTACT_ID": "10",
                              "DATE_CREATE": iso(self.now - 1000), "DATE_MODIFY": iso(self.now - 500),
                              "STAGE_ID": "NEW", "STAGE_SEMANTIC_ID": "P", "CLOSED": "N"}}
        self.bindings = {"101": ["10"]}
        self.user = {"ID": "7", "ACTIVE": True, "UF_DEPARTMENT": [1]}
        self.calls, self.writes = [], []
        self.before_call, self.transfer, self.list_response = None, None, None
        self.binding_extra = ""
        self.service = self.make_service()
        self.now += 10

    def make_service(self):
        return OpenlineDealRouter(self.store, self.call, ClaimLocks(),
                                  allowed_lines={"103": {"thoth_waba"}}, bot_id="262867",
                                  bot_client_id="test-client-secret", now=lambda: self.now)

    def call(self, method, params, timeout=8):
        self.calls.append((method, copy.deepcopy(params)))
        if self.before_call:
            self.before_call(method, params)
        if method == "imopenlines.config.get":
            return copy.deepcopy(self.config)
        if method == "imopenlines.v2.Session.list":
            if self.list_response:
                return self.list_response(params)
            rows = [copy.deepcopy(row) for row in self.sessions
                    if str(row["configId"]) == params["configId"] and row["source"] == params["source"]
                    and timestamp(params["dateCreateFrom"]) <= timestamp(row["dateCreate"]) <= timestamp(params["dateCreateTo"])]
            rows.sort(key=lambda row: timestamp(row["dateCreate"]))
            offset, limit = params["offset"], params["limit"]
            return {"sessions": rows[offset:offset + limit], "hasNextPage": offset + limit < len(rows)}
        if method == "imopenlines.dialog.get":
            return {"id": 123, "type": "lines", "owner": self.owner,
                    "entity_id": "thoth_waba|103|external|address",
                    "entity_data_1": f'Y|DEAL|{self.session.get("crmEntityId") or 0}|N|N|900',
                    "entity_data_2": f'CONTACT|10|DEAL|{self.session.get("crmEntityId") or 0}' + self.binding_extra,
                    "manager_list": copy.deepcopy(self.managers)}
        if method == "im.chat.user.list":
            return copy.deepcopy(self.participants)
        if method == "imopenlines.crm.chat.get":
            return [{"CHAT_ID": 123, "CONNECTOR_ID": "thoth_waba"}]
        if method == "crm.deal.get":
            return copy.deepcopy(self.deals[params["id"]])
        if method == "crm.deal.contact.items.get":
            return [{"CONTACT_ID": cid} for cid in self.bindings[params["id"]]]
        if method == "crm.item.list":
            items = []
            for deal in self.deals.values():
                ids = self.bindings[deal["ID"]]
                if params["filter[contactIds]"] in ids and deal["CLOSED"] == "N" and deal["STAGE_SEMANTIC_ID"] == "P":
                    items.append({**{key: deal[value] for key, value in ITEM_FIELDS.items() if value in deal}, "contactIds": ids})
            return {"items": items[params["start"]:params["start"] + 50]}
        if method == "user.get":
            return [copy.deepcopy(self.user)]
        if method == "imopenlines.bot.session.transfer":
            self.writes.append(copy.deepcopy(params))
            with self.store._connect() as db:
                intent = dict(db.execute("SELECT * FROM openline_router_jobs WHERE session_id='900'").fetchone())
            self.assertEqual(intent["dispatched"], 1)
            self.assertEqual(intent["manager_id"], "7")
            self.assertNotIn("test-client-secret", json.dumps(intent))
            if self.transfer:
                return self.transfer()
            self.session["operatorId"] = 7
            self.owner = 7
            self.session["status"] = "answered"
            self.session["dateOperatorAnswer"] = iso(self.now)
            return True
        self.fail("unexpected endpoint: " + method)

    def jobs(self):
        with self.store._connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM openline_router_jobs ORDER BY CAST(session_id AS INTEGER)")]

    def run_one(self):
        self.service.poll()
        return self.service.process()

    def test_active_deal_routes_offered_queue_session_once(self):
        self.assertEqual(self.run_one()["state"], "routed")
        self.assertEqual(self.writes, [{"CHAT_ID": "123", "USER_ID": "7", "LEAVE": "Y", "CLIENT_ID": "test-client-secret"}])
        self.service = self.make_service()
        self.now += 60
        self.service.run()
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(len(self.jobs()), 1)
        self.assertFalse(any(method.startswith("crm.") and method.endswith(("update", "add")) for method, _ in self.calls))

    def test_both_intake_stages_with_only_previous_lost_deal_are_untouched(self):
        for stage in ("UC_ZJ55BR", "C7:UC_PUCAAQ"):
            with self.subTest(stage=stage):
                if self.jobs():
                    with self.store._transaction(immediate=True) as db:
                        db.execute("DELETE FROM openline_router_jobs")
                    self.now += 10
                    self.session["dateCreate"] = iso(self.now - 3)
                self.deals["101"]["STAGE_ID"] = stage
                self.deals["100"] = {**self.deals["101"], "ID": "100", "STAGE_ID": "LOSE", "STAGE_SEMANTIC_ID": "F", "CLOSED": "Y"}
                self.bindings["100"] = ["10"]
                self.assertEqual(self.run_one()["state"], "intake")
                self.assertEqual(self.writes, [])

    def test_bound_lost_is_blocked_not_misreported_as_new_intake(self):
        self.deals["101"].update(STAGE_ID="LOSE", CLOSED="Y", STAGE_SEMANTIC_ID="F")
        self.assertEqual(self.run_one(), {"state": "blocked", "code": "router_bound_deal_not_open"})
        self.assertEqual(self.writes, [])

    def test_newer_canonical_different_from_session_is_blocked(self):
        self.deals["102"] = {**self.deals["101"], "ID": "102", "DATE_CREATE": iso(self.now)}
        self.bindings["102"] = ["10"]
        self.assertEqual(self.run_one()["code"], "router_canonical_deal_mismatch")
        self.assertEqual(self.writes, [])

    def test_all_actual_contacts_must_agree_on_same_deal(self):
        self.bindings["101"].append("11")
        self.deals["102"] = {**self.deals["101"], "ID": "102", "CONTACT_ID": "11", "DATE_CREATE": iso(self.now)}
        self.bindings["102"] = ["11"]
        self.assertEqual(self.run_one()["state"], "blocked")
        self.assertEqual(self.writes, [])

    def test_discovery_of_already_answered_session_never_routes(self):
        self.session["status"] = "answered"
        self.service.run()
        self.assertEqual(self.jobs()[0]["state"], "skipped")
        self.assertEqual(self.writes, [])

    def test_human_takes_session_during_policy_reads(self):
        def race(method, _params):
            if method == "crm.item.list":
                self.owner = 8
                self.session["operatorId"] = 8
                self.session["status"] = "answered"
        self.before_call = race
        self.assertEqual(self.run_one()["state"], "skipped")
        self.assertEqual(self.writes, [])

    def test_offered_operator_not_in_queue_is_not_replaced(self):
        self.session["operatorId"] = 8
        self.assertEqual(self.run_one()["code"], "router_other_operator_offer")
        self.assertEqual(self.writes, [])

    def test_nonzero_dialog_human_owner_blocks_even_if_session_new(self):
        self.owner = 41
        self.assertEqual(self.run_one()["code"], "router_human_takeover")
        self.assertEqual(self.writes, [])

    def test_missing_answer_metadata_is_not_proof_of_unanswered(self):
        del self.session["dateOperatorAnswer"]
        self.assertEqual(self.run_one()["state"], "pending")
        self.assertEqual(self.writes, [])

    def test_native_contact_must_be_actually_bound_to_deal(self):
        self.bindings["101"] = ["11"]
        self.assertEqual(self.run_one()["code"], "router_dialog_contact_mismatch")
        self.assertEqual(self.writes, [])

    def test_late_config_change_prevents_dispatch(self):
        def race(method, _params):
            if method == "crm.item.list":
                self.config["CRM_FORWARD"] = "Y"
        self.before_call = race
        self.assertEqual(self.run_one()["state"], "pending")
        self.assertEqual(self.jobs()[0]["dispatched"], 0)
        self.assertEqual(self.writes, [])

    def test_old_sessions_are_not_replayed_after_restart_or_downtime(self):
        old = {**self.session, "id": 899, "dateCreate": iso(self.origin - 1)}
        self.sessions.insert(0, old)
        self.service = self.make_service()
        self.now += 10000
        self.service.poll()
        self.assertEqual([row["session_id"] for row in self.jobs()], ["900"])
        self.assertEqual(float(self.service.meta("activated_at")), self.origin)
        cursor = json.loads(self.service.meta("cursor:0"))
        self.assertLessEqual(cursor["to"] - self.origin, 300)

    def test_missing_crm_binding_is_retried_without_losing_discovery(self):
        self.session["crmEntityId"] = None
        self.assertEqual(self.run_one()["state"], "pending")
        self.assertEqual(self.jobs()[0]["dispatched"], 0)
        self.session["crmEntityId"] = 101
        self.service = self.make_service()
        self.now += 20
        self.assertEqual(self.service.process()["state"], "routed")

    def test_departed_welcome_bot_can_route_proven_unanswered_queue(self):
        self.participants.remove(262867)
        self.assertEqual(self.run_one()["state"], "routed")
        self.assertEqual(len(self.writes), 1)
        self.assertFalse(any(method == "im.chat.user.list" for method, _ in self.calls))

    def test_native_queue_null_operator_is_explicitly_unassigned(self):
        self.session["operatorId"] = None
        self.assertEqual(self.run_one()["state"], "routed")
        self.assertEqual(len(self.writes), 1)
        self.assertTrue(all(params["ACTIVE_ONLY"] == "N" for method, params in self.calls
                            if method == "imopenlines.crm.chat.get"))

    def test_missing_operator_field_never_proves_unassigned(self):
        del self.session["operatorId"]
        self.assertEqual(self.run_one()["state"], "pending")
        self.assertEqual(self.writes, [])

    def test_intake_can_complete_after_bot_left_without_participant_read(self):
        self.deals["101"]["STAGE_ID"] = "UC_PUCAAQ"
        self.participants.remove(262867)
        self.assertEqual(self.run_one()["state"], "intake")
        self.assertFalse(any(method == "im.chat.user.list" for method, _ in self.calls))
        self.assertEqual(self.writes, [])

    def test_run_returns_only_safe_integer_counters(self):
        result = self.service.run()
        self.assertEqual(result, {"observed": 1, "processed": 1, "routed": 1,
                                  "intake": 0, "blocked": 0, "errors": 0, "deferred": 0})
        self.assertTrue(all(type(value) is int for value in result.values()))
        self.assertNotIn("test-client-secret", json.dumps(result))

    def test_run_reports_retry_errors_using_only_counters(self):
        self.session["crmEntityId"] = None
        result = self.service.run()
        self.assertEqual(result["processed"], 1)
        self.assertEqual(result["deferred"], 1)
        self.assertEqual(result["errors"], 1)
        self.assertTrue(all(type(value) is int for value in result.values()))

    def test_concurrent_run_returns_numeric_deferred_counter(self):
        self.service.running.acquire()
        try:
            result = self.service.run()
        finally:
            self.service.running.release()
        self.assertEqual(result["deferred"], 1)
        self.assertTrue(all(type(value) is int for value in result.values()))

    def test_timeout_after_success_reconciles_without_repeating_transfer(self):
        def timeout():
            self.session["operatorId"] = 7
            self.owner = 7
            raise TimeoutError("response lost test-client-secret")
        self.transfer = timeout
        self.assertEqual(self.run_one()["state"], "routed")
        self.service = self.make_service()
        self.now += 20
        self.service.run()
        self.assertEqual(len(self.writes), 1)

    def test_timeout_before_outcome_never_blindly_retries_after_restart(self):
        def timeout():
            raise TimeoutError("unknown test-client-secret")
        self.transfer = timeout
        self.assertEqual(self.run_one()["state"], "dispatched")
        self.assertNotIn("test-client-secret", json.dumps(self.jobs()))
        self.service = self.make_service()
        self.now += 20
        self.assertEqual(self.service.process()["state"], "dispatched")
        self.assertEqual(len(self.writes), 1)
        self.owner = 7
        self.session["operatorId"] = 7
        self.now += 40
        self.assertEqual(self.service.process()["state"], "routed")
        self.assertEqual(len(self.writes), 1)

    def test_uncertain_write_exhaustion_stays_terminal_without_duplicate(self):
        self.transfer = lambda: False
        self.run_one()
        for _ in range(12):
            self.now += 301
            self.service.process()
        self.assertEqual(self.jobs()[0]["state"], "uncertain")
        self.assertEqual(len(self.writes), 1)

    def test_page_commit_is_atomic_on_invalid_page_and_cursor_resumes(self):
        rows = [{**self.session, "id": 900 + i} for i in range(201)]
        self.sessions = rows
        self.service.poll()
        self.assertEqual(len(self.jobs()), 200)
        self.assertEqual(json.loads(self.service.meta("cursor:0"))["offset"], 200)
        self.service = self.make_service()
        self.service.poll()
        self.assertEqual(len(self.jobs()), 201)
        self.assertTrue(json.loads(self.service.meta("cursor:0"))["complete"])

    def test_malformed_page_never_advances_cursor(self):
        self.list_response = lambda _params: {"sessions": [self.session, self.session], "hasNextPage": False}
        with self.assertRaisesRegex(ValueError, "discovery_filter"):
            self.service.poll()
        cursor = json.loads(self.service.meta("cursor:0"))
        self.assertEqual(cursor["offset"], 0)
        self.assertEqual(self.jobs(), [])
        self.list_response = None
        self.service.poll()
        self.assertEqual(len(self.jobs()), 1)

    def test_repeated_id_on_later_page_cannot_silently_skip_a_date_tie(self):
        self.sessions = [{**self.session, "id": 900 + i} for i in range(201)]
        self.service.poll()
        self.list_response = lambda _params: {"sessions": [self.sessions[0]], "hasNextPage": False}
        with self.assertRaisesRegex(ValueError, "discovery_filter"):
            self.service.poll()
        self.assertEqual(json.loads(self.service.meta("cursor:0"))["offset"], 200)
        self.assertEqual(len(self.jobs()), 200)

    def test_lease_prevents_another_instance_from_dispatching_same_job(self):
        self.service.poll()
        other = self.make_service()
        called = []
        def concurrent(method, _params):
            if method == "crm.item.list" and not called:
                called.append(other.process())
        self.before_call = concurrent
        self.assertEqual(self.service.process()["state"], "routed")
        self.assertEqual(called, [None])
        self.assertEqual(len(self.writes), 1)

    def test_inactive_manager_is_blocked(self):
        self.user["ACTIVE"] = False
        self.assertEqual(self.run_one()["code"], "router_employee_unavailable")
        self.assertEqual(self.writes, [])

    def test_generic_rpc_cannot_write_to_crm_or_send_messages(self):
        for method in ("crm.deal.update", "crm.contact.update", "im.message.add", "imopenlines.bot.session.transfer"):
            with self.subTest(method=method), self.assertRaisesRegex(ValueError, "read_only"):
                self.service.rpc(method, {})

    def test_readiness_validation_is_pure_and_rejects_missing_configuration(self):
        valid = {"allowed_lines": {"103": ["thoth_waba"]}, "bot_id": "262867",
                 "bot_client_id": "test-client-secret"}
        self.assertEqual(validate_router_settings(**valid), [("103", "thoth_waba")])
        for change in ({"allowed_lines": {}}, {"allowed_lines": {"103": "thoth_waba"}},
                       {"bot_id": ""}, {"bot_client_id": ""}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate_router_settings(**{**valid, **change})
        self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()
