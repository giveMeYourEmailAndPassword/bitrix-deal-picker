"""Regression 2026-11-19: a duplicate deal must not stop the whole queue.

A client called through one line and was then contacted through two other
lines, which created duplicate deals in the intake stage. Bitrix listed the
client's accepted dialogs for every one of those deals although each dialog
is bound to a sibling deal. The guard treated that deterministic binding as
unreadable evidence, so every search stopped at the oldest duplicate.
"""

import copy
import os
import unittest
from unittest.mock import Mock, patch

import test_active_dialog_claim as active_fixtures
import test_app as fixtures
from claim_chat_guard import ClaimChatGuardUnavailable, read_claim_chat_ownership

app = fixtures.app

DUPLICATE, SIBLING, OTHER_SIBLING = "588143", "588139", "588145"
MANAGER, OWNER, BOT = "27337", "342049", "262867"
CHAT, SECOND_CHAT, SESSION, LINE = "593761", "110225", "643881", "103"
SOURCE = "synthetic_waba"


def accepted_dialog(chat=CHAT, deal=SIBLING, owner=OWNER):
    return {
        "id": int(chat), "type": "lines", "entity_type": "LINES",
        "entity_data_2": f"LEAD|0|COMPANY|0|CONTACT|89127|DEAL|{deal}",
        "owner": int(owner),
    }


def employee(owner=OWNER, bot=False):
    return {
        "id": int(owner), "bot": bot, "connector": False, "extranet": False,
        "departments": [] if bot else [187],
    }


class TestDuplicateDealChatGuard(unittest.TestCase):
    def guard(self, manager, *responses):
        call = Mock(side_effect=list(responses))
        return read_claim_chat_ownership(DUPLICATE, manager, call), call

    def test_sibling_dialog_of_another_employee_occupies_the_duplicate(self):
        occupied, call = self.guard(
            MANAGER, [{"CHAT_ID": CHAT}, {"CHAT_ID": SECOND_CHAT}],
            accepted_dialog(), employee(),
        )
        self.assertTrue(occupied)
        self.assertEqual([entry.args[0] for entry in call.call_args_list], [
            "imopenlines.crm.chat.get", "imopenlines.dialog.get", "im.user.get",
        ])
        self.assertEqual(call.call_args_list[0].args[1]["CRM_ENTITY"], DUPLICATE)
        self.assertEqual(call.call_args_list[2].args[1], {"ID": OWNER})

    def test_owner_of_every_sibling_dialog_may_take_their_own_duplicate(self):
        occupied, call = self.guard(
            OWNER, [{"CHAT_ID": CHAT}, {"CHAT_ID": SECOND_CHAT}],
            accepted_dialog(), accepted_dialog(SECOND_CHAT, OTHER_SIBLING),
        )
        self.assertFalse(occupied)
        self.assertEqual(call.call_count, 3)

    def test_own_sibling_dialog_does_not_mask_another_employee(self):
        occupied, _ = self.guard(
            MANAGER, [{"CHAT_ID": CHAT}, {"CHAT_ID": SECOND_CHAT}],
            accepted_dialog(owner=MANAGER),
            accepted_dialog(SECOND_CHAT, OTHER_SIBLING), employee(),
        )
        self.assertTrue(occupied)

    def test_verified_bot_in_sibling_dialog_is_not_an_employee(self):
        occupied, _ = self.guard(
            MANAGER, [{"CHAT_ID": CHAT}],
            accepted_dialog(owner=BOT), employee(BOT, bot=True),
        )
        self.assertFalse(occupied)

    def test_dialog_without_a_real_deal_stays_unavailable(self):
        for binding in ("CONTACT|89127", "LEAD|0|COMPANY|0|CONTACT|89127|DEAL|0"):
            with self.subTest(binding=binding):
                call = Mock(side_effect=[
                    [{"CHAT_ID": CHAT}],
                    {**accepted_dialog(), "entity_data_2": binding},
                ])
                with self.assertRaises(ClaimChatGuardUnavailable) as caught:
                    read_claim_chat_ownership(DUPLICATE, MANAGER, call)
                self.assertEqual(caught.exception.args[0], "dialog_deal_binding_mismatch")
                self.assertEqual(call.call_count, 2)

    def test_contact_number_equal_to_the_candidate_is_not_its_binding(self):
        # CONTACT carries the candidate's number, DEAL names the real sibling.
        binding = f"DEAL|{SIBLING}|CONTACT|{DUPLICATE}"
        occupied, call = self.guard(
            MANAGER, [{"CHAT_ID": CHAT}],
            {**accepted_dialog(), "entity_data_2": binding}, employee(),
        )
        self.assertTrue(occupied)
        self.assertEqual(call.call_args_list[2].args[1], {"ID": OWNER})


class TestUnacceptedSiblingDialog(unittest.TestCase):
    """A zero owner is proven against the sibling deal the dialog is bound to."""

    def dialog(self):
        return {
            "id": int(CHAT), "type": "lines", "entity_type": "LINES", "owner": 0,
            "entity_id": f"{SOURCE}|{LINE}|external-chat|external-user",
            "entity_data_1": f"Y|DEAL|{SIBLING}|N|N|{SESSION}|0|0|0|0",
            "entity_data_2": f"LEAD|0|COMPANY|0|CONTACT|89127|DEAL|{SIBLING}",
        }

    def session(self, operator, deal=SIBLING):
        return {
            "id": int(SESSION), "chatId": int(CHAT), "configId": int(LINE),
            "source": SOURCE, "crmEntityType": "deal", "crmEntityId": int(deal),
            "dateCreate": "2026-11-19T09:48:18+00:00", "dateClose": None,
            "dateOperatorAnswer": None, "operatorId": int(operator), "status": "answered",
        }

    def guard(self, operator, *, session_deal=SIBLING):
        self.calls = []

        def call(method, params, *, timeout):
            self.calls.append(method)
            if method == "imopenlines.crm.chat.get":
                return [{"CHAT_ID": CHAT}]
            if method == "imopenlines.dialog.get":
                return self.dialog()
            if method == "imopenlines.v2.Session.list":
                return {"sessions": [copy.deepcopy(self.session(operator, session_deal))],
                        "hasNextPage": False}
            if method == "im.user.get":
                return employee(str(params["ID"]), bot=str(params["ID"]) == BOT)
            self.fail(f"Unexpected read or mutation: {method}")
        return read_claim_chat_ownership(DUPLICATE, MANAGER, call)

    def test_foreign_operator_of_sibling_session_occupies_the_duplicate(self):
        self.assertTrue(self.guard(OWNER))
        self.assertEqual(self.calls, [
            "imopenlines.crm.chat.get", "imopenlines.dialog.get", "imopenlines.v2.Session.list",
            "im.user.get", "imopenlines.v2.Session.list", "imopenlines.dialog.get",
        ])

    def test_bot_or_own_sibling_session_keeps_the_duplicate_eligible(self):
        for operator in (BOT, MANAGER):
            with self.subTest(operator=operator):
                self.assertFalse(self.guard(operator))

    def test_session_of_any_other_deal_is_not_proof(self):
        for session_deal in (DUPLICATE, OTHER_SIBLING):
            with self.subTest(session_deal=session_deal):
                with self.assertRaises(ClaimChatGuardUnavailable):
                    self.guard(OWNER, session_deal=session_deal)


class TestSearchContinuesPastDuplicateDeal(fixtures.ClaimWorkflowTestCase):
    def setUp(self):
        active_fixtures.TestActiveDialogClaim.setUp(self)
        # test_app restores the real environment once its own module is done;
        # issuing a selection token still needs the synthetic configuration.
        environment = patch.dict(os.environ, fixtures._TEST_ENV)
        environment.start()
        self.addCleanup(environment.stop)

    def search(self, manager_id, sibling_owner):
        # "100" is the oldest candidate and a duplicate: the client's accepted
        # dialog is bound to sibling deal "77". "101" is an ordinary free lead.
        headers = [{"ID": i, "DATE_MODIFY": self.version} for i in ("100", "101")]
        deals = {h["ID"]: {"id": h["ID"], "version": self.version, "messages": [],
                           "classification": {"direction": "Не определено"}} for h in headers}

        def call(method, params=None, timeout=None):
            if method == "imopenlines.crm.chat.get":
                return [{"CHAT_ID": "570831"}] if params["CRM_ENTITY"] == "100" else []
            if method == "imopenlines.dialog.get":
                return {"id": 570831, "type": "lines", "entity_type": "LINES",
                        "entity_data_2": "LEAD|0|COMPANY|0|CONTACT|500|DEAL|77",
                        "owner": int(sibling_owner)}
            if method == "im.user.get":
                return {"id": int(params["ID"]), "bot": False, "connector": False,
                        "extranet": False, "departments": [187]}
            raise AssertionError(f"Unexpected non-read-only method: {method}")

        with (self.common_claim_context(),
              patch.object(app, "check_manager_access", return_value={"ok": True, "rule": {}}),
              patch.object(app, "list_allowed_deal_headers", return_value=headers),
              patch.object(app, "iter_analyzed_deal_headers",
                           side_effect=lambda batch: fixtures.analysis_fixture_rows(batch, deals, {})),
              patch.object(app, "bitrix_call", side_effect=call)):
            return app._get_next_deal_for_manager(manager_id)

    def test_other_managers_get_the_next_lead_instead_of_an_error(self):
        result = self.search(self.manager_id, sibling_owner="33")
        self.assertNotEqual(result.get("code"), "chat_ownership_unavailable")
        self.assertNotEqual(result.get("_httpStatus"), 503)
        self.assertEqual(result["deal"]["id"], "101")
        self.assertTrue(result["deal"]["selectionToken"])
        self.assertEqual(self.store.list_claims(), [])

    def test_manager_who_holds_the_client_dialog_gets_the_duplicate(self):
        result = self.search(self.manager_id, sibling_owner=self.manager_id)
        self.assertEqual(result["deal"]["id"], "100")
        self.assertTrue(result["deal"]["selectionToken"])


if __name__ == "__main__":
    unittest.main()
