"""A zero chat owner needs fresh, exact OpenLine session ownership evidence."""

import copy
import unittest
from unittest.mock import patch

from claim_chat_guard import ClaimChatGuardUnavailable, read_claim_chat_ownership


DEAL, CHAT, SESSION, LINE = "100", "700", "900", "33"
MANAGER, BOT, FOREIGN = "41", "902", "99"
SOURCE = "wz_whatsapp_synthetic"


def dialog():
    return {"id": int(CHAT), "type": "lines", "entity_type": "LINES", "owner": 0,
            "entity_id": f"{SOURCE}|{LINE}|external-chat|external-user",
            "entity_data_1": f"Y|DEAL|{DEAL}|N|N|{SESSION}|0|0|0|0",
            "entity_data_2": f"LEAD|0|DEAL|{DEAL}|CONTACT|123|COMPANY|0"}


def session(operator=BOT, status="answered"):
    return {"id": int(SESSION), "chatId": int(CHAT), "configId": int(LINE),
            "source": SOURCE, "crmEntityType": "deal", "crmEntityId": int(DEAL),
            "dateCreate": "2026-10-10T17:00:00+00:00", "dateClose": None,
            "dateOperatorAnswer": None, "operatorId": operator, "status": status}


def identity(operator=BOT):
    return {"id": int(operator), "bot": operator == BOT, "connector": False,
            "extranet": False, "departments": [] if operator == BOT else [187]}


class TestZeroOwnerChatGuard(unittest.TestCase):
    def fixture(self, initial=None, final=None, first_dialog=None, last_dialog=None,
                discovery=None, narrow=None, owner=None, second=False):
        initial = session() if initial is None else initial
        final = copy.deepcopy(initial) if final is None else final
        first_dialog = dialog() if first_dialog is None else first_dialog
        last_dialog = copy.deepcopy(first_dialog) if last_dialog is None else last_dialog
        self.calls = []
        dialogs = iter([first_dialog, last_dialog])

        def call(method, params, *, timeout):
            self.calls.append((method, copy.deepcopy(params), timeout))
            if method == "imopenlines.crm.chat.get":
                return [{"CHAT_ID": CHAT}] + ([{"CHAT_ID": "701"}] if second else [])
            if method == "imopenlines.dialog.get":
                if params["CHAT_ID"] == "701":
                    return {**dialog(), "id": 701, "owner": int(FOREIGN)}
                return next(dialogs)
            if method == "im.user.get":
                return identity(str(params["ID"])) if owner is None else owner
            if method == "imopenlines.v2.Session.list":
                pages = narrow if "dateCreateFrom" in params else discovery
                if callable(pages):
                    return pages(params)
                if pages is not None:
                    return pages
                return {"sessions": [final if "dateCreateFrom" in params else initial], "hasNextPage": False}
            self.fail(f"Unexpected read or mutation: {method}")
        return call

    def guard(self, **kwargs):
        return read_claim_chat_ownership(DEAL, MANAGER, self.fixture(**kwargs))

    def unavailable(self, **kwargs):
        with self.assertRaises(ClaimChatGuardUnavailable):
            self.guard(**kwargs)

    def test_actual_zero_owner_with_verified_bot_is_recovered_read_only(self):
        self.assertFalse(self.guard())
        self.assertEqual([entry[0] for entry in self.calls], [
            "imopenlines.crm.chat.get", "imopenlines.dialog.get", "imopenlines.v2.Session.list",
            "im.user.get", "imopenlines.v2.Session.list", "imopenlines.dialog.get"])
        discovery, narrow = self.calls[2][1], self.calls[4][1]
        self.assertEqual(discovery, {"configId": LINE, "source": SOURCE, "order": "dateCreate",
                                     "orderDirection": "desc", "offset": 0, "limit": 200})
        self.assertEqual(narrow["dateCreateFrom"], "2026-10-10T16:59:59+00:00")
        self.assertEqual(narrow["dateCreateTo"], "2026-10-10T17:00:01+00:00")
        self.assertTrue(all(0 < timeout <= 8 for _, _, timeout in self.calls))

    def test_zero_string_and_own_operator_are_verified(self):
        self.assertFalse(self.guard(first_dialog={**dialog(), "owner": "0"}, initial=session(MANAGER)))
        self.assertEqual(self.calls[3][0], "im.user.get")

    def test_foreign_employee_is_occupied_even_with_zero_dialog_owner(self):
        self.assertTrue(self.guard(initial=session(FOREIGN)))

    def test_recovered_bot_does_not_mask_a_later_foreign_active_chat(self):
        self.assertTrue(self.guard(second=True))
        self.assertEqual(self.calls[-1][1], {"ID": FOREIGN})

    def test_new_session_requires_explicit_unanswered_unclosed_zero_operator(self):
        for operator in (None, 0, "0"):
            with self.subTest(operator=operator):
                self.assertFalse(self.guard(initial=session(operator, "new")))
                self.assertNotIn("im.user.get", [entry[0] for entry in self.calls])
        for field, value in (("status", "answered"), ("status", "paused"),
                             ("dateOperatorAnswer", "2026-10-10T17:00:05Z"),
                             ("dateClose", "2026-10-10T17:01:00Z"), ("operatorId", False)):
            with self.subTest(field=field, value=value):
                self.unavailable(initial={**session(None, "new"), field: value})
        for field in ("operatorId", "dateOperatorAnswer", "dateClose"):
            current = session(None, "new")
            current.pop(field)
            with self.subTest(missing=field):
                self.unavailable(initial=current)

    def test_zero_fallback_does_not_accept_malformed_owner_or_dialog(self):
        for value in (None, False, True, 0.0, "", "00", " 0 ", -1):
            with self.subTest(owner=value):
                self.unavailable(first_dialog={**dialog(), "owner": value})
                self.assertEqual(len(self.calls), 2)
        for field, value in (("id", 0), ("type", "chat"), ("entity_type", "CRM"),
                             ("entity_data_2", "DEAL|999"), ("entity_id", "malformed"),
                             ("entity_data_1", "Y|DEAL|100|N|N|0")):
            with self.subTest(field=field):
                self.unavailable(first_dialog={**dialog(), field: value})
                self.assertEqual(len(self.calls), 2)

    def test_final_dialog_owner_session_binding_or_connector_change_fails_closed(self):
        for field, value in (("owner", FOREIGN), ("entity_data_1", "Y|DEAL|100|N|N|901"),
                             ("entity_data_2", "DEAL|101"), ("entity_id", f"another|33|client|user"),
                             ("id", 701), ("type", "chat")):
            with self.subTest(field=field):
                self.unavailable(last_dialog={**dialog(), field: value})

    def test_final_session_cannot_reuse_discovered_bot_identity_after_any_change(self):
        for field, value in (("operatorId", FOREIGN), ("status", "paused"), ("id", 901),
                             ("chatId", 701), ("configId", 34), ("source", "another"),
                             ("crmEntityType", "lead"), ("crmEntityId", 101),
                             ("dateCreate", "2026-10-10T17:00:01+00:00"),
                             ("dateOperatorAnswer", "2026-10-10T17:00:01Z"),
                             ("dateClose", "2026-10-10T17:00:01Z")):
            with self.subTest(field=field):
                self.unavailable(final={**session(), field: value})

    def test_missing_mismatched_or_ambiguous_session_is_never_free(self):
        for pages in ({"sessions": [], "hasNextPage": False},
                      {"sessions": [session(), session()], "hasNextPage": False},
                      {"sessions": [session()], "hasNextPage": "false"},
                      {"sessions": [session()] * 201, "hasNextPage": False},
                      {"sessions": [None], "hasNextPage": False}, {}):
            for phase in ("discovery", "narrow"):
                with self.subTest(phase=phase, pages=repr(pages)[:80]):
                    self.unavailable(**{phase: pages})
        for field, value in (("chatId", 1), ("configId", 1), ("source", "another"),
                             ("crmEntityType", "contact"), ("crmEntityId", 1),
                             ("dateCreate", "invalid"), ("dateCreate", "2026-10-10T17:00:00")):
            with self.subTest(field=field):
                self.unavailable(initial={**session(), field: value})

    def test_identity_must_freshly_prove_internal_bot_or_employee(self):
        for owner in ({**identity(), "id": 999}, {**identity(), "bot": "true"},
                      {**identity(), "extranet": True}, {**identity(), "connector": True},
                      {"id": int(BOT), "bot": True}):
            with self.subTest(owner=owner):
                self.unavailable(owner=owner)

    def test_discovery_and_final_pagination_are_bounded_and_final_must_complete(self):
        other = {**session(), "id": 123}
        for phase in ("discovery", "narrow"):
            with self.subTest(phase=phase):
                self.unavailable(**{phase: {"sessions": [other if phase == "discovery" else session()], "hasNextPage": True}})
                calls = [params for method, params, _ in self.calls if method == "imopenlines.v2.Session.list" and ("dateCreateFrom" in params) == (phase == "narrow")]
                self.assertLessEqual(len(calls), 4)
        def final_pages(params):
            return {"sessions": [session()] if params["offset"] == 0 else [other], "hasNextPage": params["offset"] == 0}
        self.assertFalse(self.guard(narrow=final_pages))
        self.assertEqual([params["offset"] for method, params, _ in self.calls if method == "imopenlines.v2.Session.list" and "dateCreateFrom" in params], [0, 200])
        def truncated_final(params):
            return {"sessions": [session()] if params["offset"] == 0 else [other], "hasNextPage": True}
        self.unavailable(narrow=truncated_final)
        self.assertEqual([params["offset"] for method, params, _ in self.calls if method == "imopenlines.v2.Session.list" and "dateCreateFrom" in params], [0, 200, 400, 600])

    def test_discovery_has_no_authority_until_complete_fresh_narrow_read(self):
        self.assertFalse(self.guard(discovery={"sessions": [session()], "hasNextPage": True}))
        self.assertEqual(len([entry for entry in self.calls if entry[0] == "imopenlines.v2.Session.list"]), 2)

    def test_empty_page_claiming_more_results_is_not_a_complete_proof(self):
        def pages(params):
            return {"sessions": [] if params["offset"] == 0 else [session()],
                    "hasNextPage": params["offset"] == 0}
        for phase in ("discovery", "narrow"):
            with self.subTest(phase=phase):
                self.unavailable(**{phase: pages})

    def test_expired_shared_budget_prevents_fallback_completion(self):
        now = [100.0]
        base_call = self.fixture()
        def call(method, params, *, timeout):
            result = base_call(method, params, timeout=timeout)
            now[0] += 2
            return result
        with patch("claim_chat_guard.time.monotonic", side_effect=lambda: now[0]):
            with self.assertRaisesRegex(ClaimChatGuardUnavailable, "chat_guard_timeout"):
                read_claim_chat_ownership(DEAL, MANAGER, call)
        self.assertEqual(len(self.calls), 4)
        self.assertEqual([entry[2] for entry in self.calls], [8, 6, 4, 2])

    def test_provider_failure_at_each_recovery_read_remains_unavailable(self):
        for failing in ("imopenlines.v2.Session.list", "im.user.get"):
            base_call = self.fixture()
            def call(method, params, *, timeout):
                if method == failing:
                    raise RuntimeError("private upstream details")
                return base_call(method, params, timeout=timeout)
            with self.subTest(method=failing):
                with self.assertRaisesRegex(ClaimChatGuardUnavailable, "chat_guard_read_failed"):
                    read_claim_chat_ownership(DEAL, MANAGER, call)


if __name__ == "__main__":
    unittest.main()
