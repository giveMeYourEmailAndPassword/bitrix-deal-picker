"""Old zero-owner dialogs must not strand the queue behind the recent 800."""

import copy
from datetime import datetime
import unittest
from unittest.mock import patch

from claim_chat_guard import ClaimChatGuardUnavailable, read_claim_chat_ownership
from test_zero_owner_chat_guard import (
    BOT, CHAT, DEAL, FOREIGN, MANAGER, SESSION, dialog, identity, session,
)


EPOCH = str(int(datetime.fromisoformat(session()["dateCreate"]).timestamp()))


def hinted_dialog(value=EPOCH):
    result = dialog()
    fields = result["entity_data_1"].split("|")
    fields[6] = value
    result["entity_data_1"] = "|".join(fields)
    return result


def page(*rows, more=False):
    return {"sessions": list(rows), "hasNextPage": more}


class TestSessionTimeHint(unittest.TestCase):
    def portal(self, *, first=None, final=None, initial_dialog=None,
               last_dialog=None, broad=None, operator=BOT):
        self.calls = []
        current = session(operator)
        dialogs = iter([initial_dialog or hinted_dialog(), last_dialog or hinted_dialog()])
        narrow_calls = 0

        def call(method, params, *, timeout):
            nonlocal narrow_calls
            self.calls.append((method, copy.deepcopy(params), timeout))
            if method == "imopenlines.crm.chat.get":
                return [{"CHAT_ID": CHAT}]
            if method == "imopenlines.dialog.get":
                return next(dialogs)
            if method == "im.user.get":
                return identity(str(params["ID"]))
            if method == "imopenlines.v2.Session.list":
                if "dateCreateFrom" not in params:
                    if broad is None:
                        self.fail("Old session must be found without scanning the recent channel")
                    return broad(params) if callable(broad) else broad
                narrow_calls += 1
                response = first if narrow_calls == 1 else final
                return page(current) if response is None else response
            self.fail(f"Unexpected read or mutation: {method}")
        return call

    def guard(self, **kwargs):
        return read_claim_chat_ownership(DEAL, MANAGER, self.portal(**kwargs))

    def test_old_session_beyond_recent_800_is_proven_in_two_exact_windows(self):
        self.assertFalse(self.guard())
        self.assertEqual([method for method, _, _ in self.calls], [
            "imopenlines.crm.chat.get", "imopenlines.dialog.get",
            "imopenlines.v2.Session.list", "im.user.get",
            "imopenlines.v2.Session.list", "imopenlines.dialog.get",
        ])
        windows = [params for method, params, _ in self.calls if method == "imopenlines.v2.Session.list"]
        self.assertEqual(windows[0], windows[1])
        self.assertEqual(windows[0]["dateCreateFrom"], "2026-10-10T16:59:59+00:00")
        self.assertEqual(windows[0]["dateCreateTo"], "2026-10-10T17:00:01+00:00")

    def test_incident_shape_skips_foreign_employee_but_allows_own_or_bot(self):
        for operator, occupied in ((FOREIGN, True), (MANAGER, False), (BOT, False)):
            with self.subTest(operator=operator):
                self.assertEqual(self.guard(operator=operator), occupied)

    def test_missing_or_unusable_timestamp_preserves_compatibility_lookup(self):
        for hint in ("0", "", "not-a-date", "-1", "1.5", "9" * 30, " 1794479938 "):
            with self.subTest(hint=hint):
                self.assertFalse(self.guard(initial_dialog=hinted_dialog(hint), broad=page(session())))
                self.assertNotIn("dateCreateFrom", self.calls[2][1])

    def test_complete_empty_hint_window_falls_back_without_treating_it_as_free(self):
        self.assertFalse(self.guard(first=page(), broad=page(session())))
        self.assertNotIn("dateCreateFrom", self.calls[3][1])
        with self.assertRaisesRegex(ClaimChatGuardUnavailable, "zero_owner_session_unavailable"):
            self.guard(first=page(), broad=page())
        self.assertNotIn("im.user.get", [method for method, _, _ in self.calls])

    def test_timestamp_is_only_hint_authoritative_window_uses_session_date(self):
        later = {**session(), "dateCreate": "2026-10-10T17:00:01+00:00"}
        self.assertFalse(self.guard(first=page(later), final=page(later)))
        windows = [params for method, params, _ in self.calls if method == "imopenlines.v2.Session.list"]
        self.assertEqual(windows[1]["dateCreateFrom"], "2026-10-10T17:00:00+00:00")
        self.assertEqual(windows[1]["dateCreateTo"], "2026-10-10T17:00:02+00:00")

    def test_hint_never_substitutes_for_exact_session_identity_or_owner(self):
        for field, value in (("chatId", 701), ("configId", 34), ("source", "other"),
                             ("crmEntityType", "contact"), ("crmEntityId", 101),
                             ("operatorId", False), ("dateClose", "2026-10-10T17:01:00Z")):
            with self.subTest(field=field), self.assertRaises(ClaimChatGuardUnavailable):
                self.guard(first=page({**session(), field: value}))
        with self.assertRaises(ClaimChatGuardUnavailable):
            self.guard(first=page({**session(), "id": int(SESSION) + 1}), broad=page())

    def test_malformed_duplicate_or_incomplete_hint_response_does_not_fall_back(self):
        for response in ({}, page(None), page(session(), session()),
                         {"sessions": [session()], "hasNextPage": "false"},
                         page(more=True), page(session(), more=True)):
            with self.subTest(response=response), self.assertRaises(ClaimChatGuardUnavailable):
                self.guard(first=response, final=response)
            self.assertTrue(all("dateCreateFrom" in params for method, params, _ in self.calls
                                if method == "imopenlines.v2.Session.list"))

    def test_fresh_proof_and_final_dialog_still_reject_ownership_changes(self):
        for final in (page(), page({**session(), "operatorId": FOREIGN}),
                      page({**session(), "chatId": 701}), page({**session(), "status": "paused"})):
            with self.subTest(final=final), self.assertRaises(ClaimChatGuardUnavailable):
                self.guard(final=final)
        with self.assertRaises(ClaimChatGuardUnavailable):
            self.guard(last_dialog={**hinted_dialog(), "owner": int(FOREIGN)})

    def test_hint_and_fallback_share_deadline_and_cannot_authorize_late_result(self):
        now = [100.0]
        base = self.portal(first=page(), broad=page(session()))
        def slow(method, params, *, timeout):
            result = base(method, params, timeout=timeout)
            now[0] += 2
            return result
        with patch("claim_chat_guard.time.monotonic", side_effect=lambda: now[0]):
            with self.assertRaisesRegex(ClaimChatGuardUnavailable, "chat_guard_timeout"):
                read_claim_chat_ownership(DEAL, MANAGER, slow)
        self.assertEqual([timeout for _, _, timeout in self.calls], [8, 6, 4, 2])


if __name__ == "__main__":
    unittest.main()
