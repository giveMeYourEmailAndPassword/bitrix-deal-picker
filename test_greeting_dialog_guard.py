import unittest
from unittest.mock import patch

from greeting_dialog_guard import GreetingDialogGuardError, require_quiet_claim_dialog


class UnreadText(dict):
    def get(self, key, *args):
        if key in ("text", "files", "name", "first_name"):
            raise AssertionError("message content or profile was read")
        return super().get(key, *args)


def message(id_, author=0, date="2026-11-17T00:00:02Z", codes=("CHAT_JOIN",)):
    return UnreadText(id=id_, chat_id=32, author_id=author, date=date, params={"CODE": list(codes)})


class Provider:
    def __init__(self, source="fbinstagramdirect"):
        self.calls = []
        self.dialog = {"id": 32, "entity_id": f"{source}|3|peer|contact", "type": "lines",
                       "entity_type": "LINES", "entity_data_1": "Y|DEAL|23|N|N|37|1794873600",
                       "entity_data_2": "DEAL|23|CONTACT|1", "last_message_id": 11, "owner": 41,
                       "text_field_enabled": True}
        self.last = 32
        self.active = [{"CHAT_ID": 32}]
        self.session = {"id": 37, "chatId": 32, "configId": 3, "source": source,
                        "crmEntityType": "deal", "crmEntityId": 23,
                        "dateCreate": "2026-11-17T00:00:00Z", "operatorId": 41,
                        "status": "answered", "dateOperatorAnswer": None, "dateClose": None}
        self.pages = {None: [message(11), message(10, 41, "2026-11-16T23:59:59Z")]}
        self.late = {}
        self.users = []

    def read(self, method, params, timeout):
        assert 0 < timeout <= 8
        self.calls.append(method)
        if method == "imopenlines.crm.chat.get":
            assert params["ACTIVE_ONLY"] == "Y"
            return self.active
        if method == "imopenlines.crm.chat.getLastId":
            return self.last
        if method == "imopenlines.dialog.get":
            return {**self.dialog, **(self.late if self.calls.count(method) > 1 else {})}
        if method == "imopenlines.v2.Session.list":
            assert params["source"] == self.dialog["entity_id"].split("|")[0]
            return {"sessions": [self.session], "hasNextPage": False}
        if method == "im.dialog.messages.get":
            assert params["DIALOG_ID"] == "chat32" and params["LIMIT"] == 50
            return {"messages": self.pages.get(params.get("LAST_ID"), []), "users": self.users}
        raise AssertionError("unexpected provider method")


class GreetingGuardTest(unittest.TestCase):
    def guard(self, fixture, **changes):
        args = dict(deal_id=23, manager_id=41, chat_id=32, session_id=37,
                    claimed_at="2026-11-17T00:00:00.800Z")
        return require_quiet_claim_dialog(fixture.read, **{**args, **changes})

    def denied(self, fixture, reason, **changes):
        with self.assertRaises(GreetingDialogGuardError) as raised:
            self.guard(fixture, **changes)
        self.assertEqual(str(raised.exception), reason)

    def test_exact_native_sources_system_join_then_unchanged_tail(self):
        for source in ("fbinstagramdirect", "thoth_waba", "wz_whatsapp_" + "a" * 16):
            with self.subTest(source=source):
                f = Provider(source)
                self.assertIsNone(self.guard(f))
                self.assertEqual(f.calls, ["imopenlines.crm.chat.get", "imopenlines.crm.chat.getLastId",
                                           "imopenlines.dialog.get", "im.dialog.messages.get",
                                           "imopenlines.v2.Session.list", "imopenlines.dialog.get"])

    def test_no_or_multiple_active_or_latest_is_different(self):
        f = Provider()
        f.active = []
        self.denied(f, "chat_activation_pending")
        f.last = 35
        self.denied(f, "greeting_dialog_changed")
        f.last = 32
        f.active = [{"CHAT_ID": 33}, {"CHAT_ID": 32}]
        self.denied(f, "greeting_dialog_changed")
        f.active = [{"CHAT_ID": 32}]
        f.last = 33
        self.denied(f, "greeting_dialog_changed")
        self.assertNotIn("im.dialog.messages.get", f.calls)

    def test_owner_and_operator_are_independent_and_exact(self):
        for destination, name, value, reason in [
            ("dialog", "owner", 0, "chat_activation_pending"),
            ("dialog", "owner", 42, "chat_owned_by_another_manager"),
            ("session", "operatorId", 42, "chat_owned_by_another_manager"),
            ("session", "status", "new", "chat_activation_pending"),
            ("session", "crmEntityId", 24, "greeting_history_unverified"),
            ("dialog", "entity_data_1", "Y|DEAL|25|N|N|37", "greeting_dialog_changed"),
            ("dialog", "entity_data_1", "Y|DEAL|23|N|N|38", "greeting_dialog_changed"),
        ]:
            with self.subTest(name=name, value=value):
                f = Provider()
                getattr(f, destination)[name] = value
                self.denied(f, reason)
        self.denied(Provider("unknown"), "greeting_dialog_changed")

    def test_message_at_claim_second_from_recipient_or_colleague(self):
        for author in (41, 42):
            f = Provider()
            f.users = [UnreadText(id=42, connector=False, extranet=False, bot=False, departments=[2])]
            f.pages[None][0] = message(11, author, "2026-11-17T00:00:00Z")
            self.denied(f, "greeting_staff_message_seen")
            self.assertNotIn("imopenlines.v2.Session.list", f.calls)

    def test_unknown_activity_or_external_connector_is_not_a_quiet_dialog(self):
        for row in (message(11, 50), message(11, 0, codes=()), message(11, 0, codes=("UNKNOWN",))):
            f = Provider("thoth_waba")
            f.users = [UnreadText(id=50, connector=True, extranet=True, departments=[])]
            f.pages[None][0] = row
            self.denied(f, "greeting_activity_unclassified")

    def test_multiple_pages_must_reach_preclaim_boundary(self):
        f = Provider()
        f.pages = {None: [message(11)], "11": [message(10)], "10": [message(9, 41, "2026-11-16T23:59:59Z")]}
        self.assertIsNone(self.guard(f))
        self.assertEqual(f.calls.count("im.dialog.messages.get"), 3)
        f.pages["10"][0] = message(9)
        self.denied(f, "greeting_history_unverified")
        f.pages["11"] = []
        self.denied(f, "greeting_history_unverified")
        f.pages["11"] = [message(11)]
        self.denied(f, "greeting_history_unverified")

    def test_no_gap_or_stale_page_or_other_chat_or_malformed_timestamp(self):
        for change in ({"id": 9}, {"chat_id": 34}, {"date": "yesterday"}):
            f = Provider()
            f.pages[None][0].update(change)
            self.denied(f, "greeting_history_unverified")
        f = Provider()
        f.pages[None] = [message(11), message(11)]
        self.denied(f, "greeting_history_unverified")
        f = Provider()
        f.pages[None][1]["date"] = "2026-11-17T00:00:03Z"
        self.denied(f, "greeting_history_unverified")

    def test_final_tail_owner_and_session_fence(self):
        for changed, error in [
            ({"last_message_id": 12}, "greeting_history_unverified"),
            ({"owner": 42}, "chat_owned_by_another_manager"),
            ({"entity_data_1": "Y|DEAL|23|N|N|40"}, "greeting_dialog_changed"),
        ]:
            f = Provider()
            f.late = changed
            self.denied(f, error)

    def test_read_error_or_timeout_is_sanitized(self):
        f = Provider()
        with patch.object(f, "read", side_effect=RuntimeError("sensitive provider response")):
            self.denied(f, "greeting_guard_unavailable")
        with patch("greeting_dialog_guard.time.monotonic", side_effect=[100, 100, 110]):
            self.denied(f, "greeting_guard_unavailable")
        self.denied(f, "greeting_history_unverified", claimed_at="2099-01-01T00:00:00Z")
        self.denied(f, "greeting_history_unverified", claimed_at="2026-01-01")
