"""The current OpenLine request must not become an old CRM comment."""

from contextlib import contextmanager
from unittest.mock import patch
import unittest

import test_app as fixtures

app = fixtures.app


class TestLatestChatPreview(fixtures.TemporaryStateTestCase):
    def setUp(self):
        super().setUp()
        network = patch.object(app.urllib.request, "urlopen", fixtures._network_is_forbidden)
        network.start()
        self.addCleanup(network.stop)
        portal = patch.object(app, "load_env", return_value="https://test-fake.bitrix24.test/rest/1/not-a-secret/")
        portal.start()
        self.addCleanup(portal.stop)

    def message(self, text, *, sender="123", minute=1):
        return {"senderid": sender, "text": text,
                "date": f"2026-10-10T10:{minute:02d}:00+06:00"}

    @contextmanager
    def sources(self, history, *, with_session=True):
        comments = [{"COMMENT": "Раньше хотели Турцию и Анталью",
                     "CREATED": "2026-10-09T10:00:00+06:00"}]
        activities = [{"ID": "100", "PROVIDER_ID": "IMOPENLINES_SESSION",
                       "ASSOCIATED_ENTITY_ID": "321",
                       "CREATED": "2026-10-10T10:00:00+06:00"}] if with_session else []

        def lists(method, *_args, **_kwargs):
            if method == "crm.timeline.comment.list":
                return comments
            self.assertEqual(method, "crm.activity.list")
            return activities

        def read(method, params, **_kwargs):
            if method == "batch":
                return {"result": {"timeline": comments, "activity": activities},
                        "result_error": [], "result_next": []}
            self.assertEqual(method, "imopenlines.session.history.get")
            self.assertEqual(params, {"SESSION_ID": "321"})
            if isinstance(history, Exception):
                raise history
            return history

        with patch.object(app, "bitrix_list_all", side_effect=lists), patch.object(app, "bitrix_call", side_effect=read) as remote:
            yield remote

    def test_short_new_destination_replaces_old_crm_comment(self):
        for batched in (False, True):
            for text, direction in (("Египет", "Египет"), ("ОАЭ", "ОАЭ"),
                                    ("Дубай", "ОАЭ"), ("Бали", "Индонезия")):
                with self.subTest(text=text, batched=batched), patch.object(app, "BITRIX_SOURCE_BATCH_ENABLED", batched), self.sources({"message": {"2": self.message(text)}}):
                    result = app.get_deal_messages("100")
                    self.assertEqual(result["useful"], [text])
                    self.assertEqual(app.classify(result["useful"])["direction"], direction)

    def test_short_new_destination_replaces_older_message_in_same_session(self):
        history = {"message": {
            "1": self.message("Раньше хотели Турцию и Анталью", minute=0),
            "2": self.message("Египет"),
        }}
        with self.sources(history):
            self.assertEqual(app.get_deal_messages("100")["useful"], ["Египет"])

    def test_short_cyrillic_and_kyrgyz_replies_remain_visible(self):
        for text in ("Да", "Үй-бүлө", "Өө"):
            with self.subTest(text=text), self.sources({"message": {"2": self.message(text)}}):
                result = app.get_deal_messages("100")
                self.assertEqual(result["useful"], [text])
                self.assertEqual(app.classify(result["useful"])["direction"], "Не определено")

    def test_short_followup_keeps_preceding_customer_destination(self):
        history = {"message": {"1": self.message("Нужен Египет", minute=0),
                               "2": self.message("Үй-бүлө")}}
        with self.sources(history):
            result = app.get_deal_messages("100")
        self.assertEqual(result["useful"], ["Үй-бүлө", "Нужен Египет"])
        self.assertEqual(app.classify(result["useful"])["direction"], "Египет")

    def test_empty_or_only_service_history_never_revives_old_crm_comment(self):
        for messages in ({}, [], {"1": self.message("👋")},
                         {"1": self.message("Обращение направлено")},
                         {"1": self.message("Нужен Египет", sender="0")},
                         {"1": self.message("12345")}):
            with self.subTest(messages=messages), self.sources({"message": messages}):
                result = app.get_deal_messages("100")
                self.assertEqual(result["useful"], [])
                self.assertEqual(result["openlineSessionIds"], ["321"])

    def test_crm_fallback_remains_when_no_openline_session_exists(self):
        with self.sources(None, with_session=False) as remote:
            result = app.get_deal_messages("100")
        self.assertEqual(result["useful"], ["Раньше хотели Турцию и Анталью"])
        remote.assert_not_called()

    def test_unavailable_or_malformed_history_is_not_confirmed_empty(self):
        for history in (TimeoutError("private provider detail"), None, False, [], {},
                        {"message": None}, {"message": False}, {"message": ""}):
            with self.subTest(history=history), self.sources(history):
                with self.assertRaises((RuntimeError, TimeoutError)):
                    app.get_deal_messages("100")

    def test_confirmed_empty_chat_still_offers_oldest_unclassified_deal(self):
        header = {"ID": "100", "STAGE_ID": next(iter(app.SOURCE_STAGES)),
                  "DATE_CREATE": "2026-10-10T10:00:00+06:00", "DATE_MODIFY": "v1"}
        manager = {"id": "42", "active": True, "intranet": True, "competencies": []}
        with self.sources({"message": {}}), patch.object(app, "get_manager_profile", return_value=manager), patch.object(app, "check_manager_access", return_value={"ok": True, "rule": {}}), patch.object(app, "list_allowed_deal_headers", return_value=[header]):
            result = app._get_next_deal_for_manager("42")
        self.assertIsNotNone(result["deal"], result)
        self.assertEqual(result["deal"]["id"], "100")
        self.assertEqual(result["deal"]["messages"], [])
        self.assertEqual(result["deal"]["classification"]["direction"], "Не определено")
        self.assertEqual(self.store.list_claims(), [])

    def test_confirmed_employee_reply_does_not_override_customer_destination(self):
        history = {"message": {"1": self.message("Теперь нужен Египет", minute=0),
                               "2": self.message("Могу предложить Турцию и Анталью", sender="42")},
                   "users": {"123": {"id": "123", "connector": True, "extranet": False, "departments": []},
                             "42": {"id": "42", "connector": False, "extranet": False, "departments": [187]}}}
        with self.sources(history):
            result = app.get_deal_messages("100")
        self.assertEqual(result["useful"], ["Теперь нужен Египет"])
        self.assertEqual(app.classify(result["useful"])["direction"], "Египет")
        history["message"].pop("1")
        with self.sources(history):
            self.assertEqual(app.get_deal_messages("100")["useful"], [])

    def test_unknown_or_contradictory_sender_profile_does_not_hide_message(self):
        for profile in ({}, {"id": "999", "connector": False, "extranet": False, "departments": [187]},
                        {"id": "123", "connector": True, "extranet": False, "departments": [187]}):
            history = {"message": {"1": self.message("Нужен Египет")}, "users": {"123": profile}}
            with self.subTest(profile=profile), self.sources(history):
                self.assertEqual(app.get_deal_messages("100")["useful"], ["Нужен Египет"])


if __name__ == "__main__":
    unittest.main()
