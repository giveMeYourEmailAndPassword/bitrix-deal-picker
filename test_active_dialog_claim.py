"""Regression: a native Bitrix acceptance must fence the independent picker."""
from unittest.mock import patch
import unittest
import test_app as fixtures

app = fixtures.app


class TestActiveDialogClaim(fixtures.ClaimWorkflowTestCase):
    def setUp(self):
        fixtures.ClaimWorkflowTestCase.setUp(self)
        self._chat_guard_patch.stop()  # Exercise the real remote ownership reader.
        network = patch.object(app.urllib.request, "urlopen", fixtures._network_is_forbidden)
        network.start()
        self.addCleanup(network.stop)

    def portal(self, owners=("33",), *, fail=False):
        reads = {"ownership": 0, "updated": False}

        def call(method, params=None, timeout=None):
            if method == "crm.deal.get":
                return self.claimed_deal() if reads["updated"] else self.source_deal()
            if method == "crm.deal.update":
                reads["updated"] = True
                return True
            if method == "imopenlines.crm.chat.get":
                if fail:
                    raise TimeoutError("portal unavailable")
                index = min(reads["ownership"], len(owners) - 1)
                reads["owner"] = owners[index]
                reads["ownership"] += 1
                return [{"CHAT_ID": "570831"}] if reads["owner"] else []
            if method == "imopenlines.dialog.get":
                return {"id": 570831, "type": "lines", "entity_type": "LINES",
                        "entity_data_2": f"CONTACT|151785|DEAL|{params.get('dealId', self.deal_id)}",
                        "owner": reads["owner"]}
            if method == "im.user.get":
                return {"id": int(reads["owner"]), "bot": reads["owner"] == "262867",
                        "connector": False, "extranet": False,
                        "departments": [] if reads["owner"] == "262867" else [187]}
            raise AssertionError(f"unexpected method {method}")
        return call, reads

    def assert_no_send(self, remote):
        self.assertFalse(any(x.args[0] in {
            "im.message.add", "imopenlines.crm.message.add", "imopenlines.operator.answer",
            "imopenlines.crm.chat.user.add", "imopenlines.operator.transfer",
        } for x in remote.call_args_list))

    def test_katya_accepted_native_chat_while_deal_still_waits_for_specialist(self):
        fake, state = self.portal()
        with self.common_claim_context(), patch.object(app, "bitrix_call", side_effect=fake) as remote:
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token())
        self.assertEqual(result["code"], "chat_owned_by_another_manager")
        self.assertEqual(result["_httpStatus"], 409)
        self.assertTrue(result["selectionStale"])
        self.assertFalse(state["updated"])
        self.assertIsNone(self.store.get_claim_operation(self.operation_key()))
        self.assertEqual(self.store.list_claims(), [])
        self.assertIsNone(self.store.get_greeting_outbox(self.operation_key()))
        self.assert_no_send(remote)

    def test_native_acceptance_between_claim_checks_without_crm_version_change(self):
        fake, state = self.portal(owners=(None, "33"))
        with self.common_claim_context(), patch.object(app, "bitrix_call", side_effect=fake) as remote:
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token())
        self.assertEqual(result["code"], "chat_owned_by_another_manager")
        self.assertEqual(state["ownership"], 2)
        self.assertFalse(state["updated"])
        operation = self.store.get_claim_operation(self.operation_key())
        self.assertEqual(operation["status"], "failed")
        self.assertEqual(operation["result"], {"remoteUpdated": False})
        self.assertEqual(self.store.list_claims(), [])
        self.assertIsNone(self.store.get_greeting_outbox(self.operation_key()))
        self.assert_no_send(remote)

    def test_lookup_failure_does_not_assign_or_mark_selection_stale(self):
        fake, state = self.portal(fail=True)
        with self.common_claim_context(), patch.object(app, "bitrix_call", side_effect=fake):
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token())
        self.assertEqual(result["_httpStatus"], 503)
        self.assertEqual(result["code"], "chat_ownership_unavailable")
        self.assertFalse(result.get("selectionStale", False))
        self.assertFalse(state["updated"])
        self.assertIsNone(self.store.get_claim_operation(self.operation_key()))

    def test_free_bot_and_own_dialogs_remain_claimable(self):
        for owner in (None, "262867", self.manager_id):
            with self.subTest(owner=owner):
                fake, state = self.portal(owners=(owner,))
                with self.common_claim_context(dry_run=True), patch.object(app, "bitrix_call", side_effect=fake):
                    result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token())
                self.assertTrue(result["ok"])
                self.assertTrue(result["dryRun"])
                self.assertFalse(state["updated"])

    def test_free_dialog_claim_rechecks_and_creates_one_event(self):
        fake, state = self.portal(owners=(None,))
        with self.common_claim_context(), patch.object(app, "bitrix_call", side_effect=fake):
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token())
        self.assertTrue(result["ok"])
        self.assertTrue(state["updated"])
        self.assertEqual(state["ownership"], 2)
        self.assertEqual(len(self.store.list_claims()), 1)

    def test_dry_run_also_rejects_foreign_owner(self):
        fake, state = self.portal()
        with self.common_claim_context(dry_run=True), patch.object(app, "bitrix_call", side_effect=fake):
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=self.token())
        self.assertFalse(result["ok"])
        self.assertFalse(state["updated"])

    def search(self, owners, *, fail=False):
        headers = [{"ID": i, "DATE_MODIFY": self.version} for i in ("100", "101")]
        deals = {h["ID"]: {"id": h["ID"], "version": self.version, "messages": [],
                           "classification": {"direction": "Не определено"}} for h in headers}
        base, _ = self.portal(fail=fail)
        def call(method, params=None, timeout=None):
            if method == "imopenlines.crm.chat.get":
                if fail:
                    raise TimeoutError()
                return [{"CHAT_ID": "570831"}] if params["CRM_ENTITY"] == "100" else []
            if method == "imopenlines.dialog.get":
                return {"id": 570831, "type": "lines", "entity_type": "LINES",
                        "entity_data_2": "DEAL|100", "owner": owners}
            if method == "im.user.get":
                return {"id": 33, "bot": False, "connector": False, "extranet": False, "departments": [187]}
            return base(method, params, timeout)
        with (self.common_claim_context(),
              patch.object(app, "check_manager_access", return_value={"ok": True, "rule": {}}),
              patch.object(app, "list_allowed_deal_headers", return_value=headers),
              patch.object(app, "iter_analyzed_deal_headers", side_effect=lambda batch: fixtures.analysis_fixture_rows(batch, *((deals, {})))),
              patch.object(app, "bitrix_call", side_effect=call)):
            return app._get_next_deal_for_manager(self.manager_id)

    def test_search_skips_occupied_candidate_and_keeps_free_one(self):
        result = self.search("33")
        self.assertEqual(result["deal"]["id"], "101")
        self.assertTrue(result["deal"]["selectionToken"])

    def test_search_checks_live_ownership_even_with_unchanged_analysis(self):
        self.assertEqual(self.search(self.manager_id)["deal"]["id"], "100")
        self.assertEqual(self.search("33")["deal"]["id"], "101")

    def test_many_occupied_candidates_resume_without_starving_free_leads(self):
        headers = [{"ID": str(i), "DATE_MODIFY": self.version} for i in range(100, 106)]
        def analyze(batch):
            return {h["ID"]: {"id": h["ID"], "version": self.version, "messages": [],
                    "classification": {"direction": "Не определено"}} for h in batch}, {}
        checked = []
        def occupied(deal_id, manager_id, **kwargs):
            checked.append(deal_id)
            return deal_id != "105"
        with (self.common_claim_context(),
              patch.object(app, "check_manager_access", return_value={"ok": True, "rule": {}}),
              patch.object(app, "list_allowed_deal_headers", return_value=headers),
              patch.object(app, "iter_analyzed_deal_headers", side_effect=lambda batch: fixtures.analysis_fixture_rows(batch, *(analyze(batch)))),
              patch.object(app, "claim_chat_occupied", side_effect=occupied)):
            first = app._get_next_deal_for_manager(self.manager_id)
            self.assertIsNone(first["deal"])
            self.assertTrue(first["hasMore"])
            self.assertEqual(checked, ["100", "101", "102"])
            second = app._get_next_deal_for_manager(self.manager_id, first["continuationToken"])
        self.assertEqual(second["deal"]["id"], "105")
        self.assertEqual(checked, ["100", "101", "102", "103", "104", "105"])

    def test_search_unavailable_does_not_offer_unchecked_or_newer_candidate(self):
        result = self.search("33", fail=True)
        self.assertIsNone(result["deal"])
        self.assertEqual(result["_httpStatus"], 503)
        self.assertFalse(result["hasMore"])


class TestActiveDialogGreeting(fixtures.ClaimWorkflowTestCase):
    setUp = TestActiveDialogClaim.setUp
    portal = TestActiveDialogClaim.portal
    # Only borrow fixture builders, not the existing greeting suite's test methods.
    seed_greeting_outbox = fixtures.TestGreetingOutboxWorker.seed_greeting_outbox
    official_history = fixtures.TestGreetingOutboxWorker.official_history
    actor_auth = fixtures.TestGreetingOutboxWorker.actor_auth
    active_manager = fixtures.TestGreetingOutboxWorker.active_manager

    def test_actor_greeting_never_answers_or_sends_into_foreign_accepted_chat(self):
        self.seed_greeting_outbox()
        worker = "guard-test"
        job = self.store.lease_exact_greeting_outbox(self.operation_key(), worker)
        base, _ = self.portal()
        def call(method, params=None, timeout=None):
            if method == "crm.deal.get":
                return self.claimed_deal()
            if method == "imopenlines.session.history.get":
                return self.official_history()
            return base(method, params, timeout)
        with patch.object(app, "bitrix_call", side_effect=call), patch.object(app, "bitrix_call_for_actor") as send:
            result = app.process_actor_greeting_outbox_job(job, worker, self.actor_auth(), self.active_manager())
        self.assertEqual(result["status"], "manual")
        self.assertEqual(result["errorCode"], "chat_owned_by_another_manager")
        send.assert_not_called()

    def test_background_target_does_not_join_foreign_owned_chat(self):
        context = {"openlineSessionIds": ["321"]}
        chat = {"sessionId": "321", "chatId": "570831", "entityType": "LINES",
                "entityData2": "DEAL|100", "textFieldEnabled": True}
        base, _ = self.portal()
        def call(method, params=None, timeout=None):
            if method == "imopenlines.crm.chat.getLastId":
                return "570831"
            return base(method, params, timeout)
        with (patch.object(app, "get_openline_chat_context", return_value=chat),
              patch.object(app, "bitrix_call", side_effect=call) as remote):
            with self.assertRaisesRegex(RuntimeError, "chat_owned_by_another_manager"):
                app.resolve_greeting_target(self.deal_id, self.manager_id, context)
        self.assertNotIn("imopenlines.crm.chat.user.add", [c.args[0] for c in remote.call_args_list])

    def test_busy_greeting_explanation_does_not_prompt_another_intro(self):
        result = app.public_claim_greeting({"status": "manual", "errorCode": "chat_owned_by_another_manager", "text": "Hello"})
        self.assertEqual(result["text"], "")
        self.assertIn("другим менеджером", result["message"])

    def test_background_greeting_catches_acceptance_after_target_discovery(self):
        self.seed_greeting_outbox()
        worker = "guard-test"
        job = self.store.lease_exact_greeting_outbox(self.operation_key(), worker)
        base, _ = self.portal()
        def call(method, params=None, timeout=None):
            return self.claimed_deal() if method == "crm.deal.get" else base(method, params, timeout)
        with (patch.object(app, "bitrix_call", side_effect=call),
              patch.object(app, "get_greeting_manager_profile", return_value=self.active_manager()),
              patch.object(app, "resolve_greeting_target", return_value={"chatId": "570831", "sessionId": "321"}),
              patch.object(app, "send_greeting_message") as send):
            result = app.process_greeting_outbox_job(job, worker)
        self.assertEqual(result["status"], "manual")
        self.assertEqual(result["errorCode"], "chat_owned_by_another_manager")
        send.assert_not_called()


if __name__ == "__main__":
    unittest.main()
