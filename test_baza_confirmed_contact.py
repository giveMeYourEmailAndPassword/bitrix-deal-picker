"""Hermetic Baza acknowledgement tests. No worker thread or network is started."""
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

from contact_owner_policy import ITEM_FIELDS
from contact_owner_sync import ContactOwnerSync
from state_store import StateStore
from test_app import ClaimWorkflowTestCase, app


class TestBazaConfirmedContact(ClaimWorkflowTestCase):
    def setUp(self):
        super().setUp()
        self.deal = dict(self.source_deal(), CONTACT_ID="7", STAGE_SEMANTIC_ID="P", CLOSED="N",
                         CATEGORY_ID="0", DATE_CREATE="2026-06-01T00:00:00+00:00")
        self.contact = {"ID": "7", "ASSIGNED_BY_ID": "9"}
        self.methods = []
        self.hints = queue.Queue(maxsize=1)
        for item in (patch.object(app, "selection_signing_key", return_value=b"synthetic-signing-key"),
                     patch.object(app, "BAZA_CONTACT_HINTS", self.hints),
                     patch.object(app, "CRM_CONTACT_OWNER_SYNC_ENABLED", True),
                     patch.object(app, "bitrix_call", side_effect=self.call)):
            item.start()
            self.addCleanup(item.stop)

    def call(self, method, params=None, timeout=None):
        self.methods.append(method)
        if method == "crm.deal.get":
            return dict(self.deal)
        if method == "crm.contact.get":
            return dict(self.contact)
        if method == "crm.deal.update":
            for key, value in params.items():
                if key.startswith("fields["):
                    self.deal[key[7:-1]] = value
            return True
        if method == "crm.contact.update":
            self.contact["ASSIGNED_BY_ID"] = params["fields[ASSIGNED_BY_ID]"]
            return True
        if method == "crm.deal.contact.items.get":
            return [{"CONTACT_ID": "7"}]
        if method == "crm.item.list":
            return {"items": [{**{k: self.deal[v] for k, v in ITEM_FIELDS.items()}, "contactIds": ["7"]}]}
        self.fail("unexpected provider call " + method)

    def claim(self, manager_id=None):
        manager_id = manager_id or self.manager_id
        return app.baza_picker_action("claim", {
            "bitrixUserId": manager_id, "dealId": self.deal_id, "selectionToken": self.token(manager_id),
        })

    def worker(self, store=None):
        return ContactOwnerSync(store or self.store, self.call, app.BITRIX_CLAIM_MARKER_FIELD,
                                locks=app.CLAIM_LOCKS, enforce_active_policy=True)

    def test_success_after_canonical_event_before_first_contact_write(self):
        with self.common_claim_context():
            result, status = self.claim()
            self.assertEqual((status, result["auditRecorded"], result["contactAssignment"]), (200, True, {"status": "pending"}))
            self.assertEqual(self.methods, ["crm.deal.get", "crm.contact.get", "crm.deal.get", "crm.deal.update", "crm.deal.get"])
            self.assertEqual(self.contact["ASSIGNED_BY_ID"], "9")
            self.assertEqual(self.store.confirmed_claim_time(self.operation_key(), self.deal_id, self.manager_id), result["claimedAt"])
            repeat, status = self.claim()
            self.assertTrue(repeat["idempotentReplay"])
            self.assertEqual(status, 200)
            self.assertEqual(self.hints.qsize(), 1)
            worker = self.worker()
            with patch.object(app, "readiness_state", return_value={"ok": True}):
                app.wait_for_baza_contacts(worker, poll_seconds=.07)
            self.assertEqual(worker.status(self.operation_key()), {"status": "ready"})
            self.assertEqual(self.contact["ASSIGNED_BY_ID"], "42")
            self.assertEqual(self.methods.count("crm.deal.update"), 1)
            self.assertEqual(self.methods.count("crm.contact.update"), 1)
            self.assertEqual(self.methods.count("crm.item.list"), 2)
            self.assertEqual(self.methods.count("crm.deal.contact.items.get"), 2)
            self.assertFalse(worker.process(self.operation_key()))

    def test_missing_hint_survives_restart_without_duplicate_claim(self):
        self.hints.put_nowait("unrelated")
        with self.common_claim_context():
            result, code = self.claim()
        self.assertEqual((code, result["contactAssignment"]["status"]), (200, "pending"))
        self.assertEqual(self.hints.get_nowait(), "unrelated")
        # A fresh worker sees the committed job without any process-local hint.
        worker = self.worker(StateStore(self.store.data_dir))
        self.assertTrue(worker.process())
        self.assertFalse(worker.process())
        self.assertEqual(self.methods.count("crm.contact.update"), 1)

    def test_manual_contact_change_after_http_response_is_preserved(self):
        with self.common_claim_context():
            self.assertEqual(self.claim()[1], 200)
        self.contact["ASSIGNED_BY_ID"] = "77"
        worker = self.worker()
        worker.process(self.operation_key())
        self.assertEqual(worker.status(self.operation_key())["code"], "contact_owner_changed")
        self.assertEqual(self.methods.count("crm.contact.update"), 0)

    def test_replay_with_same_marker_but_lost_owner_or_stage_is_rejected(self):
        with self.common_claim_context():
            self.assertEqual(self.claim()[1], 200)
            for field, wrong in [("ASSIGNED_BY_ID", "55"), ("STAGE_ID", "WON"), (app.BITRIX_CLAIM_MARKER_FIELD, "foreign")]:
                previous = self.deal[field]
                self.deal[field] = wrong
                response, status = self.claim()
                self.assertEqual(status, 409)
                self.assertFalse(response["ok"])
                self.deal[field] = previous
        self.assertEqual(self.methods.count("crm.deal.update"), 1)
        self.assertEqual(self.methods.count("crm.contact.update"), 0)

    def test_legacy_linked_event_without_operation_key_and_missing_audit(self):
        with self.common_claim_context():
            first, _ = self.claim()
            with self.store._transaction(immediate=True) as db:
                db.execute("UPDATE claim_events SET operation_key=NULL WHERE operation_key=?", (self.operation_key(),))
            recovered, status = self.claim()
            self.assertEqual((status, recovered["claimedAt"]), (200, first["claimedAt"]))
            with self.store._transaction(immediate=True) as db:
                db.execute("UPDATE claim_operations SET claim_event_id=NULL WHERE operation_key=?", (self.operation_key(),))
            denied, status = self.claim()
            self.assertEqual((status, denied["code"]), (503, "claim_audit_pending"))
            self.assertFalse(denied["ok"])

    def test_contact_wake_handles_only_ten_named_jobs_without_triggering_scanners(self):
        hints = queue.Queue(maxsize=16)
        for i in range(12):
            hints.put_nowait("synthetic-" + str(i))
        worker = Mock()
        with (patch.object(app, "BAZA_CONTACT_HINTS", hints),
              patch.object(app, "readiness_state", return_value={"ok": True}),
              patch.object(app, "DRY_RUN", False)):
            app.wait_for_baza_contacts(worker, poll_seconds=.03)
        self.assertEqual([c.args for c in worker.process.call_args_list], [("synthetic-" + str(i),) for i in range(10)])
        self.assertEqual(hints.qsize(), 2)

    def test_claim_race_lost_reply_and_restart_deliver_one_greeting_before_contact(self):
        update_started = threading.Event()
        competitor_started = threading.Event()
        chat_active = False
        sent = []
        session_id, chat_id = "321", "1763"

        def provider(method, params=None, timeout=None):
            if method == "crm.deal.update":
                self.call(method, params, timeout)
                update_started.set()
                self.assertTrue(competitor_started.wait(3))
                raise TimeoutError("response lost after Bitrix applied the deal change")
            if method == "imopenlines.session.history.get":
                self.assertEqual(params["SESSION_ID"], session_id)
                return {"sessionId": session_id, "chatId": chat_id, "chat": {
                    "id": chat_id, "entityType": "LINES", "textFieldEnabled": True,
                    "entityData2": "DEAL|" + self.deal_id,
                }}
            if method == "imopenlines.crm.chat.get":
                self.assertEqual(params["ACTIVE_ONLY"], "Y")
                return [{"CHAT_ID": chat_id, "CONNECTOR_ID": "fbinstagramdirect"}] if chat_active else []
            if method == "imopenlines.crm.chat.getLastId":
                return chat_id
            if method == "imopenlines.crm.chat.user.add":
                self.assertEqual(params["CHAT_ID"], chat_id)
                self.assertEqual(params["USER_ID"], self.manager_id)
                return chat_id
            if method == "imopenlines.crm.message.add":
                job = self.store.get_greeting_outbox(self.operation_key())
                self.assertEqual((job["status"], job["sessionId"], job["chatId"]), ("dispatching", session_id, chat_id))
                self.assertEqual((params["USER_ID"], params["CHAT_ID"]), (self.manager_id, chat_id))
                sent.append(params["MESSAGE"])
                return "85851"
            return self.call(method, params, timeout)

        def compete():
            competitor_started.set()
            return self.claim("43")

        snapshot = {self.deal_id: {"version": self.version, "greetingContext": {
            "openlineSessionIds": [session_id], "classification": {"direction": "Турция"},
        }}}
        profile = {"id": self.manager_id, "name": "Тест", "active": True, "intranet": True}
        with (
            self.common_claim_context(), patch.multiple(app, GREETING_AUTO_SEND=True, GREETING_AUTO_SEND_SUPPORTED=True),
            patch.dict(app.DEAL_ANALYSIS_CACHE, snapshot), patch.object(app, "bitrix_call", side_effect=provider),
            patch.object(app, "get_greeting_manager_profile", return_value=profile),
        ):
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(self.claim)
                self.assertTrue(update_started.wait(3))
                other = pool.submit(compete)
                claim, code = first.result(timeout=5)
                declined, denied = other.result(timeout=5)
            self.assertEqual((code, denied), (200, 409))
            self.assertFalse(declined["ok"])
            self.assertEqual(claim["contactAssignment"]["status"], "pending")
            self.assertEqual(claim["greeting"]["status"], "queued")
            self.assertEqual(self.deal["STAGE_ID"], app.TARGET_STAGE)
            self.assertEqual(self.deal["ASSIGNED_BY_ID"], self.manager_id)
            self.assertEqual(self.deal[app.BITRIX_CLAIM_MARKER_FIELD], self.attempt_marker())

            # Reopen the same volume with no process-local wake hints or offer cache.
            new_store = StateStore(self.data_dir, local_timezone=app.LOCAL_TZ)
            with patch.object(app, "STATE_STORE", new_store), patch.object(app, "BAZA_CONTACT_HINTS", queue.Queue()):
                replay, code = self.claim()
                self.assertEqual(code, 200)
                self.assertTrue(replay["idempotentReplay"])
                self.assertEqual(replay["claimedAt"], claim["claimedAt"])
                self.assertEqual(new_store.count_claims(self.manager_id), 1)
                self.assertEqual(len(new_store.list_greeting_outbox()), 1)
                app.process_greeting_outbox_once("before-pin")
                job = new_store.get_greeting_outbox(self.operation_key())
                self.assertEqual((job["status"], job["errorCode"]), ("pending", "chat_activation_pending"))
                self.assertEqual(sent, [])

                chat_active = True
                with new_store._transaction(immediate=True) as db:
                    db.execute("UPDATE greeting_outbox SET next_attempt_at=? WHERE operation_key=?",
                               (new_store._now_iso(), self.operation_key()))
                app.process_greeting_outbox_once("after-pin")
                self.assertEqual(new_store.get_greeting_outbox(self.operation_key())["status"], "sent")
                self.assertEqual(app.process_greeting_outbox_once("repeat")["leased"], 0)
                self.assertEqual(len(sent), 1)
                self.assertEqual(self.contact["ASSIGNED_BY_ID"], "9")
                self.assertTrue(self.worker(new_store).process())
                self.assertEqual(self.contact["ASSIGNED_BY_ID"], self.manager_id)
        self.assertEqual(self.methods.count("crm.deal.update"), 1)
        self.assertEqual(self.methods.count("crm.contact.update"), 1)
