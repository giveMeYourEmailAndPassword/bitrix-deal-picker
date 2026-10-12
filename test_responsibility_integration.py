"""Native acceptance and canonical claims share the real durable contact queue."""
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import test_native_acceptance_sync as fixtures
from contact_owner_sync import ContactOwnerSync
from test_app import app


class ResponsibilityIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.NativeAcceptanceTest()
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        f = self.f
        read = f.call
        self.writes = []
        self.timeout = False

        def call(method, params, timeout=None):
            if method in {"crm.deal.update", "crm.contact.update"}:
                self.writes.append((method, dict(params)))
                if self.timeout:
                    raise TimeoutError()
                row = f.deal if method == "crm.deal.update" else f.contact
                row["ASSIGNED_BY_ID"] = params["fields[ASSIGNED_BY_ID]"]
                if method == "crm.deal.update":
                    row["DATE_MODIFY"] = fixtures.iso(f.now)
                return True
            return read(method, params, timeout)

        real_datetime = f.store._outbox_datetime
        f.store._outbox_datetime = lambda value=None: real_datetime(value if value is not None else datetime.fromtimestamp(f.now, timezone.utc))
        f.store._now_iso = lambda: f.store._outbox_time(f.store._outbox_datetime())
        self.contacts = ContactOwnerSync(f.store, call, "UF_CRM_CLAIM", locks=f.locks)
        f.service.contacts = self.contacts
        f.service.call = call

    def test_native_acceptance_updates_exact_open_deal_then_its_primary_contact_once(self):
        f = self.f
        f.observe()
        f.service.process()
        self.contacts.process()
        self.assertEqual((f.deal["ASSIGNED_BY_ID"], f.contact["ASSIGNED_BY_ID"]), ("7", "7"))
        self.assertEqual(f.deal["STAGE_ID"], "NEW")
        self.assertEqual([method for method, _ in self.writes], ["crm.deal.update", "crm.contact.update"])
        f.now += 100
        f.service.process()
        self.contacts.process()
        self.assertEqual(len(self.writes), 2)

    def test_later_canonical_claim_on_same_contact_stops_delayed_native_assignment(self):
        f = self.f
        f.observe()
        f.now += 1
        f.store.begin_claim_operation("102", "44", operation_key="claim:new", request={
            "claimMarker": "new-marker", "contactOwnerContext": {
                "contactId": "10", "previousOwner": "9", "snapshotStartedAt": f.store._now_iso()}})
        f.store.finalize_claim_operation("claim:new", claim={"dealId": "102", "managerId": "44"}, expected_claim_marker="new-marker")
        f.service.process()
        self.assertEqual(f.jobs()[0]["status"], "conflict")
        self.assertEqual(f.jobs()[0]["error_code"], "newer_contact_claim")
        self.assertEqual(self.writes, [])

    def test_unknown_native_deal_write_blocks_canonical_claim_without_remote_mutation(self):
        f = self.f
        f.observe()
        self.timeout = True
        f.service.process()
        self.assertEqual(f.jobs()[0]["status"], "pending")
        with (patch.object(app, "STATE_STORE", f.store), patch.object(app, "DRY_RUN", False),
              patch.object(app, "claim_marker_field_valid", return_value=True),
              patch.object(app, "decode_selection_token", return_value={"version": "v1", "rejectionAfter": 0}),
              patch.object(app, "bitrix_call") as provider):
            response = app.preview_claim("101", "42", selection_token="test", send_greeting=False)
        self.assertEqual(response["code"], "native_owner_update_pending")
        self.assertEqual(response["_httpStatus"], 409)
        provider.assert_not_called()
        self.assertEqual(len(self.writes), 1)


if __name__ == "__main__":
    unittest.main()
