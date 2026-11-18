"""Complete synthetic inventories including queue and unassigned deals."""
import copy
import unittest

from returning_deal_policy import ITEM_FIELDS, open_deals_for_contacts


class ReturningDealPolicyTest(unittest.TestCase):
    def setUp(self):
        self.deals = {
            "101": {"ID": "101", "CATEGORY_ID": "0", "STAGE_ID": "NEW", "STAGE_SEMANTIC_ID": "P",
                    "CLOSED": "N", "ASSIGNED_BY_ID": "7", "DATE_CREATE": "2026-11-18T08:00:00+06:00",
                    "DATE_MODIFY": "2026-11-18T09:00:00+06:00", "ORIGINATOR_ID": "returning-client",
                    "ORIGIN_ID": "source-observation-101"},
        }
        self.bindings = {"101": ["10", "11"]}
        self.calls = []
        self.list_response, self.before_get = None, None

    def item(self, deal):
        return {**{source: deal[target] for source, target in ITEM_FIELDS.items() if target in deal},
                "contactIds": self.bindings[deal["ID"]]}

    def call(self, method, params, timeout=8):
        self.calls.append((method, copy.deepcopy(params)))
        if method == "crm.item.list":
            self.assertEqual(params["filter[stageSemanticId]"], "P")
            self.assertEqual(params["filter[closed]"], "N")
            self.assertNotIn("filter[assignedById]", params)
            if self.list_response:
                return self.list_response(params)
            rows = [self.item(self.deals[did]) for did in sorted(self.deals, key=int)
                    if params["filter[contactIds]"] in self.bindings[did]]
            return {"items": copy.deepcopy(rows[params["start"]:params["start"] + 50])}
        if method == "crm.deal.get":
            if self.before_get:
                self.before_get(params["id"])
            return copy.deepcopy(self.deals[params["id"]])
        if method == "crm.deal.contact.items.get":
            return [{"CONTACT_ID": cid} for cid in self.bindings[params["id"]]]
        self.fail("unexpected endpoint: " + method)

    def add_deal(self, did, **changes):
        self.deals[did] = {**self.deals["101"], "ID": did, **changes}
        self.bindings[did] = ["10"]

    def test_all_open_stages_and_unassigned_deals_block_duplicate_creation(self):
        self.add_deal("102", STAGE_ID="UC_ZJ55BR", ASSIGNED_BY_ID="0")
        self.add_deal("103", STAGE_ID="C7:UC_PUCAAQ", ASSIGNED_BY_ID=None)
        self.add_deal("104", STAGE_ID="NEW")
        del self.deals["104"]["ASSIGNED_BY_ID"]
        result = open_deals_for_contacts(self.call, ["10"])
        self.assertEqual([row["ID"] for row in result], ["101", "102", "103", "104"])
        self.assertEqual(result[1]["ASSIGNED_BY_ID"], "0")
        self.assertIsNone(result[2]["ASSIGNED_BY_ID"])
        self.assertNotIn("ASSIGNED_BY_ID", result[3])

    def test_secondary_binding_and_union_are_complete_and_unique(self):
        self.add_deal("102")
        result = open_deals_for_contacts(self.call, ["11", "10"])
        self.assertEqual([row["ID"] for row in result], ["101", "102"])
        self.assertEqual(result[0]["CONTACT_IDS"], ["10", "11"])
        gets = [params["id"] for method, params in self.calls if method == "crm.deal.get"]
        self.assertEqual(gets, ["101", "102"])
        self.assertEqual(result[0]["ORIGINATOR_ID"], "returning-client")
        self.assertEqual(result[0]["ORIGIN_ID"], "source-observation-101")

    def test_no_open_deals_has_explicit_complete_empty_page_for_every_contact(self):
        self.deals = {}
        self.assertEqual(open_deals_for_contacts(self.call, ["10", "11"]), [])
        self.assertEqual(len(self.calls), 2)

    def test_fifty_rows_require_a_next_empty_page(self):
        for did in range(102, 151):
            self.add_deal(str(did))
        self.assertEqual(len(open_deals_for_contacts(self.call, ["10"])), 50)
        starts = [params["start"] for method, params in self.calls if method == "crm.item.list"]
        self.assertEqual(starts, [0, 50])

    def test_exact_limit_is_not_proof_of_complete_inventory(self):
        def full(params):
            return {"items": [{**self.item(self.deals["101"]), "id": str(1000 + params["start"] + index)}
                              for index in range(50)]}
        self.list_response = full
        with self.assertRaisesRegex(ValueError, "inventory_limit"):
            open_deals_for_contacts(self.call, ["10"])
        self.assertFalse(any(method == "crm.deal.get" for method, _ in self.calls))

    def test_repeated_page_is_not_accepted_as_complete(self):
        def repeated(_params):
            return {"items": [{**self.item(self.deals["101"]), "id": str(1000 + index)} for index in range(50)]}
        self.list_response = repeated
        with self.assertRaisesRegex(ValueError, "page_repeated"):
            open_deals_for_contacts(self.call, ["10"])

    def test_invalid_or_misfiltered_rows_fail_closed(self):
        base = self.item(self.deals["101"])
        for changes in ({"contactIds": ["11"]}, {"contactIds": ["10", "10"]}, {"contactIds": []},
                        {"id": "0"}, {"categoryId": None}, {"stageId": ""}, {"stageSemanticId": "F"},
                        {"closed": "Y"}, {"createdTime": "2026-11-18T08:00:00"}):
            with self.subTest(changes=changes):
                self.list_response = lambda _params: {"items": [{**base, **changes}]}
                with self.assertRaises(ValueError):
                    open_deals_for_contacts(self.call, ["10"])

    def test_missing_page_shape_is_not_empty_inventory(self):
        for value in ({}, None, [], {"items": None}):
            with self.subTest(value=value):
                self.list_response = lambda _params: value
                with self.assertRaisesRegex(ValueError, "page_unavailable"):
                    open_deals_for_contacts(self.call, ["10"])

    def test_binding_changed_between_list_and_fresh_read_fails_closed(self):
        self.before_get = lambda _did: self.bindings.update({"101": ["10"]})
        with self.assertRaisesRegex(ValueError, "actual_bindings_changed"):
            open_deals_for_contacts(self.call, ["10"])

    def test_stage_change_cannot_turn_inventory_into_permission_to_create(self):
        self.before_get = lambda _did: self.deals["101"].update(STAGE_ID="UC_PUCAAQ")
        with self.assertRaisesRegex(ValueError, "changed_after_list"):
            open_deals_for_contacts(self.call, ["10"])

    def test_owner_change_is_returned_fresh_without_filtering_out_deal(self):
        self.before_get = lambda _did: self.deals["101"].update(ASSIGNED_BY_ID="0")
        result = open_deals_for_contacts(self.call, ["10"])
        self.assertEqual(result[0]["ASSIGNED_BY_ID"], "0")

    def test_closed_after_list_fails_closed(self):
        self.before_get = lambda _did: self.deals["101"].update(CLOSED="Y", STAGE_SEMANTIC_ID="F")
        with self.assertRaisesRegex(ValueError, "schema_unavailable"):
            open_deals_for_contacts(self.call, ["10"])

    def test_full_snapshot_requires_modification_timestamp(self):
        self.before_get = lambda _did: self.deals["101"].pop("DATE_MODIFY")
        with self.assertRaisesRegex(ValueError, "date_unavailable"):
            open_deals_for_contacts(self.call, ["10"])

    def test_contact_input_is_validated_before_any_read(self):
        for contacts in ([], "10", ["0"], ["10", "10"], [None]):
            with self.subTest(contacts=contacts), self.assertRaises(ValueError):
                open_deals_for_contacts(self.call, contacts)
        self.assertEqual(self.calls, [])

    def test_failure_on_later_contact_does_not_return_partial_inventory(self):
        def fail(params):
            if params["filter[contactIds]"] == "11":
                raise TimeoutError("native read unavailable")
            return {"items": [self.item(self.deals["101"])]}
        self.list_response = fail
        with self.assertRaises(TimeoutError):
            open_deals_for_contacts(self.call, ["10", "11"])
        self.assertFalse(any(method == "crm.deal.get" for method, _ in self.calls))


if __name__ == "__main__":
    unittest.main()
