"""Read-only ownership policy: complete lists, current bindings and real dates."""

import unittest

from contact_owner_policy import ITEM_FIELDS, contact_ids, eligible_deal, latest_eligible_deal


def deal(identity="100", owner="42", created="2026-11-18T10:00:00+06:00", **changes):
    return {"ID": identity, "ASSIGNED_BY_ID": owner, "CONTACT_ID": "7",
            "STAGE_ID": "NEW", "STAGE_SEMANTIC_ID": "P", "CLOSED": "N",
            "DATE_CREATE": created, **changes}


class PolicyTests(unittest.TestCase):
    def api(self, pages, bindings=None, fresh=None):
        calls = []

        def call(method, params, timeout=None):
            calls.append((method, params, timeout))
            self.assertEqual(timeout, 8)
            if method == "crm.item.list":
                self.assertEqual(params["entityTypeId"], 2)
                self.assertEqual(params["filter[contactIds]"], "7")
                self.assertEqual(params["filter[stageSemanticId]"], "P")
                self.assertEqual(params["filter[closed]"], "N")
                rows = pages[params["start"] // 50]
                if not isinstance(rows, list):
                    return rows
                return {"items": [({**{source: row[target] for source, target in ITEM_FIELDS.items() if target in row},
                                      "contactIds": row.get("CONTACT_IDS", ["7"])})
                                   if isinstance(row, dict) else row for row in rows]}
            if method == "crm.deal.get":
                if fresh is not None:
                    return fresh
                return next(dict(row) for page in pages for row in page if str(row["ID"]) == params["id"])
            self.assertEqual(method, "crm.deal.contact.items.get")
            return ([{"CONTACT_ID": "7"}] if bindings is None
                    else bindings[params["id"]])

        return call, calls

    def test_only_explicit_open_in_progress_with_valid_owner_qualifies(self):
        self.assertTrue(eligible_deal(deal()))
        for change in [{"CLOSED": "Y"}, {"CLOSED": False}, {"CLOSED": None},
                       {"STAGE_SEMANTIC_ID": "F"}, {"STAGE_SEMANTIC_ID": "S"},
                       {"STAGE_SEMANTIC_ID": None}, {"ID": "0"}, {"ID": True},
                       {"ASSIGNED_BY_ID": "0"}, {"ASSIGNED_BY_ID": None},
                       {"STAGE_ID": ""}]:
            with self.subTest(change=change):
                self.assertFalse(eligible_deal(deal(**change)))
        self.assertFalse(eligible_deal(None))

    def test_excludes_both_stages_in_any_pipeline(self):
        for stage in ["UC_ZJ55BR", "UC_PUCAAQ", "C15:UC_ZJ55BR", "C2:UC_PUCAAQ"]:
            with self.subTest(stage=stage):
                self.assertFalse(eligible_deal(deal(STAGE_ID=stage)))
        self.assertTrue(eligible_deal(deal(STAGE_ID="C15:NEW")))

    def test_bindings_use_actual_rows_not_legacy_primary_contact(self):
        calls = []

        def call(method, params, timeout=None):
            calls.append((method, params))
            return [{"CONTACT_ID": 8}, {"CONTACT_ID": "7"}]

        self.assertEqual(contact_ids(call, deal(CONTACT_ID="99")), ["8", "7"])
        self.assertEqual(calls, [("crm.deal.contact.items.get", {"id": "100"})])

    def test_empty_bindings_are_valid_but_malformed_or_duplicate_bindings_raise(self):
        self.assertEqual(contact_ids(lambda *a, **k: [], deal()), [])
        for rows in [None, {}, [None], [{}], [{"CONTACT_ID": 0}],
                     [{"CONTACT_ID": "7"}, {"CONTACT_ID": 7}]]:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                contact_ids(lambda *a, **k: rows, deal())

    def test_actual_date_beats_id_and_order_returned_by_api(self):
        older = deal("900", "20", "2026-11-17T23:00:00+06:00")
        newer = deal("100", "42", "2026-11-18T10:00:00+06:00")
        call, _ = self.api([[older, newer]])
        winner = latest_eligible_deal(call, "7")
        self.assertEqual((winner["ID"], winner["ASSIGNED_BY_ID"], winner["DATE_CREATE"]),
                         (newer["ID"], newer["ASSIGNED_BY_ID"], newer["DATE_CREATE"]))

    def test_timezone_aware_dates_and_equal_date_id_tiebreak(self):
        # These all represent the same instant; greatest ID wins.
        rows = [deal("99", created="2026-11-18T10:00:00+06:00"),
                deal("101", created="2026-11-18T04:00:00Z"),
                deal("100", created="2026-11-18T05:00:00+01:00")]
        call, _ = self.api([rows])
        self.assertEqual(latest_eligible_deal(call, "7")["ID"], "101")

    def test_newer_excluded_and_closed_deals_cannot_control_contact(self):
        rows = [deal("200", STAGE_ID="C15:UC_PUCAAQ"),
                deal("201", STAGE_ID="UC_ZJ55BR"),
                deal("202", CLOSED="Y", STAGE_SEMANTIC_ID="S"),
                deal("100", created="2026-11-17T04:00:00Z")]
        call, _ = self.api([rows])
        self.assertEqual(latest_eligible_deal(call, "7")["ID"], "100")

    def test_no_eligible_deals_returns_none_without_binding_reads(self):
        for rows in [[], [deal(STAGE_ID="UC_ZJ55BR")]]:
            call, calls = self.api([rows])
            self.assertIsNone(latest_eligible_deal(call, "7"))
            self.assertEqual([c[0] for c in calls], ["crm.item.list"])

    def test_pagination_collects_full_set_before_choosing(self):
        first = [deal(str(i), created="2026-11-17T04:00:00Z") for i in range(1, 51)]
        second = [deal("200", owner="43")]
        call, calls = self.api([first, second])
        self.assertEqual(latest_eligible_deal(call, "7")["ID"], "200")
        self.assertEqual([c[1]["start"] for c in calls if c[0] == "crm.item.list"], [0, 50])

    def test_full_limit_is_uncertain_even_if_first_row_would_win(self):
        pages = [[deal(str(i + 1 + page * 50)) for i in range(50)] for page in range(20)]
        call, calls = self.api(pages)
        with self.assertRaisesRegex(ValueError, "contact_owner_deal_limit"):
            latest_eligible_deal(call, "7")
        self.assertEqual(len(calls), 20)
        self.assertTrue(all(c[0] == "crm.item.list" for c in calls))

    def test_malformed_page_and_repeated_deal_never_choose_fallback(self):
        first = [deal(str(i)) for i in range(1, 51)]
        for pages in [[None], [{"error": "unavailable"}], [[deal()] * 51],
                      [first, [deal("1")]], [[deal(), deal()]]]:
            with self.subTest(pages=pages):
                call, _ = self.api(pages)
                with self.assertRaises(ValueError):
                    latest_eligible_deal(call, "7")

    def test_invalid_newest_date_or_schema_cannot_select_an_older_deal(self):
        for change in [{"DATE_CREATE": "2026-11-18T10:00:00"},
                       {"DATE_CREATE": "not-a-date"}, {"DATE_CREATE": None},
                       {"ASSIGNED_BY_ID": "0"}, {"STAGE_ID": None}]:
            call, _ = self.api([[deal("200", **change), deal("100")]])
            with self.subTest(change=change), self.assertRaises(ValueError):
                latest_eligible_deal(call, "7")

    def test_unlinked_winner_raises_instead_of_using_older_deal(self):
        call, _ = self.api([[deal("100"), deal("200")]], {"200": [{"CONTACT_ID": "99"}]})
        with self.assertRaisesRegex(ValueError, "contact_owner_winner_unlinked"):
            latest_eligible_deal(call, "7")

    def test_secondary_contact_can_control_latest_deal_and_is_checked_twice(self):
        call, calls = self.api([[deal("200", CONTACT_ID="99", CONTACT_IDS=["99", "7"])]],
                               {"200": [{"CONTACT_ID": "99"}, {"CONTACT_ID": "7"}]})
        winner = latest_eligible_deal(call, "7")
        self.assertEqual(winner["CONTACT_ID"], "99")
        self.assertEqual(winner["CONTACT_IDS"], ["99", "7"])
        self.assertEqual(calls[-1][0], "crm.deal.contact.items.get")

    def test_universal_binding_filter_must_match_and_be_complete(self):
        for ids in [None, [], ["99"], ["7", 7], ["7", 0]]:
            with self.subTest(ids=ids):
                call, _ = self.api([[deal(CONTACT_IDS=ids)]])
                with self.assertRaisesRegex(ValueError, "contact_owner_deal_bindings_invalid"):
                    latest_eligible_deal(call, "7")

    def test_returns_full_fresh_deal_including_dynamic_claim_marker(self):
        full = deal(UF_TEST_MARKER="claim-123", DATE_MODIFY="2026-11-18T11:00:00+06:00")
        call, _ = self.api([[deal()]], fresh=full)
        winner = latest_eligible_deal(call, "7")
        self.assertEqual(winner["UF_TEST_MARKER"], "claim-123")
        self.assertEqual(winner["DATE_MODIFY"], "2026-11-18T11:00:00+06:00")

    def test_winner_changed_between_list_and_get_stops_without_fallback(self):
        for change in [{"ID": "999"}, {"ASSIGNED_BY_ID": "43"},
                       {"DATE_CREATE": "2026-11-18T10:01:00+06:00"},
                       {"STAGE_ID": "EXECUTING"}, {"STAGE_ID": "UC_PUCAAQ"},
                       {"CLOSED": "Y", "STAGE_SEMANTIC_ID": "F"}]:
            with self.subTest(change=change):
                call, calls = self.api([[deal()]], fresh=deal(**change))
                with self.assertRaisesRegex(ValueError, "contact_owner_winner_changed"):
                    latest_eligible_deal(call, "7")
                self.assertEqual([c[0] for c in calls], ["crm.item.list", "crm.deal.get"])

    def test_api_failure_is_not_empty_result(self):
        def call(*args, **kwargs):
            raise TimeoutError("read failed")
        with self.assertRaises(TimeoutError):
            latest_eligible_deal(call, "7")

    def test_invalid_input_does_not_call_api(self):
        def call(*args, **kwargs):
            self.fail("invalid identity should be rejected before API")
        for cid in ["", "0", None, True, "01", "7.0"]:
            with self.subTest(cid=cid), self.assertRaises(ValueError):
                latest_eligible_deal(call, cid)
        with self.assertRaises(ValueError):
            contact_ids(call, {})


if __name__ == "__main__":
    unittest.main()
