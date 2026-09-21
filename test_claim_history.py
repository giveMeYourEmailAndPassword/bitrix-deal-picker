"""Canonical analytics history: no live network and no operational replay."""

import json
import unittest
from collections import defaultdict, deque
from unittest.mock import patch

from test_app import TemporaryStateTestCase, HandlerHarness, app, _network_is_forbidden
from test_baza_bridge import SECRET, NOW, signed_headers
import baza_bridge


class TestClaimHistory(TemporaryStateTestCase):
    def add_claim(self, deal, when, manager="27337"):
        return self.store.append_claim({
            "managerId": manager, "dealId": str(deal), "timestamp": when,
        })

    def page(self, **kwargs):
        return self.store.claim_history_page(**{
            "start": "2026-09-01", "end": "2026-09-30",
            "as_of": "2026-09-21T10:20:00.000Z", **kwargs,
        })

    def signed_request(self, payload, *, unsigned=False):
        path = "/integrations/baza/v1/claim-history"
        body = json.dumps(payload).encode()
        headers = signed_headers(path, body)
        if unsigned:
            del headers["X-Krugosvet-Signature"]
        handler = HandlerHarness.make("POST", path, body, headers=headers)
        with (
            patch.object(app, "BAZA_PICKER_BRIDGE_SECRET", SECRET),
            patch.object(app, "readiness_state", return_value={"ok": True}),
            patch.object(app, "rate_limit_allowed", return_value=True),
            patch.object(baza_bridge.time, "time", return_value=NOW),
            patch.object(app.urllib.request, "urlopen", _network_is_forbidden),
        ):
            handler.do_POST()
        return handler

    def test_history_includes_claims_before_export_cutoff_without_replay(self):
        self.add_claim(100, "2026-09-01T08:00:00+06:00")
        self.add_claim(101, "2026-09-20T08:00:00+06:00")
        before = self.store.list_outbox()
        with (
            patch.object(app, "BAZA_CLAIM_EXPORT_FROM", "2026-09-16T11:35:21.000+00:00"),
            patch.object(app, "get_manager_profile") as profile,
            patch.object(app, "get_next_deal_for_manager") as allocate,
            patch.object(app, "preview_claim") as claim,
            patch.object(app, "flush_integration_outbox") as deliver,
        ):
            handler = self.signed_request({
                "start": "2026-09-01", "end": "2026-09-30",
                "asOf": "2026-09-21T10:20:00.000Z",
            })
        self.assertEqual(HandlerHarness.status(handler), 200)
        result = HandlerHarness.json(handler)
        self.assertEqual(result["total"], 2)
        self.assertEqual([item["bitrixDealId"] for item in result["items"]], ["100", "101"])
        self.assertEqual(result["historyStartDate"], "2026-09-01")
        self.assertEqual(self.store.list_outbox(), before)
        for operation in (profile, allocate, claim, deliver):
            operation.assert_not_called()

    def test_business_day_and_exact_as_of_instants_are_respected(self):
        self.add_claim(1, "2026-08-31T17:59:59.999999Z")  # Aug 31 Bishkek
        self.add_claim(2, "2026-08-31T18:00:00Z")  # Sep 1 Bishkek
        self.add_claim(3, "2026-09-21T16:20:00.000000+06:00")
        self.add_claim(4, "2026-09-21T16:20:00.000001+06:00")
        self.add_claim(5, "2026-09-30T18:00:00Z")  # Oct 1 Bishkek
        result = self.page()
        self.assertEqual([item["bitrixDealId"] for item in result["items"]], ["2", "3"])
        self.assertEqual(result["total"], 2)
        self.assertEqual(result["historyStartDate"], "2026-08-31")
        self.assertTrue(all(item["eventDate"].startswith("2026-09") for item in result["items"]))

    def test_stable_pagination_ignores_new_backdated_claims(self):
        with self.store._transaction() as connection:
            for deal in range(501):
                self.store._insert_claim(connection, {
                    "managerId": "42", "dealId": str(deal + 1),
                    "timestamp": "2026-09-02T08:00:00+06:00",
                }, source="app")
        first = self.page()
        self.assertEqual(len(first["items"]), 500)
        self.assertEqual(first["total"], 501)
        self.add_claim(9999, "2026-09-02T08:00:00+06:00")
        second = self.page(snapshot=first["snapshot"], cursor=first["nextCursor"])
        self.assertEqual(second["total"], 501)
        self.assertEqual(len(second["items"]), 1)
        self.assertIsNone(second["nextCursor"])
        ids = [item["id"] for item in first["items"] + second["items"]]
        self.assertEqual(len(set(ids)), 501)
        self.assertEqual(self.page()["total"], 502)

    def test_history_counts_events_like_admin_even_if_same_deal_repeats(self):
        self.add_claim(100, "2026-09-02T08:00:00+06:00")
        self.add_claim(100, "2026-09-03T08:00:00+06:00")
        self.assertEqual(self.page()["total"], self.store.count_claims("27337", "2026-09-01", "2026-09-30"))
        self.assertEqual(self.page()["total"], 2)

    def test_empty_history_has_explicit_zero_and_no_cursor(self):
        result = self.page()
        self.assertEqual((result["snapshot"], result["total"], result["items"]), (0, 0, []))
        self.assertIsNone(result["historyStartDate"])
        self.assertIsNone(result["nextCursor"])

    def test_invalid_ranges_instants_and_cursors_fail_closed(self):
        for invalid in (
            {"start": "2026-02-30"}, {"start": "2026-09-31"},
            {"end": "2026-08-31"}, {"start": "2025-01-01"},
            {"as_of": "2026-09-21"}, {"as_of": "2026-09-21T16:20:00"},
            {"as_of": "2026-09-21T16:20:00+25:00"},
            {"snapshot": True}, {"cursor": 1}, {"snapshot": 0, "cursor": 1},
            {"snapshot": 100}, {"cursor": -1}, {"snapshot": 1.5},
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.page(**invalid)

    def test_route_requires_signature_and_rejects_bad_parameters(self):
        payload = {"start": "2026-09-01", "end": "2026-09-30", "asOf": "2026-09-21T10:20:00Z"}
        unsigned = self.signed_request(payload, unsigned=True)
        self.assertEqual(HandlerHarness.status(unsigned), 401)
        invalid = self.signed_request({**payload, "start": "2026-09-31"})
        self.assertEqual(HandlerHarness.status(invalid), 400)
        self.assertEqual(HandlerHarness.json(invalid), {"ok": False, "error": "invalid_payload"})

    def test_history_and_live_claims_cannot_exhaust_each_others_rate_budget(self):
        for first, second in (("claim-history", "claim"), ("claim", "claim-history")):
            with (
                self.subTest(first=first),
                patch.object(app, "BAZA_PICKER_BRIDGE_SECRET", SECRET),
                patch.object(app, "readiness_state", return_value={"ok": True}),
                patch.object(app, "RATE_LIMIT_REQUESTS", 2),
                patch.object(app, "RATE_LIMIT_BUCKETS", defaultdict(deque)),
                patch.object(app.time, "monotonic", return_value=42),
                patch.object(baza_bridge.time, "time", return_value=NOW),
                patch.object(app, "baza_picker_action", return_value=({"ok": True}, 200)) as action,
            ):
                results = []
                for name in (first, first, first, second, second, second):
                    path = "/integrations/baza/v1/" + name
                    body = b'{"bitrixUserId":"42"}'
                    handler = HandlerHarness.make("POST", path, body, headers=signed_headers(path, body))
                    handler.do_POST()
                    results.append(HandlerHarness.status(handler))
                self.assertEqual(results, [200, 200, 429, 200, 200, 429])
                self.assertEqual(action.call_count, 4)


if __name__ == "__main__":
    unittest.main()
