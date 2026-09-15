#!/usr/bin/env python3
"""Business regressions for offering previously rejected leads again.

Only temporary SQLite state is used; every unexpected HTTP request is blocked.
"""

from contextlib import contextmanager
from unittest.mock import patch
import os
import unittest

import test_app as fixtures

app = fixtures.app


class TestRejectionRetry(fixtures.ClaimWorkflowTestCase):
    def setUp(self):
        environment = patch.dict(os.environ, fixtures._TEST_ENV)
        environment.start()
        self.addCleanup(environment.stop)
        super().setUp()
        self.network_guard = patch.object(
            app.urllib.request, "urlopen", fixtures._network_is_forbidden
        )
        self.network_guard.start()
        self.addCleanup(self.network_guard.stop)

    def header(self, deal_id, version="version-1"):
        return {
            "ID": str(deal_id),
            "STAGE_ID": next(iter(app.SOURCE_STAGES)),
            "DATE_MODIFY": version,
            "DATE_CREATE": f"2026-09-{int(deal_id) % 28 + 1:02d}T00:00:00+06:00",
        }

    def profile(self, manager_id="42", competencies=("Турция",)):
        return {
            "id": manager_id, "name": "Manager", "active": True,
            "intranet": True, "competencies": list(competencies),
        }

    def root(self, header, manager_id="42"):
        return app.rejection_semantic_key(
            manager_id, header["ID"], app.deal_version(header)
        )

    def seed_rejection(self, header, manager_id="42", *, after=0):
        root = self.root(header, manager_id)
        entry = {
            "managerId": manager_id, "dealId": header["ID"], "reason": "other",
            "semanticKey": app.rejection_semantic_key(
                manager_id, header["ID"], app.deal_version(header), after
            ),
        }
        # Existing production events have the legacy semantic key and no root.
        if after:
            entry.update(rejectionRoot=root, rejectionAfter=after)
        self.store.append_reject(entry)
        return self.store.list_rejection_history(manager_id)[root]

    def analyzed(self, header, direction):
        return {
            "id": header["ID"], "title": f"Deal {header['ID']}",
            "stageId": header["STAGE_ID"], "version": app.deal_version(header),
            "messages": [] if direction == "Не определено" else [direction],
            "classification": {"direction": direction, "confidence": "high"},
        }

    @contextmanager
    def search_context(self, headers, *, directions=None, manager_id="42",
                       competencies=("Турция",), batch_limit=12):
        directions = directions or {}
        profile = self.profile(manager_id, competencies)

        def analyze(batch):
            return {
                h["ID"]: self.analyzed(h, directions.get(h["ID"], "Турция"))
                for h in batch
            }, {}

        with (
            patch.object(app, "get_manager_profile", return_value=profile),
            patch.object(app, "check_manager_access", return_value={"ok": True, "rule": {}}),
            patch.object(app, "list_allowed_deal_headers", return_value=headers),
            patch.object(app, "analyze_deal_headers", side_effect=analyze) as analysis,
            patch.object(app, "NEXT_DEAL_SCAN_LIMIT", batch_limit),
            patch.object(app, "bitrix_call", side_effect=lambda method, params=None, **kw:
                         next(dict(h) for h in headers if h["ID"] == params["id"])),
        ):
            yield analysis

    def reject_offer(self, offer, manager_id="42"):
        return app.record_rejection(manager_id, {
            "dealId": offer["id"], "selectionToken": offer["selectionToken"],
            "reason": "other",
        })

    def test_new_lead_precedes_older_rejection_and_stays_oldest_first(self):
        headers = [self.header(i) for i in (1, 2, 3)]
        self.seed_rejection(headers[0])
        with self.search_context(headers) as analysis:
            result = app._get_next_deal_for_manager("42")
        self.assertEqual(result["deal"]["id"], "2")
        self.assertEqual([h["ID"] for h in analysis.call_args.args[0]], ["2", "3", "1"])

    def test_all_rejected_leads_cycle_and_repeated_rejection_moves_to_end(self):
        headers = [self.header(i) for i in (1, 2, 3)]
        for header in headers:
            self.seed_rejection(header)
        original = self.store.list_rejections()
        offered = []
        with self.search_context(headers):
            for _ in range(4):
                deal = app._get_next_deal_for_manager("42")["deal"]
                self.assertIsNotNone(deal)
                offered.append(deal["id"])
                self.assertTrue(self.reject_offer(deal)["ok"])
        self.assertEqual(offered, ["1", "2", "3", "1"])
        self.assertEqual(len(self.store.list_rejections()), 7)
        self.assertEqual(self.store.list_rejections()[:3], original)

    def test_rejected_order_uses_last_rejection_event_not_creation_date(self):
        headers = [self.header(i) for i in (1, 2, 3)]
        for index in (2, 0, 1):
            self.seed_rejection(headers[index])
        with self.search_context(headers) as analysis:
            result = app._get_next_deal_for_manager("42")
        self.assertEqual(result["deal"]["id"], "3")
        self.assertEqual([h["ID"] for h in analysis.call_args.args[0]], ["3", "1", "2"])

    def test_repeat_rejection_is_idempotent_and_cannot_rotate_again_with_old_token(self):
        headers = [self.header(1), self.header(2)]
        with self.search_context(headers):
            first = app._get_next_deal_for_manager("42")["deal"]
            self.assertTrue(self.reject_offer(first)["ok"])
            self.seed_rejection(headers[1])
            reoffer = app._get_next_deal_for_manager("42")["deal"]
            self.assertEqual(reoffer["id"], "1")
            self.assertNotEqual(first["selectionToken"], reoffer["selectionToken"])
            self.assertTrue(self.reject_offer(reoffer)["ok"])
            history = self.store.list_rejection_history("42")
            for old_offer in (first, reoffer, first):
                replay = self.reject_offer(old_offer)
                self.assertTrue(replay["ok"])
                self.assertTrue(replay["idempotentReplay"])
            self.assertEqual(history, self.store.list_rejection_history("42"))
            self.assertEqual(app._get_next_deal_for_manager("42")["deal"]["id"], "2")
        self.assertEqual(len(self.store.list_rejections()), 3)

    def test_fresh_reoffer_can_be_claimed_but_original_selection_cannot(self):
        header = self.source_deal()
        current_time = app.time.time()
        with self.search_context([header]):
            original = app._get_next_deal_for_manager("42")["deal"]
            self.assertTrue(self.reject_offer(original)["ok"])
            reoffer = app._get_next_deal_for_manager("42")["deal"]
        with (
            self.common_claim_context(),
            patch.object(app.time, "time", return_value=current_time),
            patch.object(app, "bitrix_call") as bitrix,
        ):
            stale = app.preview_claim("100", "42", selection_token=original["selectionToken"])
        self.assertFalse(stale["ok"])
        self.assertTrue(stale["selectionStale"])
        bitrix.assert_not_called()
        with (
            self.common_claim_context(),
            patch.object(app.time, "time", return_value=current_time),
            patch.object(app, "bitrix_call", side_effect=self.claim_side_effect(
                [self.source_deal(), self.source_deal(), self.claimed_deal()]
            )) as bitrix,
        ):
            claimed = app.preview_claim("100", "42", selection_token=reoffer["selectionToken"])
        self.assertTrue(claimed["ok"], claimed)
        self.assertEqual(sum(c.args[0] == "crm.deal.update" for c in bitrix.call_args_list), 1)
        self.assertEqual(len(self.store.list_claims()), 1)
        self.assertEqual(len(self.store.list_rejections()), 1)

    def test_skill_mismatch_is_skipped_before_paginated_rejection_fallback(self):
        headers = [self.header(1), self.header(2), self.header(3)]
        self.seed_rejection(headers[0])
        with self.search_context(headers, directions={"2": "Египет", "3": "Египет"}, batch_limit=1):
            first = app._get_next_deal_for_manager("42")
            second = app._get_next_deal_for_manager("42", first["continuationToken"])
            third = app._get_next_deal_for_manager("42", second["continuationToken"])
        self.assertIsNone(first["deal"])
        self.assertTrue(first["hasMore"])
        self.assertIsNone(second["deal"])
        self.assertTrue(second["hasMore"])
        self.assertEqual(third["deal"]["id"], "1")

    def test_without_skills_only_unclassified_rejected_lead_is_reoffered(self):
        headers = [self.header(1), self.header(2)]
        for header in headers:
            self.seed_rejection(header)
        with self.search_context(headers, directions={"2": "Не определено"}, competencies=()):
            result = app._get_next_deal_for_manager("42")
        self.assertEqual(result["deal"]["id"], "2")

    def test_matching_new_lead_on_later_page_precedes_rejected_fallback(self):
        headers = [self.header(i) for i in (1, 2, 3, 4)]
        self.seed_rejection(headers[0])
        with self.search_context(headers, directions={"2": "Египет", "3": "Египет"}, batch_limit=1):
            first = app._get_next_deal_for_manager("42")
            second = app._get_next_deal_for_manager("42", first["continuationToken"])
            third = app._get_next_deal_for_manager("42", second["continuationToken"])
        self.assertIsNone(first["deal"])
        self.assertTrue(first["hasMore"])
        self.assertIsNone(second["deal"])
        self.assertTrue(second["hasMore"])
        self.assertEqual(third["deal"]["id"], "4")
        self.assertFalse(third["hasMore"])

    def test_cursor_expires_when_rejection_changes_even_if_queue_order_does_not(self):
        headers = [self.header(1), self.header(2)]
        predecessor = self.seed_rejection(headers[1])
        with self.search_context(headers, directions={"1": "Египет"}, batch_limit=1):
            first = app._get_next_deal_for_manager("42")
            self.assertTrue(first["hasMore"])
            self.seed_rejection(headers[1], after=predecessor)
            result = app._get_next_deal_for_manager("42", first["continuationToken"])
        self.assertIsNone(result["deal"])
        self.assertEqual(result["_httpStatus"], 409)

    def test_rejection_history_is_isolated_between_managers(self):
        headers = [self.header(1), self.header(2)]
        self.seed_rejection(headers[0], "42")
        self.seed_rejection(headers[1], "43")
        for manager_id, expected in (("42", "2"), ("43", "1")):
            with self.subTest(manager_id=manager_id), self.search_context(headers, manager_id=manager_id):
                result = app._get_next_deal_for_manager(manager_id)
                self.assertEqual(result["deal"]["id"], expected)
        self.assertEqual(len(self.store.list_rejection_history("42")), 1)
        self.assertEqual(len(self.store.list_rejection_history("43")), 1)

    def test_changed_deal_version_returns_to_new_leads_and_keeps_original_audit(self):
        before = self.header(1)
        self.seed_rejection(before)
        headers = [self.header(1, "version-2"), self.header(2)]
        with self.search_context(headers):
            result = app._get_next_deal_for_manager("42")
        self.assertEqual(result["deal"]["id"], "1")
        payload = app.decode_selection_token(result["deal"]["selectionToken"], "1", "42")
        self.assertEqual(payload.get("rejectionAfter", 0), 0)
        self.assertEqual(len(self.store.list_rejections()), 1)

    def test_rejected_claimed_and_unresolved_leads_remain_excluded(self):
        headers = [self.header(i) for i in (1, 2, 3)]
        for header in headers:
            self.seed_rejection(header)
        self.store.append_claim({"dealId": "1", "managerId": "43"})
        key = app.claim_operation_key("2", "older-version")
        self.store.begin_claim_operation("2", "43", operation_key=key, request={"dealVersion": "older-version"})
        self.store.fail_claim_operation(key, "unknown", result={"remoteUpdateUncertain": True})
        with self.search_context(headers) as analysis:
            result = app._get_next_deal_for_manager("42")
        self.assertEqual(result["deal"]["id"], "3")
        self.assertEqual([h["ID"] for h in analysis.call_args.args[0]], ["3"])

    def test_search_cannot_offer_a_lead_when_rejection_history_is_unavailable(self):
        with (
            self.search_context([self.header(1)]) as analysis,
            patch.object(self.store, "list_rejection_history", side_effect=RuntimeError("SQLite unavailable")),
        ):
            try:
                result = app._get_next_deal_for_manager("42")
            except RuntimeError:
                pass
            else:
                self.assertIsNone(result.get("deal"))
            analysis.assert_not_called()

    def test_legacy_and_repeat_events_have_one_latest_history_entry(self):
        header = self.header(1)
        first = self.seed_rejection(header)
        second = self.seed_rejection(header, after=first)
        third = self.seed_rejection(header, after=second)
        self.assertLess(first, second)
        self.assertLess(second, third)
        self.assertEqual(self.store.list_rejection_history("42"), {self.root(header): third})
        self.assertEqual(len(self.store.list_rejections()), 3)

    def test_reoffer_rejected_while_claim_waits_for_lock_cannot_be_claimed(self):
        header = self.header(1)
        predecessor = self.seed_rejection(header)
        with self.search_context([header]):
            offer = app._get_next_deal_for_manager("42")["deal"]
        testcase = self

        class RejectBeforeClaimLock:
            def __enter__(self):
                testcase.seed_rejection(header, after=predecessor)

            def __exit__(self, *_args):
                return False

        with (
            patch.object(app, "DATA_LOCK", RejectBeforeClaimLock()),
            patch.object(app, "bitrix_call") as bitrix,
        ):
            result = app.preview_claim("1", "42", selection_token=offer["selectionToken"])
        self.assertFalse(result["ok"])
        self.assertTrue(result["selectionStale"])
        self.assertEqual(result["_httpStatus"], 409)
        self.assertEqual(len(self.store.list_rejections()), 2)
        self.assertEqual(self.store.list_claims(), [])
        bitrix.assert_not_called()


if __name__ == "__main__":
    unittest.main()
