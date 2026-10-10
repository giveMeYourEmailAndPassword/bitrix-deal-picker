"""Search latency changes preserve ordering, source completeness and ownership."""

import concurrent.futures
from contextlib import contextmanager, redirect_stderr
from io import StringIO
import json
import os
import threading
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs

import test_app as fixtures
from search_runtime import SearchCapacityError, SharedAnalysisPool, SingleFlight, read_source_lists_batch

app = fixtures.app


class TestSharedReadWork(unittest.TestCase):
    def test_pool_coalesces_only_same_version_and_bounds_running_and_queued_work(self):
        pool = SharedAnalysisPool(2, max_pending=4)
        release = threading.Event()
        both_started = threading.Event()
        lock = threading.Lock()
        active = 0
        peak = 0
        calls = 0

        def read():
            nonlocal active, peak, calls
            with lock:
                active += 1
                calls += 1
                peak = max(peak, active)
                if active == 2:
                    both_started.set()
            try:
                if not release.wait(2):
                    raise TimeoutError("test release")
                return {"ok": True}
            finally:
                with lock:
                    active -= 1

        try:
            first = pool.submit(("1", "v1"), read)
            duplicate = pool.submit(("1", "v1"), read)
            next_version = pool.submit(("1", "v2"), read)
            self.assertIs(first, duplicate)
            self.assertIsNot(first, next_version)
            self.assertTrue(both_started.wait(1))
            queued = [pool.submit((str(i), "v1"), read) for i in (2, 3)]
            with self.assertRaises(SearchCapacityError):
                pool.submit(("4", "v1"), read)
            self.assertIs(pool.submit(("1", "v1"), read), first)
            release.set()
            for future in [first, next_version, *queued]:
                future.result(timeout=1)
            self.assertEqual(calls, 4)
            self.assertEqual(peak, 2)
        finally:
            release.set()
            pool.shutdown()

    def test_singleflight_failed_read_is_shared_and_a_later_attempt_can_retry(self):
        flight = SingleFlight()
        started = threading.Event()
        release = threading.Event()
        follower_waiting = threading.Event()

        def failing():
            started.set()
            release.wait(2)
            raise TimeoutError("private remote URL")

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as threads:
            leader = threads.submit(flight.run, "headers", failing, 1)
            self.assertTrue(started.wait(1))
            # Observe the follower attached to the already-running future,
            # without relying on thread scheduling sleeps.
            shared = flight._pending["headers"]
            original_result = shared.result
            def wait_result(*args, **kwargs):
                follower_waiting.set()
                return original_result(*args, **kwargs)
            with patch.object(shared, "result", side_effect=wait_result):
                follower = threads.submit(flight.run, "headers", lambda: self.fail("duplicate read"), 1)
                self.assertTrue(follower_waiting.wait(1))
                release.set()
                for future in (leader, follower):
                    with self.assertRaises(TimeoutError):
                        future.result(timeout=1)
        self.assertEqual(flight.run("headers", lambda: "fresh", 1), "fresh")


class TestSearchSpeed(fixtures.TemporaryStateTestCase):
    def setUp(self):
        environment = patch.dict(os.environ, fixtures._TEST_ENV)
        environment.start()
        self.addCleanup(environment.stop)
        super().setUp()
        network = patch.object(app.urllib.request, "urlopen", fixtures._network_is_forbidden)
        network.start()
        self.addCleanup(network.stop)

    def header(self, deal_id, version="v1"):
        return {"ID": str(deal_id), "TITLE": "Private customer", "DATE_MODIFY": version,
                "STAGE_ID": next(iter(app.SOURCE_STAGES)), "DATE_CREATE": f"2026-01-{int(deal_id):02d}"}

    def messages(self, text="Хочу в Турцию"):
        return {"useful": [text], "rawCount": 1, "sources": ["activity"], "openlineSessionIds": []}

    @contextmanager
    def search_context(self, headers):
        with (
            patch.object(app, "get_manager_profile", side_effect=lambda manager_id:
                         {"id": manager_id, "active": True, "intranet": True, "competencies": ["Турция"]}),
            patch.object(app, "check_manager_access", return_value={"ok": True, "rule": {}}),
            patch.object(app, "list_allowed_deal_headers", return_value=headers),
        ):
            yield

    def test_oldest_ready_match_returns_while_a_newer_analysis_is_still_running(self):
        newer_started = threading.Event()
        release = threading.Event()
        headers = [self.header(i) for i in range(1, 13)]
        reads = []

        def messages(deal_id):
            reads.append(deal_id)
            if deal_id == "1":
                self.assertTrue(newer_started.wait(1))
            else:
                newer_started.set()
                release.wait(2)
            return self.messages()

        try:
            with self.search_context(headers), patch.object(app, "get_deal_messages", side_effect=messages):
                result = app._get_next_deal_for_manager("42")
                self.assertFalse(release.is_set())
                self.assertEqual(result["deal"]["id"], "1")
                self.assertEqual(result["checkedCount"], 1)
                self.assertEqual(set(reads), {"1", "2"})
                release.set()
                self._analysis_pool.shutdown()
        finally:
            release.set()

    def test_error_on_older_candidate_never_chooses_newer_completed_match(self):
        newer_ready = threading.Event()
        def messages(deal_id):
            if deal_id == "1":
                self.assertTrue(newer_ready.wait(1))
                raise TimeoutError("private upstream secret")
            newer_ready.set()
            return self.messages()
        with self.search_context([self.header(1), self.header(2)]), patch.object(app, "get_deal_messages", side_effect=messages):
            result = app._get_next_deal_for_manager("42")
        self.assertEqual(result["_httpStatus"], 503)
        self.assertIsNone(result["deal"])
        self.assertFalse(result["hasMore"])
        self.assertNotIn("private upstream secret", json.dumps(result))

    def test_concurrent_managers_share_analysis_but_receive_separate_signed_offers(self):
        read_started = threading.Event()
        release = threading.Event()
        shared_requested = threading.Event()
        original_submit = self._analysis_pool.submit
        submits = 0
        def submit(*args, **kwargs):
            nonlocal submits
            future = original_submit(*args, **kwargs)
            submits += 1
            if submits == 2:
                shared_requested.set()
            return future
        def messages(_deal_id):
            read_started.set()
            release.wait(2)
            return self.messages()
        try:
            with self.search_context([self.header(1)]), patch.object(app, "get_deal_messages", side_effect=messages) as read, patch.object(self._analysis_pool, "submit", side_effect=submit), patch.object(app, "claim_chat_occupied", return_value=False) as ownership:
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as threads:
                    first = threads.submit(app._get_next_deal_for_manager, "42")
                    self.assertTrue(read_started.wait(1))
                    second = threads.submit(app._get_next_deal_for_manager, "43")
                    self.assertTrue(shared_requested.wait(1))
                    release.set()
                    offers = [first.result(timeout=1)["deal"], second.result(timeout=1)["deal"]]
                self.assertEqual(read.call_count, 1)
                self.assertEqual(ownership.call_count, 2)
            for manager_id, offer in zip(("42", "43"), offers):
                self.assertTrue(app.verify_selection_token(offer["selectionToken"], "1", manager_id))
            self.assertIsNot(offers[0], offers[1])
            self.assertNotEqual(offers[0]["selectionToken"], offers[1]["selectionToken"])
            self.assertNotIn("selectionToken", app.DEAL_ANALYSIS_CACHE["1"]["deal"])
        finally:
            release.set()

    def test_concurrent_headers_use_one_read_per_stage(self):
        started = threading.Event()
        release = threading.Event()
        joined = threading.Event()
        def list_stage(method, params, *args, **kwargs):
            started.set()
            release.wait(2)
            return [self.header(1)] if params["filter[STAGE_ID]"] == next(iter(app.SOURCE_STAGES)) else []
        try:
            with patch.object(app, "bitrix_list_all", side_effect=list_stage) as read:
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as threads:
                    first = threads.submit(app.list_allowed_deal_headers)
                    self.assertTrue(started.wait(1))
                    pending = next(iter(app.HEADERS_INFLIGHT._pending.values()))
                    original_result = pending.result
                    def wait_result(*args, **kwargs):
                        joined.set()
                        return original_result(*args, **kwargs)
                    with patch.object(pending, "result", side_effect=wait_result):
                        second = threads.submit(app.list_allowed_deal_headers)
                        self.assertTrue(joined.wait(1))
                        release.set()
                        results = [first.result(timeout=1), second.result(timeout=1)]
                self.assertEqual(read.call_count, len(app.SOURCE_STAGES))
            self.assertEqual(results[0], results[1])
            self.assertIsNot(results[0][0], results[1][0])
        finally:
            release.set()

    def test_invalidation_during_header_read_does_not_repopulate_stale_cache(self):
        def list_stage(*args, **kwargs):
            app.invalidate_deal_caches("1")
            return []
        with patch.object(app, "bitrix_list_all", side_effect=list_stage):
            self.assertEqual(app.list_allowed_deal_headers(), [])
        self.assertNotIn("all", app.DEAL_HEADERS_CACHE)

    def test_version_change_refetches_analysis_and_invalidation_cannot_restore_old_cache(self):
        with patch.object(app, "get_deal_messages", return_value=self.messages()) as read:
            app.analyze_deal_header(self.header(1))
            app.analyze_deal_header(self.header(1))
            app.analyze_deal_header(self.header(1, "v2"))
        self.assertEqual(read.call_count, 2)
        def invalidate(_deal_id):
            app.invalidate_deal_caches("1")
            return self.messages()
        with patch.object(app, "get_deal_messages", side_effect=invalidate):
            app.analyze_deal_header(self.header(1, "v3"))
        self.assertNotIn("1", app.DEAL_ANALYSIS_CACHE)

    def test_search_timing_logs_only_numeric_phases_and_outcome(self):
        output = StringIO()
        with self.search_context([self.header(1)]), patch.object(app, "get_deal_messages", return_value=self.messages("private customer Турция")), patch.object(app, "SEARCH_TIMING_LOG_ENABLED", True), redirect_stderr(output):
            result = app._get_next_deal_for_manager("42")
        record = json.loads(output.getvalue())
        self.assertTrue(record["offered"])
        self.assertIn("ownership", record["phaseMs"])
        self.assertIn("analysis_wait", record["phaseMs"])
        self.assertNotIn("private", output.getvalue())
        self.assertNotIn(result["deal"]["selectionToken"], output.getvalue())
        self.assertNotIn("managerId", record)


class TestBatchedSearchSources(fixtures.TemporaryStateTestCase):
    header = TestSearchSpeed.header
    search_context = TestSearchSpeed.search_context

    def setUp(self):
        environment = patch.dict(os.environ, fixtures._TEST_ENV)
        environment.start()
        self.addCleanup(environment.stop)
        super().setUp()
        network = patch.object(app.urllib.request, "urlopen", fixtures._network_is_forbidden)
        network.start()
        self.addCleanup(network.stop)
        enabled = patch.object(app, "BITRIX_SOURCE_BATCH_ENABLED", True)
        enabled.start()
        self.addCleanup(enabled.stop)

    def test_two_mandatory_sources_use_one_transport_and_keep_crm_fallback(self):
        def call(method, params, timeout=None):
            self.assertEqual(method, "batch")
            self.assertTrue(params["cmd[timeline]"].startswith("crm.timeline.comment.list?"))
            self.assertTrue(params["cmd[activity]"].startswith("crm.activity.list?"))
            self.assertGreater(timeout, 0)
            return {"result": {"timeline": [], "activity": [{"DESCRIPTION": "Хочу в Турцию", "CREATED": "2026-10-10T09:00:00+06:00"}]}, "result_error": [], "result_next": []}
        with patch.object(app, "bitrix_call", side_effect=call) as call:
            result = app.get_deal_messages("1")
        self.assertEqual(call.call_count, 1)
        self.assertEqual(app.classify(result["useful"])["direction"], "Турция")

    def test_real_cold_search_starts_two_source_batches_instead_of_twelve_deal_scans(self):
        release = threading.Event()
        newer_started = threading.Event()
        calls = []
        def call(method, params, timeout=None):
            self.assertEqual(method, "batch")
            query = parse_qs(params["cmd[timeline]"].split("?", 1)[1])
            deal_id = query["filter[ENTITY_ID]"][0]
            calls.append(deal_id)
            if deal_id == "1":
                self.assertTrue(newer_started.wait(1))
            else:
                newer_started.set()
                release.wait(2)
            return {"result": {"timeline": [], "activity": [{"DESCRIPTION": "Хочу в Турцию", "CREATED": "2026-10-10T09:00:00+06:00"}]}, "result_error": [], "result_next": []}
        try:
            with self.search_context([self.header(i) for i in range(1, 13)]), patch.object(app, "bitrix_call", side_effect=call):
                result = app._get_next_deal_for_manager("42")
                self.assertEqual(result["deal"]["id"], "1")
                self.assertEqual(set(calls), {"1", "2"})
                self.assertEqual(len(calls), 2)
                self.assertFalse(release.is_set())
                release.set()
                self._analysis_pool.shutdown()
        finally:
            release.set()

    def test_batch_paginates_only_unfinished_source_and_keeps_older_useful_message(self):
        calls = []
        def call(method, params, timeout=None):
            calls.append(params)
            query = parse_qs(params["cmd[timeline]"].split("?", 1)[1])
            if query["start"] == ["0"]:
                return {"result": {"timeline": [{"COMMENT": "Создана новая сделка"}], "activity": []}, "result_error": {}, "result_next": {"timeline": 50}}
            self.assertEqual(query["start"], ["50"])
            self.assertNotIn("cmd[activity]", params)
            return {"result": {"timeline": [{"COMMENT": "Клиент хочет Египет", "CREATED": "2026-10-09T09:00:00+06:00"}]}, "result_error": [], "result_next": {}}
        with patch.object(app, "bitrix_call", side_effect=call):
            result = app.get_deal_messages("1")
        self.assertEqual(len(calls), 2)
        self.assertEqual(app.classify(result["useful"])["direction"], "Египет")

    def test_batch_source_errors_missing_results_or_bad_cursor_fail_closed(self):
        valid = {"result": {"timeline": [], "activity": []}, "result_error": [], "result_next": []}
        invalid = [
            {**valid, "result_error": {"timeline": {"error": "private detail"}}},
            {**valid, "result": {"activity": []}},
            {**valid, "result_next": {"timeline": 0}},
            {**valid, "result_next": {"timeline": True}},
            {**valid, "result_next": {"foreign": 50}},
            {**valid, "result": {"timeline": {}, "activity": []}},
        ]
        for payload in invalid:
            with self.subTest(payload=payload), patch.object(app, "bitrix_call", return_value=payload):
                with self.assertRaisesRegex(RuntimeError, "полностью прочитать историю") as error:
                    app.get_deal_messages("1")
                self.assertNotIn("private", str(error.exception))

    def test_batch_second_page_failure_does_not_become_an_empty_request(self):
        first = {"result": {"timeline": [], "activity": []}, "result_error": [], "result_next": {"timeline": 50}}
        with patch.object(app, "bitrix_call", side_effect=[first, TimeoutError("private URL")]):
            with self.assertRaisesRegex(RuntimeError, "полностью прочитать историю"):
                app.get_deal_messages("1")

    def test_batched_newest_openline_still_overrides_old_crm_destination(self):
        def call(method, params, timeout=None):
            if method == "batch":
                return {"result": {
                    "timeline": [{"COMMENT": "Хочу в Турцию", "CREATED": "2026-10-09T09:00:00+06:00"}],
                    "activity": [{"ID": "5", "PROVIDER_ID": "IMOPENLINES_SESSION", "ASSOCIATED_ENTITY_ID": "99", "CREATED": "2026-10-10T09:00:00+06:00"}],
                }, "result_error": [], "result_next": []}
            self.assertEqual(method, "imopenlines.session.history.get")
            return {"message": {"7": {"id": "7", "senderid": "15", "text": "Теперь нужен Египет", "date": "2026-10-10T09:00:00+06:00"}}}
        with patch.object(app, "bitrix_call", side_effect=call) as call:
            result = app.get_deal_messages("1")
        self.assertEqual(call.call_count, 2)
        self.assertEqual(result["openlineSessionIds"], ["99"])
        self.assertEqual(app.classify(result["useful"])["direction"], "Египет")

    def test_batch_record_bound_and_disallowed_methods_reject(self):
        with patch.object(app, "MAX_SOURCE_RECORDS_PER_DEAL", 1), patch.object(app, "bitrix_call", return_value={"result": {"timeline": [{}, {}], "activity": []}, "result_error": [], "result_next": []}):
            with self.assertRaisesRegex(RuntimeError, "полностью прочитать историю"):
                app.get_deal_messages("1")
        with self.assertRaises(ValueError):
            read_source_lists_batch({"bad": ("crm.deal.update", {})}, lambda *a, **k: self.fail("write transport"), max_items=5, timeout=1)


if __name__ == "__main__":
    unittest.main()
