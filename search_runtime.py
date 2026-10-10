"""Bounded, shared read work and strict pagination for deal search."""

import concurrent.futures
import threading
import time
import urllib.parse


class SearchTimings:
    """Numeric phase timings only: no identifiers, params, tokens, or errors."""

    def __init__(self):
        self.started = time.monotonic()
        self.phases = {}
        self.counts = {}

    def call(self, phase, operation, *args, **kwargs):
        started = time.monotonic()
        try:
            return operation(*args, **kwargs)
        finally:
            self.phases[phase] = self.phases.get(phase, 0.0) + time.monotonic() - started
            self.counts[phase] = self.counts.get(phase, 0) + 1

    def rows(self, rows):
        iterator = iter(rows)
        while True:
            try:
                row = self.call("analysis_wait", next, iterator)
            except StopIteration:
                return
            yield row

    def summary(self, result):
        result = result or {}
        return {
            "event": "deal_search_timing",
            "totalMs": round((time.monotonic() - self.started) * 1000, 1),
            "phaseMs": {key: round(value * 1000, 1) for key, value in self.phases.items()},
            "phaseCalls": dict(self.counts),
            "offered": bool(result.get("deal")),
            "hasMore": bool(result.get("hasMore")),
            "httpStatus": int(result.get("_httpStatus", 200)),
        }


class SearchCapacityError(RuntimeError):
    pass


class SingleFlight:
    """Share only simultaneous reads; successful freshness lives in caller caches."""

    def __init__(self):
        self._lock = threading.Lock()
        self._pending = {}

    def run(self, key, read, timeout):
        with self._lock:
            future = self._pending.get(key)
            leader = future is None
            if leader:
                future = concurrent.futures.Future()
                self._pending[key] = future
        if not leader:
            return future.result(timeout=timeout)
        try:
            result = read()
            future.set_result(result)
            return result
        except BaseException as exc:
            future.set_exception(exc)
            raise
        finally:
            with self._lock:
                if self._pending.get(key) is future:
                    del self._pending[key]


class SharedAnalysisPool:
    """A process-wide worker/queue bound, including work outliving its HTTP search.

    A returning caller never cancels another caller's shared read or waits for
    newer speculative candidates. Pending keys disappear on completion, so
    failures can be retried and no unversioned result becomes a cache.
    """

    def __init__(self, workers, max_pending=None):
        self.workers = max(1, int(workers))
        self._slots = threading.BoundedSemaphore(max_pending or self.workers * 2)
        self._lock = threading.Lock()
        self._pending = {}
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.workers, thread_name_prefix="deal-analysis",
        )

    def submit(self, key, read):
        with self._lock:
            existing = self._pending.get(key)
            if existing is not None:
                return existing
            if not self._slots.acquire(blocking=False):
                raise SearchCapacityError("search_analysis_capacity")
            try:
                future = self._executor.submit(read)
            except BaseException:
                self._slots.release()
                raise
            self._pending[key] = future

        # Add outside the lock: callbacks may run inline for a completed future.
        def finished(done):
            with self._lock:
                if self._pending.get(key) is done:
                    del self._pending[key]
            self._slots.release()

        future.add_done_callback(finished)
        return future

    def shutdown(self):
        """Only for tests/process shutdown; never used when a search returns."""
        self._executor.shutdown(wait=True, cancel_futures=True)


def ordered_analysis(headers, submit, *, prefetch, timeout):
    """Yield (header, result, safe_error) in exact queue order with small lookahead."""
    waited = 0.0
    width = max(1, min(int(prefetch), len(headers)))
    pending = {}

    def schedule(index):
        try:
            pending[index] = submit(headers[index])
        except SearchCapacityError:
            pending[index] = None

    for index in range(min(width, len(headers))):
        schedule(index)
    for index, header in enumerate(headers):
        future = pending.pop(index)
        if future is None:
            # Another response may have consumed the queue's capacity while
            # this speculative candidate was scheduled. Retry its admission
            # only when it becomes the next required candidate.
            try:
                future = submit(header)
            except SearchCapacityError:
                future = None
        started = time.monotonic()
        result, error = None, None
        try:
            if future is None:
                error = "capacity"
            else:
                # Ownership checks happen in the consumer between yields and
                # have a separate budget. Only analysis waiting consumes this
                # budget; a ready result is usable without additional waiting.
                result = future.result(timeout=max(0.0, timeout - waited))
        except concurrent.futures.TimeoutError:
            error = "timeout"
        except Exception:
            error = "source_unavailable"
        finally:
            waited += time.monotonic() - started
        yield header, result, error
        # Do not enqueue another candidate until the consumer asks to continue.
        next_index = index + width
        if next_index < len(headers):
            schedule(next_index)


def read_source_lists_batch(commands, call, *, max_items, timeout):
    """Read complete mandatory lists using read-only Bitrix batch commands.

    Per-command pagination/errors are separate from the outer HTTP response.
    Never turn a missing/failed/truncated command into a confirmed empty list.
    https://apidocs.bitrix24.com/settings/how-to-call-rest-api/batch.html
    """
    allowed = {"crm.timeline.comment.list", "crm.activity.list"}
    if not commands or any(method not in allowed for method, _ in commands.values()):
        raise ValueError("unsupported_search_batch_method")
    results = {key: [] for key in commands}
    starts = {key: 0 for key in commands}
    seen = {key: set() for key in commands}
    deadline = time.monotonic() + timeout
    while starts:
        params = {"halt": 0}
        for key, start in starts.items():
            if start in seen[key]:
                raise RuntimeError("repeated_search_batch_cursor")
            seen[key].add(start)
            method, source_params = commands[key]
            query = urllib.parse.urlencode({**source_params, "start": start}, doseq=True)
            params[f"cmd[{key}]"] = method + "?" + query
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("search_sources_timeout")
        payload = call("batch", params, timeout=remaining)
        if time.monotonic() >= deadline:
            raise TimeoutError("search_sources_timeout")
        if not isinstance(payload, dict):
            raise RuntimeError("invalid_search_batch_response")
        values = payload.get("result")
        errors = payload.get("result_error")
        following = payload.get("result_next", {})
        if not isinstance(values, dict) or errors not in ({}, []):
            raise RuntimeError("incomplete_search_batch_response")
        if following == []:
            following = {}
        if not isinstance(following, dict) or set(following) - set(starts):
            raise RuntimeError("invalid_search_batch_pagination")
        next_starts = {}
        for key, start in starts.items():
            rows = values.get(key)
            if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
                raise RuntimeError("invalid_search_batch_rows")
            results[key].extend(rows)
            if len(results[key]) > max_items:
                raise RuntimeError("search_source_record_limit")
            if key not in following:
                continue
            cursor = following[key]
            if isinstance(cursor, bool) or not isinstance(cursor, (int, str)):
                raise RuntimeError("invalid_search_batch_cursor")
            if not str(cursor).isdigit() or int(cursor) <= start:
                raise RuntimeError("invalid_search_batch_cursor")
            if len(results[key]) >= max_items:
                raise RuntimeError("search_source_record_limit")
            next_starts[key] = int(cursor)
        starts = next_starts
    return results
