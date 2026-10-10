"""Guard failures must be diagnosable without exposing private provider data."""

from contextlib import redirect_stderr
from io import BytesIO, StringIO
import json
import unittest
import urllib.error
from unittest.mock import Mock, patch

import test_app as fixtures
from claim_chat_guard import ClaimChatGuardUnavailable, read_claim_chat_ownership
import test_active_dialog_claim as active_fixtures

app = fixtures.app


def diagnostic_rows(output):
    rows = []
    for line in output.getvalue().splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get("event") == "claim_chat_guard_unavailable":
            rows.append(row)
    return rows


class TestGuardDiagnosticMetadata(unittest.TestCase):
    def test_binding_failure_reports_validation_step_without_identifiers(self):
        call = Mock(side_effect=[
            [{"CHAT_ID": "300"}],
            {"id": 300, "type": "lines", "entity_type": "LINES",
             "entity_data_2": "DEAL|999", "owner": 400},
        ])
        output = StringIO()
        with patch("claim_chat_guard.time.monotonic", side_effect=[100, 100, 100.1, 100.1, 100.2]):
            with self.assertRaises(ClaimChatGuardUnavailable) as caught:
                read_claim_chat_ownership("100", "200", call)
        with redirect_stderr(output):
            app.log_claim_chat_guard_unavailable("search", caught.exception)
        self.assertEqual(diagnostic_rows(output), [{
            "event": "claim_chat_guard_unavailable", "phase": "search",
            "reason": "dialog_deal_binding_mismatch",
            "method": "imopenlines.dialog.get", "durationMs": 200.0,
            "upstreamStatus": None, "upstreamCode": None,
        }])
        self.assertEqual(call.call_count, 2)

    def test_provider_http_status_and_allowlisted_code_survive_safe_wrapping(self):
        private = "https://private.invalid/rest/manager/private-token/"
        upstream = urllib.error.HTTPError(
            private, 429, "private customer text", {},
            BytesIO(json.dumps({"error": "QUERY_LIMIT_EXCEEDED", "error_description": private}).encode()),
        )
        output = StringIO()
        with patch.object(app, "load_env", return_value=private), patch.object(app.urllib.request, "urlopen", side_effect=upstream) as remote:
            with self.assertRaises(ClaimChatGuardUnavailable) as caught:
                read_claim_chat_ownership("100", "200", app.bitrix_call)
        with redirect_stderr(output):
            app.log_claim_chat_guard_unavailable("claim", caught.exception)
        row, = diagnostic_rows(output)
        self.assertEqual(row["reason"], "chat_guard_read_failed")
        self.assertEqual(row["method"], "imopenlines.crm.chat.get")
        self.assertEqual(row["upstreamStatus"], 429)
        self.assertEqual(row["upstreamCode"], "QUERY_LIMIT_EXCEEDED")
        self.assertGreaterEqual(row["durationMs"], 0)
        self.assertNotIn("private", output.getvalue())
        remote.assert_called_once()

    def test_unknown_exception_fields_never_become_log_content(self):
        private = "Private tourist +996555000000 secret-token"
        error = ClaimChatGuardUnavailable(private)
        error.method = private
        error.duration_ms = float("nan")
        error.upstream_status = private
        error.upstream_code = private
        output = StringIO()
        with redirect_stderr(output):
            app.log_claim_chat_guard_unavailable(private, error)
        row, = diagnostic_rows(output)
        self.assertEqual(row, {
            "event": "claim_chat_guard_unavailable", "phase": "unknown",
            "reason": "unknown", "method": None, "durationMs": None,
            "upstreamStatus": None, "upstreamCode": "other",
        })
        self.assertNotIn(private, output.getvalue())

    def test_api_error_metadata_uses_allowlist_instead_of_provider_text(self):
        private = "private provider text with credentials"
        for code, expected in (("OPERATION_TIME_LIMIT", "OPERATION_TIME_LIMIT"), (private, "other")):
            with self.subTest(code=expected):
                response = BytesIO(json.dumps({"error": code, "error_description": private}).encode())
                response.status = 200
                output = StringIO()
                with patch.object(app, "load_env", return_value="https://private.invalid/"), patch.object(app.urllib.request, "urlopen", return_value=response):
                    with self.assertRaises(ClaimChatGuardUnavailable) as caught:
                        read_claim_chat_ownership("100", "200", app.bitrix_call)
                with redirect_stderr(output):
                    app.log_claim_chat_guard_unavailable("search", caught.exception)
                row, = diagnostic_rows(output)
                self.assertEqual(row["upstreamStatus"], 200)
                self.assertEqual(row["upstreamCode"], expected)
                self.assertNotIn(private, output.getvalue())


class TestGuardDiagnosticCallSites(fixtures.ClaimWorkflowTestCase):
    setUp = active_fixtures.TestActiveDialogClaim.setUp
    portal = active_fixtures.TestActiveDialogClaim.portal
    search = active_fixtures.TestActiveDialogClaim.search

    def test_search_logs_one_failure_without_advancing_or_assigning(self):
        output = StringIO()
        with redirect_stderr(output):
            result = self.search("33", fail=True)
        row, = diagnostic_rows(output)
        self.assertEqual(row["phase"], "search")
        self.assertEqual(row["reason"], "chat_guard_read_failed")
        self.assertEqual(result["code"], "chat_ownership_unavailable")
        self.assertFalse(result["hasMore"])
        self.assertIsNone(result["continuationToken"])
        self.assertEqual(self.store.list_claims(), [])

    def test_claim_and_greeting_log_separate_phases_without_changing_denial(self):
        fake, state = self.portal(fail=True)
        output = StringIO()
        with patch.object(app, "bitrix_call", side_effect=fake), redirect_stderr(output):
            result = app.claim_chat_check_response(self.deal_id, self.manager_id)
            with self.assertRaisesRegex(RuntimeError, "^chat_ownership_unavailable$"):
                app.require_available_greeting_chat(self.deal_id, self.manager_id)
        self.assertEqual([row["phase"] for row in diagnostic_rows(output)], ["claim", "greeting"])
        self.assertEqual(result["_httpStatus"], 503)
        self.assertFalse(state["updated"])

    def test_failed_log_sink_cannot_change_safe_search_response(self):
        with patch.object(app.sys.stderr, "write", side_effect=OSError("log storage unavailable")), patch.object(app, "SEARCH_TIMING_LOG_ENABLED", False):
            result = self.search("33", fail=True)
        self.assertEqual(result["code"], "chat_ownership_unavailable")
        self.assertEqual(result["_httpStatus"], 503)


if __name__ == "__main__":
    unittest.main()
