#!/usr/bin/env python3
"""Security, reliability and business-contract tests for ``app.py``.

The suite is deliberately hermetic: application state lives in temporary
directories and every accidental HTTP request fails immediately.  These tests
therefore cannot mutate Bitrix or the repository while they run.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import ExitStack, contextmanager
from datetime import datetime
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, call, patch


# Import-time configuration is security-sensitive in app.py.  Override (not
# setdefault) every relevant value before importing it so a developer's real
# credentials can never leak into this process.
_BOOT_DATA_DIR = tempfile.mkdtemp(prefix="bitrix-picker-app-tests-")
_TEST_ENV = {
    "APP_DATA_DIR": _BOOT_DATA_DIR,
    "BITRIX_WEBHOOK_BASE": "https://test-fake.bitrix24.test/rest/1/not-a-secret/",
    "BITRIX_ALLOWED_DOMAINS": "test-fake.bitrix24.test",
    "APP_ALLOWED_ORIGINS": "https://picker.example.test",
    "PUBLIC_APP_URL": "https://picker.example.test",
    "ADMIN_USER_IDS": "1,2",
    "CLAIM_STATS_SOURCE": "app_events",
    "DRY_RUN": "1",
    "BITRIX_CLAIM_MARKER_FIELD": "UF_CRM_TEST_CLAIM_MARKER",
    "GREETING_AUTO_SEND": "0",
    "ALLOW_UNVERIFIED_USERS": "0",
    "HOST": "127.0.0.1",
    "APP_TZ_OFFSET_HOURS": "6",
    "SELECTION_TOKEN_TTL_SECONDS": "1800",
    "CLAIM_OPERATION_PENDING_TTL_SECONDS": "300",
    # Most workflow tests focus on claim/rejection mechanics.  Dedicated
    # authorization tests below exercise the production fail-closed defaults.
    "REQUIRE_LEGACY_MIGRATION": "0",
    "REQUIRE_EXPLICIT_ACCESS_RULE": "0",
}
_ORIGINAL_ENV = {name: os.environ.get(name) for name in _TEST_ENV}
os.environ.update(_TEST_ENV)
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))

import app  # noqa: E402  (environment must be installed first)
from state_store import StateStore  # noqa: E402


def _network_is_forbidden(*args, **kwargs):
    target = args[0] if args else "unknown URL"
    raise AssertionError(f"unexpected network request in unit test: {target!r}")


_NETWORK_GUARD = patch.object(app.urllib.request, "urlopen", _network_is_forbidden)


def test_manager_policy(competencies=("Турция",), rule=None):
    return app.manager_policy_hash(
        {
            "active": True,
            "intranet": True,
            "competencies": list(competencies),
        },
        rule or {"enabled": True, "dailyLimit": None},
    )


def setUpModule():
    _NETWORK_GUARD.start()


def tearDownModule():
    _NETWORK_GUARD.stop()
    shutil.rmtree(_BOOT_DATA_DIR, ignore_errors=True)
    for name, value in _ORIGINAL_ENV.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


class TemporaryStateTestCase(unittest.TestCase):
    """Give each stateful test a fresh SQLite database outside the repo."""

    def setUp(self):
        self._temp_dir = tempfile.TemporaryDirectory(prefix="bitrix-picker-test-state-")
        self.data_dir = Path(self._temp_dir.name)
        self.store = StateStore(self.data_dir, db_filename="state.sqlite3", local_timezone=app.LOCAL_TZ)
        self._store_patch = patch.object(app, "STATE_STORE", self.store)
        self._store_patch.start()
        app.DEAL_ANALYSIS_CACHE.clear()
        app.DEAL_HEADERS_CACHE.clear()
        app.PORTAL_USERS_CACHE.clear()
        app.USER_VERIFY_CACHE.clear()
        app.READINESS_CACHE.update({"checkedAt": 0.0, "state": None})

    def tearDown(self):
        self._store_patch.stop()
        self._temp_dir.cleanup()


class HandlerHarness:
    """Construct a BaseHTTPRequestHandler without binding a socket."""

    @staticmethod
    def make(method="GET", path="/", body=None, *, origin=None, headers=None):
        if body is None:
            raw = b""
        elif isinstance(body, bytes):
            raw = body
        else:
            raw = json.dumps(body).encode("utf-8")

        handler = app.Handler.__new__(app.Handler)
        handler.command = method
        handler.path = path
        handler.client_address = ("127.0.0.1", 12345)
        handler.server = MagicMock()
        handler.close_connection = True
        handler.headers = {
            "Content-Length": str(len(raw)),
            **({"Origin": origin} if origin else {}),
            **(headers or {}),
        }
        handler.rfile = BytesIO(raw)
        handler.wfile = BytesIO()
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()
        return handler

    @staticmethod
    def status(handler):
        return handler.send_response.call_args.args[0]

    @staticmethod
    def json(handler):
        return json.loads(handler.wfile.getvalue().decode("utf-8"))

    @staticmethod
    def headers(handler):
        return [item.args for item in handler.send_header.call_args_list]


class TestStrictIdentityAndAuthorization(unittest.TestCase):
    @patch.object(app, "verify_bitrix_user", return_value={"id": "42", "name": "Verified"})
    def test_actor_identity_always_comes_from_verified_bitrix_user(self, _verify):
        payload = {
            "auth": {"AUTH_ID": "fake", "DOMAIN": "test-fake.bitrix24.test"},
            "managerId": "77",
            "currentUserId": "1",
        }
        self.assertEqual(app.actor_id_from_payload(payload), "42")

    @patch.object(app, "verify_bitrix_user", return_value=None)
    def test_current_user_id_and_manager_id_are_ignored_when_auth_fails(self, _verify):
        with patch.multiple(app, ALLOW_UNVERIFIED_USERS=False, DRY_RUN=True):
            self.assertIsNone(
                app.actor_id_from_payload(
                    {"currentUserId": "1", "managerId": "1", "auth": {"AUTH_ID": "forged"}}
                )
            )

    @patch.object(app, "verify_bitrix_user", return_value=None)
    def test_admin_cannot_be_impersonated_with_current_user_id(self, _verify):
        self.assertIsNone(app.require_admin({"currentUserId": "1", "managerId": "1"}))

    @patch.object(app, "verify_bitrix_user", return_value={"id": "99", "name": "Not admin"})
    def test_verified_non_admin_cannot_override_identity(self, _verify):
        self.assertIsNone(app.require_admin({"currentUserId": "1", "auth": {"AUTH_ID": "token"}}))

    @patch.object(
        app,
        "verify_bitrix_user",
        return_value={
            "id": "2",
            "name": "Admin",
            "raw": {"ACTIVE": True, "UF_DEPARTMENT": [1]},
        },
    )
    def test_verified_configured_admin_is_accepted(self, _verify):
        self.assertEqual(app.require_admin({"auth": {"AUTH_ID": "token"}})["id"], "2")

    @patch.object(app, "bitrix_call")
    def test_actor_call_never_falls_back_to_privileged_webhook(self, webhook_call):
        with self.assertRaises(PermissionError):
            app.bitrix_call_for_actor({}, "crm.deal.get", {"id": "10"})
        webhook_call.assert_not_called()

    @patch.object(app, "bitrix_call")
    @patch.object(app, "bitrix_oauth_call", side_effect=RuntimeError("invalid token"))
    def test_fake_oauth_token_does_not_fallback_to_webhook(self, oauth_call, webhook_call):
        auth = {"AUTH_ID": "fake", "DOMAIN": "test-fake.bitrix24.test"}
        self.assertIsNone(app.verify_bitrix_user(auth))
        self.assertEqual(oauth_call.call_count, 2)  # user.current, then profile
        webhook_call.assert_not_called()


class TestVerifiedUserCache(unittest.TestCase):
    def setUp(self):
        app.USER_VERIFY_CACHE.clear()

    def tearDown(self):
        app.USER_VERIFY_CACHE.clear()

    def test_verified_token_is_reused_without_a_second_oauth_call(self):
        auth = {"AUTH_ID": "token-a", "DOMAIN": "test-fake.bitrix24.test"}
        upstream_user = {
            "ID": "42",
            "NAME": "Verified",
            "ACTIVE": True,
            "UF_DEPARTMENT": [1],
        }
        with (
            patch.object(app, "USER_VERIFY_CACHE_TTL_SECONDS", 300),
            patch.object(app.time, "monotonic", side_effect=[100.0, 101.0]),
            patch.object(app, "bitrix_oauth_call", return_value=upstream_user) as oauth_call,
        ):
            first = app.verify_bitrix_user(auth, allow_cached=False)
            second = app.verify_bitrix_user(auth, allow_cached=True)

        self.assertEqual(first["id"], "42")
        self.assertEqual(second["id"], "42")
        oauth_call.assert_called_once()

    def test_cache_is_bound_to_the_exact_oauth_token(self):
        auth_a = {"AUTH_ID": "token-a", "DOMAIN": "test-fake.bitrix24.test"}
        auth_b = {"AUTH_ID": "token-b", "DOMAIN": "test-fake.bitrix24.test"}
        with patch.object(app.time, "monotonic", return_value=100.0):
            with patch.object(
                app,
                "bitrix_oauth_call",
                return_value={"ID": "42", "NAME": "Verified"},
            ):
                self.assertEqual(
                    app.verify_bitrix_user(auth_a, allow_cached=False)["id"],
                    "42",
                )
            with patch.object(
                app,
                "bitrix_oauth_call",
                side_effect=RuntimeError("upstream unavailable"),
            ) as oauth_call:
                self.assertIsNone(app.verify_bitrix_user(auth_b, allow_cached=True))

        self.assertEqual(oauth_call.call_count, 2)

    def test_expired_cache_fails_closed_when_oauth_is_unavailable(self):
        auth = {"AUTH_ID": "token-a", "DOMAIN": "test-fake.bitrix24.test"}
        with (
            patch.object(app, "USER_VERIFY_CACHE_TTL_SECONDS", 300),
            patch.object(app.time, "monotonic", return_value=100.0),
            patch.object(
                app,
                "bitrix_oauth_call",
                return_value={"ID": "42", "NAME": "Verified"},
            ),
        ):
            self.assertEqual(app.verify_bitrix_user(auth, allow_cached=False)["id"], "42")

        with (
            patch.object(app, "USER_VERIFY_CACHE_TTL_SECONDS", 300),
            patch.object(app.time, "monotonic", return_value=401.0),
            patch.object(
                app,
                "bitrix_oauth_call",
                side_effect=RuntimeError("upstream unavailable"),
            ) as oauth_call,
        ):
            self.assertIsNone(app.verify_bitrix_user(auth, allow_cached=True))

        self.assertEqual(oauth_call.call_count, 2)


class TestOAuthDomainAllowlist(unittest.TestCase):
    def test_exact_configured_domain_is_allowed(self):
        self.assertEqual(
            app.normalize_bitrix_domain("https://test-fake.bitrix24.test/"),
            "test-fake.bitrix24.test",
        )

    def test_subdomain_and_lookalike_are_rejected(self):
        for domain in (
            "evil.test-fake.bitrix24.test",
            "test-fake.bitrix24.test.evil.example",
            "evil.example",
        ):
            with self.subTest(domain=domain), self.assertRaises(PermissionError):
                app.normalize_bitrix_domain(domain)

    def test_credentials_paths_queries_and_http_are_rejected(self):
        invalid = (
            "http://test-fake.bitrix24.test",
            "https://user:pass@test-fake.bitrix24.test",
            "https://test-fake.bitrix24.test/rest/",
            "https://test-fake.bitrix24.test/?next=evil",
        )
        for domain in invalid:
            with self.subTest(domain=domain), self.assertRaises(PermissionError):
                app.normalize_bitrix_domain(domain)

    def test_oauth_rejects_domain_before_any_http_request(self):
        with patch.object(app.urllib.request, "urlopen") as urlopen:
            with self.assertRaises(PermissionError):
                app.bitrix_oauth_call("evil.example", "token", "user.current")
            urlopen.assert_not_called()

    def test_malformed_ipv6_domains_are_rejected_without_parser_exception(self):
        for domain in ("https://[", "https://[::1", "["):
            with self.subTest(domain=domain), self.assertRaises(PermissionError):
                app.normalize_bitrix_domain(domain)


class TestBrowserBootstrapSafety(unittest.TestCase):
    def run_rendered_post_json(self, scenario):
        node = shutil.which("node")
        if not node:
            self.skipTest("Node.js is unavailable for the browser behavior check")
        rendered = app.render_index_html(initial_auth={}, nonce="fixed-nonce")
        marker = '<script nonce="fixed-nonce">'
        inline_script = rendered.rsplit(marker, 1)[1].split("</script>", 1)[0]
        start = inline_script.index("function apiUrl(path)")
        end = inline_script.index("async function loadManagers()", start)
        script = inline_script[start:end] + "\n" + scenario
        completed = subprocess.run(
            [node],
            input=script,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr or completed.stdout)
        return json.loads(completed.stdout)

    def run_rendered_claim_selected(self, payload):
        node = shutil.which("node")
        if not node:
            self.skipTest("Node.js is unavailable for the browser behavior check")
        rendered = app.render_index_html(initial_auth={}, nonce="fixed-nonce")
        marker = '<script nonce="fixed-nonce">'
        inline_script = rendered.rsplit(marker, 1)[1].split("</script>", 1)[0]
        start = inline_script.index("async function claimSelected()")
        end = inline_script.index("async function rejectDeal(", start)
        claim_script = inline_script[start:end]
        scenario = f"""
let selectedDealId = '100';
let currentDeal = {{
  id: '100',
  dealUrl: 'https://test-fake.bitrix24.test/crm/deal/details/100/',
  selectionToken: 'signed-selection'
}};
const managerInput = {{value: '42'}};
const claimButton = {{disabled: false, textContent: 'Взять в работу'}};
global.document = {{
  getElementById: (id) => id === 'managerId' ? managerInput : null,
  querySelector: (selector) => selector === '.claim-button' ? claimButton : null
}};
const openCalls = [];
const popup = {{
  location: {{href: 'about:blank'}},
  closed: false,
  close() {{ this.closed = true; }}
}};
global.window = {{
  open(url, target) {{
    openCalls.push({{url, target}});
    return popup;
  }}
}};
let shownPayload = null;
let adminLoads = 0;
let statusLoads = 0;
function currentAuth() {{ return {{AUTH_ID: 'token'}}; }}
async function postJson() {{ return {json.dumps(payload, ensure_ascii=False)}; }}
function showResult(value) {{ shownPayload = value; }}
function renderGreeting() {{}}
function loadAdminPanel() {{ adminLoads += 1; }}
function loadExtraClaimStatus() {{ statusLoads += 1; }}
(async () => {{
  await claimSelected();
  process.stdout.write(JSON.stringify({{
    openCalls,
    popupClosed: popup.closed,
    popupHref: popup.location.href,
    shownPayload,
    adminLoads,
    statusLoads
  }}));
}})().catch((error) => {{
  process.stderr.write(error.stack || String(error));
  process.exitCode = 1;
}});
"""
        completed = subprocess.run(
            [node],
            input=claim_script + "\n" + scenario,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr or completed.stdout)
        return json.loads(completed.stdout)

    def test_json_for_script_neutralizes_script_breakout(self):
        encoded = app.json_for_script({"token": "</script><script>alert(1)</script>&"})
        self.assertNotIn("</script", encoded.lower())
        self.assertNotIn("<script", encoded.lower())
        self.assertNotIn("&", encoded)
        self.assertIn("\\u003c", encoded)

    def test_sanitized_initial_auth_drops_refresh_and_unneeded_fields(self):
        sanitized = app.sanitize_initial_auth(
            {
                "AUTH_ID": "access-token",
                "REFRESH_ID": "refresh-token-must-never-reach-browser",
                "DOMAIN": "test-fake.bitrix24.test",
                "member_id": "member",
                "client_endpoint": "https://test-fake.bitrix24.test/rest/",
            }
        )
        self.assertEqual(
            sanitized,
            {"AUTH_ID": "access-token", "DOMAIN": "test-fake.bitrix24.test"},
        )

    def test_rendered_bootstrap_does_not_contain_refresh_token_or_xss_payload(self):
        rendered = app.render_index_html(
            initial_auth={
                "AUTH_ID": "</script><script>alert(1)</script>",
                "REFRESH_ID": "super-secret-refresh",
                "DOMAIN": "test-fake.bitrix24.test",
            },
            nonce="fixed-nonce",
        )
        self.assertNotIn("super-secret-refresh", rendered)
        self.assertNotIn("</script><script>alert(1)</script>", rendered.lower())
        self.assertIn('nonce="fixed-nonce"', rendered)

    def test_malformed_client_endpoint_is_dropped_without_exposing_auth(self):
        malformed_auth = {"AUTH_ID": "must-not-be-rendered", "client_endpoint": "https://["}

        self.assertEqual(app.extract_auth_credentials(malformed_auth), (None, None))
        rendered = app.render_index_html(initial_auth=malformed_auth, nonce="fixed-nonce")
        self.assertNotIn("must-not-be-rendered", rendered)

    def test_rendered_inline_javascript_is_syntactically_valid(self):
        node = shutil.which("node")
        if not node:
            self.skipTest("Node.js is unavailable for the browser syntax check")
        rendered = app.render_index_html(initial_auth={}, nonce="fixed-nonce")
        marker = '<script nonce="fixed-nonce">'
        script = rendered.rsplit(marker, 1)[1].split("</script>", 1)[0]
        checked = subprocess.run(
            [node, "--check"],
            input=script,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(checked.returncode, 0, checked.stderr or checked.stdout)

    def test_dry_run_claim_closes_placeholder_and_never_opens_deal(self):
        result = self.run_rendered_claim_selected(
            {
                "ok": True,
                "dryRun": True,
                "message": "Проверка успешна. В безопасном режиме CRM не изменена.",
            }
        )

        self.assertEqual(result["openCalls"], [{"url": "about:blank", "target": "_blank"}])
        self.assertTrue(result["popupClosed"])
        self.assertEqual(result["popupHref"], "about:blank")
        self.assertIn("CRM не изменена", result["shownPayload"]["message"])
        self.assertEqual(result["adminLoads"], 0)
        self.assertEqual(result["statusLoads"], 0)

    def test_real_claim_navigates_placeholder_to_bitrix_deal(self):
        result = self.run_rendered_claim_selected(
            {"ok": True, "dryRun": False, "message": "Сделка назначена."}
        )

        self.assertEqual(result["openCalls"], [{"url": "about:blank", "target": "_blank"}])
        self.assertFalse(result["popupClosed"])
        self.assertEqual(
            result["popupHref"],
            "https://test-fake.bitrix24.test/crm/deal/details/100/",
        )
        self.assertEqual(result["adminLoads"], 1)
        self.assertEqual(result["statusLoads"], 1)

    def test_next_deal_retries_one_fetch_rejection_then_returns_success(self):
        result = self.run_rendered_post_json(
            """
let fetchCalls = 0;
global.fetch = async () => {
  fetchCalls += 1;
  if (fetchCalls === 1) throw new TypeError('Load failed');
  return {
    ok: true,
    text: async () => JSON.stringify({ok: true, attempt: fetchCalls})
  };
};
(async () => {
  const data = await postJson('/api/next-deal', {managerId: '42'});
  process.stdout.write(JSON.stringify({fetchCalls, data}));
})().catch((error) => {
  process.stderr.write(error.stack || String(error));
  process.exitCode = 1;
});
"""
        )
        self.assertEqual(result["fetchCalls"], 2)
        self.assertEqual(result["data"], {"ok": True, "attempt": 2})

    def test_next_deal_two_fetch_rejections_show_safe_russian_error(self):
        result = self.run_rendered_post_json(
            """
let fetchCalls = 0;
global.fetch = async () => {
  fetchCalls += 1;
  throw new TypeError('Load failed');
};
(async () => {
  try {
    await postJson('/api/next-deal', {managerId: '42'});
    process.stdout.write(JSON.stringify({fetchCalls, threw: false}));
  } catch (error) {
    process.stdout.write(JSON.stringify({fetchCalls, threw: true, message: error.message}));
  }
})().catch((error) => {
  process.stderr.write(error.stack || String(error));
  process.exitCode = 1;
});
"""
        )
        self.assertEqual(result["fetchCalls"], 2)
        self.assertTrue(result["threw"])
        self.assertEqual(
            result["message"],
            "Соединение с сервисом прервалось. Проверьте интернет и повторите поиск.",
        )
        self.assertNotIn("Load failed", result["message"])

    def test_next_deal_retries_when_reading_response_body_is_interrupted(self):
        result = self.run_rendered_post_json(
            """
let fetchCalls = 0;
let textCalls = 0;
global.fetch = async () => {
  fetchCalls += 1;
  const currentAttempt = fetchCalls;
  return {
    ok: true,
    text: async () => {
      textCalls += 1;
      if (currentAttempt === 1) throw new TypeError('Load failed');
      return JSON.stringify({ok: true, attempt: currentAttempt});
    }
  };
};
(async () => {
  const data = await postJson('/api/next-deal', {managerId: '42'});
  process.stdout.write(JSON.stringify({fetchCalls, textCalls, data}));
})().catch((error) => {
  process.stderr.write(error.stack || String(error));
  process.exitCode = 1;
});
"""
        )
        self.assertEqual(result["fetchCalls"], 2)
        self.assertEqual(result["textCalls"], 2)
        self.assertEqual(result["data"], {"ok": True, "attempt": 2})

    def test_next_deal_does_not_retry_a_readable_http_error(self):
        result = self.run_rendered_post_json(
            """
let fetchCalls = 0;
global.fetch = async () => {
  fetchCalls += 1;
  return {
    ok: false,
    text: async () => JSON.stringify({message: 'Дневной лимит уже достигнут.'})
  };
};
(async () => {
  try {
    await postJson('/api/next-deal', {managerId: '42'});
    process.stdout.write(JSON.stringify({fetchCalls, threw: false}));
  } catch (error) {
    process.stdout.write(JSON.stringify({fetchCalls, threw: true, message: error.message}));
  }
})().catch((error) => {
  process.stderr.write(error.stack || String(error));
  process.exitCode = 1;
});
"""
        )
        self.assertEqual(result["fetchCalls"], 1)
        self.assertTrue(result["threw"])
        self.assertEqual(result["message"], "Дневной лимит уже достигнут.")

    def test_next_deal_does_not_retry_http_503_with_interrupted_body(self):
        result = self.run_rendered_post_json(
            """
let fetchCalls = 0;
global.fetch = async () => {
  fetchCalls += 1;
  return {
    ok: false,
    status: 503,
    text: async () => { throw new TypeError('Load failed'); }
  };
};
(async () => {
  try {
    await postJson('/api/next-deal', {managerId: '42'});
    process.stdout.write(JSON.stringify({fetchCalls, threw: false}));
  } catch (error) {
    process.stdout.write(JSON.stringify({fetchCalls, threw: true, message: error.message}));
  }
})().catch((error) => {
  process.stderr.write(error.stack || String(error));
  process.exitCode = 1;
});
"""
        )
        self.assertEqual(result["fetchCalls"], 1)
        self.assertTrue(result["threw"])
        self.assertEqual(
            result["message"],
            "Сервис поиска временно недоступен. Подождите минуту и повторите поиск.",
        )
        self.assertNotIn("Load failed", result["message"])


class TestSelectionTokens(unittest.TestCase):
    def test_token_is_bound_to_deal_and_manager(self):
        token = app.issue_selection_token("100", "42", "version-1", "a" * 64, now=1_000)
        self.assertTrue(app.verify_selection_token(token, "100", "42", now=1_000))
        self.assertFalse(app.verify_selection_token(token, "101", "42", now=1_000))
        self.assertFalse(app.verify_selection_token(token, "100", "43", now=1_000))

    def test_expired_token_is_rejected(self):
        token = app.issue_selection_token("100", "42", "version-1", "a" * 64, now=1_000)
        self.assertTrue(
            app.verify_selection_token(
                token,
                "100",
                "42",
                now=1_000 + app.SELECTION_TOKEN_TTL_SECONDS,
            )
        )
        self.assertFalse(
            app.verify_selection_token(
                token,
                "100",
                "42",
                now=1_001 + app.SELECTION_TOKEN_TTL_SECONDS,
            )
        )

    def test_tampered_token_is_rejected(self):
        token = app.issue_selection_token("100", "42", "version-1", "a" * 64, now=1_000)
        encoded, signature = token.split(".", 1)
        replacement = ("0" if signature[0] != "0" else "1") + signature[1:]
        self.assertFalse(app.verify_selection_token(f"{encoded}.{replacement}", "100", "42", now=1_000))

    def test_malformed_and_missing_tokens_are_rejected(self):
        for token in (None, "", "one.two.three", "not-base64.signature"):
            with self.subTest(token=token):
                self.assertFalse(app.verify_selection_token(token, "100", "42"))

    def test_last_timeline_activity_changes_the_signed_deal_lifecycle(self):
        before = {
            "DATE_MODIFY": "2026-08-17T09:00:00+06:00",
            "LAST_ACTIVITY_TIME": "2026-08-17T09:01:00+06:00",
        }
        after = {**before, "LAST_ACTIVITY_TIME": "2026-08-17T09:02:00+06:00"}

        self.assertNotEqual(app.deal_version(before), app.deal_version(after))

    def test_token_from_previous_routing_policy_is_rejected_after_deploy(self):
        token = app.issue_selection_token(
            "100", "42", "version-1", "a" * 64, now=1_000
        )
        with patch.object(app, "ROUTING_POLICY_VERSION", "next-routing-policy"):
            self.assertFalse(
                app.verify_selection_token(token, "100", "42", now=1_000)
            )


class TestClassifierBoundaries(unittest.TestCase):
    def test_female_does_not_match_male_or_maldives(self):
        result = app.classify(["A female traveller asked for a quiet resort"])
        self.assertEqual(result["direction"], "Не определено")

    def test_inside_does_not_match_side_or_turkey(self):
        result = app.classify(["We will stay inside the hotel all week"])
        self.assertEqual(result["direction"], "Не определено")

    def test_cyrillic_stem_still_matches_inflected_destination(self):
        result = app.classify(["Хотим отдохнуть на Мальдивах"])
        self.assertEqual(result["direction"], "Мальдивы")


class TestPagination(unittest.TestCase):
    def test_list_all_follows_every_next_cursor(self):
        pages = {
            0: {"result": [{"ID": "1"}, {"ID": "2"}], "next": 50},
            50: {"result": [{"ID": "3"}], "next": 100},
            100: {"result": [{"ID": "4"}]},
        }

        def fake_call(_method, params, timeout=None):
            return pages[params["start"]]

        with patch.object(app, "bitrix_call_full", side_effect=fake_call) as call_full:
            result = app.bitrix_list_all("crm.deal.list", {"filter[X]": "Y"}, max_items=10)
        self.assertEqual([item["ID"] for item in result], ["1", "2", "3", "4"])
        self.assertEqual([item.args[1]["start"] for item in call_full.call_args_list], [0, 50, 100])
        self.assertTrue(all(item.args[1]["filter[X]"] == "Y" for item in call_full.call_args_list))

    def test_list_all_respects_max_items(self):
        with patch.object(
            app,
            "bitrix_call_full",
            return_value={"result": [{"ID": str(i)} for i in range(10)], "next": 50},
        ) as call_full:
            result = app.bitrix_list_all("crm.deal.list", max_items=3)
        self.assertEqual(len(result), 3)
        call_full.assert_called_once()

    def test_repeated_cursor_fails_instead_of_looping_forever(self):
        with patch.object(
            app,
            "bitrix_call_full",
            side_effect=[{"result": [], "next": 50}, {"result": [], "next": 50}],
        ):
            with self.assertRaises(RuntimeError):
                app.bitrix_list_all("crm.deal.list", max_items=10)

    def test_list_deadline_is_checked_after_page_response(self):
        with (
            patch.object(app, "bitrix_call_full", return_value={"result": []}),
            patch.object(app.time, "monotonic", side_effect=[0.0, 0.1, 1.1]),
        ):
            with self.assertRaisesRegex(TimeoutError, "общий таймаут списка"):
                app.bitrix_list_all("crm.deal.list", max_items=10, timeout=1.0)


class TestBitrixResponseBounds(unittest.TestCase):
    class FakeResponse:
        def __init__(self, body, content_length=None):
            self.body = body
            self.headers = {}
            if content_length is not None:
                self.headers["Content-Length"] = str(content_length)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, limit=-1):
            return self.body if limit < 0 else self.body[:limit]

    def test_webhook_response_without_length_is_bounded_and_never_echoed(self):
        secret_body = b'{"result":"private-customer-data-' + (b"x" * 100) + b'"}'
        response = self.FakeResponse(secret_body)
        with (
            patch.object(app, "MAX_BITRIX_RESPONSE_BYTES", 32),
            patch.object(app.urllib.request, "urlopen", return_value=response),
        ):
            with self.assertRaisesRegex(RuntimeError, "безопасный размер") as raised:
                app.bitrix_call_full("crm.deal.get", {"id": "1"})

        self.assertNotIn("private-customer-data", str(raised.exception))

    def test_oauth_response_content_length_is_rejected_before_decode(self):
        response = self.FakeResponse(b'{"result":{}}', content_length=10_000)
        with (
            patch.object(app, "MAX_BITRIX_RESPONSE_BYTES", 32),
            patch.object(app.urllib.request, "urlopen", return_value=response),
        ):
            with self.assertRaisesRegex(RuntimeError, "безопасный размер"):
                app.bitrix_oauth_call(
                    "test-fake.bitrix24.test", "token", "user.current"
                )


class TestManagerMatching(unittest.TestCase):
    def test_competency_matching_uses_word_boundaries(self):
        deal = {
            "messages": ["female traveler"],
            "classification": {"direction": "Не определено", "confidence": "низкая"},
        }
        # Unknown directions remain available to skilled managers by policy,
        # so use a known direction to verify the text score itself.
        deal["classification"]["direction"] = "Египет"
        self.assertEqual(app.deal_score_for_manager(deal, {"competencies": ["male"]}), 0)

    def test_manager_without_skills_gets_zero_even_for_unknown_direction(self):
        deal = {
            "messages": [],
            "classification": {"direction": "Не определено", "confidence": "низкая"},
        }
        self.assertEqual(app.deal_score_for_manager(deal, {"competencies": []}), 0)

    def test_manager_without_skills_gets_zero_for_known_direction(self):
        deal = {
            "messages": ["Хочу в Турцию"],
            "classification": {"direction": "Турция", "confidence": "средняя"},
        }
        self.assertEqual(app.deal_score_for_manager(deal, {"competencies": []}), 0)


class TestExactApplicationEventStats(TemporaryStateTestCase):
    def test_admin_stats_use_only_persisted_application_events(self):
        self.store.append_claim(
            {
                "timestamp": "2026-08-17T10:00:00+06:00",
                "managerId": "1001",
                "managerName": "Manager",
                "dealId": "100",
                "dealTitle": "One",
            }
        )
        self.store.append_claim(
            {
                "timestamp": "2026-08-17T11:00:00+06:00",
                "managerId": "1001",
                "managerName": "Manager",
                "dealId": "101",
                "dealTitle": "Two",
            }
        )
        payload = {"dateFrom": "2026-08-17", "dateTo": "2026-08-17", "currentUserId": "999"}
        with (
            patch.object(app, "require_admin", return_value={"id": "1", "name": "Admin"}),
            patch.object(app, "local_date", return_value="2026-08-17"),
            patch.object(app, "list_portal_users", return_value=[{"ID": "1001", "NAME": "M"}]),
            patch.object(
                app,
                "get_manager_profiles_bulk",
                return_value={"1001": {"name": "Manager", "competencies": ["Турция"]}},
            ),
            patch.object(app, "bitrix_call") as crm_call,
        ):
            result = app.admin_state(payload)

        self.assertTrue(result["ok"])
        self.assertEqual(result["statsSource"], "app_events")
        self.assertEqual(result["statsLabel"], "Взято через приложение")
        self.assertEqual(result["managers"][0]["takenInPeriod"], 2)
        self.assertTrue(any("Ручные переводы" in warning for warning in result["warnings"]))
        crm_call.assert_not_called()


class TestReadiness(TemporaryStateTestCase):
    def _ready_patches(self, **overrides):
        values = {
            "APP_DIR": self.data_dir,
            "MANAGERS_FILE": self.data_dir / "managers.json",
            "PUBLIC_APP_URL": "https://picker.example.test",
            "RAW_APP_ALLOWED_ORIGINS": {"https://picker.example.test"},
            "APP_ALLOWED_ORIGINS": {"https://picker.example.test"},
            "RAW_BITRIX_ALLOWED_DOMAINS": {"test-fake.bitrix24.test"},
            "ALLOWED_BITRIX_DOMAINS": {"test-fake.bitrix24.test"},
            "ADMIN_USER_IDS": {"1"},
            "CLAIM_STATS_SOURCE": "app_events",
            "REQUIRE_LEGACY_MIGRATION": False,
            "REQUIRE_EXPLICIT_ACCESS_RULE": True,
            "STATE_STORE": self.store,
        }
        values.update(overrides)
        return patch.multiple(app, **values)

    def test_valid_configuration_and_store_are_ready(self):
        with self._ready_patches():
            result = app.readiness_state(force=True)
        self.assertTrue(result["ok"], result["errors"])
        self.assertTrue(result["storage"]["ok"])
        self.assertEqual(result["storage"]["journalMode"], "wal")

    def test_unsupported_stats_source_fails_closed(self):
        with self._ready_patches(CLAIM_STATS_SOURCE="crm_guess"):
            result = app.readiness_state(force=True)
        self.assertFalse(result["ok"])
        self.assertTrue(any("app_events" in error for error in result["errors"]))

    def test_production_cannot_disable_explicit_manager_rules(self):
        with self._ready_patches(REQUIRE_EXPLICIT_ACCESS_RULE=False):
            result = app.readiness_state(force=True)

        self.assertFalse(result["ok"])
        self.assertTrue(
            any("REQUIRE_EXPLICIT_ACCESS_RULE" in error for error in result["errors"])
        )

    def test_broken_store_fails_readiness(self):
        broken = MagicMock()
        broken.readiness_check.return_value = {"ok": False, "error": "corrupt database"}
        with self._ready_patches(STATE_STORE=broken):
            result = app.readiness_state(force=True)
        self.assertFalse(result["ok"])
        self.assertFalse(result["storage"]["ok"])
        self.assertNotIn("corrupt database", result["errors"])

    def test_corrupt_autoclose_boundary_returns_controlled_not_ready(self):
        with (
            self._ready_patches(),
            patch.object(
                self.store,
                "get_lost_deal_autoclose_boundary",
                side_effect=RuntimeError("secret database detail"),
            ),
        ):
            result = app.readiness_state(force=True)
        self.assertFalse(result["ok"])
        self.assertFalse(result["lostDealAutoclose"]["armed"])
        rendered = json.dumps(result, ensure_ascii=False)
        self.assertNotIn("secret database detail", rendered)

    def test_missing_public_url_and_origins_fail_readiness(self):
        with self._ready_patches(PUBLIC_APP_URL="", RAW_APP_ALLOWED_ORIGINS=set(), APP_ALLOWED_ORIGINS=set()):
            result = app.readiness_state(force=True)
        self.assertFalse(result["ok"])
        self.assertTrue(any("PUBLIC_APP_URL" in error for error in result["errors"]))
        self.assertTrue(any("APP_ALLOWED_ORIGINS" in error for error in result["errors"]))

    def test_malformed_url_configuration_returns_not_ready_instead_of_crashing(self):
        with (
            patch.dict(os.environ, {"BITRIX_WEBHOOK_BASE": "https://["}),
            self._ready_patches(
                PUBLIC_APP_URL="https://[",
                RAW_APP_ALLOWED_ORIGINS={"https://["},
                APP_ALLOWED_ORIGINS=set(),
            ),
        ):
            with self.assertRaises(RuntimeError):
                app.load_env()
            result = app.readiness_state(force=True)

        self.assertFalse(result["ok"])
        self.assertTrue(any("BITRIX_WEBHOOK_BASE" in error for error in result["errors"]))
        self.assertTrue(any("PUBLIC_APP_URL" in error for error in result["errors"]))
        self.assertTrue(any("APP_ALLOWED_ORIGINS" in error for error in result["errors"]))

    def test_railway_runtime_without_attached_volume_fails_readiness(self):
        railway_env = {**_TEST_ENV, "RAILWAY_PROJECT_ID": "project-test"}
        with patch.dict(os.environ, railway_env, clear=True), self._ready_patches():
            result = app.readiness_state(force=True)

        self.assertFalse(result["ok"])
        self.assertTrue(any("Volume" in error for error in result["errors"]))

    def test_railway_volume_must_match_application_data_directory(self):
        railway_env = {
            **_TEST_ENV,
            "RAILWAY_PROJECT_ID": "project-test",
            "RAILWAY_VOLUME_NAME": "picker-data",
            "RAILWAY_VOLUME_MOUNT_PATH": str(self.data_dir.parent),
        }
        with patch.dict(os.environ, railway_env, clear=True), self._ready_patches():
            result = app.readiness_state(force=True)

        self.assertFalse(result["ok"])
        self.assertTrue(any("APP_DATA_DIR" in error for error in result["errors"]))

    def test_matching_official_railway_volume_variables_pass_readiness(self):
        railway_env = {
            **_TEST_ENV,
            "RAILWAY_PROJECT_ID": "project-test",
            "RAILWAY_VOLUME_NAME": "picker-data",
            "RAILWAY_VOLUME_MOUNT_PATH": str(self.data_dir),
        }
        with patch.dict(os.environ, railway_env, clear=True), self._ready_patches():
            result = app.readiness_state(force=True)

        self.assertTrue(result["ok"], result["errors"])

    def test_malformed_manager_list_item_fails_readiness(self):
        (self.data_dir / "managers.json").write_text(
            json.dumps(["not-an-object"]),
            encoding="utf-8",
        )
        with self._ready_patches():
            result = app.readiness_state(force=True)

        self.assertFalse(result["ok"])
        self.assertTrue(any("managers.json" in error for error in result["errors"]))


class TestHttpBoundary(unittest.TestCase):
    def test_manager_action_cache_policy_keeps_search_fresh(self):
        cases = (
            ("/api/next-deal", False, "get_next_deal_for_manager", {"deal": None}),
            ("/api/claim", True, "preview_claim", {"ok": True}),
            ("/api/reject", True, "record_rejection", {"ok": True}),
        )
        for path, allow_cached, operation_name, operation_result in cases:
            with self.subTest(path=path):
                payload = {
                    "auth": {
                        "AUTH_ID": "verified-token",
                        "DOMAIN": "test-fake.bitrix24.test",
                    },
                    "managerId": "999",
                    "dealId": "100",
                    "selectionToken": "signed-selection",
                }
                handler = HandlerHarness.make("POST", path, payload)
                with (
                    patch.object(app, "rate_limit_allowed", return_value=True),
                    patch.object(app, "readiness_state", return_value={"ok": True}),
                    patch.object(app, "actor_id_from_payload", return_value="42") as actor,
                    patch.object(app, operation_name, return_value=dict(operation_result)),
                ):
                    handler.do_POST()

                self.assertEqual(HandlerHarness.status(handler), 200)
                actor.assert_called_once_with(payload, allow_cached=allow_cached)

    def test_admin_and_current_user_checks_remain_fresh(self):
        auth = {"AUTH_ID": "verified-token", "DOMAIN": "test-fake.bitrix24.test"}
        with patch.object(app, "verify_bitrix_user", return_value=None) as verify:
            app.current_user_state({"auth": auth})
            verify.assert_called_once_with(auth, allow_cached=False)

        with patch.object(app, "verify_bitrix_user", return_value=None) as verify:
            app.require_admin({"auth": auth})
            verify.assert_called_once_with(auth, allow_cached=False)

    def test_access_log_does_not_persist_full_client_ip(self):
        handler = HandlerHarness.make("GET", "/api/health")
        handler.client_address = ("203.0.113.42", 12345)
        with patch.object(app.sys.stderr, "write") as write:
            handler.log_message('"%s" %s %s', "GET /api/health", "200", "2")

        rendered = "".join(item.args[0] for item in write.call_args_list)
        self.assertNotIn("203.0.113.42", rendered)
        self.assertEqual(rendered, "GET /api/health\n")

    def test_sensitive_get_routes_are_closed(self):
        for path in (
            "/api/deals?auth=secret",
            "/api/next-deal?managerId=1",
            "/api/managers",
            "/api/manager?id=1",
        ):
            with self.subTest(path=path):
                handler = HandlerHarness.make("GET", path)
                handler.do_GET()
                self.assertEqual(HandlerHarness.status(handler), 404)

    def test_malformed_urls_fail_closed_at_helpers_and_http_boundary(self):
        self.assertIsNone(app.safe_urlparse("https://["))
        self.assertEqual(app.url_origin("https://["), "")
        self.assertFalse(app.is_loopback_http_url("http://["))

        get_handler = HandlerHarness.make("GET", "https://[")
        get_handler.do_GET()
        self.assertEqual(HandlerHarness.status(get_handler), 400)

        post_handler = HandlerHarness.make("POST", "https://[", b"{}")
        post_handler.do_POST()
        self.assertEqual(HandlerHarness.status(post_handler), 400)

        origin_handler = HandlerHarness.make(
            "OPTIONS", "/api/next-deal", origin="http://["
        )
        with patch.multiple(app, ALLOW_UNVERIFIED_USERS=True, DRY_RUN=True):
            origin_handler.do_OPTIONS()
        self.assertEqual(HandlerHarness.status(origin_handler), 403)

    def test_cors_allows_only_exact_configured_origin(self):
        allowed = HandlerHarness.make("OPTIONS", "/api/next-deal", origin="https://picker.example.test")
        allowed.do_OPTIONS()
        self.assertEqual(HandlerHarness.status(allowed), 204)
        self.assertIn(
            ("Access-Control-Allow-Origin", "https://picker.example.test"),
            HandlerHarness.headers(allowed),
        )

        denied = HandlerHarness.make("OPTIONS", "/api/next-deal", origin="https://evil.example")
        denied.do_OPTIONS()
        self.assertEqual(HandlerHarness.status(denied), 403)
        self.assertNotIn(
            ("Access-Control-Allow-Origin", "https://evil.example"),
            HandlerHarness.headers(denied),
        )

    def test_missing_origin_is_allowed_for_same_origin_clients_without_wildcard_cors(self):
        handler = HandlerHarness.make("GET", "/api/health")
        handler.do_GET()
        self.assertEqual(HandlerHarness.status(handler), 200)
        self.assertFalse(any(name == "Access-Control-Allow-Origin" for name, _ in HandlerHarness.headers(handler)))

    def test_railway_real_ip_is_used_only_with_official_runtime_identity(self):
        headers = {
            "X-Railway-Edge": "edge",
            "X-Real-IP": "203.0.113.42",
        }
        handler = HandlerHarness.make("POST", "/api/current-user", headers=headers)
        with patch.dict(
            os.environ,
            {"RAILWAY_PROJECT_ID": "project-test"},
            clear=True,
        ):
            self.assertEqual(handler.client_key(), "203.0.113.42")

        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(handler.client_key(), "127.0.0.1")

    def test_oversized_request_body_is_rejected_before_read(self):
        handler = HandlerHarness.make(
            "POST",
            "/api/current-user",
            b"{}",
            headers={"Content-Length": str(app.MAX_REQUEST_BODY_BYTES + 1)},
        )
        with self.assertRaises(OverflowError):
            handler.read_body()
        self.assertEqual(handler.rfile.tell(), 0)

    def test_transfer_encoding_is_rejected(self):
        handler = HandlerHarness.make(
            "POST",
            "/api/current-user",
            b"{}",
            headers={"Transfer-Encoding": "chunked"},
        )
        with self.assertRaises(ValueError):
            handler.read_body()

    def test_json_body_must_be_an_object(self):
        handler = HandlerHarness.make("POST", "/api/current-user", b"[]")
        with self.assertRaises(ValueError):
            handler.read_json()

    def test_root_html_has_nonce_csp_and_does_not_reflect_query_auth(self):
        handler = HandlerHarness.make("GET", "/?AUTH_ID=must-not-appear&DOMAIN=evil.example")
        handler.do_GET()
        body = handler.wfile.getvalue().decode("utf-8")
        headers = HandlerHarness.headers(handler)
        csp = next(value for name, value in headers if name == "Content-Security-Policy")
        self.assertIn("script-src 'self' 'nonce-", csp)
        self.assertIn("frame-ancestors https://test-fake.bitrix24.test", csp)
        self.assertNotIn("must-not-appear", body)

    def test_regular_bitrix_launch_tokens_do_not_trigger_install_mode(self):
        payload = (
            b"AUTH_ID=access-token&DOMAIN=test-fake.bitrix24.test"
            b"&APP_SID=regular-placement&application_token=callback-token"
            b"&PLACEMENT=LEFT_MENU"
        )
        handler = HandlerHarness.make("POST", "/", payload)
        handler.do_POST()
        body = handler.wfile.getvalue().decode("utf-8")

        self.assertEqual(HandlerHarness.status(handler), 200)
        self.assertIn("const INSTALL_MODE = false;", body)

    def test_only_explicit_bitrix_install_signals_trigger_install_mode(self):
        for marker in (b"INSTALL=Y", b"event=ONAPPINSTALL"):
            with self.subTest(marker=marker):
                payload = (
                    b"AUTH_ID=access-token&DOMAIN=test-fake.bitrix24.test&" + marker
                )
                handler = HandlerHarness.make("POST", "/", payload)
                handler.do_POST()
                body = handler.wfile.getvalue().decode("utf-8")

                self.assertEqual(HandlerHarness.status(handler), 200)
                self.assertIn("const INSTALL_MODE = true;", body)


class TestRejectionRecording(TemporaryStateTestCase):
    def test_rejection_uses_live_server_deal_and_ignores_browser_deal(self):
        token = app.issue_selection_token(
            "100", "42", "version-1", test_manager_policy(), now=1_000
        )
        payload = {
            "dealId": "100",
            "reason": "duplicate",
            "selectionToken": token,
            "deal": {
                "ID": "999",
                "TITLE": "CLIENT-CONTROLLED",
                "STAGE_ID": app.TARGET_STAGE,
                "classification": {"direction": "CLIENT-CONTROLLED"},
            },
        }
        server_deal = {
            "ID": "100",
            "TITLE": "SERVER-TITLE",
            "STAGE_ID": next(iter(app.SOURCE_STAGES)),
            "DATE_MODIFY": "version-1",
        }
        with (
            patch.object(app.time, "time", return_value=1_000),
            patch.object(app, "bitrix_call", return_value=server_deal) as bitrix_call,
            patch.object(
                app,
                "get_manager_profile",
                return_value={
                    "id": "42",
                    "name": "Manager",
                    "active": True,
                    "intranet": True,
                    "competencies": ["Турция"],
                },
            ),
        ):
            result = app.record_rejection("42", payload)

        self.assertTrue(result["ok"])
        saved = self.store.list_rejections()
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0]["dealId"], "100")
        self.assertEqual(saved[0]["dealTitle"], "")
        self.assertNotIn("CLIENT-CONTROLLED", json.dumps(saved[0], ensure_ascii=False))
        bitrix_call.assert_called_once_with("crm.deal.get", {"id": "100"})

    def test_repeated_rejection_token_is_idempotent(self):
        token = app.issue_selection_token(
            "100", "42", "version-1", test_manager_policy(), now=1_000
        )
        payload = {"dealId": "100", "reason": "other", "selectionToken": token}
        server_deal = {
            "ID": "100",
            "TITLE": "Server",
            "STAGE_ID": next(iter(app.SOURCE_STAGES)),
            "DATE_MODIFY": "version-1",
        }
        with (
            patch.object(app.time, "time", return_value=1_000),
            patch.object(app, "bitrix_call", return_value=server_deal) as bitrix_call,
            patch.object(
                app,
                "get_manager_profile",
                return_value={
                    "id": "42",
                    "name": "Manager",
                    "active": True,
                    "intranet": True,
                    "competencies": ["Турция"],
                },
            ),
        ):
            first = app.record_rejection("42", payload)
            second = app.record_rejection("42", payload)

        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertTrue(second["idempotentReplay"])
        self.assertEqual(len(self.store.list_rejections()), 1)
        bitrix_call.assert_called_once()

    def test_invalid_selection_token_never_reads_bitrix(self):
        with patch.object(app, "bitrix_call") as bitrix_call:
            result = app.record_rejection(
                "42", {"dealId": "100", "reason": "other", "selectionToken": "forged"}
            )
        self.assertFalse(result["ok"])
        bitrix_call.assert_not_called()


class ClaimWorkflowTestCase(TemporaryStateTestCase):
    manager_id = "42"
    deal_id = "100"
    version = "version-1"

    def operation_key(self):
        return app.claim_operation_key(self.deal_id, self.version)

    def attempt_marker(self, manager_id=None):
        return app.claim_attempt_marker(
            self.operation_key(), manager_id or self.manager_id, nonce="test-attempt"
        )

    def begin_operation(self, manager_id=None):
        manager_id = manager_id or self.manager_id
        return self.store.begin_claim_operation(
            self.deal_id,
            manager_id,
            operation_key=self.operation_key(),
            request={
                "claimMarker": self.attempt_marker(manager_id),
                "dealVersion": self.version,
            },
        )

    def token(self, manager_id=None):
        return app.issue_selection_token(
            self.deal_id,
            manager_id or self.manager_id,
            self.version,
            test_manager_policy(),
            now=1_000,
        )

    def source_deal(self, *, manager="9"):
        return {
            "ID": self.deal_id,
            "TITLE": "Server deal",
            "STAGE_ID": next(iter(app.SOURCE_STAGES)),
            "ASSIGNED_BY_ID": manager,
            "DATE_MODIFY": self.version,
        }

    def claimed_deal(self, *, manager=None):
        return {
            "ID": self.deal_id,
            "TITLE": "Server deal",
            "STAGE_ID": app.TARGET_STAGE,
            "ASSIGNED_BY_ID": manager or self.manager_id,
            "DATE_MODIFY": "version-after-claim",
            app.BITRIX_CLAIM_MARKER_FIELD: self.attempt_marker(manager),
        }

    def claim_side_effect(self, get_results, *, update_error=None):
        remaining = iter(get_results)

        def fake_bitrix(method, params=None, timeout=None):
            if method == "crm.deal.get":
                return next(remaining)
            if method == "crm.deal.update":
                if update_error:
                    raise update_error
                return True
            raise AssertionError(f"unexpected Bitrix method: {method}")

        return fake_bitrix

    @contextmanager
    def common_claim_context(self, *, dry_run=False, greeting=True):
        patchers = [
            patch.object(app, "DRY_RUN", dry_run),
            patch.object(app.time, "time", return_value=1_000),
            patch.object(app.secrets, "token_hex", return_value="test-attempt"),
            patch.object(
                app,
                "get_manager_profile",
                return_value={
                    "id": self.manager_id,
                    "name": "Manager",
                    "active": True,
                    "intranet": True,
                    "competencies": ["Турция"],
                },
            ),
        ]
        if greeting:
            patchers.append(
                patch.object(app, "prepare_greeting", return_value={"ok": True, "status": "manual"})
            )
        with ExitStack() as stack:
            for patcher in patchers:
                stack.enter_context(patcher)
            yield

    def backdate_operation(self, operation_key=None):
        operation_key = operation_key or self.operation_key()
        with self.store._connect() as connection:
            connection.execute(
                "UPDATE claim_operations SET created_at=?, updated_at=? WHERE operation_key=?",
                ("2000-01-01T00:00:00+00:00", "2000-01-01T00:00:00+00:00", operation_key),
            )

    def set_operation_timestamps(
        self,
        created_at,
        *,
        updated_at=None,
        finalized_at=None,
        operation_key=None,
    ):
        operation_key = operation_key or self.operation_key()
        with self.store._connect() as connection:
            connection.execute(
                """
                UPDATE claim_operations
                SET created_at=?, updated_at=?, finalized_at=?
                WHERE operation_key=?
                """,
                (
                    created_at,
                    updated_at or created_at,
                    finalized_at,
                    operation_key,
                ),
            )


class TestClaimWorkflow(ClaimWorkflowTestCase):
    def test_unknown_live_claim_marker_without_local_operation_is_never_overwritten(self):
        live = self.source_deal()
        live[app.BITRIX_CLAIM_MARKER_FIELD] = "claim:unknown-surviving-evidence"
        with (
            self.common_claim_context(),
            patch.object(app, "bitrix_call", return_value=live) as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["_httpStatus"], 409)
        self.assertTrue(result["recoveryPending"])
        self.assertNotIn(
            "crm.deal.update",
            [item.args[0] for item in bitrix_call.call_args_list],
        )
        self.assertIsNone(self.store.get_claim_operation(self.operation_key()))

    def test_marker_appearing_between_reads_is_preserved_and_operation_stays_unresolved(self):
        first = self.source_deal()
        second = self.source_deal()
        second[app.BITRIX_CLAIM_MARKER_FIELD] = "claim:foreign-racing-evidence"
        with (
            self.common_claim_context(),
            patch.object(
                app,
                "bitrix_call",
                side_effect=[first, second],
            ) as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["_httpStatus"], 409)
        self.assertTrue(result["recoveryPending"])
        self.assertNotIn(
            "crm.deal.update",
            [item.args[0] for item in bitrix_call.call_args_list],
        )
        operation = self.store.get_claim_operation(self.operation_key())
        self.assertEqual(operation["status"], "failed")
        self.assertTrue(operation["result"]["recoveryRequired"])

    def test_resolved_old_marker_allows_an_intentional_new_lifecycle(self):
        old_version = "old-version"
        old_key = app.claim_operation_key(self.deal_id, old_version)
        old_marker = app.claim_attempt_marker(
            old_key,
            self.manager_id,
            nonce="old-attempt",
        )
        self.store.begin_claim_operation(
            self.deal_id,
            self.manager_id,
            operation_key=old_key,
            request={
                "claimMarker": old_marker,
                "dealVersion": old_version,
            },
        )
        self.store.finalize_claim_operation(
            old_key,
            claim={"managerId": self.manager_id, "dealId": self.deal_id},
            expected_claim_marker=old_marker,
        )
        source_with_old_marker = self.source_deal()
        source_with_old_marker[app.BITRIX_CLAIM_MARKER_FIELD] = old_marker
        side_effect = self.claim_side_effect(
            [
                source_with_old_marker,
                source_with_old_marker,
                self.claimed_deal(),
            ]
        )
        with (
            self.common_claim_context(),
            patch.object(app, "bitrix_call", side_effect=side_effect) as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            [item.args[0] for item in bitrix_call.call_args_list].count("crm.deal.update"),
            1,
        )
        self.assertEqual(self.store.get_claim_operation(self.operation_key())["status"], "succeeded")

    def test_dry_run_never_updates_crm_or_writes_claim_event(self):
        token = self.token()
        with (
            self.common_claim_context(dry_run=True),
            patch.object(app, "bitrix_call", return_value=self.source_deal()) as bitrix_call,
        ):
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=token)

        self.assertTrue(result["ok"])
        self.assertTrue(result["dryRun"])
        self.assertEqual(self.store.count_claims(self.manager_id), 0)
        self.assertIsNone(self.store.get_claim_operation(self.operation_key()))
        self.assertNotIn("crm.deal.update", [item.args[0] for item in bitrix_call.call_args_list])

    def test_new_timeline_activity_invalidates_old_selection_before_crm_update(self):
        selected = self.source_deal()
        selected["LAST_ACTIVITY_TIME"] = "2026-08-17T09:01:00+06:00"
        selected_version = app.deal_version(selected)
        token = app.issue_selection_token(
            self.deal_id,
            self.manager_id,
            selected_version,
            test_manager_policy(),
            now=1_000,
        )
        live = dict(selected)
        live["LAST_ACTIVITY_TIME"] = "2026-08-17T09:02:00+06:00"

        with (
            self.common_claim_context(),
            patch.object(app, "bitrix_call", return_value=live) as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=token,
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["_httpStatus"], 409)
        self.assertNotIn(
            "crm.deal.update",
            [item.args[0] for item in bitrix_call.call_args_list],
        )

    def test_success_updates_verifies_and_atomically_finalizes_audit(self):
        token = self.token()
        side_effect = self.claim_side_effect(
            [self.source_deal(), self.source_deal(), self.claimed_deal()]
        )
        with (
            self.common_claim_context(),
            patch.object(app, "bitrix_call", side_effect=side_effect) as bitrix_call,
        ):
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=token)

        self.assertTrue(result["ok"])
        self.assertTrue(result["auditRecorded"])
        self.assertEqual(result["updated"]["ASSIGNED_BY_ID"], self.manager_id)
        self.assertIn(
            call(
                "crm.deal.update",
                {
                    "id": self.deal_id,
                    "fields[ASSIGNED_BY_ID]": self.manager_id,
                    "fields[STAGE_ID]": app.TARGET_STAGE,
                    f"fields[{app.BITRIX_CLAIM_MARKER_FIELD}]": self.attempt_marker(),
                },
            ),
            bitrix_call.call_args_list,
        )
        operation = self.store.get_claim_operation(self.operation_key())
        self.assertEqual(operation["status"], "succeeded")
        self.assertEqual(self.store.count_claims(self.manager_id), 1)

    def test_new_success_uses_current_local_time_for_claim_event(self):
        current_time = datetime(2026, 8, 17, 0, 1, tzinfo=app.LOCAL_TZ)
        side_effect = self.claim_side_effect(
            [self.source_deal(), self.source_deal(), self.claimed_deal()]
        )
        with (
            self.common_claim_context(),
            patch.object(app, "local_now", return_value=current_time),
            patch.object(app, "bitrix_call", side_effect=side_effect),
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            self.store.list_claims(self.manager_id)[0]["timestamp"],
            "2026-08-17T00:01:00.000000+06:00",
        )

    def test_successful_replay_is_idempotent_and_does_not_update_twice(self):
        token = self.token()
        side_effect = self.claim_side_effect(
            [self.source_deal(), self.source_deal(), self.claimed_deal(), self.claimed_deal()]
        )
        with (
            self.common_claim_context(),
            patch.object(app, "bitrix_call", side_effect=side_effect) as bitrix_call,
        ):
            first = app.preview_claim(self.deal_id, self.manager_id, selection_token=token)
            second = app.preview_claim(self.deal_id, self.manager_id, selection_token=token)

        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertTrue(second["idempotentReplay"])
        self.assertEqual(self.store.count_claims(self.manager_id), 1)
        methods = [item.args[0] for item in bitrix_call.call_args_list]
        self.assertEqual(methods.count("crm.deal.update"), 1)

    def test_concurrent_succeeded_operation_is_live_marker_verified_before_replay(self):
        token = self.token()
        operation = {
            "operationKey": self.operation_key(),
            "dealId": self.deal_id,
            "managerId": self.manager_id,
            "status": "succeeded",
            "request": {"claimMarker": self.attempt_marker()},
            "result": {"dealId": self.deal_id, "auditRecorded": True},
        }
        with (
            self.common_claim_context(),
            patch.object(self.store, "begin_claim_operation", return_value=operation),
            patch.object(
                app,
                "bitrix_call",
                side_effect=[self.source_deal(), self.claimed_deal()],
            ) as bitrix_call,
        ):
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=token)

        self.assertTrue(result["ok"])
        self.assertTrue(result["idempotentReplay"])
        self.assertNotIn(
            "crm.deal.update",
            [item.args[0] for item in bitrix_call.call_args_list],
        )

    def test_lost_update_response_is_success_when_live_state_confirms_claim(self):
        token = self.token()
        side_effect = self.claim_side_effect(
            [self.source_deal(), self.source_deal(), self.claimed_deal()],
            update_error=TimeoutError("response lost"),
        )
        with (
            self.common_claim_context(),
            patch.object(app, "bitrix_call", side_effect=side_effect),
        ):
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=token)

        self.assertTrue(result["ok"])
        self.assertTrue(result["auditRecorded"])
        self.assertTrue(any("потерян" in warning for warning in result["warnings"]))
        self.assertEqual(self.store.get_claim_operation(self.operation_key())["status"], "succeeded")

    def test_remote_success_then_audit_failure_is_reported_as_success_with_warning(self):
        token = self.token()
        side_effect = self.claim_side_effect(
            [self.source_deal(), self.source_deal(), self.claimed_deal()]
        )
        original_finalize = self.store.finalize_claim_operation
        with (
            self.common_claim_context(),
            patch.object(app, "bitrix_call", side_effect=side_effect),
            patch.object(self.store, "finalize_claim_operation", side_effect=RuntimeError("disk full")),
        ):
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=token)

        self.assertTrue(result["ok"])
        self.assertFalse(result["auditRecorded"])
        self.assertTrue(any("журнал" in warning for warning in result["warnings"]))
        self.assertEqual(self.store.get_claim_operation(self.operation_key())["status"], "failed")
        self.assertEqual(self.store.count_claims(self.manager_id), 0)
        self.assertTrue(callable(original_finalize))

    def test_success_does_not_wait_for_disabled_greeting_preparation(self):
        token = self.token()
        side_effect = self.claim_side_effect(
            [self.source_deal(), self.source_deal(), self.claimed_deal()]
        )
        with (
            self.common_claim_context(greeting=False),
            patch.object(app, "GREETING_AUTO_SEND", False),
            patch.object(app, "GREETING_AUTO_SEND_SUPPORTED", False),
            patch.object(
                app,
                "prepare_greeting",
                side_effect=RuntimeError("slow chat history must not run"),
            ) as prepare_greeting,
            patch.object(app, "bitrix_call", side_effect=side_effect),
        ):
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=token)

        self.assertTrue(result["ok"])
        self.assertTrue(result["auditRecorded"])
        self.assertNotIn("greeting", result)
        self.assertNotIn("warnings", result)
        prepare_greeting.assert_not_called()
        self.assertEqual(self.store.count_claims(self.manager_id), 1)

    def test_unconfirmed_remote_update_is_failed_and_marked_uncertain(self):
        token = self.token()

        def fake_bitrix(method, params=None, timeout=None):
            if method == "crm.deal.get":
                if fake_bitrix.reads < 2:
                    fake_bitrix.reads += 1
                    return self.source_deal()
                raise TimeoutError("cannot verify")
            if method == "crm.deal.update":
                return True
            raise AssertionError(method)

        fake_bitrix.reads = 0
        with (
            self.common_claim_context(),
            patch.object(app, "bitrix_call", side_effect=fake_bitrix),
        ):
            result = app.preview_claim(self.deal_id, self.manager_id, selection_token=token)

        self.assertFalse(result["ok"])
        self.assertTrue(result["remoteUpdateUncertain"])
        self.assertEqual(result["_httpStatus"], 503)
        self.assertEqual(self.store.get_claim_operation(self.operation_key())["status"], "failed")


class TestExtraClaimAuthorization(ClaimWorkflowTestCase):
    business_date = "2026-08-17"
    request_id = "req-42"
    grant_id = "grant-42"

    def manager(self):
        return {
            "id": self.manager_id,
            "name": "Manager",
            "active": True,
            "intranet": True,
            "competencies": ["Турция"],
        }

    def quota_token(self):
        return app.issue_selection_token(
            self.deal_id,
            self.manager_id,
            self.version,
            test_manager_policy(rule=self.store.get_rule(self.manager_id)),
            now=1_000,
        )

    def seed_approved_extra_claim(self):
        self.store.set_rule(self.manager_id, enabled=True, daily_limit=1)
        self.store.append_claim(
            {
                "timestamp": f"{self.business_date}T09:00:00+06:00",
                "managerId": self.manager_id,
                "dealId": "99",
            }
        )
        request = self.store.create_extra_claim_request(
            self.manager_id,
            self.business_date,
            "Клиент ждёт дополнительную срочную заявку",
            taken_today_snapshot=1,
            daily_limit_snapshot=1,
        )
        self.store.import_extra_claim_state(
            self.manager_id,
            self.business_date,
            {
                "request": {"id": self.request_id, "status": "approved"},
                "grants": [
                    {
                        "id": self.grant_id,
                        "requestId": self.request_id,
                        "status": "approved",
                        "bitrixUserId": self.manager_id,
                        "businessDate": self.business_date,
                    }
                ],
            },
        )
        return request

    @contextmanager
    def extra_claim_context(self):
        with (
            patch.multiple(
                app,
                EXTRA_CLAIM_REQUESTS_ENABLED=True,
                BAZA_API_BASE_URL="https://baza.example.test",
                BAZA_HMAC_KEY_ID="picker-v1",
                BAZA_HMAC_SECRET="s" * 32,
            ),
            patch.object(app, "local_date", return_value=self.business_date),
            patch.object(app, "is_limit_bypassed_now", return_value=False),
        ):
            yield

    def revoked_response(self):
        return {
            "ok": True,
            "request": {
                "id": self.request_id,
                "status": "expired",
                "bitrixUserId": self.manager_id,
                "businessDate": self.business_date,
            },
            "grants": [],
        }

    def approved_response(self):
        return {
            "ok": True,
            "request": {
                "id": self.request_id,
                "status": "approved",
                "bitrixUserId": self.manager_id,
                "businessDate": self.business_date,
            },
            "grants": [
                {
                    "id": self.grant_id,
                    "requestId": self.request_id,
                    "status": "approved",
                    "bitrixUserId": self.manager_id,
                    "businessDate": self.business_date,
                }
            ],
        }

    def test_fresh_authoritative_grant_allows_one_real_claim(self):
        self.seed_approved_extra_claim()
        side_effect = self.claim_side_effect(
            [self.source_deal(), self.source_deal(), self.claimed_deal()]
        )
        with (
            self.common_claim_context(),
            self.extra_claim_context(),
            patch.object(app, "baza_post", return_value=self.approved_response()) as baza_post,
            patch.object(app, "bitrix_call", side_effect=side_effect) as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.quota_token(),
            )

        self.assertTrue(result["ok"])
        baza_post.assert_called_once_with(
            "/integrations/deal-picker/v1/grants/query",
            {"bitrixUserId": self.manager_id, "businessDate": self.business_date},
        )
        self.assertEqual(
            [item.args[0] for item in bitrix_call.call_args_list].count("crm.deal.update"),
            1,
        )
        operation = self.store.get_claim_operation(self.operation_key())
        self.assertEqual(operation["status"], "succeeded")
        self.assertEqual(operation["extraClaimGrantId"], self.grant_id)
        state = self.store.get_extra_claim_state(self.manager_id, self.business_date)
        self.assertFalse(state["grantAvailable"])

    def test_revoked_approved_grant_blocks_claim_before_crm_update(self):
        self.seed_approved_extra_claim()
        with (
            self.common_claim_context(),
            self.extra_claim_context(),
            patch.object(app, "baza_post", return_value=self.revoked_response()),
            patch.object(app, "bitrix_call", return_value=self.source_deal()) as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.quota_token(),
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["_httpStatus"], 409)
        self.assertFalse(result["integrationUnavailable"])
        self.assertIn("CRM не изменена", result["message"])
        self.assertNotIn(
            "crm.deal.update",
            [item.args[0] for item in bitrix_call.call_args_list],
        )
        self.assertIsNone(self.store.get_claim_operation(self.operation_key()))

    def test_baza_outage_blocks_over_limit_claim_before_crm_update(self):
        self.seed_approved_extra_claim()
        with (
            self.common_claim_context(),
            self.extra_claim_context(),
            patch.object(app, "baza_post", side_effect=TimeoutError("down")) as baza_post,
            patch.object(app, "bitrix_call", return_value=self.source_deal()) as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.quota_token(),
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["_httpStatus"], 503)
        self.assertTrue(result["integrationUnavailable"])
        self.assertIn("CRM не изменена", result["message"])
        baza_post.assert_called_once()
        self.assertNotIn(
            "crm.deal.update",
            [item.args[0] for item in bitrix_call.call_args_list],
        )
        self.assertIsNone(self.store.get_claim_operation(self.operation_key()))

    def test_within_limit_claim_never_depends_on_baza(self):
        self.store.set_rule(self.manager_id, enabled=True, daily_limit=1)
        side_effect = self.claim_side_effect(
            [self.source_deal(), self.source_deal(), self.claimed_deal()]
        )
        with (
            self.common_claim_context(),
            self.extra_claim_context(),
            patch.object(app, "baza_post", side_effect=TimeoutError("down")) as baza_post,
            patch.object(app, "bitrix_call", side_effect=side_effect) as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.quota_token(),
            )

        self.assertTrue(result["ok"])
        baza_post.assert_not_called()
        self.assertEqual(
            [item.args[0] for item in bitrix_call.call_args_list].count("crm.deal.update"),
            1,
        )

    def test_revoked_reserved_grant_blocks_retry_write_and_keeps_evidence(self):
        self.seed_approved_extra_claim()
        self.store.begin_claim_operation(
            self.deal_id,
            self.manager_id,
            operation_key=self.operation_key(),
            request={
                "claimMarker": self.attempt_marker(),
                "dealVersion": self.version,
                "extraClaimRequired": True,
                "businessDate": self.business_date,
            },
            require_extra_grant=True,
            business_date=self.business_date,
        )
        self.store.fail_claim_operation(
            self.operation_key(),
            "verification timeout",
            result={"remoteUpdateUncertain": True},
        )
        with (
            self.common_claim_context(),
            self.extra_claim_context(),
            patch.object(app, "baza_post", return_value=self.revoked_response()),
            patch.object(app, "bitrix_call", return_value=self.source_deal()) as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.quota_token(),
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["_httpStatus"], 409)
        self.assertNotIn(
            "crm.deal.update",
            [item.args[0] for item in bitrix_call.call_args_list],
        )
        operation = self.store.get_claim_operation(self.operation_key())
        self.assertEqual(operation["status"], "failed")
        self.assertEqual(operation["extraClaimGrantId"], self.grant_id)
        state = self.store.get_extra_claim_state(
            self.manager_id,
            self.business_date,
            operation_key=self.operation_key(),
        )
        self.assertEqual(state["grant"]["status"], "reserved")

    def test_next_deal_returns_no_deal_when_grant_refresh_is_unavailable(self):
        self.seed_approved_extra_claim()
        handler = HandlerHarness.make(
            "POST",
            "/api/next-deal",
            {"auth": {"AUTH_ID": "fresh", "DOMAIN": "test-fake.bitrix24.test"}},
        )
        with (
            self.extra_claim_context(),
            patch.object(app, "readiness_state", return_value={"ok": True}),
            patch.object(app, "rate_limit_allowed", return_value=True),
            patch.object(app, "actor_id_from_payload", return_value=self.manager_id),
            patch.object(app, "baza_post", side_effect=TimeoutError("down")),
            patch.object(app, "get_next_deal_for_manager") as search,
        ):
            handler.do_POST()

        self.assertEqual(HandlerHarness.status(handler), 200)
        payload = HandlerHarness.json(handler)
        self.assertIsNone(payload["deal"])
        self.assertTrue(payload["integrationUnavailable"])
        self.assertIn("Ничего не изменено", payload["reason"])
        search.assert_not_called()

    def test_remote_empty_grants_stop_search_before_bitrix_deal_listing(self):
        self.seed_approved_extra_claim()
        handler = HandlerHarness.make(
            "POST",
            "/api/next-deal",
            {"auth": {"AUTH_ID": "fresh", "DOMAIN": "test-fake.bitrix24.test"}},
        )
        with (
            self.extra_claim_context(),
            patch.object(app, "readiness_state", return_value={"ok": True}),
            patch.object(app, "rate_limit_allowed", return_value=True),
            patch.object(app, "actor_id_from_payload", return_value=self.manager_id),
            patch.object(app, "baza_post", return_value=self.revoked_response()),
            patch.object(app, "get_manager_profile", return_value=self.manager()),
            patch.object(app, "list_allowed_deal_headers") as list_headers,
        ):
            handler.do_POST()

        self.assertEqual(HandlerHarness.status(handler), 200)
        payload = HandlerHarness.json(handler)
        self.assertIsNone(payload["deal"])
        self.assertTrue(payload["limitReached"])
        list_headers.assert_not_called()


class TestStalePendingRecovery(ClaimWorkflowTestCase):
    def test_other_lifecycle_cannot_overwrite_an_unresolved_deal_operation(self):
        old_version = "older-lifecycle"
        old_manager = "1001"
        old_key = app.claim_operation_key(self.deal_id, old_version)
        old_marker = app.claim_attempt_marker(old_key, old_manager, nonce="old-attempt")
        self.store.begin_claim_operation(
            self.deal_id,
            old_manager,
            operation_key=old_key,
            request={
                "dealVersion": old_version,
                "claimMarker": old_marker,
                "attemptStartedAt": "2026-08-17T09:00:00+06:00",
            },
        )
        self.store.fail_claim_operation(
            old_key,
            "verification timeout",
            result={"remoteUpdateUncertain": True},
        )

        with (
            self.common_claim_context(),
            patch.object(app, "bitrix_call") as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["_httpStatus"], 409)
        self.assertTrue(result["recoveryPending"])
        bitrix_call.assert_not_called()
        self.assertEqual(self.store.get_claim_operation(old_key)["status"], "failed")
        self.assertEqual(self.store.list_claims(), [])

    def test_failed_uncertain_retry_never_overwrites_a_foreign_marker(self):
        self.begin_operation()
        self.store.fail_claim_operation(
            self.operation_key(),
            "verification timeout",
            result={"remoteUpdateUncertain": True},
        )
        live = self.source_deal()
        live[app.BITRIX_CLAIM_MARKER_FIELD] = "claim:foreign-attempt"
        with (
            self.common_claim_context(),
            patch.object(app, "bitrix_call", return_value=live) as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )

        self.assertFalse(result["ok"])
        self.assertTrue(result["recoveryPending"])
        self.assertEqual(result["_httpStatus"], 409)
        self.assertNotIn(
            "crm.deal.update",
            [item.args[0] for item in bitrix_call.call_args_list],
        )
        self.assertEqual(
            len(self.store.list_unresolved_claim_operations(self.manager_id)),
            1,
        )
        self.assertEqual(self.store.count_claims(self.manager_id), 0)

    def test_young_pending_operation_stays_locked_without_crm_write(self):
        self.begin_operation()
        with (
            self.common_claim_context(),
            patch.object(app, "bitrix_call") as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )
        self.assertFalse(result["ok"])
        self.assertEqual(result["_httpStatus"], 409)
        self.assertEqual(self.store.get_claim_operation(self.operation_key())["status"], "pending")
        bitrix_call.assert_not_called()

    def test_stale_pending_recovers_already_completed_remote_claim_without_second_update(self):
        self.begin_operation()
        self.backdate_operation()
        with (
            self.common_claim_context(),
            patch.object(app, "bitrix_call", return_value=self.claimed_deal()) as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result.get("recoveredAfterRetry") or result.get("recoveredStaleOperation"))
        self.assertNotIn("crm.deal.update", [item.args[0] for item in bitrix_call.call_args_list])
        self.assertEqual(self.store.get_claim_operation(self.operation_key())["status"], "succeeded")
        self.assertEqual(self.store.count_claims(self.manager_id), 1)

    def test_browser_recovery_after_midnight_keeps_original_reservation_date(self):
        reservation_time = "2026-08-16T23:59:00+06:00"
        reconciliation_time = datetime(2026, 8, 17, 0, 1, tzinfo=app.LOCAL_TZ)
        self.store.begin_claim_operation(
            self.deal_id,
            self.manager_id,
            operation_key=self.operation_key(),
            request={
                "claimMarker": self.attempt_marker(),
                "dealVersion": self.version,
                "attemptStartedAt": reservation_time,
            },
        )
        self.set_operation_timestamps(reservation_time)

        with (
            self.common_claim_context(),
            patch.object(app, "local_now", return_value=reconciliation_time),
            patch.object(app, "bitrix_call", return_value=self.claimed_deal()),
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            self.store.count_claims(self.manager_id, "2026-08-16", "2026-08-16"),
            1,
        )
        self.assertEqual(
            self.store.count_claims(self.manager_id, "2026-08-17", "2026-08-17"),
            0,
        )
        finalized = self.store.get_claim_operation(self.operation_key())
        self.assertEqual(finalized["request"]["attemptStartedAt"], reservation_time)
        self.assertTrue(finalized["request"]["recovery"])

    def test_legacy_retried_pending_recovery_uses_pre_transition_updated_at(self):
        first_operation_time = "2026-08-15T10:00:00+06:00"
        retry_reservation_time = "2026-08-16T23:59:00+06:00"
        reconciliation_time = datetime(2026, 8, 17, 0, 1, tzinfo=app.LOCAL_TZ)
        self.begin_operation()
        self.set_operation_timestamps(first_operation_time)
        self.store.fail_claim_operation(
            self.operation_key(),
            "legacy first attempt failed",
            result={"remoteUpdated": False},
        )
        retried = self.store.retry_failed_claim_operation(
            self.deal_id,
            self.manager_id,
            operation_key=self.operation_key(),
        )
        self.assertTrue(retried["retried"])
        self.set_operation_timestamps(
            first_operation_time,
            updated_at=retry_reservation_time,
        )

        with (
            self.common_claim_context(),
            patch.object(app, "local_now", return_value=reconciliation_time),
            patch.object(app, "bitrix_call", return_value=self.claimed_deal()),
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )

        self.assertTrue(result["ok"])
        self.assertEqual(
            self.store.count_claims(self.manager_id, "2026-08-15", "2026-08-15"),
            0,
        )
        self.assertEqual(
            self.store.count_claims(self.manager_id, "2026-08-16", "2026-08-16"),
            1,
        )
        self.assertEqual(
            self.store.count_claims(self.manager_id, "2026-08-17", "2026-08-17"),
            0,
        )

    def test_stale_pending_in_source_stage_retries_same_identity_and_updates(self):
        self.begin_operation()
        self.backdate_operation()
        side_effect = self.claim_side_effect(
            [self.source_deal(), self.source_deal(), self.claimed_deal()]
        )
        with (
            self.common_claim_context(),
            patch.object(app, "bitrix_call", side_effect=side_effect) as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )

        self.assertTrue(result["ok"])
        self.assertEqual([item.args[0] for item in bitrix_call.call_args_list].count("crm.deal.update"), 1)
        self.assertEqual(self.store.get_claim_operation(self.operation_key())["status"], "succeeded")
        self.assertEqual(self.store.count_claims(self.manager_id), 1)

    def test_stale_pending_from_other_manager_can_be_safely_reassigned_in_source_stage(self):
        old_manager = "1001"
        new_manager = "42"
        self.begin_operation(old_manager)
        self.backdate_operation()
        side_effect = self.claim_side_effect(
            [self.source_deal(manager=old_manager), self.source_deal(manager=old_manager), self.claimed_deal(manager=new_manager)]
        )
        with (
            self.common_claim_context(),
            patch.object(app, "bitrix_call", side_effect=side_effect),
        ):
            result = app.preview_claim(
                self.deal_id,
                new_manager,
                selection_token=self.token(new_manager),
            )

        self.assertTrue(result["ok"])
        operation = self.store.get_claim_operation(self.operation_key())
        self.assertEqual(operation["managerId"], new_manager)
        self.assertEqual(operation["status"], "succeeded")
        self.assertEqual(self.store.count_claims(new_manager), 1)
        self.assertEqual(self.store.count_claims(old_manager), 0)


class TestEligibilityAndAccessPolicy(ClaimWorkflowTestCase):
    def test_selection_token_is_revoked_when_competencies_change(self):
        with (
            self.common_claim_context(dry_run=True),
            patch.object(
                app,
                "get_manager_profile",
                return_value={
                    "id": self.manager_id,
                    "name": "Manager",
                    "active": True,
                    "intranet": True,
                    "competencies": [],
                },
            ),
            patch.object(app, "bitrix_call") as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["_httpStatus"], 409)
        bitrix_call.assert_not_called()

    def test_inactive_boolean_and_string_users_are_denied(self):
        for active in (False, "N", "0"):
            with self.subTest(active=active):
                profile = app.manager_profile_from_user(
                    {"ID": "42", "ACTIVE": active, "UF_DEPARTMENT": [1]}, "42"
                )
                self.assertFalse(profile["active"])

    def test_extranet_user_is_not_an_internal_employee(self):
        profile = app.manager_profile_from_user(
            {"ID": "42", "ACTIVE": True, "UF_DEPARTMENT": []}, "42"
        )
        self.assertTrue(profile["active"])
        self.assertFalse(profile["intranet"])

    def test_cleared_live_skills_never_restore_stale_local_skills_in_production(self):
        local_manager = {
            "id": "42",
            "name": "Old local manager",
            "active": True,
            "competencies": ["Турция"],
        }
        with (
            patch.object(app, "load_managers", return_value=[local_manager]),
            patch.object(app, "is_unverified_dev_mode", return_value=False),
        ):
            profile = app.manager_profile_from_user(
                {
                    "ID": "42",
                    "ACTIVE": True,
                    "UF_DEPARTMENT": [1],
                    "UF_SKILLS": [],
                },
                "42",
            )

        self.assertEqual(profile["competencies"], [])
        self.assertEqual(profile["source"], "empty")

    def test_inactive_or_extranet_configured_admin_is_denied(self):
        for raw in (
            {"ACTIVE": False, "UF_DEPARTMENT": [1]},
            {"ACTIVE": True, "UF_DEPARTMENT": []},
        ):
            with self.subTest(raw=raw), patch.object(
                app,
                "verify_bitrix_user",
                return_value={"id": "1", "name": "Admin", "raw": raw},
            ):
                self.assertIsNone(app.require_admin({"auth": {"AUTH_ID": "token"}}))

    def test_missing_explicit_rule_denies_even_with_valid_selection(self):
        with (
            self.common_claim_context(dry_run=True),
            patch.object(app, "REQUIRE_EXPLICIT_ACCESS_RULE", True),
            patch.object(app, "bitrix_call") as bitrix_call,
        ):
            result = app.preview_claim(
                self.deal_id, self.manager_id, selection_token=self.token()
            )
        self.assertFalse(result["ok"])
        self.assertEqual(result["_httpStatus"], 403)
        bitrix_call.assert_not_called()

    def test_revoked_rule_and_exhausted_limit_deny_a_previously_issued_token(self):
        for rule in (
            {"enabled": False, "daily_limit": None},
            {"enabled": True, "daily_limit": 0},
        ):
            with self.subTest(rule=rule):
                self.store.set_rule(
                    self.manager_id,
                    enabled=rule["enabled"],
                    daily_limit=rule["daily_limit"],
                )
                with (
                    self.common_claim_context(dry_run=True),
                    patch.object(app, "REQUIRE_EXPLICIT_ACCESS_RULE", True),
                    patch.object(app, "is_limit_bypassed_now", return_value=False),
                    patch.object(app, "bitrix_call") as bitrix_call,
                ):
                    result = app.preview_claim(
                        self.deal_id, self.manager_id, selection_token=self.token()
                    )
                self.assertFalse(result["ok"])
                self.assertEqual(result["_httpStatus"], 403)
                bitrix_call.assert_not_called()

    def test_succeeded_replay_survives_new_quota_without_second_update(self):
        self.store.set_rule(self.manager_id, enabled=True, daily_limit=None)
        side_effect = self.claim_side_effect(
            [self.source_deal(), self.source_deal(), self.claimed_deal(), self.claimed_deal()]
        )
        with (
            self.common_claim_context(),
            patch.object(app, "REQUIRE_EXPLICIT_ACCESS_RULE", True),
            patch.object(app, "bitrix_call", side_effect=side_effect) as bitrix_call,
        ):
            first = app.preview_claim(
                self.deal_id, self.manager_id, selection_token=self.token()
            )
            self.store.set_rule(self.manager_id, enabled=True, daily_limit=0)
            replay = app.preview_claim(
                self.deal_id, self.manager_id, selection_token=self.token()
            )
        self.assertTrue(first["ok"])
        self.assertTrue(replay["ok"])
        self.assertTrue(replay["idempotentReplay"])
        self.assertEqual(
            [item.args[0] for item in bitrix_call.call_args_list].count("crm.deal.update"),
            1,
        )


class TestAdminRuleSerialization(unittest.TestCase):
    def test_admin_rule_update_waits_for_claim_and_reject_critical_section(self):
        shared_lock = threading.Lock()
        saved = threading.Event()
        results = []

        def save_rule(*args, **kwargs):
            saved.set()
            return {
                "enabled": kwargs["enabled"],
                "dailyLimit": kwargs.get("daily_limit"),
                "note": kwargs.get("note") or "",
            }

        with (
            patch.object(app, "DATA_LOCK", shared_lock),
            patch.object(app, "require_admin", return_value={"id": "1"}),
            patch.object(app, "set_manager_rule", side_effect=save_rule),
        ):
            shared_lock.acquire()
            worker = threading.Thread(
                target=lambda: results.append(
                    app.update_admin_rule({"managerId": "42", "enabled": False})
                )
            )
            worker.start()
            self.assertFalse(saved.wait(0.05))
            shared_lock.release()
            worker.join(timeout=1)

        self.assertFalse(worker.is_alive())
        self.assertTrue(saved.is_set())
        self.assertTrue(results[0]["ok"])
        self.assertFalse(results[0]["rule"]["enabled"])


class TestBitrixSourceCompleteness(TemporaryStateTestCase):
    def test_private_greeting_session_is_version_bound_and_never_in_browser_deal(self):
        deal = {
            "ID": "100",
            "TITLE": "Deal",
            "STAGE_ID": next(iter(app.SOURCE_STAGES)),
            "DATE_MODIFY": "v1",
        }
        message_data = {
            "useful": ["Хочу в Турцию"],
            "rawCount": 1,
            "sources": ["openline_session:321"],
            "openlineSessionIds": ["321"],
        }
        with patch.object(app, "get_deal_messages", return_value=message_data):
            public_deal = app.analyze_deal_header(deal)

        self.assertNotIn("openlineSessionIds", public_deal)
        self.assertNotIn("greetingContext", public_deal)
        private = app.cached_greeting_context("100", "v1")
        self.assertEqual(private["openlineSessionIds"], ["321"])
        self.assertEqual(private["classification"]["direction"], "Турция")
        self.assertIsNone(app.cached_greeting_context("100", "v2"))

    def _header(self):
        return {
            "ID": "100",
            "TITLE": "Oldest deal",
            "STAGE_ID": next(iter(app.SOURCE_STAGES)),
            "DATE_CREATE": "2026-01-01T00:00:00+06:00",
            "DATE_MODIFY": "version-1",
        }

    def _manager(self):
        return {
            "id": "42",
            "name": "Manager",
            "active": True,
            "intranet": True,
            "competencies": ["Турция"],
        }

    def test_confirmed_empty_mandatory_sources_are_a_valid_empty_request(self):
        with patch.object(app, "bitrix_list_all", side_effect=[[], []]) as bitrix_list_all:
            result = app.get_deal_messages("100")

        self.assertEqual(result["useful"], [])
        self.assertEqual(result["rawCount"], 0)
        self.assertEqual(result["openlineSessionIds"], [])
        self.assertEqual(
            [item.args[0] for item in bitrix_list_all.call_args_list],
            ["crm.timeline.comment.list", "crm.activity.list"],
        )

    def test_mandatory_source_failure_is_not_misclassified_as_empty(self):
        def list_side_effect(method, *args, **kwargs):
            if method == "crm.timeline.comment.list":
                raise RuntimeError("private upstream detail")
            if method == "crm.activity.list":
                return []
            self.fail(f"unexpected Bitrix method: {method}")

        with patch.object(app, "bitrix_list_all", side_effect=list_side_effect):
            with self.assertRaisesRegex(
                RuntimeError,
                "Не удалось полностью прочитать историю сделки из Bitrix",
            ) as raised:
                app.get_deal_messages("100")

        self.assertNotIn("private upstream detail", str(raised.exception))

    def test_openline_history_failure_propagates_and_is_not_cached_as_empty(self):
        with patch.object(
            app,
            "bitrix_call",
            side_effect=RuntimeError("history unavailable"),
        ) as bitrix_call:
            for _ in range(2):
                with self.assertRaisesRegex(RuntimeError, "history unavailable"):
                    app.get_openline_history_messages("session-100")

        self.assertEqual(bitrix_call.call_count, 2)

    def test_openline_history_prefers_newest_useful_messages(self):
        history = {
            "message": {
                "1": {
                    "senderid": "123",
                    "date": "2026-08-17T09:00:00+06:00",
                    "text": "Сначала хотели Турцию и Анталью",
                },
                "2": {
                    "senderid": "123",
                    "date": "2026-08-17T10:00:00+06:00",
                    "text": "Теперь нужен Египет, Шарм и Хургада",
                },
            }
        }
        with patch.object(app, "bitrix_call", return_value=history):
            messages = app.get_openline_history_messages("session-100")

        self.assertIn("Египет", messages[0])
        self.assertEqual(app.classify(messages)["direction"], "Египет")

    def test_newest_openline_is_not_suppressed_by_two_old_timeline_comments(self):
        def list_side_effect(method, *args, **kwargs):
            if method == "crm.timeline.comment.list":
                return [
                    {
                        "COMMENT": "Раньше хотели Турцию",
                        "CREATED": "2026-08-17T08:00:00+06:00",
                    },
                    {
                        "COMMENT": "Смотрели отели Антальи",
                        "CREATED": "2026-08-17T08:01:00+06:00",
                    },
                ]
            if method == "crm.activity.list":
                return [
                    {
                        "ID": "100",
                        "PROVIDER_ID": "IMOPENLINES_SESSION",
                        "ASSOCIATED_ENTITY_ID": "session-100",
                        "DESCRIPTION": "",
                        "CREATED": "2026-08-17T08:02:00+06:00",
                    }
                ]
            self.fail(f"unexpected Bitrix list method: {method}")

        history = {
            "message": {
                "1": {
                    "senderid": "123",
                    "date": "2026-08-17T10:00:00+06:00",
                    "text": "Передумали: теперь нужен Египет",
                }
            }
        }
        with (
            patch.object(app, "bitrix_list_all", side_effect=list_side_effect),
            patch.object(app, "bitrix_call", return_value=history) as bitrix_call,
        ):
            result = app.get_deal_messages("100")

        self.assertIn("Египет", result["useful"][0])
        self.assertEqual(app.classify(result["useful"])["direction"], "Египет")
        self.assertEqual(len(result["useful"]), 1)
        bitrix_call.assert_called_once()

    def test_newest_openline_message_supersedes_older_same_session_destination(self):
        def list_side_effect(method, *args, **kwargs):
            if method == "crm.timeline.comment.list":
                return []
            if method == "crm.activity.list":
                return [
                    {
                        "ID": "100",
                        "PROVIDER_ID": "IMOPENLINES_SESSION",
                        "ASSOCIATED_ENTITY_ID": "session-100",
                        "DESCRIPTION": "",
                        "CREATED": "2026-08-17T10:00:00+06:00",
                    }
                ]
            self.fail(f"unexpected Bitrix list method: {method}")

        history = {
            "message": {
                "1": {
                    "senderid": "123",
                    "date": "2026-08-17T10:00:00+06:00",
                    "text": "Раньше хотели Турцию и Анталью",
                },
                "2": {
                    "senderid": "123",
                    "date": "2026-08-17T10:00:00+06:00",
                    "text": "Теперь нужен Египет",
                },
            }
        }
        with (
            patch.object(app, "bitrix_list_all", side_effect=list_side_effect),
            patch.object(app, "bitrix_call", return_value=history),
        ):
            result = app.get_deal_messages("100")

        self.assertEqual(result["useful"], ["Теперь нужен Египет"])
        self.assertEqual(app.classify(result["useful"])["direction"], "Египет")

    def test_equal_time_openline_sessions_choose_highest_activity_id(self):
        def list_side_effect(method, *args, **kwargs):
            if method == "crm.timeline.comment.list":
                return []
            if method == "crm.activity.list":
                return [
                    {
                        "ID": "100",
                        "PROVIDER_ID": "IMOPENLINES_SESSION",
                        "ASSOCIATED_ENTITY_ID": "old-session",
                        "DESCRIPTION": "",
                        "CREATED": "2026-08-17T10:00:00+06:00",
                    },
                    {
                        "ID": "101",
                        "PROVIDER_ID": "IMOPENLINES_SESSION",
                        "ASSOCIATED_ENTITY_ID": "new-session",
                        "DESCRIPTION": "",
                        "CREATED": "2026-08-17T10:00:00+06:00",
                    },
                ]
            self.fail(f"unexpected Bitrix list method: {method}")

        def history_side_effect(method, params=None, **kwargs):
            self.assertEqual(method, "imopenlines.session.history.get")
            self.assertEqual(params["SESSION_ID"], "new-session")
            return {
                "message": {
                    "1": {
                        "senderid": "123",
                        "date": "2026-08-17T10:01:00+06:00",
                        "text": "Теперь нужен Египет",
                    }
                }
            }

        with (
            patch.object(app, "bitrix_list_all", side_effect=list_side_effect),
            patch.object(app, "bitrix_call", side_effect=history_side_effect) as history_call,
        ):
            result = app.get_deal_messages("100")

        self.assertEqual(result["openlineSessionIds"], ["new-session"])
        self.assertEqual(result["useful"], ["Теперь нужен Египет"])
        history_call.assert_called_once()

    def test_openline_history_over_safe_record_bound_fails_closed(self):
        history = {
            "message": {
                str(index): {
                    "senderid": "123",
                    "date": "2026-08-17T10:00:00+06:00",
                    "text": "Хотим Египет",
                }
                for index in range(1, 4)
            }
        }
        with (
            patch.object(app, "MAX_OPENLINE_MESSAGES_PER_SESSION", 2),
            patch.object(app, "bitrix_call", return_value=history),
        ):
            with self.assertRaisesRegex(RuntimeError, "безопасный лимит"):
                app.get_openline_history_messages("session-100")

    def test_openline_deadline_is_checked_after_history_response(self):
        with (
            patch.object(app, "bitrix_call", return_value={"message": {}}),
            patch.object(app.time, "monotonic", side_effect=[0.0, 1.1]),
        ):
            with self.assertRaisesRegex(TimeoutError, "таймаут истории"):
                app.get_openline_history_messages("session-100", timeout=1.0)

    def test_second_source_page_is_read_before_empty_is_confirmed(self):
        service_page = [
            {
                "COMMENT": "Создана новая сделка",
                "CREATED": f"2026-08-17T09:{index % 60:02d}:00+06:00",
            }
            for index in range(50)
        ]

        def full_side_effect(method, params=None, timeout=None):
            start = int((params or {}).get("start", 0))
            if method == "crm.timeline.comment.list" and start == 0:
                return {"result": service_page, "next": 50}
            if method == "crm.timeline.comment.list" and start == 50:
                return {
                    "result": [
                        {
                            "COMMENT": "Клиент хочет Египет и Шарм",
                            "CREATED": "2026-08-16T09:00:00+06:00",
                        }
                    ]
                }
            if method == "crm.activity.list":
                return {"result": []}
            self.fail(f"unexpected page request: {method} start={start}")

        with patch.object(app, "bitrix_call_full", side_effect=full_side_effect) as full_call:
            result = app.get_deal_messages("100")

        self.assertEqual(result["rawCount"], 51)
        self.assertEqual(app.classify(result["useful"])["direction"], "Египет")
        self.assertEqual(
            [
                item.kwargs.get("timeout") is not None
                for item in full_call.call_args_list
            ],
            [True, True, True],
        )

    def test_second_source_page_failure_is_not_treated_as_complete(self):
        def full_side_effect(method, params=None, timeout=None):
            start = int((params or {}).get("start", 0))
            if method == "crm.timeline.comment.list" and start == 0:
                return {
                    "result": [
                        {"COMMENT": "Создана новая сделка", "CREATED": "2026-08-17T09:00:00+06:00"}
                    ],
                    "next": 50,
                }
            if method == "crm.timeline.comment.list" and start == 50:
                raise TimeoutError("second page unavailable")
            if method == "crm.activity.list":
                return {"result": []}
            self.fail(f"unexpected page request: {method} start={start}")

        with patch.object(app, "bitrix_call_full", side_effect=full_side_effect):
            with self.assertRaisesRegex(
                RuntimeError,
                "Не удалось полностью прочитать историю сделки из Bitrix",
            ):
                app.get_deal_messages("100")

    def test_new_activity_in_same_openline_session_refetches_and_reclassifies(self):
        history_texts = iter(
            [
                "Хотим подобрать семейный тур в Турцию",
                "Передумали, теперь хотим Египет и хороший семейный отель",
            ]
        )

        def list_side_effect(method, *args, **kwargs):
            if method == "crm.timeline.comment.list":
                return []
            if method == "crm.activity.list":
                return [
                    {
                        "ID": "100",
                        "PROVIDER_ID": "IMOPENLINES_SESSION",
                        "ASSOCIATED_ENTITY_ID": "session-100",
                        "DESCRIPTION": "",
                        "CREATED": "2026-08-17T09:00:00+06:00",
                    }
                ]
            self.fail(f"unexpected Bitrix list method: {method}")

        def bitrix_side_effect(method, *args, **kwargs):
            if method == "imopenlines.session.history.get":
                return {
                    "message": {
                        "1": {
                            "senderid": "123",
                            "date": "2026-08-17T09:00:00+06:00",
                            "text": next(history_texts),
                        }
                    }
                }
            self.fail(f"unexpected Bitrix method: {method}")

        base_deal = {
            "ID": "100",
            "TITLE": "Deal",
            "STAGE_ID": next(iter(app.SOURCE_STAGES)),
            "DATE_CREATE": "2026-08-17T08:00:00+06:00",
            "DATE_MODIFY": "2026-08-17T08:30:00+06:00",
            "LAST_ACTIVITY_TIME": "2026-08-17T09:00:00+06:00",
        }
        with (
            patch.object(app, "bitrix_list_all", side_effect=list_side_effect),
            patch.object(app, "bitrix_call", side_effect=bitrix_side_effect) as bitrix_call,
        ):
            first = app.analyze_deal_header(base_deal)
            second = app.analyze_deal_header(
                {**base_deal, "LAST_ACTIVITY_TIME": "2026-08-17T09:01:00+06:00"}
            )

        self.assertEqual(first["classification"]["direction"], "Турция")
        self.assertEqual(second["classification"]["direction"], "Египет")
        self.assertEqual(
            [item.args[0] for item in bitrix_call.call_args_list].count(
                "imopenlines.session.history.get"
            ),
            2,
        )

    def test_required_openline_history_failure_propagates_from_deal_analysis(self):
        def list_side_effect(method, *args, **kwargs):
            if method == "crm.timeline.comment.list":
                return []
            if method == "crm.activity.list":
                return [
                    {
                        "ID": "100",
                        "PROVIDER_ID": "IMOPENLINES_SESSION",
                        "ASSOCIATED_ENTITY_ID": "session-100",
                        "DESCRIPTION": "",
                        "CREATED": "2026-01-01T00:00:00+06:00",
                    }
                ]
            self.fail(f"unexpected Bitrix list method: {method}")

        def bitrix_side_effect(method, *args, **kwargs):
            if method == "imopenlines.session.history.get":
                raise RuntimeError("history unavailable")
            self.fail(f"unexpected Bitrix method: {method}")

        with (
            patch.object(app, "bitrix_list_all", side_effect=list_side_effect),
            patch.object(app, "bitrix_call", side_effect=bitrix_side_effect),
        ):
            with self.assertRaisesRegex(RuntimeError, "history unavailable"):
                app.get_deal_messages("100")

    def test_real_search_path_returns_generic_503_without_selection_on_source_failure(self):
        def list_side_effect(method, *args, **kwargs):
            if method == "crm.timeline.comment.list":
                raise RuntimeError("private upstream detail")
            if method == "crm.activity.list":
                return []
            self.fail(f"unexpected Bitrix method: {method}")

        with (
            patch.object(app, "get_manager_profile", return_value=self._manager()),
            patch.object(app, "check_manager_access", return_value={"ok": True, "rule": {}}),
            patch.object(app, "list_allowed_deal_headers", return_value=[self._header()]),
            patch.object(app, "bitrix_list_all", side_effect=list_side_effect),
            patch.object(app, "issue_selection_token") as issue_selection_token,
        ):
            result = app._get_next_deal_for_manager("42")

        self.assertEqual(result["_httpStatus"], 503)
        self.assertIsNone(result["deal"])
        self.assertIsNone(result["continuationToken"])
        self.assertNotIn("selectionToken", result)
        self.assertNotIn("private upstream detail", result["reason"])
        issue_selection_token.assert_not_called()


class TestSearchContinuation(TemporaryStateTestCase):
    def _deal(self, header, direction):
        return {
            "id": header["ID"],
            "title": f"Deal {header['ID']}",
            "stageId": header["STAGE_ID"],
            "version": header["DATE_MODIFY"],
            "messages": [direction],
            "classification": {"direction": direction, "confidence": "high"},
        }

    def test_only_server_signed_cursor_can_advance_oldest_first_search(self):
        headers = [
            {"ID": "1", "STAGE_ID": next(iter(app.SOURCE_STAGES)), "DATE_MODIFY": "v1"},
            {"ID": "2", "STAGE_ID": next(iter(app.SOURCE_STAGES)), "DATE_MODIFY": "v2"},
        ]

        def analyze(batch):
            header = batch[0]
            direction = "Египет" if header["ID"] == "1" else "Турция"
            return {header["ID"]: self._deal(header, direction)}, {}

        manager = {
            "id": "42",
            "name": "Manager",
            "active": True,
            "intranet": True,
            "competencies": ["Турция"],
        }
        with (
            patch.object(app, "NEXT_DEAL_SCAN_LIMIT", 1),
            patch.object(app, "get_manager_profile", return_value=dict(manager)),
            patch.object(app, "check_manager_access", return_value={"ok": True, "rule": {}}),
            patch.object(app, "list_allowed_deal_headers", return_value=headers),
            patch.object(app, "analyze_deal_headers", side_effect=analyze),
        ):
            first = app._get_next_deal_for_manager("42")
            second = app._get_next_deal_for_manager("42", first["continuationToken"])
            forged_skip = app._get_next_deal_for_manager("42", ["1"])

        self.assertIsNone(first["deal"])
        self.assertTrue(first["hasMore"])
        self.assertNotIn("scannedDealIds", first)
        self.assertEqual(second["deal"]["id"], "2")
        self.assertIsNone(second["continuationToken"])
        self.assertFalse(second["hasMore"])
        self.assertEqual(forged_skip["_httpStatus"], 409)

    def test_cursor_is_bound_to_manager_and_snapshot(self):
        snapshot = "a" * 64
        token = app.issue_search_cursor("42", 12, snapshot, now=1_000)
        self.assertEqual(app.decode_search_cursor(token, "42", snapshot, now=1_000), 12)
        self.assertIsNone(app.decode_search_cursor(token, "43", snapshot, now=1_000))
        self.assertIsNone(app.decode_search_cursor(token, "42", "b" * 64, now=1_000))

    def test_new_timeline_activity_invalidates_existing_search_cursor(self):
        manager = {
            "active": True,
            "intranet": True,
            "competencies": ["Турция"],
        }
        before = [{
            "ID": "1",
            "DATE_MODIFY": "2026-08-17T09:00:00+06:00",
            "LAST_ACTIVITY_TIME": "2026-08-17T09:01:00+06:00",
        }]
        after = [{**before[0], "LAST_ACTIVITY_TIME": "2026-08-17T09:02:00+06:00"}]
        before_snapshot = app.search_snapshot(before, manager, {})
        after_snapshot = app.search_snapshot(after, manager, {})
        token = app.issue_search_cursor("42", 1, before_snapshot, now=1_000)

        self.assertNotEqual(before_snapshot, after_snapshot)
        self.assertIsNone(
            app.decode_search_cursor(token, "42", after_snapshot, now=1_000)
        )

    def test_cursor_from_previous_routing_policy_is_rejected_after_deploy(self):
        snapshot = "a" * 64
        token = app.issue_search_cursor("42", 1, snapshot, now=1_000)

        with patch.object(app, "ROUTING_POLICY_VERSION", "next-routing-policy"):
            self.assertIsNone(
                app.decode_search_cursor(token, "42", snapshot, now=1_000)
            )

    def test_timeout_on_older_deal_never_skips_to_a_later_match(self):
        headers = [
            {"ID": "1", "STAGE_ID": next(iter(app.SOURCE_STAGES)), "DATE_MODIFY": "v1"},
            {"ID": "2", "STAGE_ID": next(iter(app.SOURCE_STAGES)), "DATE_MODIFY": "v2"},
        ]
        manager = {
            "id": "42",
            "name": "Manager",
            "active": True,
            "intranet": True,
            "competencies": ["Турция"],
        }
        with (
            patch.object(app, "get_manager_profile", return_value=manager),
            patch.object(app, "check_manager_access", return_value={"ok": True, "rule": {}}),
            patch.object(app, "list_allowed_deal_headers", return_value=headers),
            patch.object(
                app,
                "analyze_deal_headers",
                return_value=({"2": self._deal(headers[1], "Турция")}, {"1": "timeout"}),
            ),
        ):
            result = app._get_next_deal_for_manager("42")

        self.assertIsNone(result["deal"])
        self.assertEqual(result["_httpStatus"], 503)
        self.assertIsNone(result["continuationToken"])

    def test_unresolved_deal_lifecycle_is_hidden_from_every_other_manager(self):
        headers = [
            {"ID": "1", "STAGE_ID": next(iter(app.SOURCE_STAGES)), "DATE_MODIFY": "v2"},
            {"ID": "2", "STAGE_ID": next(iter(app.SOURCE_STAGES)), "DATE_MODIFY": "v1"},
        ]
        old_key = app.claim_operation_key("1", "v1")
        self.store.begin_claim_operation(
            "1", "1001", operation_key=old_key, request={"dealVersion": "v1"}
        )
        self.store.fail_claim_operation(
            old_key,
            "remote state unknown",
            result={"remoteUpdateUncertain": True},
        )
        manager = {
            "id": "42",
            "name": "Manager",
            "active": True,
            "intranet": True,
            "competencies": ["Турция"],
        }

        def analyze(batch):
            self.assertEqual([item["ID"] for item in batch], ["2"])
            return {"2": self._deal(headers[1], "Турция")}, {}

        with (
            patch.object(app, "get_manager_profile", return_value=manager),
            patch.object(app, "check_manager_access", return_value={"ok": True, "rule": {}}),
            patch.object(app, "list_allowed_deal_headers", return_value=headers),
            patch.object(app, "analyze_deal_headers", side_effect=analyze),
        ):
            result = app._get_next_deal_for_manager("42")

        self.assertEqual(result["deal"]["id"], "2")

    def test_profile_policy_change_invalidates_cursor_instead_of_skipping_oldest(self):
        headers = [
            {"ID": "1", "STAGE_ID": next(iter(app.SOURCE_STAGES)), "DATE_MODIFY": "v1"},
            {"ID": "2", "STAGE_ID": next(iter(app.SOURCE_STAGES)), "DATE_MODIFY": "v2"},
        ]
        profiles = [
            {
                "id": "42",
                "name": "Manager",
                "active": True,
                "intranet": True,
                "competencies": ["Египет"],
            },
            {
                "id": "42",
                "name": "Manager",
                "active": True,
                "intranet": True,
                "competencies": ["Турция"],
            },
        ]

        def analyze(batch):
            header = batch[0]
            return {header["ID"]: self._deal(header, "Турция")}, {}

        with (
            patch.object(app, "NEXT_DEAL_SCAN_LIMIT", 1),
            patch.object(app, "get_manager_profile", side_effect=profiles),
            patch.object(app, "check_manager_access", return_value={"ok": True, "rule": {}}),
            patch.object(app, "list_allowed_deal_headers", return_value=headers),
            patch.object(app, "analyze_deal_headers", side_effect=analyze),
        ):
            first = app._get_next_deal_for_manager("42")
            after_profile_change = app._get_next_deal_for_manager(
                "42", first["continuationToken"]
            )

        self.assertIsNone(first["deal"])
        self.assertEqual(after_profile_change["_httpStatus"], 409)
        self.assertIsNone(after_profile_change["deal"])


class TestSemanticRejectionLifecycle(TemporaryStateTestCase):
    def _profile(self):
        return {
            "id": "42",
            "name": "Manager",
            "active": True,
            "intranet": True,
            "competencies": ["Турция"],
        }

    def _deal(self, version="version-1"):
        return {
            "ID": "100",
            "STAGE_ID": next(iter(app.SOURCE_STAGES)),
            "DATE_MODIFY": version,
        }

    def test_new_token_cannot_duplicate_same_manager_deal_lifecycle_rejection(self):
        first_token = app.issue_selection_token(
            "100", "42", "version-1", test_manager_policy(), now=1_000
        )
        second_token = app.issue_selection_token(
            "100", "42", "version-1", test_manager_policy(), now=1_001
        )
        with (
            patch.object(app.time, "time", return_value=1_001),
            patch.object(app, "bitrix_call", return_value=self._deal()) as bitrix_call,
            patch.object(app, "get_manager_profile", return_value=self._profile()),
        ):
            first = app.record_rejection(
                "42", {"dealId": "100", "reason": "other", "selectionToken": first_token}
            )
            second = app.record_rejection(
                "42", {"dealId": "100", "reason": "duplicate", "selectionToken": second_token}
            )
        self.assertTrue(first["ok"])
        self.assertTrue(second["idempotentReplay"])
        self.assertEqual(len(self.store.list_rejections()), 1)
        bitrix_call.assert_called_once()

    def test_rejected_selection_cannot_later_be_claimed(self):
        token = app.issue_selection_token(
            "100", "42", "version-1", test_manager_policy(), now=1_000
        )
        with (
            patch.object(app.time, "time", return_value=1_000),
            patch.object(app, "bitrix_call", return_value=self._deal()) as bitrix_call,
            patch.object(app, "get_manager_profile", return_value=self._profile()),
        ):
            rejected = app.record_rejection(
                "42", {"dealId": "100", "reason": "other", "selectionToken": token}
            )
            claimed = app.preview_claim("100", "42", selection_token=token)
        self.assertTrue(rejected["ok"])
        self.assertFalse(claimed["ok"])
        self.assertEqual(claimed["_httpStatus"], 409)
        bitrix_call.assert_called_once()

    def test_other_unresolved_lifecycle_blocks_rejection_without_bitrix_read(self):
        old_key = app.claim_operation_key("100", "older-version")
        self.store.begin_claim_operation(
            "100", "1001", operation_key=old_key, request={"dealVersion": "older-version"}
        )
        self.store.fail_claim_operation(
            old_key,
            "remote state unknown",
            result={"remoteUpdateUncertain": True},
        )
        token = app.issue_selection_token(
            "100", "42", "version-1", test_manager_policy(), now=1_000
        )

        with (
            patch.object(app.time, "time", return_value=1_000),
            patch.object(app, "bitrix_call") as bitrix_call,
        ):
            result = app.record_rejection(
                "42",
                {"dealId": "100", "reason": "other", "selectionToken": token},
            )

        self.assertFalse(result["ok"])
        self.assertEqual(self.store.list_rejections(), [])
        bitrix_call.assert_not_called()

    def test_rejection_committed_while_claim_waits_for_lock_wins_atomically(self):
        token = app.issue_selection_token(
            "100", "42", "version-1", test_manager_policy()
        )
        semantic_key = app.rejection_semantic_key("42", "100", "version-1")
        store = self.store

        class RejectBeforeClaimLock:
            def __enter__(self):
                store.append_reject(
                    {
                        "timestamp": app.local_now().isoformat(),
                        "managerId": "42",
                        "dealId": "100",
                        "reason": "other",
                        "semanticKey": semantic_key,
                    }
                )

            def __exit__(self, *_args):
                return False

        with (
            patch.object(app, "DATA_LOCK", RejectBeforeClaimLock()),
            patch.object(app, "bitrix_call") as bitrix_call,
        ):
            claimed = app.preview_claim("100", "42", selection_token=token)

        self.assertFalse(claimed["ok"])
        self.assertEqual(claimed["_httpStatus"], 409)
        self.assertEqual(len(self.store.list_rejections()), 1)
        self.assertEqual(self.store.list_claims(), [])
        bitrix_call.assert_not_called()


class TestClaimReconciliation(ClaimWorkflowTestCase):
    def test_uncertain_remote_update_blocks_new_claims_before_audit_recovery(self):
        self.store.set_manager_rule(self.manager_id, enabled=True, daily_limit=1, note="")
        self.begin_operation()
        self.store.fail_claim_operation(
            self.operation_key(),
            "verification timeout",
            result={"remoteUpdateUncertain": True},
        )

        access = app.check_manager_access(self.manager_id)

        self.assertFalse(access["ok"])
        self.assertTrue(access["recoveryPending"])
        self.assertEqual(self.store.count_claims(self.manager_id), 0)


    def test_stale_pending_with_exact_marker_recovers_after_deal_progressed(self):
        self.begin_operation()
        self.backdate_operation()
        progressed = self.claimed_deal()
        progressed["STAGE_ID"] = "WON"
        with patch.object(app, "bitrix_call", return_value=progressed):
            summary = app.reconcile_stale_claim_operations()
        self.assertEqual(summary["recovered"], 1)
        self.assertEqual(self.store.count_claims(self.manager_id), 1)

    def test_maintenance_recovery_after_midnight_keeps_original_reservation_date(self):
        reservation_time = "2026-08-16T23:59:00+06:00"
        reconciliation_time = datetime(2026, 8, 17, 0, 1, tzinfo=app.LOCAL_TZ)
        self.begin_operation()
        self.set_operation_timestamps(reservation_time)

        with (
            patch.object(app, "local_now", return_value=reconciliation_time),
            patch.object(app, "bitrix_call", return_value=self.claimed_deal()),
        ):
            summary = app.reconcile_stale_claim_operations()

        self.assertEqual(summary["recovered"], 1)
        self.assertEqual(
            self.store.count_claims(self.manager_id, "2026-08-16", "2026-08-16"),
            1,
        )
        self.assertEqual(
            self.store.count_claims(self.manager_id, "2026-08-17", "2026-08-17"),
            0,
        )

    def test_retried_attempt_recovery_uses_latest_attempt_start_date(self):
        first_started_at = "2026-08-16T23:50:00+06:00"
        second_started_at = datetime(2026, 8, 17, 0, 5, tzinfo=app.LOCAL_TZ)
        recovered_at = datetime(2026, 8, 18, 9, 0, tzinfo=app.LOCAL_TZ)
        self.store.begin_claim_operation(
            self.deal_id,
            self.manager_id,
            operation_key=self.operation_key(),
            request={
                "claimMarker": self.attempt_marker(),
                "dealVersion": self.version,
                "attemptStartedAt": first_started_at,
            },
        )
        self.store.fail_claim_operation(
            self.operation_key(),
            "first attempt safely failed",
            result={"remoteUpdated": False},
        )
        second_marker = app.claim_attempt_marker(
            self.operation_key(), self.manager_id, nonce="second-attempt"
        )

        def lose_second_verification(method, params=None, timeout=None):
            if method == "crm.deal.get":
                lose_second_verification.reads += 1
                if lose_second_verification.reads <= 2:
                    return self.source_deal()
                raise TimeoutError("second attempt verification lost")
            if method == "crm.deal.update":
                return True
            raise AssertionError(method)

        lose_second_verification.reads = 0
        with (
            self.common_claim_context(),
            patch.object(app, "local_now", return_value=second_started_at),
            patch.object(app.secrets, "token_hex", return_value="second-attempt"),
            patch.object(app, "bitrix_call", side_effect=lose_second_verification),
        ):
            failed = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )

        self.assertFalse(failed["ok"])
        operation = self.store.get_claim_operation(self.operation_key())
        self.assertEqual(operation["status"], "failed")
        self.assertEqual(
            operation["request"]["attemptStartedAt"],
            second_started_at.isoformat(),
        )
        claimed = self.claimed_deal()
        claimed[app.BITRIX_CLAIM_MARKER_FIELD] = second_marker
        with (
            patch.object(app, "local_now", return_value=recovered_at),
            patch.object(app, "bitrix_call", return_value=claimed),
        ):
            summary = app.reconcile_stale_claim_operations()

        self.assertEqual(summary["recovered"], 1)
        self.assertEqual(
            self.store.count_claims(self.manager_id, "2026-08-16", "2026-08-16"),
            0,
        )
        self.assertEqual(
            self.store.count_claims(self.manager_id, "2026-08-17", "2026-08-17"),
            1,
        )
        self.assertEqual(
            self.store.count_claims(self.manager_id, "2026-08-18", "2026-08-18"),
            0,
        )
        finalized = self.store.get_claim_operation(self.operation_key())
        self.assertEqual(
            finalized["request"]["attemptStartedAt"],
            second_started_at.isoformat(),
        )

    def test_reassigned_attempt_recovery_uses_new_managers_attempt_date(self):
        old_manager = "1001"
        first_started_at = "2026-08-16T23:50:00+06:00"
        reassigned_started_at = datetime(2026, 8, 17, 0, 5, tzinfo=app.LOCAL_TZ)
        self.store.begin_claim_operation(
            self.deal_id,
            old_manager,
            operation_key=self.operation_key(),
            request={
                "claimMarker": self.attempt_marker(old_manager),
                "dealVersion": self.version,
                "attemptStartedAt": first_started_at,
            },
        )
        self.store.fail_claim_operation(
            self.operation_key(),
            "old manager attempt safely failed",
            result={"remoteUpdated": False},
        )
        reassigned_marker = app.claim_attempt_marker(
            self.operation_key(), self.manager_id, nonce="reassigned-attempt"
        )

        def lose_reassigned_verification(method, params=None, timeout=None):
            if method == "crm.deal.get":
                lose_reassigned_verification.reads += 1
                if lose_reassigned_verification.reads <= 2:
                    return self.source_deal(manager=old_manager)
                raise TimeoutError("reassigned attempt verification lost")
            if method == "crm.deal.update":
                return True
            raise AssertionError(method)

        lose_reassigned_verification.reads = 0
        with (
            self.common_claim_context(),
            patch.object(app, "local_now", return_value=reassigned_started_at),
            patch.object(app.secrets, "token_hex", return_value="reassigned-attempt"),
            patch.object(app, "bitrix_call", side_effect=lose_reassigned_verification),
        ):
            failed = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )

        self.assertFalse(failed["ok"])
        operation = self.store.get_claim_operation(self.operation_key())
        self.assertEqual(operation["managerId"], self.manager_id)
        self.assertEqual(
            operation["request"]["attemptStartedAt"],
            reassigned_started_at.isoformat(),
        )
        claimed = self.claimed_deal()
        claimed[app.BITRIX_CLAIM_MARKER_FIELD] = reassigned_marker
        with patch.object(app, "bitrix_call", return_value=claimed):
            summary = app.reconcile_stale_claim_operations()

        self.assertEqual(summary["recovered"], 1)
        self.assertEqual(self.store.count_claims(old_manager), 0)
        self.assertEqual(
            self.store.count_claims(self.manager_id, "2026-08-17", "2026-08-17"),
            1,
        )

    def test_failed_uncertain_update_is_recovered_without_browser_retry(self):
        self.begin_operation()
        self.store.fail_claim_operation(
            self.operation_key(),
            "update_verification_failed",
            result={"remoteUpdateUncertain": True},
        )
        with patch.object(app, "bitrix_call", return_value=self.claimed_deal()):
            summary = app.reconcile_stale_claim_operations()
        self.assertEqual(summary["recovered"], 1)
        self.assertEqual(self.store.count_claims(self.manager_id), 1)

    def test_failed_uncertain_unchanged_source_is_released_for_safe_retry(self):
        self.begin_operation()
        self.store.fail_claim_operation(
            self.operation_key(),
            "update verification timeout",
            result={"remoteUpdateUncertain": True},
        )
        with patch.object(app, "bitrix_call", return_value=self.source_deal()):
            summary = app.reconcile_stale_claim_operations()

        self.assertEqual(summary["released"], 1)
        self.assertEqual(
            self.store.list_unresolved_claim_operations(self.manager_id),
            [],
        )
        self.assertTrue(app.check_manager_access(self.manager_id)["ok"])

        new_marker = app.claim_attempt_marker(
            self.operation_key(), self.manager_id, nonce="new-attempt"
        )
        claimed = self.claimed_deal()
        claimed[app.BITRIX_CLAIM_MARKER_FIELD] = new_marker
        side_effect = self.claim_side_effect(
            [self.source_deal(), self.source_deal(), claimed]
        )
        with (
            self.common_claim_context(),
            patch.object(app.secrets, "token_hex", return_value="new-attempt"),
            patch.object(app, "bitrix_call", side_effect=side_effect),
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )

        self.assertTrue(result["ok"])
        self.assertEqual(self.store.count_claims(self.manager_id), 1)

    def test_matching_stage_and_manager_without_marker_is_not_attributed_to_app(self):
        self.begin_operation()
        self.backdate_operation()
        live = self.claimed_deal()
        live.pop(app.BITRIX_CLAIM_MARKER_FIELD)
        with patch.object(app, "bitrix_call", return_value=live):
            summary = app.reconcile_stale_claim_operations()
        self.assertEqual(summary["recovered"], 0)
        self.assertEqual(self.store.count_claims(self.manager_id), 0)
        self.assertEqual(
            len(self.store.list_unresolved_claim_operations(self.manager_id)),
            1,
        )
        access = app.check_manager_access(self.manager_id)
        self.assertFalse(access["ok"])
        self.assertTrue(access["recoveryPending"])


class TestGreetingLifecycle(TemporaryStateTestCase):
    @staticmethod
    def official_history(*, session_id="321", chat_id="1763", deal_id="100"):
        return {
            "sessionId": int(session_id),
            "chatId": int(chat_id),
            "chat": {
                str(chat_id): {
                    "id": str(chat_id),
                    "entityType": "LINES",
                    "entityData2": f"LEAD|0|COMPANY|0|CONTACT|42|DEAL|{deal_id}",
                    "textFieldEnabled": True,
                    "messageType": "L",
                }
            },
        }

    def test_openline_history_uses_official_nested_chat_shape(self):
        with patch.object(app, "bitrix_call", return_value=self.official_history()):
            context = app.get_openline_chat_context("321")

        self.assertEqual(context["sessionId"], "321")
        self.assertEqual(context["chatId"], "1763")
        self.assertEqual(context["entityType"], "LINES")
        self.assertTrue(context["textFieldEnabled"])

    def test_actor_path_accepts_the_previous_flat_chat_shape(self):
        old_portal_history = {
            "chatId": 1763,
            "chat": {"id": 1763, "name": "OpenLine chat"},
        }
        with patch.object(app, "bitrix_call", return_value=old_portal_history):
            context = app.actor_greeting_chat_context("100", "321")

        self.assertEqual(context["sessionId"], "321")
        self.assertEqual(context["chatId"], "1763")
        self.assertIsNone(context["textFieldEnabled"])

    def test_actor_path_allows_unbound_deal_zero_metadata(self):
        history = self.official_history()
        history["chat"]["1763"]["entityData2"] = "LEAD|0|DEAL|0"
        with patch.object(app, "bitrix_call", return_value=history):
            context = app.actor_greeting_chat_context("100", "321")

        self.assertEqual(context["chatId"], "1763")

    def test_entity_binding_requires_one_exact_deal_pair(self):
        self.assertTrue(
            app.openline_chat_is_bound_to_deal(
                "LEAD|0|COMPANY|0|CONTACT|42|DEAL|100",
                "100",
            )
        )
        self.assertFalse(app.openline_chat_is_bound_to_deal("DEAL|1000", "100"))
        self.assertFalse(app.openline_chat_is_bound_to_deal("DEAL|100|DEAL|100", "100"))
        self.assertFalse(app.openline_chat_is_bound_to_deal("prefixDEAL|100", "100"))

    def test_auto_greeting_uses_only_exact_crm_bound_message_method(self):
        history = self.official_history()

        def fake_bitrix(method, params=None, timeout=None):
            if method == "imopenlines.session.history.get":
                return history
            if method == "imopenlines.crm.chat.get":
                return [{"CHAT_ID": "1763", "CONNECTOR_ID": "instagram"}]
            if method == "imopenlines.crm.chat.getLastId":
                return 1763
            if method == "imopenlines.crm.chat.user.add":
                return 1763
            if method == "imopenlines.crm.message.add":
                return 85851
            raise AssertionError(f"unsafe or unexpected method: {method}")

        with patch.object(app, "bitrix_call", side_effect=fake_bitrix) as bitrix_call:
            result = app.send_greeting_message(
                "100",
                "2002",
                "Здравствуйте!",
                {"openlineSessionIds": ["321"]},
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["messageId"], "85851")
        methods = [item.args[0] for item in bitrix_call.call_args_list]
        self.assertNotIn("im.message.add", methods)
        self.assertNotIn("imopenlines.operator.answer", methods)
        self.assertIn(
            call(
                "imopenlines.crm.message.add",
                {
                    "CRM_ENTITY_TYPE": "deal",
                    "CRM_ENTITY": "100",
                    "USER_ID": "2002",
                    "CHAT_ID": "1763",
                    "MESSAGE": "Здравствуйте!",
                },
            ),
            bitrix_call.call_args_list,
        )

    def test_auto_greeting_never_sends_when_history_points_to_another_deal(self):
        with patch.object(
            app,
            "bitrix_call",
            return_value=self.official_history(deal_id="999"),
        ) as bitrix_call:
            with self.assertRaisesRegex(RuntimeError, "chat_entity_mismatch"):
                app.resolve_greeting_target(
                    "100",
                    "2002",
                    {"openlineSessionIds": ["321"]},
                )

        self.assertEqual(
            [item.args[0] for item in bitrix_call.call_args_list],
            ["imopenlines.session.history.get"],
        )

    def test_new_claim_lifecycle_never_reuses_an_old_managers_greeting(self):
        self.store.append_greeting(
            {
                "timestamp": "2026-08-17T09:00:00+06:00",
                "managerId": "1001",
                "dealId": "100",
                "operationKey": "claim:old-lifecycle",
                "direction": "Египет",
                "text": "Здравствуйте! Старый текст менеджера.",
                "status": "manual",
            }
        )
        context = {
            "classification": {
                "direction": "Турция",
                "confidence": "средняя",
                "matched": ["Турция"],
            },
            "messages": ["Хочу в Турцию"],
            "openlineSessionIds": [],
        }
        with (
            patch.object(
                app,
                "get_manager_profile",
                return_value={
                    "id": "2002",
                    "name": "Новый Менеджер",
                    "active": True,
                    "intranet": True,
                    "competencies": ["Турция"],
                },
            ),
            patch.object(app, "greeting_context_from_deal", return_value=context) as context_call,
        ):
            first = app.prepare_greeting(
                "2002",
                "100",
                operation_key="claim:new-lifecycle",
            )
            repeated = app.prepare_greeting(
                "2002",
                "100",
                operation_key="claim:new-lifecycle",
            )

        self.assertIn("Новый", first["text"])
        self.assertNotIn("Старый текст", first["text"])
        self.assertEqual(repeated["status"], "skipped_duplicate")
        self.assertEqual(len(self.store.list_greetings()), 2)
        self.assertEqual(
            self.store.latest_greeting_by_operation("claim:new-lifecycle")["managerId"],
            "2002",
        )
        context_call.assert_called_once_with("100")


class TestGreetingOutboxWorker(ClaimWorkflowTestCase):
    """The claim response stays fast while delivery remains durable and safe."""

    def greeting_context(self, *, deal_id=None):
        return {
            "classification": {
                "direction": "Турция",
                "confidence": "средняя",
                "matched": ["Турция"],
            },
            "messages": ["Хочу в Турцию"],
            "openlineSessionIds": ["321"],
        }

    def official_history(self, *, deal_id=None):
        deal_id = str(deal_id or self.deal_id)
        return {
            "sessionId": 321,
            "chatId": 1763,
            "chat": {
                "1763": {
                    "id": "1763",
                    "entityType": "LINES",
                    "entityData2": f"LEAD|0|COMPANY|0|CONTACT|42|DEAL|{deal_id}",
                    "textFieldEnabled": True,
                    "messageType": "L",
                }
            },
        }

    def seed_greeting_outbox(self):
        marker = self.attempt_marker()
        self.store.begin_claim_operation(
            self.deal_id,
            self.manager_id,
            operation_key=self.operation_key(),
            request={
                "claimMarker": marker,
                "dealVersion": self.version,
                "greetingRequested": True,
                "greetingContext": {
                    "sessionId": "321",
                    "direction": "Турция",
                },
            },
        )
        finalized = self.store.finalize_claim_operation(
            self.operation_key(),
            claim=app.claim_log_entry(self.manager_id, self.claimed_deal()),
            result={"ok": True, "auditRecorded": True, "dealId": self.deal_id},
            expected_claim_marker=marker,
        )
        self.assertTrue(finalized["greetingQueued"])
        return self.store.get_greeting_outbox(self.operation_key())

    def active_manager(self):
        return {
            "id": self.manager_id,
            "name": "Manager",
            "active": True,
            "intranet": True,
            "competencies": ["Турция"],
        }

    def actor_auth(self):
        return {
            "access_token": "actor-oauth-secret-must-stay-in-memory",
            "domain": "test-fake.bitrix24.test",
        }

    def test_attach_returns_without_waiting_for_actor_network_and_never_persists_token(self):
        self.seed_greeting_outbox()
        with patch.object(
            app,
            "bitrix_oauth_call",
            return_value={"ID": self.manager_id, "NAME": "Manager"},
        ):
            self.assertEqual(
                app.verify_bitrix_user(self.actor_auth(), allow_cached=False)["id"],
                self.manager_id,
            )
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def blocked_delivery(job, worker_token, auth, manager):
            started.set()
            release.wait(timeout=2)
            finished.set()

        original = {"ok": True, "auditRecorded": True, "dealId": self.deal_id}
        try:
            with (
                patch.multiple(
                    app,
                    GREETING_AUTO_SEND=True,
                    GREETING_AUTO_SEND_SUPPORTED=True,
                ),
                patch.object(
                    app,
                    "process_actor_greeting_outbox_job",
                    side_effect=blocked_delivery,
                ),
            ):
                before = time.monotonic()
                response = app.attach_greeting_to_claim(
                    original,
                    self.manager_id,
                    self.deal_id,
                    self.actor_auth(),
                    self.operation_key(),
                    self.active_manager(),
                )
                elapsed = time.monotonic() - before

            self.assertTrue(started.wait(timeout=1))
            self.assertLess(elapsed, 0.5)
            self.assertEqual(response["greeting"]["status"], "queued")
            self.assertEqual(
                self.store.get_greeting_outbox(self.operation_key())["status"],
                "checking",
            )
            with self.store._connect() as connection:
                database_dump = "\n".join(connection.iterdump())
            self.assertNotIn("actor-oauth-secret-must-stay-in-memory", database_dump)
        finally:
            release.set()
            finished.wait(timeout=2)

    def test_actor_sender_uses_proven_methods_and_sends_only_once(self):
        self.seed_greeting_outbox()
        worker_token = "actor-worker"
        job = self.store.lease_exact_greeting_outbox(
            self.operation_key(),
            worker_token,
        )

        def fake_bitrix(method, params=None, timeout=None):
            if method == "crm.deal.get":
                return self.claimed_deal()
            if method == "imopenlines.session.history.get":
                return self.official_history()
            raise AssertionError(f"unexpected webhook method: {method}")

        def fake_actor_call(auth, method, params=None):
            self.assertEqual(auth, self.actor_auth())
            if method == "imopenlines.operator.answer":
                raise RuntimeError("Bitrix API error: ALREADY_RESPONSIBLE")
            if method == "im.message.add":
                return 85851
            raise AssertionError(f"unexpected actor method: {method}")

        with (
            patch.object(app, "bitrix_call", side_effect=fake_bitrix),
            patch.object(
                app,
                "bitrix_call_for_actor",
                side_effect=fake_actor_call,
            ) as actor_call,
        ):
            result = app.process_actor_greeting_outbox_job(
                job,
                worker_token,
                self.actor_auth(),
                self.active_manager(),
            )
            fallback = self.store.lease_greeting_outbox("fallback-worker")

        self.assertEqual(result["status"], "sent")
        self.assertEqual(result["messageId"], "85851")
        self.assertEqual(fallback, [])
        self.assertEqual(
            actor_call.call_args_list,
            [
                call(
                    self.actor_auth(),
                    "imopenlines.operator.answer",
                    {"CHAT_ID": "1763"},
                ),
                call(
                    self.actor_auth(),
                    "im.message.add",
                    {
                        "DIALOG_ID": "chat1763",
                        "MESSAGE": (
                            "Здравствуйте! Меня зовут Manager. "
                            "Я эксперт по направлению Турция. "
                            "Сейчас посмотрю варианты и посчитаю для вас стоимость тура."
                        ),
                    },
                ),
            ],
        )

    def test_actor_sender_rejects_an_explicit_foreign_deal_binding_without_send(self):
        self.seed_greeting_outbox()
        worker_token = "actor-worker"
        job = self.store.lease_exact_greeting_outbox(
            self.operation_key(),
            worker_token,
        )

        def fake_bitrix(method, params=None, timeout=None):
            if method == "crm.deal.get":
                return self.claimed_deal()
            if method == "imopenlines.session.history.get":
                return self.official_history(deal_id="999")
            raise AssertionError(f"unexpected webhook method: {method}")

        with (
            patch.object(app, "bitrix_call", side_effect=fake_bitrix),
            patch.object(app, "bitrix_call_for_actor") as actor_call,
        ):
            result = app.process_actor_greeting_outbox_job(
                job,
                worker_token,
                self.actor_auth(),
                self.active_manager(),
            )

        self.assertEqual(result["status"], "manual")
        self.assertEqual(result["errorCode"], "chat_entity_mismatch")
        actor_call.assert_not_called()
        self.assertEqual(
            self.store.get_greeting_outbox(self.operation_key())["status"],
            "manual",
        )

    def test_actor_history_timeout_stays_pending_for_bounded_pre_send_retry(self):
        self.seed_greeting_outbox()
        worker_token = "actor-worker"
        job = self.store.lease_exact_greeting_outbox(
            self.operation_key(),
            worker_token,
        )

        def fake_bitrix(method, params=None, timeout=None):
            if method == "crm.deal.get":
                return self.claimed_deal()
            if method == "imopenlines.session.history.get":
                raise TimeoutError("history timed out")
            raise AssertionError(f"unexpected webhook method: {method}")

        with (
            patch.object(app, "bitrix_call", side_effect=fake_bitrix),
            patch.object(app, "bitrix_call_for_actor") as actor_call,
        ):
            result = app.process_actor_greeting_outbox_job(
                job,
                worker_token,
                self.actor_auth(),
                self.active_manager(),
            )

        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["errorCode"], "actor_greeting_preflight_failed")
        self.assertEqual(result["attemptCount"], 1)
        self.assertIsNotNone(result["nextAttemptAt"])
        actor_call.assert_not_called()

    def test_actor_message_timeout_is_uncertain_and_never_retried(self):
        self.seed_greeting_outbox()
        worker_token = "actor-worker"
        job = self.store.lease_exact_greeting_outbox(
            self.operation_key(),
            worker_token,
        )

        def fake_bitrix(method, params=None, timeout=None):
            if method == "crm.deal.get":
                return self.claimed_deal()
            if method == "imopenlines.session.history.get":
                return self.official_history()
            raise AssertionError(f"unexpected webhook method: {method}")

        def fake_actor_call(auth, method, params=None):
            if method == "imopenlines.operator.answer":
                return True
            if method == "im.message.add":
                raise TimeoutError("response lost after dispatch")
            raise AssertionError(f"unexpected actor method: {method}")

        with (
            patch.object(app, "bitrix_call", side_effect=fake_bitrix),
            patch.object(
                app,
                "bitrix_call_for_actor",
                side_effect=fake_actor_call,
            ) as actor_call,
        ):
            result = app.process_actor_greeting_outbox_job(
                job,
                worker_token,
                self.actor_auth(),
                self.active_manager(),
            )
            fallback = self.store.lease_greeting_outbox("fallback-worker")

        self.assertEqual(result["status"], "uncertain")
        self.assertEqual(result["errorCode"], "send_result_uncertain")
        self.assertEqual(fallback, [])
        self.assertEqual(
            [item.args[1] for item in actor_call.call_args_list],
            ["imopenlines.operator.answer", "im.message.add"],
        )

    def test_successful_claim_queues_greeting_without_synchronous_chat_work(self):
        side_effect = self.claim_side_effect(
            [self.source_deal(), self.source_deal(), self.claimed_deal()]
        )
        with (
            self.common_claim_context(greeting=False),
            patch.multiple(
                app,
                DRY_RUN=False,
                GREETING_AUTO_SEND=True,
                GREETING_AUTO_SEND_SUPPORTED=True,
            ),
            patch.object(
                app,
                "cached_greeting_context",
                return_value=self.greeting_context(),
            ) as cached_context,
            patch.object(
                app,
                "prepare_greeting",
                side_effect=AssertionError("claim path must not prepare greeting"),
            ) as prepare_greeting,
            patch.object(
                app,
                "resolve_greeting_target",
                side_effect=AssertionError("claim path must not inspect chat"),
            ) as resolve_target,
            patch.object(
                app,
                "send_greeting_message",
                side_effect=AssertionError("claim path must not send greeting"),
            ) as send_greeting,
            patch.object(app, "bitrix_call", side_effect=side_effect),
        ):
            result = app.preview_claim(
                self.deal_id,
                self.manager_id,
                selection_token=self.token(),
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["auditRecorded"])
        self.assertEqual(result["greeting"]["status"], "queued")
        self.assertEqual(result["greeting"]["text"], "")
        job = self.store.get_greeting_outbox(self.operation_key())
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["sessionId"], "321")
        self.assertEqual(job["direction"], "Турция")
        cached_context.assert_called_once_with(self.deal_id, self.version)
        prepare_greeting.assert_not_called()
        resolve_target.assert_not_called()
        send_greeting.assert_not_called()

    def test_worker_sends_once_through_exact_crm_bound_path(self):
        self.seed_greeting_outbox()

        def fake_bitrix(method, params=None, timeout=None):
            if method == "crm.deal.get":
                return self.claimed_deal()
            if method == "imopenlines.session.history.get":
                return self.official_history()
            if method == "imopenlines.crm.chat.get":
                return [{"CHAT_ID": "1763", "CONNECTOR_ID": "instagram"}]
            if method == "imopenlines.crm.chat.getLastId":
                return 1763
            if method == "imopenlines.crm.chat.user.add":
                return 1763
            if method == "imopenlines.crm.message.add":
                return 85851
            raise AssertionError(f"unsafe or unexpected method: {method}")

        with (
            patch.multiple(
                app,
                DRY_RUN=False,
                GREETING_AUTO_SEND=True,
                GREETING_AUTO_SEND_SUPPORTED=True,
            ),
            patch.object(app, "get_greeting_manager_profile", return_value=self.active_manager()),
            patch.object(app, "bitrix_call", side_effect=fake_bitrix) as bitrix_call,
        ):
            result = app.process_greeting_outbox_once(worker_token="worker-1", limit=1)

        self.assertEqual(result, {"leased": 1, "processed": 1})
        job = self.store.get_greeting_outbox(self.operation_key())
        self.assertEqual(job["status"], "sent")
        self.assertEqual(job["messageId"], "85851")
        self.assertEqual(job["attemptCount"], 1)
        methods = [item.args[0] for item in bitrix_call.call_args_list]
        self.assertNotIn("im.message.add", methods)
        self.assertNotIn("imopenlines.operator.answer", methods)
        self.assertEqual(methods.count("imopenlines.crm.message.add"), 1)
        self.assertIn(
            call(
                "imopenlines.crm.message.add",
                {
                    "CRM_ENTITY_TYPE": "deal",
                    "CRM_ENTITY": self.deal_id,
                    "USER_ID": self.manager_id,
                    "CHAT_ID": "1763",
                    "MESSAGE": (
                        "Здравствуйте! Меня зовут Manager. "
                        "Я эксперт по направлению Турция. "
                        "Сейчас посмотрю варианты и посчитаю для вас стоимость тура."
                    ),
                },
            ),
            bitrix_call.call_args_list,
        )
        audit = self.store.latest_greeting_by_operation(self.operation_key())
        self.assertEqual(audit["status"], "sent")
        self.assertTrue(audit["autoSent"])

    def test_worker_binding_mismatch_becomes_manual_without_send(self):
        self.seed_greeting_outbox()

        def fake_bitrix(method, params=None, timeout=None):
            if method == "crm.deal.get":
                return self.claimed_deal()
            if method == "imopenlines.session.history.get":
                return self.official_history(deal_id="999")
            raise AssertionError(f"unexpected write after binding mismatch: {method}")

        with (
            patch.multiple(
                app,
                DRY_RUN=False,
                GREETING_AUTO_SEND=True,
                GREETING_AUTO_SEND_SUPPORTED=True,
            ),
            patch.object(app, "get_greeting_manager_profile", return_value=self.active_manager()),
            patch.object(app, "bitrix_call", side_effect=fake_bitrix) as bitrix_call,
            patch.object(app, "send_greeting_message") as send_greeting,
        ):
            result = app.process_greeting_outbox_once(worker_token="worker-1", limit=1)

        self.assertEqual(result, {"leased": 1, "processed": 1})
        job = self.store.get_greeting_outbox(self.operation_key())
        self.assertEqual(job["status"], "manual")
        self.assertEqual(job["errorCode"], "chat_entity_mismatch")
        send_greeting.assert_not_called()
        self.assertNotIn(
            "imopenlines.crm.message.add",
            [item.args[0] for item in bitrix_call.call_args_list],
        )
        audit = self.store.latest_greeting_by_operation(self.operation_key())
        self.assertEqual(audit["status"], "manual")
        self.assertFalse(audit["autoSent"])

    def test_send_exception_is_uncertain_and_never_automatically_retried(self):
        self.seed_greeting_outbox()
        with (
            patch.multiple(
                app,
                DRY_RUN=False,
                GREETING_AUTO_SEND=True,
                GREETING_AUTO_SEND_SUPPORTED=True,
            ),
            patch.object(app, "get_greeting_manager_profile", return_value=self.active_manager()),
            patch.object(app, "bitrix_call", return_value=self.claimed_deal()),
            patch.object(
                app,
                "resolve_greeting_target",
                return_value={"chatId": "1763", "sessionId": "321"},
            ),
            patch.object(
                app,
                "send_greeting_message",
                side_effect=TimeoutError("response lost after dispatch"),
            ) as send_greeting,
        ):
            first = app.process_greeting_outbox_once(worker_token="worker-1", limit=1)
            second = app.process_greeting_outbox_once(worker_token="worker-2", limit=1)

        self.assertEqual(first, {"leased": 1, "processed": 1})
        self.assertEqual(second, {"leased": 0, "processed": 0})
        send_greeting.assert_called_once()
        job = self.store.get_greeting_outbox(self.operation_key())
        self.assertEqual(job["status"], "uncertain")
        self.assertEqual(job["errorCode"], "send_result_uncertain")
        self.assertEqual(job["attemptCount"], 1)
        audit = self.store.latest_greeting_by_operation(
            self.operation_key(),
            statuses=None,
        )
        self.assertEqual(audit["status"], "uncertain")
        self.assertFalse(audit["autoSent"])

    def test_attach_pending_job_returns_queued_without_network(self):
        self.seed_greeting_outbox()
        original = {
            "ok": True,
            "auditRecorded": True,
            "dealId": self.deal_id,
        }
        with (
            patch.multiple(
                app,
                GREETING_AUTO_SEND=True,
                GREETING_AUTO_SEND_SUPPORTED=True,
            ),
            patch.object(app, "GREETING_WAKE_EVENT") as wake_event,
            patch.object(app, "prepare_greeting") as prepare_greeting,
            patch.object(app, "resolve_greeting_target") as resolve_target,
            patch.object(app, "send_greeting_message") as send_greeting,
            patch.object(app, "bitrix_call") as bitrix_call,
        ):
            response = app.attach_greeting_to_claim(
                original,
                self.manager_id,
                self.deal_id,
                {"AUTH_ID": "must-not-be-used"},
                self.operation_key(),
            )

        self.assertIsNot(response, original)
        self.assertEqual(response["greeting"]["status"], "queued")
        self.assertEqual(response["greeting"]["text"], "")
        wake_event.set.assert_called_once_with()
        prepare_greeting.assert_not_called()
        resolve_target.assert_not_called()
        send_greeting.assert_not_called()
        bitrix_call.assert_not_called()

    def test_dry_run_pauses_an_existing_greeting_outbox_without_network(self):
        self.seed_greeting_outbox()
        with (
            patch.multiple(
                app,
                DRY_RUN=True,
                GREETING_AUTO_SEND=True,
                GREETING_AUTO_SEND_SUPPORTED=True,
            ),
            patch.object(
                app,
                "process_greeting_outbox_job",
                side_effect=AssertionError("dry run must not process greeting jobs"),
            ) as process_job,
        ):
            result = app.process_greeting_outbox_once(
                worker_token="worker-dry-run",
                limit=1,
            )

        self.assertEqual(result, {"leased": 0, "processed": 0})
        self.assertEqual(
            self.store.get_greeting_outbox(self.operation_key())["status"],
            "pending",
        )
        process_job.assert_not_called()


class TestSafeConfigurationAndMinimization(TemporaryStateTestCase):
    def test_unknown_dry_run_boolean_keeps_safe_default_and_marks_readiness_error(self):
        original = set(app.INVALID_ENV_VALUES)
        try:
            app.INVALID_ENV_VALUES.clear()
            with patch.dict(os.environ, {"DRY_RUN": "treu"}):
                self.assertTrue(app.env_bool("DRY_RUN", True))
            self.assertIn("DRY_RUN", app.INVALID_ENV_VALUES)
        finally:
            app.INVALID_ENV_VALUES.clear()
            app.INVALID_ENV_VALUES.update(original)

    def test_invalid_numeric_env_keeps_safe_default_and_is_reported(self):
        with (
            patch.object(app, "INVALID_ENV_VALUES", set()),
            patch.dict(os.environ, {"APP_TZ_OFFSET_HOURS": "not-a-number"}),
        ):
            self.assertEqual(app.env_int("APP_TZ_OFFSET_HOURS", 6, -12, 14), 6)
            self.assertIn("APP_TZ_OFFSET_HOURS", app.INVALID_ENV_VALUES)

    def test_admin_ids_are_not_embedded_in_public_html(self):
        with patch.object(app, "ADMIN_USER_IDS", {"987654321"}):
            rendered = app.render_index_html(nonce="nonce")
        self.assertNotIn("987654321", rendered)

    def test_greeting_auto_send_and_untrusted_cors_fail_readiness_without_reflection(self):
        secretish_origin = "https://user:secret@evil.example/path?token=hidden"
        with patch.multiple(
            app,
            APP_DIR=self.data_dir,
            MANAGERS_FILE=self.data_dir / "managers.json",
            PUBLIC_APP_URL="https://picker.example.test",
            RAW_APP_ALLOWED_ORIGINS={secretish_origin},
            APP_ALLOWED_ORIGINS={"https://evil.example"},
            RAW_BITRIX_ALLOWED_DOMAINS={"test-fake.bitrix24.test"},
            ALLOWED_BITRIX_DOMAINS={"test-fake.bitrix24.test"},
            ADMIN_USER_IDS={"1"},
            CLAIM_STATS_SOURCE="app_events",
            REQUIRE_LEGACY_MIGRATION=False,
            GREETING_AUTO_SEND=True,
            STATE_STORE=self.store,
        ):
            result = app.readiness_state(force=True)
        rendered = json.dumps(result, ensure_ascii=False)
        self.assertFalse(result["ok"])
        self.assertNotIn("secret", rendered)
        self.assertNotIn("hidden", rendered)

    def test_write_mode_requires_valid_claim_marker_field(self):
        with patch.multiple(
            app,
            APP_DIR=self.data_dir,
            MANAGERS_FILE=self.data_dir / "managers.json",
            PUBLIC_APP_URL="https://picker.example.test",
            RAW_APP_ALLOWED_ORIGINS={"https://picker.example.test"},
            APP_ALLOWED_ORIGINS={"https://picker.example.test"},
            RAW_BITRIX_ALLOWED_DOMAINS={"test-fake.bitrix24.test"},
            ALLOWED_BITRIX_DOMAINS={"test-fake.bitrix24.test"},
            ADMIN_USER_IDS={"1"},
            CLAIM_STATS_SOURCE="app_events",
            REQUIRE_LEGACY_MIGRATION=False,
            DRY_RUN=False,
            BITRIX_CLAIM_MARKER_FIELD="UF_CRM_REPLACE_WITH_FIELD",
            STATE_STORE=self.store,
        ):
            result = app.readiness_state(force=True)
        self.assertFalse(result["ok"])
        self.assertTrue(any("BITRIX_CLAIM_MARKER_FIELD" in item for item in result["errors"]))

    def test_enabled_baza_integration_rejects_public_placeholder_secret(self):
        with patch.multiple(
            app,
            APP_DIR=self.data_dir,
            MANAGERS_FILE=self.data_dir / "managers.json",
            PUBLIC_APP_URL="https://picker.example.test",
            RAW_APP_ALLOWED_ORIGINS={"https://picker.example.test"},
            APP_ALLOWED_ORIGINS={"https://picker.example.test"},
            RAW_BITRIX_ALLOWED_DOMAINS={"test-fake.bitrix24.test"},
            ALLOWED_BITRIX_DOMAINS={"test-fake.bitrix24.test"},
            ADMIN_USER_IDS={"1"},
            CLAIM_STATS_SOURCE="app_events",
            REQUIRE_LEGACY_MIGRATION=False,
            REQUIRE_EXPLICIT_ACCESS_RULE=True,
            DRY_RUN=True,
            GREETING_AUTO_SEND=False,
            EXTRA_CLAIM_REQUESTS_ENABLED=True,
            BAZA_API_BASE_URL="https://baza.example.test",
            BAZA_HMAC_KEY_ID="deal-picker-v1",
            BAZA_HMAC_SECRET="REPLACE_WITH_A_PUBLIC_PLACEHOLDER_SECRET",
            STATE_STORE=self.store,
        ):
            result = app.readiness_state(force=True)
        self.assertFalse(result["ok"])
        self.assertTrue(any("BAZA_HMAC_SECRET" in item for item in result["errors"]))


class TestBazaExtraClaimIntegration(TemporaryStateTestCase):
    def setUp(self):
        super().setUp()
        self.store.set_rule("42", enabled=True, daily_limit=1)

    def add_taken_claim(self, timestamp="2026-08-17T10:00:00+06:00"):
        self.store.append_claim(
            {"timestamp": timestamp, "managerId": "42", "dealId": "taken-1"}
        )

    def test_extra_claim_routes_use_fresh_verified_actor_not_browser_manager_id(self):
        body = {
            "auth": {"AUTH_ID": "fresh-token", "DOMAIN": "test-fake.bitrix24.test"},
            "managerId": "999",
            "reason": "Клиент ждёт срочный ответ сегодня",
        }
        for path in ("/api/extra-claim/status", "/api/extra-claim/request"):
            with self.subTest(path=path):
                handler = HandlerHarness.make("POST", path, body)
                with (
                    patch.object(app, "readiness_state", return_value={"ok": True}),
                    patch.object(app, "rate_limit_allowed", return_value=True),
                    patch.object(app, "actor_id_from_payload", return_value="42") as actor,
                    patch.object(
                        app,
                        "extra_claim_limit_state",
                        return_value={"ok": True, "managerId": "42"},
                    ) as status,
                    patch.object(
                        app,
                        "request_extra_claim",
                        return_value={"ok": True, "managerId": "42"},
                    ) as request,
                ):
                    handler.do_POST()

                self.assertEqual(HandlerHarness.status(handler), 200)
                actor.assert_called_once_with(body, allow_cached=False)
                if path.endswith("/status"):
                    status.assert_called_once_with("42", refresh=True)
                    request.assert_not_called()
                else:
                    request.assert_called_once_with("42", body["reason"])
                    status.assert_not_called()

    def test_hmac_headers_match_canonical_contract(self):
        body = app.canonical_json_bytes({"reason": "Пример", "count": 1})
        with patch.multiple(
            app,
            BAZA_HMAC_KEY_ID="picker-v1",
            BAZA_HMAC_SECRET="s" * 32,
        ):
            headers = app.sign_baza_request(
                "POST",
                "/integrations/deal-picker/v1/claim-events",
                body,
                timestamp="1700000000",
                nonce="nonce-1",
            )
        canonical = (
            "1700000000\nnonce-1\nPOST\n"
            "/integrations/deal-picker/v1/claim-events\n"
            + app.hashlib.sha256(body).hexdigest()
        ).encode("utf-8")
        expected = app.hmac.new(b"s" * 32, canonical, app.hashlib.sha256).hexdigest()
        self.assertEqual(headers["X-Krugosvet-Key-Id"], "picker-v1")
        self.assertEqual(headers["X-Krugosvet-Signature"], expected)

    def test_baza_outage_is_not_contacted_for_within_limit_state(self):
        with (
            patch.multiple(
                app,
                EXTRA_CLAIM_REQUESTS_ENABLED=True,
                BAZA_API_BASE_URL="https://baza.example.test",
                BAZA_HMAC_KEY_ID="picker-v1",
                BAZA_HMAC_SECRET="s" * 32,
            ),
            patch.object(app, "local_date", return_value="2026-08-17"),
            patch.object(app, "is_limit_bypassed_now", return_value=False),
            patch.object(app, "baza_post", side_effect=TimeoutError("down")) as baza_post,
        ):
            state = app.extra_claim_limit_state("42", refresh=True)
        self.assertFalse(state["limitReached"])
        baza_post.assert_not_called()

    def test_limit_response_exposes_request_and_approved_grant(self):
        self.add_taken_claim()
        request = self.store.create_extra_claim_request(
            "42",
            "2026-08-17",
            "Нужна дополнительная срочная заявка",
            taken_today_snapshot=1,
            daily_limit_snapshot=1,
        )
        self.store.apply_extra_claim_request_response(
            request["requestKey"], {"id": "req-42", "status": "pending"}
        )
        with (
            patch.multiple(app, EXTRA_CLAIM_REQUESTS_ENABLED=True),
            patch.object(app, "local_date", return_value="2026-08-17"),
            patch.object(app, "is_limit_bypassed_now", return_value=False),
        ):
            denied = app.check_manager_access("42")
            self.assertFalse(denied["ok"])
            self.assertTrue(denied["limitReached"])
            self.assertEqual(denied["extraClaimRequest"]["status"], "pending")

            self.store.import_extra_claim_state(
                "42",
                "2026-08-17",
                {
                    "request": {"id": "req-42", "status": "approved"},
                    "grants": [
                        {
                            "id": "req-42",
                            "requestId": "req-42",
                            "status": "approved",
                            "businessDate": "2026-08-17",
                        }
                    ],
                },
            )
            allowed = app.check_manager_access("42")
        self.assertTrue(allowed["ok"])
        self.assertTrue(allowed["extraClaimRequired"])

    def test_request_double_click_is_local_idempotent_during_baza_outage(self):
        self.add_taken_claim()
        manager = {
            "id": "42",
            "name": "Manager",
            "active": True,
            "intranet": True,
            "competencies": ["Турция"],
        }
        with (
            patch.multiple(
                app,
                EXTRA_CLAIM_REQUESTS_ENABLED=True,
                BAZA_API_BASE_URL="https://baza.example.test",
                BAZA_HMAC_KEY_ID="picker-v1",
                BAZA_HMAC_SECRET="s" * 32,
            ),
            patch.object(app, "local_date", return_value="2026-08-17"),
            patch.object(app, "is_limit_bypassed_now", return_value=False),
            patch.object(app, "get_manager_profile", return_value=manager),
            patch.object(app, "flush_integration_outbox", return_value={"sent": 0}),
            patch.object(app, "refresh_extra_claim_state", side_effect=TimeoutError("down")),
        ):
            first = app.request_extra_claim("42", "Клиент ждёт срочный ответ сегодня")
            second = app.request_extra_claim("42", "Повторный клик с другой причиной")
        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertEqual(
            first["request"]["requestKey"], second["request"]["requestKey"]
        )
        requests = [i for i in self.store.list_outbox() if i["kind"] == "extra_claim_request"]
        self.assertEqual(len(requests), 1)

    def test_rendered_ui_contains_reason_modal_and_all_request_states(self):
        rendered = app.render_index_html(initial_auth={}, nonce="fixed-nonce")
        for expected in (
            "Запросить ещё 1 заявку",
            "Получить дополнительную заявку",
            "Запрос на рассмотрении",
            "В дополнительной заявке отказано",
            "extraClaimReason",
            "shouldPollExtraClaimStatus",
            "/api/extra-claim/request",
            "/api/extra-claim/status",
        ):
            self.assertIn(expected, rendered)

    def test_status_refresh_never_flushes_historical_claim_events(self):
        self.add_taken_claim()
        request = self.store.create_extra_claim_request(
            "42",
            "2026-08-17",
            "Нужна ещё одна заявка для клиента",
            taken_today_snapshot=1,
            daily_limit_snapshot=1,
        )
        calls = []

        def fake_baza(path, payload, timeout=None):
            calls.append(path)
            if path.endswith("extra-claim-requests"):
                return {"request": {"id": "req-priority", "status": "pending"}}
            if path.endswith("grants/query"):
                return {
                    "request": {"id": "req-priority", "status": "pending"},
                    "grants": [],
                }
            raise AssertionError(f"unexpected synchronous path: {path}")

        with (
            patch.multiple(
                app,
                EXTRA_CLAIM_REQUESTS_ENABLED=True,
                BAZA_API_BASE_URL="https://baza.example.test",
                BAZA_HMAC_KEY_ID="picker-v1",
                BAZA_HMAC_SECRET="s" * 32,
            ),
            patch.object(app, "local_date", return_value="2026-08-17"),
            patch.object(app, "is_limit_bypassed_now", return_value=False),
            patch.object(app, "baza_post", side_effect=fake_baza),
        ):
            state = app.extra_claim_limit_state("42", refresh=True)

        self.assertEqual(
            calls,
            [
                "/integrations/deal-picker/v1/extra-claim-requests",
                "/integrations/deal-picker/v1/grants/query",
            ],
        )
        self.assertEqual(state["request"]["requestKey"], request["requestKey"])
        pending_claims = [
            item for item in self.store.list_outbox(delivered=False)
            if item["kind"] == "claim_event"
        ]
        self.assertEqual(len(pending_claims), 1)

    def test_active_request_exists_409_adopts_remote_pending_instead_of_rejecting(self):
        request = self.store.create_extra_claim_request(
            "42",
            "2026-08-17",
            "Нужна ещё одна заявка для клиента",
            taken_today_snapshot=1,
            daily_limit_snapshot=1,
        )
        conflict = app.BazaIntegrationHttpError(
            409,
            {
                "code": "active_request_exists",
                "request": {
                    "id": "remote-active",
                    "status": "pending",
                    "reason": "Запрос уже ожидает решения",
                },
            },
        )
        with (
            patch.multiple(
                app,
                EXTRA_CLAIM_REQUESTS_ENABLED=True,
                BAZA_API_BASE_URL="https://baza.example.test",
                BAZA_HMAC_KEY_ID="picker-v1",
                BAZA_HMAC_SECRET="s" * 32,
            ),
            patch.object(app, "baza_post", side_effect=conflict),
        ):
            summary = app.flush_integration_outbox(
                limit=1,
                kinds={"extra_claim_request"},
                dedupe_key=f"extra-claim-request:{request['requestKey']}",
            )
        state = self.store.get_extra_claim_state("42", "2026-08-17")
        self.assertEqual(summary["sent"], 1)
        self.assertEqual(state["request"]["status"], "pending")
        self.assertEqual(state["request"]["id"], "remote-active")
        outbox = self.store.list_outbox()[0]
        self.assertIsNotNone(outbox["deliveredAt"])
        self.assertIsNone(outbox["deadLetterAt"])

    def test_outage_then_old_409_reconciles_claim_before_creating_next_request(self):
        old_request = self.store.create_extra_claim_request(
            "42",
            "2026-08-17",
            "Нужна первая дополнительная заявка",
            taken_today_snapshot=1,
            daily_limit_snapshot=1,
        )
        self.store.apply_extra_claim_request_response(
            old_request["requestKey"],
            {"id": "remote-old", "status": "pending"},
        )
        for item in self.store.list_outbox(delivered=False):
            if item["kind"] == "extra_claim_request":
                self.store.mark_outbox_delivered(item["id"], {"ok": True})
        self.store.import_extra_claim_state(
            "42",
            "2026-08-17",
            {
                "request": {
                    "requestKey": old_request["requestKey"],
                    "id": "remote-old",
                    "status": "approved",
                },
                "grants": [
                    {
                        "id": "remote-old",
                        "requestId": "remote-old",
                        "status": "approved",
                        "bitrixUserId": "42",
                        "businessDate": "2026-08-17",
                    }
                ],
            },
        )
        self.store.begin_claim_operation(
            "deal-old",
            "42",
            operation_key="claim:old-extra",
            require_extra_grant=True,
            business_date="2026-08-17",
        )
        self.store.finalize_claim_operation(
            "claim:old-extra",
            claim={"timestamp": "2026-08-17T12:00:00+06:00"},
        )
        next_request = self.store.create_extra_claim_request(
            "42",
            "2026-08-17",
            "Нужна следующая заявка после использованной",
            taken_today_snapshot=2,
            daily_limit_snapshot=1,
        )

        outage_calls = []

        def outage(path, _payload, timeout=None):
            outage_calls.append(path)
            if path.endswith("claim-events"):
                raise TimeoutError("Baza is unavailable")
            raise app.BazaIntegrationHttpError(
                409,
                {
                    "code": "active_request_exists",
                    "request": {
                        "requestKey": old_request["requestKey"],
                        "id": "remote-old",
                        "status": "approved",
                        "bitrixUserId": "42",
                        "businessDate": "2026-08-17",
                    },
                },
            )

        with (
            patch.multiple(
                app,
                EXTRA_CLAIM_REQUESTS_ENABLED=True,
                BAZA_API_BASE_URL="https://baza.example.test",
                BAZA_HMAC_KEY_ID="picker-v1",
                BAZA_HMAC_SECRET="s" * 32,
            ),
            patch.object(app, "baza_post", side_effect=outage),
        ):
            outage_summary = app.flush_integration_outbox(limit=10)

        self.assertEqual(outage_summary["retried"], 2)
        self.assertEqual(
            outage_calls,
            [
                "/integrations/deal-picker/v1/claim-events",
                "/integrations/deal-picker/v1/extra-claim-requests",
            ],
        )
        with sqlite3.connect(self.store.db_path) as connection:
            statuses = dict(
                connection.execute(
                    "SELECT request_key, status FROM extra_claim_requests"
                ).fetchall()
            )
        self.assertEqual(statuses[old_request["requestKey"]], "consumed")
        self.assertEqual(statuses[next_request["requestKey"]], "queued")

        # Simulate the stale grant snapshot seen before Baza receives the
        # durable claim event.  It must not revive the consumed local record.
        self.store.import_extra_claim_state(
            "42",
            "2026-08-17",
            {
                "request": {
                    "requestKey": old_request["requestKey"],
                    "id": "remote-old",
                    "status": "approved",
                },
                "grants": [
                    {
                        "id": "remote-old",
                        "requestId": "remote-old",
                        "status": "approved",
                        "businessDate": "2026-08-17",
                    }
                ],
            },
        )
        with sqlite3.connect(self.store.db_path) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM extra_claim_requests "
                    "WHERE request_key=?",
                    (old_request["requestKey"],),
                ).fetchone()[0],
                "consumed",
            )
            connection.execute(
                "UPDATE integration_outbox SET next_attempt_at='2000-01-01T00:00:00+00:00' "
                "WHERE delivered_at IS NULL AND dead_letter_at IS NULL"
            )

        restored_calls = []
        old_remote_consumed = False

        def restored(path, _payload, timeout=None):
            nonlocal old_remote_consumed
            restored_calls.append(path)
            if path.endswith("claim-events"):
                old_remote_consumed = True
                return {"ok": True, "created": True}
            self.assertTrue(old_remote_consumed)
            return {
                "ok": True,
                "request": {
                    "requestKey": next_request["requestKey"],
                    "id": "remote-next",
                    "status": "pending",
                    "bitrixUserId": "42",
                    "businessDate": "2026-08-17",
                },
            }

        with (
            patch.multiple(
                app,
                EXTRA_CLAIM_REQUESTS_ENABLED=True,
                BAZA_API_BASE_URL="https://baza.example.test",
                BAZA_HMAC_KEY_ID="picker-v1",
                BAZA_HMAC_SECRET="s" * 32,
            ),
            patch.object(app, "baza_post", side_effect=restored),
        ):
            restored_summary = app.flush_integration_outbox(limit=10)

        self.assertEqual(restored_summary["sent"], 2)
        self.assertEqual(restored_summary["retried"], 0)
        self.assertEqual(
            restored_calls,
            [
                "/integrations/deal-picker/v1/claim-events",
                "/integrations/deal-picker/v1/extra-claim-requests",
            ],
        )
        self.assertEqual(self.store.list_outbox(delivered=False), [])
        state = self.store.get_extra_claim_state("42", "2026-08-17")
        self.assertEqual(state["request"]["requestKey"], next_request["requestKey"])
        self.assertEqual(state["request"]["id"], "remote-next")
        self.assertEqual(state["request"]["status"], "pending")

    def test_claim_event_integrity_409_is_dead_letter_not_false_duplicate(self):
        claim = self.store.append_claim(
            {
                "timestamp": "2026-08-17T12:00:00+06:00",
                "managerId": "42",
                "dealId": "integrity-conflict",
            }
        )
        conflict = app.BazaIntegrationHttpError(
            409,
            {
                "code": "idempotency_conflict",
                "eventUuid": claim["eventUuid"],
            },
        )
        with (
            patch.multiple(
                app,
                EXTRA_CLAIM_REQUESTS_ENABLED=True,
                BAZA_API_BASE_URL="https://baza.example.test",
                BAZA_HMAC_KEY_ID="picker-v1",
                BAZA_HMAC_SECRET="s" * 32,
            ),
            patch.object(app, "baza_post", side_effect=conflict),
        ):
            summary = app.flush_integration_outbox(limit=1, kinds={"claim_event"})

        self.assertEqual(summary["sent"], 0)
        self.assertEqual(summary["dead"], 1)
        outbox = self.store.list_outbox()[0]
        self.assertIsNone(outbox["deliveredAt"])
        self.assertIsNotNone(outbox["deadLetterAt"])

    def test_hmac_replay_409_retries_exact_durable_payload(self):
        self.store.append_claim(
            {
                "timestamp": "2026-08-17T12:00:00+06:00",
                "managerId": "42",
                "dealId": "replay",
            }
        )
        replay = app.BazaIntegrationHttpError(409, {"code": "replay_detected"})
        with (
            patch.multiple(
                app,
                EXTRA_CLAIM_REQUESTS_ENABLED=True,
                BAZA_API_BASE_URL="https://baza.example.test",
                BAZA_HMAC_KEY_ID="picker-v1",
                BAZA_HMAC_SECRET="s" * 32,
            ),
            patch.object(app, "baza_post", side_effect=replay),
        ):
            summary = app.flush_integration_outbox(limit=1, kinds={"claim_event"})

        self.assertEqual(summary["retried"], 1)
        outbox = self.store.list_outbox()[0]
        self.assertIsNone(outbox["deliveredAt"])
        self.assertIsNone(outbox["deadLetterAt"])
        self.assertEqual(outbox["attemptCount"], 1)


class TestLostDealAutoclose(TemporaryStateTestCase):
    REMOTE_TIME = "2099-01-01T10:01:00+00:00"

    @staticmethod
    def transition():
        return {
            "transitionId": "9001",
            "dealId": "7001",
            "fromSemantic": "P",
            "toSemantic": "F",
            "fromCategoryId": "0",
            "toCategoryId": "0",
            "fromStageId": "NEW",
            "toStageId": "LOSE",
            "transitionTime": "2099-01-01T10:00:01+00:00",
        }

    @staticmethod
    def snapshot(**changes):
        value = {
            "chatId": "8101",
            "sessionId": "8201",
            "lastMessageId": "8301",
            "historyMessageCount": 12,
            "latestMessageAt": "2099-01-01T09:59:50+00:00",
            "historySignature": "a" * 64,
            "activityId": "8401",
            "chatLookupMode": "active_registry",
            "activityUpdatedAt": "",
            "fallbackActivityCompleted": "",
            "fallbackActivityStatus": "",
            "fallbackActivityCreatedAt": "",
            "unreadId": "19",
            "counter": 1,
            "userCounter": 2,
            "isNew": False,
            "entityData1": "Y|DEAL|7001|N|N|8201|0|0|0|0",
            "entityData2": "LEAD|0|COMPANY|0|CONTACT|0|DEAL|7001",
        }
        value.update(changes)
        return value

    def arm(self, baseline=9000):
        return self.store.arm_lost_deal_autoclose(
            "2099-01-01T10:00:00+00:00", baseline
        )

    def test_first_poll_only_arms_remote_boundary_and_never_processes(self):
        payload = {
            "result": {"items": []},
            "time": {"date_finish": "2099-01-01T10:00:00+00:00"},
        }
        rows = [{"ID": "9000", "CREATED_TIME": "2099-01-01T09:59:59+00:00"}]
        with (
            patch.object(app, "bitrix_stagehistory_page", return_value=(payload, rows)),
            patch.object(app, "process_lost_deal_transition") as process,
        ):
            result = app.poll_lost_deal_autoclose_once()

        self.assertTrue(result["armed"])
        self.assertEqual(result["processed"], 0)
        process.assert_not_called()
        boundary = self.store.get_lost_deal_autoclose_boundary()
        self.assertEqual(boundary["baselineHistoryId"], 9000)
        self.assertEqual(boundary["scanAfterHistoryId"], 9000)

    def _exact_transition(self, previous_stage="NEW", closed="Y"):
        deal = {
            "ID": "7001",
            "CATEGORY_ID": "0",
            "STAGE_ID": "LOSE",
            "STAGE_SEMANTIC_ID": "F",
            "CLOSED": closed,
            "MOVED_TIME": "2099-01-01T10:00:00+00:00",
        }
        history = [
            {
                "id": "9001",
                "dealId": "7001",
                "categoryId": "0",
                "stageId": "LOSE",
                "createdAt": datetime.fromisoformat("2099-01-01T10:00:01+00:00"),
            },
            {
                "id": "9000",
                "dealId": "7001",
                "categoryId": "0",
                "stageId": previous_stage,
                "createdAt": datetime.fromisoformat("2099-01-01T09:00:00+00:00"),
            },
        ]

        def call_side_effect(method, params=None, timeout=None):
            if method == "crm.deal.get":
                return deal
            if method == "crm.status.list":
                return [
                    {"ENTITY_ID": "DEAL_STAGE", "STATUS_ID": "NEW", "SEMANTICS": ""},
                    {"ENTITY_ID": "DEAL_STAGE", "STATUS_ID": "WON", "SEMANTICS": "S"},
                    {"ENTITY_ID": "DEAL_STAGE", "STATUS_ID": "LOSE", "SEMANTICS": "F"},
                    {"ENTITY_ID": "DEAL_STAGE", "STATUS_ID": "LOSE_2", "SEMANTICS": "F"},
                ]
            raise AssertionError(method)

        return call_side_effect, history

    def test_exact_history_and_blank_status_semantics_prove_p_to_f(self):
        call_side_effect, history = self._exact_transition()
        with (
            patch.object(app, "bitrix_call", side_effect=call_side_effect),
            patch.object(app, "read_deal_stage_history", return_value=({}, history)),
        ):
            result = app.exact_failed_transition("7001", "9001")
        self.assertEqual(result["fromSemantic"], "P")
        self.assertEqual(result["toSemantic"], "F")

    def test_s_to_f_is_also_an_exact_close_candidate(self):
        call_side_effect, history = self._exact_transition(previous_stage="WON")
        with (
            patch.object(app, "bitrix_call", side_effect=call_side_effect),
            patch.object(app, "read_deal_stage_history", return_value=({}, history)),
        ):
            result = app.exact_failed_transition("7001", "9001")
        self.assertEqual(result["fromSemantic"], "S")
        self.assertEqual(result["toSemantic"], "F")

    def test_f_to_f_never_becomes_close_candidate(self):
        call_side_effect, history = self._exact_transition(previous_stage="LOSE_2")
        with (
            patch.object(app, "bitrix_call", side_effect=call_side_effect),
            patch.object(app, "read_deal_stage_history", return_value=({}, history)),
            patch.object(app, "read_single_active_deal_chat_snapshot") as chat_read,
        ):
            with self.assertRaisesRegex(app.LostDealCloseGuardError, "not_non_f_to_f"):
                app.process_lost_deal_transition(
                    "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
                )
        chat_read.assert_not_called()
        self.assertIsNone(self.store.get_lost_deal_close_operation("9001"))

    def test_failed_semantic_without_closed_flag_never_closes(self):
        call_side_effect, history = self._exact_transition(closed="N")
        with (
            patch.object(app, "bitrix_call", side_effect=call_side_effect),
            patch.object(app, "read_deal_stage_history", return_value=({}, history)),
            patch.object(app, "read_single_active_deal_chat_snapshot") as chat_read,
        ):
            with self.assertRaisesRegex(app.LostDealCloseGuardError, "deal_not_closed"):
                app.process_lost_deal_transition(
                    "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
                )
        chat_read.assert_not_called()

    def test_finish_is_idempotent_and_dispatch_identity_is_durable(self):
        snapshot = self.snapshot()

        def finish_side_effect(method, params):
            operation = self.store.get_lost_deal_close_operation("9001")
            self.assertEqual(operation["status"], "dispatching")
            self.assertEqual(operation["chatId"], "8101")
            self.assertEqual(operation["sessionId"], "8201")
            self.assertEqual(operation["activityId"], "8401")
            return True

        finish = MagicMock(side_effect=finish_side_effect)
        with (
            patch.object(app, "DRY_RUN", False),
            patch.object(app, "exact_failed_transition", return_value=self.transition()) as exact,
            patch.object(app, "read_single_active_deal_chat_snapshot", return_value=snapshot),
            patch.object(app, "selected_chat_is_confirmed_inactive", return_value=True),
            patch.object(app, "bitrix_call", finish),
        ):
            first = app.process_lost_deal_transition(
                "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
            )
            second = app.process_lost_deal_transition(
                "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
            )
        self.assertEqual(first, "closed")
        self.assertEqual(second, "closed")
        finish.assert_called_once_with(
            "imopenlines.operator.another.finish", {"CHAT_ID": "8101"}
        )
        operation = self.store.get_lost_deal_close_operation("9001")
        self.assertEqual(operation["status"], "closed")
        self.assertEqual(operation["sessionId"], "8201")
        self.assertEqual(operation["activityId"], "8401")
        self.assertEqual(exact.call_count, 2)

    def test_dry_run_never_calls_finish_and_is_terminal(self):
        finish = MagicMock()
        with (
            patch.object(app, "DRY_RUN", True),
            patch.object(app, "exact_failed_transition", return_value=self.transition()),
            patch.object(app, "read_single_active_deal_chat_snapshot", return_value=self.snapshot()),
            patch.object(app, "bitrix_call", finish),
        ):
            result = app.process_lost_deal_transition(
                "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
            )
        self.assertEqual(result, "dry_run")
        finish.assert_not_called()
        self.assertEqual(
            self.store.get_lost_deal_close_operation("9001")["status"], "dry_run"
        )

    def test_finish_timeout_is_uncertain_and_is_never_sent_again(self):
        finish = MagicMock(side_effect=TimeoutError("ambiguous"))
        with (
            patch.object(app, "DRY_RUN", False),
            patch.object(app, "exact_failed_transition", return_value=self.transition()),
            patch.object(app, "read_single_active_deal_chat_snapshot", return_value=self.snapshot()),
            patch.object(app, "selected_chat_is_confirmed_inactive", return_value=False),
            patch.object(app, "bitrix_call", finish),
        ):
            first = app.process_lost_deal_transition(
                "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
            )
            second = app.process_lost_deal_transition(
                "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
            )
        self.assertEqual(first, "uncertain")
        self.assertEqual(second, "uncertain")
        self.assertEqual(finish.call_count, 1)
        self.assertEqual(
            self.store.get_lost_deal_close_operation("9001")["status"], "uncertain"
        )

    def test_fallback_finish_persists_provenance_and_dispatches_once(self):
        fallback_snapshot = self.snapshot(
            chatLookupMode="activity_fallback",
            activityUpdatedAt=self.transition()["transitionTime"],
            fallbackActivityCompleted="Y",
            fallbackActivityStatus="3",
            fallbackActivityCreatedAt="2099-01-01T09:59:50+00:00",
        )

        def finish_side_effect(method, params):
            operation = self.store.get_lost_deal_close_operation("9001")
            self.assertEqual(operation["status"], "dispatching")
            self.assertEqual(operation["chatLookupMode"], "activity_fallback")
            self.assertEqual(
                operation["activityUpdatedAt"],
                "2099-01-01T10:00:01.000000+00:00",
            )
            return True

        with (
            patch.object(app, "DRY_RUN", False),
            patch.object(app, "exact_failed_transition", return_value=self.transition()),
            patch.object(
                app,
                "read_single_active_deal_chat_snapshot",
                return_value=fallback_snapshot,
            ),
            patch.object(app, "selected_chat_is_confirmed_inactive", return_value=True),
            patch.object(app, "bitrix_call", side_effect=finish_side_effect) as finish,
        ):
            first = app.process_lost_deal_transition(
                "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
            )
            second = app.process_lost_deal_transition(
                "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
            )

        self.assertEqual(first, "closed")
        self.assertEqual(second, "closed")
        finish.assert_called_once_with(
            "imopenlines.operator.another.finish", {"CHAT_ID": "8101"}
        )

    def test_fallback_timeout_is_never_reconciled_or_resent(self):
        fallback_snapshot = self.snapshot(
            chatLookupMode="activity_fallback",
            activityUpdatedAt=self.transition()["transitionTime"],
            fallbackActivityCompleted="Y",
            fallbackActivityStatus="3",
            fallbackActivityCreatedAt="2099-01-01T09:59:50+00:00",
        )
        selected = MagicMock(return_value=True)
        finish = MagicMock(side_effect=TimeoutError("ambiguous"))
        with (
            patch.object(app, "DRY_RUN", False),
            patch.object(app, "exact_failed_transition", return_value=self.transition()),
            patch.object(
                app,
                "read_single_active_deal_chat_snapshot",
                return_value=fallback_snapshot,
            ),
            patch.object(app, "selected_chat_is_confirmed_inactive", selected),
            patch.object(app, "bitrix_call", finish),
        ):
            first = app.process_lost_deal_transition(
                "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
            )
            second = app.process_lost_deal_transition(
                "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
            )
            reconciled = app.reconcile_lost_deal_close_operations()

        self.assertEqual(first, "uncertain")
        self.assertEqual(second, "uncertain")
        self.assertEqual(reconciled, 0)
        self.assertEqual(finish.call_count, 1)
        selected.assert_not_called()
        operation = self.store.get_lost_deal_close_operation("9001")
        self.assertEqual(operation["status"], "uncertain")
        self.assertEqual(operation["chatLookupMode"], "activity_fallback")

    def test_fallback_activity_timestamp_change_aborts_before_dispatch(self):
        first = self.snapshot(
            chatLookupMode="activity_fallback",
            activityUpdatedAt="2099-01-01T10:00:01+00:00",
            fallbackActivityCompleted="Y",
            fallbackActivityStatus="3",
            fallbackActivityCreatedAt="2099-01-01T09:59:50+00:00",
        )
        changed = {**first, "activityUpdatedAt": "2099-01-01T10:00:02+00:00"}
        finish = MagicMock()
        with (
            patch.object(app, "DRY_RUN", False),
            patch.object(app, "exact_failed_transition", return_value=self.transition()),
            patch.object(
                app,
                "read_single_active_deal_chat_snapshot",
                side_effect=[first, changed],
            ),
            patch.object(app, "bitrix_call", finish),
        ):
            result = app.process_lost_deal_transition(
                "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
            )

        self.assertEqual(result, "chat_changed_before_finish")
        finish.assert_not_called()

    def test_final_chat_change_aborts_before_dispatch(self):
        first = self.snapshot()
        changed = self.snapshot(lastMessageId="8302", historySignature="b" * 64)
        finish = MagicMock()
        with (
            patch.object(app, "DRY_RUN", False),
            patch.object(app, "exact_failed_transition", return_value=self.transition()),
            patch.object(
                app,
                "read_single_active_deal_chat_snapshot",
                side_effect=[first, changed],
            ),
            patch.object(app, "bitrix_call", finish),
        ):
            result = app.process_lost_deal_transition(
                "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
            )
        self.assertEqual(result, "chat_changed_before_finish")
        finish.assert_not_called()
        self.assertEqual(
            self.store.get_lost_deal_close_operation("9001")["status"], "skipped"
        )

    def test_multiple_active_chats_fail_closed_before_dialog_read(self):
        with patch.object(
            app,
            "bitrix_call",
            return_value=[{"CHAT_ID": "8101"}, {"CHAT_ID": "8102"}],
        ) as call_method:
            with self.assertRaisesRegex(
                app.LostDealCloseGuardError, "multiple_active_chats"
            ):
                app.read_single_active_deal_chat_snapshot("7001")
        call_method.assert_called_once()

    def test_automatically_completed_activity_is_valid_preflight_binding(self):
        automatically_completed = {
            "ID": "8401",
            "OWNER_TYPE_ID": "2",
            "OWNER_ID": "7001",
            "PROVIDER_ID": "IMOPENLINES_SESSION",
            "ASSOCIATED_ENTITY_ID": "8201",
            "COMPLETED": "Y",
            "STATUS": "3",
        }
        with patch.object(
            app,
            "bitrix_call_full",
            return_value={"result": [automatically_completed], "total": 1},
        ) as activity_list:
            result = app.exact_openline_activity("7001", "8201")

        self.assertEqual(result, "8401")
        params = activity_list.call_args.args[1]
        self.assertNotIn("filter[COMPLETED]", params)

    def test_unexpected_activity_completion_pair_fails_closed(self):
        inconsistent = {
            "ID": "8401",
            "OWNER_TYPE_ID": "2",
            "OWNER_ID": "7001",
            "PROVIDER_ID": "IMOPENLINES_SESSION",
            "ASSOCIATED_ENTITY_ID": "8201",
            "COMPLETED": "N",
            "STATUS": "3",
        }
        with patch.object(
            app,
            "bitrix_call_full",
            return_value={"result": [inconsistent], "total": 1},
        ):
            with self.assertRaisesRegex(
                app.LostDealCloseGuardError, "session_activity_binding_mismatch"
            ):
                app.exact_openline_activity("7001", "8201")

    def test_activity_auto_completion_between_snapshots_still_dispatches_once(self):
        pending = {
            "ID": "8401",
            "OWNER_TYPE_ID": "2",
            "OWNER_ID": "7001",
            "PROVIDER_ID": "IMOPENLINES_SESSION",
            "ASSOCIATED_ENTITY_ID": "8201",
            "COMPLETED": "N",
            "STATUS": "1",
        }
        automatically_completed = {
            **pending,
            "COMPLETED": "Y",
            "STATUS": "3",
        }
        activity_payloads = iter(
            [
                {"result": [pending], "total": 1},
                {"result": [automatically_completed], "total": 1},
            ]
        )

        def read_snapshot(_deal_id, _transition=None):
            activity_id = app.exact_openline_activity("7001", "8201")
            return self.snapshot(activityId=activity_id)

        with (
            patch.object(app, "DRY_RUN", False),
            patch.object(app, "exact_failed_transition", return_value=self.transition()),
            patch.object(app, "bitrix_call_full", side_effect=lambda *args: next(activity_payloads)),
            patch.object(app, "read_single_active_deal_chat_snapshot", side_effect=read_snapshot),
            patch.object(app, "selected_chat_is_confirmed_inactive", return_value=True),
            patch.object(app, "bitrix_call", return_value=True) as finish,
        ):
            result = app.process_lost_deal_transition(
                "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
            )

        self.assertEqual(result, "closed")
        finish.assert_called_once_with(
            "imopenlines.operator.another.finish", {"CHAT_ID": "8101"}
        )

    def test_message_after_failed_transition_is_terminal_skip(self):
        late = self.snapshot(latestMessageAt="2099-01-01T10:00:02+00:00")
        with (
            patch.object(app, "DRY_RUN", False),
            patch.object(app, "exact_failed_transition", return_value=self.transition()),
            patch.object(app, "read_single_active_deal_chat_snapshot", return_value=late),
            patch.object(app, "bitrix_call") as finish,
        ):
            result = app.process_lost_deal_transition(
                "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
            )
        self.assertEqual(result, "message_after_failed_transition")
        finish.assert_not_called()

    def test_message_in_same_second_as_failed_transition_is_terminal_skip(self):
        same_second = self.snapshot(
            latestMessageAt=self.transition()["transitionTime"]
        )
        with (
            patch.object(app, "DRY_RUN", False),
            patch.object(app, "exact_failed_transition", return_value=self.transition()),
            patch.object(
                app,
                "read_single_active_deal_chat_snapshot",
                return_value=same_second,
            ),
            patch.object(app, "bitrix_call") as finish,
        ):
            result = app.process_lost_deal_transition(
                "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
            )
        self.assertEqual(result, "message_after_failed_transition")
        finish.assert_not_called()

    def test_grace_period_keeps_transition_undiscovered_for_next_poll(self):
        with (
            patch.object(app, "exact_failed_transition", return_value=self.transition()),
            patch.object(app, "read_single_active_deal_chat_snapshot") as chat_read,
        ):
            with self.assertRaises(app.LostDealTransitionNotMature):
                app.process_lost_deal_transition(
                    "7001",
                    "9001",
                    datetime.fromisoformat("2099-01-01T10:00:10+00:00"),
                )
        chat_read.assert_not_called()
        self.assertIsNone(self.store.get_lost_deal_close_operation("9001"))

    def test_permanent_preflight_error_exhausts_without_dispatch(self):
        with (
            patch.object(app, "DRY_RUN", False),
            patch.object(app, "LOST_DEAL_MAX_PREFLIGHT_ATTEMPTS", 3),
            patch.object(app, "exact_failed_transition", return_value=self.transition()),
            patch.object(
                app,
                "read_single_active_deal_chat_snapshot",
                side_effect=RuntimeError("permanent"),
            ),
            patch.object(app, "bitrix_call") as finish,
        ):
            for _ in range(2):
                with self.assertRaises(RuntimeError):
                    app.process_lost_deal_transition(
                        "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
                    )
            result = app.process_lost_deal_transition(
                "7001", "9001", datetime.fromisoformat(self.REMOTE_TIME)
            )
        self.assertEqual(result, "pre_dispatch_attempts_exhausted")
        finish.assert_not_called()
        operation = self.store.get_lost_deal_close_operation("9001")
        self.assertEqual(operation["status"], "skipped")
        self.assertEqual(operation["attemptCount"], 3)

    def test_post_confirm_requires_exact_completed_activity_and_no_reverse_active_row(self):
        operation = {
            "dealId": "7001",
            "chatId": "8101",
            "sessionId": "8201",
            "activityId": "8401",
        }
        completed = {
            "ID": "8401",
            "OWNER_TYPE_ID": "2",
            "OWNER_ID": "7001",
            "PROVIDER_ID": "IMOPENLINES_SESSION",
            "ASSOCIATED_ENTITY_ID": "8201",
            "COMPLETED": "Y",
            "STATUS": "2",
        }
        closed_dialog = {
            "id": "8101",
            "type": "lines",
            "entity_data_1": "Y|DEAL|7001|N|N|0|0|0|0|0",
            "entity_data_2": "LEAD|0|COMPANY|0|CONTACT|0|DEAL|7001",
        }

        def completed_call(method, params=None, timeout=None):
            if method == "imopenlines.dialog.get":
                return closed_dialog
            if method == "crm.activity.get":
                return completed
            raise AssertionError(method)

        with (
            patch.object(app, "active_chat_rows_for_deal", return_value=[]),
            patch.object(
                app,
                "_history_identity",
                return_value={"chatId": "8101", "sessionId": "8201"},
            ),
            patch.object(app, "bitrix_call", side_effect=completed_call),
            patch.object(app, "openline_session_activities", return_value=[completed]),
        ):
            self.assertTrue(app.selected_chat_is_confirmed_inactive(operation))
        with (
            patch.object(app, "active_chat_rows_for_deal", return_value=[]),
            patch.object(
                app,
                "_history_identity",
                return_value={"chatId": "8101", "sessionId": "8201"},
            ),
            patch.object(app, "bitrix_call", return_value={**completed, "COMPLETED": "N"}),
        ):
            self.assertFalse(app.selected_chat_is_confirmed_inactive(operation))

    def test_post_confirm_accepts_automatically_completed_activity(self):
        operation = {
            "dealId": "7001",
            "chatId": "8101",
            "sessionId": "8201",
            "activityId": "8401",
        }
        automatically_completed = {
            "ID": "8401",
            "OWNER_TYPE_ID": "2",
            "OWNER_ID": "7001",
            "PROVIDER_ID": "IMOPENLINES_SESSION",
            "ASSOCIATED_ENTITY_ID": "8201",
            "COMPLETED": "Y",
            "STATUS": "3",
        }
        closed_dialog = {
            "id": "8101",
            "type": "lines",
            "entity_data_1": "Y|DEAL|7001|N|N|0|0|0|0|0",
            "entity_data_2": "LEAD|0|COMPANY|0|CONTACT|0|DEAL|7001",
        }

        def automatically_completed_call(method, params=None, timeout=None):
            if method == "imopenlines.dialog.get":
                return closed_dialog
            if method == "crm.activity.get":
                return automatically_completed
            raise AssertionError(method)

        with (
            patch.object(app, "active_chat_rows_for_deal", return_value=[]),
            patch.object(
                app,
                "_history_identity",
                return_value={"chatId": "8101", "sessionId": "8201"},
            ),
            patch.object(
                app,
                "bitrix_call",
                side_effect=automatically_completed_call,
            ),
            patch.object(
                app,
                "openline_session_activities",
                return_value=[automatically_completed],
            ),
        ):
            self.assertTrue(app.selected_chat_is_confirmed_inactive(operation))

    def test_post_confirm_rejects_still_embedded_fallback_session(self):
        operation = {
            "dealId": "7001",
            "chatId": "8101",
            "sessionId": "8201",
            "activityId": "8401",
        }
        still_open = {
            "id": "8101",
            "type": "lines",
            "entity_data_1": "Y|DEAL|7001|N|N|8201|0|0|0|0",
            "entity_data_2": "LEAD|0|COMPANY|0|CONTACT|0|DEAL|7001",
        }
        with (
            patch.object(app, "active_chat_rows_for_deal", return_value=[]),
            patch.object(
                app,
                "_history_identity",
                return_value={"chatId": "8101", "sessionId": "8201"},
            ),
            patch.object(app, "bitrix_call", return_value=still_open),
        ):
            self.assertFalse(app.selected_chat_is_confirmed_inactive(operation))

    def test_post_confirm_rejects_rebound_newer_session(self):
        operation = {
            "dealId": "7001",
            "chatId": "8101",
            "sessionId": "8201",
            "activityId": "8401",
        }
        rebound = {
            "id": "8101",
            "type": "lines",
            "entity_data_1": "Y|DEAL|7001|N|N|8202|0|0|0|0",
            "entity_data_2": "LEAD|0|COMPANY|0|CONTACT|0|DEAL|7001",
        }
        with (
            patch.object(app, "active_chat_rows_for_deal", return_value=[]),
            patch.object(
                app,
                "_history_identity",
                return_value={"chatId": "8101", "sessionId": "8201"},
            ),
            patch.object(app, "bitrix_call", return_value=rebound),
        ):
            self.assertFalse(app.selected_chat_is_confirmed_inactive(operation))

    def test_post_confirm_rejects_malformed_closed_session_marker(self):
        operation = {
            "dealId": "7001",
            "chatId": "8101",
            "sessionId": "8201",
            "activityId": "8401",
        }
        for malformed in (
            "",
            "Y|DEAL|7001",
            "N|DEAL|7001|N|N|0|0|0|0|0",
            "Y|CONTACT|7001|N|N|0|0|0|0|0",
            "Y|DEAL|7002|N|N|0|0|0|0|0",
        ):
            dialog = {
                "id": "8101",
                "type": "lines",
                "entity_data_1": malformed,
                "entity_data_2": "LEAD|0|COMPANY|0|CONTACT|0|DEAL|7001",
            }
            with self.subTest(entity_data_1=malformed):
                with (
                    patch.object(app, "active_chat_rows_for_deal", return_value=[]),
                    patch.object(
                        app,
                        "_history_identity",
                        return_value={"chatId": "8101", "sessionId": "8201"},
                    ),
                    patch.object(app, "bitrix_call", return_value=dialog),
                ):
                    self.assertFalse(
                        app.selected_chat_is_confirmed_inactive(operation)
                    )

    def test_auto_completed_activity_recovers_exact_missing_registry_chat(self):
        activity = {
            "ID": "8401",
            "OWNER_TYPE_ID": "2",
            "OWNER_ID": "7001",
            "PROVIDER_ID": "IMOPENLINES_SESSION",
            "ASSOCIATED_ENTITY_ID": "8201",
            "COMPLETED": "Y",
            "STATUS": "3",
            "CREATED": "2099-01-01T09:59:50+00:00",
            "LAST_UPDATED": self.transition()["transitionTime"],
        }
        history = {
            "chatId": "8101",
            "sessionId": "8201",
        }
        with (
            patch.object(
                app,
                "bitrix_call_full",
                return_value={"result": [activity], "total": 1},
            ) as activity_list,
            patch.object(app, "_history_identity", return_value=history),
        ):
            result = app.auto_completed_chat_candidate("7001", self.transition())

        self.assertEqual(result["chatId"], "8101")
        self.assertEqual(result["sessionId"], "8201")
        self.assertEqual(result["activityId"], "8401")
        params = activity_list.call_args.args[1]
        self.assertEqual(params["filter[OWNER_ID]"], "7001")
        self.assertNotIn("CONTACT_ID", json.dumps(params))
        self.assertNotIn("PHONE", json.dumps(params))

    def test_stale_auto_completed_activity_cannot_recover_chat(self):
        activity = {
            "ID": "8401",
            "OWNER_TYPE_ID": "2",
            "OWNER_ID": "7001",
            "PROVIDER_ID": "IMOPENLINES_SESSION",
            "ASSOCIATED_ENTITY_ID": "8201",
            "COMPLETED": "Y",
            "STATUS": "3",
            "CREATED": "2099-01-01T09:00:00+00:00",
            "LAST_UPDATED": "2099-01-01T09:30:00+00:00",
        }
        with patch.object(
            app,
            "bitrix_call_full",
            return_value={"result": [activity], "total": 1},
        ):
            with self.assertRaisesRegex(app.LostDealCloseGuardError, "no_active_chat"):
                app.auto_completed_chat_candidate("7001", self.transition())

    def test_multiple_auto_completed_candidates_fail_closed(self):
        first = {
            "ID": "8401",
            "OWNER_TYPE_ID": "2",
            "OWNER_ID": "7001",
            "PROVIDER_ID": "IMOPENLINES_SESSION",
            "ASSOCIATED_ENTITY_ID": "8201",
            "COMPLETED": "Y",
            "STATUS": "3",
            "CREATED": "2099-01-01T09:59:40+00:00",
            "LAST_UPDATED": self.transition()["transitionTime"],
        }
        second = {**first, "ID": "8402", "ASSOCIATED_ENTITY_ID": "8202"}

        def history(_parameter, session_id):
            return {"chatId": "8101" if session_id == "8201" else "8102", "sessionId": session_id}

        with (
            patch.object(
                app,
                "bitrix_call_full",
                return_value={"result": [first, second], "total": 2},
            ),
            patch.object(app, "_history_identity", side_effect=history),
        ):
            with self.assertRaisesRegex(
                app.LostDealCloseGuardError, "fallback_activity_not_unique"
            ):
                app.auto_completed_chat_candidate("7001", self.transition())

    def test_second_pending_activity_makes_fallback_ambiguous(self):
        candidate = {
            "ID": "8401",
            "OWNER_TYPE_ID": "2",
            "OWNER_ID": "7001",
            "PROVIDER_ID": "IMOPENLINES_SESSION",
            "ASSOCIATED_ENTITY_ID": "8201",
            "COMPLETED": "Y",
            "STATUS": "3",
            "CREATED": "2099-01-01T09:59:40+00:00",
            "LAST_UPDATED": self.transition()["transitionTime"],
        }
        pending = {
            **candidate,
            "ID": "8402",
            "ASSOCIATED_ENTITY_ID": "8202",
            "COMPLETED": "N",
            "STATUS": "1",
            "CREATED": "2099-01-01T09:00:00+00:00",
            "LAST_UPDATED": "2099-01-01T09:00:01+00:00",
        }
        with (
            patch.object(
                app,
                "bitrix_call_full",
                return_value={"result": [candidate, pending], "total": 2},
            ),
            patch.object(
                app,
                "_history_identity",
                return_value={"chatId": "8101", "sessionId": "8201"},
            ),
        ):
            with self.assertRaisesRegex(
                app.LostDealCloseGuardError, "fallback_activity_ambiguous"
            ):
                app.auto_completed_chat_candidate("7001", self.transition())

    def test_second_concurrent_completed_activity_makes_fallback_ambiguous(self):
        candidate = {
            "ID": "8401",
            "OWNER_TYPE_ID": "2",
            "OWNER_ID": "7001",
            "PROVIDER_ID": "IMOPENLINES_SESSION",
            "ASSOCIATED_ENTITY_ID": "8201",
            "COMPLETED": "Y",
            "STATUS": "3",
            "CREATED": "2099-01-01T09:59:40+00:00",
            "LAST_UPDATED": self.transition()["transitionTime"],
        }
        concurrent = {
            **candidate,
            "ID": "8402",
            "ASSOCIATED_ENTITY_ID": "8202",
            "STATUS": "2",
            "CREATED": "2099-01-01T09:00:00+00:00",
            "LAST_UPDATED": "2099-01-01T09:59:50+00:00",
        }
        with (
            patch.object(
                app,
                "bitrix_call_full",
                return_value={"result": [candidate, concurrent], "total": 2},
            ),
            patch.object(
                app,
                "_history_identity",
                return_value={"chatId": "8101", "sessionId": "8201"},
            ),
        ):
            with self.assertRaisesRegex(
                app.LostDealCloseGuardError, "fallback_activity_ambiguous"
            ):
                app.auto_completed_chat_candidate("7001", self.transition())

    def test_activity_registry_partial_pages_fail_closed(self):
        partial = {"result": [{"ID": "8401"}], "total": 2, "next": 50}
        with patch.object(app, "bitrix_call_full", return_value=partial):
            with self.assertRaisesRegex(RuntimeError, "неполный реестр чатов"):
                app.deal_openline_activities("7001")
            with self.assertRaisesRegex(RuntimeError, "неполный реестр активной"):
                app.openline_session_activities("8201")

    def test_fallback_snapshot_requires_current_embedded_session_and_exact_binding(self):
        fallback = {
            "activityId": "8401",
            "sessionId": "8201",
            "chatId": "8101",
            "activityCompleted": "Y",
            "activityStatus": "3",
            "activityCreatedAt": "2099-01-01T09:59:50+00:00",
            "activityUpdatedAt": self.transition()["transitionTime"],
        }
        identity = {
            "chatId": "8101",
            "sessionId": "8201",
            "messageCount": 1,
            "lastMessageId": "8301",
            "latestMessageAt": datetime.fromisoformat("2099-01-01T09:59:50+00:00"),
            "historySignature": "a" * 64,
        }
        dialog = {
            "id": "8101",
            "type": "lines",
            "entity_data_1": "Y|DEAL|7001|N|N|8201|0|0|0|0",
            "entity_data_2": "LEAD|0|COMPANY|0|CONTACT|0|DEAL|7001",
            "last_message_id": "8301",
            "unread_id": "0",
            "counter": 0,
            "user_counter": 1,
            "is_new": False,
        }
        with (
            patch.object(app, "active_chat_rows_for_deal", side_effect=[[], []]),
            patch.object(
                app,
                "auto_completed_chat_candidate",
                side_effect=[fallback, fallback],
            ),
            patch.object(app, "bitrix_call", return_value=dialog),
            patch.object(app, "_history_identity", return_value=identity),
            patch.object(app, "exact_openline_activity", return_value="8401"),
        ):
            snapshot = app.read_single_active_deal_chat_snapshot(
                "7001", self.transition()
            )
        self.assertEqual(snapshot["chatId"], "8101")
        self.assertEqual(snapshot["sessionId"], "8201")
        self.assertEqual(snapshot["activityId"], "8401")

    def test_transient_preflight_failure_does_not_advance_discovery_cursor(self):
        self.arm()
        payload = {
            "result": {"items": []},
            "time": {
                "date_start": self.REMOTE_TIME,
                "date_finish": self.REMOTE_TIME,
            },
        }
        rows = [
            {
                "ID": "9001",
                "OWNER_ID": "7001",
                "CATEGORY_ID": "0",
                "STAGE_ID": "LOSE",
                "CREATED_TIME": "2099-01-01T10:00:01+00:00",
            }
        ]
        process = MagicMock(side_effect=[RuntimeError("temporary"), "dry_run"])
        with (
            patch.object(app, "bitrix_stagehistory_page", return_value=(payload, rows)),
            patch.object(app, "semantic_for_stage", return_value="F"),
            patch.object(app, "process_lost_deal_transition", process),
        ):
            first = app.poll_lost_deal_autoclose_once()
            self.assertFalse(first["scanAdvanced"])
            self.assertEqual(
                self.store.get_lost_deal_autoclose_boundary()["scanAfterHistoryId"],
                9000,
            )
            second = app.poll_lost_deal_autoclose_once()
        self.assertTrue(second["scanAdvanced"])
        self.assertEqual(
            self.store.get_lost_deal_autoclose_boundary()["scanAfterHistoryId"],
            9001,
        )

    def test_in_progress_claim_prevents_cursor_advance_after_worker_crash(self):
        self.arm()
        claim = self.store.claim_lost_deal_close_transition(self.transition())
        self.assertTrue(claim["claimed"])
        payload = {
            "result": {"items": []},
            "time": {"date_finish": self.REMOTE_TIME},
        }
        rows = [{"ID": "9001", "OWNER_ID": "7001", "CATEGORY_ID": "0", "STAGE_ID": "LOSE"}]
        with (
            patch.object(app, "bitrix_stagehistory_page", return_value=(payload, rows)),
            patch.object(app, "semantic_for_stage", return_value="F"),
            patch.object(app, "exact_failed_transition", return_value=self.transition()),
        ):
            result = app.poll_lost_deal_autoclose_once()
        self.assertFalse(result["scanAdvanced"])
        self.assertEqual(
            self.store.get_lost_deal_autoclose_boundary()["scanAfterHistoryId"],
            9000,
        )

    def test_main_respects_autoclose_kill_switch(self):
        with (
            patch.object(app.sys, "argv", ["app.py"]),
            patch.object(app, "BoundedThreadingHTTPServer"),
            patch.object(app.threading, "Thread") as thread,
            patch.object(app, "LOST_DEAL_AUTOCLOSE_ENABLED", False),
        ):
            app.main()
        names = [item.kwargs.get("name") for item in thread.call_args_list]
        self.assertNotIn("lost-deal-autoclose", names)

        with (
            patch.object(app.sys, "argv", ["app.py"]),
            patch.object(app, "BoundedThreadingHTTPServer"),
            patch.object(app.threading, "Thread") as thread,
            patch.object(app, "LOST_DEAL_AUTOCLOSE_ENABLED", True),
        ):
            app.main()
        names = [item.kwargs.get("name") for item in thread.call_args_list]
        self.assertIn("lost-deal-autoclose", names)


if __name__ == "__main__":
    unittest.main(verbosity=2)
