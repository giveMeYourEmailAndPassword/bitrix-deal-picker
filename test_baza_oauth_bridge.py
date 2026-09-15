"""OAuth callback bridge regressions; no sockets, live tokens or persistent state."""

import json
import unittest
import urllib.error
from email.message import Message
from io import BytesIO, StringIO
from unittest.mock import MagicMock, patch

from test_app import HandlerHarness, app


STATE = "baza_" + "a" * 43
CODE = "synthetic-code-must-not-be-reflected"
CALLBACK = app.BAZA_OAUTH_APPROVED_CALLBACK_URL


def response(body=b'{"ok":true}', status=200, content_type="application/json"):
    result = MagicMock()
    result.__enter__.return_value = result
    result.getcode.return_value = status
    result.headers = {"Content-Type": content_type}
    result.read.return_value = body
    return result


class TestBazaOAuthBridge(unittest.TestCase):
    def setUp(self):
        self.config = patch.object(app, "BAZA_OAUTH_CALLBACK_URL", CALLBACK)
        self.config.start()
        self.addCleanup(self.config.stop)
        self.rate_limit = patch.object(app, "rate_limit_allowed", return_value=True)
        self.rate_limit.start()
        self.addCleanup(self.rate_limit.stop)
        # Fail before any accidental network I/O, including the new opener.
        self.network = patch.object(app.urllib.request.OpenerDirector, "open", side_effect=AssertionError("network forbidden"))
        self.network.start()
        self.addCleanup(self.network.stop)

    def handler(self, query=None):
        query = query if query is not None else app.urllib.parse.urlencode({"code": CODE, "state": STATE})
        return HandlerHarness.make("GET", "/?" + query)

    def test_callback_forwards_only_code_and_state_once_and_scrubs_browser_result(self):
        handler = self.handler(app.urllib.parse.urlencode({
            "code": CODE, "state": STATE, "domain": "untrusted.invalid",
            "member_id": "untrusted", "server_domain": "untrusted.invalid",
            "return_url": "https://untrusted.invalid", "access_token": "must-not-forward",
        }))
        backend = response()
        with patch.object(app.urllib.request.OpenerDirector, "open", return_value=backend) as send:
            handler.do_GET()
        send.assert_called_once()
        request = send.call_args.args[0]
        self.assertEqual(request.full_url, CALLBACK)
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(json.loads(request.data), {"code": CODE, "state": STATE})
        self.assertEqual(request.get_header("Content-type"), "application/json")
        self.assertNotIn("Authorization", request.headers)
        self.assertLessEqual(send.call_args.kwargs["timeout"], 20)
        self.assertEqual(backend.read.call_args.args, (4097,))
        self.assertEqual(HandlerHarness.status(handler), 303)
        body = handler.wfile.getvalue().decode()
        self.assertEqual(body, "")
        headers = dict(HandlerHarness.headers(handler))
        self.assertEqual(headers["Location"], "https://baza.krugo.tours/chats")
        self.assertEqual(headers["Content-Length"], "0")
        for secret in (CODE, STATE, "must-not-forward", "untrusted.invalid"):
            self.assertNotIn(secret, body + json.dumps(headers))
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertEqual(headers["Referrer-Policy"], "no-referrer")
        self.assertIn("default-src 'none'", headers["Content-Security-Policy"])

    def test_disabled_or_non_exact_callback_url_never_forwards(self):
        invalid = (
            "", "http://" + CALLBACK.removeprefix("https://"), CALLBACK + "/", CALLBACK + "?next=1",
            CALLBACK + "#fragment", CALLBACK.replace("https://", "https://user:password@"),
            CALLBACK.replace(".up.railway.app/", ".up.railway.app.evil.invalid/"),
            CALLBACK.replace("/integrations/", ":443/integrations/"),
        )
        for destination in invalid:
            with self.subTest(destination=destination), patch.object(app, "BAZA_OAUTH_CALLBACK_URL", destination), patch.object(app.urllib.request, "build_opener") as opener:
                handler = self.handler()
                handler.do_GET()
                self.assertEqual(HandlerHarness.status(handler), 503)
                self.assertNotIn("Location", dict(HandlerHarness.headers(handler)))
                opener.assert_not_called()

    def test_malformed_or_duplicate_baza_credentials_do_not_reach_backend(self):
        queries = (
            f"state={STATE}", f"state={STATE}&code=", f"state=baza_short&code={CODE}",
            f"state={STATE}&state={STATE}&code={CODE}",
            f"state=other&state={STATE}&code={CODE}",
            f"state={STATE}&code={CODE}&code=other", f"state={STATE}&code=%0Asecret",
            app.urllib.parse.urlencode({"state": STATE, "code": "x" * 2049}),
        )
        for query in queries:
            with self.subTest(query=query), patch.object(app.urllib.request, "build_opener") as opener:
                handler = self.handler(query)
                handler.do_GET()
                self.assertEqual(HandlerHarness.status(handler), 400)
                self.assertNotIn("Location", dict(HandlerHarness.headers(handler)))
                opener.assert_not_called()

    def test_ordinary_picker_get_and_install_post_are_preserved(self):
        for query in ("", f"code={CODE}&state=another-app", "AUTH_ID=ordinary-token&DOMAIN=test-fake.bitrix24.test"):
            with self.subTest(query=query), patch.object(app, "forward_baza_oauth_callback") as forward:
                handler = self.handler(query)
                handler.send_html = MagicMock()
                handler.do_GET()
                handler.send_html.assert_called_once_with(False, {})
                forward.assert_not_called()
        handler = HandlerHarness.make("POST", "/install", b"AUTH_ID=ordinary-token&DOMAIN=test-fake.bitrix24.test")
        handler.send_html = MagicMock()
        with patch.object(app, "forward_baza_oauth_callback") as forward:
            handler.do_POST()
        self.assertTrue(handler.send_html.call_args.args[0])
        self.assertEqual(handler.send_html.call_args.args[1]["AUTH_ID"], "ordinary-token")
        forward.assert_not_called()

    def test_uncertain_failed_or_unexpected_ack_never_retries_or_reflects_backend(self):
        outcomes = (
            TimeoutError(CODE + STATE),
            response(json.dumps({"ok": False, "error": CODE + STATE}).encode(), 400),
            response(b'{"ok":true}', 201), response(b'{"ok":true}', 302),
            response(b'{"ok":true}', content_type="text/html"),
            response(b'{"ok":1}'), response(b'{"ok":true,"token":"must-not-reflect"}'),
            response(b"malformed"), response(b"x" * 4097),
        )
        for outcome in outcomes:
            with self.subTest(outcome=type(outcome).__name__):
                handler = self.handler()
                options = {"side_effect": outcome} if isinstance(outcome, Exception) else {"return_value": outcome}
                with patch.object(app.urllib.request.OpenerDirector, "open", **options) as send, patch.object(app.sys, "stderr", new_callable=StringIO) as log:
                    handler.do_GET()
                    handler.log_message("untrusted %s", CODE + STATE)
                send.assert_called_once()
                self.assertEqual(HandlerHarness.status(handler), 502)
                self.assertNotIn("Location", dict(HandlerHarness.headers(handler)))
                body = handler.wfile.getvalue().decode()
                self.assertIn("Проверьте подключение", body)
                for secret in (CODE, STATE, "must-not-reflect"):
                    self.assertNotIn(secret, body)
                    self.assertNotIn(secret, log.getvalue())
                self.assertEqual(log.getvalue(), "GET /\n")

    def test_redirect_handler_rejects_every_redirect_without_a_second_request(self):
        redirect = app.BazaOAuthNoRedirect()
        app.urllib.request.build_opener(redirect)
        request = app.urllib.request.Request(CALLBACK, data=b"synthetic", method="POST")
        headers = Message()
        headers["location"] = "https://untrusted.invalid/collect"
        for status in (301, 302, 303, 307, 308):
            with self.subTest(status=status), self.assertRaises(urllib.error.HTTPError) as caught:
                redirect.parent.error("http", request, BytesIO(), status, "Redirect", headers)
            caught.exception.close()
        self.network.target.open.assert_not_called()
        with patch.object(app.urllib.request, "build_opener") as build:
            app.forward_baza_oauth_callback({"code": CODE, "state": STATE})
            self.assertIsInstance(build.call_args.args[0], app.BazaOAuthNoRedirect)

    def test_backend_replay_error_is_generic_and_does_not_break_picker(self):
        replay = urllib.error.HTTPError(CALLBACK, 409, CODE + STATE, {}, BytesIO((CODE + STATE).encode()))
        with patch.object(app.urllib.request.OpenerDirector, "open", side_effect=[response(), replay]) as send:
            first = self.handler()
            first.do_GET()
            second = self.handler()
            second.do_GET()
        self.assertEqual(send.call_count, 2)  # One attempt per navigation, never an automatic retry.
        self.assertEqual(HandlerHarness.status(first), 303)
        self.assertEqual(HandlerHarness.status(second), 502)
        self.assertNotIn("Location", dict(HandlerHarness.headers(second)))
        self.assertNotIn(CODE, second.wfile.getvalue().decode())
        ordinary = self.handler("")
        ordinary.send_html = MagicMock()
        ordinary.do_GET()
        ordinary.send_html.assert_called_once_with(False, {})

    def test_callback_rate_limit_does_not_send(self):
        with patch.object(app, "rate_limit_allowed", return_value=False), patch.object(app.urllib.request, "build_opener") as opener:
            handler = self.handler()
            handler.do_GET()
        self.assertEqual(HandlerHarness.status(handler), 429)
        self.assertNotIn("Location", dict(HandlerHarness.headers(handler)))
        opener.assert_not_called()

    def test_only_confirmed_success_redirects_and_errors_retain_safe_html(self):
        for connected, status in ((False, 200), (False, 502), (True, 502), (True, 303), (1, 200)):
            with self.subTest(connected=connected, status=status):
                handler = self.handler()
                handler.send_baza_oauth_result(connected, status)
                headers = dict(HandlerHarness.headers(handler))
                self.assertNotIn("Location", headers)
                self.assertEqual(headers["Cache-Control"], "no-store")
                self.assertEqual(headers["Referrer-Policy"], "no-referrer")
                body = handler.wfile.getvalue().decode()
                self.assertIn("Проверьте подключение", body)
                self.assertIn('history.replaceState(null,"","/")', body)
                self.assertIn('href="https://baza.krugo.tours/chats"', body)
                self.assertNotIn(CODE, body + json.dumps(headers))
                self.assertNotIn(STATE, body + json.dumps(headers))


if __name__ == "__main__":
    unittest.main()
