import http.client
import io
import threading
import unittest
import urllib.error
import urllib.request
from unittest.mock import Mock, patch

import claim_read_transport as transport


BASE = "https://portal.invalid/rest/1/nonsecret-test-value/"


class _Wire:
    def __init__(self, body=b""):
        self.body = body
        self.timeouts = []

    def settimeout(self, seconds):
        self.timeouts.append(seconds)

    def makefile(self, *_args):
        return io.BytesIO(self.body)


class _Connection:
    def __init__(self, host, timeout):
        self.host, self.timeout = host, timeout
        self.sock = None
        self.closed = 0
        self.requests = []
        self.replies = []

    def request(self, method, target, body=None, headers=None):
        if self.sock is None:
            self.sock = _Wire()
        self.requests.append((method, target, body, headers))

    def getresponse(self):
        wire = self.replies.pop(0) if self.replies else b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"
        if isinstance(wire, Exception):
            raise wire
        self.sock.body = wire
        response = http.client.HTTPResponse(self.sock)
        response.begin()
        return response

    def close(self):
        self.closed += 1
        self.sock = None


class ClaimTransportTests(unittest.TestCase):
    def setUp(self):
        self.connections = []
        self.replies = []
        def connect(host, *, timeout):
            connection = _Connection(host, timeout)
            connection.replies = self.replies.copy()
            self.connections.append(connection)
            return connection
        self.fallback = Mock(side_effect=lambda *args, **kwargs: io.BytesIO(b"legacy"))
        for context in (
            patch.object(transport.http.client, "HTTPSConnection", side_effect=connect),
            patch.object(transport.urllib.request, "getproxies", return_value={}),
            patch.object(transport.urllib.request, "urlopen", self.fallback),
            patch.object(transport, "_standard_urlopen", self.fallback),
        ):
            context.start()
            self.addCleanup(context.stop)

    def read(self, method="crm.deal.get", *, base=BASE, data=b"id=1", timeout=5, cap=8192):
        with transport.open_bitrix_response(base, method, data=data, timeout=timeout, max_bytes=cap) as response:
            return response.read(cap + 1)

    def test_only_allowlisted_reads_reuse_a_connection_with_the_next_calls_timeout(self):
        with transport.claim_read_scope():
            self.assertEqual(self.read(), b"{}")
            self.assertEqual(self.read("crm.contact.get", data=None, timeout=2), b"{}")
            for method in ["crm.deal.update", "crm.contact.update", "imopenlines.operator.answer", "batch", "new.method"]:
                self.assertEqual(self.read(method), b"legacy")
            self.assertEqual(self.read(), b"{}")
            connection, = self.connections
            self.assertEqual(connection.host, "portal.invalid")
            self.assertEqual([r[0] for r in connection.requests], ["POST", "GET", "POST"])
            self.assertEqual(connection.requests[0][1:3], ("/rest/1/nonsecret-test-value/crm.deal.get.json", b"id=1"))
            self.assertEqual(connection.requests[0][3]["Content-Type"], "application/x-www-form-urlencoded")
            self.assertEqual(connection.sock.timeouts, [2, 5])
            self.assertEqual(self.fallback.call_count, 5)
        self.assertEqual(connection.closed, 1)
        self.assertEqual(self.read(), b"legacy")

    def test_nested_scopes_threads_and_changed_webhook_never_share_a_connection(self):
        with transport.claim_read_scope():
            self.assertEqual(self.read(), b"{}")
            first = self.connections[0]
            with transport.claim_read_scope():
                self.assertEqual(self.read(), b"{}")
            self.assertIsNot(first, self.connections[1])
            self.assertEqual(self.connections[1].closed, 1)
            self.assertEqual(self.read(base=BASE.replace("/1/", "/2/")), b"legacy")
            result = []
            t = threading.Thread(target=lambda: result.append(self.read()))
            t.start()
            t.join()
            self.assertEqual(result, [b"legacy"])
            self.assertEqual(self.read(), b"{}")
            self.assertEqual(len(first.requests), 2)

    def test_custom_urlopen_and_forward_proxy_use_the_existing_transport(self):
        with transport.claim_read_scope():
            custom = Mock(return_value=io.BytesIO(b"mocked"))
            with patch.object(urllib.request, "urlopen", custom):
                self.assertEqual(self.read(), b"mocked")
            with patch.object(urllib.request, "getproxies", return_value={"https": "https://proxy.invalid"}), \
                    patch.object(urllib.request, "proxy_bypass", return_value=False):
                self.assertEqual(self.read(), b"legacy")
        self.assertFalse(self.connections)

    def test_expiry_and_maximum_calls_release_the_connection(self):
        with patch.object(transport, "_MAX_READS", 1), transport.claim_read_scope():
            self.assertEqual(self.read(), b"{}")
            self.assertEqual(self.read(), b"legacy")
        with patch.object(transport.time, "monotonic", return_value=100), transport.claim_read_scope():
            self.assertEqual(self.read(), b"{}")
            with patch.object(transport.time, "monotonic", return_value=121):
                self.assertEqual(self.read(), b"legacy")
        self.assertEqual([c.closed for c in self.connections], [1, 1])

    def test_stale_reused_socket_falls_back_once_only_for_the_read(self):
        with transport.claim_read_scope():
            self.assertEqual(self.read(), b"{}")
            self.connections[0].replies = [http.client.RemoteDisconnected()]
            self.assertEqual(self.read(timeout=2), b"legacy")
            self.assertEqual(self.fallback.call_count, 1)
            self.assertGreater(self.fallback.call_args.kwargs["timeout"], 0)
            self.assertLessEqual(self.fallback.call_args.kwargs["timeout"], 2)
            self.assertEqual(self.read("crm.deal.update"), b"legacy")
            self.assertEqual(self.fallback.call_count, 2)
        self.assertEqual(len(self.connections), 1)

    def test_http_errors_expose_the_bounded_body_and_never_forward_redirects(self):
        for status in ["429 Too Many Requests", "302 Redirect"]:
            with self.subTest(status=status), transport.claim_read_scope():
                self.replies = [f"HTTP/1.1 {status}\r\nLocation: https://other.invalid/\r\nContent-Length: 4\r\n\r\nfail".encode()]
                with self.assertRaises(urllib.error.HTTPError) as caught:
                    self.read()
                self.assertEqual(caught.exception.read(), b"fail")
                self.assertEqual(caught.exception.code, int(status[:3]))
                self.assertNotIn("nonsecret-test-value", str(caught.exception))
                self.assertEqual(self.connections[-1].closed, 1)
        self.fallback.assert_not_called()

    def test_oversized_error_is_not_read_and_a_truncated_success_never_reuses_socket(self):
        self.replies = [b"HTTP/1.1 500 Error\r\nContent-Length: 1000\r\n\r\nsecret-body"]
        with transport.claim_read_scope(), self.assertRaises(urllib.error.HTTPError) as caught:
            self.read(cap=8)
        self.assertEqual(caught.exception.read(), b"")
        self.replies = [b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\n{}"]
        with transport.claim_read_scope():
            with self.assertRaises(http.client.IncompleteRead):
                self.read()
            self.assertEqual(self.read(), b"legacy")

    def test_application_preserves_http_status_json_errors_and_response_cap(self):
        import app
        with patch.object(app, "load_env", return_value=BASE), patch.object(app, "MAX_BITRIX_RESPONSE_BYTES", 100):
            data = b'{"error":"QUERY_LIMIT_EXCEEDED"}'
            for status in ["429 Too Many Requests", "200 OK"]:
                self.replies = [f"HTTP/1.1 {status}\r\nContent-Length: {len(data)}\r\n\r\n".encode() + data]
                with self.subTest(status=status), transport.claim_read_scope(), self.assertRaises(RuntimeError) as caught:
                    app.bitrix_call_full("crm.deal.get", {"id": "1"})
                self.assertEqual(caught.exception.bitrix_http_status, int(status[:3]))
                self.assertEqual(caught.exception.bitrix_error_code, "QUERY_LIMIT_EXCEEDED")
            self.replies = [b"HTTP/1.1 200 OK\r\nContent-Length: 101\r\n\r\n"]
            with transport.claim_read_scope(), self.assertRaisesRegex(RuntimeError, "размер"):
                app.bitrix_call_full("crm.deal.get", {"id": "1"})
            self.assertEqual(self.connections[-1].closed, 1)
            self.fallback.assert_not_called()

    def test_close_header_and_invalid_json_both_discard_the_connection(self):
        self.replies = [b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\n{}"]
        with transport.claim_read_scope():
            self.assertEqual(self.read(), b"{}")
            self.assertEqual(self.read(), b"legacy")
        self.replies = []
        with transport.claim_read_scope():
            with self.assertRaises(ValueError), transport.open_bitrix_response(BASE, "user.get", data=None, timeout=1, max_bytes=1024) as response:
                response.read()
                raise ValueError("decoder rejected response")
            self.assertEqual(self.read(), b"legacy")


if __name__ == "__main__":
    unittest.main()
