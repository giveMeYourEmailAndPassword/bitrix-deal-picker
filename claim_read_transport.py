"""One bounded HTTPS connection for the fresh reads in a single Baza claim."""
import http.client
import io
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager

# Each of these exact Bitrix methods is read-only even when its parameters use POST.
_CLAIM_READ_METHODS = frozenset({
    "user.get", "crm.deal.get", "crm.contact.get", "imopenlines.crm.chat.get",
    "imopenlines.dialog.get", "imopenlines.v2.Session.list", "im.user.get",
})
_MAX_READS = 16
_MAX_SCOPE_SECONDS = 20
_thread = threading.local()
_standard_urlopen = urllib.request.urlopen if getattr(urllib.request.urlopen, "__module__", "") == "urllib.request" else None


class _ClaimReads:
    def __init__(self):
        self.deadline = time.monotonic() + _MAX_SCOPE_SECONDS
        self.base = None
        self.connection = None
        self.remaining = _MAX_READS

    def close(self):
        self.remaining = 0
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def prepare(self, base, timeout):
        if self.remaining == 0 or time.monotonic() >= self.deadline:
            self.close()
            return None
        if self.base is not None and self.base != base:
            return None
        if self.connection is None:
            url = urllib.parse.urlsplit(base)
            if (url.scheme != "https" or not url.hostname or url.username is not None
                    or url.password is not None or url.port not in (None, 443)
                    or url.query or url.fragment
                    or not re.fullmatch(r"/rest/[^/?#]+/[^/?#]+/", url.path)):
                return None
            if urllib.request.getproxies().get("https") and not urllib.request.proxy_bypass(url.hostname):
                self.close()
                return None
            self.base = base
            self.connection = http.client.HTTPSConnection(url.hostname, timeout=timeout)
        self.remaining -= 1
        self.connection.timeout = timeout
        if self.connection.sock is not None:
            self.connection.sock.settimeout(timeout)
        return self.connection


@contextmanager
def claim_read_scope():
    previous = getattr(_thread, "scope", None)
    scope = _ClaimReads()
    _thread.scope = scope
    try:
        yield
    finally:
        _thread.scope = previous
        scope.close()


def _send_read(connection, target, data):
    headers = {"User-Agent": "Python-urllib/" + urllib.request.__version__}
    if data is not None:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    connection.request("POST" if data is not None else "GET", target, body=data, headers=headers)
    return connection.getresponse()


@contextmanager
def open_bitrix_response(base, method, *, data, timeout, max_bytes):
    """Yield a urllib-compatible response. The caller keeps its normal bounded JSON decoder."""
    url = base + method + ".json"
    scope = getattr(_thread, "scope", None)
    connection = None
    if scope is not None and method in _CLAIM_READ_METHODS and urllib.request.urlopen is _standard_urlopen:
        try:
            connection = scope.prepare(base, timeout)
        except OSError:
            # A keep-alive socket can close while its timeout is being reset.
            scope.close()
    if connection is None:
        with urllib.request.urlopen(url, data=data, timeout=timeout) as response:
            yield response
        return

    target = urllib.parse.urlsplit(base).path + method + ".json"
    reused = connection.sock is not None
    started = time.monotonic()
    try:
        try:
            response = _send_read(connection, target, data)
        except (http.client.RemoteDisconnected, BrokenPipeError, ConnectionResetError):
            # A server can silently expire its keep-alive connection. Only the
            # explicitly allowed reads reach here, and only before response headers.
            scope.close()
            remaining = timeout - (time.monotonic() - started)
            if not reused or remaining <= 0:
                raise
            with urllib.request.urlopen(url, data=data, timeout=remaining) as response:
                yield response
            return
        with response:
            if not 200 <= response.status < 300:
                # The caller's existing HTTPError handler reads the bounded error
                # body after leaving this context. Never forward a credentialed
                # redirect or put the webhook path in a new error message.
                try:
                    length = int(response.headers.get("Content-Length", 0))
                except ValueError:
                    length = -1
                body = response.read(max_bytes + 1) if 0 <= length <= max_bytes else b""
                raise urllib.error.HTTPError("", response.status, response.reason, response.headers, io.BytesIO(body))
            yield response
            if response.length not in (None, 0):
                raise http.client.IncompleteRead(b"")
            if not response.isclosed() or response.will_close or connection.sock is None:
                scope.close()
    except BaseException:
        # Broken framing, a bounded/abandoned read, invalid JSON or a caller
        # exception must never leave unread bytes for the next method.
        scope.close()
        raise
