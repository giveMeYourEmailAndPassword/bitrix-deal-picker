"""Authentication for the private Baza -> picker bridge; no browser credentials."""

import hashlib
import hmac
import json
import re
import time


KEY_ID = "baza-picker-v1"
MAX_BODY_BYTES = 16_384
MAX_CLOCK_SKEW_SECONDS = 300
ACTIONS = frozenset({"status", "next", "claim", "reject", "extra-request"})
PATH_PREFIX = "/integrations/baza/v1/"


class BridgeError(Exception):
    def __init__(self, code, status):
        super().__init__(code)
        self.code = code
        self.status = status


def configured(secret):
    return bool(len(str(secret or "").encode("utf-8")) >= 32 and "REPLACE" not in str(secret).upper())


def _header(headers, key):
    values = headers.get_all(key) if hasattr(headers, "get_all") else [headers.get(key)]
    if not values or len(values) != 1 or not isinstance(values[0], str):
        raise BridgeError("invalid_signature", 401)
    return values[0]


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise BridgeError("invalid_json", 400)
        result[key] = value
    return result


def authenticate(path, body, headers, secret, state_store, *, now=None):
    """Validate exact wire bytes before parsing or reserving an atomic durable nonce."""
    if not configured(secret):
        raise BridgeError("bridge_not_configured", 503)
    if len(body) > MAX_BODY_BYTES:
        raise BridgeError("body_too_large", 413)
    if not path.startswith(PATH_PREFIX) or path[len(PATH_PREFIX):] not in ACTIONS:
        raise BridgeError("not_found", 404)
    key_id = _header(headers, "X-Krugosvet-Key-Id")
    timestamp = _header(headers, "X-Krugosvet-Timestamp")
    nonce = _header(headers, "X-Krugosvet-Nonce")
    signature = _header(headers, "X-Krugosvet-Signature")
    if key_id != KEY_ID or not re.fullmatch(r"[0-9]{10}", timestamp) or not re.fullmatch(r"[a-zA-Z0-9_-]{16,128}", nonce) or not re.fullmatch(r"[a-f0-9]{64}", signature):
        raise BridgeError("invalid_signature", 401)
    now = int(time.time() if now is None else now)
    if abs(now - int(timestamp)) > MAX_CLOCK_SKEW_SECONDS:
        raise BridgeError("expired_signature", 401)
    canonical = "\n".join((timestamp, nonce, "POST", path, hashlib.sha256(body).hexdigest())).encode("utf-8")
    expected = hmac.new(secret.encode("utf-8"), canonical, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise BridgeError("invalid_signature", 401)
    try:
        payload = json.loads(body.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeError, ValueError) as exc:
        raise BridgeError("invalid_json", 400) from exc
    if not isinstance(payload, dict):
        raise BridgeError("invalid_json", 400)
    if not state_store.consume_bridge_nonce(key_id, nonce, now=now, expires_at=now + 2 * MAX_CLOCK_SKEW_SECONDS + 60):
        raise BridgeError("replay_detected", 409)
    return payload
