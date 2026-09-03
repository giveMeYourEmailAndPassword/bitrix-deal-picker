#!/usr/bin/env python3
import concurrent.futures
import base64
import hashlib
import hmac
import html
import ipaddress
import json
import math
import os
import re
import secrets
import sys
import threading
import time
import urllib.parse
import urllib.error
import urllib.request
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from state_store import (
    ExtraClaimGrantReconciliationRequiredError,
    ExtraClaimGrantUnavailableError,
    IdempotencyConflictError,
    StateStore,
)


INVALID_ENV_VALUES = set()


def env_bool(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    INVALID_ENV_VALUES.add(name)
    return bool(default)


def env_int(name, default, minimum=None, maximum=None):
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        INVALID_ENV_VALUES.add(name)
        value = int(default)
    if minimum is not None:
        if value < minimum:
            INVALID_ENV_VALUES.add(name)
            value = minimum
    if maximum is not None:
        if value > maximum:
            INVALID_ENV_VALUES.add(name)
            value = maximum
    return value


def env_float(name, default, minimum=None, maximum=None):
    try:
        value = float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        INVALID_ENV_VALUES.add(name)
        value = float(default)
    if not math.isfinite(value):
        INVALID_ENV_VALUES.add(name)
        value = float(default)
    if minimum is not None:
        if value < minimum:
            INVALID_ENV_VALUES.add(name)
            value = minimum
    if maximum is not None:
        if value > maximum:
            INVALID_ENV_VALUES.add(name)
            value = maximum
    return value


def safe_urlparse(value):
    """Parse an untrusted URL without letting malformed IPv6 crash callers."""

    try:
        parsed = urllib.parse.urlparse(str(value or "").strip())
        # These properties are lazily validated by urllib.  Touching them here
        # keeps every caller on the same fail-closed parsing boundary.
        parsed.hostname
        parsed.port
        parsed.username
        parsed.password
        return parsed
    except (TypeError, ValueError):
        return None


def url_origin(value):
    parsed = safe_urlparse(value)
    try:
        if (
            parsed is None
            or parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            return ""
        return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
    except (TypeError, ValueError):
        return ""


def is_loopback_http_url(value):
    parsed = safe_urlparse(value)
    try:
        return bool(
            parsed is not None
            and parsed.scheme == "http"
            and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
            and is_local_runtime()
        )
    except (TypeError, ValueError):
        return False


def normalize_allowed_hostname(value):
    value = str(value or "").strip().lower().rstrip(".")
    if not value or "*" in value or len(value) > 253:
        return ""
    if not re.fullmatch(r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", value):
        return ""
    return value


APP_DIR = Path(os.environ.get("APP_DATA_DIR") or Path(__file__).resolve().parent)
MANAGERS_FILE = APP_DIR / "managers.json"
DATA_LOCK = threading.Lock()
GREETING_LOCK = threading.Lock()
GREETING_WAKE_EVENT = threading.Event()
LOST_DEAL_AUTOCLOSE_WAKE_EVENT = threading.Event()
DEAL_ANALYSIS_CACHE_LOCK = threading.Lock()
DEAL_HEADERS_CACHE_LOCK = threading.Lock()
LOCAL_TZ = timezone(timedelta(hours=env_int("APP_TZ_OFFSET_HOURS", 6, -12, 14)))
STATE_STORE = StateStore(APP_DIR, local_timezone=LOCAL_TZ, auto_initialize=False)
APP_VERSION = (
    "2026-08-18-office-extra-claims-proven-greeting-"
    "lost-deal-chat-autoclose-inbound-fix"
)
# Bump whenever classifier, eligibility, source-completeness or oldest-first
# routing semantics change. Pre-deploy tokens must not authorize post-deploy
# decisions under a different routing policy.
ROUTING_POLICY_VERSION = "2026-08-17-routing-v3-extra-claims"

SOURCE_STAGES = {
    "UC_ZJ55BR": "Необработанные ЛИДЫ",
    "UC_PUCAAQ": "ОЖИДАЮТ СПЕЦИАЛИСТА",
}
TARGET_STAGE = "NEW"
TARGET_STAGE_NAME = "В РАБОТЕ"
DEAL_ANALYSIS_CACHE = {}
DEAL_HEADERS_CACHE = {}
PORTAL_USERS_CACHE = {}
PORTAL_USERS_CACHE_TTL_SECONDS = env_float("PORTAL_USERS_CACHE_TTL_SECONDS", 300, 5, 3600)
CACHE_MAX_ENTRIES = env_int("CACHE_MAX_ENTRIES", 500, 50, 5000)
DRY_RUN = env_bool("DRY_RUN", True)
LOST_DEAL_AUTOCLOSE_ENABLED = env_bool("LOST_DEAL_AUTOCLOSE_ENABLED", True)
LOST_DEAL_CLOSE_LEASE_SECONDS = env_int(
    "LOST_DEAL_CLOSE_LEASE_SECONDS", 120, 30, 900
)
LOST_DEAL_HISTORY_MOVE_TOLERANCE_SECONDS = env_int(
    "LOST_DEAL_HISTORY_MOVE_TOLERANCE_SECONDS", 5, 1, 5
)
LOST_DEAL_AUTOCLOSE_POLL_SECONDS = env_float(
    "LOST_DEAL_AUTOCLOSE_POLL_SECONDS", 15, 5, 300
)
MAX_LOST_DEAL_STAGE_HISTORY_ROWS = env_int(
    "MAX_LOST_DEAL_STAGE_HISTORY_ROWS", 5000, 100, 20000
)
LOST_DEAL_AUTOCLOSE_GRACE_SECONDS = env_int(
    "LOST_DEAL_AUTOCLOSE_GRACE_SECONDS", 15, 5, 120
)
LOST_DEAL_MAX_PREFLIGHT_ATTEMPTS = env_int(
    "LOST_DEAL_MAX_PREFLIGHT_ATTEMPTS", 5, 1, 20
)
PUBLIC_APP_URL = os.environ.get("PUBLIC_APP_URL", "").strip().rstrip("/")
ADMIN_USER_IDS = {
    item.strip()
    for item in os.environ.get("ADMIN_USER_IDS", "").split(",")
    if item.strip()
}
ALLOW_UNVERIFIED_USERS = env_bool("ALLOW_UNVERIFIED_USERS", False)
GREETING_AUTO_SEND = env_bool("GREETING_AUTO_SEND", False)
# Automatic delivery uses Bitrix's CRM-bound OpenLine methods.  The selected
# chat must still be active for this exact deal, and the final message call
# repeats that DEAL -> CHAT check on Bitrix's side (CHAT_NOT_IN_CRM on mismatch).
GREETING_AUTO_SEND_SUPPORTED = True
GREETING_WORKER_POLL_SECONDS = env_float(
    "GREETING_WORKER_POLL_SECONDS", 1, 0.25, 30
)
GREETING_ACTOR_THREAD_LIMIT = env_int("GREETING_ACTOR_THREAD_LIMIT", 8, 1, 32)
GREETING_ACTOR_THREAD_SLOTS = threading.BoundedSemaphore(
    GREETING_ACTOR_THREAD_LIMIT
)
NEXT_DEAL_SCAN_LIMIT = env_int("NEXT_DEAL_SCAN_LIMIT", 12, 1, 50)
NEXT_DEAL_SCAN_WORKERS = env_int("NEXT_DEAL_SCAN_WORKERS", NEXT_DEAL_SCAN_LIMIT, 1, 32)
NEXT_DEAL_BATCH_TIMEOUT_SECONDS = env_float("NEXT_DEAL_BATCH_TIMEOUT_SECONDS", 12, 1, 60)
DEAL_ANALYSIS_CACHE_TTL_SECONDS = env_float("DEAL_ANALYSIS_CACHE_TTL_SECONDS", 300, 5, 3600)
DEAL_HEADERS_CACHE_TTL_SECONDS = env_float("DEAL_HEADERS_CACHE_TTL_SECONDS", 10, 1, 60)
BITRIX_TIMEOUT_SECONDS = env_float("BITRIX_TIMEOUT_SECONDS", 12, 1, 60)
BITRIX_FAST_TIMEOUT_SECONDS = env_float("BITRIX_FAST_TIMEOUT_SECONDS", 5, 1, 30)
MAX_BITRIX_RESPONSE_BYTES = env_int(
    "MAX_BITRIX_RESPONSE_BYTES", 5 * 1024 * 1024, 64 * 1024, 20 * 1024 * 1024
)
LIMIT_FREE_WINDOW_START = os.environ.get("LIMIT_FREE_WINDOW_START", "18:00")
LIMIT_FREE_WINDOW_END = os.environ.get("LIMIT_FREE_WINDOW_END", "21:30")
MAX_DEALS_PER_STAGE = env_int("MAX_DEALS_PER_STAGE", 1000, 50, 5000)
# Both Bitrix list methods used to assemble one deal's routing context return
# at most 50 rows per page.  Read enough pages to prove that a source is empty,
# but fail closed instead of silently truncating an unusually large history.
MAX_SOURCE_RECORDS_PER_DEAL = 500
MAX_OPENLINE_MESSAGES_PER_SESSION = 500
MAX_REQUEST_BODY_BYTES = env_int("MAX_REQUEST_BODY_BYTES", 262144, 4096, 1048576)
RATE_LIMIT_REQUESTS = env_int("RATE_LIMIT_REQUESTS", 120, 10, 1000)
RATE_LIMIT_WINDOW_SECONDS = env_int("RATE_LIMIT_WINDOW_SECONDS", 60, 10, 3600)
MAX_CONCURRENT_REQUESTS = env_int("MAX_CONCURRENT_REQUESTS", 64, 4, 512)
MAX_CONCURRENT_SEARCHES = env_int("MAX_CONCURRENT_SEARCHES", 4, 1, 16)
SOCKET_TIMEOUT_SECONDS = env_float("SOCKET_TIMEOUT_SECONDS", 15, 3, 60)
READINESS_CACHE_TTL_SECONDS = env_float("READINESS_CACHE_TTL_SECONDS", 5, 1, 60)
RAW_BITRIX_ALLOWED_DOMAINS = {
    item.strip()
    for item in os.environ.get("BITRIX_ALLOWED_DOMAINS", "").split(",")
    if item.strip()
}
ALLOWED_BITRIX_DOMAINS = {
    normalize_allowed_hostname(item)
    for item in RAW_BITRIX_ALLOWED_DOMAINS
    if normalize_allowed_hostname(item)
}
RAW_APP_ALLOWED_ORIGINS = {
    item.strip().rstrip("/")
    for item in os.environ.get("APP_ALLOWED_ORIGINS", "").split(",")
    if item.strip()
}
APP_ALLOWED_ORIGINS = {url_origin(item) for item in RAW_APP_ALLOWED_ORIGINS if url_origin(item)}
if url_origin(PUBLIC_APP_URL):
    APP_ALLOWED_ORIGINS.add(url_origin(PUBLIC_APP_URL))
CLAIM_STATS_SOURCE = os.environ.get("CLAIM_STATS_SOURCE", "app_events").strip().lower()
REQUIRE_LEGACY_MIGRATION = env_bool("REQUIRE_LEGACY_MIGRATION", True)
REQUIRE_EXPLICIT_ACCESS_RULE = env_bool("REQUIRE_EXPLICIT_ACCESS_RULE", True)
RATE_LIMIT_LOCK = threading.Lock()
RATE_LIMIT_BUCKETS = defaultdict(deque)
USER_VERIFY_CACHE = {}
USER_VERIFY_CACHE_LOCK = threading.Lock()
SEARCH_SEMAPHORE = threading.BoundedSemaphore(MAX_CONCURRENT_SEARCHES)
READINESS_CACHE_LOCK = threading.Lock()
READINESS_CACHE = {"checkedAt": 0.0, "state": None}
USER_VERIFY_CACHE_TTL_SECONDS = env_float("USER_VERIFY_CACHE_TTL_SECONDS", 300, 5, 300)
SELECTION_TOKEN_TTL_SECONDS = env_int("SELECTION_TOKEN_TTL_SECONDS", 1800, 60, 86400)
SEARCH_CURSOR_TTL_SECONDS = env_int("SEARCH_CURSOR_TTL_SECONDS", 120, 30, 900)
BITRIX_CLAIM_MARKER_FIELD = os.environ.get("BITRIX_CLAIM_MARKER_FIELD", "").strip().upper()
CLAIM_OPERATION_PENDING_TTL_SECONDS = env_int(
    "CLAIM_OPERATION_PENDING_TTL_SECONDS", 300, 180, 3600
)
CLAIM_RECONCILE_INTERVAL_SECONDS = env_float(
    "CLAIM_RECONCILE_INTERVAL_SECONDS", 60, 30, 3600
)
CLAIM_RECONCILE_BATCH_SIZE = env_int("CLAIM_RECONCILE_BATCH_SIZE", 100, 1, 1000)
CLAIM_EVENT_EXPORT_ENABLED = env_bool("CLAIM_EVENT_EXPORT_ENABLED", False)
EXTRA_CLAIM_REQUESTS_ENABLED = env_bool("EXTRA_CLAIM_REQUESTS_ENABLED", False)
BAZA_API_BASE_URL = os.environ.get("BAZA_API_BASE_URL", "").strip().rstrip("/")
BAZA_HMAC_SECRET = os.environ.get("BAZA_HMAC_SECRET", "")
BAZA_HMAC_KEY_ID = os.environ.get("BAZA_HMAC_KEY_ID", "").strip()
BAZA_TIMEOUT_SECONDS = env_float("BAZA_TIMEOUT_SECONDS", 5, 1, 30)
BAZA_MAX_RESPONSE_BYTES = env_int(
    "BAZA_MAX_RESPONSE_BYTES", 1024 * 1024, 4096, 5 * 1024 * 1024
)
INTEGRATION_OUTBOX_INTERVAL_SECONDS = env_float(
    "INTEGRATION_OUTBOX_INTERVAL_SECONDS", 15, 5, 3600
)
INTEGRATION_OUTBOX_BATCH_SIZE = env_int(
    "INTEGRATION_OUTBOX_BATCH_SIZE", 20, 1, 200
)
LIMIT_FREE_DATES = {"2026-08-31"}

REJECT_REASONS = {
    "not_my_country": "Не моя страна",
    "unclear_request": "Непонятный запрос",
    "duplicate": "Дубль",
    "other": "Другое",
}

SERVICE_PATTERNS = [
    r"чат открытой линии",
    r"чат с клиентом whatsapp",
    r"секундочку, подключаю специалиста",
    r"подключение #\d+",
    r"подключение #",
    r"начат новый диалог",
    r"обращение направлено",
    r"обращение перенаправлено",
    r"enquiry transferred",
    r"создана новая сделка",
    r"контактная информация сохранена",
    r"в данном канале у оператора",
    r"с вами на связи менеджер",
    r"krugosvet insta",
    r"krugosvet w/a",
    r"куда хотите поехать",
    r"подбор туров",
    r"все отели",
]

DESTINATION_KEYWORDS = {
    "Турция": [
        "турция", "турци", "turkey", "антал", "анталья", "antalya", "аланья", "алания", "alanya",
        "стамбул", "istanbul", "кемер", "kemer", "бодрум", "bodrum", "сиде", "side", "белек", "belek",
        "мармарис", "marmaris", "фетхие", "fethiye", "даламан", "dalaman", "кушадасы", "kusadasi",
        "каппадок", "cappadocia", "измир", "izmir", "памуккале", "pamukkale",
    ],
    "Египет": [
        "египет", "egypt", "шарм", "sharm", "хургада", "hurghada", "hurgada", "каир", "cairo",
        "марса алам", "марса-алам", "marsa alam", "дахаб", "dahab", "эль гуна", "эль-гуна", "el gouna",
        "макади", "makadi", "сафага", "safaga", "таба", "taba", "луксор", "luxor", "александрия",
    ],
    "ОАЭ": [
        "оаэ", "эмират", "uae", "дубай", "dubai", "абу даби", "абу-даби", "abu dhabi",
        "шарджа", "sharjah", "рас эль хайма", "рас-эль-хайма", "ras al khaimah", "аджман", "ajman",
        "фуджейра", "fujairah", "умм аль кувейн", "умм-аль-кувейн",
    ],
    "Таиланд": [
        "таиланд", "тайланд", "thailand", "пхукет", "phuket", "паттайя", "pattaya",
        "самуи", "samui", "koh samui", "бангкок", "bangkok", "краби", "krabi",
        "као лак", "khao lak", "пхи пхи", "пхи-пхи", "phi phi", "чиангмай", "chiang mai",
        "ко чанг", "koh chang", "хуахин", "hua hin",
    ],
    "Вьетнам": [
        "вьетнам", "vietnam", "нячанг", "ня чанг", "nha trang", "фукуок", "фу куок", "phu quoc",
        "дананг", "да нанг", "da nang", "хошимин", "ho chi minh", "ханой", "hanoi",
        "муйне", "муй нэ", "mui ne", "далат", "dalat", "хойан", "хой ан", "hoi an",
        "фантьет", "фан тьет", "phan thiet",
    ],
    "Мальдивы": ["мальдив", "maldives", "мале", "male", "маафуши", "maafushi", "атолл", "атол"],
    "Индонезия": [
        "индонезия", "indonesia", "бали", "bali", "убуд", "ubud", "денпасар", "denpasar",
        "джакарта", "jakarta", "нуса дуа", "nusa dua", "семиньяк", "seminyak", "санур", "sanur",
    ],
    "Грузия": [
        "грузия", "georgia", "тбилиси", "tbilisi", "батуми", "batumi", "кобулети", "kobuleti",
        "гудаури", "gudauri", "бакуриани", "bakuriani", "боржоми", "borjomi", "кутаиси", "kutaisi",
    ],
    "Китай": [
        "китай", "china", "пекин", "beijing", "шанхай", "shanghai", "санья", "sanya",
        "хайнань", "hainan", "гуанчжоу", "guangzhou", "чэнду", "chengdu", "урумчи", "urumqi",
        "гонконг", "хонконг", "hong kong", "макао", "macau", "macao",
    ],
    "Катар": ["катар", "qatar", "доха", "doha"],
    "Малайзия": [
        "малайзия", "malaysia", "куала лумпур", "куала-лумпур", "kuala lumpur",
        "лангкави", "langkawi", "пенанг", "penang", "борнео", "borneo", "кота кинабалу", "kota kinabalu",
    ],
    "Сейшелы": ["сейшел", "seychelles", "маэ", "mahe", "праслин", "praslin", "ла диг", "la digue"],
    "Европа": [
        "европа", "шенген", "италия", "рим", "милан", "франция", "париж", "испания", "барселона",
        "германия", "берлин", "чехия", "прага", "греция", "афины", "кипр", "айя напа", "протарас",
    ],
    "Визы": ["виза", "визу", "визовый", "шенген"],
}


def load_env():
    base = os.environ.get("BITRIX_WEBHOOK_BASE")
    if not base:
        raise RuntimeError("Не задан BITRIX_WEBHOOK_BASE")
    parsed = safe_urlparse(base)
    try:
        valid = bool(
            parsed is not None
            and parsed.scheme == "https"
            and normalize_allowed_hostname(parsed.hostname)
            and not parsed.username
            and not parsed.password
            and re.fullmatch(r"/rest/[^/]+/[^/]+/?", parsed.path or "")
            and not parsed.query
            and not parsed.fragment
        )
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise RuntimeError("BITRIX_WEBHOOK_BASE должен быть корректным HTTPS URL Bitrix24")
    return base.rstrip("/") + "/"


def configured_bitrix_domains():
    domains = set(ALLOWED_BITRIX_DOMAINS)
    try:
        base = load_env()
    except RuntimeError:
        base = ""
    parsed = safe_urlparse(base)
    try:
        hostname = parsed.hostname if parsed is not None else ""
    except (TypeError, ValueError):
        hostname = ""
    normalized = normalize_allowed_hostname(hostname)
    if normalized:
        domains.add(normalized)
    return domains


def webhook_bitrix_domain():
    try:
        base = load_env()
    except RuntimeError:
        return ""
    parsed = safe_urlparse(base)
    try:
        return normalize_allowed_hostname(parsed.hostname if parsed is not None else "")
    except (TypeError, ValueError):
        return ""


def normalize_bitrix_domain(value):
    raw = str(value or "").strip()
    if not raw:
        raise PermissionError("Не указан домен Bitrix24")
    parsed = safe_urlparse(raw if "://" in raw else f"https://{raw}")
    try:
        hostname = normalize_allowed_hostname(parsed.hostname if parsed is not None else "")
        valid = bool(
            parsed is not None
            and parsed.scheme == "https"
            and hostname
            and not parsed.username
            and not parsed.password
            and parsed.path in {"", "/"}
            and not parsed.query
            and not parsed.fragment
        )
    except (TypeError, ValueError):
        hostname = ""
        valid = False
    if not valid:
        raise PermissionError("Некорректный домен Bitrix24")
    if not webhook_bitrix_domain() or hostname != webhook_bitrix_domain():
        raise PermissionError("OAuth относится не к тому порталу Bitrix24")
    return hostname


def is_local_runtime():
    host = os.environ.get("HOST", "127.0.0.1").strip().lower()
    return host in {"127.0.0.1", "localhost", "::1"}


def is_railway_runtime():
    # Use only documented Railway-provided identifiers.  Relying on an
    # unofficial variable would collapse every proxied user into one rate-limit
    # bucket and would also weaken the persistent-Volume readiness gate.
    return bool(
        os.environ.get("RAILWAY_PROJECT_ID")
        or os.environ.get("RAILWAY_DEPLOYMENT_ID")
        or os.environ.get("RAILWAY_ENVIRONMENT_ID")
    )


def is_unverified_dev_mode():
    return ALLOW_UNVERIFIED_USERS and DRY_RUN and is_local_runtime()


class BazaIntegrationHttpError(RuntimeError):
    def __init__(self, status, payload=None):
        super().__init__(f"Baza HTTP {int(status)}")
        self.status = int(status)
        self.payload = payload if isinstance(payload, dict) else {}

    @property
    def retryable(self):
        return self.status == 429 or self.status >= 500


def baza_base_url_valid():
    parsed = safe_urlparse(BAZA_API_BASE_URL)
    try:
        return bool(
            parsed is not None
            and parsed.scheme in {"https", "http"}
            and parsed.hostname
            and not parsed.username
            and not parsed.password
            and parsed.path in {"", "/"}
            and not parsed.query
            and not parsed.fragment
            and (
                parsed.scheme == "https"
                or (
                    parsed.hostname in {"127.0.0.1", "localhost", "::1"}
                    and is_local_runtime()
                )
            )
        )
    except (TypeError, ValueError):
        return False


def baza_integration_enabled():
    return bool(CLAIM_EVENT_EXPORT_ENABLED or EXTRA_CLAIM_REQUESTS_ENABLED)


def baza_integration_configured():
    return bool(
        baza_base_url_valid()
        and BAZA_HMAC_KEY_ID
        and len(BAZA_HMAC_SECRET.encode("utf-8")) >= 32
        and "REPLACE" not in BAZA_HMAC_SECRET.upper()
    )


def claim_event_delivery_enabled():
    # Extra-claim grants are consumed by a claim event. Keep the historical
    # EXTRA=1 behaviour even when the independent export switch is left off.
    return bool(CLAIM_EVENT_EXPORT_ENABLED or EXTRA_CLAIM_REQUESTS_ENABLED)


def enabled_integration_outbox_kinds():
    kinds = set()
    if claim_event_delivery_enabled():
        kinds.add("claim_event")
    if EXTRA_CLAIM_REQUESTS_ENABLED:
        kinds.add("extra_claim_request")
    return kinds


def extra_claim_requests_configured():
    return bool(EXTRA_CLAIM_REQUESTS_ENABLED and baza_integration_configured())


def canonical_json_bytes(payload):
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def sign_baza_request(method, path, body, *, timestamp=None, nonce=None):
    """Return the canonical cross-system HMAC headers.

    ``body`` must be the exact bytes sent on the wire.  Keeping signing in one
    function lets both services test the same byte-for-byte contract.
    """

    method = str(method or "").upper()
    path = str(path or "")
    if not path.startswith("/") or "?" in path or "#" in path:
        raise ValueError("Baza integration path must be an absolute path without query")
    body = bytes(body)
    timestamp = str(timestamp if timestamp is not None else int(time.time()))
    nonce = str(nonce or secrets.token_hex(16))
    canonical = "\n".join(
        (
            timestamp,
            nonce,
            method,
            path,
            hashlib.sha256(body).hexdigest(),
        )
    ).encode("utf-8")
    signature = hmac.new(
        BAZA_HMAC_SECRET.encode("utf-8"), canonical, hashlib.sha256
    ).hexdigest()
    return {
        "X-Krugosvet-Key-Id": BAZA_HMAC_KEY_ID,
        "X-Krugosvet-Timestamp": timestamp,
        "X-Krugosvet-Nonce": nonce,
        "X-Krugosvet-Signature": signature,
    }


def read_bounded_json_response(response, maximum_bytes=BAZA_MAX_RESPONSE_BYTES):
    raw_length = response.headers.get("Content-Length") if response.headers else None
    if raw_length:
        try:
            if int(raw_length) > maximum_bytes:
                raise RuntimeError("Baza response exceeds the safe size")
        except ValueError as exc:
            raise RuntimeError("Baza returned an invalid response size") from exc
    raw = response.read(maximum_bytes + 1)
    if len(raw) > maximum_bytes:
        raise RuntimeError("Baza response exceeds the safe size")
    if not raw:
        return {}
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Baza returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Baza returned an unexpected response")
    return payload


def baza_post(path, payload, *, timeout=None):
    if not baza_integration_configured():
        raise RuntimeError("Baza integration is not configured")
    body = canonical_json_bytes(dict(payload or {}))
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
        **sign_baza_request("POST", path, body),
    }
    request = urllib.request.Request(
        BAZA_API_BASE_URL + path,
        data=body,
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(
            request, timeout=timeout or BAZA_TIMEOUT_SECONDS
        ) as response:
            payload = read_bounded_json_response(response)
            status = int(getattr(response, "status", 200) or 200)
    except urllib.error.HTTPError as exc:
        try:
            payload = read_bounded_json_response(exc)
        except Exception:
            payload = {}
        raise BazaIntegrationHttpError(exc.code, payload) from exc
    if not 200 <= status < 300:
        raise BazaIntegrationHttpError(status, payload)
    return payload


def json_for_script(value):
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


def bounded_cache_put(cache, key, value, maximum=CACHE_MAX_ENTRIES):
    cache[key] = value
    while len(cache) > maximum:
        oldest_key = next(iter(cache))
        cache.pop(oldest_key, None)


def rate_limit_allowed(key):
    now = time.monotonic()
    cutoff = now - RATE_LIMIT_WINDOW_SECONDS
    with RATE_LIMIT_LOCK:
        bucket = RATE_LIMIT_BUCKETS[str(key)]
        while bucket and bucket[0] <= cutoff:
            bucket.popleft()
        if len(bucket) >= RATE_LIMIT_REQUESTS:
            return False
        bucket.append(now)
        if len(RATE_LIMIT_BUCKETS) > CACHE_MAX_ENTRIES:
            excess = len(RATE_LIMIT_BUCKETS) - CACHE_MAX_ENTRIES
            for old_key in [item for item in RATE_LIMIT_BUCKETS if item != str(key)][:excess]:
                RATE_LIMIT_BUCKETS.pop(old_key, None)
        return True


def normalize_entity_id(value):
    value = str(value or "").strip()
    if not re.fullmatch(r"[1-9]\d{0,19}", value):
        return ""
    return value


def claim_marker_field_valid():
    return bool(
        re.fullmatch(r"UF_CRM_[A-Z0-9_]{3,64}", BITRIX_CLAIM_MARKER_FIELD)
        and "REPLACE" not in BITRIX_CLAIM_MARKER_FIELD
    )


def selection_signing_key():
    # The inbound webhook URL already contains a high-entropy secret.  Deriving
    # a purpose-specific key avoids another deployment secret and never exposes
    # the webhook itself to the browser.
    return hashlib.sha256(
        (
            "deal-picker-selection\0"
            + ROUTING_POLICY_VERSION
            + "\0"
            + load_env()
        ).encode("utf-8")
    ).digest()


def deal_version(deal):
    deal = deal or {}
    modified = str(
        deal.get("DATE_MODIFY")
        or deal.get("dateModify")
        or deal.get("DATE_CREATE")
        or deal.get("dateCreate")
        or ""
    ).strip()
    last_activity = str(
        deal.get("LAST_ACTIVITY_TIME")
        or deal.get("lastActivityTime")
        or ""
    ).strip()
    if not last_activity:
        return modified
    # A new timeline/chat activity can change routing without changing the
    # ordinary deal fields.  Keep that activity in the signed lifecycle while
    # returning a fixed-size, token-safe version string.
    material = json.dumps(
        {"modified": modified, "lastActivity": last_activity},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return "v2:" + hashlib.sha256(material.encode("utf-8")).hexdigest()


def _issue_signed_token(payload):
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("ascii").rstrip("=")
    signature = hmac.new(selection_signing_key(), encoded.encode("ascii"), hashlib.sha256).hexdigest()
    return f"{encoded}.{signature}"


def _decode_signed_token(token):
    token = str(token or "")
    if not token or len(token) > 2048 or token.count(".") != 1:
        return None
    encoded, signature = token.split(".", 1)
    try:
        encoded_bytes = encoded.encode("ascii")
    except UnicodeEncodeError:
        return None
    expected = hmac.new(selection_signing_key(), encoded_bytes, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        padding = "=" * (-len(encoded) % 4)
        payload = json.loads(base64.urlsafe_b64decode(encoded + padding).decode("utf-8"))
    except (ValueError, TypeError, UnicodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def issue_selection_token(deal_id, manager_id, version, policy_hash, now=None):
    deal_id = normalize_entity_id(deal_id)
    manager_id = normalize_entity_id(manager_id)
    version = str(version or "").strip()
    policy_hash = str(policy_hash or "")
    if (
        not deal_id
        or not manager_id
        or not version
        or len(version) > 256
        or not re.fullmatch(r"[0-9a-f]{64}", policy_hash)
    ):
        raise ValueError("Некорректный ID сделки, менеджера, версия или политика доступа")
    expires_at = int(now if now is not None else time.time()) + SELECTION_TOKEN_TTL_SECONDS
    return _issue_signed_token(
        {
            "kind": "selection",
            "deal": deal_id,
            "manager": manager_id,
            "version": version,
            "policy": policy_hash,
            "routing": ROUTING_POLICY_VERSION,
            "expires": expires_at,
        }
    )


def decode_selection_token(token, deal_id, manager_id, now=None):
    payload = _decode_signed_token(token)
    try:
        expires_at = int((payload or {}).get("expires"))
    except (ValueError, TypeError):
        return None
    current_time = int(now if now is not None else time.time())
    valid = (
        payload.get("kind") == "selection"
        and normalize_entity_id(payload.get("deal")) == normalize_entity_id(deal_id)
        and normalize_entity_id(payload.get("manager")) == normalize_entity_id(manager_id)
        and bool(str(payload.get("version") or "").strip())
        and len(str(payload.get("version") or "")) <= 256
        and bool(re.fullmatch(r"[0-9a-f]{64}", str(payload.get("policy") or "")))
        and payload.get("routing") == ROUTING_POLICY_VERSION
        and current_time <= expires_at <= current_time + SELECTION_TOKEN_TTL_SECONDS
    )
    return payload if valid else None


def verify_selection_token(token, deal_id, manager_id, now=None):
    return decode_selection_token(token, deal_id, manager_id, now=now) is not None


def claim_operation_key(deal_id, version):
    digest = hashlib.sha256(f"{normalize_entity_id(deal_id)}\0{version}".encode("utf-8")).hexdigest()[:24]
    return f"claim:{normalize_entity_id(deal_id)}:{digest}"


def claim_attempt_marker(operation_key, manager_id, nonce=None):
    nonce = str(nonce or secrets.token_hex(12))
    return f"{operation_key}:{normalize_entity_id(manager_id)}:{nonce}"


def rejection_semantic_key(manager_id, deal_id, version):
    return hashlib.sha256(
        f"reject\0{normalize_entity_id(manager_id)}\0{normalize_entity_id(deal_id)}\0{version}".encode("utf-8")
    ).hexdigest()


def manager_policy_hash(manager=None, rule=None):
    policy = {
        "competencies": sorted(
            {str(item or "").strip().casefold() for item in (manager or {}).get("competencies", []) if str(item or "").strip()}
        ),
        "active": (manager or {}).get("active") is True,
        "intranet": (manager or {}).get("intranet") is True,
        "rule": {
            "enabled": (rule or {}).get("enabled") is not False,
            "dailyLimit": (rule or {}).get("dailyLimit"),
        },
    }
    material = json.dumps(policy, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def search_snapshot(headers, manager=None, rule=None):
    material = ROUTING_POLICY_VERSION + "\n" + manager_policy_hash(manager, rule) + "\n"
    material += "\n".join(
        f"{normalize_entity_id(item.get('ID'))}:{deal_version(item)}" for item in headers
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def issue_search_cursor(manager_id, offset, snapshot, now=None):
    manager_id = normalize_entity_id(manager_id)
    offset = int(offset)
    if not manager_id or offset < 0 or not re.fullmatch(r"[0-9a-f]{64}", str(snapshot or "")):
        raise ValueError("Некорректное продолжение поиска")
    current_time = int(now if now is not None else time.time())
    return _issue_signed_token(
        {
            "kind": "search",
            "manager": manager_id,
            "offset": offset,
            "snapshot": snapshot,
            "routing": ROUTING_POLICY_VERSION,
            "expires": current_time + SEARCH_CURSOR_TTL_SECONDS,
        }
    )


def decode_search_cursor(token, manager_id, snapshot, now=None):
    if not token:
        return 0
    payload = _decode_signed_token(token)
    try:
        expires_at = int((payload or {}).get("expires"))
        offset = int((payload or {}).get("offset"))
    except (TypeError, ValueError):
        return None
    current_time = int(now if now is not None else time.time())
    if (
        payload.get("kind") != "search"
        or normalize_entity_id(payload.get("manager")) != normalize_entity_id(manager_id)
        or payload.get("snapshot") != snapshot
        or payload.get("routing") != ROUTING_POLICY_VERSION
        or offset < 0
        or not (current_time <= expires_at <= current_time + SEARCH_CURSOR_TTL_SECONDS)
    ):
        return None
    return offset


def portal_base_url():
    parsed = safe_urlparse(load_env())
    if parsed is None:
        raise RuntimeError("BITRIX_WEBHOOK_BASE должен быть корректным HTTPS URL Bitrix24")
    return f"{parsed.scheme}://{parsed.netloc}"


def load_managers():
    if not MANAGERS_FILE.exists():
        return []
    payload = json.loads(MANAGERS_FILE.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or len(payload) > 5000:
        raise ValueError("managers.json должен содержать ограниченный список")
    validated = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("Каждый элемент managers.json должен быть объектом")
        manager_id = str(item.get("id") or "").strip()
        name = item.get("name")
        competencies = item.get("competencies", [])
        active = item.get("active", False)
        if not manager_id or len(manager_id) > 100:
            raise ValueError("В managers.json указан некорректный id")
        if not isinstance(name, str) or not name.strip() or len(name) > 200:
            raise ValueError("В managers.json указано некорректное имя")
        if (
            not isinstance(competencies, list)
            or len(competencies) > 100
            or any(
                not isinstance(value, str)
                or not value.strip()
                or len(value) > 100
                for value in competencies
            )
        ):
            raise ValueError("В managers.json указан некорректный список компетенций")
        if not isinstance(active, bool):
            raise ValueError("В managers.json поле active должно быть boolean")
        # The committed disabled placeholder is intentionally non-numeric;
        # any enabled local development manager must use a real Bitrix ID.
        if active and not normalize_entity_id(manager_id):
            raise ValueError("Активный manager id должен быть числовым Bitrix ID")
        validated.append(
            {
                **item,
                "id": manager_id,
                "name": name.strip(),
                "competencies": [value.strip() for value in competencies],
                "active": active,
            }
        )
    return validated


def local_now():
    return datetime.now(LOCAL_TZ)


def local_date():
    return local_now().date().isoformat()


def is_limit_free_day(now=None):
    now = now or local_now()
    return now.weekday() == 6 or now.date().isoformat() in LIMIT_FREE_DATES


def parse_hhmm(value, fallback):
    value = str(value or "").strip()
    if not re.match(r"^\d{1,2}:\d{2}$", value):
        value = fallback
    hours, minutes = value.split(":", 1)
    return max(0, min(23, int(hours))) * 60 + max(0, min(59, int(minutes)))


def is_limit_free_time(now=None):
    now = now or local_now()
    current = now.hour * 60 + now.minute
    start = parse_hhmm(LIMIT_FREE_WINDOW_START, "18:00")
    end = parse_hhmm(LIMIT_FREE_WINDOW_END, "21:30")
    if start <= end:
        return start <= current < end
    return current >= start or current < end


def is_limit_bypassed_now(now=None):
    now = now or local_now()
    return is_limit_free_day(now) or is_limit_free_time(now)


def entry_date(entry):
    raw = str(entry.get("timestamp") or "")
    try:
        parsed = datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(LOCAL_TZ).date().isoformat()


def normalize_date(value, fallback):
    value = str(value or "").strip()
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except (TypeError, ValueError):
        pass
    return fallback


def load_access_rules():
    return {"managers": STATE_STORE.list_rules()}


def get_manager_rule(manager_id):
    return STATE_STORE.get_rule(manager_id)


def set_manager_rule(manager_id, enabled=True, daily_limit=None, note=""):
    return STATE_STORE.set_rule(
        manager_id,
        enabled=enabled,
        daily_limit=daily_limit,
        note=note,
    )


def load_claim_log():
    return STATE_STORE.list_claims()


def claim_log_entry(manager_id, deal, timestamp=None):
    if timestamp is None:
        timestamp = local_now().isoformat()
    elif isinstance(timestamp, datetime):
        timestamp = timestamp.isoformat()
    return {
        "timestamp": str(timestamp),
        "managerId": str(manager_id),
        "dealId": str(deal.get("ID") or deal.get("id") or ""),
    }


def claim_operation_attempt_timestamp(operation):
    """Return the best pre-recovery time for an operation's current attempt.

    New operations persist ``request.attemptStartedAt`` on every real
    begin/retry/reassign.  Legacy rows need a conservative fallback:
    ``updatedAt`` is the reservation time while pending; for the first failed
    attempt ``createdAt`` is the original reservation; after older retries,
    ``finalizedAt`` is the closest attempt-specific time still available.
    """

    operation = operation if isinstance(operation, dict) else {}
    request = operation.get("request")
    request = request if isinstance(request, dict) else {}
    history = operation.get("attemptHistory")
    history = history if isinstance(history, list) else []
    status = str(operation.get("status") or "")
    if status == "pending":
        candidates = (
            request.get("attemptStartedAt"),
            operation.get("updatedAt"),
            operation.get("createdAt"),
        )
    elif status == "failed" and not history:
        candidates = (
            request.get("attemptStartedAt"),
            operation.get("createdAt"),
            operation.get("finalizedAt"),
            operation.get("updatedAt"),
        )
    else:
        candidates = (
            request.get("attemptStartedAt"),
            operation.get("finalizedAt"),
            operation.get("updatedAt"),
            operation.get("createdAt"),
        )
    for candidate in candidates:
        raw = str(candidate or "").strip()
        if not raw:
            continue
        try:
            datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith("Z") else raw)
        except ValueError:
            continue
        return raw
    return None


def append_claim_log(manager_id, deal, operation_key=None):
    return STATE_STORE.append_claim(
        claim_log_entry(manager_id, deal),
        operation_key=operation_key,
    )


def load_reject_log():
    return STATE_STORE.list_rejections()


def load_greeting_log():
    return STATE_STORE.list_greetings()


def append_greeting_log(entry):
    return STATE_STORE.append_greeting(entry)


def latest_greeting_for_deal(deal_id):
    return STATE_STORE.latest_greeting_by_deal(deal_id)


def latest_greeting_for_operation(operation_key):
    return STATE_STORE.latest_greeting_by_operation(operation_key)


def normalize_reject_reason(reason):
    reason = str(reason or "").strip()
    return reason if reason in REJECT_REASONS else "other"


def append_reject_log(manager_id, deal, reason="other"):
    manager = get_manager_profile(manager_id) or {"name": str(manager_id)}
    reason = normalize_reject_reason(reason)
    entry = {
        "timestamp": local_now().isoformat(),
        "managerId": str(manager_id),
        "managerName": manager.get("name") or str(manager_id),
        "dealId": str(deal.get("ID") or deal.get("id") or ""),
        "dealTitle": deal.get("TITLE") or deal.get("title") or "",
        "stageId": deal.get("STAGE_ID") or deal.get("stageId") or "",
        "direction": ((deal.get("classification") or {}).get("direction") if isinstance(deal.get("classification"), dict) else "") or "",
        "reason": reason,
        "reasonLabel": REJECT_REASONS[reason],
    }
    return STATE_STORE.append_reject(entry)


def count_claims(manager_id, date_from=None, date_to=None):
    return STATE_STORE.count_claims(manager_id, date_from, date_to)


def count_claims_in_log(log, manager_id, date_from=None, date_to=None):
    date_from = normalize_date(date_from, local_date())
    date_to = normalize_date(date_to, date_from)
    total = 0
    for entry in log:
        if str(entry.get("managerId")) != str(manager_id):
            continue
        claim_date = entry_date(entry)
        if date_from <= claim_date <= date_to:
            total += 1
    return total


def count_rejections_in_log(log, manager_id, date_from=None, date_to=None):
    date_from = normalize_date(date_from, local_date())
    date_to = normalize_date(date_to, date_from)
    total = 0
    for entry in log:
        if str(entry.get("managerId")) != str(manager_id):
            continue
        reject_date = entry_date(entry)
        if date_from <= reject_date <= date_to:
            total += 1
    return total


def rejection_reason_summary(log, manager_id, date_from=None, date_to=None):
    date_from = normalize_date(date_from, local_date())
    date_to = normalize_date(date_to, date_from)
    counts = {}
    for entry in log:
        if str(entry.get("managerId")) != str(manager_id):
            continue
        reject_date = entry_date(entry)
        if not (date_from <= reject_date <= date_to):
            continue
        reason = normalize_reject_reason(entry.get("reason"))
        counts[reason] = counts.get(reason, 0) + 1
    if not counts:
        return ""
    reason, count = sorted(counts.items(), key=lambda item: (-item[1], REJECT_REASONS[item[0]]))[0]
    return f"{REJECT_REASONS[reason]} ({count})"


def _outbox_request_key(item):
    payload = item.get("payload") if isinstance(item, dict) else {}
    return str((payload or {}).get("requestKey") or "")


def _deliver_integration_outbox_item(item):
    """Deliver one item and return its summary bucket.

    The caller wraps this entire function so a local state-merge failure in a
    409 handler cannot abort the rest of the already selected batch.
    """

    try:
        response = baza_post(item["path"], item["payload"])
        if item["kind"] == "extra_claim_request":
            STATE_STORE.apply_extra_claim_request_response(
                _outbox_request_key(item), response
            )
        STATE_STORE.mark_outbox_delivered(item["id"], response)
        return "sent"
    except BazaIntegrationHttpError as exc:
        error_container = (
            exc.payload.get("data")
            if isinstance(exc.payload.get("data"), dict)
            else exc.payload
        )
        error_code = str(
            error_container.get("code")
            or error_container.get("error")
            or exc.payload.get("code")
            or exc.payload.get("error")
            or ""
        ).strip().lower()
        active_remote_request = (
            error_container.get("request") or exc.payload.get("request")
        )
        active_remote = (
            active_remote_request if isinstance(active_remote_request, dict) else {}
        )
        active_request_payload = item.get("payload") or {}
        active_remote_status = str(active_remote.get("status") or "").lower()
        active_remote_id = str(
            active_remote.get("id") or active_remote.get("requestId") or ""
        )
        active_remote_manager = str(active_remote.get("bitrixUserId") or "")
        active_remote_date = str(active_remote.get("businessDate") or "")
        active_request_exists = bool(
            item.get("kind") == "extra_claim_request"
            and exc.status == 409
            and error_code == "active_request_exists"
            and isinstance(active_remote_request, dict)
            and active_remote_id
            and active_remote_status in {"pending", "approved"}
            and (
                not active_remote_manager
                or active_remote_manager
                == str(active_request_payload.get("bitrixUserId") or "")
            )
            and (
                not active_remote_date
                or active_remote_date
                == str(active_request_payload.get("businessDate") or "")
            )
        )
        if active_request_exists:
            # A retry of the same logical request is safe to adopt.  If this
            # response actually belongs to an older locally consumed request,
            # the state store raises an association conflict.  The outer
            # per-item guard then delays this new request until the linked
            # claim event has reconciled the old remote grant.
            STATE_STORE.apply_extra_claim_request_response(
                _outbox_request_key(item), {"request": active_remote_request}
            )
            STATE_STORE.mark_outbox_delivered(item["id"], exc.payload)
            return "sent"
        if error_code == "replay_detected" or exc.retryable:
            # Every retry is signed with a fresh nonce.  A replay response is
            # therefore transient, while 5xx may mean Baza committed before
            # its response failed; the exact same durable payload must retry.
            STATE_STORE.mark_outbox_failed(item["id"], f"HTTP {exc.status}")
            return "retried"

        safe_reason = (
            "База не смогла принять запрос. Обратитесь к администратору."
            if item.get("kind") == "extra_claim_request"
            else "Baza rejected a claim event integrity check"
        )
        if item.get("kind") == "extra_claim_request":
            STATE_STORE.reject_extra_claim_request_locally(
                _outbox_request_key(item), safe_reason
            )
        # Exact duplicate claim events return 200 from Baza.  Any 409 such as
        # idempotency_conflict or grant_unavailable is integrity evidence and
        # must remain terminal/manual instead of being treated as success.
        STATE_STORE.mark_outbox_dead_letter(item["id"], safe_reason, exc.payload)
        return "dead"


def flush_integration_outbox(limit=None, *, kinds=None, dedupe_key=None):
    """Deliver a bounded durable batch without affecting Bitrix availability."""

    delivery_kinds = enabled_integration_outbox_kinds()
    if kinds is not None:
        delivery_kinds &= {str(kind) for kind in kinds}
    summary = {
        "enabled": bool(delivery_kinds and baza_integration_configured()),
        "sent": 0,
        "retried": 0,
        "dead": 0,
    }
    if not summary["enabled"]:
        return summary
    for item in STATE_STORE.list_due_outbox(
        limit=limit or INTEGRATION_OUTBOX_BATCH_SIZE,
        kinds=delivery_kinds,
        dedupe_key=dedupe_key,
    ):
        try:
            outcome = _deliver_integration_outbox_item(item)
        except Exception as exc:
            # This includes local merge/constraint failures raised while
            # handling an HTTP 409.  One poison item must not prevent a linked
            # claim event or unrelated audit records later in the batch from
            # being delivered.
            try:
                STATE_STORE.mark_outbox_failed(item["id"], type(exc).__name__)
            except Exception as state_exc:
                sys.stderr.write(
                    "Baza integration outbox state update failed: "
                    f"{type(state_exc).__name__}\n"
                )
            outcome = "retried"
        summary[outcome] += 1
    return summary


def integration_outbox_loop():
    while True:
        try:
            if readiness_state().get("ok"):
                summary = flush_integration_outbox()
                if summary["sent"] or summary["retried"] or summary["dead"]:
                    sys.stderr.write(
                        "Baza integration outbox: "
                        + " ".join(f"{key}={value}" for key, value in summary.items())
                        + "\n"
                    )
        except Exception as exc:
            sys.stderr.write(f"Baza integration outbox failed: {type(exc).__name__}\n")
        time.sleep(INTEGRATION_OUTBOX_INTERVAL_SECONDS)


def authoritative_extra_claim_grant_ids(response, manager_id, business_date):
    """Return approved grant IDs from this exact signed Baza response.

    The local store intentionally retains a reserved grant while a CRM write
    is uncertain.  Therefore a local ``grantAvailable`` flag alone cannot
    authorize a new/retried write after an office transfer or remote revoke.
    """

    response = dict(response or {})
    container = response.get("data") if isinstance(response.get("data"), dict) else response
    grants = container.get("grants")
    if grants is None and isinstance(container.get("grant"), dict):
        grants = [container["grant"]]
    if not isinstance(grants, list):
        return set()
    manager_id = str(manager_id or "")
    business_date = str(business_date or "")
    approved = set()
    for raw_grant in grants:
        if not isinstance(raw_grant, dict):
            continue
        grant_id = str(raw_grant.get("id") or raw_grant.get("requestId") or "")
        if (
            grant_id
            and str(raw_grant.get("status") or "approved").strip().lower() == "approved"
            and str(raw_grant.get("bitrixUserId") or manager_id) == manager_id
            and str(raw_grant.get("businessDate") or business_date) == business_date
        ):
            approved.add(grant_id)
    return approved


def refresh_extra_claim_state(manager_id, business_date=None, *, operation_key=None):
    business_date = business_date or local_date()
    response = baza_post(
        "/integrations/deal-picker/v1/grants/query",
        {
            "bitrixUserId": str(manager_id),
            "businessDate": business_date,
        },
    )
    STATE_STORE.import_extra_claim_state(manager_id, business_date, response)
    state = STATE_STORE.get_extra_claim_state(
        manager_id,
        business_date,
        operation_key=operation_key,
    )
    authoritative_ids = authoritative_extra_claim_grant_ids(
        response,
        manager_id,
        business_date,
    )
    local_grant_id = str((state.get("grant") or {}).get("id") or "")
    state["authoritativeGrantAvailable"] = bool(
        local_grant_id and local_grant_id in authoritative_ids
    )
    return state


def extra_claim_limit_state(manager_id, *, refresh=False):
    today = local_date()
    rule = get_manager_rule(manager_id)
    taken_today = count_claims(manager_id, today, today)
    limit = rule.get("dailyLimit")
    bypassed = bool(limit is not None and is_limit_bypassed_now())
    limit_reached = bool(
        limit is not None and not bypassed and taken_today >= int(limit)
    )
    integration_unavailable = False
    local_state = STATE_STORE.get_extra_claim_state(manager_id, today)
    if refresh and limit_reached and EXTRA_CLAIM_REQUESTS_ENABLED:
        if not extra_claim_requests_configured():
            integration_unavailable = True
        else:
            try:
                pending_request = local_state.get("request") or {}
                pending_key = str(pending_request.get("requestKey") or "")
                if pending_key and pending_request.get("status") == "queued":
                    flush_integration_outbox(
                        limit=1,
                        kinds={"extra_claim_request"},
                        dedupe_key=f"extra-claim-request:{pending_key}",
                    )
                refresh_extra_claim_state(manager_id, today)
            except Exception:
                integration_unavailable = True
        local_state = STATE_STORE.get_extra_claim_state(manager_id, today)
    request_state = local_state.get("request")
    return {
        "ok": True,
        "enabled": bool(EXTRA_CLAIM_REQUESTS_ENABLED),
        "configured": bool(extra_claim_requests_configured()),
        "businessDate": today,
        "takenToday": taken_today,
        "dailyLimit": limit,
        "limitBypassed": bypassed,
        "limitReached": limit_reached,
        "request": request_state,
        "grant": local_state.get("grant"),
        "grantAvailable": bool(local_state.get("grantAvailable")),
        "integrationUnavailable": integration_unavailable,
    }


def request_extra_claim(manager_id, reason):
    if not EXTRA_CLAIM_REQUESTS_ENABLED:
        return {
            "ok": False,
            "message": "Запросы дополнительных заявок пока не включены.",
            "_httpStatus": 403,
        }
    if not extra_claim_requests_configured():
        return {
            "ok": False,
            "message": "Связь с Базой ещё не настроена. Обратитесь к администратору.",
            "_httpStatus": 503,
        }
    manager = get_manager_profile(manager_id)
    if (
        not manager
        or manager.get("active") is not True
        or manager.get("intranet") is not True
    ):
        return {
            "ok": False,
            "message": "Запрос доступен только активному сотруднику компании.",
            "_httpStatus": 403,
        }
    configured_rules = STATE_STORE.list_rules()
    if (
        REQUIRE_EXPLICIT_ACCESS_RULE
        and not is_unverified_dev_mode()
        and str(manager_id) not in configured_rules
    ):
        return {
            "ok": False,
            "message": "Администратор ещё не открыл вам доступ к выдаче заявок.",
            "_httpStatus": 403,
        }
    rule = get_manager_rule(manager_id)
    if rule.get("enabled") is False:
        return {
            "ok": False,
            "message": "Для вас закрыт доступ к получению заявок.",
            "_httpStatus": 403,
        }
    reason = str(reason or "").strip()
    if not 10 <= len(reason) <= 500:
        return {
            "ok": False,
            "message": "Напишите причину запроса: от 10 до 500 символов.",
            "_httpStatus": 400,
        }
    state = extra_claim_limit_state(manager_id, refresh=True)
    if not state["limitReached"]:
        return {
            "ok": False,
            "message": "Ваш дневной лимит ещё не закончился или сейчас действует свободное время.",
            "_httpStatus": 409,
        }
    request = STATE_STORE.create_extra_claim_request(
        manager_id,
        state["businessDate"],
        reason,
        taken_today_snapshot=state["takenToday"],
        daily_limit_snapshot=state["dailyLimit"],
    )
    # Best effort only. The durable row/outbox is already committed, so a Baza
    # outage cannot lose the click or affect ordinary claims.
    flush_integration_outbox(
        limit=1,
        kinds={"extra_claim_request"},
        dedupe_key=f"extra-claim-request:{request['requestKey']}",
    )
    state = extra_claim_limit_state(manager_id, refresh=False)
    state["request"] = state.get("request") or request
    request_state = state.get("request") or {}
    request_status = request_state.get("status")
    if request_status == "pending":
        state["message"] = "Запрос отправлен директору офиса."
    elif request_status == "approved":
        state["message"] = "Директор одобрил одну дополнительную заявку."
    elif request_status == "rejected":
        state["message"] = (
            request_state.get("rejectionReason")
            or "Директор отказал в дополнительной заявке."
        )
    else:
        state["message"] = (
            "Запрос сохранён и будет отправлен директору, когда связь с Базой восстановится."
        )
    return state


def check_manager_access(manager_id, allow_operation_key=None):
    configured_rules = STATE_STORE.list_rules()
    if (
        REQUIRE_EXPLICIT_ACCESS_RULE
        and not is_unverified_dev_mode()
        and str(manager_id) not in configured_rules
    ):
        return {
            "ok": False,
            "rule": {"enabled": False, "dailyLimit": None, "note": ""},
            "reason": "Администратор ещё не открыл вам доступ к выдаче заявок.",
        }
    rule = get_manager_rule(manager_id)
    if not rule["enabled"]:
        return {"ok": False, "rule": rule, "reason": "Для вас временно закрыт доступ к получению заявок."}
    unresolved_operations = STATE_STORE.list_unresolved_claim_operations(manager_id)
    if allow_operation_key:
        unresolved_operations = [
            operation
            for operation in unresolved_operations
            if operation.get("operationKey") != str(allow_operation_key)
        ]
    if unresolved_operations:
        return {
            "ok": False,
            "rule": rule,
            "recoveryPending": True,
            "reason": (
                "Предыдущая выдача ещё сверяется с Bitrix24. "
                "Новая заявка станет доступна после безопасной сверки."
            ),
        }
    if rule["dailyLimit"] is not None:
        if is_limit_bypassed_now():
            return {"ok": True, "rule": rule, "limitBypassed": True}
        today = local_date()
        taken_today = count_claims(manager_id, today, today)
        if taken_today >= int(rule["dailyLimit"]):
            extra_state = STATE_STORE.get_extra_claim_state(
                manager_id,
                today,
                operation_key=allow_operation_key,
            )
            if EXTRA_CLAIM_REQUESTS_ENABLED and extra_state.get("grantAvailable"):
                return {
                    "ok": True,
                    "rule": rule,
                    "limitReached": True,
                    "takenToday": taken_today,
                    "dailyLimit": rule["dailyLimit"],
                    "extraClaimRequired": True,
                    "extraClaimGrant": extra_state.get("grant"),
                    "extraClaimRequest": extra_state.get("request"),
                }
            return {
                "ok": False,
                "rule": rule,
                "reason": f"Дневной лимит заявок уже достигнут: {taken_today}/{rule['dailyLimit']}.",
                "limitReached": True,
                "takenToday": taken_today,
                "dailyLimit": rule["dailyLimit"],
                "extraClaimEnabled": bool(EXTRA_CLAIM_REQUESTS_ENABLED),
                "extraClaimRequest": extra_state.get("request"),
                "extraClaimGrantAvailable": False,
            }
    return {"ok": True, "rule": rule}


def parse_competencies(value):
    if isinstance(value, list):
        parts = []
        for item in value:
            parts.extend(parse_competencies(item))
        return parts
    value = str(value or "")
    items = re.split(r"[,;\n\r]+", value)
    return [item.strip() for item in items if item.strip()]


def bitrix_boolean(value, default=False):
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().upper()
    if normalized in {"Y", "YES", "TRUE", "1"}:
        return True
    if normalized in {"N", "NO", "FALSE", "0", ""}:
        return False
    return bool(default)


def is_intranet_user(user):
    departments = user.get("UF_DEPARTMENT") if isinstance(user, dict) else None
    if isinstance(departments, (list, tuple, set)):
        return any(str(item or "").strip() not in {"", "0"} for item in departments)
    return str(departments or "").strip() not in {"", "0"}


def get_manager_profile(manager_id):
    manager_id = str(manager_id or "").strip()
    if not manager_id:
        return None
    local = next((item for item in load_managers() if str(item.get("id")) == manager_id), None)
    try:
        users = bitrix_call("user.get", {"ID": manager_id}, timeout=BITRIX_FAST_TIMEOUT_SECONDS) or []
    except Exception:
        users = []
    if users:
        return manager_profile_from_user(users[0], manager_id)
    if local:
        return {
            "id": manager_id,
            "name": local.get("name") or manager_id,
            "competencies": local.get("competencies") or [],
            "active": local.get("active", False) is True,
            "intranet": is_unverified_dev_mode(),
            "source": "local_fallback",
        }
    return {"id": manager_id, "name": manager_id, "competencies": [], "active": False, "source": "unavailable"}


def manager_profile_from_user(user, manager_id=None):
    manager_id = str(user.get("ID") or manager_id or "")
    competencies = parse_competencies(user.get("UF_SKILLS"))
    # A production profile is authoritative: clearing UF_SKILLS must revoke
    # routing immediately.  The legacy local fallback is available only in the
    # explicitly isolated localhost + DRY_RUN development mode.
    local = next((item for item in load_managers() if str(item.get("id")) == manager_id), None)
    used_local_fallback = bool(
        not competencies
        and local
        and local.get("active", False) is True
        and is_unverified_dev_mode()
    )
    if used_local_fallback:
        competencies = local.get("competencies", [])
    return {
        "id": manager_id,
        "name": " ".join(part for part in [user.get("NAME"), user.get("LAST_NAME")] if part).strip() or manager_id,
        "competencies": competencies,
        "active": bitrix_boolean(user.get("ACTIVE"), default=False),
        "intranet": is_intranet_user(user),
        "source": (
            "local_dev_fallback"
            if used_local_fallback
            else "UF_SKILLS"
            if competencies
            else "empty"
        ),
    }


def read_limited_bitrix_json(response):
    """Decode one bounded JSON object without echoing an upstream body."""

    headers = getattr(response, "headers", None)
    raw_length = headers.get("Content-Length") if headers is not None else None
    if raw_length not in (None, ""):
        try:
            content_length = int(raw_length)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Bitrix вернул некорректный размер ответа") from exc
        if content_length < 0 or content_length > MAX_BITRIX_RESPONSE_BYTES:
            raise RuntimeError("Ответ Bitrix превышает безопасный размер")
    body = response.read(MAX_BITRIX_RESPONSE_BYTES + 1)
    if len(body) > MAX_BITRIX_RESPONSE_BYTES:
        raise RuntimeError("Ответ Bitrix превышает безопасный размер")
    try:
        payload = json.loads(body)
    except (TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("Bitrix вернул некорректный JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Bitrix вернул неожиданный формат ответа")
    return payload


def raise_bitrix_http_error(exc):
    try:
        payload = read_limited_bitrix_json(exc)
    except RuntimeError:
        raise RuntimeError(f"Bitrix HTTP {exc.code}") from exc
    error_code = str(payload.get("error") or "").strip()
    suffix = f": {error_code}" if error_code else ""
    raise RuntimeError(f"Bitrix HTTP {exc.code}{suffix}") from exc


def bitrix_call(method, params=None, timeout=None):
    payload = bitrix_call_full(method, params, timeout=timeout)
    return payload.get("result")


def bitrix_call_full(method, params=None, timeout=None):
    base = load_env()
    url = base + method + ".json"
    data = None
    if params:
        data = urllib.parse.urlencode(params, doseq=True).encode("utf-8")
    try:
        with urllib.request.urlopen(url, data=data, timeout=timeout or BITRIX_TIMEOUT_SECONDS) as response:
            payload = read_limited_bitrix_json(response)
    except urllib.error.HTTPError as exc:
        raise_bitrix_http_error(exc)
    if "error" in payload:
        raise RuntimeError(f"Bitrix API error: {payload.get('error') or 'unknown'}")
    return payload


def bitrix_list_all(method, params=None, max_items=1000, timeout=None):
    items = []
    next_start = 0
    seen_starts = set()
    deadline = time.monotonic() + float(timeout) if timeout is not None else None
    while len(items) < max(0, int(max_items)):
        if next_start in seen_starts:
            raise RuntimeError(f"Bitrix вернул повторяющийся курсор для {method}")
        seen_starts.add(next_start)
        page_params = dict(params or {})
        page_params["start"] = next_start
        page_timeout = None
        if deadline is not None:
            page_timeout = deadline - time.monotonic()
            if page_timeout <= 0:
                raise TimeoutError(f"Истёк общий таймаут списка Bitrix для {method}")
        payload = bitrix_call_full(method, page_params, timeout=page_timeout)
        if deadline is not None and time.monotonic() > deadline:
            raise TimeoutError(f"Истёк общий таймаут списка Bitrix для {method}")
        page = payload.get("result") or []
        if not isinstance(page, list):
            raise RuntimeError(f"Bitrix вернул неожиданный формат списка для {method}")
        remaining = max_items - len(items)
        items.extend(page[:remaining])
        if len(items) >= max_items or payload.get("next") is None:
            break
        try:
            next_start = int(payload.get("next"))
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Bitrix вернул некорректный курсор для {method}") from exc
    return items


class LostDealCloseGuardError(RuntimeError):
    """A safe, expected reason why a failed deal must not close a chat."""

    def __init__(self, code):
        self.code = str(code or "guard_failed")
        super().__init__(self.code)


class LostDealTransitionNotMature(RuntimeError):
    pass


def _safe_bitrix_time(payload, key):
    value = ((payload or {}).get("time") or {}).get(key)
    return parse_source_message_time(value)


def bitrix_stagehistory_page(params):
    payload = bitrix_call_full("crm.stagehistory.list", params)
    result = payload.get("result")
    if not isinstance(result, dict) or not isinstance(result.get("items"), list):
        raise RuntimeError("Bitrix вернул неожиданный формат истории стадий")
    rows = result["items"]
    if any(not isinstance(row, dict) for row in rows):
        raise RuntimeError("Bitrix вернул повреждённую историю стадий")
    return payload, rows


def normalize_deal_category_id(value):
    value = str(value if value is not None else "").strip()
    return value if re.fullmatch(r"\d{1,19}", value) else ""


def deal_stage_entity_id(category_id):
    category_id = normalize_deal_category_id(category_id)
    if not category_id:
        raise RuntimeError("Bitrix вернул некорректную воронку сделки")
    return "DEAL_STAGE" if category_id == "0" else f"DEAL_STAGE_{category_id}"


def load_deal_stage_semantics(category_id, cache=None):
    category_id = normalize_deal_category_id(category_id)
    if not category_id:
        raise RuntimeError("Bitrix вернул некорректную воронку сделки")
    cache = cache if cache is not None else {}
    if category_id in cache:
        return cache[category_id]
    entity_id = deal_stage_entity_id(category_id)
    rows = bitrix_call(
        "crm.status.list",
        {
            "filter[ENTITY_ID]": entity_id,
            "select[]": ["ENTITY_ID", "STATUS_ID", "SEMANTICS"],
        },
    ) or []
    if not isinstance(rows, list) or len(rows) > 500:
        raise RuntimeError("Bitrix вернул неожиданный справочник стадий")
    semantics = {}
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("Bitrix вернул повреждённый справочник стадий")
        if str(row.get("ENTITY_ID") or entity_id).upper() != entity_id:
            continue
        stage_id = str(row.get("STATUS_ID") or "").strip()
        raw_semantic = str(row.get("SEMANTICS") or "").strip().upper()
        semantic = "P" if not raw_semantic else raw_semantic
        if not stage_id or semantic not in {"P", "S", "F"}:
            raise RuntimeError("Bitrix вернул некорректную семантику стадии")
        if stage_id in semantics and semantics[stage_id] != semantic:
            raise RuntimeError("Bitrix вернул неоднозначную семантику стадии")
        semantics[stage_id] = semantic
    if not semantics:
        raise RuntimeError("Bitrix не вернул справочник стадий")
    cache[category_id] = semantics
    return semantics


def semantic_for_stage(category_id, stage_id, cache=None):
    stage_id = str(stage_id or "").strip()
    semantic = load_deal_stage_semantics(category_id, cache).get(stage_id)
    if semantic not in {"P", "S", "F"}:
        raise RuntimeError("Стадия сделки отсутствует в справочнике Bitrix")
    return semantic


def read_deal_stage_history(deal_id):
    payload, rows = bitrix_stagehistory_page(
        {
            "entityTypeId": 2,
            "order[ID]": "DESC",
            "filter[OWNER_ID]": str(deal_id),
            "select[]": [
                "ID", "OWNER_ID", "CATEGORY_ID", "STAGE_ID", "CREATED_TIME"
            ],
            "start": 0,
        }
    )
    normalized = []
    for row in rows:
        row_id = normalize_entity_id(row.get("ID"))
        owner_id = normalize_entity_id(row.get("OWNER_ID"))
        category_id = normalize_deal_category_id(row.get("CATEGORY_ID"))
        stage_id = str(row.get("STAGE_ID") or "").strip()
        created = parse_source_message_time(row.get("CREATED_TIME"))
        if not row_id or owner_id != str(deal_id) or not category_id or not stage_id or not created:
            raise RuntimeError("Bitrix вернул неполную историю стадий")
        normalized.append(
            {
                "id": row_id,
                "dealId": owner_id,
                "categoryId": category_id,
                "stageId": stage_id,
                "createdAt": created,
            }
        )
    normalized.sort(key=lambda item: int(item["id"]), reverse=True)
    return payload, normalized


def exact_failed_transition(deal_id, expected_transition_id):
    deal_id = normalize_entity_id(deal_id)
    expected_transition_id = normalize_entity_id(expected_transition_id)
    if not deal_id or not expected_transition_id:
        raise LostDealCloseGuardError("invalid_transition_identity")
    deal = bitrix_call("crm.deal.get", {"id": deal_id}) or {}
    if not isinstance(deal, dict) or normalize_entity_id(deal.get("ID")) != deal_id:
        raise RuntimeError("Bitrix не вернул сделку для проверки стадии")
    category_id = normalize_deal_category_id(deal.get("CATEGORY_ID"))
    stage_id = str(deal.get("STAGE_ID") or "").strip()
    live_semantic = str(deal.get("STAGE_SEMANTIC_ID") or "").strip().upper()
    closed = str(deal.get("CLOSED") or "").strip().upper()
    moved_time = parse_source_message_time(deal.get("MOVED_TIME"))
    if not category_id or not stage_id or live_semantic not in {"P", "S", "F"}:
        raise RuntimeError("Bitrix не вернул полную текущую стадию сделки")
    if live_semantic != "F":
        raise LostDealCloseGuardError("deal_no_longer_failed")
    if closed != "Y":
        raise LostDealCloseGuardError("deal_not_closed")
    _, history = read_deal_stage_history(deal_id)
    if len(history) < 2:
        raise LostDealCloseGuardError("stage_history_incomplete")
    latest, previous = history[0], history[1]
    if latest["id"] != expected_transition_id:
        raise LostDealCloseGuardError("transition_not_latest")
    if latest["categoryId"] != category_id or latest["stageId"] != stage_id:
        raise LostDealCloseGuardError("history_not_current_stage")
    if moved_time is None or abs((latest["createdAt"] - moved_time).total_seconds()) > LOST_DEAL_HISTORY_MOVE_TOLERANCE_SECONDS:
        raise LostDealCloseGuardError("history_move_time_mismatch")
    semantic_cache = {}
    mapped_live = semantic_for_stage(category_id, stage_id, semantic_cache)
    from_semantic = semantic_for_stage(
        previous["categoryId"], previous["stageId"], semantic_cache
    )
    if mapped_live != "F" or mapped_live != live_semantic:
        raise LostDealCloseGuardError("current_semantic_mismatch")
    if from_semantic not in {"P", "S"}:
        raise LostDealCloseGuardError("not_non_f_to_f")
    return {
        "transitionId": latest["id"],
        "dealId": deal_id,
        "fromSemantic": from_semantic,
        "toSemantic": "F",
        "fromCategoryId": previous["categoryId"],
        "toCategoryId": latest["categoryId"],
        "fromStageId": previous["stageId"],
        "toStageId": latest["stageId"],
        "transitionTime": latest["createdAt"].isoformat(),
    }


def active_chat_rows_for_deal(deal_id):
    value = bitrix_call(
        "imopenlines.crm.chat.get",
        {"CRM_ENTITY_TYPE": "DEAL", "CRM_ENTITY": str(deal_id), "ACTIVE_ONLY": "Y"},
    )
    values = value if isinstance(value, list) else list(value.values()) if isinstance(value, dict) else None
    if values is None or any(not isinstance(item, dict) for item in values):
        raise RuntimeError("Bitrix вернул неожиданный список активных чатов")
    rows = []
    for item in values:
        chat_id = normalize_entity_id(item.get("CHAT_ID"))
        if not chat_id:
            raise RuntimeError("Bitrix вернул активный чат без ID")
        rows.append({"chatId": chat_id})
    return rows


def _history_identity(parameter, identifier):
    history = bitrix_call(
        "imopenlines.session.history.get",
        {parameter: str(identifier)},
    ) or {}
    if not isinstance(history, dict):
        raise RuntimeError("Bitrix вернул неожиданный формат сессии")
    chat_id = normalize_entity_id(history.get("chatId") or history.get("CHAT_ID"))
    session_id = normalize_entity_id(history.get("sessionId") or history.get("SESSION_ID"))
    messages = history.get("message") or {}
    if not isinstance(messages, (dict, list)):
        raise RuntimeError("Bitrix вернул неожиданный формат сообщений сессии")
    if len(messages) > MAX_OPENLINE_MESSAGES_PER_SESSION:
        raise RuntimeError("История открытой линии превышает безопасный лимит")
    entries = list(messages.items()) if isinstance(messages, dict) else list(enumerate(messages))
    identity_rows = []
    for fallback_id, message in entries:
        if not isinstance(message, dict):
            raise RuntimeError("Bitrix вернул повреждённое сообщение сессии")
        message_id = normalize_entity_id(
            message.get("id") or message.get("ID") or fallback_id
        )
        message_time = parse_source_message_time(
            message.get("date") or message.get("DATE")
        )
        if not message_id or message_time is None:
            raise RuntimeError("Bitrix не дал надёжную идентичность истории сессии")
        sender_id = str(
            message.get("senderid") or message.get("senderId") or message.get("SENDER_ID") or ""
        ).strip()
        identity_rows.append((int(message_id), sender_id, message_time.isoformat()))
    identity_rows.sort()
    identity_material = json.dumps(identity_rows, separators=(",", ":"))
    return {
        "chatId": chat_id,
        "sessionId": session_id,
        "messageCount": len(messages),
        "lastMessageId": str(identity_rows[-1][0]) if identity_rows else "",
        "latestMessageAt": (
            max(parse_source_message_time(item[2]) for item in identity_rows)
            if identity_rows
            else None
        ),
        "historySignature": hashlib.sha256(identity_material.encode("utf-8")).hexdigest(),
    }


def _entity_data_session_id(value):
    parts = [str(item).strip() for item in str(value or "").split("|")]
    return normalize_entity_id(parts[5]) if len(parts) > 5 else ""


def _entity_data_has_closed_session_slot(value, deal_id):
    """Require Bitrix's observed, well-formed post-finish session marker."""

    parts = [str(item).strip() for item in str(value or "").split("|")]
    return (
        len(parts) == 10
        and parts[0].upper() == "Y"
        and parts[1].upper() == "DEAL"
        and normalize_entity_id(parts[2]) == normalize_entity_id(deal_id)
        and parts[5] == "0"
    )


def openline_session_activities(session_id):
    payload = bitrix_call_full(
        "crm.activity.list",
        {
            "order[ID]": "ASC",
            "filter[OWNER_TYPE_ID]": 2,
            "filter[PROVIDER_ID]": "IMOPENLINES_SESSION",
            "filter[ASSOCIATED_ENTITY_ID]": str(session_id),
            "select[]": [
                "ID", "OWNER_TYPE_ID", "OWNER_ID", "PROVIDER_ID",
                "ASSOCIATED_ENTITY_ID", "COMPLETED", "STATUS",
            ],
            "start": 0,
        },
    )
    rows = payload.get("result") or []
    try:
        total = int(payload["total"]) if "total" in payload else len(rows)
    except (TypeError, ValueError):
        raise RuntimeError("Bitrix вернул неполный реестр активной сессии")
    if (
        not isinstance(rows, list)
        or total < 0
        or total > 50
        or total != len(rows)
        or payload.get("next") not in (None, "")
    ):
        raise RuntimeError("Bitrix вернул неполный реестр активной сессии")
    if any(not isinstance(row, dict) for row in rows):
        raise RuntimeError("Bitrix вернул повреждённый реестр активной сессии")
    return rows


def deal_openline_activities(deal_id):
    payload = bitrix_call_full(
        "crm.activity.list",
        {
            "order[ID]": "ASC",
            "filter[OWNER_TYPE_ID]": 2,
            "filter[OWNER_ID]": str(deal_id),
            "filter[PROVIDER_ID]": "IMOPENLINES_SESSION",
            "select[]": [
                "ID", "OWNER_TYPE_ID", "OWNER_ID", "PROVIDER_ID",
                "ASSOCIATED_ENTITY_ID", "COMPLETED", "STATUS",
                "CREATED", "LAST_UPDATED",
            ],
            "start": 0,
        },
    )
    rows = payload.get("result") or []
    try:
        total = int(payload["total"]) if "total" in payload else len(rows)
    except (TypeError, ValueError):
        raise RuntimeError("Bitrix вернул неполный реестр чатов сделки")
    if (
        not isinstance(rows, list)
        or total < 0
        or total > 50
        or total != len(rows)
        or payload.get("next") not in (None, "")
    ):
        raise RuntimeError("Bitrix вернул неполный реестр чатов сделки")
    if any(not isinstance(row, dict) for row in rows):
        raise RuntimeError("Bitrix вернул повреждённый реестр чатов сделки")
    return rows


def auto_completed_chat_candidate(deal_id, transition):
    transition_time = parse_source_message_time(transition.get("transitionTime"))
    if transition_time is None or str(transition.get("dealId") or "") != str(deal_id):
        raise LostDealCloseGuardError("invalid_fallback_transition")
    parsed_rows = []
    candidates = []
    for row in deal_openline_activities(deal_id):
        activity_id = normalize_entity_id(row.get("ID"))
        session_id = normalize_entity_id(row.get("ASSOCIATED_ENTITY_ID"))
        owner_id = normalize_entity_id(row.get("OWNER_ID"))
        created_at = parse_source_message_time(row.get("CREATED"))
        updated_at = parse_source_message_time(row.get("LAST_UPDATED"))
        if not activity_id or not session_id or owner_id != str(deal_id):
            raise RuntimeError("Bitrix вернул неполную активность чата сделки")
        if str(row.get("OWNER_TYPE_ID") or "") != "2" or str(
            row.get("PROVIDER_ID") or ""
        ) != "IMOPENLINES_SESSION":
            raise RuntimeError("Bitrix вернул чужую активность чата сделки")
        if created_at is None or updated_at is None:
            raise RuntimeError("Bitrix не дал время активности чата сделки")
        completed = str(row.get("COMPLETED") or "").upper()
        status = str(row.get("STATUS") or "")
        if (completed, status) not in {("N", "1"), ("Y", "2"), ("Y", "3")}:
            raise LostDealCloseGuardError("fallback_activity_state_unknown")
        parsed = {
            "activityId": activity_id,
            "sessionId": session_id,
            "completed": completed,
            "status": status,
            "createdAt": created_at,
            "updatedAt": updated_at,
        }
        parsed_rows.append(parsed)
        if (
            completed == "Y"
            and status == "3"
            and created_at <= transition_time
            and abs((updated_at - transition_time).total_seconds())
            <= LOST_DEAL_HISTORY_MOVE_TOLERANCE_SECONDS
        ):
            history = _history_identity("SESSION_ID", session_id)
            chat_id = history.get("chatId")
            if history.get("sessionId") != session_id or not chat_id:
                raise LostDealCloseGuardError("fallback_history_identity_mismatch")
            candidates.append(
                {
                    "activityId": activity_id,
                    "sessionId": session_id,
                    "chatId": chat_id,
                    "activityCompleted": completed,
                    "activityStatus": status,
                    "activityCreatedAt": created_at.isoformat(),
                    "activityUpdatedAt": updated_at.isoformat(),
                }
            )
    if not candidates:
        raise LostDealCloseGuardError("no_active_chat")
    if len(candidates) != 1:
        raise LostDealCloseGuardError("fallback_activity_not_unique")
    candidate = candidates[0]
    candidate_created_at = parse_source_message_time(candidate["activityCreatedAt"])
    ambiguity_boundary = min(
        candidate_created_at,
        transition_time
        - timedelta(seconds=LOST_DEAL_HISTORY_MOVE_TOLERANCE_SECONDS),
    )
    for row in parsed_rows:
        if row["activityId"] == candidate["activityId"]:
            continue
        if (
            (row["completed"], row["status"]) == ("N", "1")
            or row["createdAt"] >= transition_time
            or row["updatedAt"] >= ambiguity_boundary
        ):
            raise LostDealCloseGuardError("fallback_activity_ambiguous")
    return candidate


def exact_openline_activity(deal_id, session_id):
    rows = openline_session_activities(session_id)
    if len(rows) != 1:
        raise LostDealCloseGuardError("session_activity_not_unique")
    row = rows[0]
    activity_id = normalize_entity_id(row.get("ID"))
    completed = str(row.get("COMPLETED") or "").upper()
    status = str(row.get("STATUS") or "")
    if not (
        activity_id
        and str(row.get("OWNER_TYPE_ID") or "") == "2"
        and normalize_entity_id(row.get("OWNER_ID")) == str(deal_id)
        and str(row.get("PROVIDER_ID") or "") == "IMOPENLINES_SESSION"
        and normalize_entity_id(row.get("ASSOCIATED_ENTITY_ID")) == str(session_id)
        and (completed, status) in {("N", "1"), ("Y", "2"), ("Y", "3")}
    ):
        raise LostDealCloseGuardError("session_activity_binding_mismatch")
    return activity_id


def read_single_active_deal_chat_snapshot(deal_id, transition=None):
    rows = active_chat_rows_for_deal(deal_id)
    if len(rows) != 1:
        if rows:
            raise LostDealCloseGuardError("multiple_active_chats")
        if transition is None:
            raise LostDealCloseGuardError("no_active_chat")
        fallback = auto_completed_chat_candidate(deal_id, transition)
        chat_id = fallback["chatId"]
        expected_registry = []
    else:
        fallback = None
        chat_id = rows[0]["chatId"]
        expected_registry = [{"chatId": chat_id}]
    dialog = bitrix_call("imopenlines.dialog.get", {"CHAT_ID": chat_id}) or {}
    if not isinstance(dialog, dict):
        raise RuntimeError("Bitrix вернул неожиданный формат диалога")
    if normalize_entity_id(dialog.get("id")) != chat_id:
        raise LostDealCloseGuardError("dialog_id_mismatch")
    if str(dialog.get("type") or "").strip().lower() != "lines":
        raise LostDealCloseGuardError("dialog_not_openline")
    entity_data_2 = str(dialog.get("entity_data_2") or "")
    if not openline_chat_is_bound_to_deal(entity_data_2, deal_id):
        raise LostDealCloseGuardError("dialog_deal_binding_mismatch")
    by_chat = _history_identity("CHAT_ID", chat_id)
    session_id = by_chat["sessionId"]
    if by_chat["chatId"] != chat_id or not session_id:
        raise LostDealCloseGuardError("chat_history_identity_mismatch")
    by_session = _history_identity("SESSION_ID", session_id)
    if by_session["chatId"] != chat_id or by_session["sessionId"] != session_id:
        raise LostDealCloseGuardError("session_history_identity_mismatch")
    for key in (
        "messageCount", "lastMessageId", "latestMessageAt", "historySignature"
    ):
        if by_session[key] != by_chat[key]:
            raise LostDealCloseGuardError("history_changed_during_check")
    embedded_session = _entity_data_session_id(dialog.get("entity_data_1"))
    if fallback and embedded_session != session_id:
        raise LostDealCloseGuardError("fallback_session_not_current")
    if not fallback and embedded_session and embedded_session != session_id:
        raise LostDealCloseGuardError("dialog_session_mismatch")
    last_message_id = normalize_entity_id(dialog.get("last_message_id"))
    if (
        not last_message_id
        or not by_chat["lastMessageId"]
        or last_message_id != by_chat["lastMessageId"]
    ):
        raise LostDealCloseGuardError("dialog_history_message_mismatch")
    activity_id = exact_openline_activity(deal_id, session_id)
    if fallback and (
        fallback["sessionId"] != session_id
        or fallback["activityId"] != activity_id
    ):
        raise LostDealCloseGuardError("fallback_activity_binding_mismatch")
    if active_chat_rows_for_deal(deal_id) != expected_registry:
        raise LostDealCloseGuardError("active_chat_changed_during_check")
    if fallback and auto_completed_chat_candidate(deal_id, transition) != fallback:
        raise LostDealCloseGuardError("fallback_activity_changed_during_check")
    late_by_chat = _history_identity("CHAT_ID", chat_id)
    late_by_session = _history_identity("SESSION_ID", session_id)
    if late_by_chat != by_chat or late_by_session != by_session:
        raise LostDealCloseGuardError("history_changed_during_check")
    late_dialog = bitrix_call("imopenlines.dialog.get", {"CHAT_ID": chat_id}) or {}
    if not isinstance(late_dialog, dict):
        raise RuntimeError("Bitrix вернул неожиданный формат финального диалога")
    guarded_dialog_fields = (
        "id", "type", "entity_data_1", "entity_data_2", "last_message_id",
        "unread_id", "counter", "user_counter", "is_new",
    )
    if any(late_dialog.get(key) != dialog.get(key) for key in guarded_dialog_fields):
        raise LostDealCloseGuardError("dialog_changed_during_check")
    return {
        "chatId": chat_id,
        "sessionId": session_id,
        "lastMessageId": last_message_id,
        "historyMessageCount": by_chat["messageCount"],
        "latestMessageAt": (
            by_chat["latestMessageAt"].isoformat() if by_chat["latestMessageAt"] else ""
        ),
        "historySignature": by_chat["historySignature"],
        "activityId": activity_id,
        "chatLookupMode": "activity_fallback" if fallback else "active_registry",
        "activityUpdatedAt": fallback["activityUpdatedAt"] if fallback else "",
        "fallbackActivityCompleted": (
            fallback["activityCompleted"] if fallback else ""
        ),
        "fallbackActivityStatus": fallback["activityStatus"] if fallback else "",
        "fallbackActivityCreatedAt": fallback["activityCreatedAt"] if fallback else "",
        "unreadId": str(dialog.get("unread_id") or "0"),
        "counter": int(dialog.get("counter") or 0),
        "userCounter": int(dialog.get("user_counter") or 0),
        "isNew": bool(
            dialog.get("is_new") is True
            or str(dialog.get("is_new") or "").upper() in {"Y", "1", "TRUE"}
        ),
        "entityData1": str(dialog.get("entity_data_1") or ""),
        "entityData2": entity_data_2,
    }


def _same_transition(left, right):
    keys = (
        "transitionId", "dealId", "fromSemantic", "toSemantic",
        "fromCategoryId", "toCategoryId", "fromStageId", "toStageId",
        "transitionTime",
    )
    return all(str(left.get(key)) == str(right.get(key)) for key in keys)


def selected_chat_is_confirmed_inactive(operation):
    chat_id = str(operation.get("chatId") or "")
    session_id = str(operation.get("sessionId") or "")
    if not chat_id or not session_id:
        return False
    if chat_id in {row["chatId"] for row in active_chat_rows_for_deal(operation["dealId"])}:
        return False
    history = _history_identity("SESSION_ID", session_id)
    if history["sessionId"] != session_id or history["chatId"] != chat_id:
        return False
    dialog = bitrix_call("imopenlines.dialog.get", {"CHAT_ID": chat_id}) or {}
    if not (
        isinstance(dialog, dict)
        and normalize_entity_id(dialog.get("id")) == chat_id
        and str(dialog.get("type") or "").strip().lower() == "lines"
        and openline_chat_is_bound_to_deal(
            str(dialog.get("entity_data_2") or ""), operation.get("dealId")
        )
        and _entity_data_has_closed_session_slot(
            dialog.get("entity_data_1"), operation.get("dealId")
        )
    ):
        return False
    activity_id = str(operation.get("activityId") or "")
    activity = bitrix_call("crm.activity.get", {"id": activity_id}) or {}
    if not isinstance(activity, dict):
        return False
    if not (
        normalize_entity_id(activity.get("ID")) == activity_id
        and str(activity.get("OWNER_TYPE_ID") or "") == "2"
        and normalize_entity_id(activity.get("OWNER_ID")) == str(operation.get("dealId"))
        and str(activity.get("PROVIDER_ID") or "") == "IMOPENLINES_SESSION"
        and normalize_entity_id(activity.get("ASSOCIATED_ENTITY_ID")) == session_id
        and str(activity.get("COMPLETED") or "").upper() == "Y"
        and str(activity.get("STATUS") or "") in {"2", "3"}
    ):
        return False
    rows = openline_session_activities(session_id)
    if len(rows) != 1:
        return False
    listed = rows[0]
    return (
        normalize_entity_id(listed.get("ID")) == activity_id
        and str(listed.get("OWNER_TYPE_ID") or "") == "2"
        and normalize_entity_id(listed.get("OWNER_ID")) == str(operation.get("dealId"))
        and str(listed.get("PROVIDER_ID") or "") == "IMOPENLINES_SESSION"
        and normalize_entity_id(listed.get("ASSOCIATED_ENTITY_ID")) == session_id
        and str(listed.get("COMPLETED") or "").upper() == "Y"
        and str(listed.get("STATUS") or "") in {"2", "3"}
    )


def reconcile_lost_deal_close_operations():
    reconciled = 0
    for operation in STATE_STORE.list_lost_deal_close_reconciliation(limit=100):
        # For the incoming-chat fallback, ACTIVE_ONLY=Y and STATUS=3 were
        # already absent/completed before finish.  A later chat rebind is not
        # causal proof that our call succeeded, so an ambiguous result stays
        # uncertain forever and is never retried or auto-reconciled.
        if operation.get("chatLookupMode") == "activity_fallback":
            continue
        try:
            if selected_chat_is_confirmed_inactive(operation):
                STATE_STORE.mark_lost_deal_close_reconciled(operation["transitionId"])
                reconciled += 1
        except Exception:
            continue
    return reconciled


def process_lost_deal_transition(deal_id, transition_id, observed_at=None):
    existing = STATE_STORE.get_lost_deal_close_operation(transition_id)
    if existing and existing.get("status") not in {"checking", "retryable"}:
        return existing.get("status") or "already_processed"
    transition = exact_failed_transition(deal_id, transition_id)
    transition_time = parse_source_message_time(transition["transitionTime"])
    observed_at = observed_at or datetime.now(timezone.utc)
    if transition_time is None or (
        observed_at - transition_time
    ).total_seconds() < LOST_DEAL_AUTOCLOSE_GRACE_SECONDS:
        raise LostDealTransitionNotMature("transition_grace_period")
    claim = STATE_STORE.claim_lost_deal_close_transition(
        transition,
        lease_seconds=LOST_DEAL_CLOSE_LEASE_SECONDS,
    )
    if not claim.get("claimed"):
        if claim.get("status") in {"checking", "retryable"}:
            raise LostDealTransitionNotMature("transition_check_in_progress")
        return claim.get("status") or "already_processed"
    lease_token = claim["leaseToken"]
    try:
        first_snapshot = read_single_active_deal_chat_snapshot(deal_id, transition)
        latest_message_at = parse_source_message_time(first_snapshot["latestMessageAt"])
        # Bitrix message timestamps can have only second precision.  Equality
        # is therefore ambiguous: the message may have arrived just after the
        # stage change but been rounded to the same second.  Fail closed.
        if latest_message_at is not None and latest_message_at >= transition_time:
            raise LostDealCloseGuardError("message_after_failed_transition")
        final_transition = exact_failed_transition(deal_id, transition_id)
        if not _same_transition(transition, final_transition):
            raise LostDealCloseGuardError("transition_changed_before_finish")
        final_snapshot = read_single_active_deal_chat_snapshot(
            deal_id, final_transition
        )
        if final_snapshot != first_snapshot:
            raise LostDealCloseGuardError("chat_changed_before_finish")
        if DRY_RUN:
            STATE_STORE.finalize_lost_deal_close_check(
                transition_id,
                lease_token,
                status="dry_run",
                outcome_code="dry_run_verified",
                chat_id=final_snapshot["chatId"],
            )
            return "dry_run"
        dispatch = STATE_STORE.mark_lost_deal_close_dispatching(
            transition_id,
            lease_token,
            final_snapshot["chatId"],
            final_snapshot["sessionId"],
            final_snapshot["lastMessageId"],
            final_snapshot["historyMessageCount"],
            final_snapshot["historySignature"],
            final_snapshot["activityId"],
            final_snapshot["chatLookupMode"],
            final_snapshot["activityUpdatedAt"],
        )
    except LostDealCloseGuardError as exc:
        STATE_STORE.finalize_lost_deal_close_check(
            transition_id,
            lease_token,
            status="skipped",
            outcome_code=exc.code,
        )
        return exc.code
    except Exception:
        if int(claim.get("attemptCount") or 1) >= LOST_DEAL_MAX_PREFLIGHT_ATTEMPTS:
            STATE_STORE.finalize_lost_deal_close_check(
                transition_id,
                lease_token,
                status="skipped",
                outcome_code="pre_dispatch_attempts_exhausted",
            )
            return "pre_dispatch_attempts_exhausted"
        STATE_STORE.mark_lost_deal_close_retryable(
            transition_id, lease_token, "pre_dispatch_read_failed"
        )
        raise

    try:
        acknowledged = bitrix_call(
            "imopenlines.operator.another.finish",
            {"CHAT_ID": dispatch["chatId"]},
        ) is True
    except Exception:
        STATE_STORE.mark_lost_deal_close_uncertain(
            transition_id, "finish_result_uncertain"
        )
        operation = STATE_STORE.get_lost_deal_close_operation(transition_id)
        if operation.get("chatLookupMode") != "activity_fallback":
            try:
                if selected_chat_is_confirmed_inactive(operation):
                    STATE_STORE.mark_lost_deal_close_reconciled(transition_id)
                    return "closed_reconciled"
            except Exception:
                pass
        return "uncertain"
    if not acknowledged:
        STATE_STORE.mark_lost_deal_close_uncertain(
            transition_id, "finish_not_acknowledged"
        )
        return "uncertain"
    operation = STATE_STORE.get_lost_deal_close_operation(transition_id)
    try:
        confirmed = selected_chat_is_confirmed_inactive(operation)
    except Exception:
        confirmed = False
    if confirmed:
        STATE_STORE.mark_lost_deal_close_closed(transition_id)
        return "closed"
    STATE_STORE.mark_lost_deal_close_uncertain(
        transition_id, "finish_ack_unconfirmed"
    )
    return "uncertain"


def arm_lost_deal_autoclose_from_bitrix():
    payload, rows = bitrix_stagehistory_page(
        {
            "entityTypeId": 2,
            "order[ID]": "DESC",
            "select[]": ["ID", "CREATED_TIME"],
            "start": 0,
        }
    )
    valid_ids = [
        int(row_id)
        for row in rows
        if (row_id := normalize_entity_id(row.get("ID")))
    ]
    if not valid_ids:
        raise RuntimeError("Bitrix не дал удалённую границу истории стадий")
    baseline_id = max(valid_ids)
    remote_time = _safe_bitrix_time(payload, "date_finish")
    if remote_time is None:
        raise RuntimeError("Bitrix не дал серверное время для безопасной активации")
    return STATE_STORE.arm_lost_deal_autoclose(remote_time, baseline_id)


def poll_lost_deal_autoclose_once():
    boundary = STATE_STORE.get_lost_deal_autoclose_boundary()
    if boundary is None:
        arm_lost_deal_autoclose_from_bitrix()
        return {"armed": True, "processed": 0, "reconciled": 0}
    reconciled = reconcile_lost_deal_close_operations()
    start = 0
    scanned = 0
    processed = 0
    had_errors = False
    max_scanned_history_id = int(boundary["scanAfterHistoryId"])
    semantic_cache = {}
    while True:
        payload, rows = bitrix_stagehistory_page(
            {
                "entityTypeId": 2,
                "order[ID]": "ASC",
                "filter[>ID]": int(boundary["scanAfterHistoryId"]),
                "select[]": ["ID", "OWNER_ID", "CATEGORY_ID", "STAGE_ID", "CREATED_TIME"],
                "start": start,
            }
        )
        observed_at = _safe_bitrix_time(payload, "date_finish")
        if observed_at is None:
            raise RuntimeError("Bitrix не дал серверное время опроса")
        scanned += len(rows)
        if scanned > MAX_LOST_DEAL_STAGE_HISTORY_ROWS:
            raise RuntimeError("История стадий превысила безопасный лимит опроса")
        for row in rows:
            transition_id = normalize_entity_id(row.get("ID"))
            deal_id = normalize_entity_id(row.get("OWNER_ID"))
            category_id = normalize_deal_category_id(row.get("CATEGORY_ID"))
            stage_id = str(row.get("STAGE_ID") or "").strip()
            if not transition_id or not deal_id or not category_id or not stage_id:
                had_errors = True
                continue
            max_scanned_history_id = max(max_scanned_history_id, int(transition_id))
            if int(transition_id) <= int(boundary["baselineHistoryId"]):
                continue
            try:
                if semantic_for_stage(category_id, stage_id, semantic_cache) != "F":
                    continue
                process_lost_deal_transition(deal_id, transition_id, observed_at)
                processed += 1
            except LostDealCloseGuardError:
                # The row itself was read successfully; a later/current state
                # proved it is not an eligible exact P/S -> F transition.
                processed += 1
            except LostDealTransitionNotMature:
                had_errors = True
            except Exception:
                had_errors = True
        next_value = payload.get("next")
        if next_value is None:
            break
        try:
            start = int(next_value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Bitrix вернул некорректный курсор истории стадий") from exc
    if not had_errors:
        STATE_STORE.advance_lost_deal_autoclose_history_id(max_scanned_history_id)
    return {
        "armed": False,
        "processed": processed,
        "reconciled": reconciled,
        "scanAdvanced": bool(not had_errors),
    }


def lost_deal_autoclose_loop():
    while True:
        try:
            if LOST_DEAL_AUTOCLOSE_ENABLED and readiness_state().get("ok"):
                poll_lost_deal_autoclose_once()
        except Exception as exc:
            sys.stderr.write(f"Lost-deal auto-close worker error: {type(exc).__name__}\n")
        LOST_DEAL_AUTOCLOSE_WAKE_EVENT.wait(LOST_DEAL_AUTOCLOSE_POLL_SECONDS)
        LOST_DEAL_AUTOCLOSE_WAKE_EVENT.clear()


def bitrix_batch(commands):
    if not commands:
        return {}
    params = {"halt": 0}
    for key, command in commands.items():
        params[f"cmd[{key}]"] = command
    result = bitrix_call("batch", params) or {}
    return result.get("result") or {}


def get_manager_profiles_bulk(users):
    profiles = {}
    ids = [str(user.get("ID") or "") for user in users if user.get("ID")]
    for index in range(0, len(ids), 50):
        chunk = ids[index:index + 50]
        commands = {f"u{manager_id}": f"user.get?ID={urllib.parse.quote(manager_id)}" for manager_id in chunk}
        try:
            result = bitrix_batch(commands)
        except Exception:
            result = {}
        for manager_id in chunk:
            data = result.get(f"u{manager_id}") or []
            if data:
                profiles[manager_id] = manager_profile_from_user(data[0], manager_id)
    return profiles


def bitrix_oauth_call(domain, token, method, params=None, timeout=None):
    if not token:
        raise RuntimeError("Нет авторизации Битрикса")
    domain = normalize_bitrix_domain(domain)
    url = f"https://{domain}/rest/{method}.json"
    payload = dict(params or {})
    payload["auth"] = token
    data = urllib.parse.urlencode(payload, doseq=True).encode("utf-8")
    try:
        with urllib.request.urlopen(url, data=data, timeout=timeout or BITRIX_TIMEOUT_SECONDS) as response:
            result = read_limited_bitrix_json(response)
    except urllib.error.HTTPError as exc:
        raise_bitrix_http_error(exc)
    if "error" in result:
        raise RuntimeError(f"Bitrix API error: {result.get('error') or 'unknown'}")
    return result.get("result")


def bitrix_call_for_actor(auth, method, params=None):
    token, domain = extract_auth_credentials(auth)
    if not token or not domain:
        raise PermissionError("Нет подтверждённой пользовательской авторизации Bitrix24")
    return bitrix_oauth_call(domain, token, method, params)


def extract_auth_credentials(auth):
    if not isinstance(auth, dict):
        return None, None
    token = auth.get("access_token") or auth.get("AUTH_ID") or auth.get("auth")
    domain = auth.get("domain") or auth.get("DOMAIN")
    if not domain and auth.get("client_endpoint"):
        parsed = safe_urlparse(auth.get("client_endpoint"))
        try:
            domain = parsed.hostname if parsed is not None else None
        except (TypeError, ValueError):
            domain = None
    if not token or not domain:
        return None, None
    try:
        domain = normalize_bitrix_domain(domain)
    except PermissionError:
        return None, None
    return str(token), domain


def has_bitrix_auth(payload):
    auth = payload.get("auth") if isinstance(payload, dict) else None
    token, domain = extract_auth_credentials(auth)
    return bool(token and domain)


def verify_bitrix_user(auth, allow_cached=True):
    token, domain = extract_auth_credentials(auth)
    if not token or not domain:
        return None
    cache_key = hashlib.sha256(f"{domain}\0{token}".encode("utf-8")).hexdigest()
    now = time.monotonic()
    if allow_cached:
        with USER_VERIFY_CACHE_LOCK:
            cached = USER_VERIFY_CACHE.get(cache_key)
            if cached and now - cached["cachedAt"] < USER_VERIFY_CACHE_TTL_SECONDS:
                return dict(cached["user"])
    try:
        user = bitrix_oauth_call(domain, token, "user.current", timeout=BITRIX_FAST_TIMEOUT_SECONDS)
    except Exception:
        try:
            user = bitrix_oauth_call(domain, token, "profile", timeout=BITRIX_FAST_TIMEOUT_SECONDS)
        except Exception:
            return None
    if not user:
        return None
    verified = {
        "id": str(user.get("ID") or ""),
        "name": " ".join(part for part in [user.get("NAME"), user.get("LAST_NAME")] if part).strip(),
        "raw": user,
    }
    if not verified["id"]:
        return None
    with USER_VERIFY_CACHE_LOCK:
        bounded_cache_put(USER_VERIFY_CACHE, cache_key, {"cachedAt": now, "user": verified})
    return dict(verified)


def cached_verified_actor_auth(auth, manager_id):
    """Return a minimal in-memory OAuth copy only for the verified actor.

    ``/api/claim`` verifies this token immediately before the claim.  This
    helper deliberately performs no network request, and the returned token
    is passed only to the short-lived daemon sender (never SQLite or logs).
    """

    token, domain = extract_auth_credentials(auth)
    manager_id = normalize_entity_id(manager_id)
    if not token or not domain or not manager_id:
        return None
    cache_key = hashlib.sha256(f"{domain}\0{token}".encode("utf-8")).hexdigest()
    now = time.monotonic()
    with USER_VERIFY_CACHE_LOCK:
        cached = USER_VERIFY_CACHE.get(cache_key)
        if (
            not cached
            or now - cached.get("cachedAt", 0) >= USER_VERIFY_CACHE_TTL_SECONDS
            or str((cached.get("user") or {}).get("id") or "") != manager_id
        ):
            return None
    return {"access_token": str(token), "domain": str(domain)}


def actor_id_from_payload(payload, allow_cached=True):
    user = verify_bitrix_user(payload.get("auth"), allow_cached=allow_cached)
    if user and user.get("id"):
        return user["id"]
    if is_unverified_dev_mode():
        return str(payload.get("managerId") or "")
    return None


def require_admin(payload):
    user = verify_bitrix_user(payload.get("auth"), allow_cached=False)
    raw = (user or {}).get("raw") or {}
    if (
        user
        and user.get("id") in ADMIN_USER_IDS
        and bitrix_boolean(raw.get("ACTIVE"), default=False)
        and is_intranet_user(raw)
    ):
        return user
    return None


def clean_text(value):
    value = str(value or "")
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\[/?[A-Z0-9_=-]+\]", " ", value, flags=re.IGNORECASE)
    value = html.unescape(value)
    value = value.replace("\xa0", " ")
    value = re.sub(r":f0[0-9a-f]+:", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"https?://\S+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def is_service_text(text):
    lowered = str(text or "").lower()
    return any(re.search(pattern, lowered) for pattern in SERVICE_PATTERNS)


def split_message_fragments(text):
    text = clean_text(text)
    if not text:
        return []
    parts = re.split(r"(?=(?:\+?\d[\d\s()]{7,}\s+\d{1,2}:\d{2})|(?:Подключение #\d+.*?\d{1,2}:\d{2}))", text)
    fragments = []
    for part in parts:
        part = clean_text(part)
        part = re.sub(r"^(?:Подключение #\d+.*?\d{1,2}:\d{2}|[\+\d\s()]{8,}\s+\d{1,2}:\d{2})", "", part).strip()
        part = re.sub(r"^\(?\d{8,}\)?\s*", "", part).strip()
        if part:
            fragments.append(part)
    return fragments or [text]


def useful_fragments(text):
    fragments = []
    for fragment in split_message_fragments(text):
        if len(fragment) < 8:
            continue
        if not re.search(r"[A-Za-zА-Яа-яЁё]", fragment):
            continue
        if is_service_text(fragment):
            continue
        fragments.append(fragment)
    return fragments


def parse_source_message_time(value):
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith("Z") else raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def source_message_sort_time(value):
    return parse_source_message_time(value) or datetime.min.replace(tzinfo=timezone.utc)


def source_numeric_id(value):
    raw = str(value or "").strip()
    if not re.fullmatch(r"\d{1,20}", raw):
        return None
    return int(raw)


def get_openline_history_candidates(session_id, timeout=None):
    if not session_id:
        return []
    # Do not cache only by session ID: a new OpenLine activity can change the
    # destination while the same session remains attached to the deal.  The
    # outer DEAL_ANALYSIS_CACHE is already scoped to the full deal lifecycle.
    effective_timeout = (
        min(BITRIX_FAST_TIMEOUT_SECONDS, float(timeout))
        if timeout is not None
        else BITRIX_FAST_TIMEOUT_SECONDS
    )
    if effective_timeout <= 0:
        raise TimeoutError("Истёк таймаут истории открытой линии")
    deadline = time.monotonic() + effective_timeout
    history = bitrix_call(
        "imopenlines.session.history.get",
        {"SESSION_ID": session_id},
        timeout=effective_timeout,
    ) or {}
    if time.monotonic() > deadline:
        raise TimeoutError("Истёк таймаут истории открытой линии")
    raw_messages = history.get("message") or {}
    if isinstance(raw_messages, dict):
        message_entries = list(raw_messages.items())
    elif isinstance(raw_messages, list):
        message_entries = list(enumerate(raw_messages))
    else:
        raise RuntimeError("Bitrix вернул неожиданный формат истории открытой линии")
    if len(message_entries) > MAX_OPENLINE_MESSAGES_PER_SESSION:
        raise RuntimeError("История открытой линии превышает безопасный лимит")

    sortable_candidates = []
    for fallback_id, message in message_entries:
        if not isinstance(message, dict):
            raise RuntimeError("Bitrix вернул повреждённое сообщение открытой линии")
        if str(message.get("senderid", "0")) == "0":
            continue
        text = clean_text(message.get("text") or message.get("textlegacy"))
        fragments = useful_fragments(text)
        if not fragments:
            continue
        timestamp = parse_source_message_time(message.get("date"))
        message_id = source_numeric_id(
            message.get("id") or message.get("ID") or fallback_id
        )
        if timestamp is None or message_id is None:
            raise RuntimeError("Bitrix не дал надёжный порядок сообщений открытой линии")
        for fragment in fragments:
            sortable_candidates.append(
                {
                    "source": f"openline_session:{session_id}",
                    "timestamp": message.get("date") or "",
                    "recordId": message_id,
                    "text": fragment,
                    "_sortTime": timestamp,
                }
            )
    sortable_candidates.sort(
        key=lambda item: (item["_sortTime"], item["recordId"]),
        reverse=True,
    )
    if time.monotonic() > deadline:
        raise TimeoutError("Истёк таймаут истории открытой линии")

    candidates = []
    seen = set()
    for candidate in sortable_candidates:
        fragment = candidate["text"]
        if fragment in seen:
            continue
        seen.add(fragment)
        candidate.pop("_sortTime", None)
        candidates.append(candidate)
        if len(candidates) >= 2:
            return candidates
    return candidates


def get_openline_history_messages(session_id, timeout=None):
    return [
        candidate["text"]
        for candidate in get_openline_history_candidates(session_id, timeout=timeout)
    ]


def get_openline_chat_context(session_id):
    if not session_id:
        return None
    history = bitrix_call(
        "imopenlines.session.history.get",
        {"SESSION_ID": session_id},
        timeout=BITRIX_TIMEOUT_SECONDS,
    ) or {}
    returned_session_id = normalize_entity_id(history.get("sessionId"))
    expected_session_id = normalize_entity_id(session_id)
    if returned_session_id and returned_session_id != expected_session_id:
        return None
    chats = history.get("chat") or {}
    chat_id = normalize_entity_id(
        history.get("chatId")
        or (chats.get("id") if isinstance(chats, dict) else None)
    )
    if not expected_session_id or not chat_id or not isinstance(chats, dict):
        return None
    # Bitrix portals return both the older flat ``chat`` object and the newer
    # map keyed by chat ID.  The working OAuth sender supported the flat form.
    chat = chats.get(chat_id) or chats.get(int(chat_id)) or chats
    if not isinstance(chat, dict):
        return None
    returned_chat_id = normalize_entity_id(chat.get("id"))
    if returned_chat_id and returned_chat_id != chat_id:
        return None
    return {
        "sessionId": returned_session_id or expected_session_id,
        "chatId": chat_id,
        "textFieldEnabled": chat.get("textFieldEnabled"),
        "messageType": chat.get("messageType"),
        "entityType": chat.get("entityType"),
        "entityData2": chat.get("entityData2") or "",
    }


def openline_chat_is_bound_to_deal(entity_data, deal_id):
    """Validate Bitrix's typed ENTITY_DATA_2 pairs without substring matches."""

    parts = [str(item).strip() for item in str(entity_data or "").split("|")]
    if not parts or len(parts) % 2:
        return False
    pairs = [(parts[index].upper(), parts[index + 1]) for index in range(0, len(parts), 2)]
    deal_values = [value for entity_type, value in pairs if entity_type == "DEAL"]
    return len(deal_values) == 1 and normalize_entity_id(deal_values[0]) == normalize_entity_id(deal_id)


def openline_chat_has_foreign_deal(entity_data, deal_id):
    """Detect only an explicit contradictory nonzero DEAL binding."""

    parts = [str(item).strip() for item in str(entity_data or "").split("|")]
    if len(parts) % 2:
        return False
    expected_deal_id = normalize_entity_id(deal_id)
    for index in range(0, len(parts), 2):
        if parts[index].upper() != "DEAL":
            continue
        bound_deal_id = normalize_entity_id(parts[index + 1])
        if bound_deal_id and bound_deal_id != expected_deal_id:
            return True
    return False


def resolve_greeting_target(deal_id, manager_id, context):
    session_ids = list(
        dict.fromkeys(
            normalize_entity_id(item)
            for item in (context.get("openlineSessionIds") or [])
            if normalize_entity_id(item)
        )
    )[:1]
    if not session_ids:
        raise RuntimeError("openline_session_not_found")
    session_id = session_ids[0]
    chat_context = get_openline_chat_context(session_id)
    if not chat_context:
        raise RuntimeError("chat_context_unavailable")
    if str(chat_context.get("entityType") or "").upper() != "LINES":
        raise RuntimeError("chat_not_openline")
    if str(chat_context.get("textFieldEnabled")).lower() not in {"true", "1", "y"}:
        raise RuntimeError("chat_input_closed")
    if not openline_chat_is_bound_to_deal(chat_context.get("entityData2"), deal_id):
        raise RuntimeError("chat_entity_mismatch")

    crm_params = {
        "CRM_ENTITY_TYPE": "deal",
        "CRM_ENTITY": str(deal_id),
    }
    active_chats = bitrix_call(
        "imopenlines.crm.chat.get",
        {**crm_params, "ACTIVE_ONLY": "Y"},
    ) or []
    active_chat_ids = {
        normalize_entity_id(item.get("CHAT_ID"))
        for item in active_chats
        if isinstance(item, dict) and normalize_entity_id(item.get("CHAT_ID"))
    }
    chat_id = normalize_entity_id(chat_context.get("chatId"))
    last_chat_id = normalize_entity_id(
        bitrix_call("imopenlines.crm.chat.getLastId", crm_params)
    )
    if not chat_id or chat_id not in active_chat_ids or last_chat_id != chat_id:
        raise RuntimeError("chat_not_latest_active_for_deal")

    added_chat_id = normalize_entity_id(
        bitrix_call(
            "imopenlines.crm.chat.user.add",
            {
                **crm_params,
                "USER_ID": str(manager_id),
                "CHAT_ID": chat_id,
            },
        )
    )
    if added_chat_id != chat_id:
        raise RuntimeError("chat_participant_not_confirmed")
    # Close the small TOCTOU window between joining and dispatch: a newly
    # opened chat for the same deal must not receive a message based on the
    # older offer snapshot.
    if normalize_entity_id(
        bitrix_call("imopenlines.crm.chat.getLastId", crm_params)
    ) != chat_id:
        raise RuntimeError("latest_chat_changed_before_send")
    return {"chatId": chat_id, "sessionId": session_id}


def get_deal_messages(deal_id):
    deadline = time.monotonic() + NEXT_DEAL_BATCH_TIMEOUT_SECONDS

    def remaining_timeout(maximum=BITRIX_FAST_TIMEOUT_SECONDS):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Истёк общий таймаут анализа сделки")
        return min(float(maximum), remaining)

    def bounded_source_list(method, params):
        items = bitrix_list_all(
            method,
            params,
            max_items=MAX_SOURCE_RECORDS_PER_DEAL + 1,
            timeout=remaining_timeout(),
        )
        if len(items) > MAX_SOURCE_RECORDS_PER_DEAL:
            raise RuntimeError(f"Bitrix source {method} exceeds the safe history bound")
        return items

    raw_messages = []
    candidates = []
    newest_openline_session_id = None
    source_errors = []
    try:
        comments = bounded_source_list(
            "crm.timeline.comment.list",
            {
                "filter[ENTITY_ID]": deal_id,
                "filter[ENTITY_TYPE]": "deal",
                "select[]": ["ID", "COMMENT", "CREATED"],
                "order[CREATED]": "DESC",
                "order[ID]": "DESC",
            },
        ) or []
        for item in comments:
            text = clean_text(item.get("COMMENT"))
            raw_messages.append(("timeline", text))
            for fragment in useful_fragments(text):
                candidates.append(
                    {
                        "source": "timeline",
                        "timestamp": item.get("CREATED") or "",
                        "text": fragment,
                    }
                )
    except Exception:
        source_errors.append("timeline")

    try:
        activities = bounded_source_list(
            "crm.activity.list",
            {
                "filter[OWNER_ID]": deal_id,
                "filter[OWNER_TYPE_ID]": "2",
                "select[]": [
                    "ID",
                    "SUBJECT",
                    "DESCRIPTION",
                    "CREATED",
                    "PROVIDER_ID",
                    "DIRECTION",
                    "ASSOCIATED_ENTITY_ID",
                    "PROVIDER_PARAMS",
                ],
                "order[CREATED]": "DESC",
                "order[ID]": "DESC",
            },
        ) or []
        openline_activities = [
            item
            for item in activities
            if item.get("PROVIDER_ID") == "IMOPENLINES_SESSION"
        ]
        for item in openline_activities:
            if (
                not item.get("ASSOCIATED_ENTITY_ID")
                or parse_source_message_time(item.get("CREATED")) is None
                or source_numeric_id(item.get("ID")) is None
            ):
                raise RuntimeError("Bitrix не дал надёжный порядок сессий открытой линии")
        activities.sort(
            key=lambda item: (
                source_message_sort_time(item.get("CREATED")),
                source_numeric_id(item.get("ID")) or -1,
            ),
            reverse=True,
        )
        if openline_activities:
            openline_activities.sort(
                key=lambda item: (
                    source_message_sort_time(item.get("CREATED")),
                    source_numeric_id(item.get("ID")),
                ),
                reverse=True,
            )
            newest_openline_session_id = str(
                openline_activities[0]["ASSOCIATED_ENTITY_ID"]
            )
        for item in activities:
            text = clean_text(item.get("DESCRIPTION") or item.get("SUBJECT"))
            raw_messages.append(("activity", text))
            for fragment in useful_fragments(text):
                candidates.append(
                    {
                        "source": "activity",
                        "timestamp": item.get("CREATED") or "",
                        "text": fragment,
                    }
                )
    except Exception:
        source_errors.append("activity")

    if source_errors:
        raise RuntimeError("Не удалось полностью прочитать историю сделки из Bitrix.")

    # The newest live OpenLine session is authoritative. Mixing even one new
    # customer message with an older CRM comment can make stale destination
    # keywords outscore the customer's latest choice.
    openline_candidates = []
    if newest_openline_session_id:
        openline_candidates = get_openline_history_candidates(
            newest_openline_session_id,
            timeout=remaining_timeout(),
        )
        remaining_timeout()

    source_priority = {"activity": 0, "timeline": 1}
    candidates.sort(
        key=lambda item: (
            source_message_sort_time(item.get("timestamp")),
            2
            if str(item.get("source") or "").startswith("openline_session:")
            else source_priority.get(str(item.get("source") or ""), 0),
        ),
        reverse=True,
    )
    if openline_candidates:
        # If the latest message already names a destination, it supersedes an
        # older contradictory message in the same session. Otherwise keep one
        # preceding message because it can carry the destination for a short
        # follow-up such as "двое взрослых".
        candidates = openline_candidates[:2]
        if classify([candidates[0].get("text") or ""])["direction"] != "Не определено":
            candidates = candidates[:1]
    useful = []
    for candidate in candidates:
        fragment = candidate.get("text") or ""
        if fragment and fragment not in useful:
            useful.append(fragment)
        if len(useful) >= 2:
            break

    return {
        "useful": useful[:2],
        "rawCount": len(raw_messages),
        "sources": (
            [f"openline_session:{newest_openline_session_id}"]
            if newest_openline_session_id
            else []
        ) + [item[0] for item in raw_messages],
        "openlineSessionIds": (
            [newest_openline_session_id] if newest_openline_session_id else []
        ),
    }


def keyword_matches(keyword, text):
    keyword = str(keyword or "").strip().casefold()
    text = str(text or "").casefold()
    if not keyword or not text:
        return False
    escaped = re.escape(keyword).replace(r"\ ", r"\s+")
    has_cyrillic = bool(re.search(r"[а-яё]", keyword))
    suffix = r"[\w-]*" if has_cyrillic else ""
    return bool(re.search(rf"(?<![\w]){escaped}{suffix}(?![\w])", text, flags=re.IGNORECASE))


def classify(messages):
    joined = " ".join(clean_text(message) for message in (messages or [])).casefold()
    scores = {}
    for destination, keywords in DESTINATION_KEYWORDS.items():
        score = sum(1 for keyword in keywords if keyword_matches(keyword, joined))
        if score:
            scores[destination] = score
    if not scores:
        return {"direction": "Не определено", "confidence": "низкая", "matched": []}
    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
    confidence = "высокая" if ranked[0][1] >= 2 else "средняя"
    return {
        "direction": ranked[0][0],
        "confidence": confidence,
        "matched": [item[0] for item in ranked[:3]],
    }


def greeting_context_from_deal(deal_id, deal_payload=None):
    # Security invariant: deal messages, classification and Open Line session IDs
    # always come from Bitrix. Browser payload is presentation-only and untrusted.
    message_data = get_deal_messages(deal_id)
    messages = message_data["useful"]
    openline_session_ids = message_data.get("openlineSessionIds") or []
    classification = classify(messages)

    return {
        "classification": classification or {"direction": "Не определено", "confidence": "низкая", "matched": []},
        "messages": messages or [],
        "openlineSessionIds": openline_session_ids,
    }


def build_greeting_text(manager, classification):
    manager_name = ((manager or {}).get("name") or "ваш менеджер").split()[0]
    direction = (classification or {}).get("direction") or "Не определено"
    if direction != "Не определено":
        return (
            f"Здравствуйте! Меня зовут {manager_name}. "
            f"Я эксперт по направлению {direction}. "
            "Сейчас посмотрю варианты и посчитаю для вас стоимость тура."
        )
    return (
        f"Здравствуйте! Меня зовут {manager_name}. "
        "Сейчас посмотрю ваш запрос и посчитаю для вас стоимость тура."
    )


def validate_greeting_text_input(value):
    text = str(value or "").strip()
    if not text or len(text) > 2000 or "\x00" in text:
        raise RuntimeError("greeting_text_invalid")
    if any(ord(character) < 32 and character not in {"\n", "\r", "\t"} for character in text):
        raise RuntimeError("greeting_text_invalid")
    return text


def actor_greeting_chat_context(deal_id, session_id):
    """Resolve the same exact OpenLine chat used by the proven OAuth path."""

    chat_context = get_openline_chat_context(session_id)
    if not chat_context:
        raise RuntimeError("chat_context_unavailable")
    text_field_enabled = chat_context.get("textFieldEnabled")
    if (
        text_field_enabled is not None
        and str(text_field_enabled).lower() not in {"true", "1", "y"}
    ):
        raise RuntimeError("chat_input_closed")
    entity_type = chat_context.get("entityType")
    entity_data = chat_context.get("entityData2")
    if entity_type not in {None, ""} and str(entity_type).upper() != "LINES":
        raise RuntimeError("chat_not_openline")
    if openline_chat_has_foreign_deal(entity_data, deal_id):
        raise RuntimeError("chat_entity_mismatch")
    return chat_context


def answer_openline_for_actor(auth, chat_id):
    try:
        return bitrix_call_for_actor(
            auth,
            "imopenlines.operator.answer",
            {"CHAT_ID": str(chat_id)},
        )
    except Exception as exc:
        message = str(exc or "")
        if (
            "ALREADY_RESPONSIBLE" in message.upper()
            or "уже ответственный" in message.lower()
        ):
            return None
        raise


def send_chat_message_for_actor(auth, chat_id, text):
    text = validate_greeting_text_input(text)
    result = bitrix_call_for_actor(
        auth,
        "im.message.add",
        {
            "DIALOG_ID": f"chat{chat_id}",
            "MESSAGE": text,
        },
    )
    message_id = normalize_entity_id(result)
    if not message_id:
        raise RuntimeError("greeting_message_not_confirmed")
    return {
        "ok": True,
        "method": "imopenlines.operator.answer + im.message.add",
        "messageId": message_id,
    }


def send_greeting_message(deal_id, manager_id, text, context, auth=None, target=None):
    # ``auth`` remains in the signature for compatibility with callers, but no
    # short-lived OAuth token is persisted or needed by the background worker.
    # The service webhook acts only after the manager is the verified assignee,
    # adds that exact manager to the exact CRM chat, and sends as USER_ID.
    target = target or resolve_greeting_target(deal_id, manager_id, context)
    chat_id = normalize_entity_id((target or {}).get("chatId"))
    if not chat_id:
        raise RuntimeError("greeting_target_missing")
    message_result = bitrix_call(
        "imopenlines.crm.message.add",
        {
            "CRM_ENTITY_TYPE": "deal",
            "CRM_ENTITY": str(deal_id),
            "USER_ID": str(manager_id),
            "CHAT_ID": chat_id,
            "MESSAGE": text,
        },
    )
    message_id = normalize_entity_id(message_result)
    if not message_id:
        raise RuntimeError("greeting_message_not_confirmed")
    return {
        "ok": True,
        "method": "imopenlines.crm.message.add",
        "messageId": message_id,
        "chatBindingVerified": True,
    }


def greeting_response(entry, *, status=None, message=None, send_result=None):
    return {
        "ok": True,
        "status": status or entry.get("status") or "manual",
        "autoSent": bool(entry.get("autoSent")),
        "text": entry.get("text") or "",
        "direction": entry.get("direction") or "Не определено",
        "message": message or entry.get("message") or "Текст приветствия подготовлен.",
        "sendResult": send_result,
    }


def prepare_greeting(
    manager_id,
    deal_id,
    auth=None,
    operation_key=None,
    *,
    manager=None,
    context=None,
):
    operation_key = str(operation_key or "")
    if not operation_key:
        raise ValueError("Для приветствия требуется ключ конкретной операции взятия")
    # Serialize the read/reserve/send sequence in the supported one-process
    # deployment. SQLite also has a unique per-lifecycle "sending"
    # reservation so an accidental second process cannot start another
    # automatic attempt or reuse another manager's text.
    with GREETING_LOCK:
        existing = latest_greeting_for_operation(operation_key)
        if existing and str(existing.get("managerId") or "") != str(manager_id):
            existing = None
        if existing and existing.get("status") == "sending":
            return greeting_response(
                existing,
                status="manual_after_uncertain_send",
                message=(
                    "Автоотправка уже запускалась, но её итог не подтверждён. "
                    "Проверьте чат и при необходимости отправьте текст вручную."
                ),
            )
        if existing and (
            not GREETING_AUTO_SEND
            or existing.get("autoSent")
            or existing.get("autoAttempted")
        ):
            return greeting_response(
                existing,
                status="skipped_duplicate",
                message="Приветствие для этой сделки уже было подготовлено раньше.",
            )

        manager = dict(manager) if isinstance(manager, dict) else None
        manager = manager or get_manager_profile(manager_id) or {
            "id": str(manager_id),
            "name": str(manager_id),
        }
        # A supplied context is an internal, version-bound snapshot captured
        # from the server-side analysis cache.  On a cache miss the background
        # worker may re-read Bitrix, but the manager's claim response never
        # waits for this work.
        context = (
            normalize_server_greeting_context(context)
            if isinstance(context, dict)
            else greeting_context_from_deal(deal_id)
        )
        text = (existing or {}).get("text") or build_greeting_text(
            manager, context["classification"]
        )
        base_entry = {
            "timestamp": local_now().isoformat(),
            "managerId": str(manager_id),
            "dealId": str(deal_id),
            "operationKey": operation_key,
            "direction": context["classification"].get("direction") or "Не определено",
            "confidence": context["classification"].get("confidence") or "",
            "text": text,
        }

        if not GREETING_AUTO_SEND or not GREETING_AUTO_SEND_SUPPORTED:
            entry = {
                **base_entry,
                "status": "manual",
                "autoSent": False,
                "autoAttempted": False,
                "message": (
                    "Текст подготовлен. Автоотправка запрещена до безопасной привязки чата к сделке."
                    if GREETING_AUTO_SEND
                    else "Текст подготовлен. Автоотправка пока выключена."
                ),
                "sendResult": None,
            }
            append_greeting_log(entry)
            return greeting_response(entry)

        reservation = {
            **base_entry,
            "status": "sending",
            "autoSent": False,
            "autoAttempted": True,
            "message": "Автоотправка зарезервирована.",
            "sendResult": None,
        }
        append_greeting_log(reservation)
        send_result = send_greeting_message(deal_id, manager_id, text, context, auth)
        auto_sent = bool(send_result.get("ok"))
        entry = {
            **base_entry,
            "timestamp": local_now().isoformat(),
            "status": "sent" if auto_sent else "manual",
            "autoSent": auto_sent,
            "autoAttempted": True,
            "message": (
                "Приветствие автоматически отправлено клиенту."
                if auto_sent
                else "Автоотправка не подтверждена. Проверьте чат и отправьте текст вручную."
            ),
            "sendResult": send_result,
        }
        append_greeting_log(entry)
        return greeting_response(entry, send_result=send_result)


def deal_score_for_manager(deal, manager):
    competencies = [str(item).strip() for item in manager.get("competencies", []) if str(item).strip()]
    direction_name = deal["classification"]["direction"]
    if not competencies:
        return 0
    if direction_name == "Не определено":
        return 1
    direction = direction_name
    text = " ".join(deal.get("messages", []))
    score = 0
    for competency in competencies:
        if keyword_matches(competency, direction):
            score += 4
        if keyword_matches(competency, text):
            score += 2
    if deal["classification"]["direction"] != "Не определено" and score:
        score += 1
    return score


def list_allowed_deal_headers():
    now = time.monotonic()
    with DEAL_HEADERS_CACHE_LOCK:
        cached = DEAL_HEADERS_CACHE.get("all")
        if cached and now - cached.get("cachedAt", 0) < DEAL_HEADERS_CACHE_TTL_SECONDS:
            return [dict(item) for item in cached.get("headers", [])]
    pending = []
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(SOURCE_STAGES))
    futures = {
        executor.submit(
            bitrix_list_all,
            "crm.deal.list",
            {
                "filter[STAGE_ID]": stage_id,
                "select[]": [
                    "ID",
                    "TITLE",
                    "STAGE_ID",
                    "ASSIGNED_BY_ID",
                    "DATE_CREATE",
                    "DATE_MODIFY",
                    "LAST_ACTIVITY_TIME",
                ],
                "order[DATE_CREATE]": "ASC",
            },
            MAX_DEALS_PER_STAGE,
            BITRIX_TIMEOUT_SECONDS,
        ): (stage_id, stage_name)
        for stage_id, stage_name in SOURCE_STAGES.items()
    }
    try:
        for future in concurrent.futures.as_completed(
            futures,
            timeout=BITRIX_TIMEOUT_SECONDS + 1,
        ):
            _stage_id, stage_name = futures[future]
            for deal in future.result() or []:
                item = dict(deal)
                item["_stageName"] = stage_name
                pending.append(item)
    except concurrent.futures.TimeoutError as exc:
        raise RuntimeError("Не удалось вовремя получить список сделок из Bitrix.") from exc
    finally:
        for future in futures:
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)

    pending.sort(key=lambda item: (item.get("DATE_CREATE") or "", int(item.get("ID") or 0)))
    with DEAL_HEADERS_CACHE_LOCK:
        DEAL_HEADERS_CACHE["all"] = {
            "cachedAt": time.monotonic(),
            "headers": [dict(item) for item in pending],
        }
    return [dict(item) for item in pending]


def invalidate_deal_caches(deal_id=None):
    with DEAL_HEADERS_CACHE_LOCK:
        DEAL_HEADERS_CACHE.clear()
    if deal_id is not None:
        with DEAL_ANALYSIS_CACHE_LOCK:
            DEAL_ANALYSIS_CACHE.pop(str(deal_id), None)


def normalize_server_greeting_context(context):
    """Copy the private server-side routing snapshot used for a greeting."""

    context = context if isinstance(context, dict) else {}
    classification = context.get("classification")
    classification = classification if isinstance(classification, dict) else {}
    session_ids = []
    for item in context.get("openlineSessionIds") or []:
        normalized = normalize_entity_id(item)
        if normalized and normalized not in session_ids:
            session_ids.append(normalized)
    messages = [
        clean_text(item)
        for item in (context.get("messages") or [])
        if clean_text(item)
    ][:2]
    return {
        "classification": {
            "direction": clean_text(classification.get("direction")) or "Не определено",
            "confidence": clean_text(classification.get("confidence")) or "низкая",
            "matched": [
                clean_text(item)
                for item in (classification.get("matched") or [])
                if clean_text(item)
            ][:3],
        },
        "messages": messages,
        "openlineSessionIds": session_ids[:1],
    }


def cached_greeting_context(deal_id, version):
    """Return an exact version-bound private context without another REST scan."""

    with DEAL_ANALYSIS_CACHE_LOCK:
        cached = DEAL_ANALYSIS_CACHE.get(str(deal_id))
        if not cached or str(cached.get("version") or "") != str(version or ""):
            return None
        context = cached.get("greetingContext")
    normalized = normalize_server_greeting_context(context)
    return normalized if normalized.get("openlineSessionIds") else None


def analyze_deal_header(deal):
    deal_id = str(deal.get("ID") or "")
    cache_version = deal_version(deal)
    now = time.monotonic()
    with DEAL_ANALYSIS_CACHE_LOCK:
        cached = DEAL_ANALYSIS_CACHE.get(deal_id)
        if (
            cached
            and cached.get("version") == cache_version
            and now - cached.get("cachedAt", 0) < DEAL_ANALYSIS_CACHE_TTL_SECONDS
        ):
            return dict(cached["deal"])

    messages = get_deal_messages(deal_id)
    classification = classify(messages["useful"])
    analyzed = {
        "id": deal.get("ID"),
        "title": deal.get("TITLE"),
        "stageId": deal.get("STAGE_ID"),
        "stageName": deal.get("_stageName") or SOURCE_STAGES.get(deal.get("STAGE_ID"), ""),
        "assignedById": deal.get("ASSIGNED_BY_ID"),
        "dateCreate": deal.get("DATE_CREATE"),
        "dateModify": deal.get("DATE_MODIFY"),
        "version": cache_version,
        "messages": messages["useful"],
        "dealUrl": f"{portal_base_url()}/crm/deal/details/{deal_id}/",
        "classification": classification,
    }
    with DEAL_ANALYSIS_CACHE_LOCK:
        bounded_cache_put(
            DEAL_ANALYSIS_CACHE,
            deal_id,
            {
                "version": cache_version,
                "cachedAt": now,
                "deal": analyzed,
                # This never enters the browser DTO.  It is the same
                # server-read snapshot that produced the signed selection.
                "greetingContext": normalize_server_greeting_context(
                    {
                        "classification": classification,
                        "messages": messages["useful"],
                        "openlineSessionIds": messages.get("openlineSessionIds") or [],
                    }
                ),
            },
        )
    return dict(analyzed)


def analyze_deal_headers(headers):
    if not headers:
        return {}, {}
    workers = max(1, min(NEXT_DEAL_SCAN_WORKERS, len(headers)))
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
    futures = {
        executor.submit(analyze_deal_header, header): str(header.get("ID") or "")
        for header in headers
    }
    analyzed = {}
    errors = {}
    try:
        for future in concurrent.futures.as_completed(
            futures,
            timeout=NEXT_DEAL_BATCH_TIMEOUT_SECONDS,
        ):
            deal_id = futures[future]
            try:
                analyzed[deal_id] = future.result()
            except Exception as exc:
                errors[deal_id] = str(exc)
    except concurrent.futures.TimeoutError:
        pass
    finally:
        for future, deal_id in futures.items():
            if not future.done():
                errors.setdefault(deal_id, "timeout")
                future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)
    return analyzed, errors


def _get_next_deal_for_manager(manager_id, continuation_token=None):
    manager = get_manager_profile(manager_id)
    if not manager:
        return {"deal": None, "reason": "Менеджер не найден в настройке компетенций."}
    if manager.get("active") is not True:
        return {"manager": manager, "deal": None, "reason": "Пользователь деактивирован в Bitrix24."}
    if manager.get("intranet") is not True:
        return {"manager": manager, "deal": None, "reason": "Выдача доступна только сотрудникам компании."}
    access = check_manager_access(manager_id)
    if not access["ok"]:
        return {
            "manager": manager,
            "deal": None,
            "reason": access["reason"],
            "limitReached": bool(access.get("limitReached")),
            "takenToday": access.get("takenToday"),
            "dailyLimit": access.get("dailyLimit"),
            "extraClaimEnabled": bool(access.get("extraClaimEnabled")),
            "extraClaimRequest": access.get("extraClaimRequest"),
            "extraClaimGrantAvailable": bool(access.get("extraClaimGrantAvailable")),
        }

    rejected_keys = STATE_STORE.list_rejection_semantic_keys(manager_id)
    unresolved_deal_ids = STATE_STORE.list_unresolved_claim_deal_ids()
    headers = [
        header
        for header in list_allowed_deal_headers()
        if str(header.get("ID") or "") not in unresolved_deal_ids
        if rejection_semantic_key(manager_id, header.get("ID"), deal_version(header))
        not in rejected_keys
    ]
    snapshot = search_snapshot(headers, manager, access.get("rule"))
    offset = decode_search_cursor(continuation_token, manager_id, snapshot)
    if offset is None or offset > len(headers):
        return {
            "manager": manager,
            "deal": None,
            "reason": "Продолжение поиска устарело. Начните поиск заново.",
            "hasMore": False,
            "_httpStatus": 409,
        }
    batch_limit = max(1, NEXT_DEAL_SCAN_LIMIT)
    batch_headers = headers[offset : offset + batch_limit]
    analyzed, errors = analyze_deal_headers(batch_headers)
    next_offset = offset + len(batch_headers)
    has_more = next_offset < len(headers)
    response_meta = {
        "checkedCount": len(batch_headers),
        "hasMore": has_more,
        "partialTimeouts": len(errors),
        "continuationToken": (
            issue_search_cursor(manager_id, next_offset, snapshot) if has_more else None
        ),
    }

    for header in batch_headers:
        header_id = str(header.get("ID") or "")
        if header_id in errors or header_id not in analyzed:
            return {
                "manager": manager,
                "deal": None,
                "reason": (
                    "Не удалось безопасно проверить самую старую заявку. "
                    "Повторите поиск через минуту."
                ),
                "checkedCount": len(batch_headers),
                "partialTimeouts": max(1, len(errors)),
                "hasMore": False,
                "continuationToken": None,
                "_httpStatus": 503,
            }
        deal = analyzed.get(header_id)
        if not deal:
            continue
        score = deal_score_for_manager(deal, manager)
        if score <= 0:
            continue
        deal["matchScore"] = score
        if not deal.get("messages"):
            deal["matchReason"] = "Сообщения не найдены, заявка доступна всем менеджерам."
        elif deal["classification"]["direction"] == "Не определено":
            deal["matchReason"] = "Страна не определена, заявка доступна всем менеджерам."
        else:
            deal["matchReason"] = "Совпало с компетенциями менеджера."
        deal["selectionToken"] = issue_selection_token(
            deal.get("id"),
            manager_id,
            deal.get("version"),
            manager_policy_hash(manager, access.get("rule")),
        )
        return {
            "manager": manager,
            "deal": deal,
            **response_meta,
            "hasMore": False,
            "continuationToken": None,
        }

    if response_meta["hasMore"]:
        return {
            "manager": manager,
            "deal": None,
            "reason": "Проверяю следующие заявки...",
            **response_meta,
        }
    if errors and not analyzed:
        reason = "Bitrix отвечает слишком долго. Повторите поиск через минуту."
    elif not manager.get("competencies"):
        reason = "В карточке сотрудника нет навыков, а общих заявок без страны сейчас нет."
    else:
        reason = "Нет доступных сделок по навыкам менеджера или общих заявок без страны."
    return {"manager": manager, "deal": None, "reason": reason, **response_meta}


def get_next_deal_for_manager(manager_id, continuation_token=None):
    if not SEARCH_SEMAPHORE.acquire(blocking=False):
        return {
            "deal": None,
            "reason": "Сейчас уже выполняется несколько поисков. Повторите через несколько секунд.",
            "busy": True,
        }
    try:
        return _get_next_deal_for_manager(manager_id, continuation_token)
    finally:
        SEARCH_SEMAPHORE.release()


def iter_allowed_deals(limit=50):
    headers = list_allowed_deal_headers()[:max(0, int(limit))]
    analyzed, _errors = analyze_deal_headers(headers)
    for header in headers:
        deal = analyzed.get(str(header.get("ID") or ""))
        if deal:
            yield deal


def list_allowed_deals(limit=20):
    deals = []
    for deal in iter_allowed_deals(limit):
        deals.append(deal)
    return deals


def fail_claim_operation_safely(operation_key, error, result=None):
    try:
        STATE_STORE.fail_claim_operation(operation_key, error, result=result)
    except Exception as state_error:
        sys.stderr.write(f"Claim operation failure could not be persisted: {type(state_error).__name__}\n")


GREETING_TERMINAL_PREFLIGHT_ERRORS = {
    "openline_session_not_found",
    "chat_context_unavailable",
    "chat_not_openline",
    "chat_input_closed",
    "chat_entity_mismatch",
    "greeting_text_invalid",
    "chat_not_latest_active_for_deal",
    "chat_participant_not_confirmed",
    "latest_chat_changed_before_send",
    "claim_operation_not_succeeded",
    "claim_identity_mismatch",
    "claim_marker_mismatch",
    "manager_not_active",
    "manager_access_revoked",
}


def greeting_machine_error(exc, fallback="greeting_preflight_failed"):
    raw = str(exc or "").strip()
    for code in GREETING_TERMINAL_PREFLIGHT_ERRORS:
        if code in raw:
            return code
    return fallback


def append_greeting_delivery_audit(job, manager, *, status, auto_sent, message, error_code=""):
    entry = {
        "timestamp": local_now().isoformat(),
        "managerId": str(job.get("managerId") or ""),
        "managerName": str((manager or {}).get("name") or ""),
        "dealId": str(job.get("dealId") or ""),
        "operationKey": str(job.get("operationKey") or ""),
        "direction": str(job.get("direction") or "Не определено"),
        "confidence": "",
        "text": str(job.get("text") or ""),
        "status": status,
        "autoSent": bool(auto_sent),
        "autoAttempted": status in {"sent", "uncertain"},
        "message": message,
        "errorCode": error_code,
    }
    try:
        append_greeting_log(entry)
    except Exception as exc:
        sys.stderr.write(
            f"Greeting audit persistence failed: {type(exc).__name__}\n"
        )


def get_greeting_manager_profile(manager_id):
    """Load a live Bitrix profile while keeping transport failures retryable."""

    users = bitrix_call(
        "user.get",
        {"ID": str(manager_id)},
        timeout=BITRIX_FAST_TIMEOUT_SECONDS,
    ) or []
    if not users or not isinstance(users[0], dict):
        raise RuntimeError("manager_not_active")
    return manager_profile_from_user(users[0], manager_id)


def process_actor_greeting_outbox_job(job, worker_token, auth, manager):
    """Deliver one reserved greeting through the manager's proven OAuth path."""

    operation_key = str(job.get("operationKey") or "")
    deal_id = normalize_entity_id(job.get("dealId"))
    manager_id = normalize_entity_id(job.get("managerId"))
    try:
        token, domain = extract_auth_credentials(auth)
        if not token or not domain:
            raise RuntimeError("actor_auth_unavailable")
        operation = STATE_STORE.get_claim_operation(operation_key)
        if not operation or operation.get("status") != "succeeded":
            raise RuntimeError("claim_operation_not_succeeded")
        if (
            str(operation.get("dealId") or "") != deal_id
            or str(operation.get("managerId") or "") != manager_id
        ):
            raise RuntimeError("claim_identity_mismatch")
        expected_marker = str(
            ((operation.get("request") or {}).get("claimMarker")) or ""
        )
        live_deal = bitrix_call("crm.deal.get", {"id": deal_id}) or {}
        if (
            str(live_deal.get("ASSIGNED_BY_ID") or "") != manager_id
            or str(live_deal.get("STAGE_ID") or "") != TARGET_STAGE
            or not expected_marker
            or str(live_deal.get(BITRIX_CLAIM_MARKER_FIELD) or "")
            != expected_marker
        ):
            raise RuntimeError("claim_marker_mismatch")
        if (
            not isinstance(manager, dict)
            or str(manager.get("id") or "") != manager_id
            or manager.get("active") is not True
            or manager.get("intranet") is not True
        ):
            raise RuntimeError("manager_not_active")
        configured_rules = STATE_STORE.list_rules()
        if (
            REQUIRE_EXPLICIT_ACCESS_RULE
            and not is_unverified_dev_mode()
            and manager_id not in configured_rules
        ) or not get_manager_rule(manager_id).get("enabled"):
            raise RuntimeError("manager_access_revoked")

        session_id = normalize_entity_id(job.get("sessionId"))
        if not session_id:
            raise RuntimeError("openline_session_not_found")
        chat_context = actor_greeting_chat_context(deal_id, session_id)
        direction = str(job.get("direction") or "Не определено")
        text = validate_greeting_text_input(
            build_greeting_text(
                manager,
                {"direction": direction, "confidence": "", "matched": []},
            )
        )
        checked = STATE_STORE.update_greeting_outbox_check(
            operation_key,
            worker_token,
            session_id=session_id,
            direction=direction,
            text=text,
        )
        if not checked.get("transitioned"):
            return checked
        job = checked

        # This is the exact sequence used by the previous working release.
        # Taking operator responsibility is pre-send and therefore retry-safe.
        answer_openline_for_actor(auth, chat_context["chatId"])
        dispatching = STATE_STORE.mark_greeting_outbox_dispatching(
            operation_key,
            worker_token,
        )
        if not dispatching.get("transitioned"):
            return dispatching
    except Exception as exc:
        code = greeting_machine_error(exc, fallback="actor_greeting_preflight_failed")
        try:
            if code in GREETING_TERMINAL_PREFLIGHT_ERRORS:
                final = STATE_STORE.mark_greeting_outbox_manual(
                    operation_key,
                    worker_token,
                    error_code=code,
                )
            else:
                final = STATE_STORE.retry_greeting_outbox_check(
                    operation_key,
                    worker_token,
                    error_code=code,
                    delay_seconds=5,
                    max_attempts=3,
                )
        except Exception as state_exc:
            sys.stderr.write(
                f"Actor greeting preflight state update failed: {type(state_exc).__name__}\n"
            )
            return {"status": "checking", "errorCode": code}
        if final.get("status") == "manual" and final.get("transitioned"):
            append_greeting_delivery_audit(
                final,
                manager,
                status="manual",
                auto_sent=False,
                message="Автоприветствие не отправлено: чат не прошёл проверку.",
                error_code=code,
            )
        return final

    try:
        send_result = send_chat_message_for_actor(
            auth,
            chat_context["chatId"],
            dispatching.get("text") or text,
        )
        final = STATE_STORE.mark_greeting_outbox_sent(
            operation_key,
            worker_token,
            message_id=send_result.get("messageId"),
        )
        if final.get("transitioned"):
            append_greeting_delivery_audit(
                final,
                manager,
                status="sent",
                auto_sent=True,
                message="Приветствие автоматически отправлено клиенту.",
            )
        return final
    except Exception as exc:
        # Once ``dispatching`` is durable the message may already have reached
        # Bitrix.  Never retry automatically and risk greeting the client twice.
        code = greeting_machine_error(exc, fallback="send_result_uncertain")
        try:
            final = STATE_STORE.mark_greeting_outbox_uncertain(
                operation_key,
                worker_token,
                error_code=code,
            )
        except Exception as state_exc:
            sys.stderr.write(
                f"Actor greeting uncertainty state update failed: {type(state_exc).__name__}\n"
            )
            return {"status": "dispatching", "errorCode": code}
        if final.get("transitioned"):
            append_greeting_delivery_audit(
                final,
                manager,
                status="uncertain",
                auto_sent=False,
                message=(
                    "Результат автоотправки не подтверждён. Проверьте чат перед ручной отправкой."
                ),
                error_code=code,
            )
        return final


def _actor_greeting_thread_entry(job, worker_token, auth, manager):
    try:
        process_actor_greeting_outbox_job(
            job,
            worker_token,
            auth,
            manager,
        )
    except Exception as exc:
        # Never include the OAuth payload or upstream response in logs.
        sys.stderr.write(f"Actor greeting thread failed: {type(exc).__name__}\n")
    finally:
        GREETING_ACTOR_THREAD_SLOTS.release()
        GREETING_WAKE_EVENT.set()


def start_actor_greeting_delivery(operation_key, auth, manager):
    """Reserve the exact operation and start at most one bounded daemon send."""

    if not GREETING_ACTOR_THREAD_SLOTS.acquire(blocking=False):
        return None
    worker_token = secrets.token_hex(16)
    try:
        job = STATE_STORE.lease_exact_greeting_outbox(
            operation_key,
            worker_token,
            lease_seconds=max(30, int(BITRIX_TIMEOUT_SECONDS * 8)),
            max_attempts=3,
        )
        if not job.get("transitioned"):
            GREETING_ACTOR_THREAD_SLOTS.release()
            return None
        thread = threading.Thread(
            target=_actor_greeting_thread_entry,
            args=(job, worker_token, auth, dict(manager or {})),
            name="actor-greeting",
            daemon=True,
        )
        thread.start()
        return job
    except Exception:
        try:
            STATE_STORE.retry_greeting_outbox_check(
                operation_key,
                worker_token,
                error_code="actor_thread_start_failed",
                delay_seconds=0,
                max_attempts=3,
            )
        except Exception:
            pass
        GREETING_ACTOR_THREAD_SLOTS.release()
        return None


def process_greeting_outbox_job(job, worker_token):
    operation_key = str(job.get("operationKey") or "")
    deal_id = normalize_entity_id(job.get("dealId"))
    manager_id = normalize_entity_id(job.get("managerId"))
    manager = None
    try:
        operation = STATE_STORE.get_claim_operation(operation_key)
        if not operation or operation.get("status") != "succeeded":
            raise RuntimeError("claim_operation_not_succeeded")
        if (
            str(operation.get("dealId") or "") != deal_id
            or str(operation.get("managerId") or "") != manager_id
        ):
            raise RuntimeError("claim_identity_mismatch")
        expected_marker = str(
            ((operation.get("request") or {}).get("claimMarker")) or ""
        )
        live_deal = bitrix_call("crm.deal.get", {"id": deal_id}) or {}
        if (
            str(live_deal.get("ASSIGNED_BY_ID") or "") != manager_id
            or str(live_deal.get("STAGE_ID") or "") != TARGET_STAGE
            or not expected_marker
            or str(live_deal.get(BITRIX_CLAIM_MARKER_FIELD) or "")
            != expected_marker
        ):
            raise RuntimeError("claim_marker_mismatch")
        manager = get_greeting_manager_profile(manager_id)
        if (
            not manager
            or manager.get("active") is not True
            or manager.get("intranet") is not True
        ):
            raise RuntimeError("manager_not_active")
        configured_rules = STATE_STORE.list_rules()
        if (
            REQUIRE_EXPLICIT_ACCESS_RULE
            and not is_unverified_dev_mode()
            and manager_id not in configured_rules
        ) or not get_manager_rule(manager_id).get("enabled"):
            raise RuntimeError("manager_access_revoked")

        context = normalize_server_greeting_context(
            {
                "classification": {
                    "direction": job.get("direction") or "Не определено",
                    "confidence": "",
                    "matched": [],
                },
                "messages": [],
                "openlineSessionIds": [job.get("sessionId")],
            }
        )
        text = build_greeting_text(manager, context["classification"])
        checked = STATE_STORE.update_greeting_outbox_check(
            operation_key,
            worker_token,
            session_id=context["openlineSessionIds"][0],
            direction=context["classification"]["direction"],
            text=text,
        )
        if not checked.get("transitioned"):
            return checked
        job = checked
        target = resolve_greeting_target(deal_id, manager_id, context)
    except Exception as exc:
        code = greeting_machine_error(exc)
        try:
            if code in GREETING_TERMINAL_PREFLIGHT_ERRORS:
                final = STATE_STORE.mark_greeting_outbox_manual(
                    operation_key,
                    worker_token,
                    error_code=code,
                )
            else:
                final = STATE_STORE.retry_greeting_outbox_check(
                    operation_key,
                    worker_token,
                    error_code=code,
                    delay_seconds=5,
                    max_attempts=3,
                )
        except Exception as state_exc:
            sys.stderr.write(
                f"Greeting preflight state update failed: {type(state_exc).__name__}\n"
            )
            return {"status": "checking", "errorCode": code}
        if final.get("status") == "manual" and final.get("transitioned"):
            append_greeting_delivery_audit(
                final,
                manager,
                status="manual",
                auto_sent=False,
                message="Автоприветствие не отправлено: безопасная привязка чата не подтверждена.",
                error_code=code,
            )
        return final

    dispatching = STATE_STORE.mark_greeting_outbox_dispatching(
        operation_key,
        worker_token,
    )
    if not dispatching.get("transitioned"):
        return dispatching
    try:
        send_result = send_greeting_message(
            deal_id,
            manager_id,
            dispatching.get("text") or text,
            context,
            target=target,
        )
        final = STATE_STORE.mark_greeting_outbox_sent(
            operation_key,
            worker_token,
            message_id=send_result.get("messageId"),
        )
        if final.get("transitioned"):
            append_greeting_delivery_audit(
                final,
                manager,
                status="sent",
                auto_sent=True,
                message="Приветствие автоматически отправлено клиенту.",
            )
        return final
    except Exception as exc:
        # The request may have reached Bitrix even when its response was lost.
        # Never retry after crossing the durable dispatching boundary.
        code = greeting_machine_error(exc, fallback="send_result_uncertain")
        try:
            final = STATE_STORE.mark_greeting_outbox_uncertain(
                operation_key,
                worker_token,
                error_code=code,
            )
        except Exception as state_exc:
            sys.stderr.write(
                f"Greeting uncertainty state update failed: {type(state_exc).__name__}\n"
            )
            return {"status": "dispatching", "errorCode": code}
        if final.get("transitioned"):
            append_greeting_delivery_audit(
                final,
                manager,
                status="uncertain",
                auto_sent=False,
                message=(
                    "Результат автоотправки не подтверждён. Проверьте чат перед ручной отправкой."
                ),
                error_code=code,
            )
        return final


def process_greeting_outbox_once(worker_token=None, limit=1):
    if DRY_RUN or not (GREETING_AUTO_SEND and GREETING_AUTO_SEND_SUPPORTED):
        return {"leased": 0, "processed": 0}
    worker_token = str(worker_token or secrets.token_hex(16))
    jobs = STATE_STORE.lease_greeting_outbox(
        worker_token,
        limit=max(1, int(limit)),
        lease_seconds=max(30, int(BITRIX_TIMEOUT_SECONDS * 8)),
        max_attempts=3,
    )
    processed = 0
    for job in jobs:
        process_greeting_outbox_job(job, worker_token)
        processed += 1
    return {"leased": len(jobs), "processed": processed}


def greeting_outbox_loop():
    worker_token = secrets.token_hex(16)
    while True:
        try:
            if readiness_state().get("ok") and GREETING_AUTO_SEND and not DRY_RUN:
                recovered = STATE_STORE.recover_stale_greeting_dispatches()
                for job in recovered:
                    append_greeting_delivery_audit(
                        job,
                        None,
                        status="uncertain",
                        auto_sent=False,
                        message=(
                            "Сервис перезапустился во время отправки. Проверьте чат перед ручным сообщением."
                        ),
                        error_code=job.get("errorCode") or "stale_dispatching",
                    )
                process_greeting_outbox_once(worker_token=worker_token)
        except Exception as exc:
            sys.stderr.write(f"Greeting worker failed: {type(exc).__name__}\n")
        GREETING_WAKE_EVENT.wait(timeout=GREETING_WORKER_POLL_SECONDS)
        GREETING_WAKE_EVENT.clear()


def attach_greeting_to_claim(
    result,
    manager_id,
    deal_id,
    auth,
    operation_key,
    manager_profile=None,
):
    response = dict(result or {})
    if not (GREETING_AUTO_SEND and GREETING_AUTO_SEND_SUPPORTED):
        return response
    if response.get("auditRecorded") is not True:
        return response
    job = STATE_STORE.get_greeting_outbox(operation_key)
    if not job:
        response.setdefault("warnings", []).append(
            "Сделка взята, но у неё не найден подтверждённый OpenLine-чат. Напишите клиенту вручную."
        )
        return response
    status = str(job.get("status") or "pending")
    if status in {"pending", "checking", "dispatching"}:
        reserved = None
        actor_auth = cached_verified_actor_auth(auth, manager_id)
        if status == "pending" and actor_auth and manager_profile:
            reserved = start_actor_greeting_delivery(
                operation_key,
                actor_auth,
                manager_profile,
            )
        response["greeting"] = {
            "ok": True,
            "status": "queued",
            "autoSent": False,
            "text": "",
            "message": "Приветствие отправляется в фоне.",
        }
        if reserved is None:
            # Handles a restart/race or a missing short-lived actor token.  The
            # durable worker remains the fallback for still-pending jobs.
            GREETING_WAKE_EVENT.set()
    elif status == "sent":
        response["greeting"] = {
            "ok": True,
            "status": "sent",
            "autoSent": True,
            "text": job.get("text") or "",
            "message": "Приветствие автоматически отправлено клиенту.",
        }
    else:
        response["greeting"] = {
            "ok": True,
            "status": status,
            "autoSent": False,
            "text": job.get("text") or "",
            "message": "Автоприветствие не подтверждено. Проверьте чат сделки.",
        }
    return response


def claim_operation_is_stale(operation, now=None):
    raw = str((operation or {}).get("updatedAt") or "")
    try:
        updated_at = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return True
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return (current.astimezone(timezone.utc) - updated_at.astimezone(timezone.utc)).total_seconds() > CLAIM_OPERATION_PENDING_TTL_SECONDS


def preview_claim(deal_id, manager_id, auth=None, selection_token=None):
    deal_id = normalize_entity_id(deal_id)
    manager_id = normalize_entity_id(manager_id)
    if not deal_id or not manager_id:
        return {"ok": False, "message": "Некорректный ID сделки или менеджера.", "_httpStatus": 400}
    selection = decode_selection_token(selection_token, deal_id, manager_id)
    if not selection:
        return {
            "ok": False,
            "message": "Выбор сделки устарел или не принадлежит этому пользователю. Получите сделку заново.",
            "_httpStatus": 403,
        }

    selection_version = str(selection.get("version") or "")
    operation_key = claim_operation_key(deal_id, selection_version)
    semantic_rejection = rejection_semantic_key(manager_id, deal_id, selection_version)
    greeting_snapshot = None
    if GREETING_AUTO_SEND and GREETING_AUTO_SEND_SUPPORTED and not DRY_RUN:
        greeting_snapshot = cached_greeting_context(deal_id, selection_version)
    if STATE_STORE.get_rejection_by_semantic_key(semantic_rejection):
        return {
            "ok": False,
            "message": "По этой версии сделки уже сохранён отказ. Получите другую заявку.",
            "_httpStatus": 409,
        }
    if not DRY_RUN and not claim_marker_field_valid():
        return {
            "ok": False,
            "message": "Запись в CRM заблокирована: администратор не настроил маркер операции.",
            "_httpStatus": 503,
        }
    success_result = None
    suppress_replay_greeting = False
    claim_access = None
    with DATA_LOCK:
        # Reject and claim are mutually exclusive for one offered lifecycle.
        # Recheck after acquiring the same process-wide lock used by
        # ``record_rejection``; a rejection may have committed between the
        # fast-path check above and this critical section.
        if STATE_STORE.get_rejection_by_semantic_key(semantic_rejection):
            return {
                "ok": False,
                "message": "По этой версии сделки уже сохранён отказ. Получите другую заявку.",
                "_httpStatus": 409,
            }
        conflicting_deal_operations = [
            operation
            for operation in STATE_STORE.list_unresolved_claim_operations_for_deal(
                deal_id
            )
            if str(operation.get("operationKey") or "") != operation_key
        ]
        if conflicting_deal_operations:
            return {
                "ok": False,
                "message": (
                    "Предыдущая операция по этой сделке ещё сверяется с Bitrix24. "
                    "Ничего не изменено; сообщите администратору."
                ),
                "recoveryPending": True,
                "_httpStatus": 409,
            }
        existing_operation = STATE_STORE.get_claim_operation(operation_key)
        existing_attempt_timestamp = claim_operation_attempt_timestamp(existing_operation)
        if existing_operation:
            if existing_operation.get("status") == "succeeded":
                if existing_operation.get("managerId") != manager_id:
                    return {
                        "ok": False,
                        "message": "Эту сделку уже взял другой менеджер.",
                        "_httpStatus": 409,
                    }
                replay_deal = bitrix_call("crm.deal.get", {"id": deal_id}) or {}
                expected_replay_marker = str(
                    ((existing_operation.get("request") or {}).get("claimMarker")) or ""
                )
                replay_marker_matches = (
                    claim_marker_field_valid()
                    and expected_replay_marker
                    and str(replay_deal.get(BITRIX_CLAIM_MARKER_FIELD) or "")
                    == expected_replay_marker
                )
                if not replay_marker_matches:
                    return {
                        "ok": False,
                        "message": "Сделка изменилась после прошлого взятия. Получите её заново.",
                        "_httpStatus": 409,
                    }
                success_result = dict(existing_operation.get("result") or {})
                success_result.update({"ok": True, "idempotentReplay": True})

        if success_result is not None:
            actor_manager = get_manager_profile(manager_id)
            suppress_replay_greeting = (
                not actor_manager
                or actor_manager.get("active") is not True
                or actor_manager.get("intranet") is not True
                or get_manager_rule(manager_id).get("enabled") is False
            )
        else:
            actor_manager = get_manager_profile(manager_id)
            if (
                not actor_manager
                or actor_manager.get("active") is not True
                or actor_manager.get("intranet") is not True
            ):
                return {
                    "ok": False,
                    "message": "Пользователь не является активным сотрудником компании.",
                    "_httpStatus": 403,
                }
            access = check_manager_access(manager_id, allow_operation_key=operation_key)
            claim_access = access
            if not access["ok"]:
                return {
                    "ok": False,
                    "dryRun": DRY_RUN,
                    "message": access["reason"],
                    "limitReached": bool(access.get("limitReached")),
                    "takenToday": access.get("takenToday"),
                    "dailyLimit": access.get("dailyLimit"),
                    "extraClaimEnabled": bool(access.get("extraClaimEnabled")),
                    "extraClaimRequest": access.get("extraClaimRequest"),
                    "_httpStatus": 403,
                }
            if str(selection.get("policy") or "") != manager_policy_hash(
                actor_manager,
                access.get("rule"),
            ):
                return {
                    "ok": False,
                    "message": (
                        "Навыки или правила доступа изменились после выбора сделки. "
                        "Получите заявку заново."
                    ),
                    "_httpStatus": 409,
                }
            if existing_operation and existing_operation.get("status") == "pending":
                if not claim_operation_is_stale(existing_operation):
                    return {
                        "ok": False,
                        "message": "Сделка уже обрабатывается. Подождите несколько секунд и обновите страницу.",
                        "_httpStatus": 409,
                    }
                fail_claim_operation_safely(
                    operation_key,
                    "stale_pending_operation",
                    {"recoveryRequired": True},
                )
                existing_operation = STATE_STORE.get_claim_operation(operation_key)

        if success_result is None:
            deal = bitrix_call("crm.deal.get", {"id": deal_id})
            if not deal:
                return {"ok": False, "message": "Сделка не найдена.", "_httpStatus": 404}
            if (
                str(deal.get("STAGE_ID") or "") in SOURCE_STAGES
                and deal_version(deal) != selection_version
            ):
                return {
                    "ok": False,
                    "message": "Сделка изменилась после выбора. Получите её заново.",
                    "_httpStatus": 409,
                }
            current_stage = str(deal.get("STAGE_ID") or "")
            assigned_manager_id = str(deal.get("ASSIGNED_BY_ID") or "")
            original_operation_manager = str((existing_operation or {}).get("managerId") or "")

            expected_existing_marker = str(
                (((existing_operation or {}).get("request") or {}).get("claimMarker")) or ""
            )
            marker_matches_original_operation = (
                claim_marker_field_valid()
                and expected_existing_marker
                and str(deal.get(BITRIX_CLAIM_MARKER_FIELD) or "") == expected_existing_marker
            )
            live_claim_marker = str(deal.get(BITRIX_CLAIM_MARKER_FIELD) or "")
            if (
                existing_operation
                and existing_operation.get("status") == "failed"
                and original_operation_manager
                and marker_matches_original_operation
            ):
                # Snapshot the attempt-specific time before the audit retry
                # mutates operation transition timestamps.
                recovery_claim_timestamp = (
                    existing_attempt_timestamp
                    or claim_operation_attempt_timestamp(existing_operation)
                )
                recovery_request = dict(existing_operation.get("request") or {})
                recovery_request.update(
                    {"recovery": True, "claimMarker": expected_existing_marker}
                )
                operation = STATE_STORE.retry_failed_claim_operation(
                    deal_id,
                    original_operation_manager,
                    operation_key=operation_key,
                    request=recovery_request,
                )
                if not operation.get("retried"):
                    return {
                        "ok": False,
                        "message": "Сделка уже обрабатывается. Повторите через несколько секунд.",
                        "_httpStatus": 409,
                    }
                invalidate_deal_caches(deal_id)
                success_result = {
                    "ok": True,
                    "dryRun": False,
                    "dealId": deal_id,
                    "auditRecorded": True,
                    "recoveredAfterRetry": True,
                    "updated": {
                        "ASSIGNED_BY_ID": original_operation_manager,
                        "STAGE_ID": TARGET_STAGE,
                        "STAGE_NAME": TARGET_STAGE_NAME,
                    },
                    "message": "Сделка уже была назначена менеджеру; журнал восстановлен.",
                }
                try:
                    finalized_operation = STATE_STORE.finalize_claim_operation(
                        operation_key,
                        claim=claim_log_entry(
                            original_operation_manager,
                            deal,
                            timestamp=recovery_claim_timestamp,
                        ),
                        result=success_result,
                        expected_claim_marker=expected_existing_marker,
                        app_version=APP_VERSION,
                        recovered=True,
                    )
                    if (
                        finalized_operation.get("status") != "succeeded"
                        or not finalized_operation.get("claimEventId")
                        or str(finalized_operation.get("managerId") or "") != original_operation_manager
                        or str(finalized_operation.get("dealId") or "") != deal_id
                    ):
                        raise RuntimeError("claim audit recovery lost its operation lease")
                except Exception as exc:
                    fail_claim_operation_safely(
                        operation_key,
                        "audit_recovery_failed",
                        {"remoteUpdated": True},
                    )
                    sys.stderr.write(f"Claim audit recovery failed: {type(exc).__name__}\n")
                    return {
                        "ok": False,
                        "message": "Сделка уже назначена, но журнал не восстановился. Сообщите администратору.",
                        "_httpStatus": 503,
                    }
                if original_operation_manager != manager_id:
                    return {
                        "ok": False,
                        "message": "Эту сделку уже взял другой менеджер.",
                        "_httpStatus": 409,
                    }
            elif (
                existing_operation
                and existing_operation.get("status") == "failed"
                and current_stage in SOURCE_STAGES
                and live_claim_marker
            ):
                # A non-empty marker belonging to another attempt is
                # ambiguous evidence. Never overwrite it during retry. Ensure
                # even a previously "safe" failed row becomes unresolved and
                # requires an administrator to inspect Bitrix.
                try:
                    ambiguous = STATE_STORE.retry_failed_claim_operation(
                        deal_id,
                        original_operation_manager,
                        operation_key=operation_key,
                    )
                    if ambiguous.get("retried"):
                        STATE_STORE.fail_claim_operation(
                            operation_key,
                            "source_deal_has_foreign_claim_marker",
                            result={
                                "remoteUpdateUncertain": True,
                                "recoveryRequired": True,
                            },
                        )
                except Exception as exc:
                    sys.stderr.write(
                        f"Ambiguous claim marker could not be persisted: {type(exc).__name__}\n"
                    )
                return {
                    "ok": False,
                    "message": (
                        "В сделке найден маркер другой операции. "
                        "Ничего не изменено; сообщите администратору."
                    ),
                    "recoveryPending": True,
                    "_httpStatus": 409,
                }
            elif (
                current_stage in SOURCE_STAGES
                and live_claim_marker
                and not STATE_STORE.find_succeeded_claim_operation_by_marker(
                    deal_id,
                    live_claim_marker,
                )
            ):
                # Never overwrite an unknown marker. It may be the only
                # surviving evidence after a Volume restore or partial remote
                # update. A marker from a succeeded older lifecycle is the
                # only safe exception for an intentional requeue.
                return {
                    "ok": False,
                    "message": (
                        "В сделке найден неизвестный маркер предыдущей операции. "
                        "Ничего не изменено; сообщите администратору."
                    ),
                    "recoveryPending": True,
                    "_httpStatus": 409,
                }
            elif current_stage not in SOURCE_STAGES:
                return {
                    "ok": False,
                    "dryRun": DRY_RUN,
                    "message": "Сделка уже ушла из доступной стадии. Получите другую сделку.",
                    "currentStage": current_stage,
                    "_httpStatus": 409,
                }

        if success_result is None and DRY_RUN:
            return {
                "ok": True,
                "dryRun": True,
                "dealId": deal_id,
                "wouldSet": {
                    "ASSIGNED_BY_ID": manager_id,
                    "STAGE_ID": TARGET_STAGE,
                    "STAGE_NAME": TARGET_STAGE_NAME,
                },
                "message": "Проверка успешна. В безопасном режиме CRM не изменена.",
            }

        if success_result is None:
            attempt_marker = claim_attempt_marker(operation_key, manager_id)
            requires_extra_grant = bool(
                (claim_access or {}).get("extraClaimRequired")
            )
            claim_business_date = local_date()
            if requires_extra_grant:
                # This is deliberately the last external authorization check
                # before the atomic local reservation and the Bitrix write.
                # Ordinary within-limit claims never contact Baza here.
                try:
                    authoritative_extra_state = refresh_extra_claim_state(
                        manager_id,
                        claim_business_date,
                        operation_key=operation_key,
                    )
                except Exception:
                    latest_extra_state = STATE_STORE.get_extra_claim_state(
                        manager_id,
                        claim_business_date,
                        operation_key=operation_key,
                    )
                    return {
                        "ok": False,
                        "dryRun": False,
                        "message": (
                            "Не удалось подтвердить дополнительное разрешение в Базе. "
                            "CRM не изменена; повторите попытку позже."
                        ),
                        "limitReached": True,
                        "takenToday": (claim_access or {}).get("takenToday"),
                        "dailyLimit": (claim_access or {}).get("dailyLimit"),
                        "extraClaimEnabled": bool(EXTRA_CLAIM_REQUESTS_ENABLED),
                        "extraClaimRequest": latest_extra_state.get("request"),
                        "extraClaimGrantAvailable": False,
                        "integrationUnavailable": True,
                        "_httpStatus": 503,
                    }
                if not authoritative_extra_state.get(
                    "authoritativeGrantAvailable"
                ):
                    return {
                        "ok": False,
                        "dryRun": False,
                        "message": (
                            "Дополнительное разрешение отозвано, истекло или уже использовано. "
                            "CRM не изменена; обновите статус запроса."
                        ),
                        "limitReached": True,
                        "takenToday": (claim_access or {}).get("takenToday"),
                        "dailyLimit": (claim_access or {}).get("dailyLimit"),
                        "extraClaimEnabled": bool(EXTRA_CLAIM_REQUESTS_ENABLED),
                        "extraClaimRequest": authoritative_extra_state.get("request"),
                        "extraClaimGrantAvailable": False,
                        "integrationUnavailable": False,
                        "_httpStatus": 409,
                    }
            request_context = dict((existing_operation or {}).get("request") or {})
            request_context.update({
                "dealId": deal_id,
                "managerId": manager_id,
                "dealVersion": selection_version,
                "claimMarker": attempt_marker,
                "attemptStartedAt": local_now().isoformat(),
                "extraClaimRequired": requires_extra_grant,
                "businessDate": claim_business_date,
             })
            if greeting_snapshot:
                request_context["greetingRequested"] = True
                request_context["greetingContext"] = {
                    "sessionId": greeting_snapshot["openlineSessionIds"][0],
                    "direction": (
                        greeting_snapshot.get("classification") or {}
                     ).get("direction") or "Не определено",
                 }
            try:
                if existing_operation and existing_operation.get("status") == "failed":
                    if existing_operation.get("managerId") == manager_id:
                        operation = STATE_STORE.retry_failed_claim_operation(
                            deal_id,
                            manager_id,
                            operation_key=operation_key,
                            request=request_context,
                            require_extra_grant=requires_extra_grant,
                            business_date=claim_business_date,
                        )
                    else:
                        operation = STATE_STORE.reassign_failed_claim_operation(
                            deal_id,
                            manager_id,
                            operation_key=operation_key,
                            request=request_context,
                            require_extra_grant=requires_extra_grant,
                            business_date=claim_business_date,
                        )
                else:
                    operation = STATE_STORE.begin_claim_operation(
                        deal_id,
                        manager_id,
                        operation_key=operation_key,
                        request=request_context,
                        require_extra_grant=requires_extra_grant,
                        business_date=claim_business_date,
                    )
            except IdempotencyConflictError:
                return {
                    "ok": False,
                    "message": "Эту сделку уже обрабатывает другой менеджер.",
                    "_httpStatus": 409,
                }
            except ExtraClaimGrantUnavailableError:
                return {
                    "ok": False,
                    "message": (
                        "Дополнительная заявка ещё не одобрена или уже использована. "
                        "Обновите статус запроса."
                    ),
                    "limitReached": True,
                    "extraClaimEnabled": bool(EXTRA_CLAIM_REQUESTS_ENABLED),
                    "_httpStatus": 409,
                }
            except ExtraClaimGrantReconciliationRequiredError:
                return {
                    "ok": False,
                    "message": (
                        "Предыдущая попытка могла уже изменить сделку и удерживает "
                        "дополнительную заявку. Ничего не изменено; попросите "
                        "администратора сверить операцию."
                    ),
                    "recoveryPending": True,
                    "_httpStatus": 409,
                }
            if not (
                operation.get("created")
                or operation.get("retried")
                or operation.get("reassigned")
            ):
                if operation.get("status") == "succeeded":
                    if str(operation.get("managerId") or "") != manager_id:
                        return {
                            "ok": False,
                            "message": "Эту сделку уже взял другой менеджер.",
                            "_httpStatus": 409,
                        }
                    expected_race_marker = str(
                        ((operation.get("request") or {}).get("claimMarker")) or ""
                    )
                    replay_deal = bitrix_call("crm.deal.get", {"id": deal_id}) or {}
                    if (
                        not expected_race_marker
                        or str(replay_deal.get(BITRIX_CLAIM_MARKER_FIELD) or "")
                        != expected_race_marker
                    ):
                        return {
                            "ok": False,
                            "message": "Сделка изменилась во время обработки. Получите её заново.",
                            "_httpStatus": 409,
                        }
                    success_result = dict(operation.get("result") or {})
                    success_result.update({"ok": True, "idempotentReplay": True})
                else:
                    return {
                        "ok": False,
                        "message": "Сделка уже обрабатывается. Повторите через несколько секунд.",
                        "_httpStatus": 409,
                    }

        if success_result is None:
            # Re-read only after a durable attempt-specific operation lease is
            # reserved. DATA_LOCK serializes this single production process;
            # SQLite and the CRM marker protect restart/race recovery.
            live_deal = bitrix_call("crm.deal.get", {"id": deal_id})
            if not live_deal or str(live_deal.get("STAGE_ID") or "") not in SOURCE_STAGES:
                fail_claim_operation_safely(operation_key, "stage_changed_before_update")
                return {
                    "ok": False,
                    "message": "Сделка изменилась до назначения. Получите другую сделку.",
                    "_httpStatus": 409,
                }
            live_marker_before_update = str(
                live_deal.get(BITRIX_CLAIM_MARKER_FIELD) or ""
            )
            if (
                live_marker_before_update
                and not STATE_STORE.find_succeeded_claim_operation_by_marker(
                    deal_id,
                    live_marker_before_update,
                )
            ):
                fail_claim_operation_safely(
                    operation_key,
                    "unknown_claim_marker_before_update",
                    {
                        "remoteUpdated": False,
                        "recoveryRequired": True,
                    },
                )
                return {
                    "ok": False,
                    "message": (
                        "Маркер сделки изменился во время обработки. "
                        "Ничего не записано; сообщите администратору."
                    ),
                    "recoveryPending": True,
                    "_httpStatus": 409,
                }
            if deal_version(live_deal) != selection_version:
                fail_claim_operation_safely(
                    operation_key,
                    "deal_version_changed_before_update",
                    {"remoteUpdated": False},
                )
                return {
                    "ok": False,
                    "message": "Сделка изменилась до назначения. Получите её заново.",
                    "_httpStatus": 409,
                }

            update_warning = None
            try:
                update_fields = {
                    "id": deal_id,
                    "fields[ASSIGNED_BY_ID]": manager_id,
                    "fields[STAGE_ID]": TARGET_STAGE,
                    f"fields[{BITRIX_CLAIM_MARKER_FIELD}]": attempt_marker,
                }
                update_result = bitrix_call(
                    "crm.deal.update",
                    update_fields,
                )
                if not update_result:
                    raise RuntimeError("Bitrix не подтвердил обновление")
            except Exception as exc:
                update_warning = "Ответ Bitrix на обновление был потерян; состояние проверено повторно."
                sys.stderr.write(f"Claim update response error: {type(exc).__name__}\n")

            try:
                verified_deal = bitrix_call("crm.deal.get", {"id": deal_id}) or {}
            except Exception as exc:
                fail_claim_operation_safely(
                    operation_key,
                    "update_verification_failed",
                    {"remoteUpdateUncertain": True},
                )
                return {
                    "ok": False,
                    "message": "Bitrix не подтвердил итог назначения. Проверьте карточку сделки перед повтором.",
                    "remoteUpdateUncertain": True,
                    "_httpStatus": 503,
                }

            claimed = (
                str(verified_deal.get("STAGE_ID") or "") == TARGET_STAGE
                and str(verified_deal.get("ASSIGNED_BY_ID") or "") == manager_id
                and str(verified_deal.get(BITRIX_CLAIM_MARKER_FIELD) or "") == attempt_marker
            )
            if not claimed:
                fail_claim_operation_safely(
                    operation_key,
                    "post_update_state_mismatch",
                    {
                        "stageId": str(verified_deal.get("STAGE_ID") or ""),
                        "assignedById": str(verified_deal.get("ASSIGNED_BY_ID") or ""),
                        "markerMatched": str(verified_deal.get(BITRIX_CLAIM_MARKER_FIELD) or "") == attempt_marker,
                        "remoteUpdateUncertain": True,
                    },
                )
                return {
                    "ok": False,
                    "message": "Итог обновления сделки не подтверждён. Проверьте карточку и сообщите администратору.",
                    "remoteUpdateUncertain": True,
                    "_httpStatus": 503,
                }

            invalidate_deal_caches(deal_id)
            success_result = {
                "ok": True,
                "dryRun": False,
                "dealId": deal_id,
                "auditRecorded": True,
                "updated": {
                    "ASSIGNED_BY_ID": manager_id,
                    "STAGE_ID": TARGET_STAGE,
                    "STAGE_NAME": TARGET_STAGE_NAME,
                },
                "message": "Сделка назначена менеджеру и переведена в работу.",
            }
            if update_warning:
                success_result["warnings"] = [update_warning]
            try:
                finalized_operation = STATE_STORE.finalize_claim_operation(
                    operation_key,
                    claim=claim_log_entry(manager_id, verified_deal),
                    result=success_result,
                    expected_claim_marker=attempt_marker,
                    app_version=APP_VERSION,
                )
                if (
                    finalized_operation.get("status") != "succeeded"
                    or not finalized_operation.get("claimEventId")
                    or str(finalized_operation.get("managerId") or "") != manager_id
                    or str(finalized_operation.get("dealId") or "") != deal_id
                ):
                    raise RuntimeError("claim audit finalize lost its operation lease")
            except Exception as exc:
                sys.stderr.write(f"Claim audit finalize failed after CRM update: {type(exc).__name__}\n")
                fail_claim_operation_safely(
                    operation_key,
                    "audit_finalize_failed_after_remote_update",
                    {"remoteUpdated": True},
                )
                success_result["auditRecorded"] = False
                success_result.setdefault("warnings", []).append(
                    "Сделка взята, но журнал временно не записался. Сообщите администратору."
                )

    if suppress_replay_greeting:
        return success_result
    return attach_greeting_to_claim(
        success_result,
        manager_id,
        deal_id,
        auth,
        operation_key,
        actor_manager,
    )


def reconcile_stale_claim_operations(limit=None):
    """Recover audit events after a process died around a CRM update.

    This maintenance path never changes Bitrix. It only compares stale local
    leases with live CRM state, records an exact confirmed claim, or releases a
    lease whose deal is still in a source stage.
    """

    summary = {"checked": 0, "recovered": 0, "released": 0, "conflicts": 0, "errors": 0}
    candidates = list(STATE_STORE.list_claim_operations(status="pending"))
    for failed in STATE_STORE.list_claim_operations(status="failed"):
        failed_result = failed.get("result") or {}
        if (
            failed_result.get("remoteUpdated")
            or failed_result.get("remoteUpdateUncertain")
            or failed_result.get("recoveryRequired")
        ):
            candidates.append(failed)
    for listed_operation in candidates[: max(0, int(limit or CLAIM_RECONCILE_BATCH_SIZE))]:
        if listed_operation.get("status") == "pending" and not claim_operation_is_stale(listed_operation):
            continue
        with DATA_LOCK:
            operation_key = listed_operation.get("operationKey")
            operation = STATE_STORE.get_claim_operation(operation_key)
            if not operation or operation.get("status") not in {"pending", "failed"}:
                continue
            if operation.get("status") == "pending" and not claim_operation_is_stale(operation):
                continue
            summary["checked"] += 1
            deal_id = str(operation.get("dealId") or "")
            manager_id = str(operation.get("managerId") or "")
            try:
                deal = bitrix_call("crm.deal.get", {"id": deal_id}) or {}
            except Exception as exc:
                summary["errors"] += 1
                sys.stderr.write(f"Claim reconciliation read failed: {type(exc).__name__}\n")
                continue
            stage_id = str(deal.get("STAGE_ID") or "")
            assigned_manager_id = str(deal.get("ASSIGNED_BY_ID") or "")
            expected_marker = str(((operation.get("request") or {}).get("claimMarker")) or "")
            expected_deal_version = str(
                ((operation.get("request") or {}).get("dealVersion")) or ""
            )
            marker_matches = (
                claim_marker_field_valid()
                and expected_marker
                and str(deal.get(BITRIX_CLAIM_MARKER_FIELD) or "") == expected_marker
            )
            if marker_matches:
                # Snapshot the attempt-specific time before retry/finalize
                # rewrites operation transition timestamps.
                recovery_claim_timestamp = claim_operation_attempt_timestamp(operation)
                if operation.get("status") == "failed":
                    recovery_request = dict(operation.get("request") or {})
                    recovery_request.update(
                        {"maintenanceRecovery": True, "claimMarker": expected_marker}
                    )
                    operation = STATE_STORE.retry_failed_claim_operation(
                        deal_id,
                        manager_id,
                        operation_key=operation_key,
                        request=recovery_request,
                    )
                    if not operation.get("retried"):
                        summary["conflicts"] += 1
                        continue
                result = {
                    "ok": True,
                    "dryRun": False,
                    "dealId": deal_id,
                    "auditRecorded": True,
                    "recoveredByMaintenance": True,
                    "updated": {
                        "ASSIGNED_BY_ID": manager_id,
                        "STAGE_ID": TARGET_STAGE,
                        "STAGE_NAME": TARGET_STAGE_NAME,
                    },
                    "message": "Журнал взятия восстановлен после перезапуска.",
                }
                finalized = STATE_STORE.finalize_claim_operation(
                    operation_key,
                    claim=claim_log_entry(
                        manager_id,
                        deal,
                        timestamp=recovery_claim_timestamp,
                    ),
                    result=result,
                    expected_claim_marker=expected_marker,
                    app_version=APP_VERSION,
                    recovered=True,
                )
                if finalized.get("status") == "succeeded" and finalized.get("claimEventId"):
                    summary["recovered"] += 1
                else:
                    summary["conflicts"] += 1
            elif (
                stage_id in SOURCE_STAGES
                and expected_marker
                and not str(deal.get(BITRIX_CLAIM_MARKER_FIELD) or "")
                and expected_deal_version
                and deal_version(deal) == expected_deal_version
            ):
                # Exact unchanged source state proves this attempt did not
                # apply. Clear its provisional quota block while preserving
                # the prior uncertain result in attemptHistory.
                if operation.get("status") == "failed":
                    operation = STATE_STORE.retry_failed_claim_operation(
                        deal_id,
                        manager_id,
                        operation_key=operation_key,
                    )
                    if not operation.get("retried"):
                        summary["conflicts"] += 1
                        continue
                failed = STATE_STORE.fail_claim_operation(
                    operation_key,
                    "unapplied_claim_released_by_maintenance",
                    result={"remoteUpdated": False, "reconciledSource": True},
                )
                if failed.get("status") == "failed":
                    summary["released"] += 1
                else:
                    summary["conflicts"] += 1
            elif stage_id not in SOURCE_STAGES:
                STATE_STORE.fail_claim_operation(
                    operation_key,
                    "stale_pending_conflicts_with_live_crm",
                    result={
                        "remoteUpdateUncertain": True,
                        "recoveryRequired": True,
                        "stageId": stage_id,
                    },
                )
                summary["conflicts"] += 1
            else:
                # The source deal changed, retained a marker, or came from an
                # older operation without a version. Do not guess: keep the
                # manager blocked and expose the unresolved item to admin.
                summary["conflicts"] += 1
    return summary


def claim_reconciliation_loop():
    while True:
        try:
            if readiness_state().get("ok"):
                summary = reconcile_stale_claim_operations()
                if summary["recovered"] or summary["released"] or summary["conflicts"] or summary["errors"]:
                    sys.stderr.write(
                        "Claim reconciliation: "
                        + " ".join(f"{key}={value}" for key, value in summary.items())
                        + "\n"
                    )
        except Exception as exc:
            sys.stderr.write(f"Claim reconciliation failed: {type(exc).__name__}\n")
        time.sleep(CLAIM_RECONCILE_INTERVAL_SECONDS)


def record_rejection(manager_id, payload):
    manager_id = normalize_entity_id(manager_id)
    deal_id = normalize_entity_id(payload.get("dealId"))
    if not manager_id or not deal_id:
        return {"ok": False, "message": "Некорректный ID сделки или менеджера."}
    selection_token = payload.get("selectionToken")
    selection = decode_selection_token(selection_token, deal_id, manager_id)
    if not selection:
        return {"ok": False, "message": "Выбор сделки устарел. Получите сделку заново."}
    selection_version = str(selection.get("version") or "")
    semantic_key = rejection_semantic_key(manager_id, deal_id, selection_version)
    operation_key = claim_operation_key(deal_id, selection_version)
    reason = normalize_reject_reason(payload.get("reason"))
    with DATA_LOCK:
        token_hash = hashlib.sha256(str(selection_token).encode("utf-8")).hexdigest()
        existing = (
            STATE_STORE.get_rejection_by_token_hash(token_hash)
            or STATE_STORE.get_rejection_by_semantic_key(semantic_key)
        )
        if existing:
            return {
                "ok": True,
                "dealId": deal_id,
                "reason": existing.get("reason") or reason,
                "reasonLabel": existing.get("reasonLabel") or REJECT_REASONS[reason],
                "idempotentReplay": True,
                "message": "Отказ уже был сохранён.",
            }
        conflicting_deal_operations = [
            operation
            for operation in STATE_STORE.list_unresolved_claim_operations_for_deal(
                deal_id
            )
            if str(operation.get("operationKey") or "") != operation_key
        ]
        if conflicting_deal_operations:
            return {
                "ok": False,
                "message": (
                    "Предыдущая операция по этой сделке ещё сверяется с Bitrix24. "
                    "Отказ не сохранён; сообщите администратору."
                ),
            }
        actor_manager = get_manager_profile(manager_id)
        if (
            not actor_manager
            or actor_manager.get("active") is not True
            or actor_manager.get("intranet") is not True
        ):
            return {"ok": False, "message": "Пользователь не является активным сотрудником компании."}
        configured_rules = STATE_STORE.list_rules()
        if (
            REQUIRE_EXPLICIT_ACCESS_RULE
            and not is_unverified_dev_mode()
            and manager_id not in configured_rules
        ):
            return {"ok": False, "message": "Администратор не открыл доступ к выдаче заявок."}
        if get_manager_rule(manager_id).get("enabled") is False:
            return {"ok": False, "message": "Для пользователя закрыт доступ к выдаче заявок."}
        current_rule = get_manager_rule(manager_id)
        if str(selection.get("policy") or "") != manager_policy_hash(
            actor_manager,
            current_rule,
        ):
            return {
                "ok": False,
                "message": "Навыки или правила доступа изменились. Получите заявку заново.",
            }
        claim_operation = STATE_STORE.get_claim_operation(operation_key)
        claim_result = (claim_operation or {}).get("result") or {}
        if claim_operation and (
            claim_operation.get("status") in {"pending", "succeeded"}
            or claim_result.get("remoteUpdated")
            or claim_result.get("remoteUpdateUncertain")
        ):
            return {
                "ok": False,
                "message": "Сделка уже находится в процессе взятия и не может быть одновременно отклонена.",
            }
        deal = bitrix_call("crm.deal.get", {"id": deal_id}) or {}
        if str(deal.get("STAGE_ID") or "") not in SOURCE_STAGES:
            return {"ok": False, "message": "Сделка уже ушла из доступной стадии."}
        if deal_version(deal) != selection_version:
            return {"ok": False, "message": "Сделка изменилась после выбора. Получите её заново."}
        cached_deal = None
        with DEAL_ANALYSIS_CACHE_LOCK:
            cached = DEAL_ANALYSIS_CACHE.get(deal_id)
            if cached:
                cached_deal = dict(cached.get("deal") or {})
        classification = (cached_deal or {}).get("classification") or {}
        try:
            STATE_STORE.append_reject(
                {
                    "timestamp": local_now().isoformat(),
                    "managerId": manager_id,
                    "dealId": deal_id,
                    "stageId": deal.get("STAGE_ID") or "",
                    "direction": classification.get("direction") or "",
                    "reason": reason,
                    "reasonLabel": REJECT_REASONS[reason],
                    "selectionTokenHash": token_hash,
                    "semanticKey": semantic_key,
                }
            )
        except Exception:
            existing = (
                STATE_STORE.get_rejection_by_token_hash(token_hash)
                or STATE_STORE.get_rejection_by_semantic_key(semantic_key)
            )
            if existing:
                return {
                    "ok": True,
                    "dealId": deal_id,
                    "reason": existing.get("reason") or reason,
                    "reasonLabel": existing.get("reasonLabel") or REJECT_REASONS[reason],
                    "idempotentReplay": True,
                    "message": "Отказ уже был сохранён.",
                }
            raise
    return {
        "ok": True,
        "dealId": deal_id,
        "reason": reason,
        "reasonLabel": REJECT_REASONS[reason],
        "message": "Отказ сохранен.",
    }


def list_portal_users():
    now = time.monotonic()
    if PORTAL_USERS_CACHE and now - PORTAL_USERS_CACHE.get("cachedAt", 0) < PORTAL_USERS_CACHE_TTL_SECONDS:
        return list(PORTAL_USERS_CACHE["users"])
    users = bitrix_list_all(
        "user.search",
        {
            "FILTER[ACTIVE]": "Y",
            "SORT": "LAST_NAME",
            "ORDER": "ASC",
        },
        max_items=5000,
        timeout=BITRIX_TIMEOUT_SECONDS,
    )
    PORTAL_USERS_CACHE["users"] = users
    PORTAL_USERS_CACHE["cachedAt"] = now
    return list(users)


def admin_rule_state(manager_id, configured_rules):
    rule = dict(get_manager_rule(manager_id))
    rule["configured"] = str(manager_id) in configured_rules
    if REQUIRE_EXPLICIT_ACCESS_RULE and not rule["configured"]:
        rule["enabled"] = False
    return rule


def admin_state(payload):
    admin = require_admin(payload)
    if not admin:
        return {"ok": False, "isAdmin": False, "message": "Этот раздел доступен только администратору."}

    today = local_date()
    date_from = normalize_date(payload.get("dateFrom"), today)
    date_to = normalize_date(payload.get("dateTo"), date_from)
    if date_to < date_from:
        raise ValueError("Дата окончания не может быть раньше даты начала")
    rules = STATE_STORE.list_rules()
    manager_ids = set(STATE_STORE.list_manager_ids()) | set(rules)
    reject_log = STATE_STORE.list_rejections(
        date_from=min(date_from, today),
        date_to=max(date_to, today),
    )
    warnings = [
        "Показаны только точные события этого приложения. Ручные переводы в Bitrix24 в разбивку по менеджерам не включены."
    ]
    unresolved_claim_operations = 0
    for operation in STATE_STORE.list_claim_operations():
        operation_result = operation.get("result") or {}
        if (
            (operation.get("status") == "pending" and claim_operation_is_stale(operation))
            or operation_result.get("remoteUpdated")
            or operation_result.get("remoteUpdateUncertain")
            or operation_result.get("recoveryRequired")
        ):
            unresolved_claim_operations += 1
    if unresolved_claim_operations:
        warnings.append(
            f"Есть операций взятия, ожидающих сверки с Bitrix24: {unresolved_claim_operations}."
        )
    try:
        portal_users = list_portal_users()
    except Exception as exc:
        portal_users = []
        warnings.append(f"Не удалось загрузить список пользователей портала: {exc}")
    try:
        profiles_by_id = get_manager_profiles_bulk(portal_users)
    except Exception as exc:
        profiles_by_id = {}
        warnings.append(f"Не удалось загрузить профили менеджеров: {exc}")

    rows = []
    for user in portal_users:
        manager_id = str(user.get("ID") or "")
        if not manager_id:
            continue
        profile = profiles_by_id.get(manager_id) or {}
        competencies = profile.get("competencies") or []
        manager_ids.discard(manager_id)
        rule = admin_rule_state(manager_id, rules)
        rows.append(
            {
                "id": manager_id,
                "name": profile.get("name") or " ".join(part for part in [user.get("NAME"), user.get("LAST_NAME")] if part).strip() or manager_id,
                "competencies": competencies,
                "rule": rule,
                "takenInPeriod": STATE_STORE.count_claims(manager_id, date_from, date_to),
                "takenToday": STATE_STORE.count_claims(manager_id, today, today),
                "rejectedInPeriod": STATE_STORE.count_rejections(manager_id, date_from, date_to),
                "rejectedToday": STATE_STORE.count_rejections(manager_id, today, today),
                "topRejectReason": rejection_reason_summary(reject_log, manager_id, date_from, date_to),
            }
        )

    for manager_id in sorted(manager_ids):
        profile = get_manager_profile(manager_id) or {"id": manager_id, "name": manager_id, "competencies": []}
        rows.append(
            {
                "id": manager_id,
                "name": profile.get("name") or manager_id,
                "competencies": profile.get("competencies") or [],
                "rule": admin_rule_state(manager_id, rules),
                "takenInPeriod": STATE_STORE.count_claims(manager_id, date_from, date_to),
                "takenToday": STATE_STORE.count_claims(manager_id, today, today),
                "rejectedInPeriod": STATE_STORE.count_rejections(manager_id, date_from, date_to),
                "rejectedToday": STATE_STORE.count_rejections(manager_id, today, today),
                "topRejectReason": rejection_reason_summary(reject_log, manager_id, date_from, date_to),
            }
        )

    rows.sort(key=lambda item: item["name"].lower())
    result = {
        "ok": True,
        "isAdmin": True,
        "admin": {"id": admin["id"], "name": admin.get("name")},
        "dateFrom": date_from,
        "dateTo": date_to,
        "today": today,
        "statsSource": "app_events",
        "statsLabel": "Взято через приложение",
        "managers": rows,
    }
    if warnings:
        result["warnings"] = warnings
    return result


def update_admin_rule(payload):
    admin = require_admin(payload)
    if not admin:
        return {"ok": False, "isAdmin": False, "message": "Недостаточно прав."}
    manager_id = normalize_entity_id(payload.get("managerId"))
    if not manager_id:
        return {"ok": False, "message": "Некорректный ID менеджера."}
    enabled = payload.get("enabled", True)
    if not isinstance(enabled, bool):
        raise ValueError("Поле enabled должно быть true или false")
    # Serialize policy changes with claim/reject critical sections. Once the
    # administrator receives a successful disable response, no operation that
    # passed the old rule can still be waiting to write Bitrix.
    with DATA_LOCK:
        rule = set_manager_rule(
            manager_id,
            enabled=enabled,
            daily_limit=payload.get("dailyLimit"),
            note=payload.get("note") or "",
        )
    return {"ok": True, "managerId": manager_id, "rule": rule}


def current_user_state(payload):
    user = verify_bitrix_user(payload.get("auth"), allow_cached=False)
    if not user:
        return {"ok": False, "message": "Не удалось определить авторизованного пользователя Битрикс."}
    manager = get_manager_profile(user["id"])
    raw = user.get("raw") or {}
    public_user = {
        "id": user["id"],
        "name": user.get("name") or "",
        "isAdmin": bool(
            user["id"] in ADMIN_USER_IDS
            and bitrix_boolean(raw.get("ACTIVE"), default=False)
            and is_intranet_user(raw)
        ),
        "raw": {
            "ID": user["id"],
            "NAME": raw.get("NAME") or "",
            "LAST_NAME": raw.get("LAST_NAME") or "",
        },
    }
    return {"ok": True, "user": public_user, "manager": manager}


def extract_initial_auth(raw_payload):
    if not raw_payload:
        return {}
    try:
        parsed = urllib.parse.parse_qs(raw_payload.decode("utf-8", errors="ignore"))
    except Exception:
        return {}
    return extract_initial_auth_from_values(parsed)


def extract_initial_auth_from_values(parsed):
    parsed = parsed or {}

    def pick(*keys):
        for key in keys:
            values = parsed.get(key)
            if values and values[0]:
                return values[0]
        return ""

    auth = {
        "AUTH_ID": pick("AUTH_ID", "auth", "access_token"),
        "DOMAIN": pick("DOMAIN", "domain"),
        "member_id": pick("member_id", "MEMBER_ID"),
        "client_endpoint": pick("client_endpoint", "CLIENT_ENDPOINT"),
    }
    return {key: value for key, value in auth.items() if value}


def sanitize_initial_auth(initial_auth):
    token, domain = extract_auth_credentials(initial_auth)
    if not token or not domain:
        return {}
    return {"AUTH_ID": token, "DOMAIN": domain}


def render_index_html(install_mode=False, initial_auth=None, nonce=""):
    return (
        INDEX_HTML
        .replace("__PUBLIC_APP_URL__", json_for_script(PUBLIC_APP_URL))
        .replace("__INSTALL_MODE__", "true" if install_mode else "false")
        .replace("__INITIAL_AUTH__", json_for_script(sanitize_initial_auth(initial_auth or {})))
        .replace("__ALLOW_UNVERIFIED_USERS__", "true" if is_unverified_dev_mode() else "false")
        .replace("__CSP_NONCE__", str(nonce or ""))
    )


def looks_like_install_payload(raw_payload):
    if not raw_payload:
        return False
    try:
        parsed = urllib.parse.parse_qs(
            raw_payload.decode("utf-8", errors="ignore"),
            keep_blank_values=True,
        )
    except (TypeError, ValueError):
        return False
    normalized = {
        str(key).strip().lower(): [str(value).strip().lower() for value in values]
        for key, values in parsed.items()
    }
    return (
        "y" in normalized.get("install", [])
        or "onappinstall" in normalized.get("event", [])
    )


def _compute_readiness_state():
    errors = []
    for name in sorted(INVALID_ENV_VALUES):
        errors.append(f"Некорректное значение переменной {name}")
    try:
        load_env()
    except Exception as exc:
        errors.append(str(exc))
    if not RAW_BITRIX_ALLOWED_DOMAINS:
        errors.append("Не задан BITRIX_ALLOWED_DOMAINS")
    for raw_domain in sorted(RAW_BITRIX_ALLOWED_DOMAINS):
        if not normalize_allowed_hostname(raw_domain):
            errors.append("BITRIX_ALLOWED_DOMAINS содержит некорректный домен")
    webhook_domain = webhook_bitrix_domain()
    if webhook_domain and ALLOWED_BITRIX_DOMAINS != {webhook_domain}:
        errors.append("BITRIX_ALLOWED_DOMAINS должен точно совпадать с доменом server webhook")
    if not configured_bitrix_domains():
        errors.append("Не задан разрешённый домен Bitrix24")
    if CLAIM_STATS_SOURCE != "app_events":
        errors.append("CLAIM_STATS_SOURCE поддерживает только точный режим app_events")
    if baza_integration_enabled():
        if not baza_base_url_valid():
            errors.append("BAZA_API_BASE_URL должен быть корректным HTTPS URL")
        if not BAZA_HMAC_KEY_ID:
            errors.append("Не задан BAZA_HMAC_KEY_ID")
        if (
            len(BAZA_HMAC_SECRET.encode("utf-8")) < 32
            or "REPLACE" in BAZA_HMAC_SECRET.upper()
        ):
            errors.append("BAZA_HMAC_SECRET должен содержать не менее 32 байт")
    if not REQUIRE_EXPLICIT_ACCESS_RULE and not is_unverified_dev_mode():
        errors.append("REQUIRE_EXPLICIT_ACCESS_RULE должен быть включён вне локального DRY_RUN")
    if GREETING_AUTO_SEND and not GREETING_AUTO_SEND_SUPPORTED:
        errors.append("GREETING_AUTO_SEND=1 запрещён до безопасной привязки чата к сделке")
    if not DRY_RUN and not claim_marker_field_valid():
        errors.append("Для DRY_RUN=0 требуется корректный BITRIX_CLAIM_MARKER_FIELD")
    if not ADMIN_USER_IDS:
        errors.append("Не задан ADMIN_USER_IDS")
    elif any(not item.isdigit() for item in ADMIN_USER_IDS):
        errors.append("ADMIN_USER_IDS должен содержать только числовые ID")
    for raw_origin in sorted(RAW_APP_ALLOWED_ORIGINS):
        parsed = safe_urlparse(raw_origin)
        normalized = url_origin(raw_origin)
        if (
            parsed is None
            or not normalized
            or raw_origin != normalized
            or "*" in raw_origin
            or parsed.path not in {"", "/"}
        ):
            errors.append("APP_ALLOWED_ORIGINS содержит некорректный origin")
        elif parsed.scheme != "https" and not is_loopback_http_url(raw_origin):
            errors.append("APP_ALLOWED_ORIGINS должен использовать HTTPS")
    if not PUBLIC_APP_URL:
        errors.append("Не задан PUBLIC_APP_URL")
    else:
        public_url = safe_urlparse(PUBLIC_APP_URL)
        if (
            public_url is None
            or not url_origin(PUBLIC_APP_URL)
            or (public_url.scheme != "https" and not is_loopback_http_url(PUBLIC_APP_URL))
            or public_url.path not in {"", "/"}
        ):
            errors.append("PUBLIC_APP_URL должен быть корректным HTTPS URL")
    if not APP_ALLOWED_ORIGINS:
        errors.append("Не задан APP_ALLOWED_ORIGINS")
    trusted_origins = {
        item
        for item in (
            url_origin(PUBLIC_APP_URL),
            f"https://{webhook_domain}" if webhook_domain else "",
        )
        if item
    }
    if not is_local_runtime() and any(
        origin not in trusted_origins for origin in APP_ALLOWED_ORIGINS
    ):
        errors.append("APP_ALLOWED_ORIGINS содержит недоверенный production origin")
    if is_railway_runtime():
        volume_name = str(os.environ.get("RAILWAY_VOLUME_NAME") or "").strip()
        volume_mount = str(os.environ.get("RAILWAY_VOLUME_MOUNT_PATH") or "").strip()
        if not volume_name or not volume_mount:
            errors.append("Railway Volume не подключён к сервису")
        else:
            try:
                mount_path = Path(volume_mount).expanduser().resolve()
                app_data_path = APP_DIR.expanduser().resolve()
            except (OSError, RuntimeError):
                errors.append("Railway Volume имеет некорректный mount path")
            else:
                if mount_path != app_data_path:
                    errors.append("Railway Volume подключён не в APP_DATA_DIR")
    if APP_DIR.exists():
        if not APP_DIR.is_dir():
            errors.append("APP_DATA_DIR не является каталогом")
        elif not os.access(APP_DIR, os.R_OK | os.W_OK):
            errors.append("APP_DATA_DIR недоступен для чтения/записи")
    else:
        existing_parent = APP_DIR.parent
        while not existing_parent.exists() and existing_parent != existing_parent.parent:
            existing_parent = existing_parent.parent
        if not existing_parent.is_dir() or not os.access(existing_parent, os.W_OK):
            errors.append("APP_DATA_DIR нельзя создать")
    try:
        managers = load_managers()
        if not isinstance(managers, list):
            errors.append("managers.json должен содержать список")
    except Exception:
        errors.append("managers.json повреждён")
    if not errors:
        STATE_STORE.initialize(require_complete_legacy_set=REQUIRE_LEGACY_MIGRATION)
    storage_state = STATE_STORE.readiness_check()
    if not storage_state.get("ok") and (storage_state.get("migration") or {}).get("state") != "not_initialized":
        errors.append("SQLite-хранилище или миграция данных не готовы")
        sys.stderr.write(f"State store readiness error: {storage_state.get('error', 'unknown')}\n")
    migration_state = (storage_state.get("migration") or {}).get("state")
    if REQUIRE_LEGACY_MIGRATION and migration_state != "completed":
        errors.append("Ожидается обязательная миграция legacy JSON")
    lost_deal_autoclose_armed = False
    if storage_state.get("ok"):
        try:
            lost_deal_autoclose_armed = (
                STATE_STORE.get_lost_deal_autoclose_boundary() is not None
            )
        except Exception as exc:
            errors.append("Граница автозавершения диалогов повреждена")
            sys.stderr.write(
                f"Lost-deal auto-close readiness error: {type(exc).__name__}\n"
            )
    return {
        "ok": not errors,
        "version": APP_VERSION,
        "errors": errors,
        "dryRun": DRY_RUN,
        "lostDealAutoclose": {
            "enabled": bool(LOST_DEAL_AUTOCLOSE_ENABLED),
            "armed": bool(lost_deal_autoclose_armed),
        },
        "greetingAutoSend": bool(
            GREETING_AUTO_SEND and GREETING_AUTO_SEND_SUPPORTED and not DRY_RUN
        ),
        "storage": {
            "ok": bool(storage_state.get("ok")),
            "schemaVersion": storage_state.get("schemaVersion"),
            "journalMode": storage_state.get("journalMode"),
            "synchronous": storage_state.get("synchronous"),
            "migration": {
                "state": (storage_state.get("migration") or {}).get("state"),
            },
        },
    }


def readiness_state(*, force=False):
    """Return a short-lived, serialized readiness snapshot.

    Public API requests and Railway health probes must not fan out concurrent
    SQLite integrity scans. ``force`` is for hermetic tests and diagnostics.
    """

    now = time.monotonic()
    cached = READINESS_CACHE.get("state")
    if (
        not force
        and cached is not None
        and now - READINESS_CACHE.get("checkedAt", 0) < READINESS_CACHE_TTL_SECONDS
    ):
        return cached
    with READINESS_CACHE_LOCK:
        now = time.monotonic()
        cached = READINESS_CACHE.get("state")
        if (
            not force
            and cached is not None
            and now - READINESS_CACHE.get("checkedAt", 0) < READINESS_CACHE_TTL_SECONDS
        ):
            return cached
        state = _compute_readiness_state()
        READINESS_CACHE.update({"checkedAt": now, "state": state})
        return state


class Handler(BaseHTTPRequestHandler):
    server_version = "KrugosvetDealPicker/1"

    def request_origin_allowed(self):
        origin = str(self.headers.get("Origin") or "").strip().rstrip("/")
        if not origin:
            return True
        normalized_origin = url_origin(origin)
        if normalized_origin and origin == normalized_origin and origin in APP_ALLOWED_ORIGINS:
            return True
        if is_unverified_dev_mode():
            parsed = safe_urlparse(origin)
            try:
                return bool(
                    parsed is not None
                    and parsed.scheme == "http"
                    and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
                )
            except (TypeError, ValueError):
                return False
        return False

    def client_key(self):
        candidate = str(self.client_address[0])
        if is_railway_runtime() and self.headers.get("X-Railway-Edge"):
            candidate = str(self.headers.get("X-Real-IP") or candidate).strip()
        try:
            return ipaddress.ip_address(candidate).compressed
        except ValueError:
            return "invalid-client-ip"

    def setup(self):
        super().setup()
        self.connection.settimeout(SOCKET_TIMEOUT_SECONDS)

    def send_common_headers(self, nonce=None):
        self.send_header("Cache-Control", "no-store")
        self.send_header("Pragma", "no-cache")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        origin = str(self.headers.get("Origin") or "").strip().rstrip("/")
        if origin and self.request_origin_allowed():
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
        if nonce is not None:
            frame_ancestors = " ".join(
                f"https://{domain}" for domain in sorted(configured_bitrix_domains())
            ) or "'none'"
            policy = [
                "default-src 'self'",
                f"script-src 'self' 'nonce-{nonce}' https://api.bitrix24.com",
                f"style-src 'self' 'nonce-{nonce}'",
                "connect-src 'self'",
                "img-src 'self' data:",
                f"frame-ancestors {frame_ancestors}",
                "base-uri 'none'",
                "form-action 'self'",
                "object-src 'none'",
            ]
            self.send_header("Content-Security-Policy", "; ".join(policy))

    def send_json(self, payload, status=200):
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_common_headers()
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, install_mode=False, initial_auth=None, status=200):
        nonce = secrets.token_urlsafe(24)
        body = render_index_html(install_mode, initial_auth, nonce).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_common_headers(nonce=nonce)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_body(self):
        if self.headers.get("Transfer-Encoding"):
            raise ValueError("Transfer-Encoding не поддерживается")
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ValueError("Некорректный Content-Length") from exc
        if length < 0 or length > MAX_REQUEST_BODY_BYTES:
            raise OverflowError("Тело запроса слишком большое")
        return self.rfile.read(length) if length else b""

    def read_json(self):
        raw = self.read_body()
        try:
            payload = json.loads(raw or b"{}")
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("Некорректный JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("JSON-запрос должен быть объектом")
        return payload

    def do_OPTIONS(self):
        if not self.request_origin_allowed():
            self.send_json({"ok": False, "message": "Origin не разрешён"}, 403)
            return
        self.send_response(204)
        self.send_common_headers()
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Max-Age", "600")
        self.end_headers()

    def do_GET(self):
        parsed = safe_urlparse(self.path)
        if parsed is None:
            self.send_json({"ok": False, "message": "Некорректный URL запроса"}, 400)
            return
        if parsed.path == "/api/health":
            self.send_json({"ok": True, "version": APP_VERSION})
            return
        if parsed.path == "/api/ready":
            state = readiness_state()
            self.send_json(state, 200 if state.get("ok") else 503)
            return
        if parsed.path == "/install":
            self.send_html(True, {})
            return
        if parsed.path == "/":
            self.send_html(False, {})
            return
        self.send_json({"ok": False, "message": "Маршрут не найден"}, 404)

    def do_POST(self):
        parsed = safe_urlparse(self.path)
        if parsed is None:
            self.send_json({"ok": False, "message": "Некорректный URL запроса"}, 400)
            return
        if parsed.path in {"/", "/install"}:
            try:
                raw_payload = self.read_body()
            except OverflowError as exc:
                self.send_json({"ok": False, "message": str(exc)}, 413)
                return
            except ValueError as exc:
                self.send_json({"ok": False, "message": str(exc)}, 400)
                return
            install_mode = parsed.path == "/install" or looks_like_install_payload(raw_payload)
            self.send_html(install_mode, extract_initial_auth(raw_payload))
            return
        if not parsed.path.startswith("/api/"):
            self.send_json({"ok": False, "message": "Маршрут не найден"}, 404)
            return
        if not self.request_origin_allowed():
            self.send_json({"ok": False, "message": "Origin не разрешён"}, 403)
            return
        if not rate_limit_allowed(self.client_key()):
            self.send_json({"ok": False, "message": "Слишком много запросов. Повторите позже."}, 429)
            return
        if not readiness_state().get("ok"):
            self.send_json({"ok": False, "message": "Сервис ещё не готов. Обратитесь к администратору."}, 503)
            return
        try:
            payload = self.read_json()
            if parsed.path == "/api/dev/managers":
                if not is_unverified_dev_mode():
                    self.send_json({"ok": False, "message": "Маршрут доступен только локально в DRY_RUN"}, 403)
                    return
                self.send_json(
                    {
                        "ok": True,
                        "managers": [
                            {**manager, "intranet": True}
                            for manager in load_managers()
                            if isinstance(manager, dict)
                        ],
                    }
                )
                return
            if parsed.path == "/api/next-deal":
                manager_id = actor_id_from_payload(payload, allow_cached=False)
                if not manager_id:
                    self.send_json({"deal": None, "reason": "Не удалось подтвердить пользователя Битрикс."}, 401)
                    return
                extra_state = (
                    extra_claim_limit_state(manager_id, refresh=True)
                    if EXTRA_CLAIM_REQUESTS_ENABLED
                    else {"enabled": False}
                )
                if (
                    extra_state.get("enabled")
                    and extra_state.get("limitReached")
                    and extra_state.get("integrationUnavailable")
                ):
                    self.send_json(
                        {
                            "deal": None,
                            "reason": (
                                "Не удалось подтвердить дополнительное разрешение в Базе. "
                                "Ничего не изменено; повторите позже."
                            ),
                            "limitReached": True,
                            "takenToday": extra_state.get("takenToday"),
                            "dailyLimit": extra_state.get("dailyLimit"),
                            "extraClaimEnabled": True,
                            "extraClaimConfigured": bool(extra_state.get("configured")),
                            "extraClaimRequest": extra_state.get("request"),
                            "extraClaimGrantAvailable": False,
                            "integrationUnavailable": True,
                        },
                        200,
                    )
                    return
                result = get_next_deal_for_manager(
                    manager_id, payload.get("continuationToken")
                )
                status = int(result.pop("_httpStatus", 200))
                self.send_json(result, status)
                return
            if parsed.path == "/api/claim":
                # The page and search have already verified this exact OAuth
                # token + portal pair. Reuse that short-lived server-side
                # identity here so a transient Bitrix OAuth timeout cannot
                # block a signed claim after the manager has selected a deal.
                # Manager IDs supplied by the browser remain ignored.
                manager_id = actor_id_from_payload(payload, allow_cached=True)
                if not manager_id:
                    self.send_json({"ok": False, "message": "Не удалось подтвердить пользователя Битрикс."}, 401)
                    return
                extra_state = (
                    extra_claim_limit_state(manager_id, refresh=True)
                    if EXTRA_CLAIM_REQUESTS_ENABLED
                    else {"enabled": False}
                )
                if (
                    extra_state.get("enabled")
                    and extra_state.get("limitReached")
                    and extra_state.get("integrationUnavailable")
                ):
                    self.send_json(
                        {
                            "ok": False,
                            "message": (
                                "Не удалось подтвердить дополнительное разрешение в Базе. "
                                "CRM не изменена; повторите попытку позже."
                            ),
                            "limitReached": True,
                            "takenToday": extra_state.get("takenToday"),
                            "dailyLimit": extra_state.get("dailyLimit"),
                            "extraClaimEnabled": True,
                            "extraClaimRequest": extra_state.get("request"),
                            "extraClaimGrantAvailable": False,
                            "integrationUnavailable": True,
                        },
                        503,
                    )
                    return
                result = preview_claim(
                    payload.get("dealId"),
                    manager_id,
                    payload.get("auth"),
                    payload.get("selectionToken"),
                )
                status = int(result.pop("_httpStatus", 200))
                self.send_json(result, status)
                return
            if parsed.path == "/api/extra-claim/status":
                manager_id = actor_id_from_payload(payload, allow_cached=False)
                if not manager_id:
                    self.send_json(
                        {"ok": False, "message": "Не удалось подтвердить пользователя Битрикс."},
                        401,
                    )
                    return
                self.send_json(extra_claim_limit_state(manager_id, refresh=True))
                return
            if parsed.path == "/api/extra-claim/request":
                manager_id = actor_id_from_payload(payload, allow_cached=False)
                if not manager_id:
                    self.send_json(
                        {"ok": False, "message": "Не удалось подтвердить пользователя Битрикс."},
                        401,
                    )
                    return
                result = request_extra_claim(manager_id, payload.get("reason"))
                status = int(result.pop("_httpStatus", 200))
                self.send_json(result, status)
                return
            if parsed.path == "/api/reject":
                manager_id = actor_id_from_payload(payload, allow_cached=True)
                if not manager_id:
                    self.send_json({"ok": False, "message": "Не удалось подтвердить пользователя Битрикс."}, 401)
                    return
                result = record_rejection(manager_id, payload)
                self.send_json(result, 200 if result.get("ok") else 400)
                return
            if parsed.path == "/api/admin/state":
                result = admin_state(payload)
                self.send_json(result, 200 if result.get("ok") else 403)
                return
            if parsed.path == "/api/admin/rule":
                result = update_admin_rule(payload)
                self.send_json(result, 200 if result.get("ok") else 403)
                return
            if parsed.path == "/api/current-user":
                result = current_user_state(payload)
                self.send_json(result, 200 if result.get("ok") else 401)
                return
            self.send_json({"ok": False, "message": "Маршрут не найден"}, 404)
        except OverflowError as exc:
            self.send_json({"ok": False, "message": str(exc)}, 413)
        except ValueError as exc:
            self.send_json({"ok": False, "message": str(exc)}, 400)
        except PermissionError:
            self.send_json({"ok": False, "message": "Недостаточно прав"}, 403)
        except Exception as exc:
            sys.stderr.write(f"API error {parsed.path}: {type(exc).__name__}\n")
            if parsed.path == "/api/next-deal":
                self.send_json(
                    {
                        "ok": False,
                        "message": "Bitrix отвечает слишком долго. Подождите минуту и повторите поиск.",
                        "errorType": "deal_search_failed",
                    },
                    503,
                )
                return
            self.send_json({"ok": False, "message": "Внутренняя ошибка сервиса"}, 500)

    def log_message(self, fmt, *args):
        # Railway already has request correlation at the edge.  Persisting a
        # visitor's full IP address in application logs adds personal data
        # without helping this service diagnose a route-level failure.
        parsed = safe_urlparse(self.path)
        path = parsed.path if parsed is not None else "<invalid-path>"
        sys.stderr.write(f"{self.command} {path}\n")


class BoundedThreadingHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False
    request_queue_size = 64

    def __init__(self, server_address, request_handler_class):
        self._request_slots = threading.BoundedSemaphore(MAX_CONCURRENT_REQUESTS)
        super().__init__(server_address, request_handler_class)

    def process_request(self, request, client_address):
        if not self._request_slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._request_slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()


INDEX_HTML = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Получить сделку - тест</title>
  <style nonce="__CSP_NONCE__">
    :root { color-scheme: light; font-family: Arial, sans-serif; background: #f6f7f9; color: #1f2933; }
    body { margin: 0; }
    header { background: #ffffff; border-bottom: 1px solid #d9dee7; padding: 18px 22px; }
    main { padding: 18px 22px 32px; max-width: 1180px; margin: 0 auto; }
    h1 { font-size: 22px; margin: 0 0 8px; }
    .toolbar { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin: 16px 0; }
    input, select { height: 36px; border: 1px solid #b9c2cf; border-radius: 6px; padding: 0 10px; min-width: 190px; background: #fff; }
    textarea { width: 100%; min-height: 110px; box-sizing: border-box; border: 1px solid #b9c2cf; border-radius: 6px; padding: 10px; resize: vertical; font: inherit; }
    button { height: 38px; border: 1px solid #1769aa; border-radius: 6px; background: #1976c9; color: #fff; padding: 0 14px; cursor: pointer; }
    button.secondary { background: #ffffff; color: #1769aa; }
    button.reason { height: 32px; border-color: #c2ccd8; color: #52616f; background: #fff; padding: 0 10px; font-size: 12px; }
    button:disabled { opacity: .55; cursor: not-allowed; }
    .status { margin: 10px 0; color: #52616f; }
    .search-progress { max-width: 760px; margin: 10px 0 14px; }
    .search-progress-track { height: 8px; overflow: hidden; border-radius: 999px; background: #dbe7f3; }
    .search-progress-bar { width: 42%; height: 100%; border-radius: inherit; background: #1976c9; animation: searchSlide 1.05s ease-in-out infinite; }
    .search-progress-text { margin-top: 7px; color: #52616f; font-size: 13px; }
    @keyframes searchSlide {
      0% { transform: translateX(-110%); }
      50% { transform: translateX(80%); }
      100% { transform: translateX(250%); }
    }
    .hidden { display: none; }
    .extra-claim-box { max-width: 760px; margin: 10px 0 14px; }
    .extra-claim-box.approved { border-color: #55a36a; background: #f2fbf4; }
    .extra-claim-box.rejected { border-color: #e1a3a3; background: #fff6f6; }
    .modal-backdrop { position: fixed; inset: 0; z-index: 1000; display: flex; align-items: center; justify-content: center; padding: 18px; background: rgba(15, 31, 50, .55); }
    .modal-backdrop.hidden { display: none; }
    .modal-card { width: min(520px, 100%); background: #fff; border-radius: 10px; box-shadow: 0 18px 60px rgba(0,0,0,.25); padding: 20px; }
    .modal-card h2 { margin: 0 0 8px; font-size: 20px; }
    .modal-error { color: #a32626; min-height: 20px; font-size: 13px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 12px; }
    .card { background: #ffffff; border: 1px solid #d9dee7; border-radius: 8px; padding: 14px; }
    .deal-view { max-width: 760px; }
    .meta { color: #52616f; font-size: 13px; line-height: 1.45; }
    .badge { display: inline-block; border-radius: 999px; padding: 4px 8px; font-size: 12px; background: #e8f1fb; color: #155b94; margin: 4px 6px 8px 0; }
    .msg { background: #f2f5f8; border-radius: 6px; padding: 8px; margin-top: 8px; font-size: 13px; line-height: 1.45; }
    .empty { background: #fff7e6; color: #7a4b00; }
    .reject-reasons { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-top: 10px; }
    .reject-reasons .label { color: #52616f; font-size: 13px; }
    .admin-panel { margin-top: 28px; border-top: 1px solid #d9dee7; padding-top: 18px; }
    .admin-panel h2 { font-size: 18px; margin: 0 0 6px; }
    .admin-table { width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #d9dee7; border-radius: 8px; overflow: hidden; }
    .admin-table th, .admin-table td { border-bottom: 1px solid #e6ebf1; padding: 10px; text-align: left; vertical-align: middle; font-size: 13px; }
    .admin-table th { background: #f2f5f8; color: #52616f; font-weight: 600; }
    .admin-table tr:last-child td { border-bottom: 0; }
    .admin-table input[type="number"] { min-width: 90px; width: 90px; }
    .admin-table input[type="checkbox"] { min-width: 0; width: 18px; height: 18px; }
    .admin-actions { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    .greeting-box { max-width: 760px; margin-top: 12px; }
    .greeting-text { white-space: pre-wrap; background: #f2f5f8; border-radius: 6px; padding: 10px; margin: 10px 0; line-height: 1.45; }
    .greeting-title { font-weight: 700; margin-bottom: 4px; }
    pre { white-space: pre-wrap; background: #111827; color: #e5e7eb; padding: 12px; border-radius: 8px; overflow: auto; }
    pre.warning-output { background: #fff7e6; color: #7a4b00; border: 1px solid #f0c36d; }
  </style>
</head>
<body>
  <header>
    <h1>Получить сделку - безопасный тест</h1>
    <div class="meta">Читаем только “Необработанные ЛИДЫ” и “ОЖИДАЮТ СПЕЦИАЛИСТА”. Запись включается только через настройку DRY_RUN=0.</div>
  </header>
  <main>
    <div class="toolbar">
      <select id="managerSelect" class="hidden"></select>
      <input id="managerId" class="hidden" placeholder="ID менеджера для проверки">
      <button id="getDealButton" disabled>Получить сделку</button>
    </div>
    <div id="managerInfo" class="status"></div>
    <div id="status" class="status">Выберите менеджера и нажмите “Получить сделку”.</div>
    <section id="extraClaimBox" class="card extra-claim-box hidden"></section>
    <div id="searchProgress" class="search-progress hidden">
      <div class="search-progress-track"><div class="search-progress-bar"></div></div>
      <div class="search-progress-text">Ищем подходящую заявку по навыкам менеджера...</div>
    </div>
    <div id="cards"></div>
    <pre id="result" hidden></pre>
    <section id="greetingBox" class="card greeting-box hidden"></section>
    <section id="adminPanel" class="admin-panel hidden">
      <h2>Доступ менеджеров</h2>
      <div class="meta">Этот блок видит только администратор. Здесь можно закрыть выдачу заявок или поставить дневной лимит. Лимит не применяется в воскресенье и каждый день с 18:00 до 21:30. В статистике и лимите учитываются только заявки, взятые через это приложение: Bitrix REST не даёт надёжной исторической разбивки ручных переходов по менеджерам.</div>
      <div class="toolbar">
        <input id="statsFrom" type="date">
        <input id="statsTo" type="date">
        <button id="refreshAdminButton" class="secondary">Обновить статистику</button>
      </div>
      <div id="adminRows"></div>
    </section>
  </main>
  <div id="extraClaimModal" class="modal-backdrop hidden" role="dialog" aria-modal="true" aria-labelledby="extraClaimTitle">
    <div class="modal-card">
      <h2 id="extraClaimTitle">Запросить ещё 1 заявку</h2>
      <div class="meta">Коротко объясните директору офиса, почему нужна дополнительная заявка.</div>
      <div class="toolbar"><textarea id="extraClaimReason" maxlength="500" placeholder="Например: клиент ждёт срочный подбор, готов взять ещё одну заявку"></textarea></div>
      <div id="extraClaimModalError" class="modal-error"></div>
      <div class="toolbar">
        <button id="submitExtraClaimButton">Отправить запрос</button>
        <button id="cancelExtraClaimButton" class="secondary">Отмена</button>
      </div>
    </div>
  </div>
<script nonce="__CSP_NONCE__" src="https://api.bitrix24.com/api/v1/"></script>
<script nonce="__CSP_NONCE__">
let selectedDealId = null;
let currentDeal = null;
let skippedDeals = [];
let managers = [];
let currentBitrixUser = null;
let currentUserIsAdmin = false;
let isDealSearchRunning = false;
let userVerified = false;
let canRequestDeal = false;
let extraClaimState = null;
let extraClaimRequestSubmitting = false;
let extraClaimStatusLoading = false;
const MAX_SEARCH_BATCHES = 250;
const PUBLIC_APP_URL = __PUBLIC_APP_URL__;
const INSTALL_MODE = __INSTALL_MODE__;
const INITIAL_AUTH = __INITIAL_AUTH__;
const ALLOW_UNVERIFIED_USERS = __ALLOW_UNVERIFIED_USERS__;
const REJECT_REASONS = {
  not_my_country: 'Не моя страна',
  unclear_request: 'Непонятный запрос',
  duplicate: 'Дубль',
  other: 'Другое'
};
function apiUrl(path) {
  if (!String(path || '').startsWith('/')) throw new Error('Некорректный адрес API.');
  return path;
}
function currentUserId() {
  return String(
    (currentBitrixUser && (currentBitrixUser.ID || currentBitrixUser.id))
    || document.getElementById('managerId').value
    || ''
  );
}
function isCurrentUserAdmin() {
  return currentUserIsAdmin;
}
function currentAuth() {
  if (window.BX24 && BX24.getAuth) {
    try {
      const auth = BX24.getAuth();
      if (auth && (auth.access_token || auth.AUTH_ID || auth.auth)) return auth;
    } catch (error) {}
  }
  return INITIAL_AUTH && (INITIAL_AUTH.AUTH_ID || INITIAL_AUTH.access_token || INITIAL_AUTH.auth)
    ? INITIAL_AUTH
    : null;
}
async function postJson(url, payload) {
  const endpoint = apiUrl(url);
  const body = JSON.stringify(payload || {});
  const attempts = url === '/api/next-deal' ? 2 : 1;
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    let response;
    let text;
    try {
      response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body
      });
    } catch (error) {
      if (attempt + 1 < attempts) continue;
      if (url === '/api/next-deal') {
        throw new Error('Соединение с сервисом прервалось. Проверьте интернет и повторите поиск.');
      }
      throw error;
    }
    try {
      text = await response.text();
    } catch (error) {
      if (response.ok === true && attempt + 1 < attempts) continue;
      if (url === '/api/next-deal') {
        throw new Error(
          response.ok === true
            ? 'Соединение с сервисом прервалось. Проверьте интернет и повторите поиск.'
            : 'Сервис поиска временно недоступен. Подождите минуту и повторите поиск.'
        );
      }
      throw error;
    }
    let data;
    try {
      data = JSON.parse(text);
    } catch (error) {
      if (url === '/api/next-deal') {
        throw new Error('Сервис поиска временно недоступен. Подождите минуту и повторите поиск.');
      }
      throw new Error(`Сервер временно недоступен для ${url}. Обновите приложение и попробуйте ещё раз.`);
    }
    if (!response.ok) {
      throw new Error(data.message || data.reason || data.error || 'Ошибка запроса');
    }
    return data;
  }
}
async function loadManagers() {
  if (!ALLOW_UNVERIFIED_USERS) return false;
  document.getElementById('status').textContent = 'Загружаю локальный тестовый список...';
  let data;
  try {
    data = await postJson('/api/dev/managers', {});
  } catch (error) {
    document.getElementById('status').textContent = error.message || 'Не удалось загрузить приложение.';
    return false;
  }
  managers = data.managers || [];
  const select = document.getElementById('managerSelect');
  select.innerHTML = '<option value="">Выберите менеджера</option>' + managers
    .filter((manager) => manager.active !== false)
    .map((manager) => `<option value="${escapeHtml(manager.id)}">${escapeHtml(manager.name)}</option>`)
    .join('');
  select.classList.remove('hidden');
  document.getElementById('managerId').classList.remove('hidden');
  document.getElementById('status').textContent = 'Локальный DRY_RUN: выберите тестового менеджера.';
  return true;
}
function applyAuthorizedUser(user, manager) {
  const rawUser = user && user.raw ? user.raw : user;
  currentBitrixUser = rawUser || {};
  if (user && Object.prototype.hasOwnProperty.call(user, 'isAdmin')) {
    currentUserIsAdmin = Boolean(user.isAdmin);
  }
  const managerId = String((rawUser && rawUser.ID) || (user && user.id) || (manager && manager.id) || '');
  if (managerId) {
    document.getElementById('managerId').value = managerId;
  }
  document.getElementById('managerSelect').classList.add('hidden');
  document.getElementById('managerId').classList.add('hidden');
  userVerified = true;
  if (manager) {
    renderManagerInfo(manager);
    loadExtraClaimStatus();
  } else {
    canRequestDeal = false;
    document.getElementById('managerInfo').textContent = 'Пользователь подтверждён, но профиль сотрудника недоступен.';
    document.getElementById('status').textContent = 'Проверьте доступ приложения к карточкам сотрудников.';
    document.getElementById('getDealButton').disabled = true;
  }
  if (isCurrentUserAdmin()) loadAdminPanel();
}
function renderManagerInfo(manager) {
  const competencies = manager.competencies || [];
  const isActive = manager.active !== false;
  const isEmployee = manager.intranet === true;
  canRequestDeal = userVerified && isActive && isEmployee && competencies.length > 0;
  document.getElementById('getDealButton').disabled = !canRequestDeal;
  if (!isActive) {
    document.getElementById('managerInfo').textContent = `Пользователь ${manager.name} деактивирован.`;
    document.getElementById('status').textContent = 'Обратитесь к администратору Bitrix24.';
    return;
  }
  if (!isEmployee) {
    document.getElementById('managerInfo').textContent = `Пользователь ${manager.name} не относится к сотрудникам компании.`;
    document.getElementById('status').textContent = 'Выдача заявок недоступна.';
    return;
  }
  document.getElementById('managerInfo').textContent = competencies.length
    ? `Вы вошли как ${manager.name}. Навыки из карточки сотрудника: ${manager.competencies.join(', ')}`
    : `Вы вошли как ${manager.name}. В карточке сотрудника не заполнено поле “Навыки”.`;
  document.getElementById('status').textContent = competencies.length
    ? 'Нажмите “Получить сделку”.'
    : 'Заполните поле “Навыки” в карточке сотрудника, иначе подбор невозможен.';
}
function updatePrimaryButton() {
  const button = document.getElementById('getDealButton');
  if (!button) return;
  if (isDealSearchRunning) {
    button.disabled = true;
    button.textContent = 'Ищем...';
    return;
  }
  const state = extraClaimState || {};
  const requestStatus = state.request && state.request.status;
  if (state.limitReached && state.enabled) {
    if (state.grantAvailable) {
      button.disabled = !canRequestDeal;
      button.textContent = 'Получить дополнительную заявку';
    } else if (requestStatus === 'queued' || requestStatus === 'pending' || requestStatus === 'approved') {
      button.disabled = true;
      button.textContent = requestStatus === 'approved' ? 'Обновляю разрешение...' : 'Запрос на рассмотрении';
    } else {
      button.disabled = !canRequestDeal || !state.configured;
      button.textContent = 'Запросить ещё 1 заявку';
    }
    return;
  }
  button.disabled = !canRequestDeal;
  button.textContent = 'Получить сделку';
}
function renderExtraClaimState(state) {
  extraClaimState = state && state.ok ? state : null;
  const box = document.getElementById('extraClaimBox');
  box.classList.add('hidden');
  box.classList.remove('approved', 'rejected');
  if (!extraClaimState || !extraClaimState.enabled || !extraClaimState.limitReached) {
    updatePrimaryButton();
    return;
  }
  const request = extraClaimState.request || {};
  const status = request.status || '';
  const quota = `${Number(extraClaimState.takenToday || 0)}/${Number(extraClaimState.dailyLimit || 0)}`;
  let title = `Дневной лимит исчерпан: ${quota}`;
  let detail = 'Можно запросить у директора офиса ещё одну заявку.';
  if (extraClaimState.grantAvailable) {
    title = 'Дополнительная заявка одобрена';
    detail = 'Разрешение действует на одну успешную выдачу.';
    box.classList.add('approved');
  } else if (status === 'queued') {
    title = 'Запрос сохранён';
    detail = 'Он будет отправлен директору, когда восстановится связь с Базой.';
  } else if (status === 'pending') {
    title = 'Запрос на рассмотрении';
    detail = 'Директор офиса получил запрос на одну дополнительную заявку.';
  } else if (status === 'approved') {
    title = 'Дополнительная заявка одобрена';
    detail = 'Обновляем одноразовое разрешение. Нажмите обновление страницы через несколько секунд.';
  } else if (status === 'rejected') {
    title = 'В дополнительной заявке отказано';
    detail = request.rejectionReason || 'Причина отказа не указана.';
    box.classList.add('rejected');
  }
  if (extraClaimState.integrationUnavailable && status !== 'queued') {
    detail += ' Статус из Базы временно не обновился.';
  }
  box.innerHTML = `<strong>${escapeHtml(title)}</strong><div class="meta">${escapeHtml(detail)}</div>`;
  box.classList.remove('hidden');
  updatePrimaryButton();
}
async function loadExtraClaimStatus() {
  if (!userVerified || !currentAuth() || extraClaimStatusLoading) return;
  extraClaimStatusLoading = true;
  try {
    const state = await postJson('/api/extra-claim/status', { auth: currentAuth() });
    renderExtraClaimState(state);
  } catch (error) {
    // Status synchronization is optional for ordinary within-limit claims.
    // Keep the last known state and let the main claim path re-check it.
  } finally {
    extraClaimStatusLoading = false;
  }
}
function shouldPollExtraClaimStatus() {
  const state = extraClaimState || {};
  const status = state.request && state.request.status;
  return Boolean(
    userVerified
    && state.enabled
    && state.limitReached
    && (
      state.grantAvailable
      || status === 'queued'
      || status === 'pending'
      || status === 'approved'
    )
  );
}
function openExtraClaimModal() {
  if (!extraClaimState || !extraClaimState.limitReached || extraClaimState.grantAvailable) return;
  document.getElementById('extraClaimModalError').textContent = '';
  document.getElementById('extraClaimModal').classList.remove('hidden');
  document.getElementById('extraClaimReason').focus();
}
function closeExtraClaimModal() {
  if (extraClaimRequestSubmitting) return;
  document.getElementById('extraClaimModal').classList.add('hidden');
}
async function submitExtraClaimRequest() {
  if (extraClaimRequestSubmitting) return;
  const reason = document.getElementById('extraClaimReason').value.trim();
  const errorBox = document.getElementById('extraClaimModalError');
  if (reason.length < 10 || reason.length > 500) {
    errorBox.textContent = 'Напишите причину от 10 до 500 символов.';
    return;
  }
  extraClaimRequestSubmitting = true;
  const button = document.getElementById('submitExtraClaimButton');
  button.disabled = true;
  button.textContent = 'Отправляю...';
  try {
    const state = await postJson('/api/extra-claim/request', {
      auth: currentAuth(),
      reason
    });
    document.getElementById('extraClaimModal').classList.add('hidden');
    document.getElementById('extraClaimReason').value = '';
    renderExtraClaimState(state);
    if (state.message) document.getElementById('status').textContent = state.message;
  } catch (error) {
    errorBox.textContent = error.message || 'Не удалось отправить запрос.';
  } finally {
    extraClaimRequestSubmitting = false;
    button.disabled = false;
    button.textContent = 'Отправить запрос';
  }
}
function handlePrimaryAction() {
  const state = extraClaimState || {};
  const requestStatus = state.request && state.request.status;
  if (
    state.limitReached
    && state.enabled
    && !state.grantAvailable
  ) {
    if (requestStatus !== 'queued' && requestStatus !== 'pending' && requestStatus !== 'approved') openExtraClaimModal();
    return;
  }
  getDeal();
}
async function detectUserFromServerAuth() {
  const auth = currentAuth();
  if (!auth) return false;
  try {
    const data = await postJson('/api/current-user', { auth });
    if (!data.ok || !data.user) return false;
    applyAuthorizedUser(data.user, data.manager);
    return true;
  } catch (error) {
    return false;
  }
}
async function identifyUserOrShowDevPicker() {
  const found = await detectUserFromServerAuth();
  if (found) return;
  if (await loadManagers()) return;
  userVerified = false;
  canRequestDeal = false;
  document.getElementById('getDealButton').disabled = true;
  document.getElementById('status').textContent = 'Не удалось подтвердить пользователя Битрикс. Откройте приложение внутри Bitrix24 или обновите страницу.';
}
function detectBitrixUser() {
  document.getElementById('status').textContent = 'Подтверждаю пользователя Битрикс...';
  if (!window.BX24 || !BX24.init) {
    identifyUserOrShowDevPicker();
    return;
  }
  BX24.init(() => {
    if (INSTALL_MODE && BX24.installFinish) {
      document.getElementById('managerInfo').textContent = '';
      document.getElementById('cards').innerHTML = '';
      BX24.installFinish();
      document.getElementById('status').textContent = 'Установка приложения завершена.';
      return;
    }
    identifyUserOrShowDevPicker();
  });
}
function bx24Call(method, params) {
  return new Promise((resolve, reject) => {
    BX24.callMethod(method, params || {}, (result) => {
      if (result.error()) {
        reject(new Error(result.error() + ': ' + result.error_description()));
        return;
      }
      resolve(result.data());
    });
  });
}
async function bindDealPlacement() {
  if (!window.BX24 || !BX24.callMethod) {
    showResult({ ok: false, message: 'Эта настройка доступна только внутри Битрикса.' });
    return;
  }
  const handler = window.location.origin + window.location.pathname;
  const candidates = [
    'CRM_DEAL_LIST_TOOLBAR',
    'CRM_DEAL_LIST_MENU',
    'CRM_DEAL_DETAIL_TOOLBAR'
  ];
  const errors = [];
  for (const placement of candidates) {
    try {
      await bx24Call('placement.bind', {
        PLACEMENT: placement,
        HANDLER: handler,
        TITLE: 'Получить сделку',
        DESCRIPTION: 'Подбор сделки по навыкам менеджера'
      });
      showResult({ ok: true, placement, message: 'Кнопка закреплена. Обновите страницу сделок.' });
      return;
    } catch (error) {
      errors.push({ placement, error: String(error.message || error) });
    }
  }
  showResult({ ok: false, message: 'Не удалось закрепить кнопку в известных местах CRM.', errors });
}
async function bindLeftMenu() {
  if (!window.BX24 || !BX24.callMethod) {
    showResult({ ok: false, message: 'Эта настройка доступна только внутри Битрикса.' });
    return;
  }
  const handler = window.location.origin + window.location.pathname;
  const candidates = [
    'LEFT_MENU',
    'MAIN_MENU'
  ];
  const errors = [];
  for (const placement of candidates) {
    try {
      await bx24Call('placement.bind', {
        PLACEMENT: placement,
        HANDLER: handler,
        TITLE: 'Получить сделку',
        DESCRIPTION: 'Подбор сделки по навыкам менеджера'
      });
      showResult({ ok: true, placement, message: 'Пункт закреплен в левом меню. Обновите Битрикс.' });
      return;
    } catch (error) {
      const message = String(error.message || error);
      if (message.toLowerCase().includes('already binded') || message.toLowerCase().includes('already bound')) {
        showResult({ ok: true, placement, message: 'Пункт уже закреплен в левом меню. Обновите Битрикс и проверьте “Показать все”.' });
        return;
      }
      errors.push({ placement, error: message });
    }
  }
  showResult({ ok: false, message: 'Не удалось закрепить в левом меню.', errors });
}
function syncManagerId() {
  const select = document.getElementById('managerSelect');
  const managerId = select.value;
  document.getElementById('managerId').value = managerId;
  currentDeal = null;
  extraClaimState = null;
  skippedDeals = [];
  document.getElementById('cards').innerHTML = '';
  document.getElementById('result').hidden = true;
  clearGreeting();
  renderExtraClaimState(null);
  setSearching(false);
  const manager = managers.find((item) => String(item.id) === String(managerId));
  userVerified = Boolean(managerId && manager && ALLOW_UNVERIFIED_USERS);
  currentBitrixUser = userVerified ? { ID: managerId } : null;
  currentUserIsAdmin = false;
  if (manager) renderManagerInfo(manager);
  else {
    canRequestDeal = false;
    document.getElementById('managerInfo').textContent = '';
    document.getElementById('getDealButton').disabled = true;
  }
}
function setSearching(isSearching, text) {
  const progress = document.getElementById('searchProgress');
  const button = document.getElementById('getDealButton');
  progress.classList.toggle('hidden', !isSearching);
  if (button) {
    button.disabled = isSearching || !canRequestDeal;
    if (isSearching) button.textContent = 'Ищем...';
    else updatePrimaryButton();
  }
  if (text) {
    progress.querySelector('.search-progress-text').textContent = text;
  }
}
async function getDeal() {
  if (isDealSearchRunning) return;
  if (!userVerified || !canRequestDeal) {
    document.getElementById('status').textContent = 'Сначала подтвердите пользователя и заполните навыки сотрудника.';
    return;
  }
  isDealSearchRunning = true;
  selectedDealId = null;
  currentDeal = null;
  const managerId = document.getElementById('managerId').value.trim();
  const auth = currentAuth();
  if (!managerId && !auth) {
    isDealSearchRunning = false;
    document.getElementById('status').textContent = 'Не удалось определить пользователя Битрикс. Обновите страницу приложения.';
    return;
  }
  document.getElementById('status').textContent = 'Ищу подходящую сделку...';
  setSearching(true);
  document.getElementById('result').hidden = true;
  clearGreeting();
  let data = null;
  let continuationToken = null;
  let checkedCount = 0;
  try {
    for (let batch = 0; batch < MAX_SEARCH_BATCHES; batch += 1) {
      data = await postJson('/api/next-deal', {
        auth,
        managerId,
        continuationToken
      });
      continuationToken = data.continuationToken || null;
      checkedCount += Number(data.checkedCount || 0);
      if (data.deal || !data.hasMore || !continuationToken) break;
      setSearching(
        true,
        `Проверено заявок: ${checkedCount}. Ищу дальше, начиная со старых...`
      );
    }
    if (data && !data.deal && data.hasMore) {
      data.reason = 'Поиск занял больше обычного. Подождите минуту и попробуйте ещё раз.';
    }
  } catch (error) {
    isDealSearchRunning = false;
    setSearching(false);
    document.getElementById('status').textContent = error.message || 'Ошибка загрузки';
    return;
  }
  isDealSearchRunning = false;
  setSearching(false);
  if (!data.deal) {
    if (data.manager) applyAuthorizedUser({ id: data.manager.id }, data.manager);
    if (data.limitReached) {
      renderExtraClaimState({
        ok: true,
        enabled: Boolean(data.extraClaimEnabled),
        configured: data.extraClaimConfigured === undefined
          ? Boolean(data.extraClaimEnabled)
          : Boolean(data.extraClaimConfigured),
        limitReached: true,
        takenToday: data.takenToday,
        dailyLimit: data.dailyLimit,
        request: data.extraClaimRequest || null,
        grantAvailable: Boolean(data.extraClaimGrantAvailable),
        integrationUnavailable: Boolean(data.integrationUnavailable)
      });
    }
    document.getElementById('status').textContent = data.reason || 'Подходящих сделок нет.';
    document.getElementById('cards').innerHTML = '';
    return;
  }
  if (data.manager) applyAuthorizedUser({ id: data.manager.id }, data.manager);
  currentDeal = data.deal;
  selectedDealId = currentDeal.id;
  document.getElementById('status').textContent = 'Найдена сделка по компетенции. Можно взять или отказаться.';
  const cards = document.getElementById('cards');
  cards.innerHTML = '';
  cards.appendChild(renderDeal(currentDeal));
}
function renderDeal(deal) {
  const card = document.createElement('section');
  card.className = 'card deal-view';
  const messages = deal.messages.length
    ? deal.messages.map((m, i) => `<div class="msg">${i + 1}. ${escapeHtml(m.slice(0, 520))}</div>`).join('')
    : '<div class="msg empty">Полезные сообщения пока не найдены.</div>';
  const rejectButtons = Object.entries(REJECT_REASONS)
    .map(([reason, label]) => `<button class="reason reject-button" data-reject-reason="${escapeHtml(reason)}">${escapeHtml(label)}</button>`)
    .join('');
  card.innerHTML = `
    <div class="badge">Сделка #${escapeHtml(deal.id)}</div>
    <div class="badge">${escapeHtml(deal.stageName)}</div>
    <div class="badge">Направление: ${escapeHtml(deal.classification.direction)}</div>
    <h3>${escapeHtml(deal.title || 'Без названия')}</h3>
    <div class="meta">Ответственный ID: ${escapeHtml(String(deal.assignedById || ''))}<br>Создана: ${escapeHtml(deal.dateCreate || '')}<br>Уверенность: ${escapeHtml(deal.classification.confidence)}<br>${escapeHtml(deal.matchReason || '')}</div>
    ${messages}
    <div class="toolbar">
      <button class="claim-button">Взять в работу</button>
    </div>
    <div class="reject-reasons">
      <span class="label">Отказаться:</span>
      ${rejectButtons}
    </div>
  `;
  card.querySelector('.claim-button').addEventListener('click', claimSelected);
  card.querySelectorAll('.reject-button').forEach((button) => {
    button.addEventListener('click', () => rejectDeal(button.dataset.rejectReason));
  });
  return card;
}
async function claimSelected() {
    const managerId = document.getElementById('managerId').value.trim();
    const auth = currentAuth();
    if (!selectedDealId) return showResult({ ok: false, message: 'Сначала выберите сделку.' });
    if (!managerId && !auth) return showResult({ ok: false, message: 'Не удалось определить пользователя Битрикс.' });
  const button = document.querySelector('.claim-button');
  if (button) {
    button.disabled = true;
    button.textContent = 'Беру...';
  }
  const dealWindow = currentDeal && currentDeal.dealUrl ? window.open('about:blank', '_blank') : null;
  let payload;
  try {
    payload = await postJson('/api/claim', {
      auth,
      dealId: selectedDealId,
      managerId,
      selectionToken: currentDeal && currentDeal.selectionToken
    });
  } catch (error) {
    payload = { ok: false, message: error.message || 'Не удалось взять сделку.' };
  }
  showResult(payload);
  renderGreeting(payload.greeting);
  if (payload.ok && payload.dryRun !== true && currentDeal && currentDeal.dealUrl) {
    loadAdminPanel();
    loadExtraClaimStatus();
    if (dealWindow) {
      dealWindow.location.href = currentDeal.dealUrl;
    } else {
      window.open(currentDeal.dealUrl, '_blank');
    }
  } else if (dealWindow) {
    dealWindow.close();
  }
  if (button && !payload.ok) {
    button.disabled = false;
    button.textContent = 'Взять в работу';
    loadExtraClaimStatus();
  }
}
async function rejectDeal(reason) {
  if (!currentDeal) return;
  const rejectedDeal = currentDeal;
  const managerId = document.getElementById('managerId').value.trim();
  const auth = currentAuth();
  reason = reason || 'other';
  document.querySelectorAll('.reject-button').forEach((button) => { button.disabled = true; });
  document.getElementById('status').textContent = 'Сохраняю отказ...';
  try {
    await postJson('/api/reject', {
      auth,
      managerId,
      dealId: rejectedDeal.id,
      reason,
      selectionToken: rejectedDeal.selectionToken
    });
  } catch (error) {
    document.querySelectorAll('.reject-button').forEach((button) => { button.disabled = false; });
    document.getElementById('status').textContent = error.message || 'Не удалось сохранить отказ. Повторите ещё раз.';
    return;
  }
  if (!skippedDeals.includes(String(rejectedDeal.id))) skippedDeals.push(String(rejectedDeal.id));
  currentDeal = null;
  selectedDealId = null;
  document.getElementById('cards').innerHTML = '';
  document.getElementById('status').textContent = `Отказ: ${REJECT_REASONS[reason] || REJECT_REASONS.other}. Ищу следующую...`;
  document.getElementById('result').hidden = true;
  clearGreeting();
  if (isCurrentUserAdmin()) loadAdminPanel();
  getDeal();
}
function bishkekDate() {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Bishkek', year: 'numeric', month: '2-digit', day: '2-digit'
  }).formatToParts(new Date());
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${values.year}-${values.month}-${values.day}`;
}
async function loadAdminPanel() {
  if (!currentBitrixUser) return;
  if (!isCurrentUserAdmin()) return;
  const panel = document.getElementById('adminPanel');
  panel.classList.remove('hidden');
  document.getElementById('adminRows').innerHTML = '<div class="status">Загружаю настройки доступа...</div>';
  const today = bishkekDate();
  const from = document.getElementById('statsFrom');
  const to = document.getElementById('statsTo');
  if (!from.value) from.value = today;
  if (!to.value) to.value = today;
  let data;
  try {
    data = await postJson('/api/admin/state', {
      auth: currentAuth(),
      dateFrom: from.value,
      dateTo: to.value
    });
  } catch (error) {
    document.getElementById('adminRows').innerHTML = `<div class="status">${escapeHtml(error.message || 'Не удалось загрузить админку.')}</div>`;
    return;
  }
  if (!data.ok || !data.isAdmin) {
    document.getElementById('adminRows').innerHTML = '<div class="status">Админский доступ не подтвержден.</div>';
    return;
  }
  renderAdminRows(data.managers || [], data.warnings || []);
}
function renderAdminRows(rows, warnings) {
  const target = document.getElementById('adminRows');
  if (!rows.length) {
    target.innerHTML = '<div class="status">Пока нет менеджеров с навыками или статистикой.</div>';
    return;
  }
  const warningHtml = (warnings || []).length
    ? `<div class="status empty">${(warnings || []).map(escapeHtml).join('<br>')}</div>`
    : '';
  target.innerHTML = `${warningHtml}
    <table class="admin-table">
      <thead>
        <tr>
          <th>Менеджер</th>
          <th>Навыки</th>
          <th>Доступ</th>
          <th>Лимит в день</th>
          <th>Взято через приложение за период</th>
          <th>Взято через приложение сегодня</th>
          <th>Отказы период</th>
          <th>Отказы сегодня</th>
          <th>Причина отказов</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        ${rows.map((row) => `
          <tr data-manager-id="${escapeHtml(row.id)}">
            <td><strong>${escapeHtml(row.name)}</strong><br><span class="meta">ID ${escapeHtml(row.id)}</span></td>
            <td>${escapeHtml((row.competencies || []).join(', ') || 'Не заполнено')}</td>
            <td><input type="checkbox" class="rule-enabled" ${row.rule && row.rule.enabled === false ? '' : 'checked'}></td>
            <td><input type="number" class="rule-limit" min="0" value="${row.rule && row.rule.dailyLimit !== null && row.rule.dailyLimit !== undefined ? escapeHtml(row.rule.dailyLimit) : ''}" placeholder="без лимита"></td>
            <td>${escapeHtml(row.takenInPeriod || 0)}</td>
            <td>${escapeHtml(row.takenToday || 0)}</td>
            <td>${escapeHtml(row.rejectedInPeriod || 0)}</td>
            <td>${escapeHtml(row.rejectedToday || 0)}</td>
            <td>${escapeHtml(row.topRejectReason || '')}</td>
            <td><button class="secondary save-rule-button" data-manager-id="${escapeHtml(row.id)}">Сохранить</button></td>
          </tr>
        `).join('')}
      </tbody>
    </table>
  `;
  target.querySelectorAll('.save-rule-button').forEach((button) => {
    button.addEventListener('click', () => saveAccessRule(button.dataset.managerId));
  });
}
async function saveAccessRule(managerId) {
  const row = Array.from(document.querySelectorAll('tr[data-manager-id]'))
    .find((item) => item.getAttribute('data-manager-id') === String(managerId));
  if (!row) return;
  const button = row.querySelector('button');
  button.disabled = true;
  button.textContent = 'Сохраняю...';
  try {
    await postJson('/api/admin/rule', {
      auth: currentAuth(),
      managerId,
      enabled: row.querySelector('.rule-enabled').checked,
      dailyLimit: row.querySelector('.rule-limit').value
    });
    button.textContent = 'Готово';
    setTimeout(() => loadAdminPanel(), 500);
  } catch (error) {
    button.textContent = 'Ошибка';
    showResult({ ok: false, message: error.message || 'Не удалось сохранить правило.' });
  } finally {
    setTimeout(() => {
      button.disabled = false;
      button.textContent = 'Сохранить';
    }, 1200);
  }
}
function clearGreeting() {
  const box = document.getElementById('greetingBox');
  if (!box) return;
  box.classList.add('hidden');
  box.innerHTML = '';
}
function renderGreeting(greeting) {
  const box = document.getElementById('greetingBox');
  if (!box) return;
  if (!greeting) {
    clearGreeting();
    return;
  }
  box.innerHTML = '';
  box.classList.remove('hidden');

  const title = document.createElement('div');
  title.className = 'greeting-title';
  title.textContent = greeting.status === 'queued'
    ? 'Приветствие отправляется'
    : greeting.autoSent ? 'Сообщение клиенту отправлено' : 'Текст клиенту';

  const meta = document.createElement('div');
  meta.className = 'meta';
  meta.textContent = greeting.status === 'queued'
    ? 'Сделка уже закреплена за вами. Сообщение клиенту уйдёт в фоне.'
    : greeting.autoSent
    ? 'Приветствие сохранено в журнале.'
    : 'Автоотправка пока не включена для канала. Скопируйте текст и отправьте клиенту в чате сделки.';

  box.appendChild(title);
  box.appendChild(meta);
  if (!greeting.text) return;

  const text = document.createElement('div');
  text.className = 'greeting-text';
  text.textContent = greeting.text;

  const button = document.createElement('button');
  button.className = 'secondary';
  button.textContent = 'Скопировать текст';
  button.addEventListener('click', () => copyGreetingText(greeting.text, button));

  box.appendChild(text);
  if (!greeting.autoSent) box.appendChild(button);
}
async function copyGreetingText(text, button) {
  try {
    await navigator.clipboard.writeText(text || '');
    button.textContent = 'Скопировано';
  } catch (error) {
    button.textContent = 'Скопируйте вручную';
  }
  setTimeout(() => {
    button.textContent = 'Скопировать текст';
  }, 1500);
}
function showResult(payload) {
  const result = document.getElementById('result');
  if (payload && payload.ok) {
    const warnings = Array.isArray(payload.warnings) ? payload.warnings.filter(Boolean) : [];
    result.classList.toggle('warning-output', warnings.length > 0);
    result.hidden = warnings.length === 0;
    result.textContent = warnings.length ? `Внимание:\\n${warnings.join('\\n')}` : '';
    if (payload.message) document.getElementById('status').textContent = payload.message;
    return;
  }
  result.classList.remove('warning-output');
  result.hidden = false;
  result.textContent = JSON.stringify(payload, null, 2);
}
function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (ch) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[ch]));
}
document.getElementById('managerSelect').addEventListener('change', syncManagerId);
document.getElementById('getDealButton').addEventListener('click', handlePrimaryAction);
document.getElementById('submitExtraClaimButton').addEventListener('click', submitExtraClaimRequest);
document.getElementById('cancelExtraClaimButton').addEventListener('click', closeExtraClaimModal);
document.getElementById('refreshAdminButton').addEventListener('click', loadAdminPanel);
setInterval(() => {
  if (shouldPollExtraClaimStatus()) loadExtraClaimStatus();
}, 15000);
if (INSTALL_MODE) document.getElementById('status').textContent = 'Завершаю установку приложения...';
detectBitrixUser();
</script>
</body>
</html>
"""


def run_console():
    deals = list_allowed_deals()
    print(f"Найдено сделок в разрешенных стадиях: {len(deals)}")
    for deal in deals[:10]:
        print("-" * 60)
        print(f"#{deal['id']} | {deal['stageName']} | ответственный {deal.get('assignedById')}")
        print(f"Название: {deal.get('title')}")
        print(f"Направление: {deal['classification']['direction']} ({deal['classification']['confidence']})")
        if deal["messages"]:
            for index, message in enumerate(deal["messages"], 1):
                print(f"Сообщение {index}: {message[:260]}")
        else:
            print("Полезные сообщения не найдены.")


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--console":
        run_console()
        return
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "3000"))
    server = BoundedThreadingHTTPServer((host, port), Handler)
    threading.Thread(
        target=claim_reconciliation_loop,
        name="claim-reconciler",
        daemon=True,
    ).start()
    threading.Thread(
        target=integration_outbox_loop,
        name="baza-integration-outbox",
        daemon=True,
    ).start()
    threading.Thread(
        target=greeting_outbox_loop,
        name="greeting-outbox",
        daemon=True,
    ).start()
    if LOST_DEAL_AUTOCLOSE_ENABLED:
        threading.Thread(
            target=lost_deal_autoclose_loop,
            name="lost-deal-autoclose",
            daemon=True,
        ).start()
    print(f"Открыть тест: http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
