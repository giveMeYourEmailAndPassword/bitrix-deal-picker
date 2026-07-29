#!/usr/bin/env python3
import concurrent.futures
import html
import json
import os
import re
import sys
import threading
import time
import urllib.parse
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


APP_DIR = Path(os.environ.get("APP_DATA_DIR") or Path(__file__).resolve().parent)
ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env.bitrix"
MANAGERS_FILE = APP_DIR / "managers.json"
ACCESS_RULES_FILE = APP_DIR / "access_rules.json"
CLAIM_LOG_FILE = APP_DIR / "claim_log.json"
REJECT_LOG_FILE = APP_DIR / "reject_log.json"
GREETING_LOG_FILE = APP_DIR / "greeting_log.json"
DATA_LOCK = threading.Lock()
DEAL_ANALYSIS_CACHE_LOCK = threading.Lock()
LOCAL_TZ = timezone(timedelta(hours=int(os.environ.get("APP_TZ_OFFSET_HOURS", "6"))))
APP_VERSION = "2026-07-23-batched-deal-search"

SOURCE_STAGES = {
    "UC_ZJ55BR": "Необработанные ЛИДЫ",
    "UC_PUCAAQ": "ОЖИДАЮТ СПЕЦИАЛИСТА",
}
TARGET_STAGE = "NEW"
TARGET_STAGE_NAME = "В РАБОТЕ"
OPENLINE_HISTORY_CACHE = {}
DEAL_ANALYSIS_CACHE = {}
DRY_RUN = os.environ.get("DRY_RUN", "1").lower() not in {"0", "false", "no"}
PUBLIC_APP_URL = os.environ.get("PUBLIC_APP_URL", "https://app-7ecf09c67021.vibecode.bitrix24.tech").rstrip("/")
ADMIN_USER_IDS = {
    item.strip()
    for item in os.environ.get("ADMIN_USER_IDS", "41").split(",")
    if item.strip()
}
ALLOW_UNVERIFIED_USERS = os.environ.get("ALLOW_UNVERIFIED_USERS", "0").lower() in {"1", "true", "yes"}
GREETING_AUTO_SEND = os.environ.get("GREETING_AUTO_SEND", "0").lower() in {"1", "true", "yes"}
NEXT_DEAL_SCAN_LIMIT = int(os.environ.get("NEXT_DEAL_SCAN_LIMIT", "8"))
NEXT_DEAL_SCAN_WORKERS = int(os.environ.get("NEXT_DEAL_SCAN_WORKERS", str(NEXT_DEAL_SCAN_LIMIT)))
NEXT_DEAL_BATCH_TIMEOUT_SECONDS = float(os.environ.get("NEXT_DEAL_BATCH_TIMEOUT_SECONDS", "12"))
DEAL_ANALYSIS_CACHE_TTL_SECONDS = float(os.environ.get("DEAL_ANALYSIS_CACHE_TTL_SECONDS", "300"))
BITRIX_TIMEOUT_SECONDS = float(os.environ.get("BITRIX_TIMEOUT_SECONDS", "12"))
BITRIX_FAST_TIMEOUT_SECONDS = float(os.environ.get("BITRIX_FAST_TIMEOUT_SECONDS", "5"))
LIMIT_FREE_WINDOW_START = os.environ.get("LIMIT_FREE_WINDOW_START", "18:00")
LIMIT_FREE_WINDOW_END = os.environ.get("LIMIT_FREE_WINDOW_END", "21:30")

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
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())
    base = os.environ.get("BITRIX_WEBHOOK_BASE")
    if not base:
        raise RuntimeError("Не найден BITRIX_WEBHOOK_BASE в work/.env.bitrix")
    return base.rstrip("/") + "/"


def portal_base_url():
    parsed = urllib.parse.urlparse(load_env())
    return f"{parsed.scheme}://{parsed.netloc}"


def load_managers():
    if not MANAGERS_FILE.exists():
        return []
    return json.loads(MANAGERS_FILE.read_text(encoding="utf-8"))


def read_json_file(path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json_file(path, payload):
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def local_now():
    return datetime.now(LOCAL_TZ)


def local_date():
    return local_now().date().isoformat()


def is_limit_free_day():
    return local_now().weekday() == 6


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
    return now.weekday() == 6 or is_limit_free_time(now)


def entry_date(entry):
    raw = str(entry.get("timestamp") or "")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(LOCAL_TZ).date().isoformat()


def normalize_date(value, fallback):
    value = str(value or "").strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
        return value
    return fallback


def load_access_rules():
    data = read_json_file(ACCESS_RULES_FILE, {"managers": {}})
    data.setdefault("managers", {})
    return data


def save_access_rules(data):
    data.setdefault("managers", {})
    write_json_file(ACCESS_RULES_FILE, data)


def get_manager_rule(manager_id):
    rules = load_access_rules().get("managers", {})
    rule = rules.get(str(manager_id), {})
    return {
        "enabled": rule.get("enabled", True) is not False,
        "dailyLimit": rule.get("dailyLimit"),
        "note": str(rule.get("note") or ""),
    }


def set_manager_rule(manager_id, enabled=True, daily_limit=None, note=""):
    data = load_access_rules()
    if daily_limit in ("", None):
        daily_limit = None
    else:
        try:
            daily_limit = max(0, int(daily_limit))
        except (TypeError, ValueError):
            daily_limit = None
    data["managers"][str(manager_id)] = {
        "enabled": bool(enabled),
        "dailyLimit": daily_limit,
        "note": str(note or ""),
    }
    save_access_rules(data)
    return get_manager_rule(manager_id)


def load_claim_log():
    return read_json_file(CLAIM_LOG_FILE, [])


def append_claim_log(manager_id, deal):
    log = load_claim_log()
    manager = get_manager_profile(manager_id) or {"name": str(manager_id)}
    entry = {
        "timestamp": local_now().isoformat(),
        "managerId": str(manager_id),
        "managerName": manager.get("name") or str(manager_id),
        "dealId": str(deal.get("ID") or deal.get("id") or ""),
        "dealTitle": deal.get("TITLE") or deal.get("title") or "",
    }
    log.append(entry)
    write_json_file(CLAIM_LOG_FILE, log[-5000:])
    return entry


def load_reject_log():
    return read_json_file(REJECT_LOG_FILE, [])


def load_greeting_log():
    return read_json_file(GREETING_LOG_FILE, [])


def append_greeting_log(entry):
    log = load_greeting_log()
    log.append(entry)
    write_json_file(GREETING_LOG_FILE, log[-5000:])
    return entry


def latest_greeting_for_deal(deal_id):
    deal_id = str(deal_id or "")
    for entry in reversed(load_greeting_log()):
        if str(entry.get("dealId")) == deal_id and entry.get("status") in {"manual", "sent"}:
            return entry
    return None


def normalize_reject_reason(reason):
    reason = str(reason or "").strip()
    return reason if reason in REJECT_REASONS else "other"


def append_reject_log(manager_id, deal, reason="other"):
    log = load_reject_log()
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
    log.append(entry)
    write_json_file(REJECT_LOG_FILE, log[-5000:])
    return entry


def count_claims(manager_id, date_from=None, date_to=None):
    return count_claims_in_log(load_claim_log(), manager_id, date_from, date_to)


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


def check_manager_access(manager_id):
    rule = get_manager_rule(manager_id)
    if not rule["enabled"]:
        return {"ok": False, "rule": rule, "reason": "Для вас временно закрыт доступ к получению заявок."}
    if rule["dailyLimit"] is not None:
        if is_limit_bypassed_now():
            return {"ok": True, "rule": rule, "limitBypassed": True}
        today = local_date()
        taken_today = count_claims(manager_id, today, today)
        if taken_today >= int(rule["dailyLimit"]):
            return {
                "ok": False,
                "rule": rule,
                "reason": f"Дневной лимит заявок уже достигнут: {taken_today}/{rule['dailyLimit']}.",
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
            "active": True,
            "source": "local_fallback",
        }
    return {"id": manager_id, "name": manager_id, "competencies": [], "active": True, "source": "unavailable"}


def manager_profile_from_user(user, manager_id=None):
    manager_id = str(user.get("ID") or manager_id or "")
    competencies = parse_competencies(user.get("UF_SKILLS"))
    # Fallback to older local config while testing accounts whose profile field is empty.
    local = next((item for item in load_managers() if str(item.get("id")) == manager_id), None)
    if not competencies and local:
        competencies = local.get("competencies", [])
    return {
        "id": manager_id,
        "name": " ".join(part for part in [user.get("NAME"), user.get("LAST_NAME")] if part).strip() or manager_id,
        "competencies": competencies,
        "active": user.get("ACTIVE", "Y") != "N",
        "source": "UF_SKILLS" if competencies else "empty",
    }


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
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        try:
            payload = json.loads(body)
        except Exception:
            raise RuntimeError(f"HTTP {exc.code}: {body[:500]}") from exc
        raise RuntimeError(f"{payload.get('error')}: {payload.get('error_description')}") from exc
    if "error" in payload:
        raise RuntimeError(f"{payload.get('error')}: {payload.get('error_description')}")
    return payload


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
    domain = str(domain or "").replace("https://", "").replace("http://", "").strip("/")
    if not domain or not token:
        raise RuntimeError("Нет авторизации Битрикса")
    url = f"https://{domain}/rest/{method}.json"
    payload = dict(params or {})
    payload["auth"] = token
    data = urllib.parse.urlencode(payload, doseq=True).encode("utf-8")
    try:
        with urllib.request.urlopen(url, data=data, timeout=timeout or BITRIX_TIMEOUT_SECONDS) as response:
            result = json.load(response)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        try:
            result = json.loads(body)
        except Exception:
            raise RuntimeError(f"HTTP {exc.code}: {body[:500]}") from exc
        raise RuntimeError(f"{result.get('error')}: {result.get('error_description')}") from exc
    if "error" in result:
        raise RuntimeError(f"{result.get('error')}: {result.get('error_description')}")
    return result.get("result")


def bitrix_call_for_actor(auth, method, params=None):
    if isinstance(auth, dict):
        token = auth.get("access_token") or auth.get("AUTH_ID") or auth.get("auth")
        domain = auth.get("domain") or auth.get("DOMAIN")
        if not domain and auth.get("client_endpoint"):
            domain = urllib.parse.urlparse(auth.get("client_endpoint")).netloc
        if token and domain:
            try:
                return bitrix_oauth_call(domain, token, method, params)
            except Exception:
                return bitrix_call(method, params)
    return bitrix_call(method, params)


def _extract_auth_credentials(auth):
    """Извлечь (token, domain) из auth-словаря Bitrix24.

    Поддерживает все форматы:
      {access_token, domain}, {AUTH_ID, DOMAIN, client_endpoint}, {auth, client_endpoint}
    """
    if not isinstance(auth, dict):
        return None, None
    token = auth.get("access_token") or auth.get("AUTH_ID") or auth.get("auth")
    if not token:
        return None, None
    domain = auth.get("domain") or auth.get("DOMAIN")
    if not domain:
        client = auth.get("client_endpoint")
        if client:
            domain = urllib.parse.urlparse(client).netloc
    if not domain:
        return None, None
    return token, domain


def has_bitrix_auth(payload):
    auth = payload.get("auth") if isinstance(payload, dict) else None
    token, domain = _extract_auth_credentials(auth)
    return bool(token and domain)


def verify_bitrix_user(auth):
    token, domain = _extract_auth_credentials(auth)
    if not token or not domain:
        return None
    try:
        user = bitrix_oauth_call(domain, token, "user.current", timeout=BITRIX_FAST_TIMEOUT_SECONDS)
    except Exception:
        try:
            user = bitrix_oauth_call(domain, token, "profile", timeout=BITRIX_FAST_TIMEOUT_SECONDS)
        except Exception:
            return None
    if not user:
        return None
    return {
        "id": str(user.get("ID") or ""),
        "name": " ".join(part for part in [user.get("NAME"), user.get("LAST_NAME")] if part).strip(),
        "raw": user,
    }


_USER_VERIFY_CACHE = {}
_USER_VERIFY_CACHE_TTL = 300  # 5 минут

def actor_id_from_payload(payload):
    auth = payload.get("auth") if isinstance(payload, dict) else None
    if not isinstance(auth, dict):
        if ALLOW_UNVERIFIED_USERS:
            return str(payload.get("managerId") or "")
        return None

    token, domain = _extract_auth_credentials(auth)

    if not token or not domain:
        if ALLOW_UNVERIFIED_USERS:
            return str(payload.get("managerId") or "")
        return None

    cache_key = (token, domain)
    now = time.monotonic()
    cached = _USER_VERIFY_CACHE.get(cache_key)
    if cached and now - cached["at"] < _USER_VERIFY_CACHE_TTL:
        return cached["id"]

    user = verify_bitrix_user(auth)
    if user and user.get("id"):
        _USER_VERIFY_CACHE[cache_key] = {"id": user["id"], "at": time.monotonic()}
        return user["id"]

    return None


def require_admin(payload):
    """Только верифицированный через Bitrix пользователь может быть админом."""
    user = verify_bitrix_user(payload.get("auth"))
    if user and user.get("id") in ADMIN_USER_IDS:
        return user
    return None


def clean_text(value):
    value = value or ""
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\[/?[A-Z0-9_=-]+\]", " ", value, flags=re.IGNORECASE)
    value = html.unescape(value)
    value = value.replace("\xa0", " ")
    value = re.sub(r":f0[0-9a-f]+:", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"https?://\S+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def is_service_text(text):
    if not text:
        return False
    lowered = text.lower()
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


def get_openline_history_messages(session_id):
    if not session_id:
        return []
    if session_id in OPENLINE_HISTORY_CACHE:
        return OPENLINE_HISTORY_CACHE[session_id]
    try:
        history = bitrix_call(
            "imopenlines.session.history.get",
            {"SESSION_ID": session_id},
            timeout=BITRIX_FAST_TIMEOUT_SECONDS,
        ) or {}
    except Exception:
        return []
    messages = list((history.get("message") or {}).values())
    messages.sort(key=lambda item: item.get("date", ""))
    useful = []
    for message in messages:
        if str(message.get("senderid", "0")) == "0":
            continue
        text = clean_text(message.get("text") or message.get("textlegacy"))
        for fragment in useful_fragments(text):
            if fragment not in useful:
                useful.append(fragment)
            if len(useful) >= 2:
                OPENLINE_HISTORY_CACHE[session_id] = useful
                return useful
    OPENLINE_HISTORY_CACHE[session_id] = useful
    return useful


def get_openline_chat_context(session_id):
    if not session_id:
        return None
    try:
        history = bitrix_call(
            "imopenlines.session.history.get",
            {"SESSION_ID": session_id},
            timeout=BITRIX_TIMEOUT_SECONDS,
        ) or {}
    except Exception:
        return None
    chat = history.get("chat") or {}
    chat_id = history.get("chatId") or chat.get("id")
    if not chat_id:
        return None
    return {
        "sessionId": str(session_id),
        "chatId": str(chat_id),
        "textFieldEnabled": chat.get("textFieldEnabled"),
        "messageType": chat.get("messageType"),
        "entityType": chat.get("entityType"),
        "name": chat.get("name") or "",
    }


def get_deal_messages(deal_id):
    raw_messages = []
    openline_session_ids = []
    try:
        comments = bitrix_call(
            "crm.timeline.comment.list",
            {"filter[ENTITY_ID]": deal_id, "filter[ENTITY_TYPE]": "deal"},
            timeout=BITRIX_FAST_TIMEOUT_SECONDS,
        ) or []
        for item in comments:
            raw_messages.append(("timeline", clean_text(item.get("COMMENT"))))
    except Exception as exc:
        raw_messages.append(("timeline_error", str(exc)))

    try:
        activities = bitrix_call(
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
            },
            timeout=BITRIX_FAST_TIMEOUT_SECONDS,
        ) or []
        for item in activities:
            raw_messages.append(("activity", clean_text(item.get("DESCRIPTION") or item.get("SUBJECT"))))
            if item.get("PROVIDER_ID") == "IMOPENLINES_SESSION" and item.get("ASSOCIATED_ENTITY_ID"):
                openline_session_ids.append(item.get("ASSOCIATED_ENTITY_ID"))
    except Exception as exc:
        raw_messages.append(("activity_error", str(exc)))

    useful = []
    for source, text in raw_messages:
        if source.endswith("_error"):
            continue
        for fragment in useful_fragments(text):
            if fragment not in useful:
                useful.append(fragment)
            if len(useful) >= 2:
                break
        if len(useful) >= 2:
            break

    if len(useful) < 2:
        for session_id in openline_session_ids:
            for fragment in get_openline_history_messages(session_id):
                if fragment not in useful:
                    useful.append(fragment)
                if len(useful) >= 2:
                    break
            if len(useful) >= 2:
                break

    return {
        "useful": useful[:2],
        "rawCount": len([item for item in raw_messages if not item[0].endswith("_error")]),
        "sources": [f"openline_session:{item}" for item in openline_session_ids] + [item[0] for item in raw_messages],
        "openlineSessionIds": openline_session_ids,
    }


def classify(messages):
    joined = " ".join(messages).lower()
    scores = {}
    for destination, keywords in DESTINATION_KEYWORDS.items():
        score = sum(1 for keyword in keywords if keyword in joined)
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
    deal_payload = deal_payload if isinstance(deal_payload, dict) else {}
    classification = deal_payload.get("classification") if isinstance(deal_payload.get("classification"), dict) else None
    messages = deal_payload.get("messages") if isinstance(deal_payload.get("messages"), list) else None
    openline_session_ids = deal_payload.get("openlineSessionIds") if isinstance(deal_payload.get("openlineSessionIds"), list) else []

    if not classification or not openline_session_ids:
        message_data = get_deal_messages(deal_id)
        messages = messages or message_data["useful"]
        openline_session_ids = openline_session_ids or message_data.get("openlineSessionIds") or []
        classification = classification or classify(messages)

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


def send_greeting_message(deal_id, manager_id, text, context, auth=None):
    session_ids = context.get("openlineSessionIds") or []
    errors = []
    for session_id in session_ids:
        chat_context = get_openline_chat_context(session_id)
        if not chat_context:
            errors.append({"sessionId": str(session_id), "error": "Не удалось получить chatId открытой линии."})
            continue
        try:
            answer_result = None
            answer_warning = None
            try:
                answer_result = bitrix_call_for_actor(
                    auth,
                    "imopenlines.operator.answer",
                    {
                        "CHAT_ID": chat_context["chatId"],
                    },
                )
            except Exception as answer_exc:
                answer_warning = str(answer_exc)
                if "ALREADY_RESPONSIBLE" not in answer_warning and "уже ответственный" not in answer_warning.lower():
                    raise
            message_result = bitrix_call_for_actor(
                auth,
                "im.message.add",
                {
                    "DIALOG_ID": f"chat{chat_context['chatId']}",
                    "MESSAGE": text,
                },
            )
            return {
                "ok": True,
                "method": "imopenlines.operator.answer + im.message.add",
                "answerResult": answer_result,
                "answerWarning": answer_warning,
                "messageResult": message_result,
                "chat": chat_context,
            }
        except Exception as exc:
            errors.append({"sessionId": str(session_id), "chatId": chat_context["chatId"], "error": str(exc)})
    return {
        "ok": False,
        "method": "imopenlines.operator.answer + im.message.add",
        "errors": errors or [{"error": "У сделки не найдена открытая линия для автоотправки."}],
    }


def prepare_greeting(manager_id, deal_id, deal_payload=None, auth=None):
    existing = latest_greeting_for_deal(deal_id)
    if existing:
        if GREETING_AUTO_SEND and not existing.get("autoSent"):
            context = greeting_context_from_deal(deal_id, deal_payload)
            if not context.get("openlineSessionIds") and existing.get("openlineSessionIds"):
                context["openlineSessionIds"] = existing.get("openlineSessionIds") or []
            text = existing.get("text") or build_greeting_text(get_manager_profile(manager_id), context["classification"])
            send_result = send_greeting_message(deal_id, manager_id, text, context, auth)
            status = "sent" if send_result.get("ok") else "manual"
            auto_sent = bool(send_result.get("ok"))
            entry = dict(existing)
            entry.update(
                {
                    "timestamp": local_now().isoformat(),
                    "managerId": str(manager_id),
                    "dealId": str(deal_id),
                    "text": text,
                    "status": status,
                    "autoSent": auto_sent,
                    "message": (
                        "Приветствие автоматически отправлено клиенту."
                        if auto_sent
                        else "Автоотправка не прошла. Скопируйте текст и отправьте клиенту вручную."
                    ),
                    "sendResult": send_result,
                }
            )
            append_greeting_log(entry)
            return {
                "ok": True,
                "status": status,
                "autoSent": auto_sent,
                "text": text,
                "direction": entry.get("direction") or "Не определено",
                "message": entry["message"],
                "sendResult": send_result,
                "log": entry,
            }
        return {
            "ok": True,
            "status": "skipped_duplicate",
            "autoSent": existing.get("status") == "sent",
            "text": existing.get("text") or "",
            "message": "Приветствие для этой сделки уже было подготовлено раньше.",
            "log": existing,
        }

    manager = get_manager_profile(manager_id) or {"id": str(manager_id), "name": str(manager_id)}
    context = greeting_context_from_deal(deal_id, deal_payload)
    text = build_greeting_text(manager, context["classification"])
    send_result = None
    status = "manual"
    auto_sent = False
    message = "Текст подготовлен. Автоотправка будет включена после подтверждения канала отправки."
    if GREETING_AUTO_SEND:
        send_result = send_greeting_message(deal_id, manager_id, text, context, auth)
        if send_result.get("ok"):
            status = "sent"
            auto_sent = True
            message = "Приветствие автоматически отправлено клиенту."
        else:
            status = "manual"
            message = "Автоотправка не прошла. Скопируйте текст и отправьте клиенту вручную."

    entry = {
        "timestamp": local_now().isoformat(),
        "managerId": str(manager_id),
        "managerName": manager.get("name") or str(manager_id),
        "dealId": str(deal_id),
        "direction": context["classification"].get("direction") or "Не определено",
        "confidence": context["classification"].get("confidence") or "",
        "openlineSessionIds": context.get("openlineSessionIds") or [],
        "text": text,
        "status": status,
        "autoSent": auto_sent,
        "message": message,
        "sendResult": send_result,
    }
    append_greeting_log(entry)
    return {
        "ok": True,
        "status": entry["status"],
        "autoSent": auto_sent,
        "text": text,
        "direction": entry["direction"],
        "message": entry["message"],
        "sendResult": send_result,
        "log": entry,
    }


def deal_score_for_manager(deal, manager):
    competencies = [item.lower() for item in manager.get("competencies", [])]
    direction_name = deal["classification"]["direction"]
    if direction_name == "Не определено":
        return 1
    if not competencies:
        return 0
    direction = direction_name.lower()
    text = " ".join(deal.get("messages", [])).lower()
    score = 0
    for competency in competencies:
        if competency and competency in direction:
            score += 4
        if competency and competency in text:
            score += 2
    if deal["classification"]["direction"] != "Не определено" and score:
        score += 1
    return score


def list_allowed_deal_headers():
    pending = []
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(SOURCE_STAGES))
    futures = {
        executor.submit(
            bitrix_call,
            "crm.deal.list",
            {
                "filter[STAGE_ID]": stage_id,
                "select[]": ["ID", "TITLE", "STAGE_ID", "ASSIGNED_BY_ID", "DATE_CREATE", "DATE_MODIFY"],
                "order[DATE_CREATE]": "ASC",
            },
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
        executor.shutdown(wait=False, cancel_futures=True)

    pending.sort(key=lambda item: (item.get("DATE_CREATE") or "", int(item.get("ID") or 0)))
    return pending


def analyze_deal_header(deal):
    deal_id = str(deal.get("ID") or "")
    cache_version = str(deal.get("DATE_MODIFY") or deal.get("DATE_CREATE") or "")
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
    analyzed = {
        "id": deal.get("ID"),
        "title": deal.get("TITLE"),
        "stageId": deal.get("STAGE_ID"),
        "stageName": deal.get("_stageName") or SOURCE_STAGES.get(deal.get("STAGE_ID"), ""),
        "assignedById": deal.get("ASSIGNED_BY_ID"),
        "dateCreate": deal.get("DATE_CREATE"),
        "messages": messages["useful"],
        "rawMessageCount": messages["rawCount"],
        "messageSources": messages["sources"],
        "openlineSessionIds": messages["openlineSessionIds"],
        "dealUrl": f"{portal_base_url()}/crm/deal/details/{deal_id}/",
        "classification": classify(messages["useful"]),
    }
    with DEAL_ANALYSIS_CACHE_LOCK:
        DEAL_ANALYSIS_CACHE[deal_id] = {
            "version": cache_version,
            "cachedAt": now,
            "deal": analyzed,
        }
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
        executor.shutdown(wait=False, cancel_futures=True)
    return analyzed, errors


def get_next_deal_for_manager(manager_id, skipped=None):
    skipped = set(str(item) for item in (skipped or []))
    manager = get_manager_profile(manager_id)
    if not manager:
        return {"deal": None, "reason": "Менеджер не найден в настройке компетенций."}
    access = check_manager_access(manager_id)
    manager["accessRule"] = access.get("rule")
    if not access["ok"]:
        return {"manager": manager, "deal": None, "reason": access["reason"]}

    headers = [
        header
        for header in list_allowed_deal_headers()
        if str(header.get("ID") or "") not in skipped
    ]
    batch_limit = max(1, NEXT_DEAL_SCAN_LIMIT)
    batch_headers = headers[:batch_limit]
    scanned_ids = [str(header.get("ID") or "") for header in batch_headers]
    analyzed, errors = analyze_deal_headers(batch_headers)
    response_meta = {
        "scannedDealIds": scanned_ids,
        "checkedCount": len(scanned_ids),
        "hasMore": len(headers) > len(batch_headers),
        "partialTimeouts": len(errors),
    }

    for header in batch_headers:
        deal = analyzed.get(str(header.get("ID") or ""))
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
        return {"manager": manager, "deal": deal, **response_meta}

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


def preview_claim(deal_id, manager_id, deal_payload=None, auth=None):
    with DATA_LOCK:
        access = check_manager_access(manager_id)
        if not access["ok"]:
            return {
                "ok": False,
                "dryRun": DRY_RUN,
                "message": access["reason"],
                "rule": access.get("rule"),
            }
        deal = bitrix_call("crm.deal.get", {"id": deal_id})
        if not deal:
            raise RuntimeError("Сделка не найдена")
        current_stage = deal.get("STAGE_ID")
        if current_stage not in SOURCE_STAGES:
            return {
                "ok": False,
                "dryRun": True,
                "message": "Сделка уже не в разрешенной стадии, выдавать ее нельзя.",
                "currentStage": current_stage,
            }
        if DRY_RUN:
            return {
                "ok": True,
                "dryRun": True,
                "dealId": str(deal_id),
                "wouldSet": {
                    "ASSIGNED_BY_ID": str(manager_id),
                    "STAGE_ID": TARGET_STAGE,
                    "STAGE_NAME": TARGET_STAGE_NAME,
                },
                "message": "Проверка успешна. В безопасном режиме CRM не изменена.",
            }
        bitrix_call(
            "crm.deal.update",
            {
                "id": deal_id,
                "fields[ASSIGNED_BY_ID]": manager_id,
                "fields[STAGE_ID]": TARGET_STAGE,
            },
        )
        updated_deal = dict(deal)
        updated_deal["ID"] = deal_id
        log_entry = append_claim_log(manager_id, updated_deal)
        greeting = prepare_greeting(manager_id, deal_id, deal_payload, auth)
        return {
            "ok": True,
            "dryRun": False,
            "dealId": str(deal_id),
            "log": log_entry,
            "greeting": greeting,
            "updated": {
                "ASSIGNED_BY_ID": str(manager_id),
                "STAGE_ID": TARGET_STAGE,
                "STAGE_NAME": TARGET_STAGE_NAME,
            },
            "message": "Сделка назначена менеджеру и переведена в работу.",
        }


def record_rejection(manager_id, payload):
    deal = payload.get("deal") if isinstance(payload.get("deal"), dict) else {}
    deal_id = str(payload.get("dealId") or deal.get("id") or deal.get("ID") or "").strip()
    if not deal_id:
        return {"ok": False, "message": "Не указана сделка для отказа."}
    deal.setdefault("id", deal_id)
    reason = normalize_reject_reason(payload.get("reason"))
    with DATA_LOCK:
        entry = append_reject_log(manager_id, deal, reason)
    return {
        "ok": True,
        "dealId": deal_id,
        "reason": reason,
        "reasonLabel": REJECT_REASONS[reason],
        "log": entry,
        "message": "Отказ сохранен.",
    }


def list_portal_users():
    users = []
    start = 0
    while True:
        params = {
            "FILTER[ACTIVE]": "Y",
            "SORT": "LAST_NAME",
            "ORDER": "ASC",
            "start": start,
        }
        payload = bitrix_call_full("user.search", params)
        users.extend(payload.get("result") or [])
        if "next" not in payload:
            break
        start = payload.get("next")
    return users


def admin_state(payload):
    admin = require_admin(payload)
    if not admin:
        return {"ok": False, "isAdmin": False, "message": "Этот раздел доступен только администратору."}

    today = local_date()
    date_from = normalize_date(payload.get("dateFrom"), today)
    date_to = normalize_date(payload.get("dateTo"), date_from)
    rules = load_access_rules().get("managers", {})
    log = load_claim_log()
    reject_log = load_reject_log()
    log_manager_ids = {str(item.get("managerId")) for item in log if item.get("managerId")}
    reject_manager_ids = {str(item.get("managerId")) for item in reject_log if item.get("managerId")}
    manager_ids = set(rules.keys()) | log_manager_ids | reject_manager_ids
    portal_users = list_portal_users()
    profiles_by_id = get_manager_profiles_bulk(portal_users)

    rows = []
    for user in portal_users:
        manager_id = str(user.get("ID") or "")
        if not manager_id:
            continue
        profile = profiles_by_id.get(manager_id) or {}
        competencies = profile.get("competencies") or []
        if not competencies:
            continue
        manager_ids.discard(manager_id)
        rule = get_manager_rule(manager_id)
        rows.append(
            {
                "id": manager_id,
                "name": profile.get("name") or " ".join(part for part in [user.get("NAME"), user.get("LAST_NAME")] if part).strip() or manager_id,
                "competencies": competencies,
                "rule": rule,
                "takenInPeriod": count_claims_in_log(log, manager_id, date_from, date_to),
                "takenToday": count_claims_in_log(log, manager_id, today, today),
                "rejectedInPeriod": count_rejections_in_log(reject_log, manager_id, date_from, date_to),
                "rejectedToday": count_rejections_in_log(reject_log, manager_id, today, today),
                "topRejectReason": rejection_reason_summary(reject_log, manager_id, date_from, date_to),
            }
        )

    for manager_id in sorted(manager_ids):
        profile = get_manager_profile(manager_id) or {"id": manager_id, "name": manager_id, "competencies": []}
        if not profile.get("competencies"):
            continue
        rows.append(
            {
                "id": manager_id,
                "name": profile.get("name") or manager_id,
                "competencies": profile.get("competencies") or [],
                "rule": get_manager_rule(manager_id),
                "takenInPeriod": count_claims_in_log(log, manager_id, date_from, date_to),
                "takenToday": count_claims_in_log(log, manager_id, today, today),
                "rejectedInPeriod": count_rejections_in_log(reject_log, manager_id, date_from, date_to),
                "rejectedToday": count_rejections_in_log(reject_log, manager_id, today, today),
                "topRejectReason": rejection_reason_summary(reject_log, manager_id, date_from, date_to),
            }
        )

    rows.sort(key=lambda item: item["name"].lower())
    return {
        "ok": True,
        "isAdmin": True,
        "admin": {"id": admin["id"], "name": admin.get("name")},
        "dateFrom": date_from,
        "dateTo": date_to,
        "today": today,
        "managers": rows,
    }


def update_admin_rule(payload):
    admin = require_admin(payload)
    if not admin:
        return {"ok": False, "isAdmin": False, "message": "Недостаточно прав."}
    manager_id = str(payload.get("managerId") or "").strip()
    if not manager_id:
        return {"ok": False, "message": "Не указан менеджер."}
    rule = set_manager_rule(
        manager_id,
        enabled=payload.get("enabled", True),
        daily_limit=payload.get("dailyLimit"),
        note=payload.get("note") or "",
    )
    return {"ok": True, "managerId": manager_id, "rule": rule}


def current_user_state(payload):
    user = verify_bitrix_user(payload.get("auth"))
    if not user:
        return {"ok": False, "message": "Не удалось определить авторизованного пользователя Битрикс."}
    manager = get_manager_profile(user["id"])
    return {"ok": True, "user": user, "manager": manager}


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
        "REFRESH_ID": pick("REFRESH_ID", "refresh_token"),
        "DOMAIN": pick("DOMAIN", "domain"),
        "member_id": pick("member_id", "MEMBER_ID"),
        "client_endpoint": pick("client_endpoint", "CLIENT_ENDPOINT"),
        "user_id": pick("user_id", "USER_ID"),
    }
    return {key: value for key, value in auth.items() if value}




def render_index_html(install_mode=False, initial_auth=None):
    return (
        INDEX_HTML
        .replace("__PUBLIC_APP_URL__", PUBLIC_APP_URL)
        .replace("__INSTALL_MODE__", "true" if install_mode else "false")
        .replace("__ADMIN_USER_IDS__", json.dumps(sorted(ADMIN_USER_IDS), ensure_ascii=False))
        .replace("__INITIAL_AUTH__", json.dumps(initial_auth or {}, ensure_ascii=False))
        .replace("__ALLOW_UNVERIFIED_USERS__", "true" if ALLOW_UNVERIFIED_USERS else "false")
    )


def looks_like_install_payload(raw_payload):
    lowered = raw_payload.lower()
    markers = [
        b"install=y",
        b"event=onappinstall",
        b"application_token",
        b"app_sid",
    ]
    return any(marker in lowered for marker in markers)


class Handler(BaseHTTPRequestHandler):
    def send_json(self, payload, status=200):
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/install":
            query = urllib.parse.parse_qs(parsed.query)
            body = render_index_html(True, extract_initial_auth_from_values(query)).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/api/deals":
            try:
                self.send_json({"sourceStages": SOURCE_STAGES, "targetStage": TARGET_STAGE, "deals": list_allowed_deals()})
            except Exception as exc:
                self.send_json({"error": str(exc)}, 500)
            return
        if parsed.path == "/api/managers":
            try:
                self.send_json({"managers": load_managers()})
            except Exception as exc:
                self.send_json({"error": str(exc)}, 500)
            return
        if parsed.path == "/api/health":
            self.send_json(
                {
                    "ok": True,
                    "version": APP_VERSION,
                    "sourceStages": SOURCE_STAGES,
                    "debugDealLimit": 20,
                    "nextDealScanLimit": NEXT_DEAL_SCAN_LIMIT,
                    "nextDealScanWorkers": NEXT_DEAL_SCAN_WORKERS,
                    "nextDealBatchTimeoutSeconds": NEXT_DEAL_BATCH_TIMEOUT_SECONDS,
                    "dryRun": DRY_RUN,
                    "sundayLimitBypass": True,
                    "limitFreeWindow": {
                        "start": LIMIT_FREE_WINDOW_START,
                        "end": LIMIT_FREE_WINDOW_END,
                        "activeNow": is_limit_free_time(),
                    },
                    "rejectReasons": REJECT_REASONS,
                    "greetingAutoSend": GREETING_AUTO_SEND,
                    "greetingLog": True,
                }
            )
            return
        if parsed.path == "/api/manager":
            try:
                query = urllib.parse.parse_qs(parsed.query)
                manager_id = (query.get("managerId") or [""])[0]
                self.send_json({"manager": get_manager_profile(manager_id)})
            except Exception as exc:
                self.send_json({"error": str(exc)}, 500)
            return
        if parsed.path == "/" or not parsed.path.startswith("/api/"):
            query = urllib.parse.parse_qs(parsed.query)
            install_mode = parsed.path == "/install" or (query.get("install") or query.get("INSTALL") or [""])[0].upper() == "Y"
            body = render_index_html(install_mode, extract_initial_auth_from_values(query)).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path in {"/", "/install"}:
            length = int(self.headers.get("Content-Length", "0"))
            raw_payload = self.rfile.read(length) if length else b""
            install_mode = parsed.path == "/install" or looks_like_install_payload(raw_payload)
            initial_auth = extract_initial_auth(raw_payload)
            body = render_index_html(install_mode, initial_auth).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if not parsed.path.startswith("/api/"):
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length) or b"{}")
        try:
            if parsed.path == "/api/next-deal":
                manager_id = actor_id_from_payload(payload)
                if not manager_id:
                    self.send_json({"deal": None, "reason": "Не удалось подтвердить пользователя Битрикс."}, 401)
                    return
                result = get_next_deal_for_manager(manager_id, payload.get("skipped") or [])
                self.send_json(result)
                return
            if parsed.path == "/api/claim":
                manager_id = actor_id_from_payload(payload)
                if not manager_id:
                    self.send_json({"ok": False, "message": "Не удалось подтвердить пользователя Битрикс."}, 401)
                    return
                result = preview_claim(payload.get("dealId"), manager_id, payload.get("deal"), payload.get("auth"))
                self.send_json(result)
                return
            if parsed.path == "/api/reject":
                manager_id = actor_id_from_payload(payload)
                if not manager_id:
                    self.send_json({"ok": False, "message": "Не удалось подтвердить пользователя Битрикс."}, 401)
                    return
                result = record_rejection(manager_id, payload)
                self.send_json(result, 200 if result.get("ok") else 400)
                return
            if parsed.path == "/api/admin/state":
                self.send_json(admin_state(payload))
                return
            if parsed.path == "/api/admin/rule":
                self.send_json(update_admin_rule(payload))
                return
            if parsed.path == "/api/current-user":
                result = current_user_state(payload)
                self.send_json(result, 200 if result.get("ok") else 401)
                return
            self.send_error(404)
        except Exception as exc:
            sys.stderr.write(f"API error {parsed.path}: {exc}\n")
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
            self.send_json({"ok": False, "error": str(exc)}, 500)

    def log_message(self, fmt, *args):
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))


INDEX_HTML = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Получить сделку - тест</title>
  <style>
    :root { color-scheme: light; font-family: Arial, sans-serif; background: #f6f7f9; color: #1f2933; }
    body { margin: 0; }
    header { background: #ffffff; border-bottom: 1px solid #d9dee7; padding: 18px 22px; }
    main { padding: 18px 22px 32px; max-width: 1180px; margin: 0 auto; }
    h1 { font-size: 22px; margin: 0 0 8px; }
    .toolbar { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin: 16px 0; }
    input, select { height: 36px; border: 1px solid #b9c2cf; border-radius: 6px; padding: 0 10px; min-width: 190px; background: #fff; }
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
  </style>
</head>
<body>
  <header>
    <h1>Получить сделку - безопасный тест</h1>
    <div class="meta">Читаем только “Необработанные ЛИДЫ” и “ОЖИДАЮТ СПЕЦИАЛИСТА”. Запись включается только через настройку DRY_RUN=0.</div>
  </header>
  <main>
    <div class="toolbar">
      <select id="managerSelect" class="hidden" onchange="syncManagerId()"></select>
      <input id="managerId" class="hidden" placeholder="ID менеджера для проверки">
      <button id="getDealButton" onclick="getDeal()" disabled>Получить сделку</button>
    </div>
    <div id="managerInfo" class="status"></div>
    <div id="status" class="status">Определяю пользователя Битрикс...</div>
    <div id="searchProgress" class="search-progress hidden">
      <div class="search-progress-track"><div class="search-progress-bar"></div></div>
      <div class="search-progress-text">Ищем подходящую заявку по навыкам менеджера...</div>
    </div>
    <div id="cards"></div>
    <pre id="result" hidden></pre>
    <section id="greetingBox" class="card greeting-box hidden"></section>
    <section id="adminPanel" class="admin-panel hidden">
      <h2>Доступ менеджеров</h2>
      <div class="meta">Этот блок видит только администратор. Здесь можно закрыть выдачу заявок или поставить дневной лимит. Лимит не применяется в воскресенье и каждый день с 18:00 до 21:30.</div>
      <div class="toolbar">
        <input id="statsFrom" type="date">
        <input id="statsTo" type="date">
        <button class="secondary" onclick="loadAdminPanel()">Обновить статистику</button>
      </div>
      <div id="adminRows"></div>
    </section>
  </main>
<script src="//api.bitrix24.com/api/v1/"></script>
<script>
let selectedDealId = null;
let currentDeal = null;
let skippedDeals = [];
let managers = [];
let currentBitrixUser = null;
let isDealSearchRunning = false;
let userIdentified = false;
const MAX_SEARCH_BATCHES = 8;
const PUBLIC_APP_URL = "__PUBLIC_APP_URL__";
const INSTALL_MODE = __INSTALL_MODE__;
const ADMIN_USER_IDS = __ADMIN_USER_IDS__;
const INITIAL_AUTH = __INITIAL_AUTH__;
const ALLOW_UNVERIFIED_USERS = __ALLOW_UNVERIFIED_USERS__;
const REJECT_REASONS = {
  not_my_country: 'Не моя страна',
  unclear_request: 'Непонятный запрос',
  duplicate: 'Дубль',
  other: 'Другое'
};
const PUBLIC_APP_ORIGIN = (() => {
  try { return new URL(PUBLIC_APP_URL).origin; } catch (error) { return ''; }
})();
const API_BASE = PUBLIC_APP_ORIGIN && window.location.origin !== PUBLIC_APP_ORIGIN
  ? PUBLIC_APP_ORIGIN
  : '';
function apiUrl(path) {
  if (/^https?:\/\//i.test(path)) return path;
  return `${API_BASE}${path}`;
}
function currentUserId() {
  return String(
    (currentBitrixUser && (currentBitrixUser.ID || currentBitrixUser.id))
    || document.getElementById('managerId').value
    || ''
  );
}
function isCurrentUserAdmin() {
  return ADMIN_USER_IDS.includes(currentUserId());
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
  const response = await fetch(apiUrl(url), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload || {})
  });
  const text = await response.text();
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
async function getJson(url) {
  const response = await fetch(apiUrl(url));
  const text = await response.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch (error) {
    throw new Error(`Сервер вернул не JSON для ${url}.`);
  }
  if (!response.ok) {
    throw new Error(data.message || data.reason || data.error || 'Ошибка запроса');
  }
  return data;
}
async function loadManagers() {
  document.getElementById('status').textContent = 'Определяю пользователя Битрикс...';
  let data;
  try {
    data = await getJson('/api/managers');
  } catch (error) {
    document.getElementById('status').textContent = error.message || 'Не удалось загрузить приложение.';
    return;
  }
  managers = data.managers || [];
  const select = document.getElementById('managerSelect');
  select.innerHTML = '<option value="">Выберите менеджера</option>' + managers
    .filter((manager) => manager.active !== false)
    .map((manager) => `<option value="${escapeHtml(manager.id)}">${escapeHtml(manager.name)}</option>`)
    .join('');
  detectBitrixUser();
}
function applyAuthorizedUser(user, manager) {
  const rawUser = user && user.raw ? user.raw : user;
  currentBitrixUser = rawUser || {};
  const managerId = String((rawUser && rawUser.ID) || (user && user.id) || (manager && manager.id) || '');
  if (managerId) {
    document.getElementById('managerId').value = managerId;
  }
  bitrixUserVerified = true;
  updateUserReadiness();
  document.getElementById('managerSelect').classList.add('hidden');
  document.getElementById('managerId').classList.add('hidden');
  if (manager) {
    renderManagerInfo(manager);
    loadAdminPanel();
  } else if (managerId) {
    loadCurrentManagerFromBitrix(managerId).then(() => {
      loadAdminPanel();
    });
  }
}
function renderManagerInfo(manager) {
  document.getElementById('managerInfo').textContent = (manager.competencies || []).length
    ? `Вы вошли как ${manager.name}. Навыки из карточки сотрудника: ${manager.competencies.join(', ')}`
    : `Вы вошли как ${manager.name}. В карточке сотрудника не заполнено поле “Навыки”.`;
  document.getElementById('status').textContent = (manager.competencies || []).length
    ? 'Нажмите “Получить сделку”.'
    : 'Заполните поле “Навыки” в карточке сотрудника, иначе подбор невозможен.';
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
function detectBitrixUser() {
  if (!window.BX24 || !BX24.init || !BX24.callMethod) {
    detectUserFromServerAuth().then((found) => {
      if (!found) {
        if (ALLOW_UNVERIFIED_USERS) {
          document.getElementById('managerSelect').classList.remove('hidden');
          document.getElementById('managerId').classList.remove('hidden');
          document.getElementById('status').textContent = 'Локальный тест: выберите менеджера вручную.';
        } else {
          document.getElementById('status').textContent = 'Приложение должно быть открыто внутри Bitrix24.';
        }
      }
    });
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
    if (BX24.installFinish) {
      try {
        BX24.installFinish();
      } catch (error) {}
    }
    BX24.callMethod('user.current', {}, (result) => {
      if (result.error()) {
        detectUserFromServerAuth().then((found) => {
          if (!found) document.getElementById('status').textContent = 'Не удалось определить пользователя Битрикс. Проверьте права приложения.';
        });
        return;
      }
      applyAuthorizedUser(result.data(), null);
    });
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
async function loadCurrentManagerFromBitrix(managerId) {
  let data;
  try {
    data = await getJson(`/api/manager?managerId=${encodeURIComponent(managerId)}`);
  } catch (error) {
    document.getElementById('status').textContent = error.message || 'Не удалось загрузить профиль менеджера.';
    return;
  }
  const manager = data.manager;
  if (!manager) {
    document.getElementById('managerInfo').textContent = `Вы вошли как ${currentBitrixUser.NAME || ''} ${currentBitrixUser.LAST_NAME || ''}. Пользователь не найден.`;
    document.getElementById('status').textContent = 'Проверьте доступ приложения к пользователям.';
    return;
  }
  renderManagerInfo(manager);
}
let bitrixUserVerified = false;

function updateUserReadiness() {
  if (ALLOW_UNVERIFIED_USERS) {
    const managerId = document.getElementById('managerId').value.trim();
    userIdentified = Boolean(managerId) || bitrixUserVerified;
  } else {
    userIdentified = bitrixUserVerified;
  }
  document.getElementById('getDealButton').disabled = !userIdentified;
}
function syncManagerId() {
  const select = document.getElementById('managerSelect');
  const managerId = select.value;
  document.getElementById('managerId').value = managerId;
  currentDeal = null;
  skippedDeals = [];
  document.getElementById('cards').innerHTML = '';
  document.getElementById('result').hidden = true;
  clearGreeting();
  setSearching(false);
  updateUserReadiness();
  const manager = managers.find((item) => String(item.id) === String(managerId));
  document.getElementById('managerInfo').textContent = manager
    ? `Компетенции: ${(manager.competencies || []).join(', ')}`
    : '';
}
function setSearching(isSearching, text) {
  const progress = document.getElementById('searchProgress');
  const button = document.getElementById('getDealButton');
  progress.classList.toggle('hidden', !isSearching);
  if (button) {
    button.disabled = isSearching || !userIdentified;
    button.textContent = isSearching ? 'Ищем...' : 'Получить сделку';
  }
  if (text) {
    progress.querySelector('.search-progress-text').textContent = text;
  }
}
async function getDeal() {
  if (isDealSearchRunning) return;
  if (!userIdentified) {
    document.getElementById('status').textContent = 'Не удалось определить пользователя Битрикс. Обновите страницу приложения.';
    return;
  }
  isDealSearchRunning = true;
  selectedDealId = null;
  currentDeal = null;
  document.getElementById('status').textContent = 'Ищу подходящую сделку...';
  setSearching(true);
  document.getElementById('result').hidden = true;
  clearGreeting();
  const auth = currentAuth();
  const managerId = document.getElementById('managerId').value.trim();
  let data = null;
  const searchSkipped = new Set(skippedDeals.map(String));
  let checkedCount = 0;
  try {
    for (let batch = 0; batch < MAX_SEARCH_BATCHES; batch += 1) {
      data = await postJson('/api/next-deal', {
        auth,
        managerId,
        currentUserId: currentUserId(),
        skipped: Array.from(searchSkipped)
      });
      (data.scannedDealIds || []).forEach((dealId) => searchSkipped.add(String(dealId)));
      checkedCount += Number(data.checkedCount || 0);
      if (data.deal || !data.hasMore) break;
      setSearching(true, `Проверено заявок: ${checkedCount}. Ищу дальше, начиная со старых...`);
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
    .map(([reason, label]) => `<button class="reason" onclick="rejectDeal('${escapeHtml(reason)}')">${escapeHtml(label)}</button>`)
    .join('');
  card.innerHTML = `
    <div class="badge">Сделка #${deal.id}</div>
    <div class="badge">${escapeHtml(deal.stageName)}</div>
    <div class="badge">Направление: ${escapeHtml(deal.classification.direction)}</div>
    <h3>${escapeHtml(deal.title || 'Без названия')}</h3>
    <div class="meta">Ответственный ID: ${escapeHtml(String(deal.assignedById || ''))}<br>Создана: ${escapeHtml(deal.dateCreate || '')}<br>Уверенность: ${escapeHtml(deal.classification.confidence)}<br>${escapeHtml(deal.matchReason || '')}</div>
    ${messages}
    <div class="toolbar">
      <button class="claim-button" onclick="claimSelected()">Взять в работу</button>
    </div>
    <div class="reject-reasons">
      <span class="label">Отказаться:</span>
      ${rejectButtons}
    </div>
  `;
  return card;
}
async function claimSelected() {
    const managerId = document.getElementById('managerId').value.trim();
    const auth = currentAuth();
    if (!selectedDealId) return showResult({ ok: false, message: 'Сначала выберите сделку.' });
    if (!userIdentified) return showResult({ ok: false, message: 'Не удалось определить пользователя Битрикс.' });
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
      currentUserId: currentUserId(),
      deal: currentDeal ? {
        id: currentDeal.id,
        title: currentDeal.title || '',
        classification: currentDeal.classification || {},
        messages: currentDeal.messages || [],
        openlineSessionIds: currentDeal.openlineSessionIds || []
      } : null
    });
  } catch (error) {
    payload = { ok: false, message: error.message || 'Не удалось взять сделку.' };
  }
  showResult(payload);
  renderGreeting(payload.greeting);
  if (payload.ok && currentDeal && currentDeal.dealUrl) {
    loadAdminPanel();
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
  }
}
function rejectDeal(reason) {
  if (!currentDeal) return;
  const rejectedDeal = currentDeal;
  const managerId = document.getElementById('managerId').value.trim();
  const auth = currentAuth();
  reason = reason || 'other';
  skippedDeals.push(String(rejectedDeal.id));
  currentDeal = null;
  selectedDealId = null;
  document.getElementById('cards').innerHTML = '';
  document.getElementById('status').textContent = `Отказ: ${REJECT_REASONS[reason] || REJECT_REASONS.other}. Ищу следующую...`;
  document.getElementById('result').hidden = true;
  clearGreeting();
  postJson('/api/reject', {
    auth,
    managerId,
    currentUserId: currentUserId(),
    dealId: rejectedDeal.id,
    reason,
    deal: {
      id: rejectedDeal.id,
      title: rejectedDeal.title || '',
      stageId: rejectedDeal.stageId || '',
      stageName: rejectedDeal.stageName || '',
      classification: rejectedDeal.classification || {}
    }
  }).then(() => {
    loadAdminPanel();
  }).catch(() => {});
  getDeal();
}
async function loadAdminPanel() {
  if (!currentBitrixUser) return;
  if (!isCurrentUserAdmin()) return;
  const panel = document.getElementById('adminPanel');
  panel.classList.remove('hidden');
  document.getElementById('adminRows').innerHTML = '<div class="status">Загружаю настройки доступа...</div>';
  const today = new Date().toISOString().slice(0, 10);
  const from = document.getElementById('statsFrom');
  const to = document.getElementById('statsTo');
  if (!from.value) from.value = today;
  if (!to.value) to.value = today;
  let data;
  try {
    data = await postJson('/api/admin/state', {
      auth: currentAuth(),
      currentUserId: currentUserId(),
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
  renderAdminRows(data.managers || []);
}
function renderAdminRows(rows) {
  const target = document.getElementById('adminRows');
  if (!rows.length) {
    target.innerHTML = '<div class="status">Пока нет менеджеров с навыками или статистикой.</div>';
    return;
  }
  target.innerHTML = `
    <table class="admin-table">
      <thead>
        <tr>
          <th>Менеджер</th>
          <th>Навыки</th>
          <th>Доступ</th>
          <th>Лимит в день</th>
          <th>За период</th>
          <th>Сегодня</th>
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
            <td><button class="secondary" onclick="saveAccessRule('${escapeHtml(row.id)}')">Сохранить</button></td>
          </tr>
        `).join('')}
      </tbody>
    </table>
  `;
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
      currentUserId: currentUserId(),
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
  if (!greeting || !greeting.text) {
    clearGreeting();
    return;
  }
  box.innerHTML = '';
  box.classList.remove('hidden');

  const title = document.createElement('div');
  title.className = 'greeting-title';
  title.textContent = greeting.autoSent ? 'Сообщение клиенту отправлено' : 'Текст клиенту';

  const meta = document.createElement('div');
  meta.className = 'meta';
  meta.textContent = greeting.autoSent
    ? 'Приветствие сохранено в журнале.'
    : 'Автоотправка пока не включена для канала. Скопируйте текст и отправьте клиенту в чате сделки.';

  const text = document.createElement('div');
  text.className = 'greeting-text';
  text.textContent = greeting.text;

  const button = document.createElement('button');
  button.className = 'secondary';
  button.textContent = 'Скопировать текст';
  button.onclick = () => copyGreetingText(greeting.text, button);

  box.appendChild(title);
  box.appendChild(meta);
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
    result.hidden = true;
    if (payload.message) document.getElementById('status').textContent = payload.message;
    return;
  }
  result.hidden = false;
  result.textContent = JSON.stringify(payload, null, 2);
}
function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (ch) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[ch]));
}
if (INSTALL_MODE) {
  document.getElementById('status').textContent = 'Завершаю установку приложения...';
  detectBitrixUser();
} else {
  loadManagers();
}
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
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Открыть тест: http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
