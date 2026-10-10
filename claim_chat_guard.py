"""Read-only protection against claiming another operator's active dialog."""

import math
import re
import time


MAX_ACTIVE_CLAIM_CHATS = 10
MAX_CLAIM_CHAT_GUARD_SECONDS = 8.0
GUARD_REASONS = frozenset({
    "invalid_identity", "invalid_active_chat_list", "too_many_active_chats",
    "invalid_active_chat_row", "invalid_dialog_deal_binding",
    "dialog_deal_binding_mismatch", "invalid_dialog", "dialog_id_mismatch",
    "dialog_not_openline", "owner_identity_mismatch", "invalid_owner_identity",
    "invalid_owner_departments", "owner_not_internal", "owner_staff_unconfirmed",
    "invalid_guard_timeout", "chat_guard_timeout", "chat_guard_read_failed",
})
GUARD_METHODS = frozenset({
    "imopenlines.crm.chat.get", "imopenlines.dialog.get", "im.user.get",
})
GUARD_UPSTREAM_CODES = frozenset({
    "QUERY_LIMIT_EXCEEDED", "OPERATION_TIME_LIMIT", "ACCESS_DENIED",
    "ERROR_ACCESS_DENIED", "INVALID_CREDENTIALS", "INVALID_TOKEN",
    "NO_AUTH_FOUND", "METHOD_NOT_FOUND", "ERROR_METHOD_NOT_FOUND",
    "NOT_FOUND", "ERROR_NOT_FOUND",
})


class ClaimChatGuardUnavailable(RuntimeError):
    """The current dialog ownership could not be established safely."""


def claim_chat_guard_diagnostic(phase, error):
    """Return finite metadata only; never serialize an exception or its cause."""
    reason = error.args[0] if error.args else None
    method = getattr(error, "method", None)
    duration = getattr(error, "duration_ms", None)
    status = getattr(error, "upstream_status", None)
    code = getattr(error, "upstream_code", None)
    return {
        "event": "claim_chat_guard_unavailable",
        "phase": phase if type(phase) is str and phase in {"search", "claim", "greeting"} else "unknown",
        "reason": reason if type(reason) is str and reason in GUARD_REASONS else "unknown",
        "method": method if type(method) is str and method in GUARD_METHODS else None,
        "durationMs": round(duration, 1) if type(duration) in (int, float) and 0 <= duration <= 60_000 and math.isfinite(duration) else None,
        "upstreamStatus": status if type(status) is int and 100 <= status <= 599 else None,
        "upstreamCode": code if type(code) is str and code in GUARD_UPSTREAM_CODES else "other" if code is not None else None,
    }


def _positive_id(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ClaimChatGuardUnavailable("invalid_identity")
    text = str(value).strip()
    if len(text) > 20 or not re.fullmatch(r"[0-9]+", text) or int(text) <= 0:
        raise ClaimChatGuardUnavailable("invalid_identity")
    return str(int(text))


def _active_chat_ids(value):
    if isinstance(value, dict):
        rows = list(value.values())
    elif isinstance(value, list):
        rows = value
    else:
        raise ClaimChatGuardUnavailable("invalid_active_chat_list")
    if len(rows) > MAX_ACTIVE_CLAIM_CHATS:
        raise ClaimChatGuardUnavailable("too_many_active_chats")
    chat_ids = []
    for row in rows:
        if not isinstance(row, dict):
            raise ClaimChatGuardUnavailable("invalid_active_chat_row")
        chat_id = _positive_id(row.get("CHAT_ID"))
        if chat_id not in chat_ids:
            chat_ids.append(chat_id)
    return chat_ids


def _validate_deal_binding(value, deal_id):
    # ENTITY_DATA_2 consists of typed pairs, e.g. LEAD|0|DEAL|123|CONTACT|45.
    # A contact/lead ID or a substring matching the deal is not its binding.
    if not isinstance(value, str):
        raise ClaimChatGuardUnavailable("invalid_dialog_deal_binding")
    parts = [part.strip() for part in value.split("|")]
    if not parts or len(parts) % 2:
        raise ClaimChatGuardUnavailable("invalid_dialog_deal_binding")
    bindings = {}
    for index in range(0, len(parts), 2):
        kind, identifier = parts[index].upper(), parts[index + 1]
        if (
            not re.fullmatch(r"[A-Z_]+", kind)
            or len(identifier) > 20
            or not re.fullmatch(r"[0-9]+", identifier)
            or kind in bindings
        ):
            raise ClaimChatGuardUnavailable("invalid_dialog_deal_binding")
        bindings[kind] = str(int(identifier))
    if bindings.get("DEAL") != deal_id:
        raise ClaimChatGuardUnavailable("dialog_deal_binding_mismatch")


def _dialog_owner(dialog, chat_id, deal_id):
    if not isinstance(dialog, dict):
        raise ClaimChatGuardUnavailable("invalid_dialog")
    if _positive_id(dialog.get("id")) != chat_id:
        raise ClaimChatGuardUnavailable("dialog_id_mismatch")
    if (
        str(dialog.get("type") or "").strip().upper() != "LINES"
        or str(dialog.get("entity_type") or "").strip().upper() != "LINES"
    ):
        raise ClaimChatGuardUnavailable("dialog_not_openline")
    _validate_deal_binding(dialog.get("entity_data_2"), deal_id)
    return _positive_id(dialog.get("owner"))


def _owner_is_staff(user, owner_id):
    if not isinstance(user, dict) or _positive_id(user.get("id")) != owner_id:
        raise ClaimChatGuardUnavailable("owner_identity_mismatch")
    for field in ("bot", "connector", "extranet"):
        if type(user.get(field)) is not bool:
            raise ClaimChatGuardUnavailable("invalid_owner_identity")
    departments = user.get("departments")
    if not isinstance(departments, list):
        raise ClaimChatGuardUnavailable("invalid_owner_departments")
    for department in departments:
        _positive_id(department)
    if user["connector"] or user["extranet"]:
        raise ClaimChatGuardUnavailable("owner_not_internal")
    if user["bot"]:
        return False
    if not departments:
        raise ClaimChatGuardUnavailable("owner_staff_unconfirmed")
    # Account activity does not release an accepted dialog. Inactive employees
    # remain owners until an explicit transfer or completion in Bitrix.
    return True


def read_claim_chat_ownership(deal_id, manager_id, call, timeout=8):
    """Return True if a different employee owns an accepted, unfinished chat.

    ``call`` has the same signature/result as app.bitrix_call. All calls share
    one deadline; failures or incomplete evidence raise instead of freeing a
    deal. No results are cached, including user identity or an empty queue.

    Bitrix documents ACTIVE_ONLY=Y as accepted by an operator and unfinished:
    https://apidocs.bitrix24.com/api-reference/imopenlines/openlines/chats/index.html
    """
    deal_id = _positive_id(deal_id)
    manager_id = _positive_id(manager_id)
    if isinstance(timeout, bool):
        raise ClaimChatGuardUnavailable("invalid_guard_timeout")
    try:
        budget = float(timeout)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ClaimChatGuardUnavailable("invalid_guard_timeout") from exc
    if not math.isfinite(budget) or budget <= 0:
        raise ClaimChatGuardUnavailable("invalid_guard_timeout")
    started = time.monotonic()
    deadline = started + min(budget, MAX_CLAIM_CHAT_GUARD_SECONDS)
    last_method = None
    elapsed_ms = 0.0

    def bounded_call(method, params):
        nonlocal last_method, elapsed_ms
        last_method = method
        checked_at = time.monotonic()
        elapsed_ms = (checked_at - started) * 1000
        remaining = deadline - checked_at
        if remaining <= 0:
            raise ClaimChatGuardUnavailable("chat_guard_timeout")
        try:
            result = call(method, params, timeout=remaining)
        except Exception as exc:
            # Keep remote error text (which may contain credentials) out of
            # messages presented by the calling claim route.
            elapsed_ms = (time.monotonic() - started) * 1000
            error = ClaimChatGuardUnavailable("chat_guard_read_failed")
            error.upstream_status = getattr(exc, "bitrix_http_status", None)
            error.upstream_code = getattr(exc, "bitrix_error_code", None)
            raise error from exc
        checked_at = time.monotonic()
        elapsed_ms = (checked_at - started) * 1000
        if checked_at >= deadline:
            raise ClaimChatGuardUnavailable("chat_guard_timeout")
        return result

    try:
        chats = bounded_call(
            "imopenlines.crm.chat.get",
            {"CRM_ENTITY_TYPE": "DEAL", "CRM_ENTITY": deal_id, "ACTIVE_ONLY": "Y"},
        )
        for chat_id in _active_chat_ids(chats):
            owner_id = _dialog_owner(
                bounded_call("imopenlines.dialog.get", {"CHAT_ID": chat_id}),
                chat_id,
                deal_id,
            )
            if owner_id == manager_id:
                continue
            if _owner_is_staff(bounded_call("im.user.get", {"ID": owner_id}), owner_id):
                return True
        return False
    except ClaimChatGuardUnavailable as error:
        error.method = last_method
        error.duration_ms = elapsed_ms
        raise
