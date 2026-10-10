"""Read-only protection against claiming another operator's active dialog."""

import math
import re
import time
from datetime import datetime, timedelta


MAX_ACTIVE_CLAIM_CHATS = 10
MAX_CLAIM_CHAT_GUARD_SECONDS = 8.0
MAX_SESSION_PROOF_PAGES = 4
SESSION_PROOF_PAGE_SIZE = 200
GUARD_REASONS = frozenset({
    "invalid_identity", "invalid_active_chat_list", "too_many_active_chats",
    "invalid_active_chat_row", "invalid_dialog_deal_binding",
    "dialog_deal_binding_mismatch", "invalid_dialog", "dialog_id_mismatch",
    "dialog_not_openline", "owner_identity_mismatch", "invalid_owner_identity",
    "invalid_owner_departments", "owner_not_internal", "owner_staff_unconfirmed",
    "invalid_guard_timeout", "chat_guard_timeout", "chat_guard_read_failed",
    "invalid_zero_owner_session", "zero_owner_session_unavailable",
    "zero_owner_session_changed",
})
GUARD_METHODS = frozenset({
    "imopenlines.crm.chat.get", "imopenlines.dialog.get", "im.user.get",
    "imopenlines.v2.Session.list",
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
    if _explicit_zero(dialog.get("owner")):
        # This is an incomplete identity, never proof that the chat is free.
        return "0"
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


def _explicit_zero(value):
    return (type(value) is int and value == 0) or (type(value) is str and value == "0")


def _zero_owner_address(dialog):
    code, metadata = dialog.get("entity_id"), dialog.get("entity_data_1")
    if not isinstance(code, str) or not isinstance(metadata, str):
        raise ClaimChatGuardUnavailable("invalid_zero_owner_session")
    parts, fields = code.split("|"), metadata.split("|")
    if (len(parts) != 4 or not all(parts) or len(code) > 1000 or
            not re.fullmatch(r"[a-z0-9_-]{1,200}", parts[0]) or len(fields) < 6):
        raise ClaimChatGuardUnavailable("invalid_zero_owner_session")
    # The current SESSION_ID is documented as the sixth entity_data_1 field.
    # No undocumented timestamp/flags are used as session authority.
    return parts[0], _positive_id(parts[1]), _positive_id(fields[5]), code


def _session_date(value):
    if not isinstance(value, str) or len(value) > 60:
        raise ClaimChatGuardUnavailable("invalid_zero_owner_session")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.utcoffset() is None:
            raise ValueError("missing timezone")
        return parsed
    except (ValueError, OverflowError) as error:
        raise ClaimChatGuardUnavailable("invalid_zero_owner_session") from error


def _session_proof(row, address, chat_id, deal_id):
    source, config_id, session_id, _ = address
    required = {"id", "chatId", "configId", "source", "crmEntityType", "crmEntityId",
                "dateCreate", "operatorId", "status", "dateOperatorAnswer", "dateClose"}
    if not isinstance(row, dict) or not required.issubset(row):
        raise ClaimChatGuardUnavailable("invalid_zero_owner_session")
    if (_positive_id(row["id"]) != session_id or _positive_id(row["chatId"]) != chat_id or
            _positive_id(row["configId"]) != config_id or row["source"] != source or
            not isinstance(row["crmEntityType"], str) or row["crmEntityType"].lower() != "deal" or
            _positive_id(row["crmEntityId"]) != deal_id or
            row["status"] not in ("new", "answered", "paused") or row["dateClose"] is not None):
        raise ClaimChatGuardUnavailable("invalid_zero_owner_session")
    operator = row["operatorId"]
    operator_id = "0" if operator is None or _explicit_zero(operator) else _positive_id(operator)
    answer = None if row["dateOperatorAnswer"] is None else _session_date(row["dateOperatorAnswer"])
    if operator_id == "0" and (row["status"] != "new" or answer is not None):
        raise ClaimChatGuardUnavailable("invalid_zero_owner_session")
    # Include every field that decides identity, current ownership and state.
    # A later operator change cannot reuse the earlier bot/employee lookup.
    return (session_id, chat_id, config_id, source, "deal", deal_id,
            _session_date(row["dateCreate"]), operator_id, row["status"], answer, None)


def _read_session_proof(call, params, address, chat_id, deal_id, *, complete):
    matches = []
    for page in range(MAX_SESSION_PROOF_PAGES):
        result = call("imopenlines.v2.Session.list", {
            **params, "order": "dateCreate", "orderDirection": "desc",
            "offset": page * SESSION_PROOF_PAGE_SIZE, "limit": SESSION_PROOF_PAGE_SIZE,
        })
        if (not isinstance(result, dict) or not isinstance(result.get("sessions"), list) or
                len(result["sessions"]) > SESSION_PROOF_PAGE_SIZE or type(result.get("hasNextPage")) is not bool or
                (result["hasNextPage"] and not result["sessions"])):
            raise ClaimChatGuardUnavailable("zero_owner_session_unavailable")
        for row in result["sessions"]:
            if not isinstance(row, dict):
                raise ClaimChatGuardUnavailable("zero_owner_session_unavailable")
            if _positive_id(row.get("id")) == address[2]:
                matches.append(_session_proof(row, address, chat_id, deal_id))
        if len(matches) > 1:
            raise ClaimChatGuardUnavailable("zero_owner_session_unavailable")
        if not result["hasNextPage"] or (matches and not complete):
            if len(matches) != 1:
                raise ClaimChatGuardUnavailable("zero_owner_session_unavailable")
            return matches[0]
    raise ClaimChatGuardUnavailable("zero_owner_session_unavailable")


def _zero_owner_occupied(dialog, chat_id, deal_id, manager_id, call):
    address = _zero_owner_address(dialog)
    params = {"configId": address[1], "source": address[0]}
    # A bounded broad scan only locates this exact session and its date. It
    # never proves availability, including when no matching row was returned.
    observed = _read_session_proof(call, params, address, chat_id, deal_id, complete=False)
    created, operator_id = observed[6], observed[7]
    staff = _owner_is_staff(call("im.user.get", {"ID": operator_id}), operator_id) if operator_id != "0" else False
    try:
        narrow = {**params, "dateCreateFrom": (created - timedelta(seconds=1)).isoformat(),
                  "dateCreateTo": (created + timedelta(seconds=1)).isoformat()}
    except OverflowError as error:
        raise ClaimChatGuardUnavailable("invalid_zero_owner_session") from error
    # Re-read the entire narrow window after fresh user identity. The session
    # and final native dialog must still describe the exact same ownership.
    current = _read_session_proof(call, narrow, address, chat_id, deal_id, complete=True)
    if current != observed:
        raise ClaimChatGuardUnavailable("zero_owner_session_changed")
    final = call("imopenlines.dialog.get", {"CHAT_ID": chat_id})
    if _dialog_owner(final, chat_id, deal_id) != "0" or _zero_owner_address(final) != address:
        raise ClaimChatGuardUnavailable("zero_owner_session_changed")
    return staff and operator_id != manager_id


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
            dialog = bounded_call("imopenlines.dialog.get", {"CHAT_ID": chat_id})
            owner_id = _dialog_owner(dialog, chat_id, deal_id)
            if owner_id == "0":
                if _zero_owner_occupied(dialog, chat_id, deal_id, manager_id, bounded_call):
                    return True
                continue
            if owner_id == manager_id:
                continue
            if _owner_is_staff(bounded_call("im.user.get", {"ID": owner_id}), owner_id):
                return True
        return False
    except ClaimChatGuardUnavailable as error:
        error.method = last_method
        error.duration_ms = elapsed_ms
        raise
