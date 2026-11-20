"""Final, read-only Bitrix Open Lines check for a confirmed picker greeting."""

import math
import time
from datetime import datetime, timezone

from claim_chat_guard import (
    ClaimChatGuardUnavailable, _active_chat_ids, _dialog_owner, _positive_id,
    _read_session_proof, _session_date, _session_time_hint, _zero_owner_address,
)
from native_acceptance_sync import supported as known_native_source


class GreetingDialogGuardError(RuntimeError):
    """Contains only a stable reason code, never a provider response."""


def _dialog_snapshot(dialog, chat, deal, session, manager):
    owner = _dialog_owner(dialog, chat, deal)
    address = _zero_owner_address(dialog)
    if address[2] != session or not known_native_source(address[0]):
        raise GreetingDialogGuardError("greeting_dialog_changed")
    # Some dialogs have a second current CRM binding in entity_data_1.
    fields = dialog["entity_data_1"].split("|")
    if fields[1].upper() == "DEAL" and fields[2] not in ("", "0") and _positive_id(fields[2]) != deal:
        raise GreetingDialogGuardError("greeting_dialog_changed")
    if dialog.get("text_field_enabled") is not True:
        raise GreetingDialogGuardError("greeting_dialog_changed")
    if owner == "0":
        raise GreetingDialogGuardError("chat_activation_pending")
    if owner != manager:
        raise GreetingDialogGuardError("chat_owned_by_another_manager")
    return address, _positive_id(dialog.get("last_message_id"))


def _check_messages(call, chat, manager, tail, since):
    # im.dialog.messages.get returns a newest window; LAST_ID is exclusive.
    # Reaching a message older than the claim proves coverage back to it. A
    # short or empty page alone does not (permissions, deleted/invalid cursor).
    last = None
    previous_time = None
    for _ in range(3):
        params = {"DIALOG_ID": "chat" + chat, "LIMIT": 50}
        if last is not None:
            params["LAST_ID"] = last
        response = call("im.dialog.messages.get", params)
        messages = response.get("messages") if isinstance(response, dict) else None
        # Bitrix can exceed LIMIT for unread chats. Bound even that response.
        if not isinstance(messages, list) or not 0 < len(messages) <= 150:
            break
        if not all(isinstance(row, dict) for row in messages):
            break
        rows = sorted(((_positive_id(row.get("id")), row) for row in messages), key=lambda pair: int(pair[0]), reverse=True)
        if len({key for key, row in rows}) != len(rows) or (last is None and rows[0][0] != tail):
            break
        if last is not None and int(rows[0][0]) >= int(last):
            break
        users = response.get("users")
        staff = set()
        if isinstance(users, list):
            for user in users:
                if (isinstance(user, dict) and user.get("connector") is False and user.get("extranet") is False
                        and user.get("bot") is False and isinstance(user.get("departments"), list) and user["departments"]):
                    staff.add(_positive_id(user.get("id")))
        for id_, row in rows:
            if _positive_id(row.get("chat_id")) != chat:
                raise GreetingDialogGuardError("greeting_history_unverified")
            when = _session_date(row.get("date"))
            if previous_time is not None and when > previous_time:
                raise GreetingDialogGuardError("greeting_history_unverified")
            previous_time = when
            if when < since:
                continue
            author = row.get("author_id")
            # Only documented system join/leave entries are definitely not a
            # client-facing reply. An unclassified connector echo may have been
            # sent by staff in a separate app and is never counted as quiet.
            if type(author) is int and author == 0:
                flags = row.get("params")
                codes = flags.get("CODE") if isinstance(flags, dict) else None
                if isinstance(codes, list) and codes and all(code in ("CHAT_JOIN", "CHAT_LEAVE") for code in codes):
                    continue
            elif _positive_id(author) in staff | {manager}:
                raise GreetingDialogGuardError("greeting_staff_message_seen")
            raise GreetingDialogGuardError("greeting_activity_unclassified")
        if previous_time < since:
            return
        last = rows[-1][0]
    raise GreetingDialogGuardError("greeting_history_unverified")


def require_quiet_claim_dialog(call, *, deal_id, manager_id, chat_id, session_id, claimed_at, timeout=8):
    """Raise on doubt. The caller independently verifies current CRM owner/marker.

    Only metadata is inspected. Bitrix has no conditional send, and this check
    covers messages visible in Bitrix, not unmirrored external conversations.
    """
    try:
        deal, manager, chat, session = map(_positive_id, (deal_id, manager_id, chat_id, session_id))
        since = _session_date(claimed_at)
        if since > datetime.now(timezone.utc) or type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise GreetingDialogGuardError("greeting_history_unverified")
        # Message timestamps can have only second precision; include the entire
        # claim second so a subsecond reply is never mistaken for an older one.
        since = since.replace(microsecond=0)
        deadline = time.monotonic() + min(timeout, 8)

        def read(method, params):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise GreetingDialogGuardError("greeting_guard_unavailable")
            result = call(method, params, timeout=remaining)
            if time.monotonic() >= deadline:
                raise GreetingDialogGuardError("greeting_guard_unavailable")
            return result

        registry = {"CRM_ENTITY_TYPE": "deal", "CRM_ENTITY": deal}
        active = _active_chat_ids(read("imopenlines.crm.chat.get", {**registry, "ACTIVE_ONLY": "Y"}))
        if (active and active != [chat]) or _positive_id(read("imopenlines.crm.chat.getLastId", registry)) != chat:
            raise GreetingDialogGuardError("greeting_dialog_changed")

        def current_dialog():
            return read("imopenlines.dialog.get", {"CHAT_ID": chat})

        first = current_dialog()
        address, tail = _dialog_snapshot(first, chat, deal, session, manager)
        if not active:
            raise GreetingDialogGuardError("chat_activation_pending")
        _check_messages(read, chat, manager, tail, since)
        params = {"configId": address[1], "source": address[0]}
        hint = _session_time_hint(first)
        if hint is not None:
            proof = _read_session_proof(read, {**params, **hint}, address, chat, deal, complete=True, allow_missing=True)
        else:
            proof = None
        if proof is None:
            proof = _read_session_proof(read, params, address, chat, deal, complete=False)
        if proof[7] not in ("0", manager):
            raise GreetingDialogGuardError("chat_owned_by_another_manager")
        if proof[7] != manager or proof[8] not in ("answered", "paused"):
            raise GreetingDialogGuardError("chat_activation_pending")
        if _dialog_snapshot(current_dialog(), chat, deal, session, manager) != (address, tail):
            raise GreetingDialogGuardError("greeting_history_unverified")
    except GreetingDialogGuardError:
        raise
    except (ClaimChatGuardUnavailable, TypeError, ValueError, OverflowError):
        raise GreetingDialogGuardError("greeting_history_unverified") from None
    except Exception:
        raise GreetingDialogGuardError("greeting_guard_unavailable") from None
