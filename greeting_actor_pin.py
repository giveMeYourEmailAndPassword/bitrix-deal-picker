"""Reserve one personal pin per canonical greeting when Baza confirms no mapping."""

import re
import sqlite3


class GreetingActorPinError(RuntimeError):
    """The message is a stable machine code and contains no request details."""


def _identity(job, chat_id):
    operation = job.get("operationKey")
    native_ids = [job.get("dealId"), job.get("managerId"), chat_id, job.get("sessionId")]
    if (not isinstance(operation, str) or not operation or len(operation) > 512
            or not all(re.fullmatch(r"[1-9][0-9]{0,19}", str(value)) for value in native_ids)):
        raise GreetingActorPinError("greeting_actor_pin_unavailable")
    return (operation, *map(str, native_ids))


def _pin_state(store, worker, identity, *, acknowledge=False):
    operation, deal, manager, chat, session = identity
    store._ensure_ready()
    with store._transaction(immediate=True) as db:
        now = store._now_iso()
        job = db.execute("""
            SELECT g.status, g.lease_token, g.lease_expires_at, g.chat_id
            FROM greeting_outbox g
            JOIN claim_operations o ON o.operation_key=g.operation_key
            JOIN claim_events e ON e.id=o.claim_event_id AND e.operation_key=o.operation_key
            WHERE g.operation_key=? AND g.deal_id=? AND g.manager_id=? AND g.session_id=?
              AND o.status='succeeded' AND o.deal_id=g.deal_id AND o.manager_id=g.manager_id
              AND e.deal_id=g.deal_id AND e.manager_id=g.manager_id
              AND NOT EXISTS (SELECT 1 FROM claim_events later WHERE later.deal_id=e.deal_id AND later.id>e.id)
        """, (operation, deal, manager, session)).fetchone()
        if (not job or job["chat_id"] not in ("", chat)
                or not store._owns_active_greeting_check(job, worker, now)):
            raise GreetingActorPinError("greeting_actor_pin_unavailable")
        if db.execute("""
            SELECT 1 FROM greeting_outbox WHERE session_id=? AND
              (status IN ('sent','dispatching','uncertain') OR NULLIF(dispatching_at,'') IS NOT NULL) LIMIT 1
        """, (session,)).fetchone():
            raise GreetingActorPinError("greeting_session_already_contacted")

        prior = db.execute("SELECT * FROM greeting_actor_pins WHERE operation_key=?", (operation,)).fetchone()
        if prior:
            if tuple(prior[key] for key in ("deal_id", "manager_id", "chat_id", "session_id")) != identity[1:]:
                raise GreetingActorPinError("greeting_actor_pin_unavailable")
            if prior["acknowledged_at"]:
                return "confirmed"
            if acknowledge:
                db.execute("UPDATE greeting_actor_pins SET acknowledged_at=? WHERE operation_key=?", (now, operation))
                return "confirmed"
            # Includes a process that died after reservation and before the RPC.
            raise GreetingActorPinError("greeting_actor_pin_uncertain")
        if acknowledge:
            raise GreetingActorPinError("greeting_actor_pin_uncertain")
        db.execute("""INSERT INTO greeting_actor_pins
            (operation_key, deal_id, manager_id, chat_id, session_id, reserved_at) VALUES (?,?,?,?,?,?)""",
                   (*identity, now))
        return "reserved"


def _require_unmapped(status):
    if status == "alreadyReplied":
        raise GreetingActorPinError("greeting_staff_message_seen")
    if status != "unmapped":
        raise GreetingActorPinError("greeting_pin_receipt_changed")


def ensure_unmapped_actor_pin(store, job, worker_token, actor_auth, chat_id, *, receipt,
                              verify_actor, revalidate, actor_call):
    """Pin with the recipient's verified in-memory OAuth. Never use a service account.

    revalidate() must freshly check current CRM owner, marker, stage, age and the
    exact quiet Open Lines address and return Baza's signed readiness decision.
    verify_actor(auth, manager_id) is app.cached_verified_actor_auth or equivalent.
    The caller may cross the separate greeting dispatch boundary only on True.
    """
    _require_unmapped(receipt)
    identity = _identity(job, chat_id)
    if not isinstance(worker_token, str) or not worker_token or not actor_auth:
        raise GreetingActorPinError("greeting_actor_pin_unavailable")
    _require_unmapped(revalidate())
    verified = verify_actor(actor_auth, identity[2])
    if not isinstance(verified, dict) or not verified.get("access_token") or not verified.get("domain"):
        raise GreetingActorPinError("greeting_actor_pin_unavailable")

    try:
        state = _pin_state(store, worker_token, identity)
    except sqlite3.Error:
        raise GreetingActorPinError("greeting_actor_pin_unavailable") from None
    if state == "reserved":
        # The reservation committed before the only irreversible RPC. A false
        # reply, exception, crash or failed local acknowledgement is never retried.
        try:
            if actor_call(verified, "imopenlines.session.mode.pin", {"CHAT_ID": identity[3], "ACTIVATE": "Y"}) is not True:
                raise GreetingActorPinError("greeting_actor_pin_uncertain")
            _pin_state(store, worker_token, identity, acknowledge=True)
        except Exception:
            raise GreetingActorPinError("greeting_actor_pin_uncertain") from None
        # A transfer, manual reply or mapping change during the pin must stop the
        # greeting, even after a positive pin response. This never repeats the pin.
        _require_unmapped(revalidate())
    return True
