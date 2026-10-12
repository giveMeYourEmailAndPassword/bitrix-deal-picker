"""Durable primary-contact responsibility for newly confirmed picker claims.

The generation is allocated before the deal write, not during delayed audit
recovery. Contact writes are serialized and never blindly retried after dispatch.
"""

import json
import re
import secrets
from datetime import timedelta
from contextlib import nullcontext


SCHEMA = """
CREATE TABLE IF NOT EXISTS contact_owner_jobs (
    generation INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_key TEXT NOT NULL,
    claim_marker TEXT NOT NULL,
    deal_id TEXT NOT NULL,
    contact_id TEXT NOT NULL,
    manager_id TEXT NOT NULL,
    previous_owner TEXT NOT NULL,
    snapshot_started_at TEXT NOT NULL,
    confirmed INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'reserved',
    dispatched INTEGER NOT NULL DEFAULT 0,
    dispatch_owner TEXT NOT NULL DEFAULT '',
    acknowledged INTEGER NOT NULL DEFAULT 0,
    lease_token TEXT NOT NULL DEFAULT '',
    lease_expires_at TEXT,
    next_attempt_at TEXT NOT NULL,
    error_code TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(operation_key, claim_marker)
);
CREATE INDEX IF NOT EXISTS idx_contact_owner_generation
    ON contact_owner_jobs(contact_id, generation);
CREATE UNIQUE INDEX IF NOT EXISTS idx_contact_owner_lease
    ON contact_owner_jobs(contact_id) WHERE status='checking';
"""


def entity_id(value, *, allow_zero=False):
    text = str(value if value is not None else "")
    return text if re.fullmatch(r"0|[1-9]\d{0,14}" if allow_zero else r"[1-9]\d{0,14}", text) else ""


def reserve(connection, row, now):
    request = json.loads(row["request_json"] or "{}")
    context = request.get("contactOwnerContext") or {}
    contact = entity_id(context.get("contactId"))
    previous = entity_id(context.get("previousOwner"), allow_zero=True)
    marker = request.get("claimMarker")
    if not contact or not previous or not marker:
        return
    connection.execute("""
        INSERT OR IGNORE INTO contact_owner_jobs(
            operation_key,claim_marker,deal_id,contact_id,manager_id,previous_owner,snapshot_started_at,
            next_attempt_at,created_at,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?)
    """, (row["operation_key"], marker, row["deal_id"], contact,
          row["manager_id"], previous, context.get("snapshotStartedAt") or now, now, now, now))


def activate(connection, operation_key, marker, now):
    # Only attempts reserved by this release are eligible. No historical backfill.
    connection.execute("""UPDATE contact_owner_jobs SET confirmed=1,
        status='pending',updated_at=? WHERE operation_key=? AND claim_marker=?
        AND status='reserved'""", (now, operation_key, marker))


def cancel_unapplied(connection, operation_key, marker, now):
    connection.execute("""UPDATE contact_owner_jobs SET status='cancelled',updated_at=?
        WHERE operation_key=? AND claim_marker=? AND status='reserved'""",
        (now, operation_key, marker))


class ContactOwnerSync:
    def __init__(self, store, call, marker_field, *, locks=None):
        self.store, self.call, self.marker_field = store, call, marker_field
        self.locks = locks

    def reserve_native(self, operation_key, deal_id, manager_id, contact_id,
                       previous_owner, expected_claim_marker, *, snapshot_started_at=None,
                       observed_at=None):
        """Reserve once BEFORE a native deal write; replay keeps its generation."""
        if (not str(operation_key).startswith("native:")
                or not all(entity_id(value) for value in (deal_id, manager_id, contact_id))
                or not entity_id(previous_owner, allow_zero=True)):
            raise ValueError("invalid_native_contact_intent")
        now = self.store._now_iso()
        snapshot_started_at = self.store._outbox_time(self.store._outbox_datetime(snapshot_started_at or now))
        observed_at = self.store._outbox_time(self.store._outbox_datetime(observed_at or snapshot_started_at))
        values = (str(operation_key), str(expected_claim_marker or ""), str(deal_id),
                  str(contact_id), str(manager_id), str(previous_owner))
        with self.store._transaction(immediate=True) as db:
            existing = db.execute("SELECT generation FROM contact_owner_jobs WHERE operation_key=? AND claim_marker=?", values[:2]).fetchone()
            if not existing and db.execute("""SELECT 1 FROM contact_owner_jobs
                    WHERE contact_id=? AND created_at>? AND status!='cancelled' LIMIT 1""",
                    (str(contact_id), observed_at)).fetchone():
                raise ValueError("newer_contact_claim")
            db.execute("""INSERT OR IGNORE INTO contact_owner_jobs(
                operation_key,claim_marker,deal_id,contact_id,manager_id,previous_owner,snapshot_started_at,
                next_attempt_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)""", (*values, snapshot_started_at, now, now, now))
            row = db.execute("SELECT * FROM contact_owner_jobs WHERE operation_key=? AND claim_marker=?",
                             values[:2]).fetchone()
            if tuple(row[name] for name in ("operation_key", "claim_marker", "deal_id", "contact_id", "manager_id", "previous_owner")) != values:
                raise ValueError("native_contact_intent_conflict")
            return row["generation"]

    def confirm_native(self, generation):
        with self.store._transaction(immediate=True) as db:
            db.execute("""UPDATE contact_owner_jobs SET confirmed=1,status='pending',updated_at=?
                WHERE generation=? AND operation_key LIKE 'native:%' AND status='reserved'""",
                (self.store._now_iso(), generation))

    def cancel_native(self, generation):
        with self.store._transaction(immediate=True) as db:
            db.execute("""UPDATE contact_owner_jobs SET status='cancelled',updated_at=?
                WHERE generation=? AND operation_key LIKE 'native:%' AND status='reserved'""",
                (self.store._now_iso(), generation))

    def cancel_native_operation(self, operation_key):
        """Release a pre-dispatch native reservation after its caller crashed.

        The exact operation may have been reserved before the native journal
        saved its generation. Confirmed, dispatched and canonical jobs are never
        changed by this cleanup.
        """
        if not str(operation_key).startswith("native:"):
            raise ValueError("invalid_native_contact_operation")
        with self.store._transaction(immediate=True) as db:
            db.execute("""UPDATE contact_owner_jobs SET status='cancelled',updated_at=?
                WHERE operation_key=? AND status='reserved' AND confirmed=0 AND dispatched=0""",
                (self.store._now_iso(), str(operation_key)))

    def conflict_native(self, generation):
        """Retain a newer terminal fence after a native write may have applied."""
        with self.store._transaction(immediate=True) as db:
            db.execute("""UPDATE contact_owner_jobs SET status='conflict',confirmed=1,
                error_code='native_acceptance_changed',updated_at=?
                WHERE generation=? AND operation_key LIKE 'native:%' AND status='reserved'""",
                (self.store._now_iso(), generation))

    def status(self, operation_key):
        with self.store._connect() as db:
            row = db.execute("SELECT * FROM contact_owner_jobs WHERE operation_key=? ORDER BY generation DESC LIMIT 1",
                             (operation_key,)).fetchone()
        if not row:
            return None
        state = row["status"]
        return {"status": state if state in {"ready", "conflict", "cancelled"} else "pending",
                **({"code": row["error_code"]} if row["error_code"] else {})}

    def lease(self, operation_key=None):
        now = self.store._outbox_time(self.store._outbox_datetime(None))
        expiry = self.store._outbox_time(self.store._outbox_datetime(None) + timedelta(seconds=120))
        token = secrets.token_hex(16)
        with self.store._transaction(immediate=True) as db:
            db.execute("UPDATE contact_owner_jobs SET status='pending',lease_token='' WHERE status='checking' AND lease_expires_at<=?", (now,))
            row = db.execute("""SELECT j.* FROM contact_owner_jobs j
                WHERE j.confirmed=1 AND j.status='pending' AND j.next_attempt_at<=?
                AND (? IS NULL OR j.operation_key=?)
                AND NOT EXISTS(SELECT 1 FROM contact_owner_jobs busy
                    WHERE busy.contact_id=j.contact_id AND busy.generation!=j.generation
                    AND (busy.status='checking' OR (busy.status='pending' AND busy.dispatched=1)))
                ORDER BY j.next_attempt_at,j.generation LIMIT 1""", (now, operation_key, operation_key)).fetchone()
            if not row:
                return None
            db.execute("UPDATE contact_owner_jobs SET status='checking',lease_token=?,lease_expires_at=? WHERE generation=?",
                       (token, expiry, row["generation"]))
            return {**dict(row), "lease_token": token}

    def current(self, job, *, dispatch=False, dispatch_owner=""):
        now = self.store._outbox_time(self.store._outbox_datetime(None))
        with self.store._transaction(immediate=True) as db:
            row = db.execute("""SELECT * FROM contact_owner_jobs WHERE generation=?
                AND status='checking' AND lease_token=? AND lease_expires_at>?""",
                (job["generation"], job["lease_token"], now)).fetchone()
            if not row:
                return "lost"
            newer = db.execute("""SELECT confirmed FROM contact_owner_jobs WHERE contact_id=?
                AND generation>? AND status!='cancelled' ORDER BY generation DESC LIMIT 1""",
                (job["contact_id"], job["generation"])).fetchone()
            if newer:
                return "superseded" if newer["confirmed"] else "waiting"
            if dispatch:
                if not entity_id(dispatch_owner, allow_zero=True):
                    raise ValueError("missing_dispatch_owner")
                db.execute("UPDATE contact_owner_jobs SET dispatched=1,dispatch_owner=?,updated_at=? WHERE generation=?",
                           (dispatch_owner, now, job["generation"]))
            return "current"

    def expected_owner(self, job, owner):
        if owner == job["previous_owner"]:
            return True
        # An older serialized write may have been in flight when this newer
        # claim captured the owner. Its verified result is a known transition.
        with self.store._connect() as db:
            return bool(db.execute("""SELECT 1 FROM contact_owner_jobs
                WHERE contact_id=? AND generation<? AND manager_id=?
                AND previous_owner=? AND dispatched=1 AND status='ready'
                AND updated_at>=? LIMIT 1""",
                (job["contact_id"], job["generation"], owner, job["previous_owner"], job["snapshot_started_at"])).fetchone())

    def check_generation(self, job, *, dispatch=False, dispatch_owner=""):
        current = self.current(job, dispatch=dispatch, dispatch_owner=dispatch_owner)
        if current == "current":
            return True
        if current == "waiting":
            self.finish(job, "pending", "newer_contact_claim_pending")
        elif current != "lost":
            self.finish(job, "conflict", "newer_contact_claim")
        return False

    def finish(self, job, status, code=""):
        now_dt = self.store._outbox_datetime(None)
        now = self.store._outbox_time(now_dt)
        retry = self.store._outbox_time(now_dt + timedelta(seconds=60))
        with self.store._transaction(immediate=True) as db:
            db.execute("""UPDATE contact_owner_jobs SET status=?,error_code=?,
                lease_token='',lease_expires_at=NULL,next_attempt_at=?,updated_at=?
                WHERE generation=? AND status='checking' AND lease_token=?""",
                (status, code, retry, now, job["generation"], job["lease_token"]))

    def acknowledge(self, job):
        with self.store._transaction(immediate=True) as db:
            db.execute("""UPDATE contact_owner_jobs SET acknowledged=1 WHERE generation=?
                AND status='checking' AND lease_token=?""", (job["generation"], job["lease_token"]))

    def read_deal(self, job):
        deal = self.call("crm.deal.get", {"id": job["deal_id"]}, timeout=8) or {}
        return (entity_id(deal.get("ID")) == job["deal_id"]
                and entity_id(deal.get("ASSIGNED_BY_ID")) == job["manager_id"]
                and entity_id(deal.get("CONTACT_ID")) == job["contact_id"]
                and str(deal.get(self.marker_field) or "") == job["claim_marker"])

    def read_contact_owner(self, job):
        contact = self.call("crm.contact.get", {"id": job["contact_id"]}, timeout=8) or {}
        owner = entity_id(contact.get("ASSIGNED_BY_ID"), allow_zero=True)
        if entity_id(contact.get("ID")) != job["contact_id"] or not owner:
            raise ValueError("contact_identity_unavailable")
        return owner

    def process(self, operation_key=None):
        job = self.lease(operation_key)
        if not job:
            return False
        lock = self.locks.hold(job["manager_id"], job["deal_id"]) if self.locks else nullcontext()
        with lock:
            return self._process(job)

    def _process(self, job):
        try:
            # A dispatched request may still be in flight. Only read it back;
            # even a newer claim cannot authorize repeating the old write.
            if job["dispatched"]:
                owner = self.read_contact_owner(job)
                if owner == job["manager_id"]:
                    self.finish(job, "ready")
                elif job["acknowledged"] and owner != job["dispatch_owner"]:
                    self.finish(job, "conflict", "contact_owner_changed")
                else:
                    self.finish(job, "pending", "contact_update_uncertain")
                return True
            if not self.check_generation(job):
                return True
            if not self.read_deal(job):
                self.finish(job, "conflict", "deal_owner_or_contact_changed")
                return True
            owner = self.read_contact_owner(job)
            if owner == job["manager_id"]:
                self.finish(job, "ready")
                return True
            if not self.expected_owner(job, owner):
                self.finish(job, "conflict", "contact_owner_changed")
                return True
            # CRM ownership is read last after contact proof, then a durable
            # dispatch fence is committed before sending the single mutation.
            if not self.read_deal(job):
                self.finish(job, "conflict", "claim_changed_before_contact_update")
                return True
            if not self.check_generation(job, dispatch=True, dispatch_owner=owner):
                return True
            dispatch_owner = owner
            rejected = False
            acknowledged = False
            try:
                result = self.call("crm.contact.update", {"id": job["contact_id"],
                          "fields[ASSIGNED_BY_ID]": job["manager_id"]}, timeout=8)
                rejected = result is False
                acknowledged = result is True
                if acknowledged:
                    self.acknowledge(job)
            except Exception as error:
                # Never include credentials, raw responses or customer fields.
                rejected = getattr(error, "bitrix_error_code", None) in {
                    "ACCESS_DENIED", "ERROR_ACCESS_DENIED", "ERROR_METHOD_NOT_FOUND",
                    "INVALID_CREDENTIALS", "NO_AUTH_FOUND", "ERROR_NOT_FOUND",
                }
            if rejected:
                self.finish(job, "conflict", "contact_update_rejected")
                return True
            owner = self.read_contact_owner(job)
            if owner == job["manager_id"]:
                self.finish(job, "ready")
            elif acknowledged and owner != dispatch_owner:
                self.finish(job, "conflict", "contact_owner_changed")
            else:
                self.finish(job, "pending", "contact_update_uncertain")
        except Exception:
            self.finish(job, "pending", "contact_sync_unavailable")
        return True
