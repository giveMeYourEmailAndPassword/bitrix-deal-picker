"""Give a queued deal to the employee who answered the client's phone call.

Only a finished, answered inbound call on an explicitly allowed telephony line
that ended after activation authorizes a write. A missed or outbound call, a
historical record, an ambiguous CRM binding and a deal that already left the
intake stages never do. The deal gets the answering employee and the working
stage in one update; the CRM contact lane follows the deal on its own.
"""
import hashlib
import json
import logging
import re
import threading
from datetime import datetime, timezone

from contact_owner_policy import contact_ids
from contact_owner_sync import entity_id
from native_acceptance_sync import native_deal_unresolved, timestamp

SCHEMA = """
CREATE TABLE IF NOT EXISTS call_acceptance_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS call_acceptance_jobs(
 operation_key TEXT PRIMARY KEY,statistic_id TEXT NOT NULL UNIQUE,call_id TEXT NOT NULL,
 line TEXT NOT NULL,contact_id TEXT NOT NULL,manager_id TEXT NOT NULL,
 ended_at REAL NOT NULL,observed_at REAL NOT NULL,deal_id TEXT NOT NULL DEFAULT '',
 claim_fence TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'pending',
 previous_owner TEXT,expected_modified TEXT,expected_stage TEXT,expected_marker TEXT,
 dispatched INTEGER NOT NULL DEFAULT 0,acknowledged INTEGER NOT NULL DEFAULT 0,
 next_at REAL NOT NULL DEFAULT 0,error_code TEXT NOT NULL DEFAULT '');
CREATE INDEX IF NOT EXISTS call_acceptance_due ON call_acceptance_jobs(status,next_at);
CREATE INDEX IF NOT EXISTS call_acceptance_deal ON call_acceptance_jobs(deal_id,status);
"""
PAGE_SIZE = 50
MAX_PAGES_PER_POLL = 4
MAX_JOBS_PER_RUN = 5
# A call that could not be applied soon after it ended is no longer evidence
# of who works with the client now; it is closed without a write.
MAX_AGE_SECONDS = 900
RETRY_SECONDS = 30
INBOUND = "2"
ANSWERED = "200"
REJECTED_CODES = frozenset({
    "ACCESS_DENIED", "ERROR_ACCESS_DENIED", "ERROR_METHOD_NOT_FOUND",
    "INVALID_CREDENTIALS", "NO_AUTH_FOUND", "ERROR_NOT_FOUND",
})
LOG = logging.getLogger(__name__)


def call_deal_unresolved(store, deal_id):
    """Read-only fence for the canonical picker before a competing deal write."""
    with store._connect() as db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='call_acceptance_jobs'").fetchone():
            return False
        return bool(db.execute("SELECT 1 FROM call_acceptance_jobs WHERE deal_id=? AND dispatched=1 AND status='pending' LIMIT 1", (str(deal_id),)).fetchone())


class Conflict(Exception):
    """The observed call can no longer authorize a write; needs no retry."""


class Skip(Exception):
    """Nothing to assign for this call; an expected, quiet outcome."""


class CallAcceptanceSync:
    def __init__(self, store, call, locks, marker_field, *, lines, queue_stages,
                 target_stage, chat_guard, now=None, after_confirmed=None):
        self.store, self.call, self.locks = store, call, locks
        self.marker_field, self.chat_guard = marker_field, chat_guard
        self.clock = now or (lambda: datetime.now(timezone.utc))
        self.after_confirmed = after_confirmed
        self.running = threading.Lock()
        if not re.fullmatch(r"UF_CRM_[A-Z0-9_]{3,64}", marker_field or ""):
            raise ValueError("call_sync_marker_required")
        if (not isinstance(lines, (list, tuple)) or not lines or len(set(lines)) != len(lines)
                or not all(isinstance(line, str) and re.fullmatch(r"[A-Za-z0-9_+\-]{1,64}", line) for line in lines)):
            raise ValueError("call_sync_lines_required")
        stages = list(queue_stages or ())
        if (not stages or not all(isinstance(stage, str) and stage for stage in stages)
                or not isinstance(target_stage, str) or not target_stage or target_stage in stages):
            raise ValueError("call_sync_stages_required")
        self.lines, self.queue_stages, self.target_stage = frozenset(lines), frozenset(stages), target_stage
        store._ensure_ready()
        with store._transaction(immediate=True) as db:
            for statement in SCHEMA.split(";"):
                if statement.strip():
                    db.execute(statement)
            db.execute("INSERT OR IGNORE INTO call_acceptance_meta VALUES('activated_at',?)",
                       (str(self.time()),))

    def time(self):
        value = self.clock()
        return value.timestamp() if isinstance(value, datetime) else float(value)

    def rpc(self, method, params):
        return self.call(method, params, timeout=8)

    def meta(self, key):
        with self.store._connect() as db:
            row = db.execute("SELECT value FROM call_acceptance_meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def claim_fence(self, deal_id):
        # Same digest as native acceptance: any picker attempt or revision on
        # this deal between observation and dispatch supersedes the call.
        with self.store._connect() as db:
            rows = db.execute("SELECT operation_key,status,manager_id,updated_at,request_json FROM claim_operations WHERE deal_id=? ORDER BY operation_key", (deal_id,)).fetchall()
        return hashlib.sha256(json.dumps([list(row) for row in rows], separators=(",", ":")).encode()).hexdigest()

    @staticmethod
    def rows(result):
        if (not isinstance(result, list) or len(result) > PAGE_SIZE
                or not all(isinstance(row, dict) for row in result)):
            raise ValueError("call_statistic_page_unavailable")
        return result

    @staticmethod
    def statistic_id(row):
        value = entity_id(row.get("ID"))
        if not value:
            raise ValueError("call_statistic_identity_unavailable")
        return value

    def candidate(self, row):
        """Return the call's evidence, or None when it authorizes nothing."""
        statistic_id = self.statistic_id(row)
        if str(row.get("CALL_TYPE")) != INBOUND:
            # The inbound filter is part of the proof, not an optimization.
            raise ValueError("call_statistic_filter_unavailable")
        line, call_id = row.get("PORTAL_NUMBER"), row.get("CALL_ID")
        if line not in self.lines or str(row.get("CALL_FAILED_CODE")) != ANSWERED:
            return None
        manager = entity_id(row.get("PORTAL_USER_ID"))
        contact = entity_id(row.get("CRM_ENTITY_ID")) if row.get("CRM_ENTITY_TYPE") == "CONTACT" else ""
        started = timestamp(row.get("CALL_START_DATE"))
        duration = str(row.get("CALL_DURATION"))
        if (not manager or not contact or started is None or not re.fullmatch(r"\d{1,6}", duration)
                or not isinstance(call_id, str) or not re.fullmatch(r"[\w.\-]{1,128}", call_id)):
            # Answered without a portal employee or without a contact: there is
            # no one to assign or nothing to assign. Never guess either side.
            return None
        ended = started + int(duration)
        if ended < float(self.meta("activated_at")) or ended > self.time() + 60:
            return None
        return {"statistic_id": statistic_id, "call_id": call_id, "line": line,
                "contact_id": contact, "manager_id": manager, "ended_at": ended}

    def poll(self):
        cursor = self.meta("cursor")
        if cursor is None:
            # Baseline: calls already finished before the first enabled run are
            # never replayed. A failed read leaves no cursor and is retried.
            rows = self.rows(self.rpc("voximplant.statistic.get", {"SORT": "ID", "ORDER": "DESC"}))
            top = max((int(self.statistic_id(row)) for row in rows), default=0)
            with self.store._transaction(immediate=True) as db:
                db.execute("INSERT OR IGNORE INTO call_acceptance_meta VALUES('cursor',?)", (str(top),))
            return
        for _ in range(MAX_PAGES_PER_POLL):
            rows = self.rows(self.rpc("voximplant.statistic.get", {
                "FILTER[>ID]": cursor, "FILTER[CALL_TYPE]": INBOUND, "SORT": "ID", "ORDER": "ASC"}))
            last, found = int(cursor), []
            # Validate the whole page before any of it becomes a durable job.
            for row in rows:
                current = int(self.statistic_id(row))
                if current <= last:
                    raise ValueError("call_statistic_order_unavailable")
                last = current
                item = self.candidate(row)
                if item:
                    found.append(item)
            now = self.time()
            with self.store._transaction(immediate=True) as db:
                for item in found:
                    db.execute("""INSERT OR IGNORE INTO call_acceptance_jobs(
                        operation_key,statistic_id,call_id,line,contact_id,manager_id,ended_at,observed_at,next_at)
                        VALUES(?,?,?,?,?,?,?,?,?)""",
                        (f'call:{item["statistic_id"]}', item["statistic_id"], item["call_id"], item["line"],
                         item["contact_id"], item["manager_id"], item["ended_at"], now, now))
                db.execute("UPDATE call_acceptance_meta SET value=? WHERE key='cursor'", (str(last),))
            cursor = str(last)
            if len(rows) < PAGE_SIZE:
                break

    def save(self, job, status, code="", **fields):
        values = {"status": status, "error_code": code, "next_at": self.time() + RETRY_SECONDS, **fields}
        with self.store._transaction(immediate=True) as db:
            db.execute("UPDATE call_acceptance_jobs SET " + ",".join(f"{key}=?" for key in values) + " WHERE operation_key=?", (*values.values(), job["operation_key"]))
        if status == "conflict":
            LOG.warning("call acceptance stopped: %s", code)

    def status(self):
        with self.store._connect() as db:
            return {row["status"]: row["n"] for row in db.execute("SELECT status,count(*) n FROM call_acceptance_jobs GROUP BY status")}

    def job(self, operation_key):
        with self.store._connect() as db:
            row = db.execute("SELECT * FROM call_acceptance_jobs WHERE operation_key=?", (operation_key,)).fetchone()
        return dict(row) if row else None

    def resolve(self, job):
        """Find the single intake deal of the caller; never pick among several."""
        result = self.rpc("crm.item.list", {
            "entityTypeId": 2, "filter[contactIds]": job["contact_id"],
            "filter[stageSemanticId]": "P", "filter[closed]": "N",
            "select[]": ["id", "stageId", "contactIds"], "order[id]": "DESC", "start": 0})
        rows = result.get("items") if isinstance(result, dict) else None
        if not isinstance(rows, list) or len(rows) >= PAGE_SIZE:
            # A full page cannot prove that every open deal was seen.
            raise ValueError("call_deal_page_unavailable")
        queued, working = [], []
        for row in rows:
            deal_id = entity_id(row.get("id")) if isinstance(row, dict) else ""
            stage = row.get("stageId") if isinstance(row, dict) else None
            bound = row.get("contactIds") if isinstance(row, dict) else None
            if (not deal_id or not isinstance(stage, str) or not stage or not isinstance(bound, list)
                    or job["contact_id"] not in [entity_id(value) for value in bound]
                    or deal_id in queued or deal_id in working):
                raise ValueError("call_deal_schema_unavailable")
            (queued if stage in self.queue_stages else working).append(deal_id)
        if working:
            # The client already has a deal in work; its manager stays.
            raise Skip("call_contact_has_working_deal")
        if not queued:
            raise Skip("call_no_queued_deal")
        if len(queued) > 1:
            raise Conflict("call_deal_ambiguous")
        fields = {"deal_id": queued[0], "claim_fence": self.claim_fence(queued[0])}
        with self.store._transaction(immediate=True) as db:
            db.execute("UPDATE call_acceptance_jobs SET deal_id=?,claim_fence=? WHERE operation_key=? AND deal_id=''",
                       (fields["deal_id"], fields["claim_fence"], job["operation_key"]))

    def claimed(self, deal_id):
        with self.store._connect() as db:
            return [row["updated_at"] for row in db.execute(
                "SELECT updated_at FROM claim_operations WHERE deal_id=? AND status='succeeded'", (deal_id,))]

    def current(self, job):
        if self.claim_fence(job["deal_id"]) != job["claim_fence"]:
            raise Conflict("call_acceptance_superseded")
        if self.store.list_unresolved_claim_operations_for_deal(job["deal_id"], limit=1):
            raise Conflict("picker_claim_pending")
        for updated in self.claimed(job["deal_id"]):
            changed = timestamp(updated)
            if changed is None:
                raise ValueError("call_claim_time_unavailable")
            if changed > job["ended_at"]:
                raise Conflict("call_newer_picker_claim")

    def technical_owner(self, deal, owner):
        """True only for an owner nobody chose: a bot or telephony's own default."""
        profile = self.rpc("im.user.get", {"ID": owner})
        if (not isinstance(profile, dict) or entity_id(profile.get("id")) != owner
                or type(profile.get("bot")) is not bool):
            raise ValueError("call_owner_profile_unavailable")
        if profile["bot"]:
            return True
        # Telephony converts an unknown caller's lead into a deal owned by the
        # first queue member before anyone answers. Until an employee or the
        # picker changes it, that owner is a placeholder, not a decision.
        return (deal.get("SOURCE_ID") == "CALL" and bool(entity_id(deal.get("LEAD_ID")))
                and entity_id(deal.get("CREATED_BY_ID")) == owner
                and not self.claimed(entity_id(deal.get("ID"))))

    def identity(self, deal):
        marker = deal.get(self.marker_field) or ""
        owner = entity_id(deal.get("ASSIGNED_BY_ID"))
        if (not isinstance(marker, str) or not owner or not isinstance(deal.get("STAGE_ID"), str)
                or timestamp(deal.get("DATE_MODIFY")) is None):
            raise ValueError("call_deal_unavailable")
        return owner, deal["DATE_MODIFY"], deal["STAGE_ID"], marker

    def read_deal(self, job):
        deal = self.rpc("crm.deal.get", {"id": job["deal_id"]}) or {}
        if not isinstance(deal, dict) or entity_id(deal.get("ID")) != job["deal_id"]:
            raise ValueError("call_deal_unavailable")
        self.identity(deal)
        return deal

    def proof(self, job):
        self.current(job)
        rows = self.rows(self.rpc("voximplant.statistic.get", {"FILTER[ID]": job["statistic_id"]}))
        if len(rows) != 1:
            raise ValueError("call_record_unavailable")
        record = self.candidate(rows[0])
        if not record or any(record[key] != job[key] for key in ("statistic_id", "call_id", "line", "contact_id", "manager_id")):
            raise Conflict("call_record_changed")
        users = self.rpc("user.get", {"ID": job["manager_id"]})
        if not isinstance(users, list) or len(users) != 1 or entity_id(users[0].get("ID")) != job["manager_id"]:
            raise Conflict("call_employee_unavailable")
        departments = users[0].get("UF_DEPARTMENT")
        departments = departments if isinstance(departments, list) else [departments]
        if users[0].get("ACTIVE") not in (True, "Y", "1", 1) or not any(entity_id(v) for v in departments):
            raise Conflict("call_employee_inactive")
        deal = self.read_deal(job)
        owner, _modified, stage, _marker = self.identity(deal)
        if owner == job["manager_id"] and stage == self.target_stage:
            return deal
        if (deal.get("CLOSED") != "N" or deal.get("STAGE_SEMANTIC_ID") != "P"
                or deal.get("IS_ARCHIVED") in (True, "Y", "1", 1) or stage not in self.queue_stages):
            # Someone already took, closed or moved it: nothing left to do.
            raise Skip("call_deal_left_queue")
        if job["contact_id"] not in contact_ids(self.call, deal):
            raise Conflict("call_deal_binding_changed")
        if owner != job["manager_id"] and not self.technical_owner(deal, owner):
            raise Conflict("call_deal_owned_by_employee")
        try:
            occupied = self.chat_guard(job["deal_id"], job["manager_id"], self.call)
        except Exception:
            raise ValueError("call_chat_guard_unavailable") from None
        if occupied is not False:
            # Another employee accepted this client's dialog natively.
            raise Conflict("call_dialog_owned_by_employee")
        self.current(job)
        return deal

    def confirmed(self, job):
        self.save(job, "ready")
        if self.after_confirmed:
            try:
                self.after_confirmed(job["deal_id"])
            except Exception:
                LOG.warning("call acceptance cache refresh unavailable")

    def readback(self, job):
        # This path deliberately never sends another deal update.
        owner, _modified, stage, marker = self.identity(self.read_deal(job))
        if owner == job["manager_id"] and stage == self.target_stage:
            if marker != job["expected_marker"]:
                raise Conflict("call_deal_marker_changed")
            self.confirmed(job)
        elif job["acknowledged"]:
            raise Conflict("call_deal_changed_after_update")
        else:
            # A timed-out request may still arrive. Keep the per-deal fence
            # until the provider state is reconciled; never send it again.
            self.save(job, "pending", "call_deal_update_uncertain")

    def busy(self, job):
        with self.store._connect() as db:
            return bool(db.execute("""SELECT 1 FROM call_acceptance_jobs WHERE deal_id=? AND operation_key!=?
                AND dispatched=1 AND status='pending' LIMIT 1""", (job["deal_id"], job["operation_key"])).fetchone())

    def process(self):
        with self.store._connect() as db:
            row = db.execute("""SELECT * FROM call_acceptance_jobs WHERE status='pending' AND next_at<=?
                ORDER BY ended_at,CAST(statistic_id AS INTEGER) LIMIT 1""", (self.time(),)).fetchone()
        if not row:
            return False
        job = dict(row)
        try:
            if not job["dispatched"] and self.time() - job["ended_at"] > MAX_AGE_SECONDS:
                raise Conflict("call_acceptance_expired")
            if not job["deal_id"]:
                self.resolve(job)
                job = self.job(job["operation_key"])
        except Skip as exc:
            self.save(job, "skipped", str(exc))
            return True
        except Conflict as exc:
            self.save(job, "conflict", str(exc))
            return True
        except Exception:
            self.save(job, "pending", "call_sync_unavailable")
            return True
        with self.locks.hold(job["manager_id"], job["deal_id"]):
            # A second caller may have waited while the first completed this job.
            job = self.job(job["operation_key"])
            if not job or job["status"] != "pending" or job["next_at"] > self.time():
                return False
            if not job["dispatched"] and (native_deal_unresolved(self.store, job["deal_id"]) or self.busy(job)):
                self.save(job, "pending", "call_previous_update_uncertain")
                return False
            try:
                if job["dispatched"]:
                    self.readback(job)
                    return True
                deal = self.proof(job)
                owner, modified, stage, marker = self.identity(deal)
                if job["previous_owner"] is not None and (owner, modified, stage, marker) != (
                        job["previous_owner"], job["expected_modified"], job["expected_stage"], job["expected_marker"]):
                    raise Conflict("call_deal_changed_before_dispatch")
                if job["previous_owner"] is None:
                    fields = {"previous_owner": owner, "expected_modified": modified,
                              "expected_stage": stage, "expected_marker": marker}
                    self.save(job, "pending", **fields)
                    job.update(fields)
                if owner == job["manager_id"] and stage == self.target_stage:
                    # Already done by hand: nothing is sent, nothing to fence.
                    self.confirmed(job)
                    return True
                if self.identity(self.read_deal(job)) != (owner, modified, stage, marker):
                    raise Conflict("call_deal_changed_before_dispatch")
                self.current(job)
                # Persist before transport. A timeout/crash is read-only recovery.
                self.save(job, "pending", dispatched=1)
                job["dispatched"] = 1
                fields = {"id": job["deal_id"], "fields[STAGE_ID]": self.target_stage}
                if owner != job["manager_id"]:
                    fields["fields[ASSIGNED_BY_ID]"] = job["manager_id"]
                rejected = False
                try:
                    result = self.rpc("crm.deal.update", fields)
                    rejected = result is False
                    if result is True:
                        self.save(job, "pending", acknowledged=1)
                        job["acknowledged"] = 1
                except Exception as error:
                    rejected = getattr(error, "bitrix_error_code", None) in REJECTED_CODES
                if rejected:
                    self.save(job, "pending", dispatched=0)
                    job["dispatched"] = 0
                    raise Conflict("call_deal_update_rejected")
                self.readback(job)
            except Skip as exc:
                self.save(job, "skipped", str(exc))
            except Conflict as exc:
                self.save(job, "conflict", str(exc))
            except Exception:
                self.save(job, "pending", "call_sync_unavailable")
        return True

    def drain(self):
        for _ in range(MAX_JOBS_PER_RUN):
            if not self.process():
                break

    def run(self):
        if not self.running.acquire(blocking=False):
            return
        try:
            # Independent failures do not stop a previously persisted job.
            for action in (self.poll, self.drain):
                try:
                    action()
                except Exception:
                    LOG.warning("call acceptance %s unavailable", action.__name__)
        finally:
            self.running.release()
