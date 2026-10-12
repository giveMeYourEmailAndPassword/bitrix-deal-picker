"""Observe live Open Lines acceptance without replaying historical ownership.

Only a new session or an observed queue -> accepted transition authorizes a
write. An offered queue operator, an initial historical snapshot, and a silent
owner change in an already accepted session are not evidence of acceptance.
"""
import hashlib
import json
import logging
import re
import threading
from datetime import datetime, timezone

from contact_owner_sync import entity_id

SCHEMA = """
CREATE TABLE IF NOT EXISTS native_acceptance_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS native_acceptance_sessions(
 session_id TEXT PRIMARY KEY,chat_id TEXT NOT NULL,line_id TEXT NOT NULL,
 source TEXT NOT NULL,date_create TEXT NOT NULL,status TEXT NOT NULL,
 operator_id TEXT NOT NULL,armed INTEGER NOT NULL DEFAULT 0,
 revision INTEGER NOT NULL DEFAULT 0,next_check REAL NOT NULL,last_seen REAL NOT NULL);
CREATE TABLE IF NOT EXISTS native_acceptance_jobs(
 operation_key TEXT PRIMARY KEY,session_id TEXT NOT NULL,revision INTEGER NOT NULL,
 chat_id TEXT NOT NULL,line_id TEXT NOT NULL,source TEXT NOT NULL,date_create TEXT NOT NULL,
 deal_id TEXT NOT NULL,manager_id TEXT NOT NULL,observed_at REAL NOT NULL,
 claim_fence TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',
 previous_owner TEXT,expected_modified TEXT,expected_marker TEXT,contact_id TEXT,
 contact_generation INTEGER,dispatched INTEGER NOT NULL DEFAULT 0,acknowledged INTEGER NOT NULL DEFAULT 0,
 next_at REAL NOT NULL DEFAULT 0,error_code TEXT NOT NULL DEFAULT '',
 UNIQUE(session_id,revision));
CREATE INDEX IF NOT EXISTS native_acceptance_due ON native_acceptance_jobs(status,next_at);
"""
ACTIVE = {"new", "answered", "paused"}
ACCEPTED = {"answered", "paused"}
LOG = logging.getLogger(__name__)


def native_deal_unresolved(store, deal_id):
    """Read-only fence for the canonical picker before a competing deal write."""
    with store._connect() as db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='native_acceptance_jobs'").fetchone():
            return False
        return bool(db.execute("SELECT 1 FROM native_acceptance_jobs WHERE deal_id=? AND dispatched=1 AND status='pending' LIMIT 1", (str(deal_id),)).fetchone())


def timestamp(value):
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return result.timestamp() if result.tzinfo else None
    except (ValueError, OverflowError):
        return None


def iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def supported(value):
    return isinstance(value, str) and (value == "fbinstagramdirect" or
        re.fullmatch(r"wz_whatsapp_[a-f0-9]{16,64}", value) is not None)


class Conflict(Exception):
    pass


class NativeAcceptanceSync:
    def __init__(self, store, call, locks, contacts, marker_field, *, now=None,
                 after_confirmed=None):
        self.store, self.call, self.locks = store, call, locks
        self.contacts, self.marker_field = contacts, marker_field
        self.clock = now or (lambda: datetime.now(timezone.utc))
        self.after_confirmed = after_confirmed
        self.running = threading.Lock()
        if not re.fullmatch(r"UF_CRM_[A-Z0-9_]{3,64}", marker_field or ""):
            raise ValueError("native_sync_marker_required")
        store._ensure_ready()
        with store._transaction(immediate=True) as db:
            for statement in SCHEMA.split(";"):
                if statement.strip():
                    db.execute(statement)
            if "acknowledged" not in {row[1] for row in db.execute("PRAGMA table_info(native_acceptance_jobs)")}:
                db.execute("ALTER TABLE native_acceptance_jobs ADD COLUMN acknowledged INTEGER NOT NULL DEFAULT 0")
            db.execute("INSERT OR IGNORE INTO native_acceptance_meta VALUES('activated_at',?)",
                       (str(self.time()),))
            db.execute("INSERT OR IGNORE INTO native_acceptance_meta VALUES('cursor',?)",
                       (json.dumps({"lane": 0, "offset": 0, "to": self.time()}),))
            db.execute("INSERT OR IGNORE INTO native_acceptance_meta VALUES('old_queue_cursor',?)",
                       (json.dumps({"offset": 0, "to": self.time()}),))
            db.execute("INSERT OR IGNORE INTO native_acceptance_meta VALUES('ticks','0')")

    def time(self):
        value = self.clock()
        return value.timestamp() if isinstance(value, datetime) else float(value)

    def rpc(self, method, params):
        return self.call(method, params, timeout=8)

    def meta(self, key):
        with self.store._connect() as db:
            return db.execute("SELECT value FROM native_acceptance_meta WHERE key=?", (key,)).fetchone()[0]

    def claim_fence(self, deal_id):
        # Local claim writes share CLAIM_LOCKS; include unfinished attempts and
        # revisions so a newer picker action cannot be overwritten after delay.
        with self.store._connect() as db:
            rows = db.execute("SELECT operation_key,status,manager_id,updated_at,request_json FROM claim_operations WHERE deal_id=? ORDER BY operation_key", (deal_id,)).fetchall()
        return hashlib.sha256(json.dumps([list(row) for row in rows], separators=(",", ":")).encode()).hexdigest()

    def candidate(self, value):
        if not isinstance(value, dict) or not supported(value.get("source")):
            return None
        created = timestamp(value.get("dateCreate"))
        fields = {name: entity_id(value.get(key)) for name, key in (
            ("session_id", "id"), ("chat_id", "chatId"), ("line_id", "configId"))}
        if not all(fields.values()) or created is None or created > self.time() + 60:
            raise ValueError("native_session_identity_unavailable")
        status = value.get("status")
        if status not in ACTIVE | {"closed", "spam"}:
            raise ValueError("native_session_status_unavailable")
        manager = entity_id(value.get("operatorId"))
        if status in ACCEPTED and not manager:
            raise ValueError("native_operator_unavailable")
        return {**fields, "source": value["source"], "date_create": value["dateCreate"],
                "status": status, "manager_id": manager,
                "deal_id": entity_id(value.get("crmEntityId")) if value.get("crmEntityType") == "deal" else ""}

    def observe(self, value):
        item = self.candidate(value)
        if not item:
            return
        now = self.time()
        fence = self.claim_fence(item["deal_id"]) if item["deal_id"] else ""
        with self.store._transaction(immediate=True) as db:
            old = db.execute("SELECT * FROM native_acceptance_sessions WHERE session_id=?", (item["session_id"],)).fetchone()
            if old and any(old[key] != item[key] for key in ("chat_id", "line_id", "source", "date_create")):
                raise ValueError("native_session_identity_changed")
            activated = float(db.execute("SELECT value FROM native_acceptance_meta WHERE key='activated_at'").fetchone()[0])
            eligible = bool(old and old["armed"]) or (not old and timestamp(item["date_create"]) >= activated)
            accepted = item["status"] in ACCEPTED
            created = timestamp(item["date_create"])
            answered = timestamp(value.get("dateOperatorAnswer"))
            if old and old["armed"]:
                event_bound = old["last_seen"]
            else:
                # The first observation can arrive after a newer picker claim.
                # Use a provider acceptance time (or conservative creation
                # lower bound), never first-poll time as the event's authority.
                event_bound = answered if answered is not None and created <= answered <= now else created
            revision = (old["revision"] if old else 0) + (1 if eligible and accepted and item["deal_id"] else 0)
            # A first historical accepted snapshot is a baseline, never a job.
            # Preserve an observed queue transition when CRM binding appears late.
            armed = item["status"] == "new" or (eligible and accepted and not item["deal_id"])
            db.execute("""INSERT INTO native_acceptance_sessions VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(session_id) DO UPDATE SET status=excluded.status,
                operator_id=excluded.operator_id,armed=excluded.armed,revision=excluded.revision,
                next_check=excluded.next_check,last_seen=excluded.last_seen""",
                (item["session_id"], item["chat_id"], item["line_id"], item["source"], item["date_create"],
                 item["status"], item["manager_id"], int(armed), revision,
                 now + (30 if item["status"] == "new" else 300),
                 event_bound if armed and accepted else now))
            if eligible and accepted and item["deal_id"]:
                key = f'native:{item["session_id"]}:{revision}'
                db.execute("""INSERT OR IGNORE INTO native_acceptance_jobs(
                    operation_key,session_id,revision,chat_id,line_id,source,date_create,
                    deal_id,manager_id,observed_at,claim_fence) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (key, item["session_id"], revision, item["chat_id"], item["line_id"], item["source"],
                     item["date_create"], item["deal_id"], item["manager_id"], event_bound, fence))

    @staticmethod
    def page(result):
        if (not isinstance(result, dict) or not isinstance(result.get("sessions"), list)
                or type(result.get("hasNextPage")) is not bool or len(result["sessions"]) > 200
                or (result["hasNextPage"] and not result["sessions"])):
            raise ValueError("native_session_page_unavailable")
        return result["sessions"], result["hasNextPage"]

    def poll(self):
        # Recent discovery is independent of the potentially large old queue.
        # Three active-only lanes never traverse years of paused dialogs.
        cursor = json.loads(self.meta("cursor"))
        status = ("new", "answered", "paused")[cursor["lane"]]
        rows, more = self.page(self.rpc("imopenlines.v2.Session.list", {
            "status": status, "dateCreateFrom": iso(max(float(self.meta("activated_at")), cursor["to"] - 86400)),
            "dateCreateTo": iso(cursor["to"]), "order": "dateCreate", "orderDirection": "desc",
            "offset": cursor["offset"], "limit": 200}))
        # Validate the entire page before establishing any baseline from it.
        for row in rows:
            item = self.candidate(row)
            if item and item["status"] != status:
                raise ValueError("native_status_filter_unavailable")
        for row in rows:
            self.observe(row)
        if more:
            cursor["offset"] += len(rows)
            if cursor["offset"] > 10000:
                raise ValueError("native_active_session_limit")
        else:
            cursor = {"lane": (cursor["lane"] + 1) % 3, "offset": 0, "to": self.time()}
        with self.store._transaction(immediate=True) as db:
            db.execute("UPDATE native_acceptance_meta SET value=? WHERE key='cursor'", (json.dumps(cursor),))

    def poll_old_queue(self):
        cursor = json.loads(self.meta("old_queue_cursor"))
        rows, more = self.page(self.rpc("imopenlines.v2.Session.list", {
            "status": "new", "dateCreateFrom": iso(cursor["to"] - 365 * 86400),
            "dateCreateTo": iso(cursor["to"]), "order": "dateCreate", "orderDirection": "desc",
            "offset": cursor["offset"], "limit": 200}))
        for row in rows:
            item = self.candidate(row)
            if item and item["status"] != "new":
                raise ValueError("native_status_filter_unavailable")
        for row in rows:
            self.observe(row)
        cursor = ({**cursor, "offset": cursor["offset"] + len(rows)} if more
                  else {"offset": 0, "to": self.time()})
        if cursor["offset"] > 10000:
            raise ValueError("native_old_queue_limit")
        with self.store._transaction(immediate=True) as db:
            db.execute("UPDATE native_acceptance_meta SET value=? WHERE key='old_queue_cursor'", (json.dumps(cursor),))

    def read_session(self, item):
        created = timestamp(item["date_create"])
        matches = []
        for offset in (0, 200):
            rows, more = self.page(self.rpc("imopenlines.v2.Session.list", {
                "configId": item["line_id"], "source": item["source"],
                "dateCreateFrom": iso(created - 1), "dateCreateTo": iso(created + 1),
                "order": "dateCreate", "orderDirection": "desc", "offset": offset, "limit": 200}))
            matches.extend(row for row in rows if isinstance(row, dict) and entity_id(row.get("id")) == item["session_id"])
            if not more:
                if len(matches) != 1:
                    raise ValueError("native_exact_session_unavailable")
                return matches[0]
        raise ValueError("native_exact_session_ambiguous")

    def recheck(self, limit=5):
        with self.store._connect() as db:
            rows = db.execute("SELECT * FROM native_acceptance_sessions WHERE armed=1 AND next_check<=? ORDER BY next_check,session_id LIMIT ?", (self.time(), max(1, min(5, limit)))).fetchall()
        for row in rows:
            # Schedule failures as well, so one unavailable session cannot starve
            # other tracked dialogs. Failure never becomes an empty baseline.
            with self.store._transaction(immediate=True) as db:
                db.execute("UPDATE native_acceptance_sessions SET next_check=? WHERE session_id=?", (self.time() + 60, row["session_id"]))
            try:
                self.observe(self.read_session(row))
            except Exception:
                LOG.warning("native acceptance recheck unavailable")

    def save(self, job, status, code="", **fields):
        values = {"status": status, "error_code": code, "next_at": self.time() + 60, **fields}
        with self.store._transaction(immediate=True) as db:
            db.execute("UPDATE native_acceptance_jobs SET " + ",".join(f"{key}=?" for key in values) + " WHERE operation_key=?", (*values.values(), job["operation_key"]))
        if status == "conflict":
            LOG.warning("native acceptance stopped: %s", code)

    def status(self):
        with self.store._connect() as db:
            return {row["status"]: row["n"] for row in db.execute("SELECT status,count(*) n FROM native_acceptance_jobs GROUP BY status")}

    def current(self, job):
        with self.store._connect() as db:
            row = db.execute("SELECT revision FROM native_acceptance_sessions WHERE session_id=?", (job["session_id"],)).fetchone()
            newer = db.execute("""SELECT 1 FROM native_acceptance_jobs WHERE deal_id=? AND chat_id=?
                AND (julianday(date_create)>julianday(?) OR (session_id=? AND revision>?)) LIMIT 1""",
                (job["deal_id"], job["chat_id"], job["date_create"], job["session_id"], job["revision"])).fetchone()
        if not row or row["revision"] != job["revision"] or self.claim_fence(job["deal_id"]) != job["claim_fence"]:
            raise Conflict("native_acceptance_superseded")
        if newer:
            raise Conflict("native_acceptance_superseded")
        if self.store.list_unresolved_claim_operations_for_deal(job["deal_id"], limit=1):
            raise Conflict("picker_claim_pending")
        with self.store._connect() as db:
            claims = db.execute("SELECT updated_at FROM claim_operations WHERE deal_id=? AND status='succeeded'", (job["deal_id"],)).fetchall()
        for claim in claims:
            changed = timestamp(claim["updated_at"])
            if changed is None:
                raise ValueError("native_claim_time_unavailable")
            if changed > job["observed_at"]:
                raise Conflict("native_newer_picker_claim")

    def proof(self, job):
        self.current(job)
        session = self.candidate(self.read_session(job))
        if not session or any(session[key] != job[key] for key in ("session_id", "chat_id", "line_id", "source", "date_create", "deal_id", "manager_id")) or session["status"] not in ACCEPTED:
            raise Conflict("native_acceptance_changed")
        dialog = self.rpc("imopenlines.dialog.get", {"CHAT_ID": job["chat_id"]}) or {}
        code = str(dialog.get("entity_id") or "").split("|")
        data = str(dialog.get("entity_data_1") or "").split("|")
        bindings = str(dialog.get("entity_data_2") or "").split("|")
        deals = {bindings[i + 1] for i in range(0, len(bindings) - 1, 2) if bindings[i] == "DEAL" and entity_id(bindings[i + 1])}
        if len(data) > 2 and data[1] == "DEAL" and entity_id(data[2]):
            deals.add(data[2])
        if (entity_id(dialog.get("id")) != job["chat_id"] or dialog.get("entity_type") != "LINES"
                or len(code) < 3 or code[:2] != [job["source"], job["line_id"]]
                or len(data) < 6 or data[5] != job["session_id"]
                or deals != {job["deal_id"]} or entity_id(dialog.get("owner")) != job["manager_id"]):
            raise Conflict("native_dialog_binding_changed")
        chats = self.rpc("imopenlines.crm.chat.get", {"CRM_ENTITY_TYPE": "deal", "CRM_ENTITY": job["deal_id"], "ACTIVE_ONLY": "Y"})
        active = [c for c in chats if isinstance(c, dict) and supported(c.get("CONNECTOR_ID"))] if isinstance(chats, list) else []
        if len(active) != 1 or entity_id(active[0].get("CHAT_ID")) != job["chat_id"] or active[0].get("CONNECTOR_ID") != job["source"]:
            raise Conflict("native_crm_binding_changed")
        users = self.rpc("user.get", {"ID": job["manager_id"]})
        if not isinstance(users, list) or len(users) != 1 or entity_id(users[0].get("ID")) != job["manager_id"]:
            raise Conflict("native_employee_unavailable")
        user = users[0]
        departments = user.get("UF_DEPARTMENT")
        departments = departments if isinstance(departments, list) else [departments]
        if user.get("ACTIVE") not in (True, "Y", "1", 1) or not any(entity_id(v) for v in departments):
            raise Conflict("native_employee_inactive")
        deal = self.rpc("crm.deal.get", {"id": job["deal_id"]}) or {}
        if (entity_id(deal.get("ID")) != job["deal_id"] or deal.get("CLOSED") != "N"
                or deal.get("STAGE_SEMANTIC_ID") != "P" or deal.get("IS_ARCHIVED") in (True, "Y", "1", 1)
                or not entity_id(deal.get("ASSIGNED_BY_ID"), allow_zero=True)
                or timestamp(deal.get("DATE_MODIFY")) is None):
            raise Conflict("native_deal_not_open")
        self.current(job)
        return deal

    def deal_identity(self, deal):
        marker = deal.get(self.marker_field) or ""
        if not isinstance(marker, str):
            raise Conflict("native_marker_unavailable")
        return (entity_id(deal.get("ASSIGNED_BY_ID"), allow_zero=True),
                deal.get("DATE_MODIFY"), marker, entity_id(deal.get("CONTACT_ID")))

    def confirmed(self, job):
        if job["contact_generation"]:
            self.contacts.confirm_native(job["contact_generation"])
        self.save(job, "ready")
        if self.after_confirmed:
            try:
                self.after_confirmed(job["deal_id"])
            except Exception:
                LOG.warning("native acceptance cache refresh unavailable")

    def readback(self, job):
        # This path deliberately never sends another deal update.
        deal = self.rpc("crm.deal.get", {"id": job["deal_id"]}) or {}
        if entity_id(deal.get("ID")) != job["deal_id"]:
            raise ValueError("native_deal_identity_unavailable")
        owner, _modified, marker, contact = self.deal_identity(deal)
        if owner == job["manager_id"]:
            if marker != job["expected_marker"] or contact != job["contact_id"]:
                raise Conflict("native_deal_binding_changed")
            self.proof(job)
            self.confirmed(job)
        elif job["acknowledged"]:
            raise Conflict("native_deal_owner_changed")
        else:
            # A timed-out request may still arrive even if another external
            # owner is now visible. Keep the per-deal fence until reconciled.
            self.save(job, "pending", "native_deal_update_uncertain")

    def process(self):
        with self.store._connect() as db:
            row = db.execute("""SELECT j.* FROM native_acceptance_jobs j WHERE status='pending' AND next_at<=?
                AND NOT EXISTS(SELECT 1 FROM native_acceptance_jobs busy WHERE busy.deal_id=j.deal_id
                    AND busy.operation_key!=j.operation_key AND busy.dispatched=1 AND busy.status='pending')
                ORDER BY next_at,observed_at,operation_key LIMIT 1""", (self.time(),)).fetchone()
        if not row:
            return False
        job = dict(row)
        with self.locks.hold(job["manager_id"], job["deal_id"]):
            # A second caller may have waited while the first completed this job.
            with self.store._connect() as db:
                current = db.execute("SELECT * FROM native_acceptance_jobs WHERE operation_key=?", (job["operation_key"],)).fetchone()
            if not current or current["status"] != "pending" or current["next_at"] > self.time():
                return False
            job = dict(current)
            if not job["dispatched"] and native_deal_unresolved(self.store, job["deal_id"]):
                self.save(job, "pending", "native_previous_update_uncertain")
                return False
            try:
                if job["dispatched"]:
                    self.readback(job)
                    return True
                deal = self.proof(job)
                owner, modified, marker, contact = self.deal_identity(deal)
                if timestamp(modified) > job["observed_at"] + 1:
                    raise Conflict("native_deal_changed_after_acceptance")
                if job["previous_owner"] is not None and (owner, modified, marker, contact) != (
                        job["previous_owner"], job["expected_modified"], job["expected_marker"], job["contact_id"]):
                    raise Conflict("native_deal_changed_before_dispatch")
                if job["previous_owner"] is None:
                    generation = None
                    if contact:
                        snapshot_started = iso(self.time())
                        record = self.rpc("crm.contact.get", {"id": contact}) or {}
                        previous = entity_id(record.get("ASSIGNED_BY_ID"), allow_zero=True)
                        if entity_id(record.get("ID")) != contact or not previous:
                            raise ValueError("native_contact_unavailable")
                        try:
                            generation = self.contacts.reserve_native(job["operation_key"], job["deal_id"], job["manager_id"], contact, previous, marker, snapshot_started_at=snapshot_started, observed_at=iso(job["observed_at"]))
                        except ValueError as exc:
                            if str(exc) in {"newer_contact_claim", "native_contact_intent_conflict"}:
                                raise Conflict(str(exc)) from None
                            raise
                    fields = {"previous_owner": owner, "expected_modified": modified, "expected_marker": marker,
                              "contact_id": contact, "contact_generation": generation}
                    self.save(job, "pending", **fields)
                    job.update(fields)
                final = self.proof(job)
                if self.deal_identity(final) != (owner, modified, marker, contact):
                    raise Conflict("native_deal_changed_before_dispatch")
                # Persist before transport. A timeout/crash is read-only recovery.
                self.save(job, "pending", dispatched=1)
                job["dispatched"] = 1
                if owner != job["manager_id"]:
                    rejected = False
                    try:
                        result = self.rpc("crm.deal.update", {"id": job["deal_id"], "fields[ASSIGNED_BY_ID]": job["manager_id"]})
                        rejected = result is False
                        if result is True:
                            self.save(job, "pending", acknowledged=1)
                            job["acknowledged"] = 1
                    except Exception as error:
                        rejected = getattr(error, "bitrix_error_code", None) in {
                            "ACCESS_DENIED", "ERROR_ACCESS_DENIED", "ERROR_METHOD_NOT_FOUND",
                            "INVALID_CREDENTIALS", "NO_AUTH_FOUND", "ERROR_NOT_FOUND",
                        }
                    if rejected:
                        self.save(job, "pending", dispatched=0)
                        job["dispatched"] = 0
                        raise Conflict("native_deal_update_rejected")
                else:
                    # Nothing was sent: a later manual owner change cannot be
                    # mistaken for an unresolved in-flight native request.
                    self.save(job, "pending", acknowledged=1)
                    job["acknowledged"] = 1
                self.readback(job)
            except Conflict as exc:
                if job["contact_generation"]:
                    if job["dispatched"]:
                        self.contacts.conflict_native(job["contact_generation"])
                    else:
                        self.contacts.cancel_native(job["contact_generation"])
                elif not job["dispatched"]:
                    # A crash may leave the contact reservation committed just
                    # before this job records its generation. No deal write was
                    # dispatched, so release only that reserved native intent.
                    self.contacts.cancel_native_operation(job["operation_key"])
                self.save(job, "conflict", str(exc))
            except Exception:
                self.save(job, "pending", "native_sync_unavailable")
        return True

    def run(self):
        if not self.running.acquire(blocking=False):
            return
        try:
            ticks = int(self.meta("ticks")) + 1
            with self.store._transaction(immediate=True) as db:
                db.execute("UPDATE native_acceptance_meta SET value=? WHERE key='ticks'", (str(ticks),))
            actions = [self.poll, self.recheck, self.process]
            if ticks % 4 == 0:
                actions.insert(1, self.poll_old_queue)
            # Independent failures do not stop a previously persisted job.
            for action in actions:
                try:
                    action()
                except Exception:
                    LOG.warning("native acceptance %s unavailable", action.__name__)
        finally:
            self.running.release()
