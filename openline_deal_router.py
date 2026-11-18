"""Route newly created, unanswered Open Lines sessions to their active deal owner.

Discovery is durable and starts at activation. A bot transfer is preceded by a
durable intent; an uncertain result is only read back, never blindly repeated.
The module writes neither CRM entities nor customer messages. Existing sessions
which survived a lost deal are deliberately outside this new-session router.
"""
import hashlib
import json
import math
import re
import threading
import uuid
from datetime import datetime, timezone

from contact_owner_policy import EXCLUDED_STAGES, contact_ids, eligible_deal, latest_eligible_deal
from contact_owner_sync import entity_id


PAGE_SIZE = 200
WINDOW_SECONDS = 300
SETTLE_SECONDS = 2
OVERLAP_SECONDS = 2
MAX_WINDOW_ROWS = 10000
READBACK_ATTEMPTS = 10
READ_METHODS = frozenset({
    "imopenlines.config.get", "imopenlines.v2.Session.list", "imopenlines.dialog.get",
    "imopenlines.crm.chat.get", "crm.deal.get",
    "crm.deal.contact.items.get", "crm.item.list", "user.get",
})
SCHEMA = """
CREATE TABLE IF NOT EXISTS openline_router_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS openline_router_jobs(
 session_id TEXT PRIMARY KEY,chat_id TEXT NOT NULL,line_id TEXT NOT NULL,
 source TEXT NOT NULL,date_create TEXT NOT NULL,created_at REAL NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending',deal_id TEXT NOT NULL DEFAULT '',
 manager_id TEXT NOT NULL DEFAULT '',proof_hash TEXT NOT NULL DEFAULT '',
 dispatched INTEGER NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,
 next_at REAL NOT NULL DEFAULT 0,error_code TEXT NOT NULL DEFAULT '',
 lease_token TEXT NOT NULL DEFAULT '',lease_until REAL NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS openline_router_due ON openline_router_jobs(state,next_at);
"""


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("router_time_unavailable")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        result = parsed.timestamp()
        if parsed.tzinfo is None or parsed.utcoffset() is None or not math.isfinite(result):
            raise ValueError()
        return result
    except (ValueError, OverflowError, OSError):
        raise ValueError("router_time_unavailable") from None


def iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class Decision(Exception):
    """A proven terminal outcome; errors without proof remain retryable."""
    def __init__(self, state, code):
        super().__init__(code)
        self.state, self.code = state, code


def validate_router_settings(*, allowed_lines, bot_id, bot_client_id):
    """Pure startup/readiness validation; return only normalized public scope."""
    if not entity_id(bot_id) or not isinstance(bot_client_id, str) or not bot_client_id.strip():
        raise ValueError("router_bot_configuration_required")
    if not isinstance(allowed_lines, dict) or not allowed_lines:
        raise ValueError("router_scope_required")
    lanes = []
    for line, sources in allowed_lines.items():
        line = entity_id(line)
        if not line or not isinstance(sources, (list, tuple, set, frozenset)) or not sources:
            raise ValueError("router_scope_invalid")
        for source in sources:
            if not isinstance(source, str) or not re.fullmatch(r"[A-Za-z0-9_:-]{1,128}", source):
                raise ValueError("router_scope_invalid")
            lanes.append((line, source))
    return sorted(set(lanes))


class OpenlineDealRouter:
    def __init__(self, store, call, locks, *, allowed_lines, bot_id, bot_client_id, now=None):
        self.store, self.call, self.locks = store, call, locks
        self.clock = now or (lambda: datetime.now(timezone.utc))
        self.running = threading.Lock()
        self.bot_id = entity_id(bot_id)
        # This credential stays in memory and is never written to SQLite/logs.
        self.bot_client_id = bot_client_id
        self.lanes = validate_router_settings(allowed_lines=allowed_lines, bot_id=bot_id,
                                              bot_client_id=bot_client_id)
        self.scope = set(self.lanes)
        store._ensure_ready()
        with store._transaction(immediate=True) as db:
            for statement in SCHEMA.split(";"):
                if statement.strip():
                    db.execute(statement)
            # Preserve the exact instant, rather than replaying earlier rows in
            # the activation second. Bitrix's later whole-second sessions pass.
            db.execute("INSERT OR IGNORE INTO openline_router_meta VALUES('activated_at',?)", (str(self.time()),))
            scope = json.dumps(self.lanes)
            db.execute("INSERT OR IGNORE INTO openline_router_meta VALUES('scope',?)", (scope,))
            if db.execute("SELECT value FROM openline_router_meta WHERE key='scope'").fetchone()[0] != scope:
                raise ValueError("router_persisted_scope_changed")
            activation = float(db.execute("SELECT value FROM openline_router_meta WHERE key='activated_at'").fetchone()[0])
            for index in range(len(self.lanes)):
                db.execute("INSERT OR IGNORE INTO openline_router_meta VALUES(?,?)", (
                    f"cursor:{index}", json.dumps({"from": activation, "to": activation,
                                                   "offset": 0, "seen": [], "complete": True})))
            db.execute("INSERT OR IGNORE INTO openline_router_meta VALUES('lane','0')")

    def time(self):
        value = self.clock()
        result = value.timestamp() if isinstance(value, datetime) else float(value)
        if not math.isfinite(result):
            raise ValueError("router_clock_unavailable")
        return result

    def rpc(self, method, params, timeout=8):
        if method not in READ_METHODS:
            raise ValueError("router_read_only_call_required")
        return self.call(method, params, timeout=8)

    def meta(self, key):
        with self.store._connect() as db:
            return db.execute("SELECT value FROM openline_router_meta WHERE key=?", (key,)).fetchone()[0]

    def identity(self, row):
        if not isinstance(row, dict):
            raise ValueError("router_session_unavailable")
        item = {name: entity_id(row.get(field)) for name, field in
                (("session_id", "id"), ("chat_id", "chatId"), ("line_id", "configId"))}
        if (not all(item.values()) or (item["line_id"], row.get("source")) not in self.scope
                or timestamp(row.get("dateCreate")) > self.time() + 60):
            raise ValueError("router_session_identity_unavailable")
        if row.get("status") not in {"new", "answered", "paused", "closed", "spam"}:
            raise ValueError("router_session_status_unavailable")
        return {**item, "source": row["source"], "date_create": row["dateCreate"]}

    @staticmethod
    def page(result):
        if (not isinstance(result, dict) or not isinstance(result.get("sessions"), list)
                or type(result.get("hasNextPage")) is not bool
                or len(result["sessions"]) > PAGE_SIZE
                or (result["hasNextPage"] and not result["sessions"])):
            raise ValueError("router_session_page_unavailable")
        return result["sessions"], result["hasNextPage"]

    def poll(self):
        """Commit one bounded page together with all discovered session IDs.

        No status filter is used: acceptance cannot remove rows from an offset
        page while it is being traversed. Frozen time windows survive downtime.
        """
        with self.store._transaction(immediate=True) as db:
            lane = int(db.execute("SELECT value FROM openline_router_meta WHERE key='lane'").fetchone()[0])
            key = f"cursor:{lane}"
            raw = db.execute("SELECT value FROM openline_router_meta WHERE key=?", (key,)).fetchone()[0]
            cursor = json.loads(raw)
            activation = float(db.execute("SELECT value FROM openline_router_meta WHERE key='activated_at'").fetchone()[0])
            if cursor["complete"]:
                end = min(math.floor(self.time()) - SETTLE_SECONDS, cursor["to"] + WINDOW_SECONDS)
                if end <= cursor["to"]:
                    db.execute("UPDATE openline_router_meta SET value=? WHERE key='lane'", (str((lane + 1) % len(self.lanes)),))
                    return None
                cursor = {"from": max(activation, cursor["to"] - OVERLAP_SECONDS),
                          "to": end, "offset": 0, "seen": [], "complete": False}
                raw = json.dumps(cursor)
                db.execute("UPDATE openline_router_meta SET value=? WHERE key=?", (raw, key))
        line, source = self.lanes[lane]
        rows, more = self.page(self.rpc("imopenlines.v2.Session.list", {
            "configId": line, "source": source, "dateCreateFrom": iso(cursor["from"]),
            "dateCreateTo": iso(cursor["to"]), "order": "dateCreate", "orderDirection": "asc",
            "offset": cursor["offset"], "limit": PAGE_SIZE,
        }))
        validated, seen, previous = [], set(cursor.get("seen", [])), None
        for row in rows:
            item = self.identity(row)
            created = timestamp(item["date_create"])
            if ((item["line_id"], item["source"]) != (line, source)
                    or not cursor["from"] <= created <= cursor["to"]
                    or item["session_id"] in seen or (previous is not None and created < previous)):
                raise ValueError("router_discovery_filter_unavailable")
            previous = created
            seen.add(item["session_id"])
            validated.append((item, row["status"]))
        if more and cursor["offset"] + len(rows) >= MAX_WINDOW_ROWS:
            raise ValueError("router_discovery_window_limit")
        with self.store._transaction(immediate=True) as db:
            if db.execute("SELECT value FROM openline_router_meta WHERE key=?", (key,)).fetchone()[0] != raw:
                return None
            for item, status in validated:
                old = db.execute("SELECT * FROM openline_router_jobs WHERE session_id=?", (item["session_id"],)).fetchone()
                if old and any(old[field] != item[field] for field in item):
                    raise ValueError("router_session_identity_changed")
                db.execute("""INSERT OR IGNORE INTO openline_router_jobs(
                    session_id,chat_id,line_id,source,date_create,created_at,state,error_code)
                    VALUES(?,?,?,?,?,?,?,?)""", (
                        item["session_id"], item["chat_id"], line, source, item["date_create"], self.time(),
                        "pending" if status == "new" else "skipped",
                        "" if status == "new" else "router_session_already_taken"))
            updated = {**cursor, "offset": cursor["offset"] + len(rows),
                       "seen": sorted(seen, key=int), "complete": not more}
            db.execute("UPDATE openline_router_meta SET value=? WHERE key=?", (json.dumps(updated), key))
            db.execute("UPDATE openline_router_meta SET value=? WHERE key='lane'", (str((lane + 1) % len(self.lanes)),))
        return {"read": len(rows)}

    def config(self, line_id):
        config = self.rpc("imopenlines.config.get", {"CONFIG_ID": line_id})
        if (not isinstance(config, dict) or entity_id(config.get("ID")) != line_id
                or config.get("ACTIVE") != "Y" or config.get("CRM_FORWARD") != "N"
                or config.get("WELCOME_BOT_ENABLE") != "Y"
                or config.get("WELCOME_BOT_JOIN") != "always"
                or entity_id(config.get("WELCOME_BOT_ID")) != self.bot_id):
            raise ValueError("router_config_not_ready")
        queue = config.get("QUEUE")
        if not isinstance(queue, list) or any(not entity_id(value) for value in queue):
            raise ValueError("router_queue_unavailable")
        return {entity_id(value) for value in queue}

    def session(self, job):
        created = timestamp(job["date_create"])
        matches = []
        for offset in (0, PAGE_SIZE):
            rows, more = self.page(self.rpc("imopenlines.v2.Session.list", {
                "configId": job["line_id"], "source": job["source"],
                "dateCreateFrom": iso(created), "dateCreateTo": iso(created),
                "order": "dateCreate", "orderDirection": "asc", "offset": offset, "limit": PAGE_SIZE,
            }))
            for row in rows:
                if isinstance(row, dict) and entity_id(row.get("id")) == job["session_id"]:
                    matches.append(row)
            if not more:
                if len(matches) != 1:
                    raise ValueError("router_exact_session_unavailable")
                item = self.identity(matches[0])
                if any(item[key] != job[key] for key in item):
                    raise Decision("blocked", "router_session_identity_changed")
                return matches[0]
        raise ValueError("router_exact_session_limit")

    def dialog(self, job, session, *, unanswered):
        dialog = self.rpc("imopenlines.dialog.get", {"CHAT_ID": job["chat_id"]})
        if not isinstance(dialog, dict):
            raise ValueError("router_dialog_unavailable")
        code = str(dialog.get("entity_id") or "").split("|")
        data = str(dialog.get("entity_data_1") or "").split("|")
        bindings = str(dialog.get("entity_data_2") or "").split("|")
        deals = {bindings[i + 1] for i in range(0, len(bindings) - 1, 2)
                 if bindings[i] == "DEAL" and entity_id(bindings[i + 1])}
        contacts = {bindings[i + 1] for i in range(0, len(bindings) - 1, 2)
                    if bindings[i] == "CONTACT" and entity_id(bindings[i + 1])}
        if len(data) > 2 and data[1] == "DEAL" and entity_id(data[2]):
            deals.add(data[2])
        did = entity_id(session.get("crmEntityId")) if session.get("crmEntityType") == "deal" else ""
        if not did or not deals or not contacts:
            raise ValueError("router_crm_binding_pending")
        type_ok = dialog.get("type") == "lines" or dialog.get("entity_type") == "LINES"
        if (not type_ok or (dialog.get("type") not in (None, "lines"))
                or (dialog.get("entity_type") not in (None, "LINES"))
                or entity_id(dialog.get("id")) != job["chat_id"]
                or code[:2] != [job["source"], job["line_id"]]
                or len(data) < 6 or data[5] != job["session_id"] or deals != {did}):
            raise Decision("blocked", "router_dialog_binding_changed")
        owner = entity_id(dialog.get("owner"), allow_zero=True)
        managers = dialog.get("manager_list")
        if not owner or not isinstance(managers, list) or any(not entity_id(value) for value in managers):
            raise ValueError("router_dialog_owner_unavailable")
        if unanswered and (owner not in {"0", self.bot_id}
                           or any(entity_id(value) != self.bot_id for value in managers)):
            raise Decision("skipped", "router_human_takeover")
        return did, owner, contacts

    def live(self, job):
        queue = self.config(job["line_id"])
        session = self.session(job)
        if "dateOperatorAnswer" not in session or "dateClose" not in session:
            raise ValueError("router_answer_state_unavailable")
        if session["status"] != "new" or session.get("dateOperatorAnswer") not in (None, ""):
            raise Decision("skipped", "router_session_already_taken")
        if session.get("dateClose") not in (None, ""):
            raise Decision("skipped", "router_session_closed")
        # operatorId on a new session can merely be the queue offer (41 on
        # line103); only a fresh unanswered dialog proves no human took it.
        if "operatorId" not in session:
            raise ValueError("router_session_operator_unavailable")
        operator = "0" if session["operatorId"] is None else entity_id(session["operatorId"], allow_zero=True)
        if not operator:
            raise ValueError("router_session_operator_unavailable")
        if operator not in queue | {"0", self.bot_id}:
            raise Decision("skipped", "router_other_operator_offer")
        did, _, dialog_contacts = self.dialog(job, session, unanswered=True)
        # The configured welcome bot can transfer its native session even after
        # leaving the participant list (verified on an isolated live line).
        # Membership is neither authority nor evidence of human acceptance;
        # the configured bot credential and exact unanswered proofs govern IO.
        chats = self.rpc("imopenlines.crm.chat.get", {
            # A restarted native session can be current while the CRM registry
            # still reports its activity as completed. Current session/dialog
            # identity above proves activity; this registry proves association.
            "CRM_ENTITY_TYPE": "deal", "CRM_ENTITY": did, "ACTIVE_ONLY": "N",
        })
        if not isinstance(chats, list):
            raise ValueError("router_crm_chat_unavailable")
        matches = [row for row in chats if isinstance(row, dict)
                   and entity_id(row.get("CHAT_ID")) == job["chat_id"]
                   and row.get("CONNECTOR_ID") == job["source"]]
        if len(matches) != 1:
            raise ValueError("router_crm_chat_binding_pending")
        actual_contacts = set(contact_ids(self.rpc, {"ID": did}))
        if not dialog_contacts.issubset(actual_contacts):
            raise Decision("blocked", "router_dialog_contact_mismatch")
        return did

    def policy(self, did):
        deal = self.rpc("crm.deal.get", {"id": did})
        if not isinstance(deal, dict) or entity_id(deal.get("ID")) != did:
            raise ValueError("router_deal_unavailable")
        if deal.get("CLOSED") != "N" or deal.get("STAGE_SEMANTIC_ID") != "P":
            raise Decision("blocked", "router_bound_deal_not_open")
        if deal.get("IS_ARCHIVED") in (True, "Y", 1, "1"):
            raise Decision("blocked", "router_bound_deal_archived")
        ids = sorted(contact_ids(self.rpc, deal), key=int)
        if not ids:
            raise ValueError("router_contact_binding_pending")
        winners = [latest_eligible_deal(self.rpc, cid) for cid in ids]
        stage = deal.get("STAGE_ID")
        early = isinstance(stage, str) and stage.rsplit(":", 1)[-1] in EXCLUDED_STAGES
        if all(winner is None for winner in winners) and early:
            return {"decision": "intake", "deal_id": did, "manager_id": "", "contacts": ids,
                    "hash": digest([did, stage, deal.get("DATE_MODIFY"), ids, "intake"])}
        if not eligible_deal(deal) or any(winner is None or entity_id(winner.get("ID")) != did for winner in winners):
            raise Decision("blocked", "router_canonical_deal_mismatch")
        owner = entity_id(deal.get("ASSIGNED_BY_ID"))
        fields = ("ID", "ASSIGNED_BY_ID", "STAGE_ID", "STAGE_SEMANTIC_ID", "CLOSED", "DATE_MODIFY")
        if any(any(str(winner.get(field)) != str(deal.get(field)) for field in fields) for winner in winners):
            raise ValueError("router_deal_changed_during_policy")
        users = self.rpc("user.get", {"ID": owner})
        user = users[0] if isinstance(users, list) and len(users) == 1 and isinstance(users[0], dict) else {}
        departments = user.get("UF_DEPARTMENT")
        departments = departments if isinstance(departments, list) else [departments]
        if (entity_id(user.get("ID")) != owner or user.get("ACTIVE") not in (True, "Y", 1, "1")
                or not any(entity_id(value) for value in departments)
                or user.get("IS_BOT") in (True, "Y", 1, "1") or owner == self.bot_id):
            raise Decision("blocked", "router_employee_unavailable")
        return {"decision": "route", "deal_id": did, "manager_id": owner, "contacts": ids,
                "hash": digest([[field, deal.get(field)] for field in fields] + [["contacts", ids]])}

    def _finish(self, job, state, code=""):
        with self.store._transaction(immediate=True) as db:
            db.execute("""UPDATE openline_router_jobs SET state=?,error_code=?,lease_token='',lease_until=0
                WHERE session_id=? AND lease_token=?""", (state, code, job["session_id"], job["lease_token"]))
        return {"state": state, "code": code}

    def _retry(self, job, code):
        attempts = job["attempts"] + 1
        if job["dispatched"] and attempts >= READBACK_ATTEMPTS:
            return self._finish(job, "uncertain", "router_transfer_unconfirmed")
        with self.store._transaction(immediate=True) as db:
            db.execute("""UPDATE openline_router_jobs SET state=?,attempts=?,next_at=?,error_code=?,
                lease_token='',lease_until=0 WHERE session_id=? AND lease_token=?""", (
                    "dispatched" if job["dispatched"] else "pending", attempts,
                    self.time() + min(300, 15 * attempts), code, job["session_id"], job["lease_token"]))
        return {"state": "dispatched" if job["dispatched"] else "pending", "code": code}

    def reconcile(self, job):
        """Only observe a dispatched transfer; this method cannot send one."""
        session = self.session(job)
        if session["status"] in {"closed", "spam"}:
            return self._finish(job, "superseded", "router_session_closed")
        did, owner, _ = self.dialog(job, session, unanswered=False)
        operator = entity_id(session.get("operatorId"), allow_zero=True)
        if did != job["deal_id"]:
            return self._finish(job, "superseded", "router_binding_changed_after_dispatch")
        if operator == job["manager_id"] and owner == job["manager_id"]:
            return self._finish(job, "routed")
        if owner not in {"0", self.bot_id, job["manager_id"]}:
            return self._finish(job, "superseded", "router_human_takeover")
        return self._retry(job, "router_transfer_readback_pending")

    def process(self):
        with self.store._transaction(immediate=True) as db:
            row = db.execute("""SELECT * FROM openline_router_jobs
                WHERE state IN ('pending','dispatched') AND next_at<=? AND lease_until<=?
                ORDER BY next_at,created_at,CAST(session_id AS INTEGER) LIMIT 1""", (self.time(), self.time())).fetchone()
            if row is None:
                return None
            job = dict(row)
            job["lease_token"] = uuid.uuid4().hex
            db.execute("UPDATE openline_router_jobs SET lease_token=?,lease_until=? WHERE session_id=?",
                       (job["lease_token"], self.time() + 120, job["session_id"]))
        try:
            if job["dispatched"]:
                return self.reconcile(job)
            did = self.live(job)
            proposed = self.policy(did)
            if proposed["decision"] == "intake":
                # The same fresh proof is needed for intake: do not seal a
                # decision after a concurrent manager claim changed the deal.
                lock_manager = self.bot_id
            else:
                lock_manager = proposed["manager_id"]
            with self.locks.hold(lock_manager, did):
                fresh = self.policy(did)
                if fresh != proposed:
                    raise ValueError("router_policy_changed")
                if self.live(job) != did:
                    raise ValueError("router_live_deal_changed")
                if fresh["decision"] == "intake":
                    return self._finish(job, "intake")
                with self.store._transaction(immediate=True) as db:
                    changed = db.execute("""UPDATE openline_router_jobs SET dispatched=1,state='dispatched',
                        deal_id=?,manager_id=?,proof_hash=?,attempts=0,error_code=''
                        WHERE session_id=? AND dispatched=0 AND lease_token=? AND lease_until>?""", (
                            did, fresh["manager_id"], fresh["hash"], job["session_id"], job["lease_token"], self.time())).rowcount
                if changed != 1:
                    return {"state": "deferred", "code": "router_lease_changed"}
                job.update(dispatched=1, deal_id=did, manager_id=fresh["manager_id"], attempts=0)
                # No catch/retry wrapper may resend this method. Its only
                # subsequent path, including after a restart, is reconcile().
                try:
                    self.call("imopenlines.bot.session.transfer", {
                        "CHAT_ID": job["chat_id"], "USER_ID": fresh["manager_id"],
                        "LEAVE": "Y", "CLIENT_ID": self.bot_client_id,
                    }, timeout=8)
                except Exception:
                    pass
                return self.reconcile(job)
        except Decision as outcome:
            return self._finish(job, outcome.state, outcome.code)
        except Exception:
            # Never persist arbitrary remote exceptions: they can contain the
            # bot credential or customer fields. Native snapshots retry later.
            return self._retry(job, "router_readback_unavailable" if job["dispatched"] else "router_snapshot_unavailable")

    def run(self):
        # This summary is safe for the parent service's operational logs. Never
        # return native payloads, credentials or per-customer identities here.
        result = {"observed": 0, "processed": 0, "routed": 0, "intake": 0,
                  "blocked": 0, "errors": 0, "deferred": 0}
        if not self.running.acquire(blocking=False):
            result["deferred"] = 1
            return result
        try:
            try:
                observed = self.poll()
                if observed:
                    result["observed"] = observed["read"]
            except Exception:
                result["errors"] += 1
            for _ in range(5):
                processed = self.process()
                if processed is None:
                    break
                result["processed"] += 1
                state = processed["state"]
                if state in {"routed", "intake", "blocked"}:
                    result[state] += 1
                else:
                    result["deferred"] += 1
                if processed.get("code") in {"router_snapshot_unavailable", "router_readback_unavailable",
                                             "router_transfer_unconfirmed"}:
                    result["errors"] += 1
            return result
        finally:
            self.running.release()
