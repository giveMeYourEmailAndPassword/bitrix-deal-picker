"""Durable returning-client intake -> native restart -> pickup-queue saga.

Every remote step has its own intent. Uncertain adds are found only by exact
origin marker; neither add, restart nor queue transfer is ever blindly retried.
"""
import hashlib
import json
import math
import re
import threading
import uuid
from datetime import datetime, timezone

from contact_owner_policy import EXCLUDED_STAGES, contact_ids, latest_eligible_deal
from contact_owner_sync import entity_id
from returning_deal_policy import open_deals_for_contacts

PAGE_SIZE = 200
MAX_SCAN_ROWS = 10000
MAX_HISTORY_MESSAGES = 5000
MAX_EXACT_SESSION_PAGES = 4
WATCH_BUDGET = 5
NATIVE_CREATE_GRACE_SECONDS = 30
ORIGINATOR = "krugosvet-returning-client"
READ_METHODS = frozenset({"imopenlines.config.get", "imopenlines.v2.Session.list",
    "imopenlines.dialog.get", "imopenlines.session.history.get", "crm.deal.list",
    "crm.deal.get", "crm.deal.contact.items.get", "crm.item.list"})
SCHEMA = """
CREATE TABLE IF NOT EXISTS lost_session_restart_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS lost_session_restart_watches(
 session_id TEXT PRIMARY KEY,chat_id TEXT NOT NULL,line_id TEXT NOT NULL,
 source TEXT NOT NULL,date_create TEXT NOT NULL,deal_id TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'watching',next_at REAL NOT NULL DEFAULT 0,
 attempts INTEGER NOT NULL DEFAULT 0,error_code TEXT NOT NULL DEFAULT '',
 lease_token TEXT NOT NULL DEFAULT '',lease_until REAL NOT NULL DEFAULT 0,
 dispatched INTEGER NOT NULL DEFAULT 0,dispatch_at REAL,
 message_id TEXT NOT NULL DEFAULT '',message_at REAL,client_id TEXT NOT NULL DEFAULT '',
 transition_id TEXT NOT NULL DEFAULT '',proof_hash TEXT NOT NULL DEFAULT '',
 new_session_id TEXT NOT NULL DEFAULT '',new_deal_id TEXT NOT NULL DEFAULT '');
CREATE INDEX IF NOT EXISTS lost_session_restart_due ON lost_session_restart_watches(state,next_at);
CREATE UNIQUE INDEX IF NOT EXISTS lost_session_restart_message_once
 ON lost_session_restart_watches(chat_id,message_id)
 WHERE dispatched=1 AND message_id<>'';
CREATE TABLE IF NOT EXISTS lost_session_restart_contact_fences(
 contact_id TEXT PRIMARY KEY,session_id TEXT NOT NULL);
"""
NEW_COLUMNS = {
    "crm_type": "TEXT NOT NULL DEFAULT 'deal'", "crm_id": "TEXT NOT NULL DEFAULT ''",
    "reserved": "INTEGER NOT NULL DEFAULT 0", "contacts_json": "TEXT NOT NULL DEFAULT '[]'",
    "origin_id": "TEXT NOT NULL DEFAULT ''", "source_proof_json": "TEXT NOT NULL DEFAULT '{}'",
    "create_dispatched": "INTEGER NOT NULL DEFAULT 0", "create_at": "REAL",
    "target_deal_id": "TEXT NOT NULL DEFAULT ''", "target_json": "TEXT NOT NULL DEFAULT '{}'",
    "target_identity_json": "TEXT NOT NULL DEFAULT '{}'",
    "queue_dispatched": "INTEGER NOT NULL DEFAULT 0", "queue_at": "REAL",
    "source_identity_key": "TEXT NOT NULL DEFAULT ''", "target_message_id": "TEXT NOT NULL DEFAULT ''",
}


def stamp(value):
    try:
        if not isinstance(value, str):
            raise ValueError()
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        result = parsed.timestamp()
        if parsed.tzinfo is None or parsed.utcoffset() is None or not math.isfinite(result):
            raise ValueError()
        return result
    except (ValueError, OverflowError, OSError):
        raise ValueError("restart_time_unavailable") from None


def iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def source_witness(message, sender):
    """Native reset copies ID/date; connector origin and authored body survive."""
    params = message.get("params")
    if not isinstance(params, dict):
        raise ValueError("restart_connector_params_unavailable")
    keys = [key for key in ("connectorMid", "CONNECTOR_MID") if key in params]
    if not keys:
        return None
    normalized = []
    for key in keys:
        raw = params[key]
        values = raw if isinstance(raw, list) else [raw]
        if (not values or len(values) > 20 or any(type(value) not in (str, int) for value in values)):
            raise ValueError("restart_connector_identity_unavailable")
        values = [str(value) for value in values]
        if any(not value or len(value) > 1000 or value.strip() != value for value in values) or len(values) != len(set(values)):
            raise ValueError("restart_connector_identity_unavailable")
        normalized.append(values)
    if any(values != normalized[0] for values in normalized[1:]):
        raise ValueError("restart_connector_alias_mismatch")
    authored = {key: value for key, value in params.items()
                if key not in {"connectorMid", "CONNECTOR_MID", "id", "ID", "date", "DATE"}}
    authored["connectorMid"] = normalized[0]
    if any(not isinstance(message.get(key), str) for key in ("text", "textlegacy")):
        raise ValueError("restart_source_body_unavailable")
    return {"sender": sender, "connector_mid": normalized[0],
            "identity_key": "v1:" + digest([sender, normalized[0]]),
            "body_hash": digest({"text": message["text"], "textlegacy": message["textlegacy"], "params": authored})}


class Decision(Exception):
    def __init__(self, state, code):
        super().__init__(code)
        self.state, self.code = state, code


class LostSessionRestart:
    def __init__(self, store, call, locks, *, allowed_lines, prove_failed,
                 bot_id, bot_client_id, now=None):
        self.store, self.call, self.locks, self.prove_failed = store, call, locks, prove_failed
        self.clock = now or (lambda: datetime.now(timezone.utc))
        self.running = threading.Lock()
        self.bot_id, self.bot_client_id = entity_id(bot_id), bot_client_id
        if not self.bot_id or not isinstance(bot_client_id, str) or not bot_client_id.strip():
            raise ValueError("restart_bot_configuration_required")
        if not callable(prove_failed) or not isinstance(allowed_lines, dict) or not allowed_lines:
            raise ValueError("restart_scope_required")
        scope = []
        for line, sources in allowed_lines.items():
            line = entity_id(line)
            if not line or not isinstance(sources, (list, tuple, set, frozenset)) or not sources:
                raise ValueError("restart_scope_invalid")
            for source in sources:
                if not isinstance(source, str) or not re.fullmatch(r"[A-Za-z0-9_:-]{1,128}", source):
                    raise ValueError("restart_scope_invalid")
                scope.append((line, source))
        self.scope = sorted(set(scope))
        self.lanes = [(line, source, status) for line, source in self.scope
                      for status in ("new", "answered", "paused")]
        store._ensure_ready()
        with store._transaction(immediate=True) as db:
            for statement in SCHEMA.split(";"):
                if statement.strip():
                    db.execute(statement)
            columns = {row[1] for row in db.execute("PRAGMA table_info(lost_session_restart_watches)")}
            for name, definition in NEW_COLUMNS.items():
                if name not in columns:
                    db.execute(f"ALTER TABLE lost_session_restart_watches ADD COLUMN {name} {definition}")
            db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS lost_session_restart_reserved_message
                ON lost_session_restart_watches(chat_id,message_id)
                WHERE (reserved=1 OR dispatched=1) AND message_id<>''""")
            db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS lost_session_restart_external_source_once
                ON lost_session_restart_watches(chat_id,source_identity_key)
                WHERE (reserved=1 OR dispatched=1) AND source_identity_key<>''""")
            db.execute("INSERT OR IGNORE INTO lost_session_restart_meta VALUES('activated_at',?)", (str(self.time()),))
            db.execute("INSERT OR IGNORE INTO lost_session_restart_meta VALUES('scope',?)", (json.dumps(self.scope),))
            if db.execute("SELECT value FROM lost_session_restart_meta WHERE key='scope'").fetchone()[0] != json.dumps(self.scope):
                raise ValueError("restart_persisted_scope_changed")
            db.execute("INSERT OR IGNORE INTO lost_session_restart_meta VALUES('lane','0')")
            for index in range(len(self.lanes)):
                db.execute("INSERT OR IGNORE INTO lost_session_restart_meta VALUES(?,?)",
                           (f"cursor:{index}", json.dumps({"upper": 0, "offset": 0, "complete": True})))

    def time(self):
        value = self.clock()
        result = value.timestamp() if isinstance(value, datetime) else float(value)
        if not math.isfinite(result):
            raise ValueError("restart_clock_unavailable")
        return result

    def meta(self, key):
        with self.store._connect() as db:
            return db.execute("SELECT value FROM lost_session_restart_meta WHERE key=?", (key,)).fetchone()[0]

    def rpc(self, method, params, timeout=8):
        if method not in READ_METHODS:
            raise ValueError("restart_read_only_call_required")
        return self.call(method, params, timeout=8)

    @staticmethod
    def page(value):
        if (not isinstance(value, dict) or not isinstance(value.get("sessions"), list)
                or type(value.get("hasNextPage")) is not bool
                or len(value["sessions"]) > PAGE_SIZE
                or (value["hasNextPage"] and not value["sessions"])):
            raise ValueError("restart_session_page_unavailable")
        return value["sessions"], value["hasNextPage"]

    def identity(self, row):
        required = {"id", "chatId", "configId", "source", "dateCreate", "status", "dateClose", "dateOperatorAnswer",
                    "crmEntityType", "crmEntityId", "userId"}
        if not isinstance(row, dict) or not required.issubset(row):
            raise ValueError("restart_session_unavailable")
        ids = {key: entity_id(row.get(field)) for key, field in
               (("session_id", "id"), ("chat_id", "chatId"), ("line_id", "configId"))}
        if (not all(ids.values()) or (ids["line_id"], row.get("source")) not in self.scope
                or row.get("status") not in {"new", "answered", "paused", "closed", "spam"}
                or stamp(row.get("dateCreate")) > self.time() + 60):
            raise ValueError("restart_session_identity_unavailable")
        return {**ids, "source": row["source"], "date_create": row["dateCreate"]}

    def poll(self):
        """One bounded page; repeat complete sweeps so shifted offsets heal."""
        with self.store._transaction(immediate=True) as db:
            lane = int(db.execute("SELECT value FROM lost_session_restart_meta WHERE key='lane'").fetchone()[0])
            key = f"cursor:{lane}"
            raw = db.execute("SELECT value FROM lost_session_restart_meta WHERE key=?", (key,)).fetchone()[0]
            cursor = json.loads(raw)
            if cursor["complete"]:
                cursor = {"upper": math.floor(self.time()) - 2, "offset": 0, "complete": False}
                raw = json.dumps(cursor)
                db.execute("UPDATE lost_session_restart_meta SET value=? WHERE key=?", (raw, key))
        line, source, status = self.lanes[lane]
        rows, more = self.page(self.rpc("imopenlines.v2.Session.list", {
            "configId": line, "source": source, "status": status,
            "dateCreateTo": iso(cursor["upper"]), "order": "dateCreate", "orderDirection": "asc",
            "offset": cursor["offset"], "limit": PAGE_SIZE}))
        if cursor["offset"] + len(rows) > MAX_SCAN_ROWS or (more and cursor["offset"] + len(rows) >= MAX_SCAN_ROWS):
            raise ValueError("restart_discovery_limit")
        candidates, seen = [], set()
        for row in rows:
            item = self.identity(row)
            if (item["session_id"] in seen or item["line_id"] != line or item["source"] != source
                    or row["status"] != status or stamp(item["date_create"]) > cursor["upper"]):
                raise ValueError("restart_discovery_filter_unavailable")
            seen.add(item["session_id"])
            kind, linked_id = row.get("crmEntityType"), entity_id(row.get("crmEntityId"))
            if kind in {"deal", "contact"} and linked_id and row.get("dateClose") is None:
                candidates.append({**item, "deal_id": linked_id if kind == "deal" else "",
                                   "crm_type": kind, "crm_id": linked_id})
        deal_ids = sorted({row["deal_id"] for row in candidates if row["deal_id"]}, key=int)
        failed = set()
        for start in range(0, len(deal_ids), 50):
            selected = set(deal_ids[start:start + 50])
            deals = self.rpc("crm.deal.list", {"filter[@ID]": sorted(selected, key=int),
                "select[]": ["ID", "STAGE_SEMANTIC_ID", "CLOSED"], "start": 0})
            if not isinstance(deals, list) or len(deals) > len(selected):
                raise ValueError("restart_discovery_deals_unavailable")
            found = set()
            for deal in deals:
                did = entity_id(deal.get("ID")) if isinstance(deal, dict) else ""
                if did not in selected or did in found:
                    raise ValueError("restart_discovery_deal_identity_unavailable")
                found.add(did)
                if deal.get("STAGE_SEMANTIC_ID") == "F" and deal.get("CLOSED") == "Y":
                    failed.add(did)
        with self.store._transaction(immediate=True) as db:
            if db.execute("SELECT value FROM lost_session_restart_meta WHERE key=?", (key,)).fetchone()[0] != raw:
                return None
            for item in candidates:
                if item["crm_type"] == "contact" or item["deal_id"] in failed:
                    db.execute("""INSERT OR IGNORE INTO lost_session_restart_watches
                        (session_id,chat_id,line_id,source,date_create,deal_id,crm_type,crm_id)
                        VALUES(?,?,?,?,?,?,?,?)""", tuple(item[k] for k in
                        ("session_id", "chat_id", "line_id", "source", "date_create", "deal_id", "crm_type", "crm_id")))
            db.execute("UPDATE lost_session_restart_meta SET value=? WHERE key=?",
                (json.dumps({**cursor, "offset": cursor["offset"] + len(rows), "complete": not more}), key))
            db.execute("UPDATE lost_session_restart_meta SET value=? WHERE key='lane'",
                       (str((lane + 1) % len(self.lanes)),))
        return {"read": len(rows), "failed_deals": len(failed)}

    def config(self, job):
        row = self.rpc("imopenlines.config.get", {"CONFIG_ID": job["line_id"]})
        if (not isinstance(row, dict) or entity_id(row.get("ID")) != job["line_id"]
                or row.get("ACTIVE") != "Y" or row.get("CRM_FORWARD") != "N"):
            raise ValueError("restart_line_not_ready")
        queue = row.get("QUEUE")
        if not isinstance(queue, list) or not queue or any(not entity_id(value) for value in queue):
            raise ValueError("restart_queue_unavailable")
        return {**{key: row.get(key) for key in ("ID", "ACTIVE", "CRM_FORWARD")},
                "QUEUE": [entity_id(value) for value in queue]}

    def session(self, job, sid=None, lower=None, upper=None):
        sid = sid or job["session_id"]
        lower = stamp(job["date_create"]) if lower is None else lower
        upper = lower if upper is None else upper
        matches = []
        for page in range(MAX_EXACT_SESSION_PAGES):
            rows, more = self.page(self.rpc("imopenlines.v2.Session.list", {
                "configId": job["line_id"], "source": job["source"],
                "dateCreateFrom": iso(lower), "dateCreateTo": iso(upper),
                "order": "dateCreate", "orderDirection": "asc", "offset": page * PAGE_SIZE, "limit": PAGE_SIZE}))
            for row in rows:
                if entity_id(row.get("id")) == sid:
                    matches.append(row)
            if not more:
                if len(matches) != 1:
                    raise ValueError("restart_exact_session_unavailable")
                item = self.identity(matches[0])
                if any(item[k] != job[k] for k in ("chat_id", "line_id", "source")):
                    raise Decision("skipped", "restart_session_identity_changed")
                return matches[0]
        raise ValueError("restart_exact_session_limit")

    def dialog(self, job, session, expected_deal=None, *, allow_contact=False):
        row = self.rpc("imopenlines.dialog.get", {"CHAT_ID": job["chat_id"]})
        if not isinstance(row, dict) or entity_id(row.get("id")) != job["chat_id"]:
            raise ValueError("restart_dialog_unavailable")
        source = str(row.get("entity_id") or "").split("|")
        data = str(row.get("entity_data_1") or "").split("|")
        pairs = str(row.get("entity_data_2") or "").split("|")
        bindings = {}
        if len(pairs) % 2:
            raise ValueError("restart_dialog_binding_unavailable")
        for offset in range(0, len(pairs), 2):
            kind, raw = pairs[offset:offset + 2]
            value = entity_id(raw, allow_zero=True)
            if kind in bindings or not value or not re.fullmatch(r"[A-Z_]+", kind):
                raise ValueError("restart_dialog_binding_unavailable")
            bindings[kind] = value
        native_kind, native_id = session.get("crmEntityType"), entity_id(session.get("crmEntityId"))
        did = bindings.get("DEAL")
        if (not entity_id(did) or (expected_deal and did != expected_deal)
                or row.get("type") != "lines" or source[:2] != [job["source"], job["line_id"]]
                or len(data) < 6 or data[5] != str(session["id"])
                or native_kind not in ({"deal", "contact"} if allow_contact else {"deal"})
                or (native_kind == "deal" and native_id != did)
                or (native_kind == "contact" and bindings.get("CONTACT") != native_id)
                or data[1:3] != [str(native_kind).upper(), native_id]):
            raise Decision("skipped", "restart_current_binding_changed")
        return {**{key: row.get(key) for key in ("id", "owner", "entity_id", "entity_data_1",
                  "entity_data_2", "last_message_id", "manager_list")},
                "bound_deal_id": did, "bound_contact_id": bindings.get("CONTACT")}

    def history(self, job, session, cutoff, *, restarted=False, expected_witness=None,
                human_cutoff=None):
        sid, cid = entity_id(session.get("id")), entity_id(session.get("userId"))
        if not sid or not cid:
            raise ValueError("restart_client_identity_unavailable")
        params = {"SESSION_ID": sid, "LIMIT": PAGE_SIZE}
        rows, seen_ids, cursors = [], set(), set()
        previous_oldest = None
        complete = False
        for _ in range(MAX_HISTORY_MESSAGES // PAGE_SIZE + 1):
            value = self.rpc("imopenlines.session.history.get", params)
            if (not isinstance(value, dict) or entity_id(value.get("chatId")) != job["chat_id"]
                    or entity_id(value.get("sessionId")) != sid
                    or not isinstance(value.get("message"), dict)
                    or not isinstance(value.get("users"), dict)
                    or type(value.get("hasNextPage")) is not bool):
                raise ValueError("restart_history_identity_unavailable")
            messages = value["message"]
            if len(messages) > PAGE_SIZE or len(seen_ids) + len(messages) > MAX_HISTORY_MESSAGES:
                raise ValueError("restart_history_limit")
            times, ordering = [], []
            for key, message in messages.items():
                mid = entity_id(message.get("id")) if isinstance(message, dict) else ""
                if (not mid or mid != str(key) or mid in seen_ids
                        or entity_id(message.get("chatid")) != job["chat_id"]):
                    raise ValueError("restart_message_identity_unavailable")
                seen_ids.add(mid)
                when = stamp(message.get("date"))
                # Native restart can move earlier messages into this session.
                # SESSION_ID + message.chatid prove membership; creation time
                # is not a valid lower bound for every inherited message.
                times.append(when)
                ordering.append((when, int(mid)))
                sender = entity_id(message.get("senderid"), allow_zero=True)
                if not sender:
                    raise ValueError("restart_sender_unavailable")
                user = value["users"].get(sender)
                role = "unknown"
                if sender == "0":
                    role = "system"
                elif isinstance(user, dict) and entity_id(user.get("id")) == sender:
                    if user.get("connector") is True and user.get("bot") is False and sender == cid:
                        role = "customer"
                    elif user.get("bot") is True:
                        role = "bot"
                    elif user.get("connector") is False and user.get("bot") is False:
                        role = "human"
                if when > cutoff:
                    rows.append({"id": mid, "at": when, "sender": sender, "role": role,
                                 "content_hash": digest(message),
                                 "source_witness": source_witness(message, sender) if role == "customer" else None})
            if ordering and previous_oldest is not None and max(ordering) >= previous_oldest:
                raise ValueError("restart_history_order_unavailable")
            if ordering:
                previous_oldest = min(ordering)
            if not value["hasNextPage"] or (times and min(times) <= cutoff):
                complete = True
                break
            cursor = (value.get("nextId"), value.get("nextDate"))
            if not entity_id(cursor[0]) or type(cursor[1]) not in (int, float) or not math.isfinite(cursor[1]) or cursor in cursors or not messages:
                raise ValueError("restart_history_cursor_unavailable")
            cursors.add(cursor)
            params = {**params, "LAST_ID": cursor[0], "LAST_DATE": cursor[1]}
        if not complete:
            raise ValueError("restart_history_incomplete")
        rows.sort(key=lambda row: (row["at"], int(row["id"])))
        customers = [row for row in rows if row["role"] == "customer"]
        if not customers:
            raise Decision("watching", "restart_no_new_customer_message")
        if expected_witness is None:
            first = customers[0]
            if first["source_witness"] is None:
                raise ValueError("restart_source_connector_identity_required")
        else:
            matches = [row for row in customers if row["source_witness"] is not None
                       and row["source_witness"]["identity_key"] == expected_witness["identity_key"]]
            if len(matches) != 1 or matches[0]["source_witness"] != expected_witness:
                raise ValueError("restart_native_source_witness_unavailable")
            first = matches[0]
        if human_cutoff is not None and (type(human_cutoff) not in (int, float)
                                         or not math.isfinite(human_cutoff)):
            raise ValueError("restart_human_cutoff_unavailable")
        # A native copy gets a later date. Human intervention remains fenced
        # from the original return, including replies during the restart RPC;
        # the copied message's timestamp cannot move that boundary forward.
        human_after = first["at"] if human_cutoff is None else human_cutoff
        if any(row["at"] >= human_after and row["role"] in {"human", "unknown"} for row in rows):
            raise Decision("skipped", "restart_later_human_or_unknown_message")
        return {"message_id": first["id"], "message_at": first["at"], "client_id": cid,
                "first_message_hash": first["content_hash"],
                "source_witness": first["source_witness"],
                "source_identity_key": first["source_witness"]["identity_key"],
                "history_hash": digest(rows), "latest_message_id": str(max(map(int, seen_ids)))}

    def proof(self, job):
        config = self.config(job)
        session = self.session(job)
        if session.get("dateClose") is not None or session["status"] in {"closed", "spam"}:
            raise Decision("skipped", "restart_session_closed")
        dialog = self.dialog(job, session, job["deal_id"] or None, allow_contact=True)
        did = dialog["bound_deal_id"]
        deal = self.rpc("crm.deal.get", {"id": did})
        if not isinstance(deal, dict) or entity_id(deal.get("ID")) != did:
            raise ValueError("restart_deal_unavailable")
        if deal.get("STAGE_SEMANTIC_ID") != "F" or deal.get("CLOSED") != "Y":
            raise Decision("skipped", "restart_deal_not_failed")
        if str(deal.get("CATEGORY_ID")) != "0":
            raise Decision("skipped", "restart_deal_outside_category")
        transition = self.prove_failed(did)
        if (not isinstance(transition, dict) or entity_id(transition.get("dealId")) != did
                or not entity_id(transition.get("transitionId")) or transition.get("toSemantic") != "F"
                or transition.get("fromSemantic") not in {"P", "S"}):
            raise ValueError("restart_failed_transition_unavailable")
        cutoff = max(float(self.meta("activated_at")), stamp(transition.get("transitionTime")))
        contacts = sorted(contact_ids(self.rpc, deal), key=int)
        if not contacts:
            raise ValueError("restart_contact_binding_unavailable")
        if entity_id(dialog.get("bound_contact_id")) not in contacts:
            raise Decision("skipped", "restart_native_contact_not_related_to_lost_deal")
        if session.get("crmEntityType") == "contact" and entity_id(session.get("crmEntityId")) not in contacts:
            raise Decision("skipped", "restart_contact_not_related_to_lost_deal")
        for cid in contacts:
            if latest_eligible_deal(self.rpc, cid) is not None:
                raise Decision("watching", "restart_active_deal_exists")
        history = self.history(job, session, cutoff)
        answer_at = session["dateOperatorAnswer"]
        # Accepting a returned chat is already human intervention, even before
        # the employee types. Equal whole-second timestamps are ambiguous and
        # cannot authorize taking the conversation away from that employee.
        if answer_at is not None and stamp(answer_at) >= history["message_at"]:
            raise Decision("skipped", "restart_source_human_accepted")
        if entity_id(dialog.get("last_message_id")) != history["latest_message_id"]:
            raise ValueError("restart_dialog_history_changed")
        return {**history, "deal_id": did, "contacts": contacts, "config": config,
                "source_id": str(deal.get("SOURCE_ID") or ""),
                "source_operator": entity_id(session.get("operatorId"), allow_zero=True),
                "source_owner": entity_id(dialog.get("owner"), allow_zero=True),
                "source_answer_at": answer_at,
                "transition_id": entity_id(transition["transitionId"]),
                "proof_hash": digest({"config": config, "session": session, "dialog": dialog,
                    "transition": transition, "contacts": contacts, "history": history,
                    "deal": {k: deal.get(k) for k in ("ID", "STAGE_ID", "STAGE_SEMANTIC_ID", "CLOSED", "MOVED_TIME")}})}

    @staticmethod
    def intake(deal):
        return (isinstance(deal, dict) and entity_id(deal.get("ID"))
                and str(deal.get("CATEGORY_ID")) == "0"
                and deal.get("STAGE_SEMANTIC_ID") == "P" and deal.get("CLOSED") == "N"
                and str(deal.get("STAGE_ID") or "").rsplit(":", 1)[-1] in EXCLUDED_STAGES)

    def target_snapshot(self, deal, contacts):
        if not self.intake(deal):
            raise Decision("blocked", "restart_target_no_longer_intake")
        actual = sorted(contact_ids(self.rpc, deal), key=int)
        if actual != sorted(contacts, key=int):
            raise Decision("blocked", "restart_target_contacts_changed")
        stamp(deal.get("DATE_CREATE"))
        stamp(deal.get("DATE_MODIFY"))
        fields = {key: deal.get(key) for key in ("ID", "CATEGORY_ID", "STAGE_ID", "STAGE_SEMANTIC_ID",
                  "CLOSED", "ASSIGNED_BY_ID", "DATE_CREATE", "DATE_MODIFY", "ORIGINATOR_ID", "ORIGIN_ID")}
        # Include dynamic claim fields without needing to know their deployed UF name.
        fields["custom_fields_hash"] = digest({key: value for key, value in deal.items() if key.startswith("UF_")})
        return {**fields, "CONTACT_IDS": actual}

    def choose_intake(self, contacts):
        opened = open_deals_for_contacts(self.rpc, contacts)
        if any(not self.intake(row) for row in opened):
            raise Decision("watching", "restart_active_deal_exists")
        if len(opened) > 1:
            raise Decision("watching", "restart_multiple_intakes")
        if opened:
            self.target_snapshot(opened[0], contacts)
            return opened[0]
        return None

    def persist(self, job, **fields):
        allowed = set(NEW_COLUMNS) | {"state", "deal_id", "message_id", "message_at", "client_id",
                    "transition_id", "proof_hash", "dispatched", "dispatch_at", "new_session_id", "new_deal_id"}
        if not fields or set(fields) - allowed:
            raise ValueError("restart_invalid_persist_fields")
        with self.store._transaction(immediate=True) as db:
            changed = db.execute("UPDATE lost_session_restart_watches SET " + ",".join(key + "=?" for key in fields)
                + " WHERE session_id=? AND lease_token=? AND lease_until>?",
                (*fields.values(), job["session_id"], job["lease_token"], self.time())).rowcount
            if changed != 1:
                raise ValueError("restart_lease_lost")
        job.update(fields)

    def reserve(self, job, proof, existing):
        if (not isinstance(proof.get("source_witness"), dict)
                or proof["source_witness"].get("identity_key") != proof.get("source_identity_key")
                or proof["source_witness"].get("sender") != proof.get("client_id")):
            raise ValueError("restart_source_witness_required")
        marker = "v1:" + digest([proof["contacts"], job["chat_id"], proof["message_id"]])
        target = self.target_snapshot(existing, proof["contacts"]) if existing else {}
        with self.store._transaction(immediate=True) as db:
            consumed = db.execute("""SELECT session_id FROM lost_session_restart_watches
                WHERE chat_id=? AND (message_id=? OR source_identity_key=?)
                AND (reserved=1 OR dispatched=1) LIMIT 1""",
                (job["chat_id"], proof["message_id"], proof["source_identity_key"])).fetchone()
            if consumed:
                raise Decision("skipped", "restart_message_already_dispatched")
            for cid in proof["contacts"]:
                held = db.execute("SELECT session_id FROM lost_session_restart_contact_fences WHERE contact_id=?", (cid,)).fetchone()
                if held and held[0] != job["session_id"]:
                    raise Decision("watching", "restart_contact_saga_pending")
            values = {"reserved": 1, "deal_id": proof["deal_id"], "message_id": proof["message_id"],
                "message_at": proof["message_at"], "client_id": proof["client_id"],
                "transition_id": proof["transition_id"], "proof_hash": proof["proof_hash"],
                "contacts_json": json.dumps(proof["contacts"]), "origin_id": marker,
                "source_identity_key": proof["source_identity_key"],
                "source_proof_json": json.dumps(proof, sort_keys=True),
                "target_deal_id": target.get("ID", ""), "target_json": json.dumps(target, sort_keys=True)}
            changed = db.execute("UPDATE lost_session_restart_watches SET " + ",".join(k + "=?" for k in values)
                + " WHERE session_id=? AND lease_token=? AND reserved=0 AND lease_until>?",
                (*values.values(), job["session_id"], job["lease_token"], self.time())).rowcount
            if changed != 1:
                raise ValueError("restart_reservation_lease_lost")
            for cid in proof["contacts"]:
                db.execute("INSERT OR IGNORE INTO lost_session_restart_contact_fences VALUES(?,?)", (cid, job["session_id"]))
        job.update(values)

    def find_created(self, job):
        rows = self.rpc("crm.deal.list", {"filter[ORIGINATOR_ID]": ORIGINATOR,
            "filter[ORIGIN_ID]": job["origin_id"], "select[]": ["ID"], "start": 0})
        if not isinstance(rows, list) or len(rows) > 1:
            raise ValueError("restart_origin_lookup_ambiguous")
        if not rows:
            return None
        did = entity_id(rows[0].get("ID")) if isinstance(rows[0], dict) else ""
        if not did:
            raise ValueError("restart_origin_lookup_unavailable")
        deal = self.rpc("crm.deal.get", {"id": did})
        proof = json.loads(job["source_proof_json"])
        if (not isinstance(deal, dict) or entity_id(deal.get("ID")) != did
                or deal.get("ORIGINATOR_ID") != ORIGINATOR or deal.get("ORIGIN_ID") != job["origin_id"]
                or str(deal.get("CATEGORY_ID")) != "0"
                or stamp(deal.get("DATE_CREATE")) < math.floor(job["create_at"])):
            raise ValueError("restart_created_deal_identity_changed")
        contacts = json.loads(job["contacts_json"])
        if sorted(contact_ids(self.rpc, deal), key=int) != contacts:
            raise ValueError("restart_created_deal_contacts_changed")
        identity = {key: deal.get(key) for key in ("ID", "CATEGORY_ID", "DATE_CREATE", "ORIGINATOR_ID", "ORIGIN_ID")}
        identity["CONTACT_IDS"] = contacts
        # The add may have committed before a timeout, then a manager may have
        # claimed it before this first read. First persist what was created;
        # eligibility is a separate decision and must not keep its contact
        # fenced forever merely because the native owner/stage already changed.
        self.persist(job, target_deal_id=did, target_identity_json=json.dumps(identity, sort_keys=True))
        if entity_id(deal.get("ASSIGNED_BY_ID")) != proof["config"]["QUEUE"][0]:
            raise Decision("blocked", "restart_created_deal_claimed")
        self.target_snapshot(deal, contacts)
        return deal

    def ensure_target(self, job):
        if job["target_deal_id"] and json.loads(job["target_json"]).get("ID") == job["target_deal_id"]:
            return
        if job["create_dispatched"]:
            target = self.find_created(job)
            if target is None:
                raise Decision("uncertain", "restart_add_result_uncertain")
        else:
            contacts = json.loads(job["contacts_json"])
            # Give a native CRM creator every chance to win the race. Any new
            # open deal is re-evaluated; we never add on a stale empty snapshot.
            existing = self.choose_intake(contacts)
            if existing:
                target = existing
            else:
                source = json.loads(job["source_proof_json"])
                self.fresh_source(job)
                if open_deals_for_contacts(self.rpc, contacts):
                    raise ValueError("restart_open_set_changed_before_add")
                self.persist(job, create_dispatched=1, create_at=self.time(), state="creating")
                fields = {"TITLE": "Повторное обращение", "CATEGORY_ID": 0,
                    "STAGE_ID": "UC_ZJ55BR", "CONTACT_IDS": contacts,
                    "ASSIGNED_BY_ID": source["config"]["QUEUE"][0],
                    "ORIGINATOR_ID": ORIGINATOR, "ORIGIN_ID": job["origin_id"]}
                if source["source_id"]:
                    fields["SOURCE_ID"] = source["source_id"]
                try:
                    # app.bitrix_call form-encodes flat keys with doseq=True.
                    payload = {f"fields[{key}]" + ("[]" if isinstance(value, list) else ""): value
                               for key, value in fields.items()}
                    self.call("crm.deal.add", payload, timeout=8)
                except Exception:
                    pass
                target = self.find_created(job)
                if target is None:
                    raise Decision("uncertain", "restart_add_result_uncertain")
        snapshot = self.target_snapshot(target, json.loads(job["contacts_json"]))
        self.persist(job, target_deal_id=snapshot["ID"], target_json=json.dumps(snapshot, sort_keys=True), state="target_ready")

    def fresh_source(self, job):
        before = json.loads(job["source_proof_json"])
        current = self.proof(job)
        if any(current.get(key) != before.get(key) for key in (
                "message_id", "message_at", "client_id", "transition_id", "deal_id", "contacts",
                "config", "first_message_hash", "source_identity_key", "source_witness",
                "source_operator", "source_owner", "source_answer_at")):
            raise Decision("blocked", "restart_source_changed_after_reservation")
        return current

    def check_target(self, job):
        contacts = json.loads(job["contacts_json"])
        opened = open_deals_for_contacts(self.rpc, contacts)
        if len(opened) != 1 or entity_id(opened[0].get("ID")) != job["target_deal_id"]:
            raise Decision("blocked", "restart_target_open_set_changed")
        snapshot = self.target_snapshot(opened[0], contacts)
        if snapshot != json.loads(job["target_json"]):
            raise Decision("blocked", "restart_target_claimed_or_changed")
        source = json.loads(job["source_proof_json"])
        if self.config(job) != source["config"]:
            raise Decision("blocked", "restart_config_changed")
        return snapshot

    def current_target_session(self, job):
        """Prove native target binding and unchanged input, without writing."""
        dialog = self.rpc("imopenlines.dialog.get", {"CHAT_ID": job["chat_id"]})
        data = str(dialog.get("entity_data_1") or "").split("|") if isinstance(dialog, dict) else []
        sid = entity_id(data[5]) if len(data) > 5 else ""
        if not sid or sid == job["session_id"]:
            return None
        session = self.session(job, sid, job["message_at"] - 1, self.time() + 1)
        did = entity_id(session.get("crmEntityId")) if session.get("crmEntityType") == "deal" else ""
        if not did or did != job["target_deal_id"] or entity_id(session.get("userId")) != job["client_id"]:
            return None
        native = self.dialog(job, session, did)
        if session.get("dateClose") is not None or session["status"] not in {"new", "answered", "paused"}:
            return None
        # The method deliberately moves an existing message into a new session;
        # its original timestamp may precede that new session's creation time.
        baseline = json.loads(job["source_proof_json"])
        witness = baseline.get("source_witness")
        if not isinstance(witness, dict) or witness.get("identity_key") != job["source_identity_key"]:
            return None
        # The copied customer message has a different native ID/date. Match one
        # exact external origin + sender + authored-body witness in the complete
        # bounded new-session history; same text alone is never sufficient.
        history = self.history(job, session, -math.inf, restarted=True, expected_witness=witness,
                               human_cutoff=job["message_at"])
        if entity_id(native.get("last_message_id")) != history["latest_message_id"]:
            return None
        return {"session": session, "dialog": native, "history": history,
                "new_session_id": sid, "new_deal_id": did, "target_message_id": history["message_id"]}

    def queue_confirmed(self, job):
        native = self.current_target_session(job)
        if native is None:
            return None
        self.check_target(job)
        session, dialog = native["session"], native["dialog"]
        if (session["status"] != "new" or session.get("operatorId") is not None
                or session.get("dateOperatorAnswer") is not None
                or entity_id(dialog.get("owner"), allow_zero=True) != "0"
                or dialog.get("manager_list") != []):
            return None
        return {key: native[key] for key in ("new_session_id", "new_deal_id", "target_message_id")}

    def queue_proof(self, job):
        self.check_target(job)
        source = json.loads(job["source_proof_json"])
        transition = self.prove_failed(job["deal_id"])
        if (entity_id(transition.get("transitionId")) != job["transition_id"]
                or transition.get("dealId") != job["deal_id"]):
            raise Decision("blocked", "restart_loss_changed_before_queue")
        native = self.current_target_session(job)
        if native is None:
            raise Decision("uncertain", "restart_native_target_binding_pending")
        session, dialog = native["session"], native["dialog"]
        operator = entity_id(session.get("operatorId"), allow_zero=True) if session.get("operatorId") is not None else "0"
        owner = entity_id(dialog.get("owner"), allow_zero=True)
        allowed_owners = {"0", source["source_owner"], source["config"]["QUEUE"][0], self.bot_id}
        if operator not in {"0", source["source_operator"], source["config"]["QUEUE"][0], self.bot_id} or owner not in allowed_owners:
            raise Decision("blocked", "restart_operator_changed_before_queue")
        if session.get("dateOperatorAnswer") is not None:
            raise Decision("blocked", "restart_human_accepted_before_queue")
        managers = dialog.get("manager_list")
        if not isinstance(managers, list) or any(entity_id(value) not in allowed_owners for value in managers):
            raise Decision("blocked", "restart_manager_changed_before_queue")
        return native

    def finish(self, job, state, code, **extra):
        allowed = {"new_session_id", "new_deal_id", "target_message_id"}
        if set(extra) - allowed:
            raise ValueError("restart_invalid_result_fields")
        delay = (min(300, 30 * (2 ** min(int(job.get("attempts") or 0), 4)))
                 if code in {"restart_preflight_unavailable", "restart_result_uncertain", "restart_ack_unconfirmed"}
                 else 30)
        fields = {"state": state, "error_code": code, "next_at": self.time() + delay,
                  "lease_token": "", "lease_until": 0, **extra}
        with self.store._transaction(immediate=True) as db:
            changed = db.execute("UPDATE lost_session_restart_watches SET " + ",".join(k + "=?" for k in fields)
                       + " WHERE session_id=? AND lease_token=?",
                       (*fields.values(), job["session_id"], job["lease_token"])).rowcount
            # Keep unresolved add intents fenced until an exact target is known.
            # A proven terminal handoff/claim must not block the client's next
            # independent return forever. The message reservation is permanent.
            known_target = bool(job.get("target_deal_id") and any(
                json.loads(job.get(key) or "{}").get("ID") == job["target_deal_id"]
                for key in ("target_json", "target_identity_json")))
            release = state == "restarted" or (state in {"blocked", "skipped"}
                and (not job.get("create_dispatched") or known_target))
            if changed and release:
                db.execute("DELETE FROM lost_session_restart_contact_fences WHERE session_id=?", (job["session_id"],))
        return {"session_id": job["session_id"], "state": state, "code": code, **extra}

    def process(self):
        token = uuid.uuid4().hex
        with self.store._transaction(immediate=True) as db:
            row = db.execute("""SELECT * FROM lost_session_restart_watches
                WHERE state IN ('watching','checking','creating','target_ready','dispatching','queue_dispatching','uncertain')
                AND next_at<=? AND lease_until<=? ORDER BY next_at,CAST(session_id AS INTEGER) LIMIT 1""",
                (self.time(), self.time())).fetchone()
            if row is None:
                return None
            job = dict(row)
            db.execute("UPDATE lost_session_restart_watches SET state=?,lease_token=?,lease_until=?,attempts=attempts+1 WHERE session_id=?",
                       ("uncertain" if job["dispatched"] else "checking", token, self.time() + 180, job["session_id"]))
            job["lease_token"] = token
        try:
            # Legacy uncertain reset-only jobs have no target/intake intent.
            # Do not retrofit creation or transfers into an unproven operation.
            if job["dispatched"] and not job["reserved"]:
                return self.finish(job, "uncertain", "restart_legacy_intent_requires_review")
            if job["reserved"] and (not job["source_identity_key"]
                    or not isinstance(json.loads(job["source_proof_json"]).get("source_witness"), dict)):
                return self.finish(job, "uncertain", "restart_legacy_source_witness_requires_review")
            if not job["reserved"]:
                first = self.proof(job)
                contact_locks = ["contact:" + cid for cid in first["contacts"]]
                with self.locks.hold(contact_locks[0], first["deal_id"], additional_manager_ids=contact_locks[1:]):
                    second = self.proof(job)
                    if first != second:
                        raise ValueError("restart_proof_changed")
                    existing = self.choose_intake(first["contacts"])
                    if existing is None and self.time() - first["message_at"] < NATIVE_CREATE_GRACE_SECONDS:
                        raise Decision("watching", "restart_waiting_native_creator")
                    self.reserve(job, first, existing)
            contacts = json.loads(job["contacts_json"])
            keys = ["contact:" + cid for cid in contacts]
            if not keys:
                raise ValueError("restart_contact_fence_unavailable")
            with self.locks.hold(keys[0], job["target_deal_id"] or job["deal_id"], additional_manager_ids=keys[1:]):
                self.ensure_target(job)
            # Reacquire the target deal lock, shared with native claim handlers.
            # Never nest claim-lock contexts or mutate a target under only the
            # old lost deal's lock.
            with self.locks.hold(keys[0], job["target_deal_id"], additional_manager_ids=keys[1:]):
                if job["queue_dispatched"]:
                    confirmed = self.queue_confirmed(job)
                    return self.finish(job, "restarted" if confirmed else "uncertain",
                        "restart_queue_confirmed" if confirmed else "restart_queue_result_uncertain", **(confirmed or {}))
                if not job["dispatched"]:
                    first = self.fresh_source(job)
                    self.check_target(job)
                    second = self.fresh_source(job)
                    self.check_target(job)
                    if first != second:
                        raise ValueError("restart_proof_changed")
                    self.persist(job, dispatched=1, dispatch_at=self.time(), state="dispatching")
                    try:
                        self.call("imopenlines.message.session.start",
                            {"CHAT_ID": job["chat_id"], "MESSAGE_ID": job["message_id"]}, timeout=8)
                    except Exception:
                        pass
                first = self.queue_proof(job)
                second = self.queue_proof(job)
                if first != second:
                    raise ValueError("restart_queue_proof_changed")
                already = self.queue_confirmed(job)
                if already:
                    return self.finish(job, "restarted", "restart_native_queue_confirmed", **already)
                self.persist(job, queue_dispatched=1, queue_at=self.time(), state="queue_dispatching")
                try:
                    self.call("imopenlines.bot.session.transfer", {
                        "CHAT_ID": job["chat_id"], "QUEUE_ID": job["line_id"],
                        "CLIENT_ID": self.bot_client_id}, timeout=8)
                except Exception:
                    pass
                confirmed = self.queue_confirmed(job)
                return self.finish(job, "restarted" if confirmed else "uncertain",
                    "restart_queue_confirmed" if confirmed else "restart_queue_result_uncertain", **(confirmed or {}))
        except Decision as decision:
            return self.finish(job, decision.state, decision.code)
        except Exception:
            mutated = job["dispatched"] or job["create_dispatched"] or job["queue_dispatched"]
            return self.finish(job, "uncertain" if mutated else "watching",
                               "restart_result_uncertain" if mutated else "restart_preflight_unavailable")

    def tick(self):
        if not self.running.acquire(blocking=False):
            return None
        try:
            try:
                discovery = self.poll()
            except Exception:
                discovery = {"error": "restart_discovery_unavailable"}
            results = []
            for _ in range(WATCH_BUDGET):
                outcome = self.process()
                if outcome is None:
                    break
                results.append(outcome)
            return {
                "read": int((discovery or {}).get("read", 0)),
                "failed_deals": int((discovery or {}).get("failed_deals", 0)),
                "processed": len(results),
                "restarted": sum(item["state"] == "restarted" for item in results),
                "uncertain": sum(item["state"] == "uncertain" for item in results),
                "skipped": sum(item["state"] == "skipped" for item in results),
                "blocked": sum(item["state"] == "blocked" for item in results),
                "watching": sum(item["state"] == "watching" for item in results),
                "errors": int(bool((discovery or {}).get("error"))) + sum(
                    item["code"] == "restart_preflight_unavailable" for item in results),
            }
        finally:
            self.running.release()
