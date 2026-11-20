"""Observe future CRM changes and enqueue the shared contact-owner writer.

The fixed modification window and keyset cursor survive restarts. A page and its
durable observations commit together; individual read failures use a separate
retry queue and cannot advance or erase another observation. This module never
writes to Bitrix24.
"""
import hashlib
import json
import logging
import math
import threading
from datetime import datetime, timezone

from contact_owner_sync import entity_id
from contact_owner_policy import EXCLUDED_STAGES, eligible_deal, contact_ids, latest_eligible_deal

LOG = logging.getLogger(__name__)
PAGE_SIZE = 50
OVERLAP_SECONDS = 60
SETTLE_SECONDS = 2
READ_METHODS = {"crm.deal.list", "crm.deal.get", "crm.deal.contact.items.get",
                "crm.contact.get", "crm.item.list"}
FIELDS = ("ID", "DATE_MODIFY", "DATE_CREATE", "ASSIGNED_BY_ID", "CONTACT_ID",
          "STAGE_ID", "STAGE_SEMANTIC_ID", "CLOSED")
SCHEMA = """
CREATE TABLE IF NOT EXISTS crm_contact_owner_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS crm_contact_owner_observations(
 deal_id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,source_modified REAL NOT NULL,
 revision INTEGER NOT NULL DEFAULT 1,done_revision INTEGER NOT NULL DEFAULT 0,
 received_at REAL NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,
 next_at REAL NOT NULL DEFAULT 0,error_code TEXT NOT NULL DEFAULT '');
CREATE INDEX IF NOT EXISTS crm_contact_owner_due
 ON crm_contact_owner_observations(next_at,received_at);
"""


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("crm_modification_time_unavailable")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError()
        result = parsed.timestamp()
        if not math.isfinite(result):
            raise ValueError()
        return result
    except (ValueError, OverflowError, OSError):
        raise ValueError("crm_modification_time_unavailable") from None


def iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def settled_by_row(row):
    """True when the list row itself proves the deal cannot name a contact owner.

    Only explicit, well-formed evidence counts: a closed deal, a finished stage
    or one of the two intake stages. A missing or unexpected value proves
    nothing, so that row keeps the full deal read. Skipping the queue here keeps
    a bulk edit of old deals from delaying a manager change made minutes ago.
    """
    stage = row.get("STAGE_ID")
    return (row.get("CLOSED") == "Y"
            or row.get("STAGE_SEMANTIC_ID") in ("S", "F")
            or (isinstance(stage, str) and bool(stage) and stage.strip() == stage
                and stage.rsplit(":", 1)[-1] in EXCLUDED_STAGES))


class CrmContactOwnerSync:
    def __init__(self, store, call, contacts, marker_field, *, now=None):
        self.store, self.call, self.contacts = store, call, contacts
        self.marker_field = marker_field
        self.clock = now or (lambda: datetime.now(timezone.utc))
        self.running = threading.Lock()
        if not isinstance(marker_field, str) or not marker_field:
            raise ValueError("crm_contact_marker_required")
        store._ensure_ready()
        activated = math.floor(self.time())
        with store._transaction(immediate=True) as db:
            for statement in SCHEMA.split(";"):
                if statement.strip():
                    db.execute(statement)
            db.execute("INSERT OR IGNORE INTO crm_contact_owner_meta VALUES('activated_at',?)", (str(activated),))
            # The first run starts at activation, never at the oldest CRM row.
            activation = float(db.execute("SELECT value FROM crm_contact_owner_meta WHERE key='activated_at'").fetchone()[0])
            db.execute("INSERT OR IGNORE INTO crm_contact_owner_meta VALUES('cursor',?)",
                       (json.dumps({"from": activation, "to": activation, "after": 0, "complete": True}),))

    def time(self):
        value = self.clock()
        result = value.timestamp() if isinstance(value, datetime) else float(value)
        if not math.isfinite(result):
            raise ValueError("crm_clock_unavailable")
        return result

    def rpc(self, method, params, timeout=8):
        if method not in READ_METHODS:
            raise ValueError("crm_scanner_write_forbidden")
        return self.call(method, params, timeout=8)

    def meta(self, key):
        with self.store._connect() as db:
            return db.execute("SELECT value FROM crm_contact_owner_meta WHERE key=?", (key,)).fetchone()[0]

    def fingerprint(self, deal):
        values = []
        for field in (*FIELDS, self.marker_field):
            value = deal.get(field)
            if isinstance(value, (dict, list)) or (value is not None and len(str(value)) > 4096):
                raise ValueError("crm_deal_field_unavailable")
            values.append([field, value])
        return digest(values)

    def poll(self):
        """Read at most one page. Return None if no new time window is ready."""
        with self.store._transaction(immediate=True) as db:
            raw = db.execute("SELECT value FROM crm_contact_owner_meta WHERE key='cursor'").fetchone()[0]
            cursor = json.loads(raw)
            activation = float(db.execute("SELECT value FROM crm_contact_owner_meta WHERE key='activated_at'").fetchone()[0])
            if cursor["complete"]:
                end = math.floor(self.time()) - SETTLE_SECONDS
                if end <= cursor["to"]:
                    return None
                cursor = {"from": max(activation, cursor["to"] - OVERLAP_SECONDS),
                          "to": end, "after": 0, "complete": False}
                raw = json.dumps(cursor)
                db.execute("UPDATE crm_contact_owner_meta SET value=? WHERE key='cursor'", (raw,))
        page = self.rpc("crm.deal.list", {
            "filter[>=DATE_MODIFY]": iso(cursor["from"]),
            "filter[<=DATE_MODIFY]": iso(cursor["to"]),
            "filter[>ID]": cursor["after"], "order[ID]": "ASC", "start": -1,
            "select[]": [*FIELDS, self.marker_field],
        })
        if not isinstance(page, list) or len(page) > PAGE_SIZE:
            raise ValueError("crm_modified_page_unavailable")
        validated, previous = [], cursor["after"]
        for row in page:
            if not isinstance(row, dict) or not entity_id(row.get("ID")):
                raise ValueError("crm_modified_identity_unavailable")
            did = entity_id(row["ID"])
            modified = timestamp(row.get("DATE_MODIFY"))
            if int(did) <= previous or not cursor["from"] <= modified <= cursor["to"]:
                raise ValueError("crm_modified_filter_unavailable")
            validated.append((did, self.fingerprint(row), modified, settled_by_row(row)))
            previous = int(did)
        queued = 0
        with self.store._transaction(immediate=True) as db:
            # Another instance may have already committed this page. It is safe
            # to re-read later, but never replace its newer durable cursor.
            if db.execute("SELECT value FROM crm_contact_owner_meta WHERE key='cursor'").fetchone()[0] != raw:
                return None
            for did, fingerprint, modified, settled in validated:
                old = db.execute("SELECT fingerprint,source_modified FROM crm_contact_owner_observations WHERE deal_id=?", (did,)).fetchone()
                if old and (old["fingerprint"] == fingerprint or old["source_modified"] > modified):
                    continue
                db.execute("""INSERT INTO crm_contact_owner_observations(deal_id,fingerprint,source_modified,received_at)
                    VALUES(?,?,?,?) ON CONFLICT(deal_id) DO UPDATE SET
                    fingerprint=excluded.fingerprint,source_modified=excluded.source_modified,
                    revision=revision+1,received_at=excluded.received_at,attempts=0,next_at=0,error_code=''""",
                           (did, fingerprint, modified, self.time()))
                if settled:
                    # The newest snapshot supersedes any earlier pending one:
                    # the full read would reach the same "ineligible" outcome.
                    db.execute("""UPDATE crm_contact_owner_observations
                        SET done_revision=revision,error_code='ineligible' WHERE deal_id=?""", (did,))
                    continue
                queued += 1
            updated = {**cursor, "after": previous, "complete": len(page) < PAGE_SIZE}
            db.execute("UPDATE crm_contact_owner_meta SET value=? WHERE key='cursor'", (json.dumps(updated),))
        return {"read": len(page), "queued": queued}

    def _derive(self, job):
        deal = self.rpc("crm.deal.get", {"id": job["deal_id"]})
        if not isinstance(deal, dict) or entity_id(deal.get("ID")) != job["deal_id"]:
            raise ValueError("crm_deal_identity_unavailable")
        modified = timestamp(deal.get("DATE_MODIFY"))
        if modified < float(self.meta("activated_at")):
            raise ValueError("crm_deal_snapshot_predates_activation")
        if not eligible_deal(deal):
            return "ineligible", 0
        source_fingerprint = self.fingerprint(deal)
        queued = 0
        for cid in contact_ids(self.rpc, deal):
            winner = latest_eligible_deal(self.rpc, cid)
            if winner is None:
                continue
            if not eligible_deal(winner):
                raise ValueError("crm_winner_ineligible")
            owner = entity_id(winner.get("ASSIGNED_BY_ID"))
            did = entity_id(winner.get("ID"))
            marker = winner.get(self.marker_field) or ""
            if not owner or not did or not isinstance(marker, str):
                raise ValueError("crm_winner_identity_unavailable")
            started = iso(self.time())
            contact = self.rpc("crm.contact.get", {"id": cid})
            previous = entity_id(contact.get("ASSIGNED_BY_ID"), allow_zero=True) if isinstance(contact, dict) else ""
            if not isinstance(contact, dict) or entity_id(contact.get("ID")) != cid or not previous:
                raise ValueError("crm_contact_identity_unavailable")
            if previous == owner:
                continue
            operation = "crm:" + digest([job["deal_id"], source_fingerprint, cid, self.fingerprint(winner)])
            try:
                self.contacts.reserve_crm(operation, did, owner, cid, previous, marker,
                                          snapshot_started_at=started)
                queued += 1
            except ValueError as error:
                # Replaying a completed observation must not turn a manual
                # contact edit into a new generation with another before value.
                if str(error) != "crm_contact_intent_conflict":
                    raise
        return "queued" if queued else "unchanged", queued

    def process(self):
        with self.store._connect() as db:
            row = db.execute("""SELECT * FROM crm_contact_owner_observations
                WHERE revision>done_revision AND next_at<=?
                ORDER BY next_at,received_at,CAST(deal_id AS INTEGER) LIMIT 1""", (self.time(),)).fetchone()
        if row is None:
            return None
        job = dict(row)
        try:
            outcome, queued = self._derive(job)
        except Exception:
            with self.store._transaction(immediate=True) as db:
                db.execute("""UPDATE crm_contact_owner_observations SET attempts=attempts+1,next_at=?,error_code='crm_source_unavailable'
                    WHERE deal_id=? AND revision=?""",
                           (self.time() + min(300, 15 * (job["attempts"] + 1)), job["deal_id"], job["revision"]))
            LOG.warning("CRM contact source read unavailable")
            return {"processed": 0, "queued": 0, "errors": 1}
        with self.store._transaction(immediate=True) as db:
            db.execute("""UPDATE crm_contact_owner_observations SET done_revision=MAX(done_revision,?),
                attempts=CASE WHEN revision=? THEN 0 ELSE attempts END,
                next_at=CASE WHEN revision=? THEN 0 ELSE next_at END,
                error_code=CASE WHEN revision=? THEN ? ELSE error_code END WHERE deal_id=?""",
                       (job["revision"], job["revision"], job["revision"], job["revision"], outcome, job["deal_id"]))
        return {"processed": 1, "queued": queued, "errors": 0}

    def status(self):
        with self.store._connect() as db:
            pending = db.execute("SELECT count(*) FROM crm_contact_owner_observations WHERE revision>done_revision").fetchone()[0]
            failed = db.execute("SELECT count(*) FROM crm_contact_owner_observations WHERE revision>done_revision AND attempts>0").fetchone()[0]
        return {"activatedAt": iso(float(self.meta("activated_at"))), "pending": pending, "retrying": failed}

    def run(self):
        if not self.running.acquire(blocking=False):
            return None
        summary = {"pages": 0, "observed": 0, "processed": 0, "queued": 0, "errors": 0}
        try:
            for _ in range(2):
                try:
                    result = self.poll()
                except Exception:
                    LOG.warning("CRM contact observation page unavailable")
                    summary["errors"] += 1
                    break
                if result is None:
                    break
                summary["pages"] += 1
                summary["observed"] += result["queued"]
            # Polling failures never prevent earlier durable work from retrying.
            for _ in range(5):
                result = self.process()
                if result is None:
                    break
                for key, value in result.items():
                    summary[key] += value
            return summary
        finally:
            self.running.release()
