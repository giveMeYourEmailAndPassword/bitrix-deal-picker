"""Atomic request ownership across duplicate CRM cards.

Provider reads live in request_claim_evidence.py/app.py. This module accepts only
server-generated evidence, validates it again inside the SQLite transaction, and
never calls Bitrix, sends messages, or changes CRM data. Phone identities are
hashed by the provider reader; no phone numbers belong in this journal.
"""
import json
import re
from datetime import datetime, timezone


SCHEMA = """
CREATE TABLE IF NOT EXISTS request_claim_allocations (
    operation_key TEXT PRIMARY KEY,
    deal_id TEXT NOT NULL,
    manager_id TEXT NOT NULL,
    claim_marker TEXT NOT NULL DEFAULT '',
    inbound_event_key TEXT NOT NULL DEFAULT '',
    inbound_at TEXT NOT NULL DEFAULT '',
    identity_keys_json TEXT NOT NULL DEFAULT '[]',
    predecessors_json TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL CHECK(status IN ('reserved','succeeded','released')),
    confirmed INTEGER NOT NULL DEFAULT 0 CHECK(confirmed IN (0,1)),
    legacy INTEGER NOT NULL DEFAULT 0 CHECK(legacy IN (0,1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_request_claim_inbound_once
    ON request_claim_allocations(inbound_event_key)
    WHERE inbound_event_key!='' AND (status!='released' OR confirmed=1);
CREATE TABLE IF NOT EXISTS request_claim_aliases (
    identity_key TEXT NOT NULL,
    operation_key TEXT NOT NULL REFERENCES request_claim_allocations(operation_key),
    PRIMARY KEY(identity_key,operation_key)
);
CREATE INDEX IF NOT EXISTS idx_claim_operations_request_contact
    ON claim_operations(json_extract(request_json,'$.contactOwnerContext.contactId'))
    WHERE json_valid(request_json);
CREATE INDEX IF NOT EXISTS idx_request_claim_alias_operation
    ON request_claim_aliases(operation_key);
"""


class RequestClaimEvidenceError(ValueError):
    def __init__(self, code='request_identity_unavailable'):
        self.code = code
        super().__init__(code)


class RequestClaimConflictError(ValueError):
    def __init__(self, code='request_already_claimed', deal_id=''):
        self.code, self.deal_id = code, str(deal_id)
        super().__init__(code)


def _id(value):
    return isinstance(value, str) and bool(re.fullmatch(r'[1-9]\d{0,19}', value))


def _time(value):
    if not isinstance(value, str) or not value or len(value) > 40:
        raise RequestClaimEvidenceError()
    try:
        result = datetime.fromisoformat(value[:-1] + '+00:00' if value.endswith('Z') else value)
        if result.utcoffset() is None:
            raise ValueError()
        return result.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        raise RequestClaimEvidenceError() from None


def identity_keys(values):
    if not isinstance(values, list) or not 1 <= len(values) <= 16:
        raise RequestClaimEvidenceError()
    if any(not isinstance(v, str) or not re.fullmatch(
            r'(?:contact|chat):[1-9]\d{0,19}|phone:[a-f0-9]{64}', v) for v in values):
        raise RequestClaimEvidenceError()
    if len(set(values)) != len(values):
        raise RequestClaimEvidenceError()
    return sorted(values)


def validate_context(context, now, *, allow_stale=False):
    if not isinstance(context, dict) or set(context) != {
            'identityKeys', 'inboundEventKey', 'inboundAt', 'checkedPrior', 'observedAt'}:
        raise RequestClaimEvidenceError()
    keys = identity_keys(context['identityKeys'])
    observed, current = _time(context['observedAt']), _time(now)
    age = (current - observed).total_seconds()
    if age < -30 or (not allow_stale and age > 300):
        raise RequestClaimEvidenceError('request_evidence_stale')
    event, inbound = context['inboundEventKey'], context['inboundAt']
    if not isinstance(event, str) or (event and not re.fullmatch(r'[a-f0-9]{64}', event)):
        raise RequestClaimEvidenceError()
    if bool(event) != bool(inbound) or not isinstance(inbound, str):
        raise RequestClaimEvidenceError()
    inbound_time = _time(inbound) if event else None
    if inbound_time is not None and (inbound_time - observed).total_seconds() > 5:
        raise RequestClaimEvidenceError()
    prior = context['checkedPrior']
    if not isinstance(prior, list) or len(prior) > 100:
        raise RequestClaimEvidenceError()
    closed = {}
    for item in prior:
        if not isinstance(item, dict) or set(item) != {'dealId', 'closedAt'} or not _id(item['dealId']):
            raise RequestClaimEvidenceError()
        if item['dealId'] in closed:
            raise RequestClaimEvidenceError()
        closed_time = _time(item['closedAt'])
        if closed_time > observed:
            raise RequestClaimEvidenceError()
        closed[item['dealId']] = closed_time
    return keys, event, inbound_time, closed


def _json(value):
    try:
        parsed = json.loads(value or '{}')
        return parsed if isinstance(parsed, dict) else {}
    except (ValueError, TypeError):
        raise RequestClaimEvidenceError() from None


def _unapplied(result):
    return result.get('remoteUpdated') is False and not result.get('remoteUpdateUncertain')


def list_conflicts(connection, keys, except_operation_key=''):
    """Read current fences and pre-release primary-contact claim evidence.

    Old native ownership jobs are evidence only: this reader never acquires,
    cancels, or changes their workflow. Old claims without a stored identity must
    additionally be found by the provider-side CRM sibling-card lookup.
    """
    keys = identity_keys(keys)
    marks = ','.join('?' for _ in keys)
    result = {}
    rows = connection.execute(f"""SELECT DISTINCT a.* FROM request_claim_allocations a
        JOIN request_claim_aliases k ON k.operation_key=a.operation_key
        WHERE k.identity_key IN ({marks}) AND a.operation_key!=? AND a.status!='released'""",
        (*keys, str(except_operation_key))).fetchall()
    for row in rows:
        result[row['operation_key']] = {
            'operationKey': row['operation_key'], 'dealId': row['deal_id'],
            'managerId': row['manager_id'], 'status': row['status'],
            'inboundEventKey': row['inbound_event_key'], 'inboundAt': row['inbound_at'],
            'legacy': bool(row['legacy']), 'claimedAt': row['created_at'],
        }
    contacts = [key.split(':', 1)[1] for key in keys if key.startswith('contact:')]
    if not contacts:
        return list(result.values())
    cmarks = ','.join('?' for _ in contacts)
    # Prefer authoritative operation state over the companion contact job.
    operations = connection.execute(f"""SELECT o.* FROM claim_operations o
        WHERE json_valid(o.request_json)
          AND json_extract(o.request_json,'$.contactOwnerContext.contactId') IN ({cmarks})
          AND o.operation_key!=?
          AND NOT EXISTS(SELECT 1 FROM request_claim_allocations a WHERE a.operation_key=o.operation_key)""",
        (*contacts, str(except_operation_key))).fetchall()
    operation_ids = set()
    for row in operations:
        operation_ids.add(row['operation_key'])
        if row['status'] == 'failed' and _unapplied(_json(row['result_json'])):
            continue
        result[row['operation_key']] = {
            'operationKey': row['operation_key'], 'dealId': row['deal_id'],
            'managerId': row['manager_id'],
            'status': 'succeeded' if row['status'] == 'succeeded' else 'reserved',
            'inboundEventKey': '', 'inboundAt': '', 'legacy': True, 'claimedAt': row['created_at'],
        }
    jobs = connection.execute(f"""SELECT j.* FROM contact_owner_jobs j
        WHERE j.contact_id IN ({cmarks}) AND j.operation_key!=?
          AND (j.status!='cancelled' OR j.confirmed=1)
          AND NOT EXISTS(SELECT 1 FROM request_claim_allocations a WHERE a.operation_key=j.operation_key)
        ORDER BY j.generation DESC""", (*contacts, str(except_operation_key))).fetchall()
    for row in jobs:
        if row['operation_key'] in result or row['operation_key'] in operation_ids:
            continue
        result[row['operation_key']] = {
            'operationKey': row['operation_key'], 'dealId': row['deal_id'],
            'managerId': row['manager_id'],
            'status': 'succeeded' if row['confirmed'] else 'reserved',
            'inboundEventKey': '', 'inboundAt': '', 'legacy': True, 'claimedAt': row['created_at'],
        }
    if len(result) > 100:
        raise RequestClaimEvidenceError('request_history_incomplete')
    return list(result.values())


def reserve(connection, operation, now):
    request = _json(operation['request_json'])
    context = request.get('requestClaimContext')
    key, deal, manager = operation['operation_key'], operation['deal_id'], operation['manager_id']
    existing = connection.execute('SELECT * FROM request_claim_allocations WHERE operation_key=?', (key,)).fetchone()
    if context is None:
        if existing:
            raise RequestClaimEvidenceError()
        return  # Historical recovery and independent native flows stay separate.
    keys, event, inbound, closed = validate_context(
        context, now, allow_stale=bool(existing and (existing['status'] != 'released' or existing['confirmed'])))
    contact = (request.get('contactOwnerContext') or {}).get('contactId')
    if not _id(deal) or not _id(manager) or (contact and 'contact:' + str(contact) not in keys):
        raise RequestClaimEvidenceError('request_identity_changed')
    if existing:
        old_keys = json.loads(existing['identity_keys_json'])
        if existing['deal_id'] != deal or old_keys != keys or existing['inbound_event_key'] != event:
            raise RequestClaimEvidenceError('request_identity_changed')
        if existing['status'] != 'released' or existing['confirmed']:
            if existing['manager_id'] != manager:
                raise RequestClaimConflictError(deal_id=deal)
            return  # Same operation recovery cannot spend the request twice.
    if event:
        other = connection.execute("""SELECT deal_id FROM request_claim_allocations
            WHERE inbound_event_key=? AND operation_key!=?
              AND (status!='released' OR confirmed=1) LIMIT 1""", (event, key)).fetchone()
        if other:
            raise RequestClaimConflictError('request_event_already_claimed', other['deal_id'])
    conflicts = list_conflicts(connection, keys, key)
    for previous in conflicts:
        terminal = closed.get(previous['dealId'])
        if previous['status'] != 'succeeded':
            raise RequestClaimConflictError('request_claim_pending', previous['dealId'])
        if (inbound is None or terminal is None or inbound <= terminal
                or terminal < _time(previous['claimedAt'])):
            raise RequestClaimConflictError(deal_id=previous['dealId'])
        # Event identity is stronger than a session/CRM timestamp.
        if previous['inboundEventKey'] and previous['inboundEventKey'] == event:
            raise RequestClaimConflictError('request_event_already_claimed', previous['dealId'])
    # Carry every proven alias across renewed requests. Removing a phone or
    # creating a duplicate contact must not erase the old identity's boundary.
    effective_keys = set(keys)
    for previous in conflicts:
        effective_keys.update(row[0] for row in connection.execute(
            'SELECT identity_key FROM request_claim_aliases WHERE operation_key=?',
            (previous['operationKey'],)))
    if len(effective_keys) > 128:
        raise RequestClaimEvidenceError('request_history_incomplete')
    # Keep predecessors owned until the replacement succeeds. A failed new
    # attempt must leave the old request boundary intact, even after restart.
    for previous in conflicts:
        if previous['legacy']:
            connection.execute("""INSERT OR IGNORE INTO request_claim_allocations(
                operation_key,deal_id,manager_id,status,confirmed,legacy,created_at,updated_at)
                VALUES(?,?,?,'succeeded',1,1,?,?)""",
                (previous['operationKey'], previous['dealId'], previous['managerId'], previous['claimedAt'], now))
            connection.executemany('INSERT OR IGNORE INTO request_claim_aliases(identity_key,operation_key) VALUES(?,?)',
                                  [(alias, previous['operationKey']) for alias in keys])
    connection.execute("""INSERT INTO request_claim_allocations(
        operation_key,deal_id,manager_id,claim_marker,inbound_event_key,inbound_at,
        identity_keys_json,predecessors_json,status,created_at,updated_at)
        VALUES(?,?,?,?,?,?,?,?,'reserved',?,?)
        ON CONFLICT(operation_key) DO UPDATE SET status='reserved', manager_id=excluded.manager_id,
        claim_marker=excluded.claim_marker,predecessors_json=excluded.predecessors_json,
        updated_at=excluded.updated_at""",
        (key, deal, manager, str(request.get('claimMarker') or ''), event,
         context['inboundAt'], json.dumps(keys), json.dumps([row['operationKey'] for row in conflicts]), now, now))
    connection.executemany('INSERT OR IGNORE INTO request_claim_aliases(identity_key,operation_key) VALUES(?,?)',
                           [(alias, key) for alias in effective_keys])


def confirm(connection, operation_key, now):
    row = connection.execute("SELECT * FROM request_claim_allocations WHERE operation_key=? AND status='reserved'",
                             (operation_key,)).fetchone()
    if not row:
        return
    # BEGIN IMMEDIATE also contains the successful claim event and greeting
    # enqueue. Retiring the exact predecessor set cannot race another claimant.
    for predecessor in json.loads(row['predecessors_json']):
        connection.execute("UPDATE request_claim_allocations SET status='released',updated_at=? WHERE operation_key=? AND status='succeeded'",
                           (now, predecessor))
    connection.execute("UPDATE request_claim_allocations SET status='succeeded',confirmed=1,updated_at=? WHERE operation_key=? AND status='reserved'",
                       (now, operation_key))


def fail(connection, operation_key, result, now):
    if _unapplied(result):
        connection.execute("UPDATE request_claim_allocations SET status='released',updated_at=? WHERE operation_key=? AND confirmed=0 AND status='reserved'",
                           (now, operation_key))
