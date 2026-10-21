"""Read-only, bounded proof that a CRM card is a new customer request.

Bitrix connector=True is NOT inbound proof: Wazzup sends outgoing echoes to
OpenLines as customer messages. Renewal therefore needs an authenticated native
provider UUID join supplied by ``inbound_origins``. Session creation, CRM creation
and text matching never establish a new request.
"""
import hashlib
import re
import time
from datetime import datetime, timezone

from request_claim_guard import RequestClaimConflictError, RequestClaimEvidenceError


READ_BUDGET_SECONDS = 16
MAX_CONTACTS = 8
MAX_DEALS = 100
MAX_PAGES = 20
MAX_SESSIONS = 8
MAX_MESSAGES = 500
QUEUE_STAGES = frozenset({'UC_ZJ55BR', 'UC_PUCAAQ'})
DEAL_FIELDS = ['ID', 'CONTACT_ID', 'CONTACT_IDS', 'CLOSED', 'STAGE_ID',
               'STAGE_SEMANTIC_ID', 'CATEGORY_ID', 'MOVED_TIME', 'DATE_CREATE']


def _error(code='request_evidence_unavailable'):
    raise RequestClaimEvidenceError(code)


def _id(value):
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        _error()
    value = str(value)
    if not re.fullmatch(r'[1-9]\d{0,19}', value):
        _error()
    return value


def _date(value):
    if not isinstance(value, str) or len(value) > 40:
        _error()
    try:
        parsed = datetime.fromisoformat(value[:-1] + '+00:00' if value.endswith('Z') else value)
        if parsed.utcoffset() is None:
            _error()
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        _error()


def _phone(value):
    if not isinstance(value, str) or not re.fullmatch(r'[+\d\s().-]{5,40}', value):
        _error('request_phone_invalid')
    digits = re.sub(r'\D', '', value)
    if digits.startswith('00'):
        digits = digits[2:]
    elif len(digits) == 10 and digits.startswith('0'):
        digits = '996' + digits[1:]
    elif len(digits) == 9:
        digits = '996' + digits
    elif len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    if not re.fullmatch(r'[1-9]\d{7,14}', digits):
        _error('request_phone_invalid')
    return digits


def _flag(value):
    return value is True or value == 'Y' or (type(value) is int and value == 1)


def _false(value):
    return value is False or value == 'N' or (type(value) is int and value == 0)


class _Reader:
    def __init__(self, call, timeout=READ_BUDGET_SECONDS):
        self.call = call
        if not isinstance(timeout, (int, float)) or not 0 < timeout <= READ_BUDGET_SECONDS:
            _error('request_evidence_timeout')
        self.deadline = time.monotonic() + timeout

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            _error('request_evidence_timeout')
        return remaining

    def read(self, method, params):
        try:
            result = self.call(method, params, timeout=self.remaining())
        except RequestClaimEvidenceError:
            raise
        except Exception:
            _error()
        self.remaining()
        if not isinstance(result, dict) or 'result' not in result or result.get('error'):
            _error()
        return result

    def one(self, method, params):
        payload = self.read(method, params)
        if payload.get('next') not in (None, ''):
            _error('request_history_incomplete')
        return payload['result']

    def listing(self, method, params, *, limit=MAX_DEALS):
        rows, start, total = [], 0, None
        for _ in range(MAX_PAGES):
            payload = self.read(method, dict(params, start=start))
            page = payload['result']
            if not isinstance(page, list) or any(not isinstance(row, dict) for row in page):
                _error('request_history_incomplete')
            raw_total = payload.get('total')
            if isinstance(raw_total, bool) or not isinstance(raw_total, (int, str)) or not str(raw_total).isdigit():
                _error('request_history_incomplete')
            current_total = int(raw_total)
            if current_total > limit or (total is not None and total != current_total):
                _error('request_history_incomplete')
            total = current_total
            rows.extend(page)
            if len(rows) > total or len(rows) > limit:
                _error('request_history_incomplete')
            next_page = payload.get('next')
            if next_page in (None, ''):
                if len(rows) != total:
                    _error('request_history_incomplete')
                return rows
            if not page or isinstance(next_page, bool) or not str(next_page).isdigit() or int(next_page) != len(rows) or int(next_page) <= start:
                _error('request_history_incomplete')
            start = int(next_page)
        _error('request_history_incomplete')


def _contacts(reader, deal):
    ids = set()
    if deal.get('CONTACT_ID') not in (None, '', '0', 0):
        ids.add(_id(deal['CONTACT_ID']))
    raw_ids = deal.get('CONTACT_IDS', [])
    if not isinstance(raw_ids, list):
        _error()
    ids.update(_id(value) for value in raw_ids)
    bindings = reader.one('crm.deal.contact.items.get', {'id': _id(deal.get('ID'))})
    if not isinstance(bindings, list) or len(bindings) > MAX_CONTACTS:
        _error('request_identity_unavailable')
    for binding in bindings:
        if not isinstance(binding, dict):
            _error()
        ids.add(_id(binding.get('CONTACT_ID')))
    checked, phones = set(), set()
    while ids - checked:
        if len(ids) > MAX_CONTACTS:
            _error('request_history_incomplete')
        contact_id = sorted(ids - checked, key=int)[0]
        contact = reader.one('crm.contact.get', {'id': contact_id})
        if not isinstance(contact, dict) or _id(contact.get('ID')) != contact_id:
            _error('request_identity_unavailable')
        values = contact.get('PHONE', [])
        if not isinstance(values, list) or len(values) > 8:
            _error('request_identity_unavailable')
        new_phones = set()
        for item in values:
            if not isinstance(item, dict):
                _error()
            new_phones.add(_phone(item.get('VALUE')))
        checked.add(contact_id)
        for phone in sorted(new_phones - phones):
            duplicates = reader.one('crm.duplicate.findbycomm', {
                'entity_type': 'CONTACT', 'type': 'PHONE', 'values[]': [phone]})
            if duplicates == []:
                duplicates = {}
            if not isinstance(duplicates, dict) or any(key != 'CONTACT' for key in duplicates):
                _error('request_identity_unavailable')
            matches = duplicates.get('CONTACT', [])
            # This API has no pagination; a large alias set exceeds this
            # reader's explicitly bounded identity scope.
            if not isinstance(matches, list) or len(matches) > MAX_CONTACTS:
                _error('request_history_incomplete')
            ids.update(_id(value) for value in matches)
        phones.update(new_phones)
        if len(ids) + len(phones) > 16:
            _error('request_history_incomplete')
    return sorted(ids, key=int), sorted(phones)


def _terminal(reader, deal, observed):
    deal_id = _id(deal.get('ID'))
    if deal.get('CLOSED') != 'Y' or deal.get('STAGE_SEMANTIC_ID') not in ('S', 'F'):
        raise RequestClaimConflictError('request_previous_deal_open', deal_id)
    category = deal.get('CATEGORY_ID')
    if (isinstance(category, bool) or not isinstance(category, (int, str))
            or not re.fullmatch(r'0|[1-9]\d{0,19}', str(category))
            or not isinstance(deal.get('STAGE_ID'), str) or not deal['STAGE_ID']):
        _error('request_terminal_unproven')
    payload = reader.read('crm.stagehistory.list', {
        'entityTypeId': 2, 'order[ID]': 'DESC', 'filter[OWNER_ID]': deal_id,
        'select[]': ['ID', 'OWNER_ID', 'CATEGORY_ID', 'STAGE_ID', 'CREATED_TIME'], 'start': 0})
    result = payload['result']
    rows = result.get('items') if isinstance(result, dict) else None
    if not isinstance(rows, list) or not rows or len(rows) > 50:
        _error('request_terminal_unproven')
    ids = [_id(row.get('ID')) if isinstance(row, dict) else _error() for row in rows]
    if len(set(ids)) != len(ids) or ids != sorted(ids, key=int, reverse=True):
        _error('request_terminal_unproven')
    latest = rows[0]
    if (_id(latest.get('OWNER_ID')) != deal_id or latest.get('STAGE_ID') != deal.get('STAGE_ID')
            or str(latest.get('CATEGORY_ID')) != str(deal.get('CATEGORY_ID'))):
        _error('request_terminal_unproven')
    closed = _date(latest.get('CREATED_TIME'))
    if closed > observed:
        _error('request_terminal_unproven')
    moved = _date(deal.get('MOVED_TIME'))
    if abs((moved - closed).total_seconds()) > 5:
        _error('request_terminal_unproven')
    # The later boundary is conservative when Bitrix's two timestamps differ.
    if moved > observed:
        _error('request_terminal_unproven')
    return max(moved, closed)


def _chat_keys(reader, deal_id):
    result = reader.one('imopenlines.crm.chat.get', {
        'CRM_ENTITY_TYPE': 'DEAL', 'CRM_ENTITY': deal_id, 'ACTIVE_ONLY': 'N'})
    rows = result if isinstance(result, list) else list(result.values()) if isinstance(result, dict) else None
    if rows is None or len(rows) > MAX_SESSIONS or any(not isinstance(row, dict) for row in rows):
        _error('request_session_unproven')
    ids = [_id(row.get('CHAT_ID')) for row in rows]
    if len(set(ids)) != len(ids):
        _error('request_session_unproven')
    return ['chat:' + value for value in ids]


def _histories(reader, deal_id):
    activities = reader.listing('crm.activity.list', {
        'order[ID]': 'DESC', 'filter[OWNER_TYPE_ID]': 2,
        'filter[OWNER_ID]': deal_id, 'filter[PROVIDER_ID]': 'IMOPENLINES_SESSION',
        'select[]': ['ID', 'OWNER_TYPE_ID', 'OWNER_ID', 'PROVIDER_ID', 'ASSOCIATED_ENTITY_ID'],
    }, limit=MAX_SESSIONS)
    session_ids, activity_ids = set(), set()
    for activity in activities:
        activity_id = _id(activity.get('ID'))
        if (activity_id in activity_ids or str(activity.get('OWNER_TYPE_ID')) != '2'
                or _id(activity.get('OWNER_ID')) != deal_id
                or activity.get('PROVIDER_ID') != 'IMOPENLINES_SESSION'):
            _error('request_session_unproven')
        activity_ids.add(activity_id)
        session_ids.add(_id(activity.get('ASSOCIATED_ENTITY_ID')))
    for session_id in sorted(session_ids, key=int, reverse=True):
        history = reader.one('imopenlines.session.history.get', {'SESSION_ID': session_id})
        if not isinstance(history, dict) or _id(history.get('sessionId')) != session_id:
            _error('request_session_unproven')
        yield history


def read_request_claim_context(deal, *, call, store, operation_key='', observed_at=None,
                               inbound_origins=None, timeout=READ_BUDGET_SECONDS):
    """Build server-only evidence. ``inbound_origins`` receives a bounded proof
    request plus ``timeout``; absence/unavailability cannot authorize renewal.
    """
    observed = _date(observed_at) if observed_at is not None else datetime.now(timezone.utc)
    if not isinstance(deal, dict):
        _error()
    deal_id = _id(deal.get('ID'))
    reader = _Reader(call, timeout)
    contact_ids, phones = _contacts(reader, deal)
    keys = ['contact:' + value for value in contact_ids]
    keys += ['phone:' + hashlib.sha256(value.encode()).hexdigest() for value in phones]
    keys += _chat_keys(reader, deal_id)
    if not keys:
        _error('request_identity_unavailable')
    if len(keys) > 16:
        _error('request_history_incomplete')
    siblings = {}
    for contact_id in contact_ids:
        rows = reader.listing('crm.deal.list', {
            'filter[CONTACT_ID]': contact_id, 'select[]': DEAL_FIELDS, 'order[ID]': 'ASC'})
        seen = set()
        for row in rows:
            row_id = _id(row.get('ID'))
            if row_id in seen or (row_id in siblings and siblings[row_id] != row):
                _error('request_history_incomplete')
            seen.add(row_id)
            siblings[row_id] = row
        if len(siblings) > MAX_DEALS:
            _error('request_history_incomplete')
    if not contact_ids:
        siblings[deal_id] = deal
    if deal_id not in siblings:
        _error('request_identity_changed')
    if any(field in deal and siblings[deal_id].get(field) != deal[field]
           for field in ('STAGE_ID', 'STAGE_SEMANTIC_ID', 'CLOSED', 'CONTACT_ID', 'MOVED_TIME')):
        _error('request_identity_changed')
    try:
        conflicts = store.list_request_claim_conflicts(keys, except_operation_key=operation_key)
    except RequestClaimEvidenceError:
        raise
    except Exception:
        _error('request_history_incomplete')
    if not isinstance(conflicts, list) or len(conflicts) > MAX_DEALS:
        _error('request_history_incomplete')
    for conflict in conflicts:
        if not isinstance(conflict, dict):
            _error()
        previous_id = _id(conflict.get('dealId'))
        if conflict.get('status') != 'succeeded':
            raise RequestClaimConflictError('request_claim_pending', previous_id)
        if previous_id == deal_id:
            raise RequestClaimConflictError(deal_id=previous_id)
        if previous_id not in siblings:
            previous = reader.one('crm.deal.get', {'id': previous_id})
            if not isinstance(previous, dict) or _id(previous.get('ID')) != previous_id:
                _error()
            siblings[previous_id] = previous
    if len(siblings) > MAX_DEALS:
        _error('request_history_incomplete')
    prior = []
    conflict_ids = {_id(item.get('dealId')) for item in conflicts}
    for previous_id, previous in siblings.items():
        if previous_id == deal_id:
            continue
        if (previous.get('CLOSED') == 'N' and previous.get('STAGE_SEMANTIC_ID') == 'P'
                and previous.get('STAGE_ID') in QUEUE_STAGES and previous_id not in conflict_ids):
            continue  # An unclaimed duplicate is fenced atomically by its aliases.
        if previous.get('CLOSED') not in ('Y', 'N') or previous.get('STAGE_SEMANTIC_ID') not in ('P', 'S', 'F'):
            _error('request_terminal_unproven')
        prior.append({'dealId': previous_id, 'closedAt': _terminal(reader, previous, observed).isoformat()})
    context = {'identityKeys': sorted(keys), 'inboundEventKey': '', 'inboundAt': '',
               'checkedPrior': sorted(prior, key=lambda item: int(item['dealId'])),
               'observedAt': observed.isoformat()}
    if not prior:
        return context
    cutoff = max(_date(item['closedAt']) for item in prior)
    return _renewal(reader, deal_id, phones, context, cutoff, observed, inbound_origins)


def _renewal(reader, deal_id, phones, context, cutoff, observed, inbound_origins):
    # The provider callback adapter is intentionally isolated from Bitrix's
    # undocumented connector-specific parameters and from CRM writes.
    candidates, seen, chats = [], set(), set()
    ambiguous = False
    for history in _histories(reader, deal_id):
        chat_id = _id(history.get('chatId'))
        if 'chat:' + chat_id not in context['identityKeys']:
            _error('request_identity_changed')
        chats.add(chat_id)
        users, messages = history.get('users'), history.get('message')
        if users == []:
            users = {}
        if not isinstance(users, dict) or not isinstance(messages, (dict, list)) or len(messages) > MAX_MESSAGES:
            _error('request_history_incomplete')
        entries = messages.items() if isinstance(messages, dict) else [(None, item) for item in messages]
        session_seen = set()
        for key, message in entries:
            if (not isinstance(message, dict) or (key is not None and _id(message.get('id')) != _id(key))
                    or _id(message.get('chatid')) != chat_id):
                _error('request_session_unproven')
            message_id = _id(message['id'])
            if message_id in session_seen:
                _error('request_history_incomplete')
            session_seen.add(message_id)
            stamp = _date(message.get('date'))
            if stamp > observed:
                _error('request_evidence_stale')
            if stamp <= cutoff:
                continue
            sender = str(message.get('senderid', ''))
            if sender == '0':
                continue
            params = message.get('params', {})
            if params == []:
                params = {}
            if not isinstance(params, dict):
                _error()
            if any(_flag(value) for value in (params.get('IS_DELETED'), params.get('isDeleted'), message.get('isDeleted'))):
                continue
            user = users.get(sender)
            if not isinstance(user, dict) or str(user.get('id')) != sender:
                ambiguous = True
                continue
            if _flag(user.get('bot')) or user.get('type') == 'bot' or _false(user.get('connector')):
                continue
            if not _flag(user.get('connector')) or user.get('departments') not in (None, []):
                ambiguous = True
                continue
            mids = params.get('connectorMid')
            if not isinstance(mids, list) or not 1 <= len(mids) <= 20 or any(not isinstance(mid, str) or not re.fullmatch(r'[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}', mid) for mid in mids):
                ambiguous = True
                continue
            identity = (chat_id, message_id)
            if identity in seen:
                _error('request_history_incomplete')
            seen.add(identity)
            candidates.append({'chatId': chat_id, 'sessionId': _id(history['sessionId']),
                               'messageId': message_id, 'connectorMids': sorted(set(mids)),
                               'occurredAt': stamp.isoformat()})
    if len(candidates) > 200:
        _error('request_history_incomplete')
    if candidates:
        return _verified_renewal(reader, deal_id, phones, context, candidates, cutoff, observed, inbound_origins)
    if ambiguous:
        _error('request_inbound_unproven')
    raise RequestClaimConflictError('request_no_new_customer_message', context['checkedPrior'][-1]['dealId'])


def _verified_renewal(reader, deal_id, phones, context, candidates, cutoff, observed, inbound_origins):
    if inbound_origins is None:
        _error('request_inbound_unproven')
    provider_ids = sorted({mid for candidate in candidates for mid in candidate['connectorMids']})
    if not phones or len(provider_ids) > 100:
        _error('request_inbound_unproven')
    hashes = sorted(hashlib.sha256(phone.encode()).hexdigest() for phone in phones)
    try:
        response = inbound_origins({'providerIds': provider_ids, 'phoneHashes': hashes}, timeout=reader.remaining())
    except Exception:
        _error('request_inbound_unproven')
    reader.remaining()
    if not isinstance(response, dict) or type(response.get('schemaVersion')) is not int or response.get('schemaVersion') != 1:
        _error('request_inbound_unproven')
    proof_time = _date(response.get('observedAt'))
    if not -30 <= (observed - proof_time).total_seconds() <= 300:
        _error('request_inbound_unproven')
    rows = response.get('proofs')
    if not isinstance(rows, list) or len(rows) != len(provider_ids):
        _error('request_inbound_unproven')
    proofs = {}
    for proof in rows:
        if not isinstance(proof, dict) or proof.get('providerId') not in provider_ids or proof['providerId'] in proofs:
            _error('request_inbound_unproven')
        if proof.get('state') not in ('incoming', 'outgoing', 'invalid', 'unknown'):
            _error('request_inbound_unproven')
        proofs[proof['providerId']] = proof
    eligible, unknown = [], False
    for candidate in candidates:
        # A many-to-one provider mapping cannot define one spendable event.
        if len(candidate['connectorMids']) != 1:
            unknown = True
            continue
        provider_id = candidate['connectorMids'][0]
        proof = proofs[provider_id]
        if proof['state'] in ('outgoing', 'invalid'):
            continue
        if proof['state'] == 'unknown':
            unknown = True
            continue
        channel = proof.get('channelId')
        if (not isinstance(channel, str) or not re.fullmatch(r'[A-Za-z0-9_.:@-]{1,128}', channel)
                or proof.get('phoneHash') not in hashes):
            _error('request_inbound_unproven')
        provider_date = _date(proof.get('createdAt'))
        if provider_date > observed:
            _error('request_inbound_unproven')
        # Both clocks must be after closure. This rejects old provider UUIDs
        # replayed as new Bitrix messages and conservative minute rounding.
        if provider_date <= cutoff:
            continue
        event = hashlib.sha256(('wazzup:' + channel + ':' + provider_id).encode()).hexdigest()
        # Use the earlier proof time for the atomic fence's closure comparison.
        inbound = min(provider_date, _date(candidate['occurredAt']))
        eligible.append((inbound, event))
    if eligible:
        inbound, event = max(eligible)
        context['inboundAt'], context['inboundEventKey'] = inbound.isoformat(), event
        return context
    if unknown:
        _error('request_inbound_unproven')
    raise RequestClaimConflictError('request_no_new_customer_message', context['checkedPrior'][-1]['dealId'])
