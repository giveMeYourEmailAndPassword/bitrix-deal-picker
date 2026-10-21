"""Provider reads must distinguish a new customer request from session echoes."""
import copy
import hashlib
import unittest
from unittest.mock import patch

import request_claim_evidence as evidence
from request_claim_guard import RequestClaimConflictError, RequestClaimEvidenceError, validate_context


NOW = '2026-10-21T08:40:00+00:00'
CLOSED = '2026-10-21T04:58:00+00:00'
INBOUND = '2026-10-21T08:00:00+00:00'
PHONE_HASH = hashlib.sha256(b'996779378485').hexdigest()
MID = '11111111-2222-4333-8444-555555555555'


class Provider:
    def __init__(self):
        self.current = {'ID': '576953', 'CONTACT_ID': '245', 'CONTACT_IDS': ['245'],
                        'CLOSED': 'N', 'STAGE_ID': 'UC_PUCAAQ', 'STAGE_SEMANTIC_ID': 'P',
                        'CATEGORY_ID': '0', 'DATE_CREATE': CLOSED}
        self.previous = {'ID': '576891', 'CONTACT_ID': '245', 'CLOSED': 'Y',
                         'STAGE_ID': 'UC_WANTS_HOT', 'STAGE_SEMANTIC_ID': 'F',
                         'CATEGORY_ID': '0', 'MOVED_TIME': CLOSED}
        self.contacts = {'245': {'ID': '245', 'PHONE': [{'VALUE': '0779 378 485'}]}}
        self.deals = [self.previous, self.current]
        self.conflicts = []
        self.messages = {'23868279': self.message('23868279', '2026-10-20T20:03:00+00:00')}
        self.users = {'404113': {'id': '404113', 'connector': True, 'type': 'user', 'departments': []},
                      '412901': {'id': '412901', 'connector': False, 'type': 'user', 'departments': [187]},
                      '262867': {'id': '262867', 'connector': False, 'type': 'bot', 'departments': []}}
        self.sessions = ['627799']
        self.calls = []
        self.proof = {'providerId': MID, 'state': 'incoming', 'channelId': 'channel-one',
                      'phoneHash': PHONE_HASH, 'createdAt': INBOUND}

    def message(self, mid, stamp, *, sender='404113', provider=MID, chat='555949'):
        return {'id': mid, 'chatid': chat, 'senderid': sender, 'date': stamp,
                'text': 'Новый запрос клиента', 'params': {'connectorMid': [provider]}}

    def call(self, method, params, *, timeout):
        self.calls.append((method, copy.deepcopy(params), timeout))
        if method == 'crm.deal.contact.items.get':
            return {'result': [{'CONTACT_ID': '245'}]}
        if method == 'crm.contact.get':
            return {'result': self.contacts[params['id']]}
        if method == 'crm.duplicate.findbycomm':
            return {'result': {'CONTACT': list(self.contacts)}}
        if method == 'imopenlines.crm.chat.get':
            return {'result': [{'CHAT_ID': '555949'}]}
        if method == 'crm.deal.list':
            return {'result': self.deals, 'total': len(self.deals)}
        if method == 'crm.deal.get':
            return {'result': self.previous}
        if method == 'crm.stagehistory.list':
            row = dict(self.previous, ID='9001', OWNER_ID=self.previous['ID'], CREATED_TIME=CLOSED)
            return {'result': {'items': [row]}, 'total': 1}
        if method == 'crm.activity.list':
            rows = [{'ID': str(8000+i), 'OWNER_TYPE_ID': '2', 'OWNER_ID': '576953',
                     'PROVIDER_ID': 'IMOPENLINES_SESSION', 'ASSOCIATED_ENTITY_ID': value}
                    for i, value in enumerate(self.sessions)]
            return {'result': rows, 'total': len(rows)}
        if method == 'imopenlines.session.history.get':
            return {'result': {'chatId': '555949', 'sessionId': params['SESSION_ID'],
                               'users': self.users, 'message': self.messages}}
        raise AssertionError(method)

    def list_request_claim_conflicts(self, keys, *, except_operation_key):
        if 'phone:' + PHONE_HASH not in keys:
            raise AssertionError('Canonical identity missing')
        return self.conflicts

    def origins(self, payload, *, timeout):
        if payload != {'providerIds': [MID], 'phoneHashes': [PHONE_HASH]} or not 0 < timeout <= 16:
            raise AssertionError(payload)
        return {'schemaVersion': 1, 'observedAt': NOW, 'proofs': [self.proof]}

    def read(self, **kwargs):
        return evidence.read_request_claim_context(
            self.current, call=kwargs.pop('call', self.call), store=self,
            observed_at=NOW, inbound_origins=kwargs.pop('inbound_origins', self.origins), **kwargs)


class RequestClaimEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.p = Provider()

    def new_inbound(self):
        self.p.messages['23870000'] = self.p.message('23870000', INBOUND)

    def test_incident_closed_576891_new_576953_same_chat_without_inbound_is_blocked(self):
        self.p.messages.update({
            '23869000': self.p.message('23869000', '2026-10-21T04:57:00+00:00', sender='412901'),
            '23869001': self.p.message('23869001', '2026-10-21T04:58:24+00:00', sender='0'),
        })
        with self.assertRaises(RequestClaimConflictError) as raised:
            self.p.read()
        self.assertEqual(raised.exception.code, 'request_no_new_customer_message')
        self.assertEqual(raised.exception.deal_id, '576891')

    def test_first_request_needs_no_fake_inbound_or_openline_session(self):
        self.p.deals = [self.p.current]
        result = self.p.read(inbound_origins=None)
        self.assertEqual(result['identityKeys'], ['chat:555949', 'contact:245', 'phone:' + PHONE_HASH])
        self.assertEqual(result['checkedPrior'], [])
        self.assertEqual(result['inboundEventKey'], '')
        self.assertFalse(any(method == 'imopenlines.session.history.get' for method, *_ in self.p.calls))
        validate_context(result, NOW)

    def test_current_phone_is_canonicalized_and_never_returned(self):
        self.p.deals = [self.p.current]
        for phone in ['0779378485', '779378485', '+779378485', '+996 (779) 378-485', '00996779378485']:
            with self.subTest(phone=phone):
                self.p.contacts['245']['PHONE'][0]['VALUE'] = phone
                result = self.p.read()
                self.assertIn('phone:' + PHONE_HASH, result['identityKeys'])
                self.assertNotIn('779378485', str(result))

    def test_phone_normalization_matches_baza_russian_prefix(self):
        self.assertEqual(evidence._phone('8 (999) 123-45-67'), '79991234567')
        self.assertEqual(evidence._phone('+7 999 123-45-67'), '79991234567')

    def test_new_customer_event_after_terminal_allows_renewal(self):
        self.new_inbound()
        result = self.p.read()
        self.assertEqual(result['checkedPrior'], [{'dealId': '576891', 'closedAt': CLOSED}])
        self.assertEqual(result['inboundAt'], INBOUND)
        self.assertEqual(result['inboundEventKey'], hashlib.sha256(('wazzup:channel-one:' + MID).encode()).hexdigest())
        self.assertIn('chat:555949', result['identityKeys'])
        validate_context(result, NOW)

    def test_history_list_uses_explicit_message_ids_and_rejects_duplicate_ids(self):
        self.new_inbound()
        self.p.messages = list(self.p.messages.values())
        self.assertTrue(self.p.read()['inboundEventKey'])
        self.p.messages.append(copy.deepcopy(self.p.messages[-1]))
        with self.assertRaises(RequestClaimEvidenceError):
            self.p.read()

    def test_empty_php_params_and_system_rows_do_not_create_false_inbound(self):
        self.p.messages['23870000'] = self.p.message('23870000', INBOUND, sender='0')
        self.p.messages['23870000']['params'] = []
        with self.assertRaises(RequestClaimConflictError):
            self.p.read()

    def test_no_contact_first_claim_can_use_stable_chat_and_no_identity_fails_closed(self):
        self.p.current.pop('CONTACT_ID')
        self.p.current.pop('CONTACT_IDS')
        self.p.deals = [self.p.current]
        self.p.list_request_claim_conflicts = lambda keys, **kwargs: []
        base = self.p.call
        def read(method, params, **kwargs):
            if method == 'crm.deal.contact.items.get':
                return {'result': []}
            return base(method, params, **kwargs)
        self.assertEqual(self.p.read(call=read)['identityKeys'], ['chat:555949'])
        def no_identity(method, params, **kwargs):
            if method == 'imopenlines.crm.chat.get':
                return {'result': []}
            return read(method, params, **kwargs)
        with self.assertRaises(RequestClaimEvidenceError):
            self.p.read(call=no_identity)

    def test_no_contact_chat_conflict_still_loads_and_blocks_open_predecessor(self):
        self.p.current.pop('CONTACT_ID')
        self.p.current.pop('CONTACT_IDS')
        self.p.previous.update(CLOSED='N', STAGE_ID='NEW', STAGE_SEMANTIC_ID='P')
        self.p.list_request_claim_conflicts = lambda keys, **kwargs: [{'dealId': '576891', 'status': 'succeeded'}]
        base = self.p.call
        def read(method, params, **kwargs):
            if method == 'crm.deal.contact.items.get':
                return {'result': []}
            return base(method, params, **kwargs)
        with self.assertRaises(RequestClaimConflictError):
            self.p.read(call=read)

    def test_outgoing_echo_that_looks_like_customer_is_not_a_new_request(self):
        self.new_inbound()
        self.p.proof['state'] = 'outgoing'
        with self.assertRaises(RequestClaimConflictError):
            self.p.read()

    def test_connector_flag_without_native_origin_cannot_authorize(self):
        self.new_inbound()
        with self.assertRaises(RequestClaimEvidenceError):
            self.p.read(inbound_origins=None)

    def test_unknown_or_deleted_provider_message_is_not_new_inbound(self):
        self.new_inbound()
        for state, error in [('unknown', RequestClaimEvidenceError), ('invalid', RequestClaimConflictError)]:
            with self.subTest(state=state):
                self.p.proof['state'] = state
                with self.assertRaises(error):
                    self.p.read()

    def test_employee_bot_system_and_deleted_messages_do_not_renew(self):
        for sender, deleted in [('412901', False), ('262867', False), ('0', False), ('404113', True)]:
            with self.subTest(sender=sender, deleted=deleted):
                message = self.p.message('23870000', INBOUND, sender=sender)
                if deleted:
                    message['params']['IS_DELETED'] = 'Y'
                self.p.messages = {'23870000': message}
                with self.assertRaises(RequestClaimConflictError):
                    self.p.read()

    def test_unknown_sender_and_missing_mid_fail_closed(self):
        for alteration in ['missing-profile', 'missing-mid', 'false-direction-param']:
            with self.subTest(alteration=alteration):
                message = self.p.message('23870000', INBOUND)
                if alteration == 'missing-profile':
                    message['senderid'] = '999'
                else:
                    message['params'] = {'isEcho': False} if alteration == 'false-direction-param' else {}
                self.p.messages = {'23870000': message}
                with self.assertRaises(RequestClaimEvidenceError):
                    self.p.read()

    def test_real_event_at_or_before_close_cannot_be_replayed_with_new_native_time(self):
        self.new_inbound()
        for stamp in [CLOSED, '2026-10-20T20:03:00+00:00']:
            with self.subTest(stamp=stamp):
                self.p.proof['createdAt'] = stamp
                with self.assertRaises(RequestClaimConflictError):
                    self.p.read()

    def test_event_key_survives_native_session_and_message_copy(self):
        self.new_inbound()
        first = self.p.read()
        self.p.sessions = ['700000']
        self.p.messages = {'30000000': self.p.message('30000000', '2026-10-21T08:30:00+00:00')}
        second = self.p.read()
        self.assertEqual(first['inboundEventKey'], second['inboundEventKey'])

    def test_native_message_must_also_be_strictly_after_closure(self):
        self.p.messages = {'23870000': self.p.message('23870000', CLOSED)}
        with self.assertRaises(RequestClaimConflictError):
            self.p.read()

    def test_pending_legacy_allocation_blocks_even_when_previous_deal_closed(self):
        self.new_inbound()
        self.p.conflicts = [{'dealId': '576891', 'status': 'reserved'}]
        with self.assertRaises(RequestClaimConflictError) as raised:
            self.p.read()
        self.assertEqual(raised.exception.code, 'request_claim_pending')

    def test_active_previous_deal_blocks(self):
        self.p.previous.update(CLOSED='N', STAGE_SEMANTIC_ID='P', STAGE_ID='NEW')
        self.new_inbound()
        with self.assertRaises(RequestClaimConflictError):
            self.p.read()

    def test_unclaimed_queued_duplicate_can_use_atomic_identity_fence(self):
        self.p.previous.update(CLOSED='N', STAGE_SEMANTIC_ID='P', STAGE_ID='UC_PUCAAQ')
        self.assertEqual(self.p.read()['checkedPrior'], [])

    def test_confirmed_claim_in_queue_still_requires_terminal(self):
        self.p.previous.update(CLOSED='N', STAGE_SEMANTIC_ID='P', STAGE_ID='UC_PUCAAQ')
        self.p.conflicts = [{'dealId': '576891', 'status': 'succeeded'}]
        with self.assertRaises(RequestClaimConflictError):
            self.p.read()

    def test_terminal_time_must_match_exact_latest_stage_transition(self):
        self.p.previous['MOVED_TIME'] = '2026-10-21T04:59:00+00:00'
        self.new_inbound()
        with self.assertRaises(RequestClaimEvidenceError):
            self.p.read()

    def test_missing_terminal_category_stage_or_moved_time_is_not_proof(self):
        for field in ['CATEGORY_ID', 'STAGE_ID', 'MOVED_TIME']:
            with self.subTest(field=field):
                previous = self.p.previous.pop(field)
                with self.assertRaises(RequestClaimEvidenceError):
                    self.p.read()
                self.p.previous[field] = previous

    def test_duplicate_contact_same_phone_adds_both_aliases(self):
        self.p.contacts['246'] = {'ID': '246', 'PHONE': [{'VALUE': '+996779378485'}]}
        self.p.deals = [self.p.current]
        result = self.p.read()
        self.assertEqual(result['identityKeys'], ['chat:555949', 'contact:245', 'contact:246', 'phone:' + PHONE_HASH])

    def test_full_pagination_is_consumed_and_partial_page_rejected(self):
        base = self.p.call
        def paginated(method, params, **kwargs):
            if method == 'crm.deal.list':
                if params['start'] == 0:
                    return {'result': [self.p.previous], 'total': 2, 'next': 1}
                return {'result': [self.p.current], 'total': 2}
            return base(method, params, **kwargs)
        with self.assertRaises(RequestClaimConflictError):
            self.p.read(call=paginated)
        def incomplete(method, params, **kwargs):
            if method == 'crm.deal.list':
                return {'result': [self.p.current], 'total': 2}
            return base(method, params, **kwargs)
        with self.assertRaises(RequestClaimEvidenceError):
            self.p.read(call=incomplete)

    def test_missing_total_duplicate_rows_and_bad_cursor_fail_closed(self):
        base = self.p.call
        for payload in [{'result': [self.p.current]}, {'result': [self.p.current]*2, 'total': 2},
                        {'result': [self.p.current], 'total': 2, 'next': 0},
                        {'result': [self.p.current], 'total': 101, 'next': 1}]:
            with self.subTest(payload=payload):
                def bad(method, params, **kwargs):
                    return payload if method == 'crm.deal.list' else base(method, params, **kwargs)
                with self.assertRaises(RequestClaimEvidenceError):
                    self.p.read(call=bad)

    def test_wrong_session_identity_or_message_chat_fails_closed(self):
        self.new_inbound()
        self.p.messages['23870000']['chatid'] = '1234'
        with self.assertRaises(RequestClaimEvidenceError):
            self.p.read()

    def test_provider_scope_missing_or_malformed_response_fails_closed(self):
        self.new_inbound()
        for field, value in [('phoneHash', '0'*64), ('channelId', ''), ('createdAt', 'yesterday')]:
            with self.subTest(field=field):
                old = self.p.proof[field]
                self.p.proof[field] = value
                with self.assertRaises(RequestClaimEvidenceError):
                    self.p.read()
                self.p.proof[field] = old
        for response in [{}, {'schemaVersion': 1, 'observedAt': NOW, 'proofs': []},
                         {'schemaVersion': 1, 'observedAt': NOW, 'proofs': [self.p.proof]*2}]:
            with self.subTest(response=response):
                with self.assertRaises(RequestClaimEvidenceError):
                    self.p.read(inbound_origins=lambda *_a, **_kw: response)

    def test_budget_is_total_not_per_call_and_errors_do_not_leak_provider_detail(self):
        with patch.object(evidence.time, 'monotonic', side_effect=[0, 1, 17]):
            with self.assertRaises(RequestClaimEvidenceError) as raised:
                self.p.read()
            self.assertEqual(raised.exception.code, 'request_evidence_timeout')
        def unavailable(*_a, **_kw):
            raise RuntimeError('secret-token-and-phone')
        with self.assertRaises(RequestClaimEvidenceError) as raised:
            self.p.read(call=unavailable)
        self.assertNotIn('secret-token', str(raised.exception))
        self.assertTrue(all(0 < timeout <= 16 for _, _, timeout in self.p.calls))


if __name__ == '__main__':
    unittest.main()
