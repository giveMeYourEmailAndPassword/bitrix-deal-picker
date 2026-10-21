"""Duplicate CRM request allocation tests; no network and temporary SQLite only."""
import hashlib
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from state_store import StateStore
from request_claim_guard import RequestClaimConflictError, RequestClaimEvidenceError


class RequestClaimGuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.store = StateStore(self.path)
        self.now = datetime.now(timezone.utc)
        self.old = self.now - timedelta(minutes=10)
        self.keys = ['contact:245', 'phone:' + hashlib.sha256(b'fake-test-phone').hexdigest()]

    def stamp(self, offset=0):
        return (self.now + timedelta(seconds=offset)).isoformat()

    def context(self, *, event='request-one', inbound=-700, prior=None, keys=None, observed=None):
        return {'identityKeys': self.keys if keys is None else keys,
                'inboundEventKey': hashlib.sha256(event.encode()).hexdigest() if event else '',
                'inboundAt': self.stamp(inbound) if event else '',
                'checkedPrior': prior or [], 'observedAt': observed or self.stamp()}

    def reserve(self, deal='100', manager='42', *, context=None, old=False, legacy=False):
        key = 'claim:' + deal
        when = self.old.isoformat() if old else self.stamp()
        request = {'claimMarker': 'marker:' + deal,
                   'contactOwnerContext': {'contactId': next((k.split(':')[1] for k in (context or {}).get('identityKeys', self.keys) if k.startswith('contact:')), '245'), 'previousOwner': '9'}}
        if not legacy:
            request['requestClaimContext'] = context or self.context(observed=when)
        with patch.object(self.store, '_now_iso', return_value=when):
            self.store.begin_claim_operation(deal, manager, operation_key=key, request=request)
        return key

    def confirm(self, key='claim:100', *, old=True):
        with patch.object(self.store, '_now_iso', return_value=self.old.isoformat() if old else self.stamp()):
            return self.store.finalize_claim_operation(key, claim={})

    def proof(self, deal='100', offset=-60):
        return [{'dealId': deal, 'closedAt': self.stamp(offset)}]

    def test_exact_incident_closed_old_deal_new_session_without_inbound_is_blocked(self):
        self.reserve(old=True); self.confirm()
        with self.assertRaises(RequestClaimConflictError):
            self.reserve('101', '43', context=self.context(event='', prior=self.proof()))
        self.assertIsNone(self.store.get_claim_operation('claim:101'))
        self.assertEqual(len(self.store.list_claims()), 1)

    def test_old_inbound_cannot_authorize_new_deal_even_after_terminal(self):
        self.reserve(old=True); self.confirm()
        with self.assertRaises(RequestClaimConflictError):
            self.reserve('101', '43', context=self.context(event='old-message-copy', prior=self.proof()))

    def test_customer_return_after_closure_is_allowed_but_active_previous_is_not(self):
        self.reserve(old=True); self.confirm()
        new = self.context(event='new-customer-inbound', inbound=-30)
        with self.assertRaises(RequestClaimConflictError):
            self.reserve('101', '43', context=new)
        new['checkedPrior'] = self.proof()
        self.reserve('101', '43', context=new)
        self.confirm('claim:101', old=False)
        conflicts = self.store.list_request_claim_conflicts(self.keys)
        self.assertEqual([row['dealId'] for row in conflicts], ['101'])

    def test_consumed_inbound_identity_remains_unique_after_request_closed(self):
        self.reserve(old=True); self.confirm()
        newer = self.context(event='second-inbound', inbound=-30, prior=self.proof())
        self.reserve('101', '43', context=newer); self.confirm('claim:101', old=False)
        with self.assertRaises(RequestClaimConflictError) as caught:
            self.reserve('102', '44', context=self.context(prior=self.proof('101', 0), inbound=0))
        self.assertEqual(caught.exception.code, 'request_event_already_claimed')

    def test_duplicate_contacts_same_phone_are_fenced(self):
        self.reserve(old=True); self.confirm()
        with self.assertRaises(RequestClaimConflictError):
            self.reserve('101', '43', context=self.context(keys=['contact:246', self.keys[1]], event='new', inbound=-20))

    def test_pending_and_uncertain_failure_survive_restart_and_do_not_renew(self):
        self.reserve(old=True)
        self.store.fail_claim_operation('claim:100', 'lost_reply', result={'remoteUpdateUncertain': True})
        self.store = StateStore(self.path)
        with self.assertRaises(RequestClaimConflictError) as caught:
            self.reserve('101', '43', context=self.context(event='new', inbound=-20, prior=self.proof()))
        self.assertEqual(caught.exception.code, 'request_claim_pending')

    def test_unknown_failure_retains_but_explicit_unapplied_releases(self):
        for result in ({}, {'remoteUpdated': False}, {'remoteUpdated': False, 'remoteUpdateUncertain': True}):
            with self.subTest(result=result), tempfile.TemporaryDirectory() as tmp:
                self.store = StateStore(Path(tmp))
                self.reserve(old=True)
                self.store.fail_claim_operation('claim:100', 'failure', result=result)
                if result == {'remoteUpdated': False}:
                    self.reserve('101', '43')
                else:
                    with self.assertRaises(RequestClaimConflictError):
                        self.reserve('101', '43')

    def test_legacy_success_is_fenced_and_can_renew_with_new_customer_evidence(self):
        self.reserve(old=True, legacy=True); self.confirm()
        conflicts = self.store.list_request_claim_conflicts(self.keys)
        self.assertTrue(conflicts[0]['legacy'])
        with self.assertRaises(RequestClaimConflictError):
            self.reserve('101', '43', context=self.context(event='', prior=self.proof()))
        self.reserve('101', '43', context=self.context(event='new', inbound=-20, prior=self.proof()))
        self.confirm('claim:101', old=False)
        self.assertEqual([x['dealId'] for x in self.store.list_request_claim_conflicts(self.keys)], ['101'])

    def test_legacy_failed_unknown_is_not_lost_when_contact_worker_cancelled(self):
        self.reserve(old=True, legacy=True)
        self.store.fail_claim_operation('claim:100', 'unknown')
        self.assertEqual(self.store.list_request_claim_conflicts(self.keys)[0]['status'], 'reserved')

    def test_failed_renewal_preserves_previous_request_boundary(self):
        self.reserve(old=True); self.confirm()
        self.reserve('101', '43', context=self.context(event='new', inbound=-20, prior=self.proof()))
        self.store.fail_claim_operation('claim:101', 'unapplied', result={'remoteUpdated': False})
        self.store = StateStore(self.path)
        self.assertEqual([x['dealId'] for x in self.store.list_request_claim_conflicts(self.keys)], ['100'])
        with self.assertRaises(RequestClaimConflictError):
            self.reserve('102', '44', context=self.context(event=''))

    def test_renewal_inherits_old_contact_even_when_phone_disappears(self):
        self.reserve(old=True); self.confirm()
        self.reserve('101', '43', context=self.context(keys=['contact:246', self.keys[1]],
            event='new', inbound=-20, prior=self.proof()))
        self.confirm('claim:101', old=False)
        self.assertEqual([x['dealId'] for x in self.store.list_request_claim_conflicts(['contact:245'])], ['101'])
        with self.assertRaises(RequestClaimConflictError):
            self.reserve('102', '44', context=self.context(keys=['contact:245'], event=''))

    def test_reading_conflicts_does_not_mutate_native_jobs(self):
        from contact_owner_sync import ContactOwnerSync
        sync = ContactOwnerSync(self.store, lambda *_: None, 'UF_FAKE')
        generation = sync.reserve_native('native:session:1', '100', '42', '245', '9', '')
        sync.confirm_native(generation)
        with self.store._connect() as c:
            before = [tuple(x) for x in c.execute('SELECT * FROM contact_owner_jobs')]
        self.assertTrue(self.store.list_request_claim_conflicts(self.keys)[0]['legacy'])
        with self.store._connect() as c:
            self.assertEqual(before, [tuple(x) for x in c.execute('SELECT * FROM contact_owner_jobs')])

    def test_two_concurrent_distinct_deals_same_identity_have_one_winner(self):
        barrier = threading.Barrier(2)
        successes, failures = [], []
        def attempt(deal):
            store = StateStore(self.path)
            barrier.wait()
            try:
                store.begin_claim_operation(deal, deal, operation_key='claim:' + deal,
                    request={'requestClaimContext': self.context(event='event-' + deal)})
                successes.append(deal)
            except RequestClaimConflictError:
                failures.append(deal)
        workers = [threading.Thread(target=attempt, args=(deal,)) for deal in ('100','101')]
        for worker in workers: worker.start()
        for worker in workers: worker.join(timeout=5)
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(failures), 1)
        self.assertEqual(len(self.store.list_claim_operations()), 1)

    def test_unrelated_identity_remains_available(self):
        self.reserve(old=True); self.confirm()
        self.reserve('101', '43', context=self.context(keys=['contact:999'], event='other'))

    def test_same_operation_recovery_is_idempotent_but_cannot_drop_identity(self):
        key = self.reserve()
        self.store.fail_claim_operation(key, 'lost_reply', result={'remoteUpdateUncertain': True})
        original = self.store.get_claim_operation(key)['request']
        self.store.retry_failed_claim_operation('100', '42', operation_key=key, request=original)
        self.assertEqual(len(self.store.list_request_claim_conflicts(self.keys)), 1)
        self.store.fail_claim_operation(key, 'unapplied', result={'remoteUpdated': False})
        with self.assertRaises(RequestClaimEvidenceError):
            self.store.retry_failed_claim_operation('100', '42', operation_key=key, request={})

    def test_uncertain_same_operation_recovery_does_not_expire_its_owned_fence(self):
        key = self.reserve(old=True)
        self.store.fail_claim_operation(key, 'lost_reply', result={'remoteUpdateUncertain': True})
        original = self.store.get_claim_operation(key)['request']
        self.store.retry_failed_claim_operation('100', '42', operation_key=key, request=original)
        self.assertEqual(len(self.store.list_request_claim_conflicts(self.keys)), 1)

    def test_invalid_or_stale_context_rolls_back_claim_contact_and_fence(self):
        invalid = [self.context(observed=self.stamp(-301)),
                   self.context(keys=['phone:0779378485']),
                   self.context(prior=[{'dealId':'100','closedAt':'2026-01-01'}]),
                   self.context(keys=['contact:245','contact:245'])]
        for context in invalid:
            with self.subTest(context=context):
                with self.assertRaises(RequestClaimEvidenceError): self.reserve(context=context)
                self.assertEqual(self.store.list_claim_operations(), [])
                self.assertEqual(self.store.list_request_claim_conflicts(self.keys), [])

    def test_closure_before_previous_claim_is_not_a_new_request_boundary(self):
        self.reserve(old=True); self.confirm()
        with self.assertRaises(RequestClaimConflictError):
            self.reserve('101','43',context=self.context(event='new',inbound=-20,prior=self.proof(offset=-650)))


if __name__ == '__main__': unittest.main()
