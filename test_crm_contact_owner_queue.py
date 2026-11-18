"""Real shared queue with the active/newest policy; no external IO."""
import tempfile
import unittest
from pathlib import Path

from contact_owner_sync import ContactOwnerSync
from state_store import StateStore


class CrmContactQueueTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.store = StateStore(Path(tmp.name))
        self.owner, self.writes = '9', []
        self.deals = {'100': self.deal('100', '42', '2026-11-18T10:00:00+06:00')}
        self.sync = ContactOwnerSync(self.store, self.call, 'UF_MARKER', enforce_active_policy=True)

    def deal(self, did, owner, created):
        return {'ID': did, 'CONTACT_ID': '7', 'ASSIGNED_BY_ID': owner, 'DATE_CREATE': created,
                'STAGE_ID': 'NEW', 'STAGE_SEMANTIC_ID': 'P', 'CLOSED': 'N', 'UF_MARKER': '', 'contacts': ['7']}

    def call(self, method, params, timeout=None):
        if method == 'crm.deal.get':
            return dict(self.deals[str(params['id'])])
        if method == 'crm.contact.get':
            return {'ID': str(params['id']), 'ASSIGNED_BY_ID': self.owner}
        if method == 'crm.deal.contact.items.get':
            return [{'CONTACT_ID': cid} for cid in self.deals[str(params['id'])]['contacts']]
        if method == 'crm.item.list':
            rows = []
            for d in self.deals.values():
                if d['CLOSED'] != 'N' or d['STAGE_SEMANTIC_ID'] != 'P':
                    continue
                rows.append({'id': int(d['ID']), 'assignedById': int(d['ASSIGNED_BY_ID']),
                             'createdTime': d['DATE_CREATE'], 'stageId': d['STAGE_ID'],
                             'stageSemanticId': d['STAGE_SEMANTIC_ID'], 'closed': d['CLOSED'],
                             'contactIds': [int(cid) for cid in d['contacts']]})
            return {'items': rows}
        self.assertEqual(method, 'crm.contact.update')
        self.assertEqual(set(params), {'id', 'fields[ASSIGNED_BY_ID]'})
        self.writes.append(dict(params))
        self.owner = params['fields[ASSIGNED_BY_ID]']
        return True

    def reserve(self, key='crm:first', did='100', owner='42'):
        return self.sync.reserve_crm(key, did, owner, '7', self.owner, '')

    def test_new_crm_job_uses_shared_durable_queue_once(self):
        generation = self.reserve()
        self.assertEqual(self.reserve(), generation)
        self.sync.process()
        self.sync.process()
        self.assertEqual(self.owner, '42')
        self.assertEqual(len(self.writes), 1)

    def test_newer_active_deal_blocks_old_claim_and_crm_job(self):
        self.reserve()
        self.deals['101'] = self.deal('101', '43', '2026-11-18T11:00:00+06:00')
        self.sync.process()
        self.assertEqual(self.writes, [])
        self.assertEqual(self.sync.status('crm:first')['status'], 'conflict')

    def test_delayed_picker_claim_obeys_same_newest_active_policy(self):
        self.deals['100']['UF_MARKER'] = 'old-claim'
        self.store.begin_claim_operation('100', '42', operation_key='claim:old', request={
            'claimMarker': 'old-claim', 'contactOwnerContext': {'contactId': '7', 'previousOwner': '9'}})
        self.store.finalize_claim_operation('claim:old', claim={'dealId': '100', 'managerId': '42'},
                                            expected_claim_marker='old-claim')
        self.deals['101'] = self.deal('101', '43', '2026-11-18T11:00:00+06:00')
        self.sync.process()
        self.assertEqual(self.writes, [])
        self.assertEqual(self.sync.status('claim:old')['status'], 'conflict')

    def test_closed_and_excluded_stage_do_not_write(self):
        for did, stage, closed in [('101', 'C15:UC_ZJ55BR', 'N'), ('102', 'UC_PUCAAQ', 'N'), ('103', 'LOSE', 'Y')]:
            with self.subTest(stage=stage):
                self.deals = {did: self.deal(did, '42', '2026-11-18T10:00:00+06:00')}
                self.deals[did].update(STAGE_ID=stage, CLOSED=closed)
                self.reserve('crm:' + did, did)
                self.sync.process()
        self.assertEqual(self.writes, [])

    def test_contact_changed_after_snapshot_is_preserved(self):
        self.reserve()
        self.owner = '99'
        self.sync.process()
        self.assertEqual(self.owner, '99')
        self.assertEqual(self.writes, [])

    def test_secondary_contact_uses_verified_binding(self):
        self.deals['100']['CONTACT_ID'] = '8'
        self.deals['100']['contacts'] = ['8', '7']
        self.reserve()
        self.sync.process()
        self.assertEqual(self.owner, '42')

    def test_feature_off_does_not_dispatch_a_pending_crm_job(self):
        self.reserve()
        disabled = ContactOwnerSync(self.store, self.call, 'UF_MARKER')
        disabled.process()
        self.assertEqual(self.writes, [])

    def test_replayed_observation_cannot_refresh_a_generation(self):
        first = self.reserve()
        second = self.sync.reserve_crm('crm:second', '100', '42', '7', '9', '')
        self.assertGreater(second, first)
        self.assertEqual(self.reserve(), first)
        with self.assertRaisesRegex(ValueError, 'crm_contact_intent_conflict'):
            self.sync.reserve_crm('crm:first', '100', '42', '7', '99', '')


if __name__ == '__main__':
    unittest.main()
