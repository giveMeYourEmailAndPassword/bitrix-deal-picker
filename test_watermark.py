"""Real native SQLite and actual app flush tests; no network or production data."""
import hashlib
import json
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

# Reuse the existing hermetic bootstrap, including when this module loads first.
from test_app import app
from state_store import StateStore, normalize_claim_export_from

CUTOFF = "2020-02-03T04:05:06.000Z"
BEFORE = "2020-02-03T04:05:05.999Z"
AFTER = "2020-02-03T04:05:06.001Z"


class WatermarkTests(unittest.TestCase):
    def setUp(self):
        network_guard = patch.object(app.urllib.request, "urlopen", side_effect=AssertionError("Network forbidden"))
        network_guard.start()
        self.addCleanup(network_guard.stop)
        self.directory = tempfile.TemporaryDirectory(prefix="picker-watermark-sqlite-")
        self.addCleanup(self.directory.cleanup)
        self.store = StateStore(self.directory.name, db_filename="state.sqlite3")
        self.sequence = 0

    def add(self, occurred=BEFORE, *, kind="claim_event", grant=False, payload=None):
        self.sequence += 1
        if payload is None:
            payload = {"occurredAt": occurred, "eventUuid": "synthetic", "operationKey": "synthetic"}
            if grant:
                payload["extraClaimRequestId"] = "synthetic-grant"
            payload = json.dumps(payload)
        with sqlite3.connect(self.store.db_path) as connection:
            cursor = connection.execute("""INSERT INTO integration_outbox
                (dedupe_key,kind,path,payload_json,next_attempt_at,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?)""", (str(self.sequence), kind,
                "/integrations/deal-picker/v1/claim-events", payload,
                "2019-01-01T00:00:00Z", "2021-01-01T00:00:00Z", "2021-01-01T00:00:00Z"))
            return cursor.lastrowid

    def snapshot(self, ids=None):
        with sqlite3.connect(self.store.db_path) as connection:
            rows = connection.execute("SELECT * FROM integration_outbox ORDER BY id").fetchall()
        if ids is not None:
            rows = [row for row in rows if row[0] in ids]
        return hashlib.sha256(repr(rows).encode()).hexdigest()

    def ids(self, **options):
        return [item["id"] for item in self.store.list_due_outbox(**options)]

    def settings(self, **kwargs):
        values = dict(STATE_STORE=self.store, CLAIM_EVENT_EXPORT_ENABLED=True,
                      EXTRA_CLAIM_REQUESTS_ENABLED=False, BAZA_CLAIM_EXPORT_FROM=CUTOFF,
                      INVALID_ENV_VALUES=set(), BAZA_API_BASE_URL="https://baza.example.test",
                      BAZA_HMAC_KEY_ID="synthetic", BAZA_HMAC_SECRET="s" * 32)
        values.update(kwargs)
        return patch.multiple(app, **values)

    def test_utc_parser_preserves_empty_and_rejects_invalid_or_excess_precision(self):
        self.assertIsNone(normalize_claim_export_from(None))
        self.assertIsNone(normalize_claim_export_from(""))
        for valid in (CUTOFF, "2020-02-03T04:05:06Z", "2020-02-03T04:05:06+00:00"):
            self.assertEqual(normalize_claim_export_from(valid), "2020-02-03T04:05:06.000+00:00")
        for invalid in (" ", "2020-02-30T04:05:06Z", "2020-02-03", "2020-02-03T04:05:06",
                        "2020-02-03T10:05:06+06:00", "2020-02-03T04:05:06.0001Z", "garbage"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                normalize_claim_export_from(invalid)

    def test_3993_historical_rows_do_not_consume_new_claim_slots_or_change(self):
        payload = json.dumps({"occurredAt": BEFORE})
        with sqlite3.connect(self.store.db_path) as connection:
            connection.executemany("""INSERT INTO integration_outbox
                (dedupe_key,kind,path,payload_json,next_attempt_at,created_at,updated_at)
                VALUES(?,'claim_event','/integrations/deal-picker/v1/claim-events',?,'2019-01-01T00:00:00Z','2021-01-01T00:00:00Z','2021-01-01T00:00:00Z')""",
                [("old-" + str(index), payload) for index in range(3993)])
        fresh = self.add(CUTOFF)
        before = self.snapshot()
        self.assertEqual(self.ids(limit=1, claim_export_from=CUTOFF), [fresh])
        self.assertEqual(self.snapshot(), before)
        reopened = StateStore(self.directory.name, db_filename="state.sqlite3")
        self.assertEqual([item["id"] for item in reopened.list_due_outbox(limit=1, claim_export_from=CUTOFF)], [fresh])
        self.assertEqual(self.snapshot(), before)

    def test_occurrence_boundary_inclusive_and_timezone_equivalent_not_recording_time(self):
        old = self.add(BEFORE)
        equal = self.add(CUTOFF)
        future = self.add(AFTER)
        equivalent = self.add("2020-02-03T10:05:06.000+06:00")
        self.assertEqual(self.ids(claim_export_from=CUTOFF), [equal, future, equivalent])
        self.assertNotIn(old, self.ids(claim_export_from=CUTOFF), "all rows were recorded later, which must not promote old claims")

    def test_empty_cutoff_preserves_previous_behavior_and_extra_request_kind(self):
        old = self.add()
        request = self.add(kind="extra_claim_request")
        self.assertEqual(set(self.ids()), {old, request})
        self.assertEqual(self.ids(claim_export_from=CUTOFF), [request])

    def test_grant_linked_exception_requires_explicit_extra_feature(self):
        old = self.add()
        grant = self.add(grant=True)
        self.assertEqual(self.ids(claim_export_from=CUTOFF), [])
        self.assertEqual(self.ids(claim_export_from=CUTOFF, preserve_grant_claims=True), [grant])
        self.assertNotIn(old, self.ids(claim_export_from=CUTOFF, preserve_grant_claims=True))

    def test_malformed_or_missing_occurrence_cannot_bypass_cutoff(self):
        self.add(payload="not-json")
        self.add(payload=json.dumps({"other": "value"}))
        self.add(payload=json.dumps({"occurredAt": "bad-time"}))
        fresh = self.add(CUTOFF)
        self.assertEqual(self.ids(claim_export_from=CUTOFF), [fresh])

    def test_actual_flush_sends_only_new_and_preserves_old_pending_then_no_replay(self):
        old = self.add()
        fresh = self.add(CUTOFF)
        before = self.snapshot({old})
        with self.settings(), patch.object(app, "baza_post", return_value={"ok": True}) as send:
            self.assertEqual(app.flush_integration_outbox(), {"enabled": True, "sent": 1, "retried": 0, "dead": 0})
            self.assertEqual(app.flush_integration_outbox()["sent"], 0)
            self.assertEqual(send.call_count, 1)
        self.assertEqual(self.snapshot({old}), before)
        self.assertEqual(self.ids(), [old])
        self.assertEqual(len(self.store.list_outbox(delivered=True)), 1)

    def test_actual_flush_preserves_extra_grant_recovery_when_extra_enabled(self):
        old = self.add()
        self.add(grant=True)
        before = self.snapshot({old})
        with self.settings(EXTRA_CLAIM_REQUESTS_ENABLED=True), patch.object(app, "baza_post", return_value={"ok": True}) as send:
            self.assertEqual(app.flush_integration_outbox()["sent"], 1)
            self.assertEqual(send.call_count, 1)
        self.assertEqual(self.snapshot({old}), before)

    def test_invalid_configuration_fails_closed_before_queue_read_or_http(self):
        self.add(CUTOFF)
        before = self.snapshot()
        with self.settings(INVALID_ENV_VALUES={"BAZA_CLAIM_EXPORT_FROM"}), patch.object(app, "baza_post") as send, patch.object(self.store, "list_due_outbox") as read:
            self.assertFalse(app.baza_integration_configured())
            self.assertFalse(app.flush_integration_outbox()["enabled"])
            send.assert_not_called()
            read.assert_not_called()
        self.assertEqual(self.snapshot(), before)

    def test_disabled_export_keeps_queue_untouched(self):
        self.add(CUTOFF)
        before = self.snapshot()
        with self.settings(CLAIM_EVENT_EXPORT_ENABLED=False), patch.object(app, "baza_post") as send:
            self.assertFalse(app.flush_integration_outbox()["enabled"])
            send.assert_not_called()
        self.assertEqual(self.snapshot(), before)


if __name__ == "__main__":
    unittest.main()
