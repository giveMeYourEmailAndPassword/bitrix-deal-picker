import concurrent.futures
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from state_store import (
    ExtraClaimGrantReconciliationRequiredError,
    ExtraClaimGrantUnavailableError,
    ExtraClaimRequestAssociationConflictError,
    IdempotencyConflictError,
    MIGRATION_MARKER,
    StateStore,
    StateStoreNotReadyError,
)


LOCAL_TZ = timezone(timedelta(hours=6))


class StateStoreTestCase(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="bitrix-state-store-")
        self.data_dir = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def make_store(self, **kwargs):
        return StateStore(self.data_dir, local_timezone=LOCAL_TZ, **kwargs)

    def write_json(self, filename, payload):
        path = self.data_dir / filename
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def raw_count(self, table):
        with sqlite3.connect(self.data_dir / "state.sqlite3") as connection:
            return connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


class TestInitialization(StateStoreTestCase):
    def test_invalid_timezone_env_cannot_crash_constructor_or_override_explicit_timezone(self):
        with patch.dict(os.environ, {"APP_TZ_OFFSET_HOURS": "not-a-number"}):
            explicit = StateStore(self.data_dir, local_timezone=LOCAL_TZ)
        self.assertEqual(explicit.local_timezone, LOCAL_TZ)
        self.assertTrue(explicit.readiness_check()["ok"])

        second_dir = self.data_dir / "fallback-timezone"
        with patch.dict(os.environ, {"APP_TZ_OFFSET_HOURS": "not-a-number"}):
            fallback = StateStore(second_dir)
        self.assertEqual(fallback.local_timezone.utcoffset(None), timedelta(hours=6))
        self.assertTrue(fallback.readiness_check()["ok"])

    def test_clean_install_is_ready_without_consuming_migration_marker(self):
        store = self.make_store()

        readiness = store.readiness_check()
        self.assertTrue(readiness["ok"])
        self.assertEqual(readiness["journalMode"], "wal")
        self.assertEqual(readiness["synchronous"], 2)
        self.assertEqual(readiness["migration"]["state"], "waiting_for_legacy")
        self.assertFalse(readiness["hasApplicationState"])

        with sqlite3.connect(store.db_path) as connection:
            journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
            schema_version = connection.execute(
                "SELECT value FROM meta WHERE key = 'schema_version'"
            ).fetchone()[0]
            marker = connection.execute(
                "SELECT value FROM meta WHERE key = ?", (MIGRATION_MARKER,)
            ).fetchone()
        self.assertEqual(journal_mode.lower(), "wal")
        self.assertEqual(schema_version, "2")
        self.assertIsNone(marker)
        with store._connect() as connection:
            self.assertEqual(connection.execute("PRAGMA synchronous").fetchone()[0], 2)

    def test_late_legacy_upload_is_imported_after_restart(self):
        first_store = self.make_store()
        self.assertTrue(first_store.readiness_check()["ok"])
        self.assertEqual(first_store.list_claims(), [])

        self.write_json(
            "claim_log.json",
            [
                {
                    "timestamp": "2026-08-01T10:00:00+06:00",
                    "managerId": "1001",
                    "managerName": "Manager A",
                    "dealId": "900",
                    "dealTitle": "Late upload",
                }
            ],
        )
        restarted_store = self.make_store()

        self.assertTrue(restarted_store.readiness_check()["ok"])
        self.assertEqual(restarted_store.readiness_check()["migration"]["state"], "completed")
        self.assertEqual(len(restarted_store.list_claims()), 1)

    def test_late_legacy_upload_cannot_mix_with_native_sqlite_state(self):
        first_store = self.make_store()
        first_store.set_rule("1001", enabled=False, daily_limit=2, note="native")
        first_store.append_claim(
            {
                "timestamp": "2026-08-01T10:00:00+06:00",
                "managerId": "1001",
                "dealId": "native-deal",
            }
        )
        self.assertEqual(first_store.readiness_check()["migration"]["state"], "native_state")
        self.assertTrue(first_store.readiness_check()["hasApplicationState"])
        legacy_path = self.write_json(
            "claim_log.json",
            [
                {
                    "timestamp": "2026-08-01T11:00:00+06:00",
                    "managerId": "1001",
                    "dealId": "legacy-deal",
                }
            ],
        )
        source_before = legacy_path.read_bytes()

        restarted_store = self.make_store()

        self.assertFalse(restarted_store.readiness_check()["ok"])
        self.assertIn("already contains application state", restarted_store.readiness_check()["error"])
        self.assertEqual(legacy_path.read_bytes(), source_before)
        self.assertEqual(self.raw_count("claim_events"), 1)
        with sqlite3.connect(first_store.db_path) as connection:
            deal_ids = {
                row[0] for row in connection.execute("SELECT deal_id FROM claim_events").fetchall()
            }
            marker = connection.execute(
                "SELECT 1 FROM meta WHERE key = ?", (MIGRATION_MARKER,)
            ).fetchone()
        self.assertEqual(deal_ids, {"native-deal"})
        self.assertIsNone(marker)

    def test_state_db_filename_env_is_scoped_to_data_dir(self):
        previous = os.environ.get("STATE_DB_FILENAME")
        os.environ["STATE_DB_FILENAME"] = "custom.sqlite3"
        try:
            store = self.make_store()
        finally:
            if previous is None:
                os.environ.pop("STATE_DB_FILENAME", None)
            else:
                os.environ["STATE_DB_FILENAME"] = previous
        self.assertEqual(store.db_path, (self.data_dir / "custom.sqlite3").resolve())
        self.assertTrue(store.db_path.exists())

    def test_database_filename_cannot_escape_data_dir(self):
        with self.assertRaises(ValueError):
            StateStore(self.data_dir, "../outside.sqlite3", local_timezone=LOCAL_TZ)

    def test_unknown_schema_version_fails_closed_without_downgrading_marker(self):
        db_path = self.data_dir / "state.sqlite3"
        with sqlite3.connect(db_path) as connection:
            connection.execute(
                "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO meta(key, value, updated_at) VALUES('schema_version', '99', 'future')"
            )
            connection.execute("CREATE TABLE future_only(id INTEGER PRIMARY KEY, payload TEXT)")
            objects_before = connection.execute(
                "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
            ).fetchall()

        restarted_store = self.make_store()

        self.assertFalse(restarted_store.readiness_check()["ok"])
        self.assertIn("unsupported state schema version 99", restarted_store.readiness_check()["error"])
        with sqlite3.connect(db_path) as connection:
            persisted = connection.execute(
                "SELECT value FROM meta WHERE key = 'schema_version'"
            ).fetchone()[0]
            objects_after = connection.execute(
                "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
            ).fetchall()
        self.assertEqual(persisted, "99")
        self.assertEqual(objects_after, objects_before)

    def test_corrupt_migration_marker_fails_closed_for_all_operations(self):
        store = self.make_store()
        with sqlite3.connect(store.db_path) as connection:
            connection.execute(
                "INSERT INTO meta(key, value, updated_at) VALUES(?, ?, ?)",
                (MIGRATION_MARKER, "not-json", "2026-08-01T00:00:00+06:00"),
            )

        restarted_store = self.make_store()

        self.assertFalse(restarted_store.readiness_check()["ok"])
        self.assertIn("marker is invalid JSON", restarted_store.readiness_check()["error"])
        with self.assertRaises(StateStoreNotReadyError):
            restarted_store.set_rule("1001", enabled=False)
        self.assertEqual(self.raw_count("manager_rules"), 0)


class TestLegacyMigration(StateStoreTestCase):
    def test_strict_migration_rejects_partial_manifest_without_importing(self):
        source = self.write_json(
            "access_rules.json",
            {"managers": {"1001": {"enabled": True, "dailyLimit": 2}}},
        )
        before = source.read_bytes()
        store = self.make_store(auto_initialize=False)

        store.initialize(require_complete_legacy_set=True)
        readiness = store.readiness_check()

        self.assertFalse(readiness["ok"])
        self.assertIn("complete four-file manifest", readiness["error"])
        self.assertEqual(source.read_bytes(), before)
        self.assertEqual(self.raw_count("manager_rules"), 0)
        with sqlite3.connect(store.db_path) as connection:
            marker = connection.execute(
                "SELECT 1 FROM meta WHERE key = ?", (MIGRATION_MARKER,)
            ).fetchone()
        self.assertIsNone(marker)

    def test_strict_migration_accepts_exact_four_file_manifest_and_records_counts(self):
        for filename, payload in {
            "access_rules.json": {"managers": {}},
            "claim_log.json": [],
            "reject_log.json": [],
            "greeting_log.json": [],
        }.items():
            self.write_json(filename, payload)
        store = self.make_store(auto_initialize=False)

        store.initialize(require_complete_legacy_set=True)
        readiness = store.readiness_check()

        self.assertTrue(readiness["ok"], readiness.get("error"))
        self.assertEqual(readiness["migration"]["state"], "completed")
        self.assertEqual(
            readiness["migration"]["details"]["files"],
            {
                "access_rules.json": 0,
                "claim_log.json": 0,
                "reject_log.json": 0,
                "greeting_log.json": 0,
            },
        )
        self.assertEqual(
            set(readiness["migration"]["details"]["sourceDigests"]),
            {
                "access_rules.json",
                "claim_log.json",
                "reject_log.json",
                "greeting_log.json",
            },
        )

    def test_strict_reopen_fails_if_retained_source_changed_after_migration(self):
        for filename, payload in {
            "access_rules.json": {"managers": {}},
            "claim_log.json": [],
            "reject_log.json": [],
            "greeting_log.json": [],
        }.items():
            self.write_json(filename, payload)
        first_store = self.make_store(auto_initialize=False)
        first_store.initialize(require_complete_legacy_set=True)
        self.assertTrue(first_store.readiness_check()["ok"])

        self.write_json(
            "claim_log.json",
            [
                {
                    "timestamp": "2026-08-17T10:00:00+06:00",
                    "managerId": "1001",
                    "dealId": "changed-after-migration",
                }
            ],
        )
        restarted = self.make_store(auto_initialize=False)
        restarted.initialize(require_complete_legacy_set=True)

        readiness = restarted.readiness_check()
        self.assertFalse(readiness["ok"])
        self.assertIn("changed after migration", readiness["error"])
        self.assertEqual(restarted._initialization_error.__class__.__name__, "StateStoreError")
        self.assertEqual(self.raw_count("claim_events"), 0)

    def test_strict_reopen_rejects_old_partial_migration_marker(self):
        self.write_json(
            "claim_log.json",
            [
                {
                    "timestamp": "2026-08-01T10:00:00+06:00",
                    "managerId": "1001",
                    "dealId": "900",
                }
            ],
        )
        non_strict_store = self.make_store()
        self.assertEqual(non_strict_store.readiness_check()["migration"]["state"], "completed")

        strict_store = self.make_store(auto_initialize=False)
        strict_store.initialize(require_complete_legacy_set=True)

        self.assertFalse(strict_store.readiness_check()["ok"])
        self.assertIn("does not cover every required", strict_store.readiness_check()["error"])

    def test_all_legacy_data_migrates_once_and_sources_are_unchanged(self):
        files = {
            "access_rules.json": {
                "managers": {
                    "1001": {"enabled": False, "dailyLimit": 3, "note": "vacation"},
                    "1002": {"enabled": True, "dailyLimit": None, "note": ""},
                }
            },
            "claim_log.json": [
                {
                    "timestamp": "2026-08-01T18:30:00+00:00",
                    "managerId": "1001",
                    "managerName": "Manager A",
                    "dealId": "100",
                    "dealTitle": "Turkey",
                    "legacyExtra": {"preserved": True},
                }
            ],
            "reject_log.json": [
                {
                    "timestamp": "2026-08-02T09:00:00+06:00",
                    "managerId": "1002",
                    "managerName": "Manager B",
                    "dealId": "101",
                    "dealTitle": "Egypt",
                    "stageId": "UC_OLD",
                    "direction": "Египет",
                    "reason": "duplicate",
                    "reasonLabel": "Дубль",
                }
            ],
            "greeting_log.json": [
                {
                    "timestamp": "2026-08-02T09:01:00+06:00",
                    "managerId": "1002",
                    "managerName": "Manager B",
                    "dealId": "101",
                    "direction": "Египет",
                    "confidence": "high",
                    "text": "Hello",
                    "status": "sent",
                    "autoSent": True,
                    "sendResult": {"ok": True},
                }
            ],
        }
        original_bytes = {}
        for filename, payload in files.items():
            path = self.write_json(filename, payload)
            original_bytes[filename] = path.read_bytes()

        store = self.make_store()

        self.assertTrue(store.readiness_check()["ok"])
        self.assertEqual(store.readiness_check()["migration"]["state"], "completed")
        self.assertEqual(store.get_rule("1001"), {"enabled": False, "dailyLimit": 3, "note": "vacation"})
        self.assertEqual(store.list_rules()["1002"]["dailyLimit"], None)
        claims = store.list_claims()
        self.assertEqual(len(claims), 1)
        self.assertEqual(claims[0]["legacyExtra"], {"preserved": True})
        # 18:30 UTC is the next calendar day in Bishkek.
        self.assertEqual(store.count_claims("1001", "2026-08-02", "2026-08-02"), 1)
        self.assertEqual(store.list_rejections()[0]["reason"], "duplicate")
        self.assertTrue(store.latest_greeting_by_deal("101")["autoSent"])
        self.assertEqual(set(store.list_manager_ids()), {"1001", "1002"})
        for filename, expected in original_bytes.items():
            self.assertEqual((self.data_dir / filename).read_bytes(), expected)

        # A second process/startup sees the marker and cannot duplicate rows.
        second_store = self.make_store()
        self.assertEqual(len(second_store.list_claims()), 1)
        self.assertEqual(len(second_store.list_rejections()), 1)
        self.assertEqual(len(second_store.list_greetings()), 1)

    def test_corrupt_present_json_blocks_every_import_and_keeps_sources(self):
        valid_path = self.write_json(
            "access_rules.json",
            {"managers": {"1001": {"enabled": False, "dailyLimit": 1}}},
        )
        corrupt_path = self.data_dir / "claim_log.json"
        corrupt_bytes = b'[{"managerId":"1001"'
        corrupt_path.write_bytes(corrupt_bytes)

        store = self.make_store()
        readiness = store.readiness_check()

        self.assertFalse(readiness["ok"])
        self.assertEqual(readiness["migration"]["state"], "error")
        self.assertIn("claim_log.json", readiness["error"])
        self.assertEqual(corrupt_path.read_bytes(), corrupt_bytes)
        self.assertTrue(valid_path.exists())
        self.assertEqual(self.raw_count("manager_rules"), 0)
        self.assertEqual(self.raw_count("claim_events"), 0)
        with sqlite3.connect(store.db_path) as connection:
            marker = connection.execute(
                "SELECT 1 FROM meta WHERE key = ?", (MIGRATION_MARKER,)
            ).fetchone()
        self.assertIsNone(marker)
        with self.assertRaises(StateStoreNotReadyError):
            store.set_rule("1001", enabled=True)
        with self.assertRaises(StateStoreNotReadyError):
            store.list_claims()

        # Repairing the source and restarting performs the untouched migration.
        corrupt_path.write_text("[]", encoding="utf-8")
        restarted_store = self.make_store()
        self.assertTrue(restarted_store.readiness_check()["ok"])
        self.assertEqual(restarted_store.get_rule("1001")["dailyLimit"], 1)

    def test_wrong_top_level_shape_is_an_error_not_an_empty_log(self):
        path = self.write_json("reject_log.json", {"events": []})
        before = path.read_bytes()

        store = self.make_store()

        self.assertFalse(store.readiness_check()["ok"])
        self.assertIn("expected list", store.readiness_check()["error"])
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(self.raw_count("reject_events"), 0)

    def test_semantically_invalid_list_item_blocks_migration(self):
        self.write_json("greeting_log.json", [{"dealId": "1"}, "not-an-object"])
        store = self.make_store()
        self.assertFalse(store.readiness_check()["ok"])
        self.assertIn("every event must be an object", store.readiness_check()["error"])
        self.assertEqual(self.raw_count("greeting_events"), 0)

    def test_semantically_invalid_manager_rule_blocks_migration(self):
        self.write_json("access_rules.json", {"managers": {"1001": "not-an-object"}})
        store = self.make_store()
        self.assertFalse(store.readiness_check()["ok"])
        self.assertIn("every manager rule must be an object", store.readiness_check()["error"])
        self.assertEqual(self.raw_count("manager_rules"), 0)

    def test_missing_event_identity_blocks_migration_without_rewriting_history(self):
        path = self.write_json(
            "claim_log.json",
            [{"timestamp": "2026-08-01T10:00:00+06:00", "managerId": "1001"}],
        )
        before = path.read_bytes()

        store = self.make_store()

        self.assertFalse(store.readiness_check()["ok"])
        self.assertIn("requires non-empty managerId and dealId", store.readiness_check()["error"])
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(self.raw_count("claim_events"), 0)

    def test_invalid_or_naive_legacy_timestamp_blocks_migration(self):
        for raw_timestamp, expected_error in (
            ("not-a-time", "invalid ISO timestamp"),
            ("2026-08-01T10:00:00", "timestamp has no timezone"),
        ):
            with self.subTest(raw_timestamp=raw_timestamp):
                with tempfile.TemporaryDirectory(prefix="bitrix-bad-time-") as data_dir:
                    path = Path(data_dir) / "reject_log.json"
                    path.write_text(
                        json.dumps(
                            [
                                {
                                    "timestamp": raw_timestamp,
                                    "managerId": "1001",
                                    "dealId": "deal-1",
                                }
                            ]
                        ),
                        encoding="utf-8",
                    )
                    before = path.read_bytes()
                    store = StateStore(data_dir, local_timezone=LOCAL_TZ)
                    self.assertFalse(store.readiness_check()["ok"])
                    self.assertIn(expected_error, store.readiness_check()["error"])
                    self.assertEqual(path.read_bytes(), before)
                    with sqlite3.connect(store.db_path) as connection:
                        count = connection.execute("SELECT COUNT(*) FROM reject_events").fetchone()[0]
                    self.assertEqual(count, 0)

    def test_migration_never_truncates_more_than_five_thousand_events(self):
        entries = [
            {
                "timestamp": f"2026-08-01T10:{index % 60:02d}:00+06:00",
                "managerId": "1001",
                "dealId": str(index),
                "sequence": index,
            }
            for index in range(5_205)
        ]
        self.write_json("claim_log.json", entries)

        store = self.make_store()
        imported = store.list_claims()

        self.assertEqual(len(imported), 5_205)
        self.assertEqual(imported[0]["sequence"], 0)
        self.assertEqual(imported[-1]["sequence"], 5_204)
        self.assertEqual(store.count_claims("1001", "2026-08-01", "2026-08-01"), 5_205)


class TestRulesAndConcurrency(StateStoreTestCase):
    def test_rule_defaults_and_invalid_limits_fail_closed(self):
        store = self.make_store()
        self.assertEqual(store.get_rule("missing"), {"enabled": True, "dailyLimit": None, "note": ""})
        for invalid_limit in ("-12", "not-a-number", True):
            with self.subTest(invalid_limit=invalid_limit):
                with self.assertRaises(ValueError):
                    store.set_rule(
                        "1001",
                        enabled=False,
                        daily_limit=invalid_limit,
                        note=None,
                    )
        with self.assertRaises(ValueError):
            store.set_rule("not-an-id", enabled=True)
        with self.assertRaises(ValueError):
            store.set_rule("1001", enabled="false")

    def test_concurrent_rule_writes_are_serialized_without_lost_managers(self):
        store = self.make_store(busy_timeout_ms=30_000)

        def write_rule(index):
            return store.set_rule(
                str(index),
                enabled=index % 2 == 0,
                daily_limit=index,
                note=f"manager-{index}",
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(write_rule, range(1, 81)))

        self.assertEqual(len(results), 80)
        rules = store.list_rules()
        self.assertEqual(len(rules), 80)
        for index in range(1, 81):
            self.assertEqual(rules[str(index)]["dailyLimit"], index)
            self.assertEqual(rules[str(index)]["enabled"], index % 2 == 0)

    def test_concurrent_writes_to_same_rule_leave_one_complete_valid_record(self):
        store = self.make_store(busy_timeout_ms=30_000)

        def write_rule(index):
            store.set_rule("1001", enabled=index % 2 == 0, daily_limit=index, note=f"write-{index}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            list(pool.map(write_rule, range(60)))

        rule = store.get_rule("1001")
        self.assertIn(rule["dailyLimit"], range(60))
        self.assertEqual(rule["note"], f"write-{rule['dailyLimit']}")
        self.assertEqual(rule["enabled"], rule["dailyLimit"] % 2 == 0)


class TestEventAPI(StateStoreTestCase):
    def test_greetings_are_queryable_by_claim_lifecycle(self):
        store = self.make_store()
        store.append_greeting(
            {
                "managerId": "1001",
                "dealId": "10",
                "operationKey": "claim:first",
                "text": "first",
                "status": "manual",
            }
        )
        store.append_greeting(
            {
                "managerId": "2002",
                "dealId": "10",
                "operationKey": "claim:second",
                "text": "second",
                "status": "manual",
            }
        )

        self.assertEqual(
            store.latest_greeting_by_operation("claim:first")["text"],
            "first",
        )
        self.assertEqual(
            store.latest_greeting_by_operation("claim:second")["managerId"],
            "2002",
        )

    def test_rejection_semantic_key_is_unique_across_processes(self):
        store = self.make_store()
        event = {
            "timestamp": "2026-08-02T12:00:00+06:00",
            "managerId": "1001",
            "dealId": "10",
            "semanticKey": "same-manager-deal-lifecycle",
        }
        store.append_reject(event)
        with self.assertRaises(sqlite3.IntegrityError):
            self.make_store().append_reject({**event, "reason": "duplicate"})
        self.assertEqual(
            store.list_rejection_semantic_keys("1001"),
            {"same-manager-deal-lifecycle"},
        )

    def test_append_list_count_filter_and_latest_greeting(self):
        store = self.make_store()
        first_claim = store.append_claim(
            {
                "timestamp": "2026-08-01T17:59:00+00:00",
                "managerId": "1001",
                "managerName": "Manager A",
                "dealId": "10",
                "dealTitle": "First",
            }
        )
        second_claim = store.append_claim(
            {
                "timestamp": "2026-08-01T18:01:00+00:00",
                "managerId": "1001",
                "dealId": "11",
            }
        )
        store.append_claim("1002", {"ID": "12", "TITLE": "Third"}, manager_name="Manager B")
        rejection = store.append_reject(
            {
                "timestamp": "2026-08-02T12:00:00+06:00",
                "managerId": "1001",
                "dealId": "10",
                "stageId": "UC_STAGE",
                "reason": "duplicate",
                "reasonLabel": "Дубль",
            }
        )
        store.append_greeting(
            {
                "timestamp": "2026-08-02T12:00:00+06:00",
                "managerId": "1001",
                "dealId": "10",
                "text": "failed",
                "status": "error",
            }
        )
        store.append_greeting(
            {
                "timestamp": "2026-08-02T12:01:00+06:00",
                "managerId": "1001",
                "dealId": "10",
                "text": "prepared",
                "status": "manual",
                "autoSent": False,
            }
        )

        self.assertRegex(first_claim["timestamp"], r"[+-]\d\d:\d\d$")
        self.assertEqual(first_claim["dealTitle"], "First")
        self.assertEqual(second_claim["dealId"], "11")
        self.assertEqual(rejection["stageId"], "UC_STAGE")
        # 17:59 UTC is Aug 1 in Bishkek, 18:01 UTC is Aug 2.
        self.assertEqual(store.count_claims("1001", "2026-08-01", "2026-08-01"), 1)
        self.assertEqual(store.count_claims("1001", "2026-08-02", "2026-08-02"), 1)
        self.assertEqual(len(store.list_claims(manager_id="1001", date_from="2026-08-02")), 1)
        self.assertEqual(store.count_rejections("1001", "2026-08-02", "2026-08-02"), 1)
        self.assertEqual(store.latest_greeting_by_deal("10")["text"], "prepared")
        self.assertEqual(store.latest_greeting_by_deal("10", statuses=None)["text"], "prepared")
        self.assertEqual(set(store.list_manager_ids()), {"1001", "1002"})

    def test_invalid_timestamp_and_date_filters_fail_loudly(self):
        store = self.make_store()
        with self.assertRaises(ValueError):
            store.append_claim({"timestamp": "yesterday", "managerId": "1", "dealId": "2"})
        with self.assertRaises(ValueError):
            store.count_claims("1", "01.08.2026", "2026-08-01")

    def test_list_limit_is_explicit_and_default_is_unbounded(self):
        store = self.make_store()
        for index in range(25):
            store.append_claim({"managerId": "1", "dealId": str(index), "sequence": index})
        self.assertEqual(len(store.list_claims()), 25)
        self.assertEqual([row["sequence"] for row in store.list_claims(limit=3)], [0, 1, 2])
        self.assertEqual(
            [row["sequence"] for row in store.list_claims(limit=3, descending=True)],
            [24, 23, 22],
        )


class TestClaimOperations(StateStoreTestCase):
    def test_succeeded_operation_can_be_resolved_by_its_exact_claim_marker(self):
        store = self.make_store()
        store.begin_claim_operation(
            "100",
            "1001",
            operation_key="resolved-lifecycle",
            request={"claimMarker": "claim:resolved-marker"},
        )
        store.finalize_claim_operation(
            "resolved-lifecycle",
            claim={"managerId": "1001", "dealId": "100"},
            expected_claim_marker="claim:resolved-marker",
        )

        resolved = store.find_succeeded_claim_operation_by_marker(
            "100", "claim:resolved-marker"
        )
        self.assertEqual(resolved["operationKey"], "resolved-lifecycle")
        self.assertIsNone(
            store.find_succeeded_claim_operation_by_marker(
                "100", "claim:unknown-marker"
            )
        )
        self.assertIsNone(
            store.find_succeeded_claim_operation_by_marker(
                "different-deal", "claim:resolved-marker"
            )
        )

    def test_unresolved_operations_are_filtered_by_manager_and_remote_uncertainty(self):
        store = self.make_store()
        store.begin_claim_operation("100", "1001", operation_key="pending")
        store.begin_claim_operation("101", "1001", operation_key="safe-failure")
        store.fail_claim_operation("safe-failure", "not sent", result={"remoteUpdated": False})
        store.begin_claim_operation("102", "1001", operation_key="uncertain")
        store.fail_claim_operation(
            "uncertain", "timeout", result={"remoteUpdateUncertain": True}
        )
        store.begin_claim_operation("103", "2002", operation_key="other-manager")

        unresolved = store.list_unresolved_claim_operations("1001")
        self.assertEqual(
            [operation["operationKey"] for operation in unresolved],
            ["pending", "uncertain"],
        )
        self.assertEqual(
            len(store.list_unresolved_claim_operations("1001", limit=1)),
            1,
        )
        self.assertEqual(
            [
                operation["operationKey"]
                for operation in store.list_unresolved_claim_operations_for_deal("100")
            ],
            ["pending"],
        )
        self.assertEqual(
            store.list_unresolved_claim_operations_for_deal("101"),
            [],
        )
        self.assertEqual(
            store.list_unresolved_claim_deal_ids(),
            {"100", "102", "103"},
        )

    def test_stale_attempt_marker_cannot_finalize_a_new_generation(self):
        store = self.make_store()
        store.begin_claim_operation(
            "100",
            "1001",
            operation_key="claim-generation",
            request={"claimMarker": "attempt-old"},
        )
        store.fail_claim_operation("claim-generation", "stale")
        retried = store.retry_failed_claim_operation(
            "100",
            "1001",
            operation_key="claim-generation",
            request={"claimMarker": "attempt-new"},
        )
        self.assertTrue(retried["retried"])
        with self.assertRaises(IdempotencyConflictError):
            store.finalize_claim_operation(
                "claim-generation",
                claim={"managerId": "1001", "dealId": "100"},
                expected_claim_marker="attempt-old",
            )
        finalized = store.finalize_claim_operation(
            "claim-generation",
            claim={"managerId": "1001", "dealId": "100"},
            expected_claim_marker="attempt-new",
        )
        self.assertTrue(finalized["transitioned"])

    def test_begin_is_idempotent_and_conflicting_identity_is_rejected(self):
        store = self.make_store()
        first = store.begin_claim_operation(
            "100", "1001", operation_key="request-abc", request={"source": "button"}
        )
        repeated = store.begin_claim_operation(
            "100", "1001", operation_key="request-abc", request={"source": "other"}
        )

        self.assertTrue(first["created"])
        self.assertFalse(first["retried"])
        self.assertFalse(repeated["created"])
        self.assertFalse(repeated["retried"])
        self.assertEqual(repeated["request"], {"source": "button"})
        self.assertEqual(len(store.list_claim_operations()), 1)
        with self.assertRaises(IdempotencyConflictError):
            store.begin_claim_operation("different", "1001", operation_key="request-abc")
        with self.assertRaises(IdempotencyConflictError):
            store.begin_claim_operation("100", "different", operation_key="request-abc")

    def test_finalize_is_atomic_and_repetition_never_duplicates_claim(self):
        store = self.make_store()
        store.begin_claim_operation("100", "1001", operation_key="claim-once")
        first = store.finalize_claim_operation(
            "claim-once",
            claim={
                "timestamp": "2026-08-03T10:00:00+06:00",
                "managerName": "Manager A",
                "dealTitle": "Deal",
            },
            result={"bitrixUpdated": True},
        )
        repeated = store.finalize_claim_operation(
            "claim-once",
            claim={"managerId": "1001", "dealId": "100", "dealTitle": "duplicate"},
            result={"bitrixUpdated": False},
        )

        self.assertTrue(first["transitioned"])
        self.assertEqual(first["status"], "succeeded")
        self.assertIsNotNone(first["claimEventId"])
        self.assertFalse(repeated["transitioned"])
        self.assertEqual(repeated["result"], {"bitrixUpdated": True})
        claims = store.list_claims()
        self.assertEqual(len(claims), 1)
        self.assertEqual(claims[0]["dealTitle"], "Deal")

    def test_failed_and_succeeded_operations_are_terminal(self):
        store = self.make_store()
        store.begin_claim_operation("100", "1001", operation_key="will-fail")
        failed = store.fail_claim_operation("will-fail", "Bitrix timeout", result={"retry": False})
        repeated_failure = store.fail_claim_operation("will-fail", "different")
        late_finalize = store.finalize_claim_operation(
            "will-fail", claim={"managerId": "1001", "dealId": "100"}
        )

        self.assertTrue(failed["transitioned"])
        self.assertEqual(failed["status"], "failed")
        self.assertFalse(repeated_failure["transitioned"])
        self.assertEqual(repeated_failure["error"], "Bitrix timeout")
        self.assertFalse(late_finalize["transitioned"])
        self.assertEqual(late_finalize["status"], "failed")
        self.assertEqual(store.list_claims(), [])

        store.begin_claim_operation("101", "1001", operation_key="will-succeed")
        store.finalize_claim_operation(
            "will-succeed", claim={"managerId": "1001", "dealId": "101"}
        )
        late_failure = store.fail_claim_operation("will-succeed", "too late")
        self.assertFalse(late_failure["transitioned"])
        self.assertEqual(late_failure["status"], "succeeded")
        self.assertIsNone(late_failure["error"])

    def test_failed_operation_can_be_retried_only_for_same_identity(self):
        store = self.make_store()
        store.begin_claim_operation(
            "100", "1001", operation_key="retry-me", request={"attempt": 1}
        )
        failed = store.fail_claim_operation(
            "retry-me", "temporary timeout", result={"remote": "unknown"}
        )
        self.assertIsNotNone(failed["finalizedAt"])

        with self.assertRaises(IdempotencyConflictError):
            store.begin_claim_operation(
                "another-deal", "1001", operation_key="retry-me", retry_failed=True
            )
        with self.assertRaises(IdempotencyConflictError):
            store.retry_failed_claim_operation(
                "100", "another-manager", operation_key="retry-me"
            )

        retried = store.retry_failed_claim_operation(
            "100", "1001", operation_key="retry-me", request={"attempt": 2}
        )

        self.assertFalse(retried["created"])
        self.assertTrue(retried["retried"])
        self.assertEqual(retried["status"], "pending")
        self.assertEqual(retried["request"], {"attempt": 2})
        self.assertIsNone(retried["result"])
        self.assertIsNone(retried["error"])
        self.assertIsNone(retried["claimEventId"])
        self.assertIsNone(retried["finalizedAt"])
        self.assertEqual(len(retried["attemptHistory"]), 1)
        self.assertEqual(retried["attemptHistory"][0]["error"], "temporary timeout")
        self.assertEqual(retried["attemptHistory"][0]["request"], {"attempt": 1})
        self.assertEqual(retried["attemptHistory"][0]["result"], {"remote": "unknown"})

        finalized = store.finalize_claim_operation(
            "retry-me",
            claim={"managerId": "1001", "dealId": "100"},
            result={"remote": "updated"},
        )
        self.assertEqual(finalized["status"], "succeeded")
        immutable = store.retry_failed_claim_operation(
            "100", "1001", operation_key="retry-me", request={"attempt": 3}
        )
        self.assertFalse(immutable["retried"])
        self.assertEqual(immutable["status"], "succeeded")
        self.assertEqual(immutable["request"], {"attempt": 2})
        self.assertEqual(len(store.list_claims()), 1)

    def test_pending_finalize_requires_one_matching_claim_event(self):
        store = self.make_store()
        store.begin_claim_operation("100", "1001", operation_key="strict-finalize")

        with self.assertRaisesRegex(ValueError, "requires a claim event"):
            store.finalize_claim_operation("strict-finalize")
        self.assertEqual(store.get_claim_operation("strict-finalize")["status"], "pending")
        self.assertEqual(store.list_claims(), [])

        with self.assertRaises(IdempotencyConflictError):
            store.finalize_claim_operation(
                "strict-finalize", claim={"managerId": "different", "dealId": "100"}
            )
        with self.assertRaises(IdempotencyConflictError):
            store.finalize_claim_operation(
                "strict-finalize", claim={"managerId": "1001", "dealId": "different"}
            )
        self.assertEqual(store.get_claim_operation("strict-finalize")["status"], "pending")
        self.assertEqual(store.list_claims(), [])

        finalized = store.finalize_claim_operation(
            "strict-finalize", claim={"dealTitle": "Authoritative identity"}
        )
        self.assertEqual(finalized["status"], "succeeded")
        self.assertEqual(len(store.list_claims()), 1)
        self.assertEqual(store.list_claims()[0]["managerId"], "1001")
        self.assertEqual(store.list_claims()[0]["dealId"], "100")

    def test_failed_deal_lock_can_be_reassigned_but_terminal_or_pending_cannot(self):
        store = self.make_store()
        store.begin_claim_operation("100", "1001", operation_key="claim:100")
        pending = store.reassign_failed_claim_operation(
            "100", "1002", operation_key="claim:100"
        )
        self.assertFalse(pending["reassigned"])
        self.assertEqual(pending["managerId"], "1001")

        store.fail_claim_operation("claim:100", "stale pending")
        with self.assertRaises(IdempotencyConflictError):
            store.reassign_failed_claim_operation(
                "different", "1002", operation_key="claim:100"
            )
        reassigned = store.reassign_failed_claim_operation(
            "100",
            "1002",
            operation_key="claim:100",
            request={"reason": "deal still available"},
        )
        self.assertTrue(reassigned["reassigned"])
        self.assertEqual(reassigned["status"], "pending")
        self.assertEqual(reassigned["managerId"], "1002")
        self.assertEqual(reassigned["attemptHistory"][0]["managerId"], "1001")

        store.finalize_claim_operation(
            "claim:100", claim={"managerId": "1002", "dealId": "100"}
        )
        terminal = store.reassign_failed_claim_operation(
            "100", "1003", operation_key="claim:100"
        )
        self.assertFalse(terminal["reassigned"])
        self.assertEqual(terminal["managerId"], "1002")

    def test_retry_does_not_restart_a_pending_operation(self):
        store = self.make_store()
        store.begin_claim_operation(
            "100", "1001", operation_key="already-pending", request={"attempt": 1}
        )
        repeated = store.retry_failed_claim_operation(
            "100", "1001", operation_key="already-pending", request={"attempt": 2}
        )
        self.assertFalse(repeated["retried"])
        self.assertEqual(repeated["status"], "pending")
        self.assertEqual(repeated["request"], {"attempt": 1})

    def test_concurrent_begin_creates_exactly_one_operation(self):
        store = self.make_store(busy_timeout_ms=30_000)

        def begin(_index):
            return store.begin_claim_operation("100", "1001", operation_key="same-key")

        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(begin, range(40)))

        self.assertEqual(sum(1 for result in results if result["created"]), 1)
        self.assertEqual(len(store.list_claim_operations()), 1)

    def test_concurrent_failed_retry_transitions_exactly_once(self):
        store = self.make_store(busy_timeout_ms=30_000)
        store.begin_claim_operation("100", "1001", operation_key="retry-race")
        store.fail_claim_operation("retry-race", "temporary")

        def retry(_index):
            return store.retry_failed_claim_operation(
                "100", "1001", operation_key="retry-race"
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(retry, range(40)))

        self.assertEqual(sum(1 for result in results if result["retried"]), 1)
        self.assertTrue(all(result["status"] == "pending" for result in results))
        operation = store.get_claim_operation("retry-race")
        self.assertEqual(operation["status"], "pending")
        self.assertIsNone(operation["error"])
        self.assertIsNone(operation["finalizedAt"])

    def test_unknown_operation_cannot_be_finalized_or_failed(self):
        store = self.make_store()
        with self.assertRaisesRegex(Exception, "not found"):
            store.finalize_claim_operation("missing")
        with self.assertRaisesRegex(Exception, "not found"):
            store.fail_claim_operation("missing", "error")


class TestSchemaV2IntegrationMigration(StateStoreTestCase):
    def create_v1_database(self):
        db_path = self.data_dir / "state.sqlite3"
        with sqlite3.connect(db_path) as connection:
            connection.executescript(
                """
                CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL);
                INSERT INTO meta(key, value, updated_at) VALUES('schema_version', '1', 'old');
                CREATE TABLE claim_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    event_date TEXT NOT NULL,
                    manager_id TEXT NOT NULL,
                    manager_name TEXT NOT NULL DEFAULT '',
                    deal_id TEXT NOT NULL,
                    deal_title TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'app',
                    operation_key TEXT UNIQUE,
                    created_at TEXT NOT NULL
                );
                INSERT INTO claim_events(
                    timestamp,event_date,manager_id,manager_name,deal_id,deal_title,
                    payload_json,source,operation_key,created_at
                ) VALUES(
                    '2026-08-16T23:59:00+06:00','2026-08-16','42','Manager','900','Deal',
                    '{"extraClaimRequestId":"req-historical"}',
                    'app','claim:900','2026-08-16T23:59:00+06:00'
                );
                """
            )
        return db_path

    def test_v1_claims_get_stable_uuid_and_exactly_one_backfill_outbox_item(self):
        self.create_v1_database()
        first = self.make_store()
        claim = first.list_claims()[0]
        outbox = [item for item in first.list_outbox() if item["kind"] == "claim_event"]

        self.assertEqual(first.readiness_check()["schemaVersion"], 2)
        self.assertRegex(
            claim["eventUuid"],
            r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
        )
        self.assertEqual(len(outbox), 1)
        self.assertEqual(outbox[0]["payload"]["eventUuid"], claim["eventUuid"])
        self.assertEqual(
            outbox[0]["payload"]["extraClaimRequestId"], "req-historical"
        )
        self.assertTrue(outbox[0]["payload"]["recovered"])

        reopened = self.make_store()
        self.assertEqual(reopened.list_claims()[0]["eventUuid"], claim["eventUuid"])
        self.assertEqual(
            len([item for item in reopened.list_outbox() if item["kind"] == "claim_event"]),
            1,
        )

    def test_interrupted_v1_backfill_rolls_back_version_and_retries_cleanly(self):
        db_path = self.create_v1_database()
        with patch.object(
            StateStore,
            "_enqueue_claim_export_in_connection",
            side_effect=RuntimeError("simulated interruption"),
        ):
            broken = self.make_store()
        self.assertFalse(broken.readiness_check()["ok"])
        with sqlite3.connect(db_path) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT value FROM meta WHERE key='schema_version'"
                ).fetchone()[0],
                "1",
            )

        recovered = self.make_store()
        self.assertTrue(recovered.readiness_check()["ok"])
        self.assertEqual(recovered.readiness_check()["schemaVersion"], 2)
        self.assertEqual(len(recovered.list_outbox()), 1)


class TestExtraClaimRequestsAndGrants(StateStoreTestCase):
    def approve_one(self, store, *, manager="42", date="2026-08-17", request_id="req-1"):
        request = store.create_extra_claim_request(
            manager,
            date,
            "Нужна ещё одна срочная заявка",
            taken_today_snapshot=4,
            daily_limit_snapshot=4,
        )
        store.apply_extra_claim_request_response(
            request["requestKey"],
            {"id": request_id, "status": "pending"},
        )
        return store.import_extra_claim_state(
            manager,
            date,
            {
                "request": {"id": request_id, "status": "approved"},
                "grants": [
                    {
                        "id": request_id,
                        "requestId": request_id,
                        "status": "approved",
                        "businessDate": date,
                    }
                ],
            },
        )

    def test_double_click_returns_one_request_but_rejection_allows_a_new_uuid(self):
        store = self.make_store()
        first = store.create_extra_claim_request(
            "42",
            "2026-08-17",
            "Клиент ждёт срочное предложение",
            taken_today_snapshot=4,
            daily_limit_snapshot=4,
        )
        repeated = store.create_extra_claim_request(
            "42",
            "2026-08-17",
            "Другой текст повторного клика",
            taken_today_snapshot=4,
            daily_limit_snapshot=4,
        )
        self.assertTrue(first["created"])
        self.assertFalse(repeated["created"])
        self.assertEqual(first["requestKey"], repeated["requestKey"])
        self.assertEqual(len([i for i in store.list_outbox() if i["kind"] == "extra_claim_request"]), 1)

        store.reject_extra_claim_request_locally(first["requestKey"], "Не подтверждено")
        later = store.create_extra_claim_request(
            "42",
            "2026-08-17",
            "Появилась новая подтверждённая причина",
            taken_today_snapshot=4,
            daily_limit_snapshot=4,
        )
        self.assertNotEqual(first["requestKey"], later["requestKey"])

    def test_decision_note_is_preserved_for_rejected_status(self):
        store = self.make_store()
        request = store.create_extra_claim_request(
            "42",
            "2026-08-17",
            "Нужна дополнительная заявка сегодня",
            taken_today_snapshot=3,
            daily_limit_snapshot=3,
        )
        state = store.import_extra_claim_state(
            "42",
            "2026-08-17",
            {
                "request": {
                    "requestKey": request["requestKey"],
                    "id": "req-rejected",
                    "status": "rejected",
                    "decisionNote": "Сначала обработайте текущие заявки",
                },
                "grants": [],
            },
        )
        self.assertEqual(state["request"]["status"], "rejected")
        self.assertEqual(
            state["request"]["rejectionReason"],
            "Сначала обработайте текущие заявки",
        )

    def test_one_grant_cannot_be_reserved_by_two_claims(self):
        store = self.make_store()
        self.approve_one(store)
        first = store.begin_claim_operation(
            "100",
            "42",
            operation_key="claim:first",
            require_extra_grant=True,
            business_date="2026-08-17",
        )
        self.assertEqual(first["extraClaimGrantId"], "req-1")
        with self.assertRaises(ExtraClaimGrantUnavailableError):
            store.begin_claim_operation(
                "101",
                "42",
                operation_key="claim:second",
                require_extra_grant=True,
                business_date="2026-08-17",
            )
        self.assertIsNone(store.get_claim_operation("claim:second"))

    def test_grant_is_scoped_to_its_bishkek_business_date_at_midnight(self):
        store = self.make_store()
        self.approve_one(store, date="2026-08-17")
        self.assertTrue(store.get_extra_claim_state("42", "2026-08-17")["grantAvailable"])
        self.assertFalse(store.get_extra_claim_state("42", "2026-08-18")["grantAvailable"])
        with self.assertRaises(ExtraClaimGrantUnavailableError):
            store.begin_claim_operation(
                "100",
                "42",
                operation_key="claim:after-midnight",
                require_extra_grant=True,
                business_date="2026-08-18",
            )

    def test_expired_grant_is_not_claimable_and_allows_a_new_request(self):
        store = self.make_store()
        request = store.create_extra_claim_request(
            "42",
            "2026-08-17",
            "Нужна ещё одна срочная заявка",
            taken_today_snapshot=4,
            daily_limit_snapshot=4,
        )
        expired = store.import_extra_claim_state(
            "42",
            "2026-08-17",
            {
                "request": {"id": "req-expired", "status": "approved"},
                "grants": [
                    {
                        "id": "req-expired",
                        "requestId": "req-expired",
                        "status": "approved",
                        "businessDate": "2026-08-17",
                        "expiresAt": "2000-01-01T00:00:00Z",
                    }
                ],
            },
        )
        self.assertFalse(expired["grantAvailable"])
        self.assertEqual(expired["request"]["status"], "expired")

        replacement = store.create_extra_claim_request(
            "42",
            "2026-08-17",
            "Причина повторного запроса после истечения",
            taken_today_snapshot=4,
            daily_limit_snapshot=4,
        )
        self.assertTrue(replacement["created"])
        self.assertNotEqual(request["requestKey"], replacement["requestKey"])

    def test_authoritative_empty_grants_revokes_approved_but_not_reserved_grant(self):
        store = self.make_store()
        self.approve_one(store)
        revoked = store.import_extra_claim_state(
            "42",
            "2026-08-17",
            {
                "request": {"id": "req-1", "status": "expired"},
                "grants": [],
            },
        )
        self.assertEqual(revoked["request"]["status"], "expired")
        self.assertFalse(revoked["grantAvailable"])
        with self.assertRaises(ExtraClaimGrantUnavailableError):
            store.begin_claim_operation(
                "100",
                "42",
                operation_key="claim:revoked",
                require_extra_grant=True,
                business_date="2026-08-17",
            )

        self.approve_one(store, request_id="req-held")
        store.begin_claim_operation(
            "101",
            "42",
            operation_key="claim:held-authoritative",
            require_extra_grant=True,
            business_date="2026-08-17",
        )
        held = store.import_extra_claim_state(
            "42",
            "2026-08-17",
            {
                "request": {"id": "req-held", "status": "expired"},
                "grants": [],
            },
        )
        self.assertFalse(held["grantAvailable"])
        self.assertTrue(
            store.get_extra_claim_state(
                "42", "2026-08-17", operation_key="claim:held-authoritative"
            )["grantAvailable"]
        )

    def test_definite_failure_releases_but_uncertainty_holds_the_grant(self):
        store = self.make_store()
        self.approve_one(store)
        store.begin_claim_operation(
            "100", "42", operation_key="claim:definite",
            require_extra_grant=True, business_date="2026-08-17",
        )
        store.fail_claim_operation(
            "claim:definite", "stage changed", result={"remoteUpdated": False}
        )
        self.assertTrue(store.get_extra_claim_state("42", "2026-08-17")["grantAvailable"])

        store.begin_claim_operation(
            "101", "42", operation_key="claim:uncertain",
            require_extra_grant=True, business_date="2026-08-17",
        )
        store.fail_claim_operation(
            "claim:uncertain", "timeout", result={"remoteUpdateUncertain": True}
        )
        self.assertFalse(store.get_extra_claim_state("42", "2026-08-17")["grantAvailable"])
        self.assertTrue(
            store.get_extra_claim_state(
                "42", "2026-08-17", operation_key="claim:uncertain"
            )["grantAvailable"]
        )

    def test_uncertainty_held_grant_blocks_reassignment_without_orphaning_it(self):
        store = self.make_store()
        self.approve_one(store)
        store.begin_claim_operation(
            "100",
            "42",
            operation_key="claim:held",
            require_extra_grant=True,
            business_date="2026-08-17",
        )
        store.fail_claim_operation(
            "claim:held", "timeout", result={"remoteUpdateUncertain": True}
        )

        with self.assertRaises(ExtraClaimGrantReconciliationRequiredError):
            store.reassign_failed_claim_operation(
                "100", "43", operation_key="claim:held"
            )

        operation = store.get_claim_operation("claim:held")
        self.assertEqual(operation["status"], "failed")
        self.assertEqual(operation["managerId"], "42")
        self.assertEqual(operation["extraClaimGrantId"], "req-1")
        self.assertTrue(
            store.get_extra_claim_state(
                "42", "2026-08-17", operation_key="claim:held"
            )["grantAvailable"]
        )

    def test_finalize_consumes_grant_and_claim_export_contains_request_id(self):
        store = self.make_store()
        self.approve_one(store, request_id="req-final")
        store.begin_claim_operation(
            "100", "42", operation_key="claim:final",
            require_extra_grant=True, business_date="2026-08-17",
        )
        finalized = store.finalize_claim_operation(
            "claim:final",
            claim={"timestamp": "2026-08-17T23:59:59+06:00"},
            app_version="test-version",
        )
        self.assertEqual(finalized["status"], "succeeded")
        self.assertFalse(store.get_extra_claim_state("42", "2026-08-17")["grantAvailable"])
        claim = store.list_claims()[0]
        self.assertEqual(claim["extraClaimRequestId"], "req-final")
        event = [item for item in store.list_outbox() if item["kind"] == "claim_event"][0]
        self.assertEqual(event["payload"]["extraClaimRequestId"], "req-final")
        self.assertEqual(event["payload"]["businessDate"], "2026-08-17")

    def test_stale_remote_snapshot_never_revives_consumed_request_or_grant(self):
        store = self.make_store()
        self.approve_one(store, request_id="req-consumed")
        store.begin_claim_operation(
            "100",
            "42",
            operation_key="claim:consumed",
            require_extra_grant=True,
            business_date="2026-08-17",
        )
        store.finalize_claim_operation(
            "claim:consumed",
            claim={"timestamp": "2026-08-17T12:00:00+06:00"},
        )

        stale = store.import_extra_claim_state(
            "42",
            "2026-08-17",
            {
                "request": {"id": "req-consumed", "status": "approved"},
                "grants": [
                    {
                        "id": "req-consumed",
                        "requestId": "req-consumed",
                        "status": "approved",
                        "businessDate": "2026-08-17",
                    }
                ],
            },
        )
        store.apply_extra_claim_request_response(
            stale["request"]["requestKey"],
            {"id": "req-consumed", "status": "pending"},
        )

        self.assertEqual(
            store.get_extra_claim_state("42", "2026-08-17")["request"]["status"],
            "consumed",
        )
        with sqlite3.connect(store.db_path) as connection:
            request_status = connection.execute(
                "SELECT status FROM extra_claim_requests WHERE external_id='req-consumed'"
            ).fetchone()[0]
            grant_status = connection.execute(
                "SELECT status FROM extra_claim_grants WHERE grant_id='req-consumed'"
            ).fetchone()[0]
        self.assertEqual(request_status, "consumed")
        self.assertEqual(grant_status, "consumed")

    def test_remote_request_identity_cannot_move_to_a_new_local_request(self):
        store = self.make_store()
        self.approve_one(store, request_id="req-old")
        store.begin_claim_operation(
            "100",
            "42",
            operation_key="claim:old",
            require_extra_grant=True,
            business_date="2026-08-17",
        )
        store.finalize_claim_operation(
            "claim:old",
            claim={"timestamp": "2026-08-17T12:00:00+06:00"},
        )
        replacement = store.create_extra_claim_request(
            "42",
            "2026-08-17",
            "Нужна следующая заявка после первой",
            taken_today_snapshot=5,
            daily_limit_snapshot=4,
        )

        with self.assertRaises(ExtraClaimRequestAssociationConflictError):
            store.apply_extra_claim_request_response(
                replacement["requestKey"],
                {"id": "req-old", "status": "approved"},
            )

        with sqlite3.connect(store.db_path) as connection:
            rows = connection.execute(
                "SELECT request_key, external_id, status FROM extra_claim_requests "
                "ORDER BY created_at, request_key"
            ).fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][1:], ("req-old", "consumed"))
        self.assertEqual(rows[1], (replacement["requestKey"], None, "queued"))

    def test_claim_export_uses_canonical_milliseconds_without_losing_local_precision(self):
        store = self.make_store()
        store.append_claim(
            {
                "timestamp": "2026-08-17T23:59:59.123456+06:00",
                "managerId": "42",
                "dealId": "100",
            }
        )

        claim = store.list_claims()[0]
        event = [
            item for item in store.list_outbox() if item["kind"] == "claim_event"
        ][0]
        self.assertEqual(
            claim["timestamp"], "2026-08-17T23:59:59.123456+06:00"
        )
        self.assertEqual(
            event["payload"]["occurredAt"],
            "2026-08-17T23:59:59.123+06:00",
        )

    def test_outbox_failure_backoff_is_persistent_and_delivery_is_idempotent(self):
        store = self.make_store()
        request = store.create_extra_claim_request(
            "42", "2026-08-17", "Нужна ещё одна заявка срочно",
            taken_today_snapshot=4, daily_limit_snapshot=4,
        )
        item = store.list_due_outbox(limit=1)[0]
        failed = store.mark_outbox_failed(item["id"], "timeout", base_delay_seconds=7)
        self.assertEqual(failed["attemptCount"], 1)
        self.assertGreater(failed["nextAttemptAt"], failed["updatedAt"])
        self.assertEqual(store.list_due_outbox(limit=1), [])
        delivered = store.mark_outbox_delivered(item["id"], {"ok": True})
        repeated = store.mark_outbox_delivered(item["id"], {"ok": False})
        self.assertIsNotNone(delivered["deliveredAt"])
        self.assertEqual(repeated["response"], {"ok": True})
        self.assertEqual(request["requestKey"], item["payload"]["requestKey"])

    def test_new_request_is_delivered_before_older_claim_backlog(self):
        store = self.make_store()
        for index in range(5):
            store.append_claim(
                {
                    "timestamp": "2026-08-17T09:00:00+06:00",
                    "managerId": "42",
                    "dealId": str(100 + index),
                }
            )
        request = store.create_extra_claim_request(
            "42",
            "2026-08-17",
            "Клиент ждёт срочный ответ по туру",
            taken_today_snapshot=5,
            daily_limit_snapshot=5,
        )
        due = store.list_due_outbox(limit=1)
        self.assertEqual(due[0]["kind"], "extra_claim_request")
        self.assertEqual(due[0]["payload"]["requestKey"], request["requestKey"])

    def test_grant_linked_claim_event_precedes_a_later_extra_request(self):
        store = self.make_store()
        self.approve_one(store, request_id="req-linked")
        for item in store.list_outbox(delivered=False):
            if item["kind"] == "extra_claim_request":
                store.mark_outbox_delivered(item["id"], {"ok": True})
        store.begin_claim_operation(
            "100",
            "42",
            operation_key="claim:linked",
            require_extra_grant=True,
            business_date="2026-08-17",
        )
        store.finalize_claim_operation(
            "claim:linked",
            claim={"timestamp": "2026-08-17T12:00:00+06:00"},
        )
        replacement = store.create_extra_claim_request(
            "42",
            "2026-08-17",
            "Нужна ещё одна заявка после использованной",
            taken_today_snapshot=5,
            daily_limit_snapshot=4,
        )

        due = store.list_due_outbox(limit=2)
        self.assertEqual(due[0]["kind"], "claim_event")
        self.assertEqual(due[0]["payload"]["extraClaimRequestId"], "req-linked")
        self.assertEqual(due[1]["kind"], "extra_claim_request")
        self.assertEqual(due[1]["payload"]["requestKey"], replacement["requestKey"])


class TestGreetingOutbox(StateStoreTestCase):
    REQUEST = {
        "claimMarker": "claim:greeting",
        "greetingRequested": True,
        "greetingContext": {
            "sessionId": "session-77",
            "direction": "Турция",
            # Unknown private data must not be persisted with the target.
            "accessToken": "must-not-be-stored",
        },
    }

    def create_job(self, store, operation_key="greeting-op"):
        started = store.begin_claim_operation(
            "700",
            "1001",
            operation_key=operation_key,
            request=self.REQUEST,
        )
        self.assertNotIn("greetingContext", started["request"])
        finalized = store.finalize_claim_operation(
            operation_key,
            claim={"managerId": "1001", "dealId": "700"},
            expected_claim_marker="claim:greeting",
        )
        self.assertTrue(finalized["greetingQueued"])
        return store.get_greeting_outbox(operation_key)

    def test_finalize_atomically_creates_exactly_one_minimal_outbox_job(self):
        store = self.make_store()
        job = self.create_job(store)

        self.assertEqual(job["operationKey"], "greeting-op")
        self.assertEqual(job["dealId"], "700")
        self.assertEqual(job["managerId"], "1001")
        self.assertEqual(job["sessionId"], "session-77")
        self.assertEqual(job["direction"], "Турция")
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["attemptCount"], 0)
        self.assertNotIn("leaseToken", job)
        with sqlite3.connect(store.db_path) as connection:
            raw_request = connection.execute(
                "SELECT request_json FROM claim_operations WHERE operation_key='greeting-op'"
            ).fetchone()[0]
        self.assertNotIn("accessToken", raw_request)
        self.assertIn("session-77", raw_request)

        replay = store.finalize_claim_operation(
            "greeting-op",
            claim={"managerId": "1001", "dealId": "700"},
        )
        self.assertFalse(replay["transitioned"])
        self.assertTrue(replay["greetingQueued"])
        self.assertEqual(self.raw_count("claim_events"), 1)
        self.assertEqual(self.raw_count("greeting_outbox"), 1)

    def test_extra_grant_finalize_queues_one_claim_export_and_one_greeting(self):
        store = self.make_store()
        request = store.create_extra_claim_request(
            "1001",
            "2026-08-17",
            "Клиент ждёт дополнительную заявку",
            taken_today_snapshot=4,
            daily_limit_snapshot=4,
        )
        store.apply_extra_claim_request_response(
            request["requestKey"], {"id": "req-cross-feature", "status": "pending"}
        )
        store.import_extra_claim_state(
            "1001",
            "2026-08-17",
            {
                "request": {"id": "req-cross-feature", "status": "approved"},
                "grants": [
                    {
                        "id": "req-cross-feature",
                        "requestId": "req-cross-feature",
                        "status": "approved",
                        "businessDate": "2026-08-17",
                    }
                ],
            },
        )
        store.begin_claim_operation(
            "700",
            "1001",
            operation_key="claim:cross-feature",
            request=self.REQUEST,
            require_extra_grant=True,
            business_date="2026-08-17",
        )

        finalized = store.finalize_claim_operation(
            "claim:cross-feature",
            claim={"timestamp": "2026-08-17T12:00:00+06:00"},
            expected_claim_marker="claim:greeting",
        )
        replay = store.finalize_claim_operation(
            "claim:cross-feature",
            claim={"timestamp": "2026-08-17T12:00:00+06:00"},
        )

        self.assertTrue(finalized["transitioned"])
        self.assertTrue(finalized["greetingQueued"])
        self.assertFalse(replay["transitioned"])
        self.assertTrue(replay["greetingQueued"])
        with sqlite3.connect(store.db_path) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM extra_claim_grants "
                    "WHERE grant_id='req-cross-feature'"
                ).fetchone()[0],
                "consumed",
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM extra_claim_requests "
                    "WHERE external_id='req-cross-feature'"
                ).fetchone()[0],
                "consumed",
            )
        claim_exports = [
            item
            for item in store.list_outbox()
            if item["kind"] == "claim_event"
            and item["payload"].get("extraClaimRequestId") == "req-cross-feature"
        ]
        self.assertEqual(len(claim_exports), 1)
        self.assertEqual(self.raw_count("claim_events"), 1)
        self.assertEqual(self.raw_count("greeting_outbox"), 1)

    def test_outbox_failure_rolls_back_claim_and_operation_finalize_together(self):
        store = self.make_store()
        store.begin_claim_operation(
            "700", "1001", operation_key="atomic", request=self.REQUEST
        )

        with patch.object(
            StateStore,
            "_enqueue_greeting_from_request",
            side_effect=RuntimeError("simulated outbox failure"),
        ):
            with self.assertRaisesRegex(RuntimeError, "simulated outbox failure"):
                store.finalize_claim_operation(
                    "atomic", claim={"managerId": "1001", "dealId": "700"}
                )

        self.assertEqual(store.get_claim_operation("atomic")["status"], "pending")
        self.assertEqual(self.raw_count("claim_events"), 0)
        self.assertEqual(self.raw_count("greeting_outbox"), 0)

    def test_two_workers_can_never_lease_the_same_job(self):
        store = self.make_store(busy_timeout_ms=30_000)
        job = self.create_job(store)
        now = datetime.fromisoformat(job["nextAttemptAt"]) + timedelta(seconds=1)

        def lease(token):
            return store.lease_greeting_outbox(token, now=now)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lease, ("worker-a", "worker-b")))

        self.assertEqual(sum(len(result) for result in results), 1)
        job = store.get_greeting_outbox("greeting-op")
        self.assertEqual(job["status"], "checking")
        self.assertEqual(job["attemptCount"], 1)
        self.assertNotIn("leaseToken", job)

    def test_exact_lease_reserves_only_the_requested_operation(self):
        store = self.make_store()
        self.create_job(store, operation_key="greeting-a")
        self.create_job(store, operation_key="greeting-b")

        leased = store.lease_exact_greeting_outbox(
            "greeting-b",
            "actor-worker",
        )

        self.assertTrue(leased["transitioned"])
        self.assertEqual(leased["operationKey"], "greeting-b")
        self.assertEqual(leased["status"], "checking")
        self.assertEqual(store.get_greeting_outbox("greeting-a")["status"], "pending")
        repeated = store.lease_exact_greeting_outbox(
            "greeting-b",
            "other-worker",
        )
        self.assertFalse(repeated["transitioned"])
        self.assertEqual(repeated["attemptCount"], 1)

    def test_stale_checking_lease_has_exactly_one_safe_new_owner(self):
        store = self.make_store(busy_timeout_ms=30_000)
        job = self.create_job(store)
        first_now = datetime.fromisoformat(job["nextAttemptAt"]) + timedelta(seconds=1)
        store.lease_greeting_outbox(
            "dead-worker", now=first_now, lease_seconds=10
        )
        stale_now = first_now + timedelta(seconds=11)

        def reclaim(token):
            return store.lease_greeting_outbox(
                token, now=stale_now, lease_seconds=10
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(reclaim, ("worker-b", "worker-c")))

        self.assertEqual(sum(len(result) for result in results), 1)
        reclaimed = next(result[0] for result in results if result)
        self.assertEqual(reclaimed["status"], "checking")
        self.assertEqual(reclaimed["attemptCount"], 2)

    def test_checked_job_crosses_dispatch_boundary_then_becomes_sent(self):
        store = self.make_store()
        self.create_job(store)
        leased = store.lease_greeting_outbox("worker-a")[0]
        self.assertEqual(leased["status"], "checking")
        checked = store.update_greeting_outbox_check(
            "greeting-op",
            "worker-a",
            session_id="verified-session",
            direction="Египет",
            text="Здравствуйте! Я ваш менеджер.",
        )
        self.assertTrue(checked["transitioned"])

        wrong_worker = store.mark_greeting_outbox_dispatching(
            "greeting-op", "worker-b"
        )
        self.assertFalse(wrong_worker["transitioned"])
        dispatching = store.mark_greeting_outbox_dispatching(
            "greeting-op", "worker-a"
        )
        self.assertTrue(dispatching["transitioned"])
        self.assertEqual(dispatching["status"], "dispatching")
        self.assertIsNotNone(dispatching["dispatchingAt"])
        self.assertEqual(store.lease_greeting_outbox("worker-b"), [])

        sent = store.mark_greeting_outbox_sent(
            "greeting-op", "worker-a", message_id="message-900"
        )
        self.assertTrue(sent["transitioned"])
        self.assertEqual(sent["status"], "sent")
        self.assertEqual(sent["messageId"], "message-900")
        self.assertIsNotNone(sent["finalizedAt"])
        self.assertNotIn("leaseToken", sent)

    def test_checking_failure_retries_only_before_send_and_is_bounded(self):
        store = self.make_store()
        job = self.create_job(store)
        first_now = datetime.fromisoformat(job["nextAttemptAt"]) + timedelta(seconds=1)
        store.lease_greeting_outbox(
            "worker-a", now=first_now, max_attempts=2
        )
        retry = store.retry_greeting_outbox_check(
            "greeting-op",
            "worker-a",
            error_code="history_timeout",
            delay_seconds=10,
            max_attempts=2,
            now=first_now,
        )
        self.assertEqual(retry["status"], "pending")
        self.assertEqual(
            store.lease_greeting_outbox(
                "worker-b", now=first_now + timedelta(seconds=9), max_attempts=2
            ),
            [],
        )
        second = store.lease_greeting_outbox(
            "worker-b", now=first_now + timedelta(seconds=10), max_attempts=2
        )[0]
        self.assertEqual(second["attemptCount"], 2)
        exhausted = store.retry_greeting_outbox_check(
            "greeting-op",
            "worker-b",
            error_code="history_timeout",
            max_attempts=2,
            now=first_now + timedelta(seconds=10),
        )
        self.assertEqual(exhausted["status"], "manual")
        self.assertEqual(exhausted["errorCode"], "history_timeout")
        self.assertEqual(
            store.lease_greeting_outbox(
                "worker-c", now=first_now + timedelta(hours=1), max_attempts=2
            ),
            [],
        )

    def test_stale_dispatching_is_uncertain_and_never_automatically_retried(self):
        store = self.make_store()
        self.create_job(store)
        store.lease_greeting_outbox("worker-a")
        store.update_greeting_outbox_check(
            "greeting-op", "worker-a", text="Здравствуйте!"
        )
        dispatching = store.mark_greeting_outbox_dispatching(
            "greeting-op", "worker-a"
        )
        recovered = store.recover_stale_greeting_dispatches(
            stale_after_seconds=300,
            now=datetime.fromisoformat(dispatching["dispatchingAt"])
            + timedelta(seconds=301),
        )

        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0]["status"], "uncertain")
        self.assertEqual(recovered[0]["errorCode"], "stale_dispatching")
        self.assertEqual(store.lease_greeting_outbox("worker-b"), [])

    def test_terminal_old_operation_is_not_backfilled_after_additive_schema_install(self):
        store = self.make_store()
        self.create_job(store, operation_key="historical")
        with sqlite3.connect(store.db_path) as connection:
            connection.execute("DELETE FROM greeting_outbox WHERE operation_key='historical'")
            connection.execute("DROP TABLE greeting_outbox")

        restarted = self.make_store()
        self.assertEqual(restarted.readiness_check()["schemaVersion"], 2)
        self.assertEqual(restarted.list_greeting_outbox(), [])
        replay = restarted.finalize_claim_operation(
            "historical", claim={"managerId": "1001", "dealId": "700"}
        )
        self.assertFalse(replay["transitioned"])
        self.assertFalse(replay["greetingQueued"])
        self.assertEqual(restarted.list_greeting_outbox(), [])

    def test_incomplete_or_disabled_greeting_request_does_not_create_job(self):
        store = self.make_store()
        for suffix, request in (
            ("disabled", {"greetingRequested": False, "greetingContext": {"sessionId": "1", "direction": "x"}}),
            ("missing-direction", {"greetingRequested": True, "greetingContext": {"sessionId": "1"}}),
            ("missing-context", {"greetingRequested": True}),
        ):
            operation_key = f"no-job-{suffix}"
            store.begin_claim_operation(
                suffix, "1001", operation_key=operation_key, request=request
            )
            finalized = store.finalize_claim_operation(
                operation_key, claim={"managerId": "1001", "dealId": suffix}
            )
            self.assertFalse(finalized["greetingQueued"])
        self.assertEqual(store.list_greeting_outbox(), [])
if __name__ == "__main__":
    unittest.main()
