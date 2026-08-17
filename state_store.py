"""Durable SQLite state for the Bitrix deal picker.

The application used to keep mutable state in four JSON files.  This module
provides a small, stdlib-only SQLite store and an all-or-nothing migration for
those files.  Legacy files are deliberately retained after a successful
migration: they are evidence and a recovery source, not temporary files.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional


SCHEMA_VERSION = 1
DEFAULT_DB_FILENAME = "state.sqlite3"
MIGRATION_MARKER = "legacy_json_migration_v1"

LEGACY_FILES = {
    "access_rules.json": dict,
    "claim_log.json": list,
    "reject_log.json": list,
    "greeting_log.json": list,
}

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class StateStoreError(RuntimeError):
    """Base exception raised by the state store."""


class StateStoreNotReadyError(StateStoreError):
    """Raised when a migration or database initialization failed."""


class LegacyMigrationError(StateStoreError):
    """Raised when a present legacy file cannot be migrated safely."""


class IdempotencyConflictError(StateStoreError):
    """Raised when an operation key is reused for another deal or manager."""


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=_json_default,
    )


class StateStore:
    """Connection-per-operation SQLite store.

    ``data_dir`` defaults to ``APP_DATA_DIR`` and the database filename to
    ``STATE_DB_FILENAME`` (``state.sqlite3``).  Only a filename is accepted,
    keeping the database inside ``APP_DATA_DIR``.

    Initialization errors are retained instead of being thrown from the
    constructor.  This lets the HTTP health endpoint report a useful 503.  All
    state-changing/query methods still fail closed with
    :class:`StateStoreNotReadyError` until the process is restarted after the
    underlying problem is fixed.
    """

    def __init__(
        self,
        data_dir: Optional[os.PathLike[str] | str] = None,
        db_filename: Optional[str] = None,
        *,
        local_timezone: Optional[tzinfo] = None,
        busy_timeout_ms: int = 15_000,
        auto_initialize: bool = True,
    ) -> None:
        raw_data_dir = data_dir or os.environ.get("APP_DATA_DIR") or Path(__file__).resolve().parent
        self.data_dir = Path(raw_data_dir).expanduser().resolve()
        filename = str(db_filename or os.environ.get("STATE_DB_FILENAME") or DEFAULT_DB_FILENAME).strip()
        if not filename or Path(filename).name != filename or filename in {".", ".."}:
            raise ValueError("STATE_DB_FILENAME must be a filename inside APP_DATA_DIR")
        self.db_path = self.data_dir / filename
        if local_timezone is None:
            try:
                offset = int(os.environ.get("APP_TZ_OFFSET_HOURS", "6"))
            except (TypeError, ValueError):
                offset = 6
            offset = max(-12, min(14, offset))
            self.local_timezone = timezone(timedelta(hours=offset))
        else:
            self.local_timezone = local_timezone
        self.busy_timeout_ms = max(1, int(busy_timeout_ms))
        self._initialization_error: Optional[BaseException] = None
        self._initialized = False
        self._require_complete_legacy_set = False

        if auto_initialize:
            self.initialize()

    def initialize(self, *, require_complete_legacy_set: bool = False) -> None:
        """Create/validate the database and migrate legacy JSON exactly once."""

        if self._initialized or self._initialization_error is not None:
            return
        self._require_complete_legacy_set = bool(require_complete_legacy_set)
        try:
            self.data_dir.mkdir(parents=True, exist_ok=True)
            self._initialize_schema()
            self._migrate_legacy_json_once()
            self._initialized = True
        except Exception as exc:  # keep health/readiness available on boot failure
            self._initialization_error = exc

    # ------------------------------------------------------------------
    # Database lifecycle
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.db_path),
            timeout=self.busy_timeout_ms / 1000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA foreign_keys = ON")
        # Claim leases/audit events coordinate irreversible remote CRM writes.
        # FULL makes a committed local transaction survive host/power loss,
        # which is more important here than the small extra fsync latency.
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @contextmanager
    def _transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize_schema(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            schema_row = connection.execute(
                "SELECT value FROM meta WHERE key = 'schema_version'"
            ).fetchone()
            if schema_row and schema_row["value"] != str(SCHEMA_VERSION):
                raise StateStoreError(
                    f"unsupported state schema version {schema_row['value']}; expected {SCHEMA_VERSION}"
                )

            schema_sql = (
                """
                CREATE TABLE IF NOT EXISTS manager_rules (
                    manager_id TEXT PRIMARY KEY,
                    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
                    daily_limit INTEGER CHECK (daily_limit IS NULL OR daily_limit >= 0),
                    note TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS claim_events (
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

                CREATE TABLE IF NOT EXISTS reject_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    event_date TEXT NOT NULL,
                    manager_id TEXT NOT NULL,
                    manager_name TEXT NOT NULL DEFAULT '',
                    deal_id TEXT NOT NULL,
                    deal_title TEXT NOT NULL DEFAULT '',
                    stage_id TEXT NOT NULL DEFAULT '',
                    direction TEXT NOT NULL DEFAULT '',
                    reason TEXT NOT NULL DEFAULT 'other',
                    reason_label TEXT NOT NULL DEFAULT '',
                    selection_token_hash TEXT NOT NULL DEFAULT '',
                    semantic_key TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'app',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS greeting_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    event_date TEXT NOT NULL,
                    manager_id TEXT NOT NULL,
                    manager_name TEXT NOT NULL DEFAULT '',
                    deal_id TEXT NOT NULL,
                    operation_key TEXT NOT NULL DEFAULT '',
                    direction TEXT NOT NULL DEFAULT '',
                    confidence TEXT NOT NULL DEFAULT '',
                    text TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'manual',
                    auto_sent INTEGER NOT NULL DEFAULT 0 CHECK (auto_sent IN (0, 1)),
                    payload_json TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'app',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS claim_operations (
                    operation_key TEXT PRIMARY KEY,
                    deal_id TEXT NOT NULL,
                    manager_id TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('pending', 'succeeded', 'failed')),
                    request_json TEXT,
                    result_json TEXT,
                    error TEXT,
                    attempt_history_json TEXT NOT NULL DEFAULT '[]',
                    claim_event_id INTEGER REFERENCES claim_events(id),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    finalized_at TEXT
                );

                CREATE TABLE IF NOT EXISTS greeting_outbox (
                    operation_key TEXT PRIMARY KEY
                        REFERENCES claim_operations(operation_key),
                    deal_id TEXT NOT NULL,
                    manager_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN (
                            'pending', 'checking', 'dispatching',
                            'sent', 'manual', 'uncertain'
                        )),
                    text TEXT NOT NULL DEFAULT '',
                    attempt_count INTEGER NOT NULL DEFAULT 0
                        CHECK (attempt_count >= 0),
                    lease_token TEXT NOT NULL DEFAULT '',
                    leased_at TEXT,
                    lease_expires_at TEXT,
                    next_attempt_at TEXT,
                    error_code TEXT NOT NULL DEFAULT '',
                    message_id TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    dispatching_at TEXT,
                    finalized_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_claim_events_manager_date
                    ON claim_events(manager_id, event_date);
                CREATE INDEX IF NOT EXISTS idx_reject_events_manager_date
                    ON reject_events(manager_id, event_date);
                CREATE INDEX IF NOT EXISTS idx_greeting_events_deal
                    ON greeting_events(deal_id, id DESC);
                CREATE INDEX IF NOT EXISTS idx_claim_operations_deal
                    ON claim_operations(deal_id);
                CREATE INDEX IF NOT EXISTS idx_claim_operations_manager_status
                    ON claim_operations(manager_id, status);
                CREATE INDEX IF NOT EXISTS idx_claim_operations_status_deal
                    ON claim_operations(status, deal_id);
                CREATE INDEX IF NOT EXISTS idx_greeting_outbox_work
                    ON greeting_outbox(status, next_attempt_at, lease_expires_at, created_at);
                CREATE INDEX IF NOT EXISTS idx_greeting_outbox_manager_status
                    ON greeting_outbox(manager_id, status);
                """
            )
            for statement in schema_sql.split(";"):
                if statement.strip():
                    connection.execute(statement)
            operation_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(claim_operations)")
            }
            if "attempt_history_json" not in operation_columns:
                connection.execute(
                    "ALTER TABLE claim_operations "
                    "ADD COLUMN attempt_history_json TEXT NOT NULL DEFAULT '[]'"
                )
            reject_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(reject_events)")
            }
            if "selection_token_hash" not in reject_columns:
                connection.execute(
                    "ALTER TABLE reject_events "
                    "ADD COLUMN selection_token_hash TEXT NOT NULL DEFAULT ''"
                )
            if "semantic_key" not in reject_columns:
                connection.execute(
                    "ALTER TABLE reject_events "
                    "ADD COLUMN semantic_key TEXT NOT NULL DEFAULT ''"
                )
            greeting_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(greeting_events)")
            }
            if "operation_key" not in greeting_columns:
                connection.execute(
                    "ALTER TABLE greeting_events "
                    "ADD COLUMN operation_key TEXT NOT NULL DEFAULT ''"
                )
            # Older development builds keyed the uncertain auto-send
            # reservation only by dealId, which incorrectly crossed claim
            # lifecycles. Recreate it against the operation key.
            connection.execute("DROP INDEX IF EXISTS idx_greeting_auto_attempt_once")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_greeting_events_operation "
                "ON greeting_events(operation_key, id DESC)"
            )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_greeting_auto_attempt_once "
                "ON greeting_events(operation_key) "
                "WHERE status = 'sending' AND operation_key <> ''"
            )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_reject_selection_token_once "
                "ON reject_events(selection_token_hash) WHERE selection_token_hash <> ''"
            )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_reject_semantic_once "
                "ON reject_events(semantic_key) WHERE semantic_key <> ''"
            )
            now = self._now_iso()
            if not schema_row:
                connection.execute(
                    "INSERT INTO meta(key, value, updated_at) VALUES('schema_version', ?, ?)",
                    (str(SCHEMA_VERSION), now),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _ensure_ready(self) -> None:
        if self._initialization_error is not None:
            raise StateStoreNotReadyError(str(self._initialization_error)) from self._initialization_error
        if not self._initialized:
            raise StateStoreNotReadyError("state store initialization is gated by application readiness")

    def _migration_marker_from_connection(
        self,
        connection: sqlite3.Connection,
    ) -> Optional[Dict[str, Any]]:
        row = connection.execute(
            "SELECT value FROM meta WHERE key = ?", (MIGRATION_MARKER,)
        ).fetchone()
        if not row:
            return None
        try:
            marker = json.loads(row["value"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise StateStoreError("legacy migration marker is invalid JSON") from exc
        if (
            not isinstance(marker, dict)
            or not isinstance(marker.get("completedAt"), str)
            or not isinstance(marker.get("files"), dict)
            or marker.get("sourceFilesRetained") is not True
        ):
            raise StateStoreError("legacy migration marker has an invalid structure")
        file_counts = marker["files"]
        if any(
            not isinstance(count, int) or isinstance(count, bool) or count < 0
            for count in file_counts.values()
        ):
            raise StateStoreError("legacy migration marker has invalid file counts")
        if self._require_complete_legacy_set and set(file_counts) != set(LEGACY_FILES):
            raise StateStoreError("legacy migration marker does not cover every required source file")
        source_digests = marker.get("sourceDigests")
        if self._require_complete_legacy_set and (
            not isinstance(source_digests, dict)
            or set(source_digests) != set(LEGACY_FILES)
        ):
            raise StateStoreError(
                "legacy migration marker does not contain every required source digest"
            )
        if source_digests is not None:
            if not isinstance(source_digests, dict) or set(source_digests) != set(file_counts):
                raise StateStoreError("legacy migration marker has invalid source digests")
            for filename, expected_digest in source_digests.items():
                if (
                    not isinstance(expected_digest, str)
                    or not re.fullmatch(r"[0-9a-f]{64}", expected_digest)
                ):
                    raise StateStoreError("legacy migration marker has invalid source digests")
                source_path = self.data_dir / filename
                try:
                    current_digest = hashlib.sha256(source_path.read_bytes()).hexdigest()
                except OSError as exc:
                    raise StateStoreError(
                        "a retained legacy migration source is missing or unreadable"
                    ) from exc
                if current_digest != expected_digest:
                    raise StateStoreError(
                        "a retained legacy migration source changed after migration"
                    )
        return marker

    def readiness_check(self) -> Dict[str, Any]:
        """Return a health-safe readiness result without exposing file contents."""

        if not self._initialized and self._initialization_error is None:
            return {
                "ok": False,
                "database": str(self.db_path),
                "schemaVersion": SCHEMA_VERSION,
                "migration": {"state": "not_initialized", "marker": MIGRATION_MARKER},
                "error": "state store initialization has not run",
            }
        if self._initialization_error is not None:
            return {
                "ok": False,
                "database": str(self.db_path),
                "schemaVersion": SCHEMA_VERSION,
                "migration": {"state": "error", "marker": MIGRATION_MARKER},
                "error": str(self._initialization_error),
            }
        try:
            with self._connect() as connection:
                quick_check = connection.execute("PRAGMA quick_check(1)").fetchone()[0]
                journal_mode = connection.execute("PRAGMA journal_mode").fetchone()[0]
                synchronous = int(connection.execute("PRAGMA synchronous").fetchone()[0])
                marker = self._migration_marker_from_connection(connection)
                schema_row = connection.execute(
                    "SELECT value FROM meta WHERE key = 'schema_version'"
                ).fetchone()
                application_rows = sum(
                    int(connection.execute(f"SELECT EXISTS(SELECT 1 FROM {table} LIMIT 1)").fetchone()[0])
                    for table in (
                        "manager_rules",
                        "claim_events",
                        "reject_events",
                        "greeting_events",
                        "claim_operations",
                        "greeting_outbox",
                    )
                )
            if quick_check != "ok":
                raise StateStoreError(f"SQLite quick_check failed: {quick_check}")
            if synchronous != 2:
                raise StateStoreError("SQLite synchronous mode is not FULL")
            return {
                "ok": True,
                "database": str(self.db_path),
                "schemaVersion": int(schema_row["value"]) if schema_row else SCHEMA_VERSION,
                "journalMode": str(journal_mode).lower(),
                "synchronous": synchronous,
                "hasApplicationState": bool(application_rows or marker),
                "migration": {
                    "state": (
                        "completed"
                        if marker
                        else "native_state"
                        if application_rows
                        else "waiting_for_legacy"
                    ),
                    "marker": MIGRATION_MARKER,
                    "details": marker,
                },
            }
        except BaseException as exc:
            return {
                "ok": False,
                "database": str(self.db_path),
                "schemaVersion": SCHEMA_VERSION,
                "migration": {"state": "error", "marker": MIGRATION_MARKER},
                "error": str(exc),
            }

    # ------------------------------------------------------------------
    # Legacy migration
    # ------------------------------------------------------------------

    def _load_legacy_files(self) -> tuple[Dict[str, Any], Dict[str, str]]:
        loaded: Dict[str, Any] = {}
        source_digests: Dict[str, str] = {}
        errors = []
        present_files = {
            filename for filename in LEGACY_FILES if (self.data_dir / filename).exists()
        }
        if self._require_complete_legacy_set and present_files != set(LEGACY_FILES):
            missing_count = len(set(LEGACY_FILES) - present_files)
            raise LegacyMigrationError(
                "Strict legacy migration requires the complete four-file manifest "
                f"(missing={missing_count}); source files were retained and no data was imported"
            )
        for filename, expected_type in LEGACY_FILES.items():
            path = self.data_dir / filename
            if not path.exists():
                continue
            try:
                source_bytes = path.read_bytes()
                payload = json.loads(source_bytes.decode("utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                errors.append(f"{filename}: invalid JSON ({exc})")
                continue
            if not isinstance(payload, expected_type):
                errors.append(
                    f"{filename}: expected {expected_type.__name__}, got {type(payload).__name__}"
                )
                continue
            if filename == "access_rules.json" and not isinstance(payload.get("managers", {}), dict):
                errors.append("access_rules.json: 'managers' must be an object")
                continue
            if filename == "access_rules.json" and any(
                not isinstance(rule, dict) for rule in payload.get("managers", {}).values()
            ):
                errors.append("access_rules.json: every manager rule must be an object")
                continue
            if filename == "access_rules.json":
                for manager_id, rule in payload.get("managers", {}).items():
                    if not re.fullmatch(r"[1-9]\d{0,19}", str(manager_id)):
                        errors.append("access_rules.json: manager id must be a positive numeric ID")
                        break
                    if "enabled" in rule and not isinstance(rule["enabled"], bool):
                        errors.append(
                            f"access_rules.json: manager {manager_id!s} has a non-boolean enabled value"
                        )
                        break
                    daily_limit = rule.get("dailyLimit")
                    if daily_limit not in (None, "") and (
                        isinstance(daily_limit, bool)
                        or not isinstance(daily_limit, int)
                        or daily_limit < 0
                    ):
                        errors.append(
                            f"access_rules.json: manager {manager_id!s} has an invalid dailyLimit"
                        )
                        break
                if errors:
                    continue
            if expected_type is list and any(not isinstance(item, dict) for item in payload):
                errors.append(f"{filename}: every event must be an object")
                continue
            if expected_type is list:
                for index, entry in enumerate(payload):
                    manager_id = str(entry.get("managerId") or entry.get("manager_id") or "")
                    deal_id = str(entry.get("dealId") or entry.get("deal_id") or "")
                    if not manager_id or not deal_id:
                        errors.append(
                            f"{filename}: event {index} requires non-empty managerId and dealId"
                        )
                        break
                    raw_timestamp = entry.get("timestamp")
                    if not isinstance(raw_timestamp, str) or not raw_timestamp.strip():
                        errors.append(f"{filename}: event {index} requires an ISO timestamp")
                        break
                    raw_timestamp = raw_timestamp.strip()
                    try:
                        parsed_timestamp = datetime.fromisoformat(
                            raw_timestamp[:-1] + "+00:00"
                            if raw_timestamp.endswith("Z")
                            else raw_timestamp
                        )
                    except ValueError:
                        errors.append(f"{filename}: event {index} has an invalid ISO timestamp")
                        break
                    if parsed_timestamp.tzinfo is None or parsed_timestamp.utcoffset() is None:
                        errors.append(f"{filename}: event {index} timestamp has no timezone")
                        break
                if errors:
                    continue
            loaded[filename] = payload
            source_digests[filename] = hashlib.sha256(source_bytes).hexdigest()
        if errors:
            raise LegacyMigrationError(
                "Legacy state migration stopped; source files were retained: " + "; ".join(errors)
            )
        return loaded, source_digests

    def _migrate_legacy_json_once(self) -> None:
        with self._connect() as connection:
            if self._migration_marker_from_connection(connection) is not None:
                return

        loaded, source_digests = self._load_legacy_files()
        # A clean install is ready, but deliberately remains eligible for a
        # later migration if the operator uploads the legacy files and restarts.
        if not loaded:
            return

        migrated_at = self._now_iso()
        with self._transaction(immediate=True) as connection:
            if self._migration_marker_from_connection(connection) is not None:
                return

            state_counts = {
                table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in (
                    "manager_rules",
                    "claim_events",
                    "reject_events",
                    "greeting_events",
                    "claim_operations",
                    "greeting_outbox",
                )
            }
            if any(state_counts.values()):
                raise LegacyMigrationError(
                    "Legacy state migration stopped because SQLite already contains application state; "
                    "source files were retained and no data was imported"
                )

            access_rules = loaded.get("access_rules.json", {"managers": {}})
            for manager_id, rule in access_rules.get("managers", {}).items():
                rule = rule if isinstance(rule, dict) else {}
                self._set_rule_in_connection(
                    connection,
                    manager_id,
                    enabled=rule.get("enabled", True) is not False,
                    daily_limit=rule.get("dailyLimit"),
                    note=rule.get("note", ""),
                    timestamp=migrated_at,
                )

            for entry in loaded.get("claim_log.json", []):
                self._insert_claim(connection, entry, source="legacy_json", migration=True)
            for entry in loaded.get("reject_log.json", []):
                self._insert_reject(connection, entry, source="legacy_json", migration=True)
            for entry in loaded.get("greeting_log.json", []):
                self._insert_greeting(connection, entry, source="legacy_json", migration=True)

            summary = {
                "completedAt": migrated_at,
                "files": {
                    filename: (
                        len(payload.get("managers", {}))
                        if filename == "access_rules.json"
                        else len(payload)
                    )
                    for filename, payload in loaded.items()
                },
                "sourceDigests": source_digests,
                "sourceFilesRetained": True,
            }
            connection.execute(
                "INSERT INTO meta(key, value, updated_at) VALUES(?, ?, ?)",
                (MIGRATION_MARKER, _json_dumps(summary), migrated_at),
            )

    # ------------------------------------------------------------------
    # Time and row helpers
    # ------------------------------------------------------------------

    def _now_iso(self) -> str:
        return datetime.now(self.local_timezone).isoformat(timespec="microseconds")

    def _normalize_timestamp(self, value: Any, *, migration: bool = False) -> tuple[str, str]:
        if value in (None, ""):
            parsed = datetime.now(self.local_timezone)
        elif isinstance(value, datetime):
            parsed = value
        else:
            raw = str(value).strip()
            try:
                parsed = datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith("Z") else raw)
            except ValueError:
                raise ValueError("timestamp must be an ISO-8601 datetime")
        # Match the old application's behavior for legacy naive timestamps.
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        normalized = parsed.isoformat(timespec="microseconds")
        event_date = parsed.astimezone(self.local_timezone).date().isoformat()
        return normalized, event_date

    @staticmethod
    def _normalize_date_filter(value: Optional[str], name: str) -> Optional[str]:
        if value in (None, ""):
            return None
        value = str(value)
        if not _DATE_RE.fullmatch(value):
            raise ValueError(f"{name} must be YYYY-MM-DD")
        return value

    @staticmethod
    def _payload_from_row(row: sqlite3.Row) -> Dict[str, Any]:
        try:
            payload = json.loads(row["payload_json"])
        except (json.JSONDecodeError, TypeError):
            payload = {}
        return payload if isinstance(payload, dict) else {}

    # ------------------------------------------------------------------
    # Manager rules
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_daily_limit(value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        if isinstance(value, bool):
            raise ValueError("daily_limit must be a non-negative integer or empty")
        try:
            normalized = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("daily_limit must be a non-negative integer or empty") from exc
        if normalized < 0 or normalized > 100_000:
            raise ValueError("daily_limit must be between 0 and 100000")
        return normalized

    def _set_rule_in_connection(
        self,
        connection: sqlite3.Connection,
        manager_id: Any,
        *,
        enabled: bool,
        daily_limit: Any,
        note: Any,
        timestamp: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO manager_rules(manager_id, enabled, daily_limit, note, updated_at)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(manager_id) DO UPDATE SET
                enabled=excluded.enabled,
                daily_limit=excluded.daily_limit,
                note=excluded.note,
                updated_at=excluded.updated_at
            """,
            (
                str(manager_id),
                1 if enabled else 0,
                self._normalize_daily_limit(daily_limit),
                str(note or ""),
                timestamp,
            ),
        )

    def get_rule(self, manager_id: Any) -> Dict[str, Any]:
        self._ensure_ready()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT enabled, daily_limit, note FROM manager_rules WHERE manager_id = ?",
                (str(manager_id),),
            ).fetchone()
        if not row:
            return {"enabled": True, "dailyLimit": None, "note": ""}
        return {
            "enabled": bool(row["enabled"]),
            "dailyLimit": row["daily_limit"],
            "note": row["note"],
        }

    get_manager_rule = get_rule

    def set_rule(
        self,
        manager_id: Any,
        *,
        enabled: bool = True,
        daily_limit: Any = None,
        note: Any = "",
    ) -> Dict[str, Any]:
        self._ensure_ready()
        manager_id = str(manager_id)
        if not re.fullmatch(r"[1-9]\d{0,19}", manager_id):
            raise ValueError("manager_id must be a positive numeric Bitrix user ID")
        if not isinstance(enabled, bool):
            raise ValueError("enabled must be a boolean")
        note = str(note or "")
        if len(note) > 2000:
            raise ValueError("note is too long")
        with self._transaction(immediate=True) as connection:
            self._set_rule_in_connection(
                connection,
                manager_id,
                enabled=bool(enabled),
                daily_limit=daily_limit,
                note=note,
                timestamp=self._now_iso(),
            )
        return self.get_rule(manager_id)

    set_manager_rule = set_rule

    def list_rules(self) -> Dict[str, Dict[str, Any]]:
        self._ensure_ready()
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT manager_id, enabled, daily_limit, note FROM manager_rules ORDER BY manager_id"
            ).fetchall()
        return {
            row["manager_id"]: {
                "enabled": bool(row["enabled"]),
                "dailyLimit": row["daily_limit"],
                "note": row["note"],
            }
            for row in rows
        }

    def load_access_rules(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Compatibility shape used by the original ``app.py`` helpers."""

        return {"managers": self.list_rules()}

    # ------------------------------------------------------------------
    # Event inserts
    # ------------------------------------------------------------------

    def _insert_claim(
        self,
        connection: sqlite3.Connection,
        entry: Mapping[str, Any],
        *,
        source: str,
        migration: bool = False,
        operation_key: Optional[str] = None,
    ) -> int:
        payload = dict(entry)
        timestamp, event_date = self._normalize_timestamp(payload.get("timestamp"), migration=migration)
        manager_id = str(payload.get("managerId") or payload.get("manager_id") or "")
        deal_id = str(payload.get("dealId") or payload.get("deal_id") or "")
        raw_operation_key = (
            operation_key or payload.get("operationKey") or payload.get("operation_key")
        )
        operation_key = str(raw_operation_key) if raw_operation_key else None
        if not migration and (not manager_id or not deal_id):
            raise ValueError("claim event requires managerId and dealId")
        payload.update(
            {
                "timestamp": timestamp,
                "managerId": manager_id,
                "dealId": deal_id,
                "operationKey": operation_key or "",
            }
        )
        cursor = connection.execute(
            """
            INSERT INTO claim_events(
                timestamp, event_date, manager_id, manager_name, deal_id, deal_title,
                payload_json, source, operation_key, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                event_date,
                manager_id,
                str(payload.get("managerName") or payload.get("manager_name") or ""),
                deal_id,
                str(payload.get("dealTitle") or payload.get("deal_title") or ""),
                _json_dumps(payload),
                source,
                operation_key,
                self._now_iso(),
            ),
        )
        return int(cursor.lastrowid)

    def _insert_reject(
        self,
        connection: sqlite3.Connection,
        entry: Mapping[str, Any],
        *,
        source: str,
        migration: bool = False,
    ) -> int:
        payload = dict(entry)
        timestamp, event_date = self._normalize_timestamp(payload.get("timestamp"), migration=migration)
        manager_id = str(payload.get("managerId") or payload.get("manager_id") or "")
        deal_id = str(payload.get("dealId") or payload.get("deal_id") or "")
        if not migration and (not manager_id or not deal_id):
            raise ValueError("reject event requires managerId and dealId")
        payload.update({"timestamp": timestamp, "managerId": manager_id, "dealId": deal_id})
        cursor = connection.execute(
            """
            INSERT INTO reject_events(
                timestamp, event_date, manager_id, manager_name, deal_id, deal_title,
                stage_id, direction, reason, reason_label, selection_token_hash,
                semantic_key, payload_json, source, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                event_date,
                manager_id,
                str(payload.get("managerName") or payload.get("manager_name") or ""),
                deal_id,
                str(payload.get("dealTitle") or payload.get("deal_title") or ""),
                str(payload.get("stageId") or payload.get("stage_id") or ""),
                str(payload.get("direction") or ""),
                str(payload.get("reason") or "other"),
                str(payload.get("reasonLabel") or payload.get("reason_label") or ""),
                str(payload.get("selectionTokenHash") or payload.get("selection_token_hash") or ""),
                str(payload.get("semanticKey") or payload.get("semantic_key") or ""),
                _json_dumps(payload),
                source,
                self._now_iso(),
            ),
        )
        return int(cursor.lastrowid)

    def _insert_greeting(
        self,
        connection: sqlite3.Connection,
        entry: Mapping[str, Any],
        *,
        source: str,
        migration: bool = False,
    ) -> int:
        payload = dict(entry)
        timestamp, event_date = self._normalize_timestamp(payload.get("timestamp"), migration=migration)
        manager_id = str(payload.get("managerId") or payload.get("manager_id") or "")
        deal_id = str(payload.get("dealId") or payload.get("deal_id") or "")
        operation_key = str(payload.get("operationKey") or payload.get("operation_key") or "")
        if not migration and not deal_id:
            raise ValueError("greeting event requires dealId")
        payload.update(
            {
                "timestamp": timestamp,
                "managerId": manager_id,
                "dealId": deal_id,
                "operationKey": operation_key,
            }
        )
        auto_sent = payload.get("autoSent", payload.get("auto_sent", False))
        cursor = connection.execute(
            """
            INSERT INTO greeting_events(
                timestamp, event_date, manager_id, manager_name, deal_id, operation_key, direction,
                confidence, text, status, auto_sent, payload_json, source, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                event_date,
                manager_id,
                str(payload.get("managerName") or payload.get("manager_name") or ""),
                deal_id,
                operation_key,
                str(payload.get("direction") or ""),
                str(payload.get("confidence") or ""),
                str(payload.get("text") or ""),
                str(payload.get("status") or "manual"),
                1 if auto_sent else 0,
                _json_dumps(payload),
                source,
                self._now_iso(),
            ),
        )
        return int(cursor.lastrowid)

    def append_claim(
        self,
        entry_or_manager_id: Mapping[str, Any] | Any,
        deal: Optional[Mapping[str, Any]] = None,
        *,
        manager_name: str = "",
        operation_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._ensure_ready()
        if isinstance(entry_or_manager_id, Mapping) and deal is None:
            entry = dict(entry_or_manager_id)
        else:
            deal = dict(deal or {})
            entry = {
                "timestamp": self._now_iso(),
                "managerId": str(entry_or_manager_id),
                "managerName": manager_name or str(entry_or_manager_id),
                "dealId": str(deal.get("ID") or deal.get("id") or ""),
                "dealTitle": deal.get("TITLE") or deal.get("title") or "",
            }
        with self._transaction(immediate=True) as connection:
            event_id = self._insert_claim(
                connection, entry, source="app", operation_key=operation_key
            )
        return self._get_claim_event(event_id)

    append_claim_event = append_claim

    def append_reject(
        self,
        entry_or_manager_id: Mapping[str, Any] | Any,
        deal: Optional[Mapping[str, Any]] = None,
        *,
        reason: str = "other",
        reason_label: str = "",
        manager_name: str = "",
    ) -> Dict[str, Any]:
        self._ensure_ready()
        if isinstance(entry_or_manager_id, Mapping) and deal is None:
            entry = dict(entry_or_manager_id)
        else:
            deal = dict(deal or {})
            entry = {
                "timestamp": self._now_iso(),
                "managerId": str(entry_or_manager_id),
                "managerName": manager_name or str(entry_or_manager_id),
                "dealId": str(deal.get("ID") or deal.get("id") or ""),
                "dealTitle": deal.get("TITLE") or deal.get("title") or "",
                "stageId": deal.get("STAGE_ID") or deal.get("stageId") or "",
                "direction": (
                    (deal.get("classification") or {}).get("direction", "")
                    if isinstance(deal.get("classification"), dict)
                    else ""
                ),
                "reason": reason,
                "reasonLabel": reason_label,
            }
        with self._transaction(immediate=True) as connection:
            event_id = self._insert_reject(connection, entry, source="app")
        return self._get_reject_event(event_id)

    append_reject_event = append_reject

    def append_greeting(self, entry: Mapping[str, Any]) -> Dict[str, Any]:
        self._ensure_ready()
        with self._transaction(immediate=True) as connection:
            event_id = self._insert_greeting(connection, entry, source="app")
        return self._get_greeting_event(event_id)

    append_greeting_event = append_greeting

    # ------------------------------------------------------------------
    # Event reads/counts
    # ------------------------------------------------------------------

    def _event_filters(
        self,
        manager_id: Optional[Any],
        date_from: Optional[str],
        date_to: Optional[str],
    ) -> tuple[str, list[Any]]:
        date_from = self._normalize_date_filter(date_from, "date_from")
        date_to = self._normalize_date_filter(date_to, "date_to")
        clauses = []
        params: list[Any] = []
        if manager_id is not None:
            clauses.append("manager_id = ?")
            params.append(str(manager_id))
        if date_from:
            clauses.append("event_date >= ?")
            params.append(date_from)
        if date_to:
            clauses.append("event_date <= ?")
            params.append(date_to)
        return (" WHERE " + " AND ".join(clauses) if clauses else ""), params

    def _list_event_rows(
        self,
        table: str,
        converter: Any,
        *,
        manager_id: Optional[Any] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        descending: bool = False,
        limit: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        self._ensure_ready()
        where, params = self._event_filters(manager_id, date_from, date_to)
        sql = f"SELECT * FROM {table}{where} ORDER BY id {'DESC' if descending else 'ASC'}"
        if limit is not None:
            limit = max(0, int(limit))
            sql += " LIMIT ?"
            params.append(limit)
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [converter(row) for row in rows]

    def _claim_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        payload = self._payload_from_row(row)
        payload.update(
            {
                "timestamp": row["timestamp"],
                "managerId": row["manager_id"],
                "managerName": row["manager_name"],
                "dealId": row["deal_id"],
                "operationKey": row["operation_key"],
                "dealTitle": row["deal_title"],
            }
        )
        return payload

    def _reject_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        payload = self._payload_from_row(row)
        payload.update(
            {
                "timestamp": row["timestamp"],
                "managerId": row["manager_id"],
                "managerName": row["manager_name"],
                "dealId": row["deal_id"],
                "dealTitle": row["deal_title"],
                "stageId": row["stage_id"],
                "direction": row["direction"],
                "reason": row["reason"],
                "reasonLabel": row["reason_label"],
                "selectionTokenHash": row["selection_token_hash"],
                "semanticKey": row["semantic_key"],
            }
        )
        return payload

    def _greeting_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        payload = self._payload_from_row(row)
        payload.update(
            {
                "timestamp": row["timestamp"],
                "managerId": row["manager_id"],
                "managerName": row["manager_name"],
                "dealId": row["deal_id"],
                "operationKey": row["operation_key"],
                "direction": row["direction"],
                "confidence": row["confidence"],
                "text": row["text"],
                "status": row["status"],
                "autoSent": bool(row["auto_sent"]),
            }
        )
        return payload

    def _get_claim_event(self, event_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM claim_events WHERE id = ?", (event_id,)).fetchone()
        if not row:
            raise StateStoreError(f"claim event {event_id} not found")
        return self._claim_row(row)

    def _get_reject_event(self, event_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM reject_events WHERE id = ?", (event_id,)).fetchone()
        if not row:
            raise StateStoreError(f"reject event {event_id} not found")
        return self._reject_row(row)

    def _get_greeting_event(self, event_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM greeting_events WHERE id = ?", (event_id,)).fetchone()
        if not row:
            raise StateStoreError(f"greeting event {event_id} not found")
        return self._greeting_row(row)

    def list_claims(
        self,
        manager_id: Optional[Any] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        *,
        descending: bool = False,
        limit: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        return self._list_event_rows(
            "claim_events",
            self._claim_row,
            manager_id=manager_id,
            date_from=date_from,
            date_to=date_to,
            descending=descending,
            limit=limit,
        )

    list_claim_events = list_claims

    load_claim_log = list_claims

    def list_rejections(
        self,
        manager_id: Optional[Any] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        *,
        descending: bool = False,
        limit: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        return self._list_event_rows(
            "reject_events",
            self._reject_row,
            manager_id=manager_id,
            date_from=date_from,
            date_to=date_to,
            descending=descending,
            limit=limit,
        )

    list_reject_events = list_rejections

    load_reject_log = list_rejections

    def list_greetings(
        self,
        manager_id: Optional[Any] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        *,
        descending: bool = False,
        limit: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        return self._list_event_rows(
            "greeting_events",
            self._greeting_row,
            manager_id=manager_id,
            date_from=date_from,
            date_to=date_to,
            descending=descending,
            limit=limit,
        )

    list_greeting_events = list_greetings
    load_greeting_log = list_greetings

    def count_claims(
        self,
        manager_id: Any,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> int:
        self._ensure_ready()
        where, params = self._event_filters(manager_id, date_from, date_to)
        with self._connect() as connection:
            return int(connection.execute(f"SELECT COUNT(*) FROM claim_events{where}", params).fetchone()[0])

    def count_rejections(
        self,
        manager_id: Any,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> int:
        self._ensure_ready()
        where, params = self._event_filters(manager_id, date_from, date_to)
        with self._connect() as connection:
            return int(connection.execute(f"SELECT COUNT(*) FROM reject_events{where}", params).fetchone()[0])

    def get_rejection_by_token_hash(self, token_hash: Any) -> Optional[Dict[str, Any]]:
        self._ensure_ready()
        token_hash = str(token_hash or "")
        if not token_hash:
            return None
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM reject_events WHERE selection_token_hash = ? LIMIT 1",
                (token_hash,),
            ).fetchone()
        return self._reject_row(row) if row else None

    def get_rejection_by_semantic_key(self, semantic_key: Any) -> Optional[Dict[str, Any]]:
        self._ensure_ready()
        semantic_key = str(semantic_key or "")
        if not semantic_key:
            return None
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM reject_events WHERE semantic_key = ? LIMIT 1",
                (semantic_key,),
            ).fetchone()
        return self._reject_row(row) if row else None

    def list_rejection_semantic_keys(self, manager_id: Any) -> set[str]:
        self._ensure_ready()
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT semantic_key FROM reject_events "
                "WHERE manager_id = ? AND semantic_key <> ''",
                (str(manager_id),),
            ).fetchall()
        return {str(row[0]) for row in rows}

    def latest_greeting_by_deal(
        self,
        deal_id: Any,
        *,
        statuses: Optional[Iterable[str]] = ("manual", "sent", "sending"),
    ) -> Optional[Dict[str, Any]]:
        self._ensure_ready()
        params: list[Any] = [str(deal_id)]
        sql = "SELECT * FROM greeting_events WHERE deal_id = ?"
        if statuses is not None:
            statuses = tuple(str(status) for status in statuses)
            if not statuses:
                return None
            sql += " AND status IN (" + ",".join("?" for _ in statuses) + ")"
            params.extend(statuses)
        sql += " ORDER BY id DESC LIMIT 1"
        with self._connect() as connection:
            row = connection.execute(sql, params).fetchone()
        return self._greeting_row(row) if row else None

    latest_greeting_for_deal = latest_greeting_by_deal

    def latest_greeting_by_operation(
        self,
        operation_key: Any,
        *,
        statuses: Optional[Iterable[str]] = ("manual", "sent", "sending"),
    ) -> Optional[Dict[str, Any]]:
        self._ensure_ready()
        operation_key = str(operation_key or "")
        if not operation_key:
            return None
        params: list[Any] = [operation_key]
        sql = "SELECT * FROM greeting_events WHERE operation_key = ?"
        if statuses is not None:
            statuses = tuple(str(status) for status in statuses)
            if not statuses:
                return None
            sql += " AND status IN (" + ",".join("?" for _ in statuses) + ")"
            params.extend(statuses)
        sql += " ORDER BY id DESC LIMIT 1"
        with self._connect() as connection:
            row = connection.execute(sql, params).fetchone()
        return self._greeting_row(row) if row else None

    latest_greeting_for_operation = latest_greeting_by_operation

    def list_manager_ids(self) -> list[str]:
        self._ensure_ready()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT manager_id FROM manager_rules
                UNION SELECT manager_id FROM claim_events
                UNION SELECT manager_id FROM reject_events
                UNION SELECT manager_id FROM greeting_events
                UNION SELECT manager_id FROM claim_operations
                UNION SELECT manager_id FROM greeting_outbox
                """
            ).fetchall()
        return sorted((row[0] for row in rows if row[0]), key=lambda value: (not value.isdigit(), value))

    # ------------------------------------------------------------------
    # Durable greeting outbox
    # ------------------------------------------------------------------

    _GREETING_OUTBOX_STATUSES = frozenset(
        {"pending", "checking", "dispatching", "sent", "manual", "uncertain"}
    )

    def _outbox_datetime(self, value: Optional[Any] = None) -> datetime:
        if value is None:
            parsed = datetime.now(self.local_timezone)
        elif isinstance(value, datetime):
            parsed = value
        else:
            raw = str(value).strip()
            try:
                parsed = datetime.fromisoformat(
                    raw[:-1] + "+00:00" if raw.endswith("Z") else raw
                )
            except ValueError as exc:
                raise ValueError("now must be an ISO-8601 datetime") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(self.local_timezone)

    @staticmethod
    def _outbox_time(value: datetime) -> str:
        return value.isoformat(timespec="microseconds")

    @staticmethod
    def _outbox_error_code(value: Any) -> str:
        code = str(value or "unspecified").strip().lower()
        if not re.fullmatch(r"[a-z0-9][a-z0-9_.:-]{0,79}", code):
            raise ValueError("error_code must be a short machine-readable code")
        return code

    @staticmethod
    def _outbox_message_id(value: Any) -> str:
        message_id = str(value or "").strip()
        if message_id and not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", message_id):
            raise ValueError("message_id must be a short opaque identifier")
        return message_id

    @staticmethod
    def _greeting_outbox_row(
        row: sqlite3.Row,
        *,
        transitioned: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Return worker/UI-safe job state (never the private lease token)."""

        result = {
            "operationKey": row["operation_key"],
            "dealId": row["deal_id"],
            "managerId": row["manager_id"],
            "sessionId": row["session_id"],
            "direction": row["direction"],
            "status": row["status"],
            "text": row["text"],
            "attemptCount": row["attempt_count"],
            "leasedAt": row["leased_at"],
            "leaseExpiresAt": row["lease_expires_at"],
            "nextAttemptAt": row["next_attempt_at"],
            "errorCode": row["error_code"],
            "messageId": row["message_id"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "dispatchingAt": row["dispatching_at"],
            "finalizedAt": row["finalized_at"],
        }
        if transitioned is not None:
            result["transitioned"] = transitioned
        return result

    @staticmethod
    def _owns_active_greeting_check(
        row: sqlite3.Row,
        worker_token: str,
        now_iso: str,
    ) -> bool:
        return bool(
            row["status"] == "checking"
            and row["lease_token"] == worker_token
            and row["lease_expires_at"]
            and row["lease_expires_at"] > now_iso
        )

    def get_greeting_outbox(self, operation_key: Any) -> Optional[Dict[str, Any]]:
        self._ensure_ready()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM greeting_outbox WHERE operation_key = ?",
                (str(operation_key),),
            ).fetchone()
        return self._greeting_outbox_row(row) if row else None

    def list_greeting_outbox(
        self,
        *,
        statuses: Optional[Iterable[str]] = None,
        limit: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        self._ensure_ready()
        params: list[Any] = []
        sql = "SELECT * FROM greeting_outbox"
        if statuses is not None:
            normalized = tuple(dict.fromkeys(str(status) for status in statuses))
            if not normalized:
                return []
            unknown = set(normalized) - self._GREETING_OUTBOX_STATUSES
            if unknown:
                raise ValueError("unsupported greeting outbox status")
            sql += " WHERE status IN (" + ",".join("?" for _ in normalized) + ")"
            params.extend(normalized)
        sql += " ORDER BY created_at, operation_key"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(max(0, int(limit)))
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [self._greeting_outbox_row(row) for row in rows]

    def list_pending_greeting_outbox(
        self,
        *,
        limit: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        return self.list_greeting_outbox(
            statuses=("pending", "checking"),
            limit=limit,
        )

    def lease_greeting_outbox(
        self,
        worker_token: Any,
        *,
        limit: int = 1,
        lease_seconds: int = 60,
        max_attempts: int = 3,
        now: Optional[Any] = None,
    ) -> list[Dict[str, Any]]:
        """Atomically lease eligible pre-send jobs to one worker.

        A stale ``checking`` lease is safe to retry because dispatch has not
        started.  ``dispatching`` is deliberately never selected here.
        """

        self._ensure_ready()
        worker_token = str(worker_token or "").strip()
        if not worker_token or len(worker_token) > 200:
            raise ValueError("worker_token is required")
        limit = max(0, min(100, int(limit)))
        if not limit:
            return []
        lease_seconds = max(1, min(3600, int(lease_seconds)))
        max_attempts = max(1, min(100, int(max_attempts)))
        now_dt = self._outbox_datetime(now)
        now_iso = self._outbox_time(now_dt)
        expires_iso = self._outbox_time(now_dt + timedelta(seconds=lease_seconds))
        with self._transaction(immediate=True) as connection:
            # A worker that died during its final safe checking attempt must
            # not leave the job permanently invisible.
            connection.execute(
                """
                UPDATE greeting_outbox SET
                    status='manual', lease_token='', lease_expires_at=NULL,
                    next_attempt_at=NULL, error_code='checking_attempts_exhausted',
                    updated_at=?, finalized_at=?
                WHERE status='checking'
                  AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?
                  AND attempt_count >= ?
                """,
                (now_iso, now_iso, now_iso, max_attempts),
            )
            rows = connection.execute(
                """
                SELECT operation_key FROM greeting_outbox
                WHERE attempt_count < ? AND (
                    (status='pending' AND (
                        next_attempt_at IS NULL OR next_attempt_at <= ?
                    )) OR
                    (status='checking' AND lease_expires_at IS NOT NULL
                        AND lease_expires_at <= ?)
                )
                ORDER BY created_at, operation_key
                LIMIT ?
                """,
                (max_attempts, now_iso, now_iso, limit),
            ).fetchall()
            leased: list[Dict[str, Any]] = []
            for candidate in rows:
                operation_key = candidate["operation_key"]
                cursor = connection.execute(
                    """
                    UPDATE greeting_outbox SET
                        status='checking', attempt_count=attempt_count + 1,
                        lease_token=?, leased_at=?, lease_expires_at=?,
                        next_attempt_at=NULL, error_code='', updated_at=?
                    WHERE operation_key=? AND attempt_count < ? AND (
                        (status='pending' AND (
                            next_attempt_at IS NULL OR next_attempt_at <= ?
                        )) OR
                        (status='checking' AND lease_expires_at IS NOT NULL
                            AND lease_expires_at <= ?)
                    )
                    """,
                    (
                        worker_token,
                        now_iso,
                        expires_iso,
                        now_iso,
                        operation_key,
                        max_attempts,
                        now_iso,
                        now_iso,
                    ),
                )
                if cursor.rowcount != 1:
                    continue
                leased_row = connection.execute(
                    "SELECT * FROM greeting_outbox WHERE operation_key = ?",
                    (operation_key,),
                ).fetchone()
                leased.append(self._greeting_outbox_row(leased_row))
            return leased

    def lease_exact_greeting_outbox(
        self,
        operation_key: Any,
        worker_token: Any,
        *,
        lease_seconds: int = 60,
        max_attempts: int = 3,
        now: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Atomically reserve one exact pre-send job for an in-request actor.

        The regular worker leases the oldest eligible job.  A freshly
        authenticated claim must instead reserve only its own operation so
        the manager's short-lived OAuth token can never be applied to another
        manager or deal.
        """

        self._ensure_ready()
        operation_key = str(operation_key or "").strip()
        worker_token = str(worker_token or "").strip()
        if not operation_key or not worker_token or len(worker_token) > 200:
            raise ValueError("operation_key and worker_token are required")
        lease_seconds = max(1, min(3600, int(lease_seconds)))
        max_attempts = max(1, min(100, int(max_attempts)))
        now_dt = self._outbox_datetime(now)
        now_iso = self._outbox_time(now_dt)
        expires_iso = self._outbox_time(now_dt + timedelta(seconds=lease_seconds))
        with self._transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM greeting_outbox WHERE operation_key = ?",
                (operation_key,),
            ).fetchone()
            if not row:
                raise StateStoreError(f"greeting outbox job {operation_key!r} not found")
            eligible = bool(
                int(row["attempt_count"]) < max_attempts
                and (
                    (
                        row["status"] == "pending"
                        and (
                            row["next_attempt_at"] is None
                            or row["next_attempt_at"] <= now_iso
                        )
                    )
                    or (
                        row["status"] == "checking"
                        and row["lease_expires_at"] is not None
                        and row["lease_expires_at"] <= now_iso
                    )
                )
            )
            if not eligible:
                return self._greeting_outbox_row(row, transitioned=False)
            cursor = connection.execute(
                """
                UPDATE greeting_outbox SET
                    status='checking', attempt_count=attempt_count + 1,
                    lease_token=?, leased_at=?, lease_expires_at=?,
                    next_attempt_at=NULL, error_code='', updated_at=?
                WHERE operation_key=? AND attempt_count < ? AND (
                    (status='pending' AND (
                        next_attempt_at IS NULL OR next_attempt_at <= ?
                    )) OR
                    (status='checking' AND lease_expires_at IS NOT NULL
                        AND lease_expires_at <= ?)
                )
                """,
                (
                    worker_token,
                    now_iso,
                    expires_iso,
                    now_iso,
                    operation_key,
                    max_attempts,
                    now_iso,
                    now_iso,
                ),
            )
            row = connection.execute(
                "SELECT * FROM greeting_outbox WHERE operation_key = ?",
                (operation_key,),
            ).fetchone()
            return self._greeting_outbox_row(
                row,
                transitioned=cursor.rowcount == 1,
            )

    def update_greeting_outbox_check(
        self,
        operation_key: Any,
        worker_token: Any,
        *,
        session_id: Optional[Any] = None,
        direction: Optional[Any] = None,
        text: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Persist a checked target/text while the caller owns the lease."""

        self._ensure_ready()
        operation_key = str(operation_key)
        worker_token = str(worker_token or "").strip()
        if not operation_key or not worker_token:
            raise ValueError("operation_key and worker_token are required")
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM greeting_outbox WHERE operation_key = ?",
                (operation_key,),
            ).fetchone()
            if not row:
                raise StateStoreError(f"greeting outbox job {operation_key!r} not found")
            if not self._owns_active_greeting_check(row, worker_token, now):
                return self._greeting_outbox_row(row, transitioned=False)
            new_session_id = row["session_id"] if session_id is None else str(session_id).strip()
            new_direction = row["direction"] if direction is None else str(direction).strip()
            new_text = row["text"] if text is None else str(text)
            if not new_session_id or not new_direction:
                raise ValueError("session_id and direction must remain non-empty")
            connection.execute(
                """
                UPDATE greeting_outbox SET
                    session_id=?, direction=?, text=?, updated_at=?
                WHERE operation_key=? AND status='checking' AND lease_token=?
                """,
                (
                    new_session_id,
                    new_direction,
                    new_text,
                    now,
                    operation_key,
                    worker_token,
                ),
            )
            row = connection.execute(
                "SELECT * FROM greeting_outbox WHERE operation_key = ?",
                (operation_key,),
            ).fetchone()
            return self._greeting_outbox_row(row, transitioned=True)

    def retry_greeting_outbox_check(
        self,
        operation_key: Any,
        worker_token: Any,
        *,
        error_code: Any,
        delay_seconds: int = 5,
        max_attempts: int = 3,
        now: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Retry only a pre-send checking failure, with a hard bound."""

        self._ensure_ready()
        operation_key = str(operation_key)
        worker_token = str(worker_token or "").strip()
        code = self._outbox_error_code(error_code)
        delay_seconds = max(0, min(86_400, int(delay_seconds)))
        max_attempts = max(1, min(100, int(max_attempts)))
        now_dt = self._outbox_datetime(now)
        now_iso = self._outbox_time(now_dt)
        with self._transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM greeting_outbox WHERE operation_key = ?",
                (operation_key,),
            ).fetchone()
            if not row:
                raise StateStoreError(f"greeting outbox job {operation_key!r} not found")
            if not self._owns_active_greeting_check(row, worker_token, now_iso):
                return self._greeting_outbox_row(row, transitioned=False)
            exhausted = int(row["attempt_count"]) >= max_attempts
            status = "manual" if exhausted else "pending"
            next_attempt = (
                None
                if exhausted
                else self._outbox_time(now_dt + timedelta(seconds=delay_seconds))
            )
            connection.execute(
                """
                UPDATE greeting_outbox SET
                    status=?, lease_token='', lease_expires_at=NULL,
                    next_attempt_at=?, error_code=?, updated_at=?, finalized_at=?
                WHERE operation_key=? AND status='checking' AND lease_token=?
                """,
                (
                    status,
                    next_attempt,
                    code,
                    now_iso,
                    now_iso if exhausted else None,
                    operation_key,
                    worker_token,
                ),
            )
            row = connection.execute(
                "SELECT * FROM greeting_outbox WHERE operation_key = ?",
                (operation_key,),
            ).fetchone()
            return self._greeting_outbox_row(row, transitioned=True)

    def _mark_greeting_outbox_from_checking(
        self,
        operation_key: Any,
        worker_token: Any,
        *,
        status: str,
        error_code: Any = "",
    ) -> Dict[str, Any]:
        self._ensure_ready()
        operation_key = str(operation_key)
        worker_token = str(worker_token or "").strip()
        if status not in {"dispatching", "manual"}:
            raise ValueError("invalid checking transition")
        code = "" if status == "dispatching" else self._outbox_error_code(error_code)
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM greeting_outbox WHERE operation_key = ?",
                (operation_key,),
            ).fetchone()
            if not row:
                raise StateStoreError(f"greeting outbox job {operation_key!r} not found")
            if not self._owns_active_greeting_check(row, worker_token, now):
                return self._greeting_outbox_row(row, transitioned=False)
            if status == "dispatching" and (
                not row["session_id"] or not row["direction"] or not row["text"]
            ):
                raise StateStoreError("checked greeting target and text are required before dispatch")
            connection.execute(
                """
                UPDATE greeting_outbox SET
                    status=?, lease_token=?, lease_expires_at=NULL, next_attempt_at=NULL,
                    error_code=?, updated_at=?, dispatching_at=?, finalized_at=?
                WHERE operation_key=? AND status='checking' AND lease_token=?
                """,
                (
                    status,
                    worker_token if status == "dispatching" else "",
                    code,
                    now,
                    now if status == "dispatching" else None,
                    now if status == "manual" else None,
                    operation_key,
                    worker_token,
                ),
            )
            row = connection.execute(
                "SELECT * FROM greeting_outbox WHERE operation_key = ?",
                (operation_key,),
            ).fetchone()
            return self._greeting_outbox_row(row, transitioned=True)

    def mark_greeting_outbox_dispatching(
        self,
        operation_key: Any,
        worker_token: Any,
    ) -> Dict[str, Any]:
        """Durably cross the no-automatic-retry boundary before remote send."""

        return self._mark_greeting_outbox_from_checking(
            operation_key,
            worker_token,
            status="dispatching",
        )

    def mark_greeting_outbox_manual(
        self,
        operation_key: Any,
        worker_token: Any,
        *,
        error_code: Any,
    ) -> Dict[str, Any]:
        return self._mark_greeting_outbox_from_checking(
            operation_key,
            worker_token,
            status="manual",
            error_code=error_code,
        )

    def _mark_greeting_outbox_after_dispatch(
        self,
        operation_key: Any,
        worker_token: Any,
        *,
        status: str,
        error_code: Any = "",
        message_id: Any = "",
    ) -> Dict[str, Any]:
        self._ensure_ready()
        operation_key = str(operation_key)
        worker_token = str(worker_token or "").strip()
        if status not in {"sent", "uncertain"}:
            raise ValueError("invalid dispatch transition")
        code = "" if status == "sent" else self._outbox_error_code(error_code)
        normalized_message_id = self._outbox_message_id(message_id)
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM greeting_outbox WHERE operation_key = ?",
                (operation_key,),
            ).fetchone()
            if not row:
                raise StateStoreError(f"greeting outbox job {operation_key!r} not found")
            if row["status"] != "dispatching" or row["lease_token"] != worker_token:
                return self._greeting_outbox_row(row, transitioned=False)
            connection.execute(
                """
                UPDATE greeting_outbox SET
                    status=?, lease_token='', lease_expires_at=NULL,
                    error_code=?, message_id=?, updated_at=?, finalized_at=?
                WHERE operation_key=? AND status='dispatching' AND lease_token=?
                """,
                (
                    status,
                    code,
                    normalized_message_id,
                    now,
                    now,
                    operation_key,
                    worker_token,
                ),
            )
            row = connection.execute(
                "SELECT * FROM greeting_outbox WHERE operation_key = ?",
                (operation_key,),
            ).fetchone()
            return self._greeting_outbox_row(row, transitioned=True)

    def mark_greeting_outbox_sent(
        self,
        operation_key: Any,
        worker_token: Any,
        *,
        message_id: Any = "",
    ) -> Dict[str, Any]:
        return self._mark_greeting_outbox_after_dispatch(
            operation_key,
            worker_token,
            status="sent",
            message_id=message_id,
        )

    def mark_greeting_outbox_uncertain(
        self,
        operation_key: Any,
        worker_token: Any,
        *,
        error_code: Any,
    ) -> Dict[str, Any]:
        return self._mark_greeting_outbox_after_dispatch(
            operation_key,
            worker_token,
            status="uncertain",
            error_code=error_code,
        )

    def recover_stale_greeting_dispatches(
        self,
        *,
        stale_after_seconds: int = 300,
        now: Optional[Any] = None,
    ) -> list[Dict[str, Any]]:
        """Quarantine stale sends; an external send may already have happened."""

        self._ensure_ready()
        stale_after_seconds = max(1, min(86_400, int(stale_after_seconds)))
        now_dt = self._outbox_datetime(now)
        now_iso = self._outbox_time(now_dt)
        cutoff = self._outbox_time(now_dt - timedelta(seconds=stale_after_seconds))
        with self._transaction(immediate=True) as connection:
            candidates = connection.execute(
                """
                SELECT operation_key FROM greeting_outbox
                WHERE status='dispatching' AND dispatching_at IS NOT NULL
                  AND dispatching_at <= ?
                ORDER BY dispatching_at, operation_key
                """,
                (cutoff,),
            ).fetchall()
            if not candidates:
                return []
            keys = [row["operation_key"] for row in candidates]
            placeholders = ",".join("?" for _ in keys)
            connection.execute(
                f"""
                UPDATE greeting_outbox SET
                    status='uncertain', lease_token='', lease_expires_at=NULL,
                    error_code='stale_dispatching', updated_at=?, finalized_at=?
                WHERE status='dispatching' AND operation_key IN ({placeholders})
                """,
                (now_iso, now_iso, *keys),
            )
            rows = connection.execute(
                f"SELECT * FROM greeting_outbox WHERE operation_key IN ({placeholders}) "
                "ORDER BY created_at, operation_key",
                keys,
            ).fetchall()
            return [self._greeting_outbox_row(row) for row in rows]

    # ------------------------------------------------------------------
    # Idempotent claim operations
    # ------------------------------------------------------------------

    @staticmethod
    def _prepare_operation_request(request: Mapping[str, Any]) -> Dict[str, Any]:
        """Keep the private greeting target minimal before durable storage.

        The outbox never needs an OAuth token, chat transcript, or arbitrary
        browser payload.  Only the already-discovered session and direction
        are retained under the private ``greetingContext`` key.
        """

        prepared = dict(request)
        context = prepared.get("greetingContext")
        if isinstance(context, Mapping):
            prepared["greetingContext"] = {
                "sessionId": str(context.get("sessionId") or "").strip(),
                "direction": str(context.get("direction") or "").strip(),
            }
        elif "greetingContext" in prepared:
            prepared.pop("greetingContext", None)
        return prepared

    @staticmethod
    def _safe_operation_request(request: Any) -> Any:
        """Remove internal greeting routing data from status/JSON output."""

        if not isinstance(request, dict):
            return request
        safe = dict(request)
        safe.pop("greetingContext", None)
        return safe

    @staticmethod
    def _operation_row(row: sqlite3.Row, *, created: Optional[bool] = None) -> Dict[str, Any]:
        def decode(column: str) -> Any:
            raw = row[column]
            if raw is None:
                return None
            try:
                return json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                return None

        request = StateStore._safe_operation_request(decode("request_json"))
        attempt_history = decode("attempt_history_json") or []
        if isinstance(attempt_history, list):
            safe_attempt_history = []
            for attempt in attempt_history:
                if not isinstance(attempt, dict):
                    safe_attempt_history.append(attempt)
                    continue
                safe_attempt = dict(attempt)
                safe_attempt["request"] = StateStore._safe_operation_request(
                    safe_attempt.get("request")
                )
                safe_attempt_history.append(safe_attempt)
            attempt_history = safe_attempt_history
        result = {
            "operationKey": row["operation_key"],
            "dealId": row["deal_id"],
            "managerId": row["manager_id"],
            "status": row["status"],
            "request": request,
            "result": decode("result_json"),
            "error": row["error"],
            "attemptHistory": attempt_history,
            "claimEventId": row["claim_event_id"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "finalizedAt": row["finalized_at"],
        }
        if created is not None:
            result["created"] = created
        return result

    def begin_claim_operation(
        self,
        deal_id: Any,
        manager_id: Any,
        *,
        operation_key: Optional[str] = None,
        request: Optional[Mapping[str, Any]] = None,
        retry_failed: bool = False,
    ) -> Dict[str, Any]:
        """Create an idempotent claim operation or safely retry a failed one.

        With ``retry_failed=True`` only a matching ``failed`` operation moves
        back to ``pending``.  A pending operation is left alone, a succeeded
        operation is immutable, and identity conflicts always fail loudly.
        """

        self._ensure_ready()
        deal_id = str(deal_id)
        manager_id = str(manager_id)
        operation_key = str(operation_key or f"claim:{deal_id}")
        if not deal_id or not manager_id or not operation_key:
            raise ValueError("deal_id, manager_id and operation_key are required")
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM claim_operations WHERE operation_key = ?", (operation_key,)
            ).fetchone()
            if row:
                if row["deal_id"] != deal_id or row["manager_id"] != manager_id:
                    raise IdempotencyConflictError(
                        f"operation key {operation_key!r} belongs to another deal or manager"
                    )
                if retry_failed and row["status"] == "failed":
                    request_json = (
                        _json_dumps(self._prepare_operation_request(request))
                        if request is not None
                        else row["request_json"]
                    )
                    try:
                        attempt_history = json.loads(row["attempt_history_json"])
                    except (TypeError, json.JSONDecodeError):
                        attempt_history = []
                    if not isinstance(attempt_history, list):
                        attempt_history = []
                    attempt_history.append(
                        {
                            "status": "failed",
                            "request": json.loads(row["request_json"])
                            if row["request_json"] is not None
                            else None,
                            "result": json.loads(row["result_json"])
                            if row["result_json"] is not None
                            else None,
                            "error": row["error"],
                            "createdAt": row["created_at"],
                            "finalizedAt": row["finalized_at"],
                        }
                    )
                    connection.execute(
                        """
                        UPDATE claim_operations SET
                            status='pending', request_json=?, result_json=NULL, error=NULL,
                            attempt_history_json=?, claim_event_id=NULL,
                            updated_at=?, finalized_at=NULL
                        WHERE operation_key = ? AND status = 'failed'
                        """,
                        (request_json, _json_dumps(attempt_history), now, operation_key),
                    )
                    row = connection.execute(
                        "SELECT * FROM claim_operations WHERE operation_key = ?", (operation_key,)
                    ).fetchone()
                    operation = self._operation_row(row, created=False)
                    operation["retried"] = True
                    return operation
                operation = self._operation_row(row, created=False)
                operation["retried"] = False
                return operation
            connection.execute(
                """
                INSERT INTO claim_operations(
                    operation_key, deal_id, manager_id, status, request_json,
                    created_at, updated_at
                ) VALUES(?, ?, ?, 'pending', ?, ?, ?)
                """,
                (
                    operation_key,
                    deal_id,
                    manager_id,
                    _json_dumps(self._prepare_operation_request(request))
                    if request is not None
                    else None,
                    now,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM claim_operations WHERE operation_key = ?", (operation_key,)
            ).fetchone()
            operation = self._operation_row(row, created=True)
            operation["retried"] = False
            return operation

    def retry_failed_claim_operation(
        self,
        deal_id: Any,
        manager_id: Any,
        *,
        operation_key: Optional[str] = None,
        request: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Explicit convenience wrapper for a safe failed-operation retry."""

        return self.begin_claim_operation(
            deal_id,
            manager_id,
            operation_key=operation_key,
            request=request,
            retry_failed=True,
        )

    def reassign_failed_claim_operation(
        self,
        deal_id: Any,
        manager_id: Any,
        *,
        operation_key: Optional[str] = None,
        request: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Atomically reuse a failed deal lock for a different manager.

        This is intentionally separate from ``retry_failed_claim_operation``:
        normal idempotent retries must keep the original identity.  The app may
        call this method only after re-reading Bitrix and confirming the deal is
        still in a source stage.  Pending and succeeded operations are never
        reassigned.
        """

        self._ensure_ready()
        deal_id = str(deal_id)
        manager_id = str(manager_id)
        operation_key = str(operation_key or f"claim:{deal_id}")
        if not deal_id or not manager_id or not operation_key:
            raise ValueError("deal_id, manager_id and operation_key are required")
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM claim_operations WHERE operation_key = ?", (operation_key,)
            ).fetchone()
            if not row:
                raise StateStoreError(f"claim operation {operation_key!r} not found")
            if row["deal_id"] != deal_id:
                raise IdempotencyConflictError(
                    f"operation key {operation_key!r} belongs to another deal"
                )
            if row["status"] != "failed":
                operation = self._operation_row(row, created=False)
                operation["reassigned"] = False
                return operation
            try:
                attempt_history = json.loads(row["attempt_history_json"])
            except (TypeError, json.JSONDecodeError):
                attempt_history = []
            if not isinstance(attempt_history, list):
                attempt_history = []
            attempt_history.append(
                {
                    "status": "failed",
                    "managerId": row["manager_id"],
                    "request": json.loads(row["request_json"])
                    if row["request_json"] is not None
                    else None,
                    "result": json.loads(row["result_json"])
                    if row["result_json"] is not None
                    else None,
                    "error": row["error"],
                    "createdAt": row["created_at"],
                    "finalizedAt": row["finalized_at"],
                }
            )
            connection.execute(
                """
                UPDATE claim_operations SET
                    manager_id=?, status='pending', request_json=?, result_json=NULL,
                    error=NULL, attempt_history_json=?, claim_event_id=NULL,
                    updated_at=?, finalized_at=NULL
                WHERE operation_key = ? AND status = 'failed'
                """,
                (
                    manager_id,
                    _json_dumps(self._prepare_operation_request(request))
                    if request is not None
                    else None,
                    _json_dumps(attempt_history),
                    now,
                    operation_key,
                ),
            )
            row = connection.execute(
                "SELECT * FROM claim_operations WHERE operation_key = ?", (operation_key,)
            ).fetchone()
            operation = self._operation_row(row, created=False)
            operation["reassigned"] = True
            return operation

    def get_claim_operation(self, operation_key: Any) -> Optional[Dict[str, Any]]:
        self._ensure_ready()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM claim_operations WHERE operation_key = ?", (str(operation_key),)
            ).fetchone()
        return self._operation_row(row) if row else None

    def find_succeeded_claim_operation_by_marker(
        self,
        deal_id: Any,
        claim_marker: Any,
    ) -> Optional[Dict[str, Any]]:
        """Find resolved evidence that authorizes replacing an old marker.

        A source-stage deal may legitimately re-enter the picker after an
        earlier completed claim.  Only a marker recorded on a succeeded local
        operation proves that lifecycle; unknown/non-terminal markers must be
        preserved for investigation rather than overwritten.
        """

        self._ensure_ready()
        deal_id = str(deal_id)
        claim_marker = str(claim_marker)
        if not deal_id or not claim_marker:
            return None
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM claim_operations
                WHERE deal_id = ? AND status = 'succeeded'
                ORDER BY finalized_at DESC, operation_key DESC
                """,
                (deal_id,),
            ).fetchall()
        for row in rows:
            operation = self._operation_row(row)
            request = operation.get("request") or {}
            if isinstance(request, dict) and str(request.get("claimMarker") or "") == claim_marker:
                return operation
        return None

    @staticmethod
    def _enqueue_greeting_from_request(
        connection: sqlite3.Connection,
        operation_row: sqlite3.Row,
        request: Any,
        timestamp: str,
    ) -> bool:
        """Create one durable job from the claim's private request context.

        This helper is intentionally called only while the pending claim is
        being finalized.  Therefore installing the additive table on an old
        database cannot backfill greetings for historical claims.
        """

        if not isinstance(request, dict) or request.get("greetingRequested") is not True:
            return False
        context = request.get("greetingContext")
        if not isinstance(context, dict):
            return False
        session_id = str(context.get("sessionId") or "").strip()
        direction = str(context.get("direction") or "").strip()
        if not session_id or not direction:
            return False
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO greeting_outbox(
                operation_key, deal_id, manager_id, session_id, direction,
                status, text, attempt_count, lease_token, next_attempt_at,
                error_code, message_id, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, 'pending', '', 0, '', ?, '', '', ?, ?)
            """,
            (
                operation_row["operation_key"],
                operation_row["deal_id"],
                operation_row["manager_id"],
                session_id,
                direction,
                timestamp,
                timestamp,
                timestamp,
            ),
        )
        return cursor.rowcount == 1

    def finalize_claim_operation(
        self,
        operation_key: Any,
        *,
        claim: Optional[Mapping[str, Any]] = None,
        result: Optional[Mapping[str, Any]] = None,
        expected_claim_marker: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Atomically persist a claim event and mark an operation succeeded.

        Terminal operations are immutable.  Repeating the same finalize call
        returns the original operation and never inserts a second event.
        """

        self._ensure_ready()
        operation_key = str(operation_key)
        with self._transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM claim_operations WHERE operation_key = ?", (operation_key,)
            ).fetchone()
            if not row:
                raise StateStoreError(f"claim operation {operation_key!r} not found")
            try:
                current_request = json.loads(row["request_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                current_request = {}
            if expected_claim_marker is not None:
                if (
                    not isinstance(current_request, dict)
                    or current_request.get("claimMarker") != str(expected_claim_marker)
                ):
                    raise IdempotencyConflictError(
                        f"claim operation {operation_key!r} belongs to another attempt"
                    )
            if row["status"] != "pending":
                operation = self._operation_row(row)
                operation["transitioned"] = False
                operation["greetingQueued"] = bool(
                    connection.execute(
                        "SELECT 1 FROM greeting_outbox WHERE operation_key = ?",
                        (operation_key,),
                    ).fetchone()
                )
                return operation

            if claim is None:
                raise ValueError("a pending claim operation requires a claim event")
            claim_payload = dict(claim)
            supplied_manager_id = str(
                claim_payload.get("managerId") or claim_payload.get("manager_id") or ""
            )
            supplied_deal_id = str(
                claim_payload.get("dealId") or claim_payload.get("deal_id") or ""
            )
            if supplied_manager_id and supplied_manager_id != row["manager_id"]:
                raise IdempotencyConflictError(
                    f"claim event manager does not match operation {operation_key!r}"
                )
            if supplied_deal_id and supplied_deal_id != row["deal_id"]:
                raise IdempotencyConflictError(
                    f"claim event deal does not match operation {operation_key!r}"
                )
            claim_payload["managerId"] = row["manager_id"]
            claim_payload["dealId"] = row["deal_id"]
            claim_event_id = self._insert_claim(
                connection,
                claim_payload,
                source="app",
                operation_key=operation_key,
            )
            now = self._now_iso()
            connection.execute(
                """
                UPDATE claim_operations SET
                    status='succeeded', result_json=?, error=NULL,
                    claim_event_id=?, updated_at=?, finalized_at=?
                WHERE operation_key = ?
                """,
                (
                    _json_dumps(dict(result)) if result is not None else None,
                    claim_event_id,
                    now,
                    now,
                    operation_key,
                ),
            )
            greeting_queued = self._enqueue_greeting_from_request(
                connection,
                row,
                current_request,
                now,
            )
            row = connection.execute(
                "SELECT * FROM claim_operations WHERE operation_key = ?", (operation_key,)
            ).fetchone()
            operation = self._operation_row(row)
            operation["transitioned"] = True
            operation["greetingQueued"] = greeting_queued
            return operation

    def fail_claim_operation(
        self,
        operation_key: Any,
        error: Any,
        *,
        result: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Mark a pending operation failed without altering terminal states."""

        self._ensure_ready()
        operation_key = str(operation_key)
        with self._transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM claim_operations WHERE operation_key = ?", (operation_key,)
            ).fetchone()
            if not row:
                raise StateStoreError(f"claim operation {operation_key!r} not found")
            if row["status"] != "pending":
                operation = self._operation_row(row)
                operation["transitioned"] = False
                return operation
            now = self._now_iso()
            connection.execute(
                """
                UPDATE claim_operations SET
                    status='failed', result_json=?, error=?, updated_at=?, finalized_at=?
                WHERE operation_key = ?
                """,
                (
                    _json_dumps(dict(result)) if result is not None else None,
                    str(error),
                    now,
                    now,
                    operation_key,
                ),
            )
            row = connection.execute(
                "SELECT * FROM claim_operations WHERE operation_key = ?", (operation_key,)
            ).fetchone()
            operation = self._operation_row(row)
            operation["transitioned"] = True
            return operation

    def list_claim_operations(self, *, status: Optional[str] = None) -> list[Dict[str, Any]]:
        self._ensure_ready()
        sql = "SELECT * FROM claim_operations"
        params: tuple[Any, ...] = ()
        if status is not None:
            if status not in {"pending", "succeeded", "failed"}:
                raise ValueError("status must be pending, succeeded or failed")
            sql += " WHERE status = ?"
            params = (status,)
        sql += " ORDER BY created_at, operation_key"
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [self._operation_row(row) for row in rows]

    def list_unresolved_claim_operations(
        self,
        manager_id: Any,
        *,
        limit: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        """Return operations that make another claim unsafe for a manager.

        A pending operation may already be changing Bitrix.  A failed operation
        is also unresolved when its result says that the remote update happened
        or may have happened.  Keeping this query manager/status-indexed lets
        access checks fail closed without scanning the full audit history.
        """

        self._ensure_ready()
        manager_id = str(manager_id)
        if not manager_id:
            return []
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM claim_operations
                WHERE manager_id = ? AND status IN ('pending', 'failed')
                ORDER BY created_at, operation_key
                """,
                (manager_id,),
            ).fetchall()
        return self._filter_unresolved_claim_rows(rows, limit=limit)

    @staticmethod
    def _claim_operation_is_unresolved(operation: Mapping[str, Any]) -> bool:
        result = operation.get("result") or {}
        return operation.get("status") == "pending" or (
            isinstance(result, dict)
            and bool(
                result.get("remoteUpdated")
                or result.get("remoteUpdateUncertain")
                or result.get("recoveryRequired")
            )
        )

    def _filter_unresolved_claim_rows(
        self,
        rows: Iterable[sqlite3.Row],
        *,
        limit: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        unresolved = []
        for row in rows:
            operation = self._operation_row(row)
            if self._claim_operation_is_unresolved(operation):
                unresolved.append(operation)
                if limit is not None and len(unresolved) >= max(0, int(limit)):
                    break
        return unresolved

    def list_unresolved_claim_operations_for_deal(
        self,
        deal_id: Any,
        *,
        limit: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        """Return every unresolved lifecycle lease for one Bitrix deal."""

        self._ensure_ready()
        deal_id = str(deal_id)
        if not deal_id:
            return []
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM claim_operations
                WHERE deal_id = ? AND status IN ('pending', 'failed')
                ORDER BY created_at, operation_key
                """,
                (deal_id,),
            ).fetchall()
        return self._filter_unresolved_claim_rows(rows, limit=limit)

    def list_unresolved_claim_deal_ids(self) -> set[str]:
        """Return deal IDs hidden from search until reconciliation finishes."""

        self._ensure_ready()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM claim_operations
                WHERE status IN ('pending', 'failed')
                ORDER BY created_at, operation_key
                """
            ).fetchall()
        return {
            str(operation.get("dealId") or "")
            for operation in self._filter_unresolved_claim_rows(rows)
            if operation.get("dealId")
        }


def create_state_store() -> StateStore:
    """Create a store from ``APP_DATA_DIR`` and ``STATE_DB_FILENAME``."""

    return StateStore()


__all__ = [
    "DEFAULT_DB_FILENAME",
    "IdempotencyConflictError",
    "LegacyMigrationError",
    "MIGRATION_MARKER",
    "SCHEMA_VERSION",
    "StateStore",
    "StateStoreError",
    "StateStoreNotReadyError",
    "create_state_store",
]
