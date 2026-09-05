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
import secrets
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional


SCHEMA_VERSION = 2
DEFAULT_DB_FILENAME = "state.sqlite3"
MIGRATION_MARKER = "legacy_json_migration_v1"
LOST_DEAL_AUTOCLOSE_WATERMARK = "lost_deal_autoclose_watermark_v1"
LOST_DEAL_AUTOCLOSE_ARMED = "lost_deal_autoclose_armed_v1"

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


class DealAlreadyClaimedError(IdempotencyConflictError):
    """A successful allocation permanently excludes a deal from automatic issuance."""


class ExtraClaimGrantUnavailableError(StateStoreError):
    """Raised when an over-limit operation cannot reserve a one-use grant."""


class ExtraClaimGrantReconciliationRequiredError(StateStoreError):
    """Raised when an uncertainty-held grant prevents safe reassignment."""


class ExtraClaimRequestAssociationConflictError(StateStoreError):
    """Raised when a remote request is already bound to another local request."""


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
            if schema_row and schema_row["value"] not in {"1", str(SCHEMA_VERSION)}:
                raise StateStoreError(
                    f"unsupported state schema version {schema_row['value']}; "
                    f"expected 1 or {SCHEMA_VERSION}"
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
                    event_uuid TEXT UNIQUE,
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
                    extra_claim_grant_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    finalized_at TEXT
                );

                CREATE TABLE IF NOT EXISTS extra_claim_requests (
                    request_key TEXT PRIMARY KEY,
                    external_id TEXT UNIQUE,
                    manager_id TEXT NOT NULL,
                    business_date TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN ('queued', 'pending', 'approved', 'rejected', 'consumed', 'expired')
                    ),
                    taken_today_snapshot INTEGER NOT NULL CHECK (taken_today_snapshot >= 0),
                    daily_limit_snapshot INTEGER NOT NULL CHECK (daily_limit_snapshot >= 0),
                    rejection_reason TEXT NOT NULL DEFAULT '',
                    remote_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS extra_claim_grants (
                    grant_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    request_key TEXT,
                    manager_id TEXT NOT NULL,
                    business_date TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN ('approved', 'reserved', 'consumed', 'expired')
                    ),
                    reserved_operation_key TEXT UNIQUE,
                    expires_at TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    consumed_at TEXT
                );

                CREATE TABLE IF NOT EXISTS integration_outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dedupe_key TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL CHECK (kind IN ('extra_claim_request', 'claim_event')),
                    path TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
                    next_attempt_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT '',
                    response_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    delivered_at TEXT,
                    dead_letter_at TEXT
                );

                CREATE TABLE IF NOT EXISTS baza_bridge_nonces (
                    key_id TEXT NOT NULL,
                    nonce TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    PRIMARY KEY (key_id, nonce)
                );
                CREATE INDEX IF NOT EXISTS idx_baza_bridge_nonce_expiry
                    ON baza_bridge_nonces(expires_at);

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

                CREATE TABLE IF NOT EXISTS lost_deal_close_operations (
                    transition_id TEXT PRIMARY KEY,
                    deal_id TEXT NOT NULL,
                    from_semantic TEXT NOT NULL CHECK (from_semantic IN ('P', 'S')),
                    to_semantic TEXT NOT NULL CHECK (to_semantic = 'F'),
                    from_category_id TEXT NOT NULL,
                    to_category_id TEXT NOT NULL,
                    from_stage_id TEXT NOT NULL,
                    to_stage_id TEXT NOT NULL,
                    transition_time TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN (
                        'checking', 'retryable', 'skipped', 'dry_run',
                        'dispatching', 'closed', 'uncertain'
                    )),
                    outcome_code TEXT NOT NULL DEFAULT '',
                    chat_id TEXT NOT NULL DEFAULT '',
                    session_id TEXT NOT NULL DEFAULT '',
                    last_message_id TEXT NOT NULL DEFAULT '',
                    history_message_count INTEGER NOT NULL DEFAULT 0
                        CHECK (history_message_count >= 0),
                    history_signature TEXT NOT NULL DEFAULT '',
                    activity_id TEXT NOT NULL DEFAULT '',
                    chat_lookup_mode TEXT NOT NULL DEFAULT '',
                    activity_updated_at TEXT NOT NULL DEFAULT '',
                    attempt_count INTEGER NOT NULL DEFAULT 1 CHECK (attempt_count >= 1),
                    lease_token TEXT NOT NULL DEFAULT '',
                    lease_expires_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    dispatching_at TEXT,
                    finalized_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_claim_events_manager_date
                    ON claim_events(manager_id, event_date);
                CREATE INDEX IF NOT EXISTS idx_claim_events_deal
                    ON claim_events(deal_id, operation_key);
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
                CREATE INDEX IF NOT EXISTS idx_extra_claim_requests_manager_date
                    ON extra_claim_requests(manager_id, business_date, created_at DESC);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_extra_claim_requests_one_active
                    ON extra_claim_requests(manager_id, business_date)
                    WHERE status IN ('queued', 'pending', 'approved');
                CREATE INDEX IF NOT EXISTS idx_extra_claim_grants_available
                    ON extra_claim_grants(manager_id, business_date, status, created_at);
                CREATE INDEX IF NOT EXISTS idx_integration_outbox_due
                    ON integration_outbox(delivered_at, next_attempt_at, id);
                CREATE INDEX IF NOT EXISTS idx_greeting_outbox_work
                    ON greeting_outbox(status, next_attempt_at, lease_expires_at, created_at);
                CREATE INDEX IF NOT EXISTS idx_greeting_outbox_manager_status
                    ON greeting_outbox(manager_id, status);
                CREATE INDEX IF NOT EXISTS idx_lost_deal_close_deal
                    ON lost_deal_close_operations(deal_id, transition_time DESC);
                CREATE INDEX IF NOT EXISTS idx_lost_deal_close_status
                    ON lost_deal_close_operations(status, updated_at);
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
            if "extra_claim_grant_id" not in operation_columns:
                connection.execute(
                    "ALTER TABLE claim_operations ADD COLUMN extra_claim_grant_id TEXT"
                )
            claim_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(claim_events)")
            }
            if "event_uuid" not in claim_columns:
                connection.execute("ALTER TABLE claim_events ADD COLUMN event_uuid TEXT")
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_claim_events_event_uuid "
                "ON claim_events(event_uuid) WHERE event_uuid IS NOT NULL"
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
            outbox_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(integration_outbox)")
            }
            if "dead_letter_at" not in outbox_columns:
                connection.execute(
                    "ALTER TABLE integration_outbox ADD COLUMN dead_letter_at TEXT"
                )
            lost_close_columns = {
                row["name"]
                for row in connection.execute(
                    "PRAGMA table_info(lost_deal_close_operations)"
                )
            }
            if "from_category_id" not in lost_close_columns:
                connection.execute(
                    "ALTER TABLE lost_deal_close_operations "
                    "ADD COLUMN from_category_id TEXT NOT NULL DEFAULT '0'"
                )
            if "to_category_id" not in lost_close_columns:
                connection.execute(
                    "ALTER TABLE lost_deal_close_operations "
                    "ADD COLUMN to_category_id TEXT NOT NULL DEFAULT '0'"
                )
            if "session_id" not in lost_close_columns:
                connection.execute(
                    "ALTER TABLE lost_deal_close_operations "
                    "ADD COLUMN session_id TEXT NOT NULL DEFAULT ''"
                )
            if "last_message_id" not in lost_close_columns:
                connection.execute(
                    "ALTER TABLE lost_deal_close_operations "
                    "ADD COLUMN last_message_id TEXT NOT NULL DEFAULT ''"
                )
            if "history_message_count" not in lost_close_columns:
                connection.execute(
                    "ALTER TABLE lost_deal_close_operations "
                    "ADD COLUMN history_message_count INTEGER NOT NULL DEFAULT 0"
                )
            if "activity_id" not in lost_close_columns:
                connection.execute(
                    "ALTER TABLE lost_deal_close_operations "
                    "ADD COLUMN activity_id TEXT NOT NULL DEFAULT ''"
                )
            if "history_signature" not in lost_close_columns:
                connection.execute(
                    "ALTER TABLE lost_deal_close_operations "
                    "ADD COLUMN history_signature TEXT NOT NULL DEFAULT ''"
                )
            if "chat_lookup_mode" not in lost_close_columns:
                connection.execute(
                    "ALTER TABLE lost_deal_close_operations "
                    "ADD COLUMN chat_lookup_mode TEXT NOT NULL DEFAULT ''"
                )
            if "activity_updated_at" not in lost_close_columns:
                connection.execute(
                    "ALTER TABLE lost_deal_close_operations "
                    "ADD COLUMN activity_updated_at TEXT NOT NULL DEFAULT ''"
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
            # Schema v2 gives every historical claim a durable cross-system
            # identity and queues it exactly once for the Baza dashboard.  The
            # backfill and version marker share this transaction, so a crash
            # can neither lose nor duplicate an event.
            historical_rows = connection.execute(
                "SELECT * FROM claim_events WHERE event_uuid IS NULL OR event_uuid = '' ORDER BY id"
            ).fetchall()
            for historical_row in historical_rows:
                event_uuid = str(uuid.uuid4())
                historical_payload = self._payload_from_row(historical_row)
                connection.execute(
                    "UPDATE claim_events SET event_uuid = ? WHERE id = ?",
                    (event_uuid, historical_row["id"]),
                )
                self._enqueue_claim_export_in_connection(
                    connection,
                    event_uuid=event_uuid,
                    operation_key=historical_row["operation_key"] or f"legacy:{event_uuid}",
                    deal_id=historical_row["deal_id"],
                    manager_id=historical_row["manager_id"],
                    occurred_at=historical_row["timestamp"],
                    business_date=historical_row["event_date"],
                    extra_claim_request_id=(
                        historical_payload.get("extraClaimRequestId")
                        or historical_payload.get("extra_claim_request_id")
                    ),
                    app_version=historical_payload.get("appVersion") or "legacy-v1",
                    recovered=True,
                    now=now,
                )
            # A v2 database can contain rows created before an integration was
            # configured.  Ensure those rows also have one durable outbox item.
            for claim_row in connection.execute(
                "SELECT * FROM claim_events WHERE event_uuid IS NOT NULL AND event_uuid <> ''"
            ).fetchall():
                claim_payload = self._payload_from_row(claim_row)
                self._enqueue_claim_export_in_connection(
                    connection,
                    event_uuid=claim_row["event_uuid"],
                    operation_key=claim_row["operation_key"] or f"legacy:{claim_row['event_uuid']}",
                    deal_id=claim_row["deal_id"],
                    manager_id=claim_row["manager_id"],
                    occurred_at=claim_row["timestamp"],
                    business_date=claim_row["event_date"],
                    extra_claim_request_id=(
                        claim_payload.get("extraClaimRequestId")
                        or claim_payload.get("extra_claim_request_id")
                    ),
                    app_version=claim_payload.get("appVersion") or "legacy-v1",
                    recovered=bool(claim_payload.get("recovered", True)),
                    now=now,
                )
            if not schema_row:
                connection.execute(
                    "INSERT INTO meta(key, value, updated_at) VALUES('schema_version', ?, ?)",
                    (str(SCHEMA_VERSION), now),
                )
            elif schema_row["value"] == "1":
                connection.execute(
                    "UPDATE meta SET value = ?, updated_at = ? WHERE key = 'schema_version'",
                    (str(SCHEMA_VERSION), now),
                )
            # Keep the local schema-install time only as an audit watermark.
            # The worker's real no-backlog boundary is armed separately from
            # the current remote stage-history ID on its first enabled poll.
            connection.execute(
                """
                INSERT OR IGNORE INTO meta(key, value, updated_at)
                VALUES(?, ?, ?)
                """,
                (LOST_DEAL_AUTOCLOSE_WATERMARK, now, now),
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

    def consume_bridge_nonce(self, key_id: str, nonce: str, *, now: int, expires_at: int) -> bool:
        """Reserve a signed request once across concurrent processes and restarts."""
        self._ensure_ready()
        if not key_id or not nonce or expires_at <= now:
            raise ValueError("invalid bridge nonce")
        with self._transaction(immediate=True) as connection:
            connection.execute("DELETE FROM baza_bridge_nonces WHERE expires_at < ?", (now,))
            cursor = connection.execute(
                "INSERT OR IGNORE INTO baza_bridge_nonces(key_id, nonce, expires_at) VALUES (?, ?, ?)",
                (key_id, nonce, expires_at),
            )
            return cursor.rowcount == 1

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
                        "extra_claim_requests",
                        "extra_claim_grants",
                        "integration_outbox",
                        "greeting_outbox",
                        "lost_deal_close_operations",
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
                    "extra_claim_requests",
                    "extra_claim_grants",
                    "integration_outbox",
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
    def _claim_export_timestamp(value: Any) -> str:
        """Canonical cross-service timestamp with millisecond precision."""

        raw = str(value or "").strip()
        try:
            parsed = datetime.fromisoformat(
                raw[:-1] + "+00:00" if raw.endswith("Z") else raw
            )
        except ValueError as exc:
            raise ValueError("claim export timestamp must be an ISO-8601 datetime") from exc
        if parsed.tzinfo is None:
            raise ValueError("claim export timestamp must include an offset")
        return parsed.isoformat(timespec="milliseconds")

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

    def _enqueue_outbox_in_connection(
        self,
        connection: sqlite3.Connection,
        *,
        dedupe_key: str,
        kind: str,
        path: str,
        payload: Mapping[str, Any],
        now: Optional[str] = None,
    ) -> None:
        now = now or self._now_iso()
        connection.execute(
            """
            INSERT OR IGNORE INTO integration_outbox(
                dedupe_key, kind, path, payload_json, next_attempt_at,
                created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (dedupe_key, kind, path, _json_dumps(dict(payload)), now, now, now),
        )

    def _enqueue_claim_export_in_connection(
        self,
        connection: sqlite3.Connection,
        *,
        event_uuid: str,
        operation_key: str,
        deal_id: str,
        manager_id: str,
        occurred_at: str,
        business_date: str,
        extra_claim_request_id: Optional[str],
        app_version: str,
        recovered: bool,
        now: Optional[str] = None,
    ) -> None:
        payload: Dict[str, Any] = {
            "schemaVersion": 1,
            "eventUuid": str(event_uuid),
            "operationKey": str(operation_key),
            "bitrixDealId": str(deal_id),
            "bitrixUserId": str(manager_id),
            "occurredAt": self._claim_export_timestamp(occurred_at),
            "businessDate": str(business_date),
            "appVersion": str(app_version or "unknown"),
            "recovered": bool(recovered),
        }
        if extra_claim_request_id:
            payload["extraClaimRequestId"] = str(extra_claim_request_id)
        self._enqueue_outbox_in_connection(
            connection,
            dedupe_key=f"claim-event:{event_uuid}",
            kind="claim_event",
            path="/integrations/deal-picker/v1/claim-events",
            payload=payload,
            now=now,
        )

    def _insert_claim(
        self,
        connection: sqlite3.Connection,
        entry: Mapping[str, Any],
        *,
        source: str,
        migration: bool = False,
        operation_key: Optional[str] = None,
        extra_claim_request_id: Optional[str] = None,
        app_version: str = "unknown",
        recovered: bool = False,
    ) -> int:
        payload = dict(entry)
        timestamp, event_date = self._normalize_timestamp(payload.get("timestamp"), migration=migration)
        manager_id = str(payload.get("managerId") or payload.get("manager_id") or "")
        deal_id = str(payload.get("dealId") or payload.get("deal_id") or "")
        raw_operation_key = (
            operation_key or payload.get("operationKey") or payload.get("operation_key")
        )
        operation_key = str(raw_operation_key) if raw_operation_key else None
        event_uuid = str(
            payload.get("eventUuid")
            or payload.get("event_uuid")
            or uuid.uuid4()
        )
        try:
            event_uuid = str(uuid.UUID(event_uuid))
        except (ValueError, AttributeError, TypeError) as exc:
            raise ValueError("eventUuid must be a UUID") from exc
        if not migration and (not manager_id or not deal_id):
            raise ValueError("claim event requires managerId and dealId")
        extra_claim_request_id = (
            extra_claim_request_id
            or payload.get("extraClaimRequestId")
            or payload.get("extra_claim_request_id")
        )
        recovered = bool(recovered or migration or source != "app")
        payload.update(
            {
                "eventUuid": event_uuid,
                "timestamp": timestamp,
                "managerId": manager_id,
                "dealId": deal_id,
                "operationKey": operation_key or "",
                "appVersion": str(app_version or "unknown"),
                "recovered": recovered,
            }
        )
        if extra_claim_request_id:
            payload["extraClaimRequestId"] = str(extra_claim_request_id)
        cursor = connection.execute(
            """
            INSERT INTO claim_events(
                event_uuid, timestamp, event_date, manager_id, manager_name, deal_id, deal_title,
                payload_json, source, operation_key, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_uuid,
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
        self._enqueue_claim_export_in_connection(
            connection,
            event_uuid=event_uuid,
            operation_key=operation_key or f"legacy:{event_uuid}",
            deal_id=deal_id,
            manager_id=manager_id,
            occurred_at=timestamp,
            business_date=event_date,
            extra_claim_request_id=extra_claim_request_id,
            app_version=app_version,
            recovered=recovered,
            now=self._now_iso(),
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
                "eventUuid": row["event_uuid"],
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
    # Baza integration: request state, one-use grants and durable outbox
    # ------------------------------------------------------------------

    @staticmethod
    def _extra_request_row(row: sqlite3.Row) -> Dict[str, Any]:
        remote = None
        if row["remote_json"]:
            try:
                remote = json.loads(row["remote_json"])
            except (TypeError, json.JSONDecodeError):
                remote = None
        return {
            "requestKey": row["request_key"],
            "id": row["external_id"],
            "managerId": row["manager_id"],
            "businessDate": row["business_date"],
            "reason": row["reason"],
            "status": row["status"],
            "takenTodaySnapshot": row["taken_today_snapshot"],
            "dailyLimitSnapshot": row["daily_limit_snapshot"],
            "rejectionReason": row["rejection_reason"],
            "remote": remote,
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
        }

    @staticmethod
    def _grant_row(row: sqlite3.Row) -> Dict[str, Any]:
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, json.JSONDecodeError):
            payload = {}
        return {
            "id": row["grant_id"],
            "requestId": row["request_id"],
            "requestKey": row["request_key"],
            "managerId": row["manager_id"],
            "businessDate": row["business_date"],
            "status": row["status"],
            "reservedOperationKey": row["reserved_operation_key"],
            "expiresAt": row["expires_at"],
            "payload": payload if isinstance(payload, dict) else {},
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "consumedAt": row["consumed_at"],
        }

    @staticmethod
    def _outbox_row(row: sqlite3.Row) -> Dict[str, Any]:
        def decode(column: str) -> Any:
            raw = row[column]
            if raw is None:
                return None
            try:
                return json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                return None

        return {
            "id": row["id"],
            "dedupeKey": row["dedupe_key"],
            "kind": row["kind"],
            "path": row["path"],
            "payload": decode("payload_json") or {},
            "attemptCount": row["attempt_count"],
            "nextAttemptAt": row["next_attempt_at"],
            "lastError": row["last_error"],
            "response": decode("response_json"),
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "deliveredAt": row["delivered_at"],
            "deadLetterAt": row["dead_letter_at"],
        }

    def create_extra_claim_request(
        self,
        manager_id: Any,
        business_date: str,
        reason: str,
        *,
        taken_today_snapshot: int,
        daily_limit_snapshot: int,
        request_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create one active request or return the current active request.

        The UUID is generated once and persisted before any network attempt.
        This makes double-clicks and process restarts idempotent without
        preventing a later request after rejection or consumption.
        """

        self._ensure_ready()
        manager_id = str(manager_id or "")
        business_date = self._normalize_date_filter(business_date, "business_date") or ""
        reason = str(reason or "").strip()
        if not manager_id or not business_date:
            raise ValueError("manager_id and business_date are required")
        if not 10 <= len(reason) <= 500:
            raise ValueError("reason must contain 10 to 500 characters")
        request_key = str(request_key or uuid.uuid4())
        try:
            request_key = str(uuid.UUID(request_key))
        except (ValueError, AttributeError, TypeError) as exc:
            raise ValueError("request_key must be a UUID") from exc
        taken_today_snapshot = max(0, int(taken_today_snapshot))
        daily_limit_snapshot = max(0, int(daily_limit_snapshot))
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            active = connection.execute(
                """
                SELECT * FROM extra_claim_requests
                WHERE manager_id = ? AND business_date = ?
                  AND status IN ('queued', 'pending', 'approved')
                ORDER BY created_at DESC LIMIT 1
                """,
                (manager_id, business_date),
            ).fetchone()
            if active:
                result = self._extra_request_row(active)
                result["created"] = False
                return result
            connection.execute(
                """
                INSERT INTO extra_claim_requests(
                    request_key, manager_id, business_date, reason, status,
                    taken_today_snapshot, daily_limit_snapshot, created_at, updated_at
                ) VALUES(?, ?, ?, ?, 'queued', ?, ?, ?, ?)
                """,
                (
                    request_key,
                    manager_id,
                    business_date,
                    reason,
                    taken_today_snapshot,
                    daily_limit_snapshot,
                    now,
                    now,
                ),
            )
            self._enqueue_outbox_in_connection(
                connection,
                dedupe_key=f"extra-claim-request:{request_key}",
                kind="extra_claim_request",
                path="/integrations/deal-picker/v1/extra-claim-requests",
                payload={
                    "requestKey": request_key,
                    "bitrixUserId": manager_id,
                    "businessDate": business_date,
                    "requestedQuantity": 1,
                    "takenTodaySnapshot": taken_today_snapshot,
                    "dailyLimitSnapshot": daily_limit_snapshot,
                    "reason": reason,
                },
                now=now,
            )
            row = connection.execute(
                "SELECT * FROM extra_claim_requests WHERE request_key = ?",
                (request_key,),
            ).fetchone()
            result = self._extra_request_row(row)
            result["created"] = True
            return result

    def _find_request_for_remote_in_connection(
        self,
        connection: sqlite3.Connection,
        remote: Mapping[str, Any],
        manager_id: str,
        business_date: str,
    ) -> Optional[sqlite3.Row]:
        request_key = str(remote.get("requestKey") or "")
        external_id = str(remote.get("id") or remote.get("requestId") or "")
        if request_key:
            row = connection.execute(
                "SELECT * FROM extra_claim_requests WHERE request_key = ?",
                (request_key,),
            ).fetchone()
            if row:
                return row
        if external_id:
            row = connection.execute(
                "SELECT * FROM extra_claim_requests WHERE external_id = ?",
                (external_id,),
            ).fetchone()
            if row:
                return row
        return connection.execute(
            """
            SELECT * FROM extra_claim_requests
            WHERE manager_id = ? AND business_date = ?
              AND status IN ('queued', 'pending', 'approved')
            ORDER BY created_at DESC LIMIT 1
            """,
            (manager_id, business_date),
        ).fetchone()

    @staticmethod
    def _normalize_remote_request_status(value: Any) -> str:
        status = str(value or "pending").strip().lower()
        aliases = {"declined": "rejected", "used": "consumed", "cancelled": "expired"}
        status = aliases.get(status, status)
        return status if status in {"pending", "approved", "rejected", "consumed", "expired"} else "pending"

    @staticmethod
    def _merge_extra_claim_request_status(local_status: Any, remote_status: Any) -> str:
        """Apply only forward request transitions from a remote snapshot.

        A grants/query response can be stale while the durable claim-event
        export is still waiting in the local outbox.  In particular, Baza may
        still report ``approved`` after this process has atomically consumed
        the grant.  Terminal local evidence must never be revived, and an
        approved request must not move backwards to pending.
        """

        local = str(local_status or "queued").strip().lower()
        remote = StateStore._normalize_remote_request_status(remote_status)
        if local in {"rejected", "consumed", "expired"}:
            return local
        if local == "approved":
            return remote if remote in {"consumed", "expired"} else "approved"
        if local == "pending":
            return remote if remote != "pending" else "pending"
        return remote

    @staticmethod
    def _assert_extra_request_identity_available(
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        external_id: Optional[str],
    ) -> None:
        """Keep the local-to-remote request identity immutable and one-to-one."""

        if not external_id:
            return
        current_external_id = str(row["external_id"] or "")
        if current_external_id and current_external_id != external_id:
            raise ExtraClaimRequestAssociationConflictError(
                f"local request {row['request_key']!r} is already bound to "
                f"remote request {current_external_id!r}"
            )
        owner = connection.execute(
            "SELECT request_key FROM extra_claim_requests "
            "WHERE external_id=? AND request_key<>? LIMIT 1",
            (external_id, row["request_key"]),
        ).fetchone()
        if owner:
            raise ExtraClaimRequestAssociationConflictError(
                f"remote request {external_id!r} is already bound to "
                f"local request {owner['request_key']!r}"
            )

    def import_extra_claim_state(
        self,
        manager_id: Any,
        business_date: str,
        response: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """Merge a signed Baza status response without reviving used grants."""

        self._ensure_ready()
        manager_id = str(manager_id or "")
        business_date = self._normalize_date_filter(business_date, "business_date") or ""
        response = dict(response or {})
        container = response.get("data") if isinstance(response.get("data"), dict) else response
        request_payload = container.get("request") if isinstance(container.get("request"), dict) else None
        grants = container.get("grants")
        if grants is None and isinstance(container.get("grant"), dict):
            grants = [container["grant"]]
        grants = grants if isinstance(grants, list) else []
        authoritative_grant_ids = {
            str(raw_grant.get("id") or raw_grant.get("requestId") or "")
            for raw_grant in grants
            if isinstance(raw_grant, dict)
            and str(raw_grant.get("status") or "approved").lower() == "approved"
            and str(raw_grant.get("bitrixUserId") or manager_id) == manager_id
            and str(raw_grant.get("businessDate") or business_date) == business_date
            and str(raw_grant.get("id") or raw_grant.get("requestId") or "")
        }
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            # grants/query is authoritative for grants that have not yet been
            # used.  Keeping an omitted approved row would let a revoked grant
            # change Bitrix before the later claim-event export is rejected.
            # Reserved rows are deliberately retained: they belong to an
            # in-flight or uncertainty-held CRM operation and must be resolved
            # through the claim reconciliation path.
            for local_grant in connection.execute(
                """
                SELECT grant_id FROM extra_claim_grants
                WHERE manager_id=? AND business_date=? AND status='approved'
                """,
                (manager_id, business_date),
            ).fetchall():
                if local_grant["grant_id"] not in authoritative_grant_ids:
                    connection.execute(
                        "UPDATE extra_claim_grants SET status='expired', updated_at=? "
                        "WHERE grant_id=? AND status='approved'",
                        (now, local_grant["grant_id"]),
                    )
            if request_payload:
                row = self._find_request_for_remote_in_connection(
                    connection, request_payload, manager_id, business_date
                )
                external_id = str(
                    request_payload.get("id") or request_payload.get("requestId") or ""
                ) or None
                status = self._normalize_remote_request_status(request_payload.get("status"))
                rejection_reason = str(
                    request_payload.get("decisionNote")
                    or request_payload.get("rejectionReason")
                    or request_payload.get("reviewerComment")
                    or ""
                )
                if row:
                    self._assert_extra_request_identity_available(
                        connection, row, external_id
                    )
                    merged_status = self._merge_extra_claim_request_status(
                        row["status"], status
                    )
                    merged_rejection_reason = (
                        rejection_reason or str(row["rejection_reason"] or "")
                    )
                    connection.execute(
                        """
                        UPDATE extra_claim_requests SET
                            external_id=COALESCE(external_id, ?), status=?,
                            rejection_reason=?, remote_json=?, updated_at=?
                        WHERE request_key=?
                        """,
                        (
                            external_id,
                            merged_status,
                            merged_rejection_reason,
                            _json_dumps(request_payload),
                            now,
                            row["request_key"],
                        ),
                    )
                elif external_id:
                    synthetic_key = str(request_payload.get("requestKey") or uuid.uuid4())
                    try:
                        synthetic_key = str(uuid.UUID(synthetic_key))
                    except (ValueError, AttributeError, TypeError):
                        synthetic_key = str(uuid.uuid4())
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO extra_claim_requests(
                            request_key, external_id, manager_id, business_date, reason,
                            status, taken_today_snapshot, daily_limit_snapshot,
                            rejection_reason, remote_json, created_at, updated_at
                        ) VALUES(?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?)
                        """,
                        (
                            synthetic_key,
                            external_id,
                            manager_id,
                            business_date,
                            str(request_payload.get("reason") or "Запрос восстановлен из Базы"),
                            status,
                            rejection_reason,
                            _json_dumps(request_payload),
                            now,
                            now,
                        ),
                    )
            for raw_grant in grants:
                if not isinstance(raw_grant, dict):
                    continue
                grant_id = str(raw_grant.get("id") or raw_grant.get("requestId") or "")
                request_id = str(raw_grant.get("requestId") or raw_grant.get("id") or "")
                grant_manager = str(raw_grant.get("bitrixUserId") or manager_id)
                grant_date = str(raw_grant.get("businessDate") or business_date)
                if not grant_id or not request_id or grant_manager != manager_id or grant_date != business_date:
                    continue
                remote_status = str(raw_grant.get("status") or "approved").lower()
                if remote_status != "approved":
                    continue
                existing = connection.execute(
                    "SELECT * FROM extra_claim_grants WHERE grant_id = ?",
                    (grant_id,),
                ).fetchone()
                request_row = connection.execute(
                    "SELECT * FROM extra_claim_requests WHERE external_id = ? LIMIT 1",
                    (request_id,),
                ).fetchone()
                if not request_row:
                    request_row = connection.execute(
                        """
                        SELECT * FROM extra_claim_requests
                        WHERE manager_id=? AND business_date=?
                          AND status IN ('queued', 'pending', 'approved')
                        ORDER BY created_at DESC LIMIT 1
                        """,
                        (manager_id, business_date),
                    ).fetchone()
                request_key = request_row["request_key"] if request_row else None
                if existing:
                    if existing["status"] in {"reserved", "consumed"}:
                        continue
                    connection.execute(
                        """
                        UPDATE extra_claim_grants SET request_id=?, request_key=?,
                            status='approved', reserved_operation_key=NULL,
                            expires_at=?, payload_json=?, updated_at=?, consumed_at=NULL
                        WHERE grant_id=?
                        """,
                        (
                            request_id,
                            request_key,
                            raw_grant.get("expiresAt"),
                            _json_dumps(raw_grant),
                            now,
                            grant_id,
                        ),
                    )
                else:
                    connection.execute(
                        """
                        INSERT INTO extra_claim_grants(
                            grant_id, request_id, request_key, manager_id,
                            business_date, status, expires_at, payload_json,
                            created_at, updated_at
                        ) VALUES(?, ?, ?, ?, ?, 'approved', ?, ?, ?, ?)
                        """,
                        (
                            grant_id,
                            request_id,
                            request_key,
                            manager_id,
                            business_date,
                            raw_grant.get("expiresAt"),
                            _json_dumps(raw_grant),
                            now,
                            now,
                        ),
                    )
                if request_row and request_row["status"] in {"queued", "pending"}:
                    connection.execute(
                        """
                        UPDATE extra_claim_requests SET external_id=COALESCE(external_id, ?),
                            status='approved', updated_at=? WHERE request_key=?
                        """,
                        (request_id, now, request_row["request_key"]),
                    )
        return self.get_extra_claim_state(manager_id, business_date)

    def apply_extra_claim_request_response(
        self,
        request_key: str,
        response: Mapping[str, Any],
    ) -> Optional[Dict[str, Any]]:
        self._ensure_ready()
        response = dict(response or {})
        container = response.get("data") if isinstance(response.get("data"), dict) else response
        remote = container.get("request") if isinstance(container.get("request"), dict) else container
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM extra_claim_requests WHERE request_key=?",
                (str(request_key),),
            ).fetchone()
            if not row:
                return None
            external_id = str(remote.get("id") or remote.get("requestId") or "") or None
            status = self._normalize_remote_request_status(remote.get("status"))
            self._assert_extra_request_identity_available(connection, row, external_id)
            merged_status = self._merge_extra_claim_request_status(
                row["status"], status
            )
            remote_rejection_reason = str(
                remote.get("decisionNote")
                or remote.get("rejectionReason")
                or remote.get("reviewerComment")
                or ""
            )
            connection.execute(
                """
                UPDATE extra_claim_requests SET external_id=COALESCE(external_id, ?),
                    status=?, rejection_reason=?, remote_json=?, updated_at=?
                WHERE request_key=?
                """,
                (
                    external_id,
                    merged_status,
                    remote_rejection_reason or str(row["rejection_reason"] or ""),
                    _json_dumps(remote),
                    now,
                    str(request_key),
                ),
            )
            row = connection.execute(
                "SELECT * FROM extra_claim_requests WHERE request_key=?",
                (str(request_key),),
            ).fetchone()
        return self._extra_request_row(row) if row else None

    def get_extra_claim_state(
        self,
        manager_id: Any,
        business_date: str,
        *,
        operation_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._ensure_ready()
        manager_id = str(manager_id or "")
        business_date = self._normalize_date_filter(business_date, "business_date") or ""
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            for candidate in connection.execute(
                """
                SELECT * FROM extra_claim_grants
                WHERE manager_id=? AND business_date=? AND status='approved'
                """,
                (manager_id, business_date),
            ).fetchall():
                if candidate["expires_at"] and self._iso_is_expired(
                    candidate["expires_at"], now
                ):
                    connection.execute(
                        "UPDATE extra_claim_grants SET status='expired', updated_at=? WHERE grant_id=?",
                        (now, candidate["grant_id"]),
                    )
                    if candidate["request_key"]:
                        connection.execute(
                            "UPDATE extra_claim_requests SET status='expired', updated_at=? "
                            "WHERE request_key=? AND status='approved'",
                            (now, candidate["request_key"]),
                        )
            request = connection.execute(
                """
                SELECT * FROM extra_claim_requests
                WHERE manager_id=? AND business_date=?
                ORDER BY created_at DESC LIMIT 1
                """,
                (manager_id, business_date),
            ).fetchone()
            params: list[Any] = [manager_id, business_date]
            clause = "status='approved'"
            if operation_key:
                clause = "(status='approved' OR (status='reserved' AND reserved_operation_key=?))"
                params.append(str(operation_key))
            grant = connection.execute(
                f"""
                SELECT * FROM extra_claim_grants
                WHERE manager_id=? AND business_date=? AND {clause}
                ORDER BY CASE status WHEN 'reserved' THEN 0 ELSE 1 END, created_at
                LIMIT 1
                """,
                params,
            ).fetchone()
        return {
            "request": self._extra_request_row(request) if request else None,
            "grant": self._grant_row(grant) if grant else None,
            "grantAvailable": bool(grant),
        }

    def _iso_is_expired(self, expires_at: Any, now: Any) -> bool:
        try:
            expiry = datetime.fromisoformat(str(expires_at).replace("Z", "+00:00"))
            current = datetime.fromisoformat(str(now).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return True
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=self.local_timezone)
        if current.tzinfo is None:
            current = current.replace(tzinfo=self.local_timezone)
        return expiry.astimezone(timezone.utc) <= current.astimezone(timezone.utc)

    def _reserve_extra_claim_grant_in_connection(
        self,
        connection: sqlite3.Connection,
        manager_id: str,
        business_date: str,
        operation_key: str,
        now: str,
    ) -> sqlite3.Row:
        grant = connection.execute(
            """
            SELECT * FROM extra_claim_grants
            WHERE manager_id=? AND business_date=?
              AND status='reserved' AND reserved_operation_key=?
            LIMIT 1
            """,
            (manager_id, business_date, operation_key),
        ).fetchone()
        if grant:
            return grant
        candidates = connection.execute(
            """
            SELECT * FROM extra_claim_grants
            WHERE manager_id=? AND business_date=? AND status='approved'
            ORDER BY created_at, grant_id
            """,
            (manager_id, business_date),
        ).fetchall()
        for candidate in candidates:
            expires_at = str(candidate["expires_at"] or "")
            if expires_at:
                expired = self._iso_is_expired(expires_at, now)
                if expired:
                    connection.execute(
                        "UPDATE extra_claim_grants SET status='expired', updated_at=? WHERE grant_id=?",
                        (now, candidate["grant_id"]),
                    )
                    if candidate["request_key"]:
                        connection.execute(
                            "UPDATE extra_claim_requests SET status='expired', updated_at=? "
                            "WHERE request_key=? AND status='approved'",
                            (now, candidate["request_key"]),
                        )
                    continue
            changed = connection.execute(
                """
                UPDATE extra_claim_grants SET status='reserved', reserved_operation_key=?, updated_at=?
                WHERE grant_id=? AND status='approved'
                """,
                (operation_key, now, candidate["grant_id"]),
            ).rowcount
            if changed:
                return connection.execute(
                    "SELECT * FROM extra_claim_grants WHERE grant_id=?",
                    (candidate["grant_id"],),
                ).fetchone()
        raise ExtraClaimGrantUnavailableError("no approved extra-claim grant is available")

    def list_due_outbox(
        self,
        *,
        now: Optional[str] = None,
        limit: int = 20,
        kinds: Optional[Iterable[str]] = None,
        dedupe_key: Optional[str] = None,
    ) -> list[Dict[str, Any]]:
        self._ensure_ready()
        now = now or self._now_iso()
        clauses = [
            "delivered_at IS NULL",
            "dead_letter_at IS NULL",
            "next_attempt_at <= ?",
        ]
        params: list[Any] = [now]
        if kinds is not None:
            normalized_kinds = tuple(
                kind for kind in (str(item) for item in kinds) if kind in {"extra_claim_request", "claim_event"}
            )
            if not normalized_kinds:
                return []
            clauses.append("kind IN (" + ",".join("?" for _ in normalized_kinds) + ")")
            params.extend(normalized_kinds)
        if dedupe_key is not None:
            clauses.append("dedupe_key = ?")
            params.append(str(dedupe_key))
        params.append(max(0, min(1000, int(limit))))
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM integration_outbox WHERE "
                + " AND ".join(clauses)
                + " ORDER BY CASE "
                + "WHEN kind='claim_event' "
                + "AND instr(payload_json, '\"extraClaimRequestId\":') > 0 THEN 0 "
                + "WHEN kind='extra_claim_request' THEN 1 ELSE 2 END, id LIMIT ?",
                params,
            ).fetchall()
        return [self._outbox_row(row) for row in rows]

    def mark_outbox_delivered(
        self,
        outbox_id: int,
        response: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        self._ensure_ready()
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            connection.execute(
                """
                UPDATE integration_outbox SET delivered_at=?, updated_at=?,
                    last_error='', response_json=?
                WHERE id=? AND delivered_at IS NULL
                """,
                (
                    now,
                    now,
                    _json_dumps(dict(response)) if response is not None else None,
                    int(outbox_id),
                ),
            )
            row = connection.execute(
                "SELECT * FROM integration_outbox WHERE id=?", (int(outbox_id),)
            ).fetchone()
        if not row:
            raise StateStoreError(f"outbox item {outbox_id} not found")
        return self._outbox_row(row)

    def mark_outbox_failed(
        self,
        outbox_id: int,
        error: Any,
        *,
        base_delay_seconds: int = 5,
        maximum_delay_seconds: int = 3600,
    ) -> Dict[str, Any]:
        self._ensure_ready()
        now_dt = datetime.now(self.local_timezone)
        now = now_dt.isoformat(timespec="microseconds")
        with self._transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM integration_outbox WHERE id=?", (int(outbox_id),)
            ).fetchone()
            if not row:
                raise StateStoreError(f"outbox item {outbox_id} not found")
            if row["delivered_at"]:
                return self._outbox_row(row)
            attempt_count = int(row["attempt_count"]) + 1
            delay = min(
                max(1, int(maximum_delay_seconds)),
                max(1, int(base_delay_seconds)) * (2 ** min(attempt_count - 1, 16)),
            )
            next_attempt = (now_dt + timedelta(seconds=delay)).isoformat(timespec="microseconds")
            connection.execute(
                """
                UPDATE integration_outbox SET attempt_count=?, next_attempt_at=?,
                    last_error=?, updated_at=? WHERE id=? AND delivered_at IS NULL
                """,
                (attempt_count, next_attempt, str(error)[:1000], now, int(outbox_id)),
            )
            row = connection.execute(
                "SELECT * FROM integration_outbox WHERE id=?", (int(outbox_id),)
            ).fetchone()
        return self._outbox_row(row)

    def mark_outbox_dead_letter(
        self,
        outbox_id: int,
        error: Any,
        response: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Stop permanent 4xx failures without deleting audit evidence."""

        self._ensure_ready()
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            connection.execute(
                """
                UPDATE integration_outbox SET dead_letter_at=?, updated_at=?,
                    last_error=?, response_json=?
                WHERE id=? AND delivered_at IS NULL AND dead_letter_at IS NULL
                """,
                (
                    now,
                    now,
                    str(error)[:1000],
                    _json_dumps(dict(response)) if response is not None else None,
                    int(outbox_id),
                ),
            )
            row = connection.execute(
                "SELECT * FROM integration_outbox WHERE id=?", (int(outbox_id),)
            ).fetchone()
        if not row:
            raise StateStoreError(f"outbox item {outbox_id} not found")
        return self._outbox_row(row)

    def reject_extra_claim_request_locally(
        self,
        request_key: str,
        reason: str,
    ) -> Optional[Dict[str, Any]]:
        self._ensure_ready()
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            connection.execute(
                """
                UPDATE extra_claim_requests SET status='rejected',
                    rejection_reason=?, updated_at=?
                WHERE request_key=? AND status IN ('queued','pending')
                """,
                (str(reason)[:500], now, str(request_key)),
            )
            row = connection.execute(
                "SELECT * FROM extra_claim_requests WHERE request_key=?",
                (str(request_key),),
            ).fetchone()
        return self._extra_request_row(row) if row else None

    def list_outbox(self, *, delivered: Optional[bool] = None) -> list[Dict[str, Any]]:
        self._ensure_ready()
        sql = "SELECT * FROM integration_outbox"
        params: tuple[Any, ...] = ()
        if delivered is True:
            sql += " WHERE delivered_at IS NOT NULL"
        elif delivered is False:
            sql += " WHERE delivered_at IS NULL AND dead_letter_at IS NULL"
        sql += " ORDER BY id"
        with self._connect() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [self._outbox_row(row) for row in rows]
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
            "extraClaimGrantId": row["extra_claim_grant_id"],
            "createdAt": row["created_at"],
            "updatedAt": row["updated_at"],
            "finalizedAt": row["finalized_at"],
        }
        if created is not None:
            result["created"] = created
        return result

    def list_claimed_deal_ids(self) -> set[str]:
        """Include legacy successful claims without rewriting their audit history."""
        self._ensure_ready()
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT deal_id FROM claim_events UNION "
                "SELECT deal_id FROM claim_operations WHERE status='succeeded'"
            ).fetchall()
        return {str(row["deal_id"]) for row in rows}

    def deal_was_claimed(self, deal_id: Any, *, except_operation_key: str = "") -> bool:
        self._ensure_ready()
        with self._connect() as connection:
            return self._deal_was_claimed(connection, str(deal_id), except_operation_key)

    @staticmethod
    def _deal_was_claimed(connection: sqlite3.Connection, deal_id: str, operation_key: str) -> bool:
        return bool(connection.execute(
            "SELECT 1 FROM claim_events WHERE deal_id=? "
            "AND (operation_key IS NULL OR operation_key != ?) "
            "UNION ALL SELECT 1 FROM claim_operations WHERE deal_id=? "
            "AND operation_key != ? AND status='succeeded' LIMIT 1",
            (deal_id, operation_key, deal_id, operation_key),
        ).fetchone())

    def _assert_deal_available_for_claim(
        self, connection: sqlite3.Connection, deal_id: str, operation_key: str,
        *, confirmed_recovery: bool = False,
    ) -> None:
        # This runs inside the same BEGIN IMMEDIATE transaction as reservation.
        # A process-local lock cannot protect different service instances.
        if not confirmed_recovery and self._deal_was_claimed(connection, deal_id, operation_key):
            raise DealAlreadyClaimedError("deal was already allocated successfully")
        rows = connection.execute(
            "SELECT * FROM claim_operations WHERE deal_id=? AND operation_key != ? "
            "AND status IN ('pending','failed')",
            (deal_id, operation_key),
        ).fetchall()
        if self._filter_unresolved_claim_rows(rows, limit=1):
            raise IdempotencyConflictError("another deal lifecycle requires reconciliation")

    def get_claim_by_operation_key(self, operation_key: Any) -> Optional[Dict[str, Any]]:
        self._ensure_ready()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM claim_events WHERE operation_key=?", (str(operation_key),)
            ).fetchone()
        return self._claim_row(row) if row else None

    def begin_claim_operation(
        self,
        deal_id: Any,
        manager_id: Any,
        *,
        operation_key: Optional[str] = None,
        request: Optional[Mapping[str, Any]] = None,
        retry_failed: bool = False,
        require_extra_grant: bool = False,
        business_date: Optional[str] = None,
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
        if require_extra_grant:
            business_date = self._normalize_date_filter(business_date, "business_date")
            if not business_date:
                raise ValueError("business_date is required for an extra-claim grant")
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
                    original_request = json.loads(row["request_json"] or "{}")
                    confirmed_recovery = bool(
                        request
                        and (request.get("recovery") or request.get("maintenanceRecovery"))
                        and original_request.get("claimMarker")
                        and request.get("claimMarker") == original_request.get("claimMarker")
                    )
                    self._assert_deal_available_for_claim(
                        connection, deal_id, operation_key, confirmed_recovery=confirmed_recovery,
                    )
                    extra_grant = None
                    if require_extra_grant:
                        extra_grant = self._reserve_extra_claim_grant_in_connection(
                            connection, manager_id, business_date, operation_key, now
                        )
                    if request is not None:
                        prepared_request = dict(request)
                        # Public operation DTOs omit the private greeting target.
                        # Exact audit recovery keeps that existing frozen target;
                        # it must neither lose an accepted greeting nor create one
                        # for an old claim that never requested a greeting.
                        if (
                            (prepared_request.get("recovery") or prepared_request.get("maintenanceRecovery"))
                            and prepared_request.get("claimMarker") == original_request.get("claimMarker")
                            and prepared_request.get("greetingRequested") is True
                            and original_request.get("greetingRequested") is True
                        ):
                            prepared_request["greetingContext"] = original_request.get("greetingContext")
                        request_json = _json_dumps(self._prepare_operation_request(prepared_request))
                    else:
                        request_json = row["request_json"]
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
                            extra_claim_grant_id=?,
                            updated_at=?, finalized_at=NULL
                        WHERE operation_key = ? AND status = 'failed'
                        """,
                        (
                            request_json,
                            _json_dumps(attempt_history),
                            extra_grant["grant_id"] if extra_grant else row["extra_claim_grant_id"],
                            now,
                            operation_key,
                        ),
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
            self._assert_deal_available_for_claim(connection, deal_id, operation_key)
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
            if require_extra_grant:
                extra_grant = self._reserve_extra_claim_grant_in_connection(
                    connection, manager_id, business_date, operation_key, now
                )
                connection.execute(
                    "UPDATE claim_operations SET extra_claim_grant_id=? WHERE operation_key=?",
                    (extra_grant["grant_id"], operation_key),
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
        require_extra_grant: bool = False,
        business_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Explicit convenience wrapper for a safe failed-operation retry."""

        return self.begin_claim_operation(
            deal_id,
            manager_id,
            operation_key=operation_key,
            request=request,
            retry_failed=True,
            require_extra_grant=require_extra_grant,
            business_date=business_date,
        )

    def reassign_failed_claim_operation(
        self,
        deal_id: Any,
        manager_id: Any,
        *,
        operation_key: Optional[str] = None,
        request: Optional[Mapping[str, Any]] = None,
        require_extra_grant: bool = False,
        business_date: Optional[str] = None,
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
        if require_extra_grant:
            business_date = self._normalize_date_filter(business_date, "business_date")
            if not business_date:
                raise ValueError("business_date is required for an extra-claim grant")
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
            self._assert_deal_available_for_claim(connection, deal_id, operation_key)
            if row["extra_claim_grant_id"]:
                held_grant = connection.execute(
                    "SELECT * FROM extra_claim_grants WHERE grant_id=?",
                    (row["extra_claim_grant_id"],),
                ).fetchone()
                if (
                    held_grant
                    and held_grant["status"] == "reserved"
                    and held_grant["reserved_operation_key"] == operation_key
                ):
                    # A failed operation retains its grant only when the remote
                    # CRM write may already have happened.  Moving that same
                    # operation key to another manager would detach the audit
                    # evidence and either leak or double-spend the grant.
                    raise ExtraClaimGrantReconciliationRequiredError(
                        "uncertainty-held extra-claim grant requires reconciliation"
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
            extra_grant = None
            if require_extra_grant:
                extra_grant = self._reserve_extra_claim_grant_in_connection(
                    connection, manager_id, business_date, operation_key, now
                )
            connection.execute(
                """
                UPDATE claim_operations SET
                    manager_id=?, status='pending', request_json=?, result_json=NULL,
                    error=NULL, attempt_history_json=?, claim_event_id=NULL,
                    extra_claim_grant_id=?,
                    updated_at=?, finalized_at=NULL
                WHERE operation_key = ? AND status = 'failed'
                """,
                (
                    manager_id,
                    _json_dumps(self._prepare_operation_request(request))
                    if request is not None
                    else None,
                    _json_dumps(attempt_history),
                    extra_grant["grant_id"] if extra_grant else None,
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
        """Recognize a resolved historical marker for recovery checks.

        Recognition never authorizes a new automatic allocation. Lifetime claim
        exclusion applies separately; unknown/non-terminal markers remain intact
        for investigation.
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
        app_version: str = "unknown",
        recovered: bool = False,
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
            self._assert_deal_available_for_claim(
                connection, row["deal_id"], operation_key,
                confirmed_recovery=bool(recovered and expected_claim_marker),
            )
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
            extra_request_id = None
            grant = None
            if row["extra_claim_grant_id"]:
                grant = connection.execute(
                    "SELECT * FROM extra_claim_grants WHERE grant_id=?",
                    (row["extra_claim_grant_id"],),
                ).fetchone()
                if (
                    not grant
                    or grant["status"] != "reserved"
                    or grant["reserved_operation_key"] != operation_key
                ):
                    raise StateStoreError(
                        f"extra-claim grant for operation {operation_key!r} is not reserved"
                    )
                extra_request_id = str(grant["request_id"])
                claim_payload["extraClaimRequestId"] = extra_request_id
            claim_event_id = self._insert_claim(
                connection,
                claim_payload,
                source="app",
                operation_key=operation_key,
                extra_claim_request_id=extra_request_id,
                app_version=app_version,
                recovered=recovered,
            )
            now = self._now_iso()
            if grant:
                connection.execute(
                    """
                    UPDATE extra_claim_grants SET status='consumed', consumed_at=?,
                        updated_at=? WHERE grant_id=? AND status='reserved'
                          AND reserved_operation_key=?
                    """,
                    (now, now, grant["grant_id"], operation_key),
                )
                if grant["request_key"]:
                    connection.execute(
                        "UPDATE extra_claim_requests SET status='consumed', updated_at=? "
                        "WHERE request_key=?",
                        (now, grant["request_key"]),
                    )
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
            result_payload = dict(result) if result is not None else {}
            uncertainty_holds_grant = bool(
                result_payload.get("remoteUpdated") is True
                or result_payload.get("remoteUpdateUncertain")
                or (
                    result_payload.get("recoveryRequired")
                    and result_payload.get("remoteUpdated") is not False
                )
            )
            extra_claim_grant_id = row["extra_claim_grant_id"]
            if extra_claim_grant_id and not uncertainty_holds_grant:
                grant = connection.execute(
                    "SELECT * FROM extra_claim_grants WHERE grant_id=?",
                    (extra_claim_grant_id,),
                ).fetchone()
                connection.execute(
                    """
                    UPDATE extra_claim_grants SET status='approved',
                        reserved_operation_key=NULL, updated_at=?
                    WHERE grant_id=? AND status='reserved'
                      AND reserved_operation_key=?
                    """,
                    (now, extra_claim_grant_id, operation_key),
                )
                if grant and grant["request_key"]:
                    connection.execute(
                        "UPDATE extra_claim_requests SET status='approved', updated_at=? "
                        "WHERE request_key=? AND status NOT IN ('consumed','expired')",
                        (now, grant["request_key"]),
                    )
            connection.execute(
                """
                UPDATE claim_operations SET
                    status='failed', result_json=?, error=?, extra_claim_grant_id=?,
                    updated_at=?, finalized_at=?
                WHERE operation_key = ?
                """,
                (
                    _json_dumps(result_payload) if result is not None else None,
                    str(error),
                    extra_claim_grant_id if uncertainty_holds_grant else None,
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

    # ------------------------------------------------------------------
    # Lost-deal OpenLine auto-close
    # ------------------------------------------------------------------

    @staticmethod
    def _lost_deal_close_row(row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "transitionId": str(row["transition_id"]),
            "dealId": str(row["deal_id"]),
            "fromSemantic": str(row["from_semantic"]),
            "toSemantic": str(row["to_semantic"]),
            "fromCategoryId": str(row["from_category_id"]),
            "toCategoryId": str(row["to_category_id"]),
            "fromStageId": str(row["from_stage_id"]),
            "toStageId": str(row["to_stage_id"]),
            "transitionTime": str(row["transition_time"]),
            "status": str(row["status"]),
            "outcomeCode": str(row["outcome_code"] or ""),
            "chatId": str(row["chat_id"] or ""),
            "sessionId": str(row["session_id"] or ""),
            "lastMessageId": str(row["last_message_id"] or ""),
            "historyMessageCount": int(row["history_message_count"] or 0),
            "historySignature": str(row["history_signature"] or ""),
            "activityId": str(row["activity_id"] or ""),
            "chatLookupMode": str(row["chat_lookup_mode"] or ""),
            "activityUpdatedAt": str(row["activity_updated_at"] or ""),
            "attemptCount": int(row["attempt_count"]),
            "leaseToken": str(row["lease_token"] or ""),
            "leaseExpiresAt": row["lease_expires_at"],
            "createdAt": str(row["created_at"]),
            "updatedAt": str(row["updated_at"]),
            "dispatchingAt": row["dispatching_at"],
            "finalizedAt": row["finalized_at"],
        }

    def get_lost_deal_autoclose_watermark(self) -> datetime:
        """Return the immutable first-install boundary for close candidates."""

        self._ensure_ready()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT value FROM meta WHERE key = ?",
                (LOST_DEAL_AUTOCLOSE_WATERMARK,),
            ).fetchone()
        if not row:
            raise StateStoreError("lost-deal auto-close watermark is missing")
        try:
            value = datetime.fromisoformat(str(row["value"]))
        except (TypeError, ValueError) as exc:
            raise StateStoreError("lost-deal auto-close watermark is invalid") from exc
        if value.tzinfo is None:
            raise StateStoreError("lost-deal auto-close watermark has no timezone")
        return value

    def arm_lost_deal_autoclose(
        self,
        remote_time: Any,
        baseline_history_id: Any,
    ) -> Dict[str, Any]:
        """Create the remote-bound first-run boundary exactly once."""

        self._ensure_ready()
        remote_time, _ = self._normalize_timestamp(remote_time)
        try:
            baseline_history_id = int(str(baseline_history_id or "0"))
        except (TypeError, ValueError) as exc:
            raise ValueError("baseline_history_id must be a non-negative integer") from exc
        if baseline_history_id < 0:
            raise ValueError("baseline_history_id must be a non-negative integer")
        install_time = self.get_lost_deal_autoclose_watermark()
        parsed_remote = datetime.fromisoformat(remote_time)
        boundary_time = max(parsed_remote, install_time).isoformat(timespec="microseconds")
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT value FROM meta WHERE key = ?",
                (LOST_DEAL_AUTOCLOSE_ARMED,),
            ).fetchone()
            armed_now = row is None
            if row is None:
                value = {
                    "armedAt": boundary_time,
                    "baselineHistoryId": baseline_history_id,
                    "scanAfterHistoryId": baseline_history_id,
                }
                connection.execute(
                    "INSERT INTO meta(key, value, updated_at) VALUES(?, ?, ?)",
                    (LOST_DEAL_AUTOCLOSE_ARMED, _json_dumps(value), now),
                )
            else:
                try:
                    value = json.loads(row["value"])
                except (TypeError, json.JSONDecodeError) as exc:
                    raise StateStoreError("lost-deal armed boundary is invalid") from exc
        if not isinstance(value, dict):
            raise StateStoreError("lost-deal armed boundary is invalid")
        return {**value, "armedNow": armed_now}

    def get_lost_deal_autoclose_boundary(self) -> Optional[Dict[str, Any]]:
        self._ensure_ready()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT value FROM meta WHERE key = ?",
                (LOST_DEAL_AUTOCLOSE_ARMED,),
            ).fetchone()
        if not row:
            return None
        try:
            value = json.loads(row["value"])
            armed_at = datetime.fromisoformat(str(value["armedAt"]))
            baseline_id = int(value["baselineHistoryId"])
            scan_after_id = int(value.get("scanAfterHistoryId", baseline_id))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise StateStoreError("lost-deal armed boundary is invalid") from exc
        if (
            armed_at.tzinfo is None
            or baseline_id < 0
            or scan_after_id < baseline_id
        ):
            raise StateStoreError("lost-deal armed boundary is invalid")
        return {
            "armedAt": armed_at,
            "baselineHistoryId": baseline_id,
            "scanAfterHistoryId": scan_after_id,
        }

    def advance_lost_deal_autoclose_history_id(self, value: Any) -> Dict[str, Any]:
        self._ensure_ready()
        try:
            requested = int(str(value))
        except (TypeError, ValueError) as exc:
            raise ValueError("history cursor must be a non-negative integer") from exc
        if requested < 0:
            raise ValueError("history cursor must be a non-negative integer")
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT value FROM meta WHERE key=?",
                (LOST_DEAL_AUTOCLOSE_ARMED,),
            ).fetchone()
            if not row:
                raise StateStoreError("lost-deal auto-close is not armed")
            try:
                boundary = json.loads(row["value"])
                baseline = int(boundary["baselineHistoryId"])
                current = int(boundary.get("scanAfterHistoryId", baseline))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise StateStoreError("lost-deal armed boundary is invalid") from exc
            boundary["scanAfterHistoryId"] = max(baseline, current, requested)
            connection.execute(
                "UPDATE meta SET value=?, updated_at=? WHERE key=?",
                (_json_dumps(boundary), now, LOST_DEAL_AUTOCLOSE_ARMED),
            )
        return boundary

    def _normalize_lost_deal_transition(
        self,
        transition: Mapping[str, Any],
    ) -> Dict[str, str]:
        if not isinstance(transition, Mapping):
            raise ValueError("transition must be an object")
        transition_id = str(transition.get("transitionId") or "").strip()
        deal_id = str(transition.get("dealId") or "").strip()
        from_semantic = str(transition.get("fromSemantic") or "").strip().upper()
        to_semantic = str(transition.get("toSemantic") or "").strip().upper()
        from_category_id = str(transition.get("fromCategoryId") or "").strip()
        to_category_id = str(transition.get("toCategoryId") or "").strip()
        from_stage_id = str(transition.get("fromStageId") or "").strip()
        to_stage_id = str(transition.get("toStageId") or "").strip()
        if not re.fullmatch(r"[1-9]\d{0,19}", transition_id):
            raise ValueError("transitionId must be a positive integer")
        if not re.fullmatch(r"[1-9]\d{0,19}", deal_id):
            raise ValueError("dealId must be a positive integer")
        if from_semantic not in {"P", "S"} or to_semantic != "F":
            raise ValueError("transition must be exactly non-F to F")
        if not re.fullmatch(r"\d{1,19}", from_category_id):
            raise ValueError("fromCategoryId must be a non-negative integer")
        if not re.fullmatch(r"\d{1,19}", to_category_id):
            raise ValueError("toCategoryId must be a non-negative integer")
        if not from_stage_id or len(from_stage_id) > 128:
            raise ValueError("fromStageId is invalid")
        if not to_stage_id or len(to_stage_id) > 128:
            raise ValueError("toStageId is invalid")
        transition_time, _ = self._normalize_timestamp(
            transition.get("transitionTime"),
        )
        return {
            "transitionId": transition_id,
            "dealId": deal_id,
            "fromSemantic": from_semantic,
            "toSemantic": to_semantic,
            "fromCategoryId": from_category_id,
            "toCategoryId": to_category_id,
            "fromStageId": from_stage_id,
            "toStageId": to_stage_id,
            "transitionTime": transition_time,
        }

    @staticmethod
    def _normalize_lost_deal_outcome_code(value: Any) -> str:
        value = str(value or "").strip().lower()
        if value and not re.fullmatch(r"[a-z0-9_]{1,80}", value):
            raise ValueError("outcome code is invalid")
        return value

    def claim_lost_deal_close_transition(
        self,
        transition: Mapping[str, Any],
        *,
        lease_seconds: int = 120,
    ) -> Dict[str, Any]:
        """Claim safe pre-dispatch checks for one proven stage transition.

        ``dispatching`` is deliberately terminal for this method.  Once the
        irreversible REST call may have started, no worker can acquire the
        transition again, even after a process crash or lease timeout.
        """

        self._ensure_ready()
        normalized = self._normalize_lost_deal_transition(transition)
        lease_seconds = max(30, min(900, int(lease_seconds)))
        now_dt = datetime.now(self.local_timezone)
        now = now_dt.isoformat(timespec="microseconds")
        lease_expires_at = (now_dt + timedelta(seconds=lease_seconds)).isoformat(
            timespec="microseconds"
        )
        lease_token = secrets.token_urlsafe(24)
        with self._transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT * FROM lost_deal_close_operations WHERE transition_id = ?",
                (normalized["transitionId"],),
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO lost_deal_close_operations(
                        transition_id, deal_id, from_semantic, to_semantic,
                        from_category_id, to_category_id,
                        from_stage_id, to_stage_id, transition_time,
                        status, attempt_count, lease_token, lease_expires_at,
                        created_at, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'checking', 1, ?, ?, ?, ?)
                    """,
                    (
                        normalized["transitionId"],
                        normalized["dealId"],
                        normalized["fromSemantic"],
                        normalized["toSemantic"],
                        normalized["fromCategoryId"],
                        normalized["toCategoryId"],
                        normalized["fromStageId"],
                        normalized["toStageId"],
                        normalized["transitionTime"],
                        lease_token,
                        lease_expires_at,
                        now,
                        now,
                    ),
                )
                claimed = True
                reason = "new_transition"
            else:
                existing = self._lost_deal_close_row(row)
                immutable = (
                    existing["dealId"],
                    existing["fromSemantic"],
                    existing["toSemantic"],
                    existing["fromCategoryId"],
                    existing["toCategoryId"],
                    existing["fromStageId"],
                    existing["toStageId"],
                    existing["transitionTime"],
                )
                requested = (
                    normalized["dealId"],
                    normalized["fromSemantic"],
                    normalized["toSemantic"],
                    normalized["fromCategoryId"],
                    normalized["toCategoryId"],
                    normalized["fromStageId"],
                    normalized["toStageId"],
                    normalized["transitionTime"],
                )
                if immutable != requested:
                    raise IdempotencyConflictError(
                        "stage-history transition identity was reused with different data"
                    )
                status = existing["status"]
                expired = True
                if existing.get("leaseExpiresAt"):
                    try:
                        expiry = datetime.fromisoformat(str(existing["leaseExpiresAt"]))
                        expired = expiry <= now_dt
                    except (TypeError, ValueError):
                        expired = True
                if status == "retryable" or (status == "checking" and expired):
                    connection.execute(
                        """
                        UPDATE lost_deal_close_operations
                        SET status='checking', outcome_code='',
                            attempt_count=attempt_count + 1,
                            lease_token=?, lease_expires_at=?, updated_at=?
                        WHERE transition_id=?
                        """,
                        (
                            lease_token,
                            lease_expires_at,
                            now,
                            normalized["transitionId"],
                        ),
                    )
                    claimed = True
                    reason = "retry_pre_dispatch"
                else:
                    claimed = False
                    reason = "checking_in_progress" if status == "checking" else "already_final"
            result_row = connection.execute(
                "SELECT * FROM lost_deal_close_operations WHERE transition_id = ?",
                (normalized["transitionId"],),
            ).fetchone()
        result = self._lost_deal_close_row(result_row)
        result["claimed"] = claimed
        result["claimReason"] = reason
        # Do not expose an old/stale lease token to callers that did not claim.
        if not claimed:
            result["leaseToken"] = ""
        return result

    def get_lost_deal_close_operation(self, transition_id: Any) -> Optional[Dict[str, Any]]:
        self._ensure_ready()
        transition_id = str(transition_id or "").strip()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM lost_deal_close_operations WHERE transition_id = ?",
                (transition_id,),
            ).fetchone()
        return self._lost_deal_close_row(row) if row else None

    def _update_lost_deal_check_status(
        self,
        transition_id: Any,
        lease_token: Any,
        *,
        status: str,
        outcome_code: Any,
        chat_id: Any = "",
        finalized: bool,
    ) -> Dict[str, Any]:
        self._ensure_ready()
        transition_id = str(transition_id or "").strip()
        lease_token = str(lease_token or "").strip()
        chat_id = str(chat_id or "").strip()
        if not transition_id or not lease_token:
            raise ValueError("transition_id and lease_token are required")
        if chat_id and not re.fullmatch(r"[1-9]\d{0,19}", chat_id):
            raise ValueError("chat_id must be a positive integer")
        outcome_code = self._normalize_lost_deal_outcome_code(outcome_code)
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            cursor = connection.execute(
                """
                UPDATE lost_deal_close_operations
                SET status=?, outcome_code=?, chat_id=?, lease_token='',
                    lease_expires_at=NULL, updated_at=?, finalized_at=?
                WHERE transition_id=? AND status='checking' AND lease_token=?
                """,
                (
                    status,
                    outcome_code,
                    chat_id,
                    now,
                    now if finalized else None,
                    transition_id,
                    lease_token,
                ),
            )
            if cursor.rowcount != 1:
                raise StateStoreError("lost-deal close check lease is no longer owned")
            row = connection.execute(
                "SELECT * FROM lost_deal_close_operations WHERE transition_id = ?",
                (transition_id,),
            ).fetchone()
        return self._lost_deal_close_row(row)

    def mark_lost_deal_close_retryable(
        self,
        transition_id: Any,
        lease_token: Any,
        outcome_code: Any,
    ) -> Dict[str, Any]:
        return self._update_lost_deal_check_status(
            transition_id,
            lease_token,
            status="retryable",
            outcome_code=outcome_code,
            finalized=False,
        )

    def finalize_lost_deal_close_check(
        self,
        transition_id: Any,
        lease_token: Any,
        *,
        status: str,
        outcome_code: Any,
        chat_id: Any = "",
    ) -> Dict[str, Any]:
        if status not in {"skipped", "dry_run"}:
            raise ValueError("pre-dispatch final status must be skipped or dry_run")
        return self._update_lost_deal_check_status(
            transition_id,
            lease_token,
            status=status,
            outcome_code=outcome_code,
            chat_id=chat_id,
            finalized=True,
        )

    def mark_lost_deal_close_dispatching(
        self,
        transition_id: Any,
        lease_token: Any,
        chat_id: Any,
        session_id: Any,
        last_message_id: Any,
        history_message_count: int,
        history_signature: Any,
        activity_id: Any,
        chat_lookup_mode: Any,
        activity_updated_at: Any,
    ) -> Dict[str, Any]:
        """Commit the no-retry boundary before the remote finish call."""

        self._ensure_ready()
        transition_id = str(transition_id or "").strip()
        lease_token = str(lease_token or "").strip()
        chat_id = str(chat_id or "").strip()
        session_id = str(session_id or "").strip()
        last_message_id = str(last_message_id or "").strip()
        activity_id = str(activity_id or "").strip()
        history_signature = str(history_signature or "").strip().lower()
        chat_lookup_mode = str(chat_lookup_mode or "")
        activity_updated_at = str(activity_updated_at or "").strip()
        if not re.fullmatch(r"[1-9]\d{0,19}", chat_id):
            raise ValueError("chat_id must be a positive integer")
        if not re.fullmatch(r"[1-9]\d{0,19}", session_id):
            raise ValueError("session_id must be a positive integer")
        if last_message_id and not re.fullmatch(r"[1-9]\d{0,19}", last_message_id):
            raise ValueError("last_message_id must be a positive integer or empty")
        history_message_count = int(history_message_count)
        if history_message_count < 0 or history_message_count > 1_000_000:
            raise ValueError("history_message_count is invalid")
        if not re.fullmatch(r"[1-9]\d{0,19}", activity_id):
            raise ValueError("activity_id must be a positive integer")
        if not re.fullmatch(r"[0-9a-f]{64}", history_signature):
            raise ValueError("history_signature must be a SHA-256 digest")
        if chat_lookup_mode not in {"active_registry", "activity_fallback"}:
            raise ValueError(
                "chat_lookup_mode must be active_registry or activity_fallback"
            )
        if chat_lookup_mode == "active_registry":
            if activity_updated_at:
                raise ValueError(
                    "activity_updated_at must be empty for active_registry"
                )
        else:
            if not activity_updated_at:
                raise ValueError(
                    "activity_updated_at is required for activity_fallback"
                )
            try:
                parsed_activity_updated_at = datetime.fromisoformat(
                    activity_updated_at[:-1] + "+00:00"
                    if activity_updated_at.endswith("Z")
                    else activity_updated_at
                )
            except ValueError as exc:
                raise ValueError(
                    "activity_updated_at must be an ISO-8601 datetime"
                ) from exc
            if (
                parsed_activity_updated_at.tzinfo is None
                or parsed_activity_updated_at.utcoffset() is None
            ):
                raise ValueError("activity_updated_at must include a timezone")
            activity_updated_at = parsed_activity_updated_at.isoformat(
                timespec="microseconds"
            )
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            cursor = connection.execute(
                """
                UPDATE lost_deal_close_operations
                SET status='dispatching', outcome_code='finish_started',
                    chat_id=?, session_id=?, last_message_id=?,
                    history_message_count=?, history_signature=?, activity_id=?,
                    chat_lookup_mode=?, activity_updated_at=?,
                    lease_token='', lease_expires_at=NULL,
                    updated_at=?, dispatching_at=?
                WHERE transition_id=? AND status='checking' AND lease_token=?
                """,
                (
                    chat_id,
                    session_id,
                    last_message_id,
                    history_message_count,
                    history_signature,
                    activity_id,
                    chat_lookup_mode,
                    activity_updated_at,
                    now,
                    now,
                    transition_id,
                    lease_token,
                ),
            )
            if cursor.rowcount != 1:
                raise StateStoreError("lost-deal close dispatch boundary was not acquired")
            row = connection.execute(
                "SELECT * FROM lost_deal_close_operations WHERE transition_id = ?",
                (transition_id,),
            ).fetchone()
        return self._lost_deal_close_row(row)

    def _finalize_lost_deal_dispatch(
        self,
        transition_id: Any,
        *,
        status: str,
        outcome_code: Any,
    ) -> Dict[str, Any]:
        self._ensure_ready()
        transition_id = str(transition_id or "").strip()
        outcome_code = self._normalize_lost_deal_outcome_code(outcome_code)
        if status not in {"closed", "uncertain"}:
            raise ValueError("dispatch final status must be closed or uncertain")
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            cursor = connection.execute(
                """
                UPDATE lost_deal_close_operations
                SET status=?, outcome_code=?, updated_at=?, finalized_at=?
                WHERE transition_id=? AND status='dispatching'
                """,
                (status, outcome_code, now, now, transition_id),
            )
            if cursor.rowcount != 1:
                raise StateStoreError("lost-deal close dispatch is not pending")
            row = connection.execute(
                "SELECT * FROM lost_deal_close_operations WHERE transition_id = ?",
                (transition_id,),
            ).fetchone()
        return self._lost_deal_close_row(row)

    def mark_lost_deal_close_closed(self, transition_id: Any) -> Dict[str, Any]:
        return self._finalize_lost_deal_dispatch(
            transition_id,
            status="closed",
            outcome_code="finished",
        )

    def mark_lost_deal_close_uncertain(
        self,
        transition_id: Any,
        outcome_code: Any = "finish_result_uncertain",
    ) -> Dict[str, Any]:
        return self._finalize_lost_deal_dispatch(
            transition_id,
            status="uncertain",
            outcome_code=outcome_code,
        )

    def list_lost_deal_close_reconciliation(self, limit: int = 100) -> list[Dict[str, Any]]:
        self._ensure_ready()
        limit = max(1, min(1000, int(limit)))
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM lost_deal_close_operations
                WHERE status IN ('dispatching', 'uncertain')
                ORDER BY updated_at, transition_id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._lost_deal_close_row(row) for row in rows]

    def mark_lost_deal_close_reconciled(self, transition_id: Any) -> Dict[str, Any]:
        self._ensure_ready()
        transition_id = str(transition_id or "").strip()
        now = self._now_iso()
        with self._transaction(immediate=True) as connection:
            cursor = connection.execute(
                """
                UPDATE lost_deal_close_operations
                SET status='closed', outcome_code='finished_reconciled',
                    updated_at=?, finalized_at=?
                WHERE transition_id=? AND status IN ('dispatching', 'uncertain')
                """,
                (now, now, transition_id),
            )
            if cursor.rowcount != 1:
                raise StateStoreError("lost-deal close is not awaiting reconciliation")
            row = connection.execute(
                "SELECT * FROM lost_deal_close_operations WHERE transition_id=?",
                (transition_id,),
            ).fetchone()
        return self._lost_deal_close_row(row)


def create_state_store() -> StateStore:
    """Create a store from ``APP_DATA_DIR`` and ``STATE_DB_FILENAME``."""

    return StateStore()


__all__ = [
    "DEFAULT_DB_FILENAME",
    "ExtraClaimGrantReconciliationRequiredError",
    "ExtraClaimGrantUnavailableError",
    "ExtraClaimRequestAssociationConflictError",
    "IdempotencyConflictError",
    "LegacyMigrationError",
    "LOST_DEAL_AUTOCLOSE_WATERMARK",
    "MIGRATION_MARKER",
    "SCHEMA_VERSION",
    "StateStore",
    "StateStoreError",
    "StateStoreNotReadyError",
    "create_state_store",
]
