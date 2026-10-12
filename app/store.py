import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable, Optional

from .db import connect, immediate_transaction
from .events import Event, EventType


class LeaseUnavailable(RuntimeError):
    """Another worker holds an unexpired lease on the workflow."""


class LeaseLost(RuntimeError):
    """The caller's lease expired or was taken over by another worker."""


class VersionConflict(RuntimeError):
    """The workflow history changed after the caller read it."""


@dataclass(frozen=True)
class Lease:
    workflow_id: str
    owner: str
    token: int
    expires_at: float


SCHEMA = """
CREATE TABLE IF NOT EXISTS workflow_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    action_id TEXT NOT NULL,
    action TEXT NOT NULL,
    event_type TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    lease_token INTEGER NOT NULL,
    operation_id TEXT,
    payload_fingerprint TEXT,
    detail TEXT,
    UNIQUE (workflow_id, version)
);

-- One request per attempt, and at most one terminal outcome per attempt.
-- ACTION_UNKNOWN may repeat: each inconclusive reconciliation is recorded.
CREATE UNIQUE INDEX IF NOT EXISTS one_request_per_attempt
    ON workflow_events (workflow_id, action_id, attempt)
    WHERE event_type = 'action_requested';
CREATE UNIQUE INDEX IF NOT EXISTS one_outcome_per_attempt
    ON workflow_events (workflow_id, action_id, attempt)
    WHERE event_type IN ('action_succeeded', 'action_failed', 'action_conflict');
CREATE UNIQUE INDEX IF NOT EXISTS one_completion
    ON workflow_events (workflow_id)
    WHERE event_type = 'workflow_completed';

CREATE TRIGGER IF NOT EXISTS workflow_events_no_update
BEFORE UPDATE ON workflow_events
BEGIN SELECT RAISE(ABORT, 'workflow events are append-only'); END;

CREATE TRIGGER IF NOT EXISTS workflow_events_no_delete
BEFORE DELETE ON workflow_events
BEGIN SELECT RAISE(ABORT, 'workflow events are append-only'); END;

-- Tokens only increase. Releasing a lease expires it instead of deleting the
-- row, so a token is never handed out twice for the same workflow.
CREATE TABLE IF NOT EXISTS workflow_leases (
    workflow_id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    token INTEGER NOT NULL,
    expires_at REAL NOT NULL
);
"""

EVENT_COLUMNS = (
    "sequence, workflow_id, version, action_id, action, event_type, attempt, "
    "worker_id, lease_token, operation_id, payload_fingerprint, detail"
)


class EventStore:
    """Durable event history plus per-workflow leases."""

    def __init__(self, path: str | Path, clock: Callable[[], float] = time.time):
        self.path = str(path)
        self.clock = clock
        self.connection = connect(self.path, SCHEMA)

    def acquire_lease(self, workflow_id: str, owner: str, ttl: float) -> Lease:
        now = self.clock()
        with immediate_transaction(self.connection):
            row = self.connection.execute(
                "SELECT owner, token, expires_at FROM workflow_leases WHERE workflow_id = ?",
                (workflow_id,),
            ).fetchone()
            if row is not None and row[2] > now:
                raise LeaseUnavailable(f"{workflow_id} is leased by {row[0]}")
            lease = Lease(workflow_id, owner, (row[1] if row else 0) + 1, now + ttl)
            self.connection.execute(
                """
                INSERT INTO workflow_leases (workflow_id, owner, token, expires_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (workflow_id) DO UPDATE SET
                    owner = excluded.owner,
                    token = excluded.token,
                    expires_at = excluded.expires_at
                """,
                (lease.workflow_id, lease.owner, lease.token, lease.expires_at),
            )
        return lease

    def release_lease(self, lease: Lease) -> None:
        with immediate_transaction(self.connection):
            self.connection.execute(
                """
                UPDATE workflow_leases SET expires_at = ?
                WHERE workflow_id = ? AND owner = ? AND token = ?
                """,
                (self.clock(), lease.workflow_id, lease.owner, lease.token),
            )

    def _check_lease(self, lease: Lease) -> None:
        row = self.connection.execute(
            "SELECT owner, token, expires_at FROM workflow_leases WHERE workflow_id = ?",
            (lease.workflow_id,),
        ).fetchone()
        if row is None or (row[0], row[1]) != (lease.owner, lease.token):
            raise LeaseLost(f"lease token {lease.token} for {lease.workflow_id} was superseded")
        if row[2] <= self.clock():
            raise LeaseLost(f"lease token {lease.token} for {lease.workflow_id} expired")

    def append(
        self,
        lease: Lease,
        expected_version: int,
        *,
        action_id: str,
        action: str,
        event_type: EventType,
        attempt: int,
        operation_id: Optional[str] = None,
        payload_fingerprint: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> Event:
        """Append one event if the caller still owns the lease and saw the latest version."""
        with immediate_transaction(self.connection):
            self._check_lease(lease)
            current = self.connection.execute(
                "SELECT COALESCE(MAX(version), 0) FROM workflow_events WHERE workflow_id = ?",
                (lease.workflow_id,),
            ).fetchone()[0]
            if current != expected_version:
                raise VersionConflict(
                    f"{lease.workflow_id} is at version {current}, caller read {expected_version}"
                )
            event = Event(
                sequence=0,
                workflow_id=lease.workflow_id,
                version=current + 1,
                action_id=action_id,
                action=action,
                event_type=event_type,
                attempt=attempt,
                worker_id=lease.owner,
                lease_token=lease.token,
                operation_id=operation_id,
                payload_fingerprint=payload_fingerprint,
                detail=detail,
            )
            cursor = self.connection.execute(
                f"INSERT INTO workflow_events ({EVENT_COLUMNS}) "
                "VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    event.workflow_id, event.version, event.action_id, event.action,
                    event.event_type.value, event.attempt, event.worker_id,
                    event.lease_token, event.operation_id, event.payload_fingerprint,
                    event.detail,
                ),
            )
        return replace(event, sequence=cursor.lastrowid)

    def events(self, workflow_id: str) -> list[Event]:
        rows = self.connection.execute(
            f"SELECT {EVENT_COLUMNS} FROM workflow_events WHERE workflow_id = ? ORDER BY version",
            (workflow_id,),
        ).fetchall()
        return [
            Event(
                sequence=row[0], workflow_id=row[1], version=row[2], action_id=row[3],
                action=row[4], event_type=EventType(row[5]), attempt=row[6],
                worker_id=row[7], lease_token=row[8], operation_id=row[9],
                payload_fingerprint=row[10], detail=row[11],
            )
            for row in rows
        ]

    def close(self) -> None:
        self.connection.close()
