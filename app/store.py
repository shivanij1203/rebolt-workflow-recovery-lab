import sqlite3
from pathlib import Path

from .events import Event, EventType


class EventStore:
    """Small durable event store used by the lab."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        self.connection = sqlite3.connect(self.path)
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS workflow_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_id TEXT NOT NULL,
                action_id TEXT NOT NULL,
                action TEXT NOT NULL,
                event_type TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                detail TEXT
            )
            """
        )
        self.connection.commit()

    def append(
        self, workflow_id: str, action_id: str, action: str,
        event_type: EventType, attempt: int = 1, detail: str = ""
    ) -> Event:
        cursor = self.connection.execute(
            """
            INSERT INTO workflow_events
                (workflow_id, action_id, action, event_type, attempt, detail)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                workflow_id, action_id, action, event_type.value,
                attempt, detail or None,
            ),
        )
        self.connection.commit()
        return Event(
            sequence=cursor.lastrowid,
            workflow_id=workflow_id,
            action_id=action_id,
            action=action,
            event_type=event_type,
            attempt=attempt,
            detail=detail or None,
        )

    def events(self, workflow_id: str) -> list[Event]:
        rows = self.connection.execute(
            """
            SELECT sequence, workflow_id, action_id, action, event_type, attempt, detail
            FROM workflow_events
            WHERE workflow_id = ?
            ORDER BY sequence
            """,
            (workflow_id,),
        ).fetchall()
        return [
            Event(
                sequence=row[0], workflow_id=row[1], action_id=row[2],
                action=row[3], event_type=EventType(row[4]),
                attempt=row[5], detail=row[6],
            )
            for row in rows
        ]

    def close(self) -> None:
        self.connection.close()


class ExternalOperationStatus(str):
    COMMITTED = "committed"
    NOT_FOUND = "not_found"
    UNKNOWN = "unknown"


class ExternalSystem:
    """Synthetic external system with durable idempotent operation IDs."""

    def __init__(self, path: str | Path, name: str):
        self.path = str(path)
        self.name = name
        self.connection = sqlite3.connect(self.path)
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS external_operations (
                system TEXT NOT NULL,
                operation_id TEXT NOT NULL,
                status TEXT NOT NULL,
                PRIMARY KEY (system, operation_id)
            )
            """
        )
        self.connection.commit()

    def apply(self, operation_id: str) -> None:
        self.connection.execute(
            """
            INSERT INTO external_operations (system, operation_id, status)
            VALUES (?, ?, 'committed')
            ON CONFLICT(system, operation_id) DO NOTHING
            """,
            (self.name, operation_id),
        )
        self.connection.commit()

    def status(self, operation_id: str) -> str:
        row = self.connection.execute(
            """
            SELECT status
            FROM external_operations
            WHERE system = ? AND operation_id = ?
            """,
            (self.name, operation_id),
        ).fetchone()
        return row[0] if row else ExternalOperationStatus.NOT_FOUND

    def committed_operations(self) -> list[str]:
        rows = self.connection.execute(
            """
            SELECT operation_id
            FROM external_operations
            WHERE system = ? AND status = 'committed'
            ORDER BY operation_id
            """,
            (self.name,),
        ).fetchall()
        return [row[0] for row in rows]

    def close(self) -> None:
        self.connection.close()
