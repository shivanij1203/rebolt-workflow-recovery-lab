import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from .db import connect, immediate_transaction


class LookupStatus(str, Enum):
    COMMITTED = "committed"
    AUTHORITATIVELY_NOT_FOUND = "authoritatively_not_found"
    INCONCLUSIVE = "inconclusive"


class PayloadMismatch(RuntimeError):
    """An operation ID was reused with a different payload."""


class ConnectorUnavailable(RuntimeError):
    """The external system could not answer a status lookup."""


def fingerprint(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True)
class Receipt:
    operation_id: str
    payload_fingerprint: str
    result: str
    replayed: bool


@dataclass(frozen=True)
class Lookup:
    status: LookupStatus
    payload_fingerprint: Optional[str] = None


SCHEMA = """
CREATE TABLE IF NOT EXISTS external_operations (
    system TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    payload_fingerprint TEXT NOT NULL,
    result TEXT NOT NULL,
    PRIMARY KEY (system, operation_id)
);

-- The business effect itself, e.g. the ERP row that changed. Duplicate
-- effects would show up here even if the operation table looked fine.
CREATE TABLE IF NOT EXISTS business_effects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    system TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    payload TEXT NOT NULL
);
"""


class ExternalSystem:
    """Synthetic external system that deduplicates writes by operation ID.

    `authoritative_lookup` stands in for the connector contract. When it is
    True, a lookup that finds nothing proves the operation never committed.
    When it is False, a missing record is only a missing record: the write may
    be delayed, replicated later, or stored somewhere the lookup cannot see.
    """

    def __init__(
        self,
        path: str | Path,
        name: str,
        authoritative_lookup: bool = True,
        lookup_available: bool = True,
    ):
        self.path = str(path)
        self.name = name
        self.authoritative_lookup = authoritative_lookup
        self.lookup_available = lookup_available
        self.connection = connect(self.path, SCHEMA)

    def apply(self, operation_id: str, payload: dict[str, Any]) -> Receipt:
        payload_fingerprint = fingerprint(payload)
        with immediate_transaction(self.connection):
            row = self.connection.execute(
                """
                SELECT payload_fingerprint, result FROM external_operations
                WHERE system = ? AND operation_id = ?
                """,
                (self.name, operation_id),
            ).fetchone()
            if row is not None:
                if row[0] != payload_fingerprint:
                    raise PayloadMismatch(
                        f"{operation_id} was already committed with a different payload"
                    )
                return Receipt(operation_id, payload_fingerprint, row[1], replayed=True)

            cursor = self.connection.execute(
                "INSERT INTO business_effects (system, operation_id, payload) VALUES (?, ?, ?)",
                (self.name, operation_id, json.dumps(payload, sort_keys=True)),
            )
            result = f"{self.name}-effect-{cursor.lastrowid}"
            self.connection.execute(
                """
                INSERT INTO external_operations
                    (system, operation_id, payload_fingerprint, result)
                VALUES (?, ?, ?, ?)
                """,
                (self.name, operation_id, payload_fingerprint, result),
            )
        return Receipt(operation_id, payload_fingerprint, result, replayed=False)

    def lookup(self, operation_id: str) -> Lookup:
        if not self.lookup_available:
            raise ConnectorUnavailable(f"{self.name} status lookup timed out")
        row = self.connection.execute(
            """
            SELECT payload_fingerprint FROM external_operations
            WHERE system = ? AND operation_id = ?
            """,
            (self.name, operation_id),
        ).fetchone()
        if row is not None:
            return Lookup(LookupStatus.COMMITTED, row[0])
        if self.authoritative_lookup:
            return Lookup(LookupStatus.AUTHORITATIVELY_NOT_FOUND)
        return Lookup(LookupStatus.INCONCLUSIVE)

    def committed_operations(self) -> list[str]:
        rows = self.connection.execute(
            """
            SELECT operation_id FROM external_operations
            WHERE system = ? ORDER BY operation_id
            """,
            (self.name,),
        ).fetchall()
        return [row[0] for row in rows]

    def effect_count(self) -> int:
        return self.connection.execute(
            "SELECT COUNT(*) FROM business_effects WHERE system = ?", (self.name,)
        ).fetchone()[0]

    def close(self) -> None:
        self.connection.close()
