import copy
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional

from .external import (
    ConnectorUnavailable,
    ExternalSystem,
    LookupStatus,
    PayloadMismatch,
    fingerprint,
)


class Outcome(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    UNKNOWN = "unknown"
    CONFLICT = "conflict"


class CrashPoint(str, Enum):
    BEFORE_WRITE = "before_write"
    AFTER_COMMIT = "after_commit"
    AFTER_LOOKUP = "after_lookup"


class WorkerCrash(RuntimeError):
    """Simulated process death at a named point in an action."""


@dataclass(frozen=True)
class ActionResult:
    outcome: Outcome
    detail: str = ""


class ExternalAction:
    def __init__(
        self,
        name: str,
        system: ExternalSystem,
        payload: dict[str, Any],
        outcome_provider: Optional[Callable[[], Outcome]] = None,
        crash_at: Optional[CrashPoint] = None,
    ):
        self.name = name
        self.system = system
        self.payload = copy.deepcopy(payload)
        self.fingerprint = fingerprint(self.payload)
        self.outcome_provider = outcome_provider
        self.crash_at = crash_at

    def _crash_if(self, point: CrashPoint, operation_id: str) -> None:
        if self.crash_at == point:
            raise WorkerCrash(f"worker crashed {point.value} for {operation_id}")

    def execute(self, operation_id: str) -> ActionResult:
        self._crash_if(CrashPoint.BEFORE_WRITE, operation_id)

        outcome = self.outcome_provider() if self.outcome_provider else Outcome.SUCCESS
        if outcome == Outcome.FAILURE:
            return ActionResult(Outcome.FAILURE, "external system rejected the request")
        if outcome == Outcome.UNKNOWN:
            return ActionResult(Outcome.UNKNOWN, "request timed out without a response")

        try:
            receipt = self.system.apply(operation_id, self.payload)
        except PayloadMismatch as error:
            return ActionResult(Outcome.CONFLICT, str(error))

        self._crash_if(CrashPoint.AFTER_COMMIT, operation_id)
        verb = "replayed" if receipt.replayed else "committed"
        return ActionResult(Outcome.SUCCESS, f"{verb} {receipt.result}")

    def reconcile(self, operation_id: str, expected_fingerprint: str) -> ActionResult:
        """Ask the external system what happened. Only positive evidence counts."""
        try:
            lookup = self.system.lookup(operation_id)
        except ConnectorUnavailable as error:
            result = ActionResult(Outcome.UNKNOWN, f"lookup failed: {error}")
        else:
            result = _interpret(lookup.status, lookup.payload_fingerprint, expected_fingerprint)

        self._crash_if(CrashPoint.AFTER_LOOKUP, operation_id)
        return result


def _interpret(
    status: LookupStatus, found_fingerprint: Optional[str], expected_fingerprint: str
) -> ActionResult:
    if status == LookupStatus.COMMITTED:
        if found_fingerprint != expected_fingerprint:
            return ActionResult(
                Outcome.CONFLICT, "committed operation has a different payload fingerprint"
            )
        return ActionResult(Outcome.SUCCESS, "lookup found the committed operation")
    if status == LookupStatus.AUTHORITATIVELY_NOT_FOUND:
        return ActionResult(
            Outcome.FAILURE, "authoritative lookup found no operation; retry is safe"
        )
    return ActionResult(Outcome.UNKNOWN, "lookup cannot prove the operation is absent")
