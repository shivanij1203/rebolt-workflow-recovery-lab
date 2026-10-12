from dataclasses import dataclass
from enum import Enum
from typing import Callable

from .store import ExternalOperationStatus, ExternalSystem


class Outcome(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    UNKNOWN = "unknown"


class WorkerCrash(RuntimeError):
    """Raised after an external side effect commits but before local history does."""


@dataclass(frozen=True)
class ActionResult:
    outcome: Outcome
    detail: str = ""


class ExternalAction:
    def __init__(self, name: str, system: ExternalSystem,
                 outcome_provider: Callable[[], Outcome] | None = None,
                 crash_after_commit: bool = False):
        self.name = name
        self.system = system
        self.outcome_provider = outcome_provider
        self.crash_after_commit = crash_after_commit

    def execute(self, operation_id: str) -> ActionResult:
        current = self.system.status(operation_id)
        if current == ExternalOperationStatus.COMMITTED:
            return ActionResult(Outcome.SUCCESS, "already applied")

        outcome = self.outcome_provider() if self.outcome_provider else Outcome.SUCCESS
        if outcome == Outcome.FAILURE:
            return ActionResult(Outcome.FAILURE, "external system rejected the operation")
        if outcome == Outcome.UNKNOWN:
            return ActionResult(Outcome.UNKNOWN, "response was lost before the outcome was known")

        self.system.apply(operation_id)
        if self.crash_after_commit:
            raise WorkerCrash(
                f"worker crashed after {self.name} committed {operation_id}"
            )
        return ActionResult(Outcome.SUCCESS, "external system confirmed the operation")

    def reconcile(self, operation_id: str) -> ActionResult:
        status = self.system.status(operation_id)
        if status == ExternalOperationStatus.COMMITTED:
            return ActionResult(Outcome.SUCCESS, "reconciliation found a committed operation")
        if status == ExternalOperationStatus.NOT_FOUND:
            return ActionResult(Outcome.FAILURE, "reconciliation found no committed operation")
        return ActionResult(Outcome.UNKNOWN, "external status is still unknown")
