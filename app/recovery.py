from dataclasses import dataclass
from enum import Enum

from .history import WorkflowState
from .workflow import Workflow


class RecoveryDecision(str, Enum):
    RESUME = "resume"
    RECONCILE = "reconcile"
    REVIEW = "review"
    COMPLETE = "complete"


@dataclass(frozen=True)
class RecoveryPlan:
    decision: RecoveryDecision
    next_actions: tuple[str, ...]
    reason: str


def plan_recovery(workflow: Workflow, action_order: list[str]) -> RecoveryPlan:
    history = workflow.history()
    state = history.state()

    if state == WorkflowState.COMPLETED:
        return RecoveryPlan(RecoveryDecision.COMPLETE, (), "workflow already completed")

    if state == WorkflowState.BLOCKED:
        conflicted = tuple(sorted(history.conflicted_actions()))
        latest = history.latest()
        detail = "; ".join(f"{a}: {latest[a].detail}" for a in conflicted)
        return RecoveryPlan(RecoveryDecision.REVIEW, conflicted, detail)

    unknown = tuple(sorted(history.unknown_actions()))
    if unknown:
        return RecoveryPlan(
            RecoveryDecision.RECONCILE,
            unknown,
            "the latest attempt has no trustworthy terminal outcome",
        )

    succeeded = history.succeeded_actions()
    return RecoveryPlan(
        RecoveryDecision.RESUME,
        tuple(a for a in action_order if a not in succeeded),
        "durable history shows which latest attempts already succeeded",
    )
