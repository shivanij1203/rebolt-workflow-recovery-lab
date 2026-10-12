from dataclasses import dataclass
from enum import Enum

from .workflow import Workflow, WorkflowState


class RecoveryDecision(str, Enum):
    RESUME = "resume"
    RECONCILE = "reconcile"
    COMPLETE = "complete"


@dataclass(frozen=True)
class RecoveryPlan:
    decision: RecoveryDecision
    next_actions: tuple[str, ...]
    reason: str


def plan_recovery(workflow: Workflow, action_order: list[str]) -> RecoveryPlan:
    if workflow.state() == WorkflowState.COMPLETED:
        return RecoveryPlan(RecoveryDecision.COMPLETE, (), "workflow already completed")

    if workflow.unknown_actions():
        return RecoveryPlan(
            RecoveryDecision.RECONCILE,
            tuple(sorted(workflow.unknown_actions())),
            "the latest attempt has no trustworthy terminal outcome",
        )

    next_actions = tuple(a for a in action_order if a not in workflow.succeeded_actions())
    return RecoveryPlan(
        RecoveryDecision.RESUME,
        next_actions,
        "durable history shows which latest attempts already succeeded",
    )
