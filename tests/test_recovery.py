import pytest

from app.actions import CrashPoint, Outcome, WorkerCrash
from app.events import EventType
from app.recovery import RecoveryDecision, plan_recovery
from app.workflow import WorkflowState

from conftest import ORDER, count, events_of, expire_leases, outcomes


def test_all_success_completes(start):
    worker = start("worker-a")
    assert worker.runner.run(ORDER) == WorkflowState.COMPLETED
    assert worker.workflow.succeeded_actions() == {"erp", "notify"}
    assert worker.erp.committed_operations() == ["purchase-42:erp"]
    assert worker.erp.effect_count() == 1


def test_unknown_response_is_not_treated_as_failure(start):
    worker = start("worker-a", erp_provider=outcomes(Outcome.UNKNOWN))
    assert worker.runner.run(ORDER) == WorkflowState.UNKNOWN
    assert worker.workflow.unknown_actions() == {"erp"}
    assert not worker.workflow.failed_actions()
    with pytest.raises(RuntimeError):
        worker.runner.recover(ORDER)


def test_crash_before_external_write_retries_after_authoritative_not_found(start, clock):
    crashed = start("worker-a", crash_at=CrashPoint.BEFORE_WRITE)
    with pytest.raises(WorkerCrash):
        crashed.runner.run(ORDER)
    assert crashed.erp.effect_count() == 0

    expire_leases(clock)
    restarted = start("worker-b")
    assert plan_recovery(restarted.workflow, ORDER).decision == RecoveryDecision.RECONCILE
    assert restarted.runner.reconcile("erp") == WorkflowState.RECOVERABLE
    assert restarted.runner.recover(ORDER) == WorkflowState.COMPLETED

    assert events_of(restarted, "erp") == [
        ("action_requested", 1), ("action_failed", 1),
        ("action_requested", 2), ("action_succeeded", 2),
    ]
    assert restarted.erp.effect_count() == 1


def test_crash_after_commit_reconciles_without_repeating_the_write(start, clock):
    crashed = start("worker-a", crash_at=CrashPoint.AFTER_COMMIT)
    with pytest.raises(WorkerCrash):
        crashed.runner.run(ORDER)
    assert crashed.erp.effect_count() == 1

    expire_leases(clock)
    restarted = start("worker-b")
    assert restarted.runner.reconcile("erp") == WorkflowState.RUNNING
    assert restarted.runner.recover(ORDER) == WorkflowState.COMPLETED

    assert count(restarted, EventType.ACTION_REQUESTED, "erp") == 1
    assert restarted.erp.effect_count() == 1
    assert restarted.slack.effect_count() == 1


def test_crash_during_reconciliation_leaves_history_unchanged(start, clock):
    crashed = start("worker-a", crash_at=CrashPoint.AFTER_COMMIT)
    with pytest.raises(WorkerCrash):
        crashed.runner.run(ORDER)
    before = crashed.workflow.events

    expire_leases(clock)
    reconciler = start("worker-b", crash_at=CrashPoint.AFTER_LOOKUP)
    with pytest.raises(WorkerCrash):
        reconciler.runner.reconcile("erp")
    assert reconciler.workflow.events == before

    expire_leases(clock)
    final = start("worker-c")
    assert final.runner.reconcile("erp") == WorkflowState.RUNNING
    assert final.runner.recover(ORDER) == WorkflowState.COMPLETED
    assert final.erp.effect_count() == 1


def test_failed_notification_does_not_repeat_the_erp_write(start):
    worker = start("worker-a", notify_provider=outcomes(Outcome.FAILURE))
    assert worker.runner.run(ORDER) == WorkflowState.RECOVERABLE
    assert worker.runner.recover(ORDER) == WorkflowState.COMPLETED

    assert count(worker, EventType.ACTION_REQUESTED, "erp") == 1
    assert worker.erp.effect_count() == 1
    assert worker.slack.effect_count() == 1


@pytest.mark.parametrize(
    "connector",
    [{"authoritative_lookup": False}, {"lookup_available": False}],
    ids=["absence-not-authoritative", "lookup-unavailable"],
)
def test_inconclusive_reconciliation_does_not_retry(start, clock, connector):
    crashed = start("worker-a", crash_at=CrashPoint.BEFORE_WRITE)
    with pytest.raises(WorkerCrash):
        crashed.runner.run(ORDER)

    expire_leases(clock)
    restarted = start("worker-b", **connector)
    assert restarted.runner.reconcile("erp") == WorkflowState.UNKNOWN
    assert plan_recovery(restarted.workflow, ORDER).decision == RecoveryDecision.RECONCILE
    with pytest.raises(RuntimeError):
        restarted.runner.recover(ORDER)
    assert count(restarted, EventType.ACTION_REQUESTED, "erp") == 1
    assert restarted.erp.effect_count() == 0

    # The operation stays pending until a lookup can actually prove something.
    later = start("worker-c")
    assert later.runner.reconcile("erp") == WorkflowState.RECOVERABLE
    assert later.runner.recover(ORDER) == WorkflowState.COMPLETED
    assert later.erp.effect_count() == 1


def test_completion_requires_every_action_to_be_resolved(start):
    worker = start("worker-a", erp_provider=outcomes(Outcome.UNKNOWN))
    assert worker.runner.run(ORDER) == WorkflowState.UNKNOWN
    # A caller that leaves the unresolved action out of the order cannot complete it.
    assert worker.runner.run([]) == WorkflowState.UNKNOWN
    assert worker.runner.run(["notify"]) == WorkflowState.UNKNOWN
    assert count(worker, EventType.WORKFLOW_COMPLETED, "workflow") == 0
