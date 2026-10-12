from pathlib import Path

import pytest

from app.actions import ExternalAction, Outcome, WorkerCrash
from app.events import EventType
from app.recovery import RecoveryDecision, plan_recovery
from app.store import EventStore, ExternalSystem
from app.workflow import Workflow, WorkflowRunner, WorkflowState


def make_lab(tmp_path: Path, erp_outcomes=None, notification_outcomes=None,
              crash_after_erp=False):
    store = EventStore(tmp_path / "workflow.db")
    erp_system = ExternalSystem(tmp_path / "erp.db", "erp")
    slack_system = ExternalSystem(tmp_path / "slack.db", "slack")

    erp_values = iter(erp_outcomes or [Outcome.SUCCESS])
    slack_values = iter(notification_outcomes or [Outcome.SUCCESS])

    actions = {
        "erp": ExternalAction(
            "update_erp", erp_system, lambda: next(erp_values, Outcome.SUCCESS), crash_after_erp
        ),
        "notify": ExternalAction(
            "notify_slack", slack_system, lambda: next(slack_values, Outcome.SUCCESS)
        ),
    }
    workflow = Workflow("purchase-42", store)
    return store, erp_system, slack_system, workflow, actions


def close_all(*resources):
    for resource in resources:
        resource.close()


def test_all_success_completes(tmp_path):
    store, erp, slack, workflow, actions = make_lab(tmp_path)
    try:
        state = WorkflowRunner(workflow, actions).run(["erp", "notify"])
        assert state == WorkflowState.COMPLETED
        assert workflow.succeeded_actions() == {"erp", "notify"}
        assert erp.committed_operations() == ["purchase-42:erp"]
    finally:
        close_all(store, erp, slack)


def test_unknown_response_is_not_treated_as_failure(tmp_path):
    store, erp, slack, workflow, actions = make_lab(
        tmp_path, erp_outcomes=[Outcome.UNKNOWN]
    )
    try:
        state = WorkflowRunner(workflow, actions).run(["erp", "notify"])
        assert state == WorkflowState.UNKNOWN
        assert workflow.unknown_actions() == {"erp"}
        assert not workflow.failed_actions()
    finally:
        close_all(store, erp, slack)


def test_failed_notification_retries_without_repeating_erp(tmp_path):
    store, erp, slack, workflow, actions = make_lab(
        tmp_path, notification_outcomes=[Outcome.FAILURE, Outcome.SUCCESS]
    )
    try:
        runner = WorkflowRunner(workflow, actions)
        assert runner.run(["erp", "notify"]) == WorkflowState.RECOVERABLE
        assert runner.recover(["erp", "notify"]) == WorkflowState.COMPLETED
        erp_successes = [
            e for e in workflow.events
            if e.action_id == "erp" and e.event_type == EventType.ACTION_SUCCEEDED
        ]
        assert len(erp_successes) == 1
    finally:
        close_all(store, erp, slack)


def test_unknown_outcome_requires_reconciliation(tmp_path):
    store, erp, slack, workflow, actions = make_lab(
        tmp_path, erp_outcomes=[Outcome.UNKNOWN]
    )
    try:
        runner = WorkflowRunner(workflow, actions)
        runner.run(["erp", "notify"])
        plan = plan_recovery(workflow, ["erp", "notify"])
        assert plan.decision == RecoveryDecision.RECONCILE
        with pytest.raises(RuntimeError):
            runner.recover(["erp", "notify"])
    finally:
        close_all(store, erp, slack)


def test_crash_after_external_commit_is_recovered_by_reconciliation(tmp_path):
    store, erp, slack, workflow, actions = make_lab(
        tmp_path, crash_after_erp=True
    )
    try:
        runner = WorkflowRunner(workflow, actions)
        with pytest.raises(WorkerCrash):
            runner.run(["erp", "notify"])

        # Simulate a real process restart by closing every connection and opening
        # the durable stores again.
        close_all(store, erp, slack)

        store2 = EventStore(tmp_path / "workflow.db")
        erp2 = ExternalSystem(tmp_path / "erp.db", "erp")
        slack2 = ExternalSystem(tmp_path / "slack.db", "slack")
        workflow2 = Workflow("purchase-42", store2)
        actions2 = {
            "erp": ExternalAction("update_erp", erp2),
            "notify": ExternalAction("notify_slack", slack2),
        }
        restarted = WorkflowRunner(workflow2, actions2)

        plan = plan_recovery(workflow2, ["erp", "notify"])
        assert plan.decision == RecoveryDecision.RECONCILE
        assert plan.next_actions == ("erp",)

        assert restarted.reconcile("erp") == WorkflowState.RUNNING
        assert restarted.recover(["erp", "notify"]) == WorkflowState.COMPLETED

        erp_attempts = [e.attempt for e in workflow2.events if e.action_id == "erp"]
        assert erp_attempts == [1, 1]
        erp_ops = erp2.committed_operations()
        assert erp_ops == ["purchase-42:erp"]
        close_all(store2, erp2, slack2)
    finally:
        # Resources are already closed in the crash test path.
        pass


def test_reconciliation_of_not_found_allows_safe_retry(tmp_path):
    store, erp, slack, workflow, actions = make_lab(tmp_path)
    try:
        workflow.append(EventType.ACTION_REQUESTED, "erp", "update_erp")
        runner = WorkflowRunner(workflow, actions)

        assert runner.reconcile("erp") == WorkflowState.RECOVERABLE
        assert workflow.failed_actions() == {"erp"}
        assert runner.recover(["erp", "notify"]) == WorkflowState.COMPLETED
        assert erp.committed_operations() == ["purchase-42:erp"]
    finally:
        close_all(store, erp, slack)


def test_unknown_reconciliation_not_found_can_be_retried(tmp_path):
    store, erp, slack, workflow, actions = make_lab(
        tmp_path, erp_outcomes=[Outcome.UNKNOWN, Outcome.SUCCESS]
    )
    try:
        runner = WorkflowRunner(workflow, actions)
        assert runner.run(["erp", "notify"]) == WorkflowState.UNKNOWN
        assert runner.reconcile("erp") == WorkflowState.RECOVERABLE
        assert runner.recover(["erp", "notify"]) == WorkflowState.COMPLETED

        erp_attempts = [e.attempt for e in workflow.events if e.action_id == "erp"]
        assert erp_attempts == [1, 1, 1, 2, 2]
        assert workflow.unknown_actions() == set()
        assert erp.committed_operations() == ["purchase-42:erp"]
    finally:
        close_all(store, erp, slack)
