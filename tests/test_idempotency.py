import pytest

from app.actions import CrashPoint, WorkerCrash
from app.events import EventType
from app.external import ExternalSystem, LookupStatus, PayloadMismatch, fingerprint
from app.recovery import RecoveryDecision, plan_recovery
from app.store import EventStore
from app.workflow import Workflow, WorkflowState

from conftest import ERP_PAYLOAD, ORDER, count, expire_leases


@pytest.fixture
def erp(tmp_path):
    system = ExternalSystem(tmp_path / "erp.db", "erp")
    yield system
    system.close()


def test_fingerprint_is_independent_of_key_order():
    assert fingerprint({"a": 1, "b": 2}) == fingerprint({"b": 2, "a": 1})
    assert fingerprint({"a": 1}) != fingerprint({"a": 2})


def test_repeated_identical_request_returns_the_original_result(erp):
    first = erp.apply("purchase-42:erp", ERP_PAYLOAD)
    second = erp.apply("purchase-42:erp", dict(reversed(list(ERP_PAYLOAD.items()))))
    assert not first.replayed
    assert second.replayed
    assert second.result == first.result
    assert erp.effect_count() == 1


def test_same_operation_id_with_different_payload_is_rejected(erp):
    original = erp.apply("purchase-42:erp", ERP_PAYLOAD)
    with pytest.raises(PayloadMismatch):
        erp.apply("purchase-42:erp", {**ERP_PAYLOAD, "amount_cents": 999_999})
    assert erp.effect_count() == 1
    assert erp.lookup("purchase-42:erp").payload_fingerprint == original.payload_fingerprint


def test_absence_only_counts_as_evidence_under_an_authoritative_contract(tmp_path):
    strict = ExternalSystem(tmp_path / "a.db", "erp", authoritative_lookup=True)
    loose = ExternalSystem(tmp_path / "b.db", "erp", authoritative_lookup=False)
    try:
        assert strict.lookup("x").status == LookupStatus.AUTHORITATIVELY_NOT_FOUND
        assert loose.lookup("x").status == LookupStatus.INCONCLUSIVE
        loose.apply("x", ERP_PAYLOAD)
        assert loose.lookup("x").status == LookupStatus.COMMITTED
    finally:
        strict.close()
        loose.close()


def test_changed_payload_after_crash_blocks_instead_of_retrying(start, clock):
    crashed = start("worker-a", crash_at=CrashPoint.BEFORE_WRITE)
    with pytest.raises(WorkerCrash):
        crashed.runner.run(ORDER)

    expire_leases(clock)
    changed = start("worker-b", erp_payload={**ERP_PAYLOAD, "amount_cents": 1})
    assert changed.runner.reconcile("erp") == WorkflowState.RECOVERABLE
    assert changed.runner.recover(ORDER) == WorkflowState.BLOCKED

    assert plan_recovery(changed.workflow, ORDER).decision == RecoveryDecision.REVIEW
    assert count(changed, EventType.ACTION_REQUESTED, "erp") == 1
    assert changed.erp.effect_count() == 0


def test_committed_operation_with_different_payload_is_a_conflict(start, clock):
    crashed = start("worker-a", crash_at=CrashPoint.AFTER_COMMIT)
    with pytest.raises(WorkerCrash):
        crashed.runner.run(ORDER)

    # The external record no longer matches what this workflow requested, for
    # example because another client collided on the operation ID. The external
    # system refuses such a write, so the mismatch is injected directly.
    crashed.erp.connection.execute(
        "UPDATE external_operations SET payload_fingerprint = 'other'"
    )

    expire_leases(clock)
    restarted = start("worker-b")
    assert restarted.runner.reconcile("erp") == WorkflowState.BLOCKED
    assert plan_recovery(restarted.workflow, ORDER).decision == RecoveryDecision.REVIEW


def test_changed_payload_after_commit_is_not_reported_as_done(start, clock):
    crashed = start("worker-a", crash_at=CrashPoint.AFTER_COMMIT)
    with pytest.raises(WorkerCrash):
        crashed.runner.run(ORDER)

    expire_leases(clock)
    changed = start("worker-b", erp_payload={**ERP_PAYLOAD, "amount_cents": 1})
    # Reconciliation records what really happened: the original payload committed.
    assert changed.runner.reconcile("erp") == WorkflowState.RUNNING
    # The new payload was never applied, so the workflow must not complete.
    assert changed.runner.recover(ORDER) == WorkflowState.BLOCKED
    assert plan_recovery(changed.workflow, ORDER).decision == RecoveryDecision.REVIEW
    assert changed.erp.effect_count() == 1


def test_operation_ids_cannot_collide_across_workflows(tmp_path):
    store = EventStore(tmp_path / "workflow.db")
    try:
        with pytest.raises(ValueError):
            Workflow("purchase:42", store)
        with pytest.raises(ValueError):
            Workflow("purchase-42", store).operation_id("erp:retry")
    finally:
        store.close()
