import sqlite3
import threading

import pytest

from app.actions import CrashPoint, Outcome, WorkerCrash
from app.events import EventType
from app.store import EventStore, LeaseLost, LeaseUnavailable, VersionConflict
from app.workflow import DEFAULT_LEASE_TTL, WorkflowState

from conftest import ORDER, WORKFLOW_ID, Process, count, expire_leases


def race(*workers):
    """Run each callable on its own thread, released together by a barrier."""
    barrier = threading.Barrier(len(workers))
    results: list = [None] * len(workers)

    def run(index, work):
        barrier.wait()
        try:
            results[index] = work()
        except Exception as error:  # collected so the test can assert on it
            results[index] = error

    threads = [threading.Thread(target=run, args=(i, w)) for i, w in enumerate(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return results


def test_only_one_of_two_racing_workers_gets_the_lease(tmp_path, clock):
    EventStore(tmp_path / "workflow.db", clock=clock).close()

    def contender(owner, workflow_id):
        def work():
            store = EventStore(tmp_path / "workflow.db", clock=clock)
            try:
                return store.acquire_lease(workflow_id, owner, DEFAULT_LEASE_TTL)
            finally:
                store.close()
        return work

    for round_number in range(25):
        workflow_id = f"purchase-{round_number}"
        results = race(contender("a", workflow_id), contender("b", workflow_id))
        winners = [r for r in results if not isinstance(r, Exception)]
        losers = [r for r in results if isinstance(r, LeaseUnavailable)]
        assert len(winners) == 1 and len(losers) == 1, results


def test_racing_recovery_workers_apply_each_effect_once(start, clock, tmp_path):
    crashed = start("worker-a", crash_at=CrashPoint.BEFORE_WRITE)
    with pytest.raises(WorkerCrash):
        crashed.runner.run(ORDER)
    expire_leases(clock)

    def recover(worker_id):
        def work():
            # Opened inside the thread: SQLite connections belong to one thread.
            worker = Process(tmp_path, clock, worker_id)
            try:
                worker.runner.reconcile("erp")
                return worker.runner.recover(ORDER)
            except LeaseUnavailable as error:
                return error
            finally:
                worker.close()
        return work

    results = race(recover("worker-b"), recover("worker-c"))
    assert all(r == WorkflowState.COMPLETED or isinstance(r, LeaseUnavailable) for r in results)

    # Whichever worker loses a lease race backs off, and the other finishes.
    assert WorkflowState.COMPLETED in results
    check = start("checker")
    assert check.workflow.state() == WorkflowState.COMPLETED

    events = check.workflow.events
    assert [e.version for e in events] == list(range(1, len(events) + 1))
    assert count(check, EventType.WORKFLOW_COMPLETED, "workflow") == 1
    assert check.erp.effect_count() == 1
    assert check.slack.effect_count() == 1


def test_expired_lease_cannot_append(tmp_path, clock):
    store = EventStore(tmp_path / "workflow.db", clock=clock)
    try:
        lease = store.acquire_lease(WORKFLOW_ID, "worker-a", DEFAULT_LEASE_TTL)
        expire_leases(clock)
        with pytest.raises(LeaseLost):
            store.append(lease, 0, action_id="erp", action="update_erp",
                         event_type=EventType.ACTION_REQUESTED, attempt=1)
        assert store.events(WORKFLOW_ID) == []
    finally:
        store.close()


def test_stale_owner_is_fenced_after_takeover(tmp_path, clock):
    store_a = EventStore(tmp_path / "workflow.db", clock=clock)
    store_b = EventStore(tmp_path / "workflow.db", clock=clock)
    try:
        stale = store_a.acquire_lease(WORKFLOW_ID, "worker-a", DEFAULT_LEASE_TTL)
        with pytest.raises(LeaseUnavailable):
            store_b.acquire_lease(WORKFLOW_ID, "worker-b", DEFAULT_LEASE_TTL)

        expire_leases(clock)
        current = store_b.acquire_lease(WORKFLOW_ID, "worker-b", DEFAULT_LEASE_TTL)
        assert current.token > stale.token

        with pytest.raises(LeaseLost):
            store_a.append(stale, 0, action_id="erp", action="update_erp",
                           event_type=EventType.ACTION_REQUESTED, attempt=1)
        event = store_b.append(current, 0, action_id="erp", action="update_erp",
                               event_type=EventType.ACTION_REQUESTED, attempt=1)
        assert (event.worker_id, event.lease_token) == ("worker-b", current.token)
    finally:
        store_a.close()
        store_b.close()


def test_stale_worker_side_effect_is_deduplicated_and_its_history_write_rejected(start, clock):
    """Local fencing cannot stop a paused worker from calling the ERP when it wakes.

    Worker A sends its request, then stalls past its lease. Worker B takes over,
    finds no committed operation, retries, and finishes. When A resumes, its ERP
    call still goes out. The stable operation ID turns it into a replay, and the
    lease check rejects A's attempt to record anything.
    """

    def stall_then_continue():
        clock.advance(DEFAULT_LEASE_TTL + 1)
        takeover = start("worker-b")
        assert takeover.runner.reconcile("erp") == WorkflowState.RECOVERABLE
        assert takeover.runner.recover(ORDER) == WorkflowState.COMPLETED
        return Outcome.SUCCESS

    stale = start("worker-a", erp_provider=stall_then_continue)
    with pytest.raises(LeaseLost):
        stale.runner.run(ORDER)

    events = stale.workflow.events
    assert stale.erp.effect_count() == 1
    assert count(stale, EventType.WORKFLOW_COMPLETED, "workflow") == 1
    stale_events = [e for e in events if e.worker_id == "worker-a"]
    assert [e.event_type for e in stale_events] == [EventType.ACTION_REQUESTED]


def test_decision_based_on_stale_history_is_rejected(tmp_path, clock):
    store = EventStore(tmp_path / "workflow.db", clock=clock)
    try:
        lease = store.acquire_lease(WORKFLOW_ID, "worker-a", DEFAULT_LEASE_TTL)
        store.append(lease, 0, action_id="erp", action="update_erp",
                     event_type=EventType.ACTION_REQUESTED, attempt=1)
        with pytest.raises(VersionConflict):
            store.append(lease, 0, action_id="erp", action="update_erp",
                         event_type=EventType.ACTION_REQUESTED, attempt=2)
    finally:
        store.close()


def test_history_rejects_contradictions_and_rewrites(tmp_path, clock):
    store = EventStore(tmp_path / "workflow.db", clock=clock)
    try:
        lease = store.acquire_lease(WORKFLOW_ID, "worker-a", DEFAULT_LEASE_TTL)
        for version, event_type in enumerate(
            [EventType.ACTION_REQUESTED, EventType.ACTION_SUCCEEDED]
        ):
            store.append(lease, version, action_id="erp", action="update_erp",
                         event_type=event_type, attempt=1)

        with pytest.raises(sqlite3.IntegrityError):
            store.append(lease, 2, action_id="erp", action="update_erp",
                         event_type=EventType.ACTION_FAILED, attempt=1)
        with pytest.raises(sqlite3.IntegrityError):
            store.connection.execute("UPDATE workflow_events SET event_type = 'action_failed'")
        with pytest.raises(sqlite3.IntegrityError):
            store.connection.execute("DELETE FROM workflow_events")
        assert len(store.events(WORKFLOW_ID)) == 2
    finally:
        store.close()


def test_released_lease_cannot_be_reused_after_reacquisition(tmp_path, clock):
    store = EventStore(tmp_path / "workflow.db", clock=clock)
    try:
        old = store.acquire_lease(WORKFLOW_ID, "worker-a", DEFAULT_LEASE_TTL)
        store.release_lease(old)
        new = store.acquire_lease(WORKFLOW_ID, "worker-a", DEFAULT_LEASE_TTL)
        assert new.token > old.token
        with pytest.raises(LeaseLost):
            store.append(old, 0, action_id="erp", action="update_erp",
                         event_type=EventType.ACTION_REQUESTED, attempt=1)
    finally:
        store.close()


def test_live_lease_is_exclusive_even_for_the_same_worker_id(tmp_path, clock):
    # Two processes misconfigured with one worker ID must not both own the workflow.
    store_a = EventStore(tmp_path / "workflow.db", clock=clock)
    store_b = EventStore(tmp_path / "workflow.db", clock=clock)
    try:
        store_a.acquire_lease(WORKFLOW_ID, "worker-a", DEFAULT_LEASE_TTL)
        with pytest.raises(LeaseUnavailable):
            store_b.acquire_lease(WORKFLOW_ID, "worker-a", DEFAULT_LEASE_TTL)
    finally:
        store_a.close()
        store_b.close()
