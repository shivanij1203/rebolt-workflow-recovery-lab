import tempfile
from pathlib import Path
from typing import Optional

from app.actions import CrashPoint, ExternalAction, WorkerCrash
from app.clock import ManualClock
from app.external import ExternalSystem, PayloadMismatch
from app.recovery import plan_recovery
from app.store import EventStore
from app.workflow import DEFAULT_LEASE_TTL, Workflow, WorkflowRunner

ORDER = ["erp", "notify"]
ERP_PAYLOAD = {"purchase_id": "42", "status": "approved", "amount_cents": 125_000}
SLACK_PAYLOAD = {"channel": "#purchasing", "text": "Purchase 42 approved"}


class Worker:
    """One process with its own connections to the durable stores."""

    def __init__(self, root: Path, clock: ManualClock, worker_id: str,
                 crash_at: Optional[CrashPoint] = None, authoritative_lookup: bool = True):
        self.store = EventStore(root / "workflow.db", clock=clock)
        self.erp = ExternalSystem(root / "erp.db", "erp", authoritative_lookup)
        self.slack = ExternalSystem(root / "slack.db", "slack")
        self.workflow = Workflow("purchase-42", self.store)
        self.runner = WorkflowRunner(self.workflow, {
            "erp": ExternalAction("update_erp", self.erp, ERP_PAYLOAD, crash_at=crash_at),
            "notify": ExternalAction("notify_slack", self.slack, SLACK_PAYLOAD),
        }, worker_id)

    def close(self) -> None:
        for resource in (self.store, self.erp, self.slack):
            resource.close()


def crash_then_recover(title: str, crash_at: CrashPoint, authoritative_lookup: bool) -> None:
    print(f"\n{title}")
    with tempfile.TemporaryDirectory() as directory:
        root, clock = Path(directory), ManualClock()
        first = Worker(root, clock, "worker-a", crash_at=crash_at)
        try:
            first.runner.run(ORDER)
        except WorkerCrash as error:
            print(f"  worker-a: {error}")
        first.close()

        clock.advance(DEFAULT_LEASE_TTL + 1)
        second = Worker(root, clock, "worker-b", authoritative_lookup=authoritative_lookup)
        plan = plan_recovery(second.workflow, ORDER)
        print(f"  worker-b plan: {plan.decision.value} {list(plan.next_actions)}")
        state = second.runner.reconcile("erp")
        print(f"  reconcile erp: {state.value} ({second.workflow.events[-1].detail})")
        if not second.workflow.unknown_actions():
            state = second.runner.recover(ORDER)
        print(f"  final state: {state.value}, erp effects: {second.erp.effect_count()}")
        second.close()


def payload_reuse() -> None:
    print("\nsame operation ID, different payload")
    with tempfile.TemporaryDirectory() as directory:
        erp = ExternalSystem(Path(directory) / "erp.db", "erp")
        print(f"  first:  {erp.apply('purchase-42:erp', ERP_PAYLOAD).result}")
        print(f"  repeat: replayed={erp.apply('purchase-42:erp', ERP_PAYLOAD).replayed}")
        try:
            erp.apply("purchase-42:erp", {**ERP_PAYLOAD, "amount_cents": 1})
        except PayloadMismatch as error:
            print(f"  changed payload rejected: {error}")
        print(f"  erp effects: {erp.effect_count()}")
        erp.close()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        worker = Worker(Path(directory), ManualClock(), "worker-a")
        print(f"normal run: {worker.runner.run(ORDER).value}")
        worker.close()

    crash_then_recover("crash after ERP commit", CrashPoint.AFTER_COMMIT, True)
    crash_then_recover("crash before ERP write", CrashPoint.BEFORE_WRITE, True)
    crash_then_recover(
        "crash before ERP write, lookup not authoritative", CrashPoint.BEFORE_WRITE, False
    )
    payload_reuse()
