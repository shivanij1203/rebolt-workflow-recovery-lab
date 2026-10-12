from pathlib import Path
from typing import Callable, Optional

import pytest

from app.actions import CrashPoint, ExternalAction, Outcome
from app.clock import ManualClock
from app.events import EventType
from app.external import ExternalSystem
from app.store import EventStore
from app.workflow import DEFAULT_LEASE_TTL, Workflow, WorkflowRunner

WORKFLOW_ID = "purchase-42"
ORDER = ["erp", "notify"]
ERP_PAYLOAD = {"purchase_id": "42", "status": "approved", "amount_cents": 125_000}
SLACK_PAYLOAD = {"channel": "#purchasing", "text": "Purchase 42 approved"}


def outcomes(*values: Outcome) -> Callable[[], Outcome]:
    remaining = iter(values)
    return lambda: next(remaining, Outcome.SUCCESS)


class Process:
    """One worker process: its own connections to the durable stores."""

    def __init__(
        self,
        root: Path,
        clock: ManualClock,
        worker_id: str,
        erp_provider: Optional[Callable[[], Outcome]] = None,
        notify_provider: Optional[Callable[[], Outcome]] = None,
        crash_at: Optional[CrashPoint] = None,
        erp_payload: Optional[dict] = None,
        authoritative_lookup: bool = True,
        lookup_available: bool = True,
    ):
        self.store = EventStore(root / "workflow.db", clock=clock)
        self.erp = ExternalSystem(
            root / "erp.db", "erp", authoritative_lookup, lookup_available
        )
        self.slack = ExternalSystem(root / "slack.db", "slack")
        self.actions = {
            "erp": ExternalAction(
                "update_erp", self.erp, erp_payload or ERP_PAYLOAD, erp_provider, crash_at
            ),
            "notify": ExternalAction("notify_slack", self.slack, SLACK_PAYLOAD, notify_provider),
        }
        self.workflow = Workflow(WORKFLOW_ID, self.store)
        self.runner = WorkflowRunner(self.workflow, self.actions, worker_id)

    def close(self) -> None:
        for resource in (self.store, self.erp, self.slack):
            resource.close()


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def start(tmp_path, clock):
    """Start a worker process against the shared durable files in tmp_path."""
    opened: list[Process] = []

    def _start(worker_id: str, **options) -> Process:
        process = Process(tmp_path, clock, worker_id, **options)
        opened.append(process)
        return process

    yield _start
    for process in opened:
        process.close()


def expire_leases(clock: ManualClock) -> None:
    clock.advance(DEFAULT_LEASE_TTL + 1)


def events_of(process: Process, action_id: str) -> list[tuple[str, int]]:
    return [
        (e.event_type.value, e.attempt)
        for e in process.workflow.events
        if e.action_id == action_id
    ]


def count(process: Process, event_type: EventType, action_id: str) -> int:
    return sum(
        1 for e in process.workflow.events
        if e.event_type == event_type and e.action_id == action_id
    )
