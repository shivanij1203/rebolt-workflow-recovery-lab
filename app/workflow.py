from enum import Enum
from typing import Dict

from .actions import ExternalAction, Outcome
from .events import Event, EventType
from .store import EventStore


class WorkflowState(str, Enum):
    RUNNING = "running"
    RECOVERABLE = "recoverable"
    UNKNOWN = "unknown"
    COMPLETED = "completed"


class Workflow:
    def __init__(self, workflow_id: str, store: EventStore):
        self.workflow_id = workflow_id
        self.store = store

    @property
    def events(self) -> list[Event]:
        return self.store.events(self.workflow_id)

    def append(
        self, event_type: EventType, action_id: str, action: str,
        attempt: int = 1, detail: str = ""
    ) -> Event:
        return self.store.append(
            self.workflow_id, action_id, action, event_type, attempt, detail
        )

    def _latest_events(self) -> dict[str, Event]:
        latest: dict[str, Event] = {}
        for event in self.events:
            current = latest.get(event.action_id)
            if current is None or (event.attempt, event.sequence) > (current.attempt, current.sequence):
                latest[event.action_id] = event
        return latest

    def state(self) -> WorkflowState:
        events = self.events
        if any(e.event_type == EventType.WORKFLOW_COMPLETED for e in events):
            return WorkflowState.COMPLETED

        latest = self._latest_events().values()
        if any(e.event_type in {EventType.ACTION_REQUESTED, EventType.ACTION_UNKNOWN} for e in latest):
            return WorkflowState.UNKNOWN
        if any(e.event_type == EventType.ACTION_FAILED for e in latest):
            return WorkflowState.RECOVERABLE
        return WorkflowState.RUNNING

    def succeeded_actions(self) -> set[str]:
        return {
            action_id for action_id, event in self._latest_events().items()
            if event.event_type == EventType.ACTION_SUCCEEDED
        }

    def failed_actions(self) -> set[str]:
        return {
            action_id for action_id, event in self._latest_events().items()
            if event.event_type == EventType.ACTION_FAILED
        }

    def unknown_actions(self) -> set[str]:
        return {
            action_id for action_id, event in self._latest_events().items()
            if event.event_type in {EventType.ACTION_REQUESTED, EventType.ACTION_UNKNOWN}
        }

    def pending_actions(self) -> set[str]:
        return {
            action_id for action_id, event in self._latest_events().items()
            if event.event_type == EventType.ACTION_REQUESTED
        }

    def next_attempt(self, action_id: str) -> int:
        attempts = [e.attempt for e in self.events if e.action_id == action_id]
        return max(attempts, default=0) + 1


class WorkflowRunner:
    def __init__(self, workflow: Workflow, actions: Dict[str, ExternalAction]):
        self.workflow = workflow
        self.actions = actions

    def run(self, action_order: list[str]) -> WorkflowState:
        for action_id in action_order:
            if action_id in self.workflow.succeeded_actions():
                continue
            if action_id in self.workflow.unknown_actions():
                return self.workflow.state()

            action = self.actions[action_id]
            operation_id = f"{self.workflow.workflow_id}:{action_id}"
            attempt = self.workflow.next_attempt(action_id)
            self.workflow.append(
                EventType.ACTION_REQUESTED, action_id, action.name, attempt
            )

            try:
                result = action.execute(operation_id)
            except Exception:
                raise

            self._record_result(action_id, action.name, result, attempt)
            if result.outcome != Outcome.SUCCESS:
                return self.workflow.state()

        if self.workflow.state() != WorkflowState.COMPLETED:
            self.workflow.append(
                EventType.WORKFLOW_COMPLETED,
                action_id="workflow",
                action="workflow",
            )
        return self.workflow.state()

    def _record_result(self, action_id: str, action_name: str, result, attempt: int) -> None:
        event_type = {
            Outcome.SUCCESS: EventType.ACTION_SUCCEEDED,
            Outcome.FAILURE: EventType.ACTION_FAILED,
            Outcome.UNKNOWN: EventType.ACTION_UNKNOWN,
        }[result.outcome]
        self.workflow.append(event_type, action_id, action_name, attempt, result.detail)

    def reconcile(self, action_id: str) -> WorkflowState:
        if action_id not in self.workflow.unknown_actions():
            return self.workflow.state()

        action = self.actions[action_id]
        operation_id = f"{self.workflow.workflow_id}:{action_id}"
        latest = [e for e in self.workflow.events if e.action_id == action_id][-1]
        attempt = latest.attempt
        try:
            result = action.reconcile(operation_id)
        except Exception:
            return self.workflow.state()
        self._record_result(action_id, action.name, result, attempt)
        return self.workflow.state()

    def recover(self, action_order: list[str]) -> WorkflowState:
        if self.workflow.state() == WorkflowState.COMPLETED:
            return WorkflowState.COMPLETED
        if self.workflow.unknown_actions():
            raise RuntimeError("reconcile unknown external outcome before retrying")
        return self.run(action_order)
