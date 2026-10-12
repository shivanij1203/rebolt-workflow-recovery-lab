from dataclasses import dataclass
from enum import Enum
from typing import Optional

from .events import Event, EventType

PENDING = {EventType.ACTION_REQUESTED, EventType.ACTION_UNKNOWN}


class WorkflowState(str, Enum):
    RUNNING = "running"
    RECOVERABLE = "recoverable"
    UNKNOWN = "unknown"
    BLOCKED = "blocked"
    COMPLETED = "completed"


@dataclass(frozen=True)
class History:
    """Read-only view of one workflow's events, used to make one decision."""

    events: tuple[Event, ...]

    @property
    def version(self) -> int:
        return self.events[-1].version if self.events else 0

    def _action_events(self, action_id: str) -> list[Event]:
        return [e for e in self.events if e.action_id == action_id]

    def latest(self) -> dict[str, Event]:
        latest: dict[str, Event] = {}
        for event in self.events:
            if event.event_type == EventType.WORKFLOW_COMPLETED:
                continue
            current = latest.get(event.action_id)
            if current is None or (event.attempt, event.version) > (current.attempt, current.version):
                latest[event.action_id] = event
        return latest

    def _actions_where(self, event_types: set[EventType]) -> set[str]:
        return {a for a, e in self.latest().items() if e.event_type in event_types}

    def succeeded_actions(self) -> set[str]:
        return self._actions_where({EventType.ACTION_SUCCEEDED})

    def failed_actions(self) -> set[str]:
        return self._actions_where({EventType.ACTION_FAILED})

    def unknown_actions(self) -> set[str]:
        return self._actions_where(PENDING)

    def conflicted_actions(self) -> set[str]:
        return self._actions_where({EventType.ACTION_CONFLICT})

    def state(self) -> WorkflowState:
        if any(e.event_type == EventType.WORKFLOW_COMPLETED for e in self.events):
            return WorkflowState.COMPLETED
        if self.conflicted_actions():
            return WorkflowState.BLOCKED
        if self.unknown_actions():
            return WorkflowState.UNKNOWN
        if self.failed_actions():
            return WorkflowState.RECOVERABLE
        return WorkflowState.RUNNING

    def next_attempt(self, action_id: str) -> int:
        return max((e.attempt for e in self._action_events(action_id)), default=0) + 1

    def request(self, action_id: str, attempt: Optional[int] = None) -> Optional[Event]:
        """The request event for an attempt, or the first request if attempt is None."""
        for event in self._action_events(action_id):
            if event.event_type == EventType.ACTION_REQUESTED and attempt in (None, event.attempt):
                return event
        return None
