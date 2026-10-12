from dataclasses import dataclass
from enum import Enum
from typing import Optional


class EventType(str, Enum):
    WORKFLOW_STARTED = "workflow_started"
    ACTION_REQUESTED = "action_requested"
    ACTION_SUCCEEDED = "action_succeeded"
    ACTION_FAILED = "action_failed"
    ACTION_UNKNOWN = "action_unknown"
    WORKFLOW_COMPLETED = "workflow_completed"


@dataclass(frozen=True)
class Event:
    sequence: int
    workflow_id: str
    action_id: str
    action: str
    event_type: EventType
    attempt: int = 1
    detail: Optional[str] = None
