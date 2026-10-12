from typing import Callable, Dict

from .actions import ActionResult, ExternalAction, Outcome
from .events import Event, EventType
from .history import History, WorkflowState
from .store import EventStore, Lease

DEFAULT_LEASE_TTL = 30.0

OUTCOME_EVENTS = {
    Outcome.SUCCESS: EventType.ACTION_SUCCEEDED,
    Outcome.FAILURE: EventType.ACTION_FAILED,
    Outcome.UNKNOWN: EventType.ACTION_UNKNOWN,
    Outcome.CONFLICT: EventType.ACTION_CONFLICT,
}


class Workflow:
    def __init__(self, workflow_id: str, store: EventStore):
        _require_no_separator(workflow_id)
        self.workflow_id = workflow_id
        self.store = store

    @property
    def events(self) -> list[Event]:
        return self.store.events(self.workflow_id)

    def history(self) -> History:
        return History(tuple(self.events))

    def state(self) -> WorkflowState:
        return self.history().state()

    def succeeded_actions(self) -> set[str]:
        return self.history().succeeded_actions()

    def failed_actions(self) -> set[str]:
        return self.history().failed_actions()

    def unknown_actions(self) -> set[str]:
        return self.history().unknown_actions()

    def operation_id(self, action_id: str) -> str:
        # Stable across attempts and workers: this is what makes a retry safe to repeat.
        _require_no_separator(action_id)
        return f"{self.workflow_id}:{action_id}"


def _require_no_separator(identifier: str) -> None:
    # "a:b" + "c" and "a" + "b:c" would otherwise share an operation ID.
    if ":" in identifier:
        raise ValueError(f"identifier {identifier!r} must not contain ':'")


class WorkflowRunner:
    def __init__(
        self,
        workflow: Workflow,
        actions: Dict[str, ExternalAction],
        worker_id: str,
        lease_ttl: float = DEFAULT_LEASE_TTL,
    ):
        self.workflow = workflow
        self.actions = actions
        self.worker_id = worker_id
        self.lease_ttl = lease_ttl

    def _with_lease(self, work: Callable[[Lease], WorkflowState]) -> WorkflowState:
        lease = self.workflow.store.acquire_lease(
            self.workflow.workflow_id, self.worker_id, self.lease_ttl
        )
        state = work(lease)
        # Released only on a clean return. A crash leaves the lease to expire,
        # which is how the next worker detects that this one is gone.
        self.workflow.store.release_lease(lease)
        return state

    def run(self, action_order: list[str]) -> WorkflowState:
        return self._with_lease(lambda lease: self._advance(lease, action_order))

    def recover(self, action_order: list[str]) -> WorkflowState:
        history = self.workflow.history()
        if history.state() == WorkflowState.COMPLETED:
            return WorkflowState.COMPLETED
        if history.unknown_actions():
            raise RuntimeError("reconcile unknown external outcome before retrying")
        return self.run(action_order)

    def reconcile(self, action_id: str) -> WorkflowState:
        return self._with_lease(lambda lease: self._reconcile(lease, action_id))

    def _record(
        self, lease: Lease, expected_version: int, action_id: str,
        attempt: int, result: ActionResult, payload_fingerprint: str,
    ) -> Event:
        return self.workflow.store.append(
            lease,
            expected_version,
            action_id=action_id,
            action=self.actions[action_id].name,
            event_type=OUTCOME_EVENTS[result.outcome],
            attempt=attempt,
            operation_id=self.workflow.operation_id(action_id),
            payload_fingerprint=payload_fingerprint,
            detail=result.detail,
        )

    def _advance(self, lease: Lease, action_order: list[str]) -> WorkflowState:
        for action_id in action_order:
            history = self.workflow.history()
            if history.state() == WorkflowState.BLOCKED:
                return WorkflowState.BLOCKED

            action = self.actions[action_id]
            attempt = history.next_attempt(action_id)
            first_request = history.request(action_id)
            # Checked before the "already succeeded" shortcut: a caller that now
            # wants a different payload must not see the old effect as its own.
            if first_request and first_request.payload_fingerprint != action.fingerprint:
                conflict = ActionResult(
                    Outcome.CONFLICT,
                    f"payload differs from attempt {first_request.attempt}; not applying",
                )
                self._record(lease, history.version, action_id, attempt, conflict, action.fingerprint)
                return WorkflowState.BLOCKED
            if action_id in history.succeeded_actions():
                continue
            if history.state() == WorkflowState.UNKNOWN:
                return WorkflowState.UNKNOWN

            requested = self.workflow.store.append(
                lease,
                history.version,
                action_id=action_id,
                action=action.name,
                event_type=EventType.ACTION_REQUESTED,
                attempt=attempt,
                operation_id=self.workflow.operation_id(action_id),
                payload_fingerprint=action.fingerprint,
            )
            result = action.execute(self.workflow.operation_id(action_id))
            self._record(lease, requested.version, action_id, attempt, result, action.fingerprint)
            if result.outcome != Outcome.SUCCESS:
                return self.workflow.state()

        history = self.workflow.history()
        # Every action in the order succeeded above; this also refuses to
        # complete over unresolved actions the caller left out of the order.
        if history.state() != WorkflowState.RUNNING:
            return history.state()
        self.workflow.store.append(
            lease,
            history.version,
            action_id="workflow",
            action="workflow",
            event_type=EventType.WORKFLOW_COMPLETED,
            attempt=1,
        )
        return WorkflowState.COMPLETED

    def _reconcile(self, lease: Lease, action_id: str) -> WorkflowState:
        history = self.workflow.history()
        if action_id not in history.unknown_actions():
            return history.state()

        attempt = history.latest()[action_id].attempt
        request = history.request(action_id, attempt)
        result = self.actions[action_id].reconcile(
            self.workflow.operation_id(action_id), request.payload_fingerprint
        )
        self._record(lease, history.version, action_id, attempt, result, request.payload_fingerprint)
        return self.workflow.state()
