import tempfile
from pathlib import Path

from app.actions import ExternalAction, Outcome, WorkerCrash
from app.recovery import plan_recovery
from app.store import EventStore, ExternalSystem
from app.workflow import Workflow, WorkflowRunner


def run_case(name: str, crash_after_erp: bool = False):
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        store = EventStore(root / "workflow.db")
        erp = ExternalSystem(root / "erp.db", "erp")
        slack = ExternalSystem(root / "slack.db", "slack")
        workflow = Workflow(name, store)
        actions = {
            "erp": ExternalAction("update_erp", erp, crash_after_commit=crash_after_erp),
            "notify": ExternalAction("notify_slack", slack),
        }
        runner = WorkflowRunner(workflow, actions)

        try:
            first = runner.run(["erp", "notify"])
        except WorkerCrash as error:
            print(f"\n{name}")
            print(f"first run: worker crashed ({error})")
            first = workflow.state()

        plan = plan_recovery(workflow, ["erp", "notify"])
        print(f"recovery: {plan.decision.value} {plan.reason}")
        print(f"events: {[e.event_type.value + ':' + e.action_id for e in workflow.events]}")

        if plan.decision.value == "reconcile":
            action_id = plan.next_actions[0]
            print(f"reconcile: {action_id} -> {runner.reconcile(action_id).value}")
            plan = plan_recovery(workflow, ["erp", "notify"])

        if plan.decision.value == "resume":
            print(f"resume: {runner.recover(['erp', 'notify']).value}")
        else:
            print(f"final state: {workflow.state().value}")

        store.close()
        erp.close()
        slack.close()


if __name__ == "__main__":
    run_case("normal_completion")
    run_case("worker_crashes_after_erp_commit", crash_after_erp=True)
