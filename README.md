# Workflow Recovery Lab

A small distributed workflow experiment built around one question.

**What should a workflow do when an external write may have succeeded, but the worker does not know the outcome?**

The lab models a simple business operation.

`purchase approval → ERP update → notification → completion`

The systems are synthetic. The point is not to build another agent. The point is to make failure and recovery explicit when a workflow crosses system boundaries.

## The failure that matters

Consider this sequence.

```text
1. The workflow asks the ERP to update a purchase.
2. The ERP commits the update.
3. The worker crashes before recording the success locally.
4. A new worker starts.
```

The new worker cannot safely assume the ERP update failed. Retrying the write could create a duplicate business effect.

The lab therefore separates two pieces of durable state.

* The workflow event history records what the worker knows.
* The synthetic external systems record committed operation IDs.

Every business action gets a stable operation ID such as `purchase-42:erp`. The external system treats that ID as idempotent, so repeating the request does not create another business effect.

## Recovery model

The workflow distinguishes three outcomes.

**Success** means the external system confirmed the action.

**Failure** means the external system reported that the action did not happen. The workflow can retry it.

**Unknown** means the workflow does not have enough information to know whether the action happened. It must reconcile before retrying.

A worker crash after an external commit is represented by an `ACTION_REQUESTED` event with no terminal event. On restart, that incomplete request is also treated as unknown.

Recovery first asks the external system for the operation status.

* `committed` means record success locally and continue.
* `not_found` means the operation did not commit and can safely be retried.
* `unknown` means the workflow remains blocked.

The system does not pretend that separate external systems form one atomic transaction.

## Attempt history

Each action attempt is versioned in the event log. A retry creates a new attempt rather than overwriting the previous outcome. Recovery decisions use the latest attempt, so an old unknown result cannot permanently block a workflow after reconciliation.

The lab intentionally keeps concurrency simple. It does not claim to be a distributed lock service. In production, multiple workers would need a coordination mechanism such as a lease or optimistic concurrency check so two workers cannot both advance the same workflow at once. The external operation ID still protects the business effect when the connector supports idempotency.

## What is tested

The test suite covers:

* normal completion
* an explicitly unknown external outcome
* retrying a failed notification without repeating the successful ERP action
* requiring reconciliation for an unknown outcome
* a worker crash after the ERP commits but before local success is recorded
* reopening the durable stores after a simulated process restart
* reconciling a missing operation and then safely retrying it

## Run

```bash
python -m pytest -q
python run_lab.py
```

## Why SQLite

SQLite is intentionally enough for this experiment. It gives us real persistence and process restart behavior without hiding the important recovery semantics behind a queue, ORM, cloud service, or distributed database.

A production implementation would need stronger guarantees around transaction boundaries, connector behavior, concurrency, leases, observability, retention, and the exact idempotency contract of each external system.

The important design constraint is that **at least once execution is not the same thing as duplicate business effects**. The latter depends on stable operation identity and an external system that can safely recognize repeated operations.
