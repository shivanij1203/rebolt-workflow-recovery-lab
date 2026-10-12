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

Every business action gets a stable operation ID such as `purchase-42:erp`. The external system stores that ID together with a SHA-256 fingerprint of the canonical JSON payload.

* Repeating the same ID with the same payload returns the original result without a second business effect.
* Reusing the ID with a different payload is rejected.

The workflow also pins the payload of the first request. If a later attempt would send a different payload, the workflow records a conflict and stops instead of retrying.

This gives at least once delivery with deduplication at the receiver. It is not exactly once execution.

## Recovery model

The workflow distinguishes four outcomes.

**Success** means the external system confirmed the action.

**Failure** means the external system reported that the action did not happen. The workflow can retry it.

**Unknown** means the workflow does not have enough information to know whether the action happened. It must reconcile before retrying.

**Conflict** means the operation ID is tied to a different payload than the one the workflow now wants. The workflow stops for review and does not retry.

A worker crash after an external commit is represented by an `ACTION_REQUESTED` event with no terminal event. On restart, that incomplete request is also treated as unknown.

Before any retry, reconciliation asks the external system for the operation status.

* `committed` means record success locally and continue. If the committed payload fingerprint differs from the one this workflow requested, it is recorded as a conflict for review.
* `authoritatively_not_found` means the connector contract says a missing record proves the operation never committed. Only then is a retry allowed.
* `inconclusive` means the lookup could not prove anything, either because the connector does not offer read-your-writes lookups or because the lookup itself failed. The operation stays pending and is never retried on this evidence.

An ordinary "not found" is not treated as proof. Whether absence is authoritative is a property of each connector, modeled here as a flag on the synthetic system.

The system does not pretend that separate external systems form one atomic transaction.

## Attempt history

Each action attempt is versioned in the event log. A retry creates a new attempt rather than overwriting the previous outcome. Recovery decisions use the latest attempt, so an old unknown result cannot permanently block a workflow after reconciliation.

The event table is append-only. Triggers reject updates and deletes, and unique indexes allow one request and at most one terminal outcome (succeeded, failed or conflict) per attempt. Repeated inconclusive reconciliations are each recorded. Every event carries the worker ID, lease token, operation ID, payload fingerprint and a detail string, so the history shows which worker made each decision and on what evidence.

## Concurrent recovery

Two workers may try to recover the same workflow. Two checks run inside one `BEGIN IMMEDIATE` transaction on every append:

* **Lease.** A worker must hold an unexpired lease on the workflow. Each acquisition gets a higher token than the last. Releasing a lease expires it instead of deleting it, so a token is never issued twice. A worker whose lease expired or was taken over gets `LeaseLost`.
* **Version.** The worker passes the history version it based its decision on. If the history moved, the append fails with `VersionConflict`.

A crashed worker does not release its lease. The next worker can take over once the lease expires.

This fences the local history only. A worker that pauses after its lease check (a GC pause, a slow network call) can still reach the ERP after another worker has taken over, because the ERP does not see the lease token. In this lab that late write lands on the same operation ID and payload, so the ERP treats it as a replay, and the stale worker's attempt to record it is rejected. The same gap affects reconciliation: an authoritative "not found" describes the moment of the lookup, and a stale write still in flight can land afterwards. Retrying is safe here only because the retry reuses the operation ID and payload, so whichever write arrives second is a replay. A connector without idempotent operation IDs would need to accept and check a fencing token itself. Nothing here can stop that write.

## What is tested

The tests use real SQLite files and separate connections per simulated process. Lease expiry uses a manual clock instead of sleeping. They cover:

* normal completion, and an explicitly unknown external outcome
* a crash before the external write, after the external commit but before local acknowledgment, and during reconciliation, each followed by a restart on fresh connections
* retrying a failed notification without repeating the successful ERP action
* inconclusive reconciliation (lookup not authoritative, lookup unavailable) leaving the operation pending
* repeated identical requests, and the same operation ID with a different payload
* a payload that changes between attempts, or after the original already committed
* two threads racing for the same lease, and two workers racing to recover the same workflow
* expired leases, a stale owner after takeover, and reuse of a released lease
* a stale worker whose ERP call lands after another worker finished the workflow
* history writes based on a stale version, contradictory outcomes, and attempts to update or delete events

## Run

```bash
python -m pytest -q
python run_lab.py
```

## Why SQLite

SQLite is intentionally enough for this experiment. It gives us real persistence and process restart behavior without hiding the important recovery semantics behind a queue, ORM, cloud service, or distributed database.

## What this does not do

* Leases use the local wall clock. Workers on different machines would need bounded clock skew or a lease service that does not depend on it.
* Lease renewal is not implemented. An action that runs longer than the lease loses ownership, and its result is recovered through reconciliation instead.
* External systems never see the fencing token.
* Idempotency records are kept forever. Real providers often expire idempotency keys, which reopens the duplicate window for late retries.
* Conflicts and long-pending operations stop the workflow, but there is no review queue, alerting or retry limit.
* SQLite on one machine. The same checks would need serializable transactions or conditional writes in a shared database.

A production implementation would need stronger guarantees around transaction boundaries, connector behavior, concurrency, leases, observability, retention, and the exact idempotency contract of each external system.

The important design constraint is that **at least once execution is not the same thing as duplicate business effects**. The latter depends on stable operation identity and an external system that can safely recognize repeated operations.
