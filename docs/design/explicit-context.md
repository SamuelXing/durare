# Explicit context

**Status:** accepted for durare 0.5.0.

Workflows take a `DurableContext` argument. Helpers that perform durable work
receive that context from the workflow. This note explains the choice and the
replay rules it supports. The crate's `design` guide lists the guarantees,
checks, and tests.

## Call positions

Recovery re-runs a workflow function. Each durable operation must find the
same position in the history that it used on the first run. Otherwise it can
read another operation's result or repeat an effect that was already recorded.

Before 0.5.0, steps claimed positions when first polled. Awaiting two calls in
reverse order reversed their positions, and `tokio::select!` could change them
through randomized poll order. A crash-and-recover test with two same-named
steps reproduced the failure: the first call returned the second call's value.

Most durable calls now return `PendingStep` and reserve a position during
construction. Poll order does not affect that position. Dropping a successfully
built call leaves its position spent, which is why `PendingStep` is `#[must_use]`.

Patches are an exception. `patch` and `deprecate_patch` inspect recorded history
before deciding whether to consume a position, so they allocate when polled.
Await them sequentially, before constructing subsequent durable calls.

Construction order must also be reproducible. Prebuilt calls work with `join!`.
Calls constructed after an await in concurrent branches can still be ordered
by timing. The SDK does not check that case; use child workflows when each
branch needs its own sequence of durable operations.

These rules preserve replay when a step moves into a helper or a concurrent
block with a fixed construction order. Moving it into another operation's body
or a spawned task requires additional checks.

## Operation bodies

A recorded step's body is skipped on replay. A durable call inside that body
would consume a position on the first run that replay never consumes. Later
operations would then read the wrong positions. Different operation names can
expose this as `UnexpectedStep`; identical names can hide it.

We considered running nested steps as plain functions, without checkpoints.
We chose to reject them because a `ctx.step` call should not lose its checkpoint
when moved into another body. Rejecting this placement also leaves room to
support it later with a defined replay contract.

A task-local marker identifies body execution during each poll. It is restored
when the poll returns or panics. A flag held for the body's entire lifetime
would incorrectly reject sibling calls while the body was waiting at an await.

For `PendingStep`, construction inside a body returns `NestedDurableCall`
without claiming a position. Polling a prebuilt call inside a body returns
`DurableCallCrossedBody`; the position reserved at construction remains spent.
The poll check runs every time, including after a future has already yielded.
Async patch calls also check placement when polled.

Prebuilt calls carried into bodies are rejected too. Allowing them inside a
`select` branch would let a losing branch write a separate checkpoint, although
`select` records the race as one operation. Supporting that would require rules
for replaying losers and assigning retry and cancellation ownership.

## Spawned tasks

A cloned `DurableContext` shares its workflow's position counter. If a spawned
task could use it, the order of positions would depend on task scheduling.
A prototype without the check produced nine position orderings in forty runs.

The context carries an execution identity, checked against a task-local scope
around workflow construction and polling. A spawned task does not inherit
that scope. Constructing a `PendingStep` there returns
`DurableCallOutsideExecution` without claiming a position. Polling a call moved
there also fails, but retains any position it already reserved. Calls check
placement on every poll, before polling their body or provider work; work done
on an earlier valid poll is not undone.

For concurrent durable work, use prebuilt calls or child workflows. Plain
spawned work inside a step remains allowed.

We also prototyped a borrowed context, `&DurableContext`, to prevent a `'static`
task from capturing it. That change is deferred. The prototype required
`Box::pin` wrappers for capturing registration closures, restored the boxed
future form of `transaction_on`, and prevented holding the context across an
await in a transaction callback. The evaluated migration touched 94 files.
The runtime check already catches the placement error, so those API changes
need a stronger justification.

## Comparison with DBOS's Rust SDK

DBOS's Rust SDK supplies context through task-local state, so workflow functions
need no context parameter. For steps called inline, through helpers, or as
prebuilt `join!` arguments, both SDKs reserve positions at construction.
Their step placement rules differ:

| Step placement | durare | DBOS Rust SDK |
|---|---|---|
| Constructed inside a spawned task | Rejected without claiming a position. | Runs without a checkpoint. It repeats if the code spawning it runs again. |
| Constructed inside another step's body | Rejected without claiming a position. | Runs as plain work within the enclosing body. |
| Constructed in the workflow body, then polled inside a step body | `DurableCallCrossedBody`; the reserved position stays spent. | `StepBuiltElsewhere`. |
| Constructed in one execution, then polled in another | `DurableCallOutsideExecution`. | `StepBuiltElsewhere`. |

This comparison is specific to step calls, verified against upstream commit
[`7937413`](https://github.com/dbos-inc/dbos-transact-rust/tree/7937413).
Other operations have different rules: upstream `set_event` and `recv`, for
example, reject calls outside a workflow or inside a step. Running a step
without a checkpoint is an SDK policy, not a requirement of task-local context.

The practical tradeoff is a context parameter for workflows and durable helpers
in exchange for rejecting step placements that would silently lose a checkpoint.
Both models still require deterministic workflow control flow.

## Other decisions

- **Typed application errors.** A generic `Error<E>` was considered. Errors
  crossing a durable boundary must serialize and replay with the same
  classification; the versioned error envelope handles that today. Expected
  business outcomes can be modeled as return values. A typed error channel
  remains an option if users need it across that boundary.
- **Racing durable branches.** `select` races plain futures and records one
  winner. Racing `PendingStep` calls would require separate branch checkpoints
  and a replay policy for losing branches. That would be a separate operation.
- **Step identity.** `StepCtx` exposes the step's reserved position and attempt.
  Capturing the outer context cannot supply that position because its counter
  may already have advanced. `idempotency_key_for` derives a key from the
  workflow id, step id, and effect label. The attempt is excluded so retries
  reuse the key.

## Caller rules

Placement errors are recordable programming errors and bypass retry. A refused
constructor spends no position; a refusal while polling does not release an
already reserved position. These checks prevent nested and detached calls,
but do not validate all workflow ordering. Keep construction order fixed,
await patches sequentially, and record timing-dependent choices before using
them to decide later durable work. See the `determinism` guide for examples.
