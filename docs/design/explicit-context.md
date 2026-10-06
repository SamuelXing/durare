# Explicit context

**Status:** accepted, 2026-10-06. In effect from durare 0.5.0.

A workflow in durare takes a `DurableContext` argument, and every durable
call is a method on it. This note records why, what the alternative was, and
what was set aside along the way. The user-facing statement of the result is
the `design` guide in the crate documentation; this is the derivation.

## The question the design has to answer

Durable execution replays a workflow function to recover it. Replay is sound
only if each durable call finds the record the first run wrote for it, which
means each call has to be matched to a stable position in the workflow's
history. The design question is where that position comes from, and what
happens to it when the code around the call changes.

Three concrete questions, about any one call:

1. On recovery, does it re-run or return its record?
2. If the effect happened but the record did not commit, can it repeat?
3. If the call moves — into a helper, a `join!` branch, a `select`, a spawned
   task — do the first two answers still hold?

The third is the design question. Code moves constantly. An API where moving
a line can silently change whether it is recorded makes every refactor a
correctness review, and the failure mode is the worst kind: a step that runs
twice, or returns a neighbour's result, with no error.

The principle adopted: **durable semantics survive code motion, or the SDK
refuses early and loudly.** Every rule in the `design` guide's contract table
is an application of it, and the table's "enforced by" column is the honest
accounting of how far the principle reaches — compiler, runtime check, or the
programmer's own discipline.

## Where the position comes from

Before 0.5.0 a durable call was an `async fn`, and it claimed its position
when first polled. Poll order is decided by the combinator, not the code:
`tokio::select!` randomises it, and awaiting two calls in the reverse of the
order they were written reversed their positions. Two same-named steps that
swapped positions replayed each other's output, silently. Measured on a real
crash-and-recover run, the recovered workflow returned the second step's value
for the first.

The fix was mechanical once seen: claim the position in the call's synchronous
prelude, so it follows source order, which a replay reproduces by
construction. The call became a plain `fn` returning a `PendingStep` future.
Call sites did not change. What did change is that building a call and
dropping it spends a position — deterministic, since a replay does the same,
but no longer a no-op, hence `#[must_use]`.

This settles question 3 for helpers, branches, and `join!`. It does not settle
it for a body nested in a body, or for a task.

## Bodies

A step's body does not run on a replay; the step is served from its record. So
a durable call made *inside* a body claimed a position on the first run that
no replay claims again, and every later call shifted onto it. Under a
different name this surfaced as `UnexpectedStep` somewhere unrelated; under
the same name nothing detected it. The `select` documentation asked callers
to keep this rule themselves, and nothing checked it.

Two designs were considered. **Degrade**: a nested call runs as a plain
function, unrecorded. **Refuse**: a nested call is an error at the call. The
first is what an ambient-context model naturally does (see below). durare
refuses, for two reasons. Refuse-then-allow is a relaxation that can ship
later without breaking anyone; allow-then-refuse is not. And a degraded call
is the quiet kind of change the principle rules out: the programmer wrote
`ctx.step` and got a function call.

The mechanism is a task-local marker set around a body's *poll*, not held for
its lifetime. A lifetime flag cannot tell a body that is running from one that
is parked at an await, and would refuse a sibling call the workflow makes
while a step waits. The marker is restored however the poll ends, including a
panic. Two checks follow from it: at construction (`NestedDurableCall`, no
position spent) and on every poll of a `PendingStep` (`DurableCallCrossedBody`,
because a call built outside and moved inside after its first poll has
already claimed). A call *carried into* a body is refused for now: allowing
it would let a losing `select` branch write a row, which falsifies `select`'s
"plain work, one checkpoint" contract, and the retry and cancellation
ownership between the two layers has no answer yet. That is a separate
proposal if anyone needs it.

## Tasks

A `DurableContext` moved into `tokio::spawn` shares the workflow's position
counter with a task whose timing is not part of the history. Forty identical
runs produced nine distinct position orderings. The call is refused at
construction and on every poll (`DurableCallOutsideExecution`), before any
position is claimed, because the context carries its execution's identity and
the spawned task is not that execution. Plain work inside a step, or a child
workflow, are the supported shapes.

This check is at runtime. A compile-time version exists and was prototyped:
make the context a borrow, `&DurableContext`, so a `'static` task cannot hold
it. That is deferred, not rejected. The runtime refusal already makes the
mistake loud; the compile-time upgrade costs every capturing registration
closure a `Box::pin` wrapper, returns `transaction_on` to the boxed-future
shape 0.4.x removed, and forbids holding the context across an await inside a
transaction callback. The evaluation measured a 94-file migration for one
hazard that is already caught. It can be revisited with evidence that runtime
refusal is not enough.

## The alternative: ambient context

The other way to supply a call's execution identity is ambient state — a
`tokio::task_local` the call looks up — so a workflow is a plain `async fn`
with no context parameter. DBOS's own Rust SDK takes this route. The two
models answer the three questions identically for inline calls, helpers and
`join!`. They diverge at the boundaries:

| Situation | Explicit (durare) | Ambient |
|---|---|---|
| Call inside a spawned task | Refused at the call. | The task-local is empty; the call runs as a plain function, unrecorded, and re-runs on every replay. |
| Call inside another body | Refused at the call. | Runs as a plain function, no position. |
| Call built in one scope, polled in another | Refused on poll (`DurableCallCrossedBody`). | Refused on poll (`StepBuiltElsewhere`). |

The ambient model's spawn behaviour is documented and tested there as the
intended default, on the reasoning that spawned work is not part of the
workflow. That is a coherent position. durare's is that the programmer wrote
a durable call and should get one or an error — the same sentence as the
principle. The cost is the parameter.

## Set aside

- **Generic error channel.** An `Error<E>` with a typed application variant
  was considered and kept off. The durable boundary serialises every error;
  a typed `E` has to round-trip through that, and the common path — a
  recorded failure replaying as the same failure — is served by the versioned
  envelope without it. Model expected business outcomes as values the step
  returns. Reopen if a user needs typed errors *across* the boundary.
- **Narrowing `select` to durable branches.** `select` stays a race over plain
  work recorded as one step. A durable race over `PendingStep`s would have a
  different contract (every branch recorded; losers' rows reconciled on
  replay) and would need a different name.
- **A step without its own identity.** A step body needs its own position to
  build an idempotency key, and it cannot get it by capturing the outer
  context — the outer counter has moved on. So `StepCtx` exists, carrying the
  position, the attempt, and `idempotency_key_for`, with the key defined as
  `(workflow_id, step_id)` and never the attempt. This did not require the
  borrowed context; it shipped on the owned one.

## Consequences

Every workflow and helper takes a `DurableContext`. Durable work does not
happen on spawned tasks. A nested durable call, a crossed body, or a
detached context is an error at the call with no position spent, and the
error is a recordable programming error that bypasses retry. A built call is
`#[must_use]`. One case remains the programmer's: two calls whose
construction order depends on an await between them. The contract table in
the `design` guide names it as documented-only rather than implying a
mechanism.
