# The durable-execution contract: what is promised, what enforces it, what proves it

This document is the working specification behind the 0.5.0 API changes. It
lists every promise durare makes about a durable call, states for each one
whether it is a documented rule the user keeps, a proposal, or something the
engine already enforces, and names the evidence. It is updated by every pull
request that changes a row. When the rows have settled it becomes a rustdoc
guide next to [`durability`](../../src/durability.rs),
[`determinism`](../../src/determinism.rs) and
[`transactions`](../../src/transactions.rs), which already cover the execution
model, the determinism rulebook, the at-least-once window and exactly-once
transactions. Nothing here repeats them; this is what they do not yet answer.

Baseline: `main` at 63cad0a (v0.4.1 released; the position-at-construction
change from #210 is on `main`, unreleased).

## The three questions

For a line like

```rust
let receipt = ctx.step("charge", || charge(order)).await?;
```

the user must be able to answer, from the documentation alone:

1. On recovery, does `charge` run again or return the recorded receipt?
2. If the charge succeeded but its record did not commit, can it run twice?
3. If this line is moved into a helper, a `join!` branch, a `select` branch,
   a step body or a spawned task, do the answers to 1 and 2 change?

The guides answer 1 and 2. The third is the reason durare keeps an explicit
context: **the durable semantics of a call should survive moving the code,
and where they cannot, the SDK should refuse early and say why.** That is a
goal with a known unenforced case (construction order that depends on
wake-up time, below), not an invariant, and the table says which is which.

## The contract table

Columns:

- **Status** — *Documented rule*: written down, the user keeps it, nothing
  checks. *Proposed*: agreed, not built. *Implemented*: built and on `main`.
- **Mechanism** — *compile*, *runtime*, or *user* (no mechanism).
- **Evidence** — the test that pins the row, or *none*. Doctests only cover
  examples; recovery and concurrency rows link integration tests.

### Identity: which position a call occupies

| Semantic | Status | Mechanism | Evidence |
|---|---|---|---|
| A durable call claims its position where it is written, not where it is first polled. | Implemented (#210) | runtime, at construction | `tests/step_position.rs`: `awaiting_out_of_order_does_not_move_a_position`, `a_randomised_poll_order_does_not_move_a_position`, `the_rule_holds_across_the_kinds_of_call` |
| A call that is built and dropped without being awaited has still spent its position. | Implemented | runtime; `PendingStep` is `#[must_use]` | `a_built_call_that_is_dropped_has_spent_its_position` |
| A call refused before it runs spends no position. | Implemented for the nested-transaction refusal; Proposed for every refusal the nesting guard adds | runtime | `a_refused_nested_transaction_leaves_the_counter_alone`; guard refusals: none yet |
| A recorded position is served only to a call of the same name; a different name is `Error::UnexpectedStep`, never a wrong value. | Implemented | runtime | asserted in `tests/sqlite.rs`, `tests/postgres.rs` |
| A durable call cannot be **created** inside another call's body (nested step). Today it silently shifts every later position. | Proposed: refuse at the call | runtime, construction-time check in every durable entry | none yet; the refusal test and the sibling-concurrency test are part of the guard PR |
| A durable call cannot be **created** inside a `select` branch. Today the position it claims is never claimed on replay and the next call collides with it: a differently named call fails with `UnexpectedStep` on code that did not change; a same-named call silently returns the inner call's value. | Documented rule → Proposed: refuse at the call | runtime, same check | `tests/select_branch_desync.rs` on branch `test/select-branch-desync` pins today's behaviour (both outcomes, plus a plain-branch control); the guard PR flips the two defect assertions |
| A `PendingStep` built outside a body cannot be **executed** inside one (carried in). Position alignment alone does not make this safe: an outer retry cannot re-await a consumed inner; cancel and timeout ownership is undefined; a carried-in `select` branch that loses may already have written its row, which would falsify select's contract below. | Proposed: refuse at the poll | runtime, poll-time check on **every** poll (`PendingStep` is `Unpin` and can be moved after a first poll); the error names where the call was built and where it was polled | none yet |
| The check marker covers only the user body's poll, never the call's own replay lookup or checkpoint write, and is restored on every exit from the poll: `Ready`, `Pending`, `?`, cancellation by drop, panic. | Proposed | runtime, scope guard | none yet; tests must cover all five exits and the legal case of a sibling call built in the workflow body while another body is mid-flight |
| Orchestration capability cannot enter a detached task (`tokio::spawn`, `spawn_local`, `spawn_blocking`). Today a cloned context in a spawned task shares the counter and interleaves positions unpredictably. | Documented rule (`determinism` guide, spawn row) → Proposed: compile error | compile: the context is borrowed and not `Clone`, every durable future is bound to that borrow | compile-fail tests (prototype in progress); the model was proven outside the crate with nine `cargo check` cases |
| Construction order that depends on wake-up time is not reproducible: a call built after an `.await` inside a `join!` branch, or reached by a `FuturesUnordered` loop, takes whichever position the timing gives it. | Documented rule | user | none; no mechanism exists in any DBOS SDK. Recording the order (as `select` records its winner) is the only known fix and is not planned for 0.5.0 |
| `join!` over durable calls is deterministic **only when each branch builds its durable calls before its first `.await`**. `tokio::join!` rotates its starting branch each poll, so a call built later in a branch is ordered by wake-up. The `determinism` guide's `join!` row lacks this precondition. | Proposed: doc fix | user | none |

### Replay and recovery

| Semantic | Status | Mechanism | Evidence |
|---|---|---|---|
| A recorded success is served without running the body; a recorded failure is served as the same error. | Implemented | runtime | `tests/crash_consistency.rs`, `tests/durability.rs` |
| The at-least-once window is exactly one step wide: a side effect can repeat only if a crash landed between it and its record. | Implemented | runtime | `tests/crash_consistency.rs` (seven crash boundaries, exact execution counts) |
| A transactional step is exactly-once; a separate application database keeps that with the two-commit witness protocol. | Implemented | runtime | `tests/transaction.rs`, `tests/datasource.rs` |
| Cancellation is a request to stop, not an undo. A cancelled workflow's completed effects stand. Today cancellation is observed only when a fresh attempt begins (a status check); a running body cannot see it. | Proposed: document; give bodies a signal (see `StepCtx`) | user today; runtime signal proposed | none; `durability` guide never mentions cancellation |
| A nested call's own retry policy and timeout never apply, because nesting is refused. | Follows from the refusal | runtime | covered by the refusal test |

### `select`: a race of plain work, one checkpoint

`ctx.select` records `(index, value)` as one step. Its documentation today
says only that the winner is recorded, that branches are not polled on replay,
and that branches must not contain durable calls. It is a step whose body is
a race, so the step contract applies to it in full:

| Semantic | Status | Mechanism | Evidence |
|---|---|---|---|
| Once the select's row is recorded, replay returns it without polling any branch. | Implemented | runtime | none linked; add |
| If the process crashes after a branch completed but before the row is written, the whole race re-runs on recovery, and a different branch may win. | Documented rule → Proposed: state it in `select`'s docs | user | none; a crash-here test belongs to the replay verifier |
| A losing branch may have produced side effects before it was dropped. Dropping it cancels the future, not the effects. | Proposed: document | user | none |
| Dropping a branch does not stop tasks the branch spawned. | Proposed: document | user | none |
| The at-least-once window applies to the winner's effect exactly as to any step. | Proposed: document in `select`, not only in the guide | user | none |
| Branches contain plain work only: no durable call is created in a branch (checked at construction) and no `PendingStep` is executed in one (checked at poll). | Proposed | runtime | guard PR |

A durable race over `PendingStep`s, where each branch is itself recorded, is
a different operation with a different contract. If it is ever built it gets
a different name.

### What a step body can observe

| Semantic | Status | Mechanism | Evidence |
|---|---|---|---|
| A body can learn its own stable position (`step_id`), the current attempt, the attempt limit, and whether it should stop. The Python, Go and official Rust SDKs expose these; durare exposes none — `current_step_id()` is the *next* position, not the body's own. | Proposed: `StepCtx` as the body's parameter, single entry `ctx.step("x", \|step\| async move { .. })` | API | none |
| The body signature is settled by the borrowed-context prototype: a closure returning an async block and an async closure borrowing its argument have different bounds; whichever type-checks with `step_with`'s retry loop and is `Send` on the multi-thread runtime wins. | Proposed | — | prototype report |
| `idempotency_key()` identifies the **logical operation**: `(workflow_id, step_id)`, never the attempt, in an unambiguous encoding. It is stable across retries and recovery for the life of one execution under version gating. It is **not** an exactly-once guarantee for an external service; it is the input to that service's own deduplication. | Proposed | API + docs | none |
| Open before `idempotency_key_for(label)` ships: the sub-key encoding; a cross-deployment namespace (two applications sharing one downstream account can collide on workflow ids); behaviour when a workflow id is reused (`start` with an existing id returns the existing run, so the key is stable); behaviour after `fork_workflow` (a fork gets a **new** workflow id, so positions re-executed after the fork point carry new keys and copied positions are never re-run). | Open | — | to be written as rows |

### Errors and business outcomes

| Semantic | Status | Mechanism | Evidence |
|---|---|---|---|
| A checkpointed application error keeps its message and loses its source chain; a replay returns the message. | Implemented, documented on `Error::App` | runtime | none linked; add |
| A business outcome the workflow branches on (declined, out of stock) is modelled as a serializable **return value**, not as an error, so it is recorded and replayed with its structure. | Documented rule | user | none |
| A generic `Error<E>` is not planned. The 2026-07 review rejected it for inference ambiguity; the official Rust SDK's own tests carry the cost it predicted (`Ok::<_, dbos::Error>` 28 times). Revisit only if a user needs typed errors across the boundary or a default type parameter is shown to spare the common path. | Decided | — | — |

## Crash points for the replay verifier

Each row above that says "crash-here" or "recovery" resolves to one of these.
The verifier runs the workflow, kills it at the point, recovers, and asserts
positions, names, values and execution counts.

| # | Crash after… | …and before | Expected on recovery |
|---|---|---|---|
| C1 | a step's side effect | its row | the step runs again (at-least-once); later positions unchanged |
| C2 | a `select` winner's effect | the select row | the race re-runs; any branch may win; positions after the select unchanged |
| C3 | the witness row of a two-commit transaction | the system checkpoint | the body does not run again; the checkpoint is completed from the witness |
| C4 | a call claimed its position | it was polled | nothing was written; the replay claims the same position for the same call |
| C5 | a refused call (nested, in-branch, carried-in) | anything | the refusal is deterministic and the counter shows the same positions on both runs |
| C6 | one `join!` branch's row | the other branch's row | the recorded branch is served, the other runs, positions match the source order |
| C7 | a workflow was dispatched for recovery | its first new record | a second recovery of the same id does not double-execute |

## What the guides already answer

Do not re-derive these; link them.

- Execution model, the three consequences of replay, `UnexpectedStep` as the
  backstop — `durability` guide.
- Crash recovery, executor ownership, `recover_on_launch` — `durability`.
- The at-least-once window and the two closed cases (transactions, messages)
  — `durability`.
- Version gating, patching, forking — `durability`.
- The non-determinism catalogue: clocks, randomness, `HashMap` order, spawn,
  `tokio::select!`, `FuturesUnordered`, environment, `Drop` — `determinism`.
- Durable-safe data and the portable format — `determinism`.
- Where dependencies live — `determinism`.
- Exactly-once transactions, failure semantics, conflict retries, the
  two-commit protocol for a separate database — `transactions`.
