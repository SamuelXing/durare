# Recovery contract and implementation gaps

Baseline: `e0ed95aa0c66727ae3be2bb2a2ad9ce451be7471` (2026-09-26).
This is an implementation review, not a claim that proposed behavior ships.
The [development plan](../development-plan.md) tracks delivery separately.

## The problem in application code

This workflow is deterministic with respect to its input and step results:

```rust
use durare::{DurableContext, Error, ErrorCode, Result};

async fn workflow(ctx: DurableContext, _: ()) -> Result<()> {
    let error = ctx
        .step("request", || async { Err::<(), _>(Error::Timeout) })
        .await
        .unwrap_err();
    let next = match error.code() {
        ErrorCode::Timeout => "retry_later",
        _ => "reject",
    };
    ctx.step(next, || async { Ok(()) }).await
}
```

At the baseline, the first execution records `retry_later`. Replaying that
history requests `reject`: `record_failure` returns the original error, but
`StepOutcome::into_value_result` reconstructs a default-format failure as
`Error::App`. `verify_replay` reports a mismatch at position 1. The application
has not changed. Preserving an operation's failure requires more than preserving
its display message. This fix does not require changing the workflow signature.

A second failure has a different meaning:

```text
payment succeeds -> checkpoint write fails -> workflow returns the storage error
```

The engine has not established a durable business failure. At the baseline,
`run_to_completion` can nevertheless write `ERROR` through its generic error
branch. Changing the error codec alone cannot fix this recovery decision.

## Required behavior

These are requirements for the fixes, not their final public types or wire format.

1. **Recorded outcomes govern execution.** Under compatible code, recovery
   consumes each operation's recorded result when present. New work is allowed
   only where that operation has no result and the recovery protocol permits it.
   Concurrent histories can contain holes; the largest recorded position is not
   evidence that every earlier operation completed.
2. **Recordable failures have a stable business meaning.** The initial return
   and replay preserve the documented classification and payload. Both use the
   authoritative recorded outcome, including a concurrent writer's outcome or
   the existing ownership-conflict protocol. Returning the local error merely
   because some failure was stored is insufficient.
3. **Engine failures are not business outcomes.** Failure to read or commit the
   execution record must not be checkpointed as if a user operation had failed.
   Preserve a recoverable state or use an explicitly documented operator action;
   do not promise immediate, infinite retries for every storage failure.
4. **Effect repetition is explicit.** An external effect may succeed before its
   checkpoint commits. Recovery can repeat it. Only an applicable transaction or
   a downstream idempotency protocol closes that window.
5. **Illegal placement is explicit.** Preserve the existing leaf rule. Any new
   task-placement restriction must say where it is enforced and what it misses.
   Moving code into a helper alone should not change its durability.

## Failure boundary to implement

Classification depends on where a failure arose, not only its Rust variant.
A database error returned by application SQL inside a step is not automatically
the same event as a system-database checkpoint write failure. Likewise, an
operation timeout is distinct from a workflow deadline or a storage timeout.

| Event | Required handling | Required evidence |
| --- | --- | --- |
| A body exhausts its retry policy with a recordable failure | Persist a stable classification and permitted data; return that representation on initial execution and replay | A workflow branching on those fields issues the same subsequent operations |
| Reading or writing a checkpoint fails | Preserve the engine-failure origin; do not turn it into a durable application failure | Injection before commit and after an ambiguous commit, followed by recovery |
| Recording the workflow's terminal result fails | Do not claim successful durable completion; recovery reconciles the stored status | Retry/recovery serves the committed result, or resumes if none committed |
| Cancellation or a workflow deadline interrupts execution | Preserve the existing status/ownership rules while specifying returned errors separately | Cancel-before-commit, cancel-after-commit, and deadline tests |
| Another execution owns the checkpoint | Stop or adopt the recorded outcome according to the ownership protocol; never continue with a divergent local result | Existing ownership tests plus failure/failure races |
| A record cannot be decoded | Surface a decoding/compatibility problem; do not rerun the body or overwrite the record as a business failure | Malformed and unknown-version fixtures |

The implementation must account for a workflow catching an engine error. A
durable boundary must not lose that failure's origin just because user code
handles an ordinary `Result`. Decide and test whether the engine stops the run
or permits a specific reconciliation path before finalizing the control channel.

### Stable data and diagnostics

- Keep the non-generic `Error` API in scope. Introducing `Error<E>` is not a
  prerequisite.
- Enumerate the exact promised observations before implementing the codec:
  `ErrorCode`, supported variant fields, portable application data, and any
  database-classification helpers exposed across a recorded boundary.
- Do not fix the timeout example by degrading every initial error to `App`.
- Arbitrary driver objects, downcasts, source chains, and backtraces are not
  promised to round-trip. `app_source` already documents a live-only source.
  Workflow decisions must use the stable fields, not those diagnostics. Any
  normalization on the initial durable return must be documented explicitly.
- Retry predicates inside a live attempt can inspect the original error before
  final recording. Do not confuse that local decision with the failure observed
  by the workflow after the durable boundary.

### Compatibility work required before choosing bytes

The baseline stores bare messages in the default format and portable envelopes
in `portable_json`. Preserve reading both, including foreign SDK envelopes and
their application `name`, `code`, and `data`. Old messages cannot regain missing
classifications by guessing from their text.

Design a versioned representation with an unambiguous discriminator. Test
malformed records, unknown versions, and collision with user-supplied data.
Specify both directions: a new reader consuming old records, and what an old or
foreign reader sees when it consumes new records. Reading old rows is not proof
that mixed-version writers and readers are compatible. Document rollout limits
before release. Do not silently add a schema migration or overwrite user fields.

## Contract ledger

`Implemented` means a mechanism exists at the pinned baseline, not a universal
proof. `Documented rule` means application code must obey it. `Proposed` is not
shipping behavior. A failing review probe is evidence of a gap, not acceptance.

| Semantic rule | Status | Mechanism | Evidence and gap |
| --- | --- | --- | --- |
| Building a `PendingStep` claims its position before polling | Implemented | Synchronous claim | `step_position::awaiting_out_of_order_does_not_move_a_position`; a built-and-dropped call still spends its position |
| Native `transaction_on` claims its position before polling | Proposed | Interface decision pending | Baseline `transaction_on` is `async fn` and explicitly documents poll-time allocation; #216 changes it but is not merged |
| Call construction order is replayable | Documented rule | User orders construction before timing-dependent awaits | `determinism` guide; neither borrowing nor `join!` proves this property |
| Durable work cannot escape into a detached task | Proposed | Compare a borrowed capability with runtime placement checks | Baseline context is owned/cloneable; #216 compile fixtures cover ordinary `'static` escape only |
| Nested durable construction is refused without spending another position | Implemented | Body scope at callback invocation and each poll | `durable_call_nesting::a_step_inside_a_step_body_is_refused_and_claims_no_position` |
| A pending operation brought into another durable body is refused | Implemented | Poll-time body check | `durable_call_nesting::a_call_built_outside_and_polled_inside_a_body_is_refused`; its construction already spent a position |
| Legal sibling operations do not look nested while another body is parked | Implemented | Scope is not a shared lifetime flag | `durable_call_nesting::a_sibling_built_while_a_body_is_in_flight_is_allowed` |
| Recorded failure classification is stable | Proposed | Versioned failure representation and canonical return path | Timeout branch probe fails at position 1; audit all recording and reading paths |
| Checkpoint failures remain engine failures | Proposed | Origin-preserving recovery/control handling | Review-only provider injection currently produces terminal `ERROR` with no checkpoint |
| Ordinary external effects can repeat before commit | Documented rule | User idempotency; no atomic external checkpoint | `crash_consistency::{sqlite,pg}_crash_sweep_at_every_boundary`; these are in-process crash models |
| Application SQL and its durability witness commit atomically on supported transaction paths | Implemented | Same-database transaction or application-database witness protocol | `datasource::commits_writes_with_witness_row_exactly_once`, `completion_row_replays_without_rerunning_the_body`; does not cover arbitrary external effects |
| `select` records its winning index and value before returning success | Implemented | One checkpointed plain-work race | `concurrency::select_returns_first_to_complete`, `durable_call_nesting::a_plain_branch_replays_cleanly` |
| A `select` winner can change if its record did not commit | Documented rule | At-least-once body execution | Dedicated crash test still required; a chosen local winner is not yet a durable decision |
| `select` branches cannot create or poll durable operations | Implemented | Existing body guard | `durable_call_nesting::a_step_inside_a_select_branch_is_refused` plus the cross-body test |
| Losing branches may already have produced effects; dropping them does not undo effects or necessarily stop spawned work | Documented rule | User cancellation/idempotency protocol | Dedicated effect and detached-work tests required; no durable-race guarantee |
| Cancellation does not undo committed SQL or external effects | Documented rule | Transaction/receiver boundaries, not cancellation itself | Add explicit guide text and cancellation-window tests; no wired public StepCtx token on main |
| Stable body identity is available for idempotency | Proposed | StepCtx independent of context ownership | #216 prototype only; main `current_step_id()` exposes the next counter, not the executing body's identity |
| Keys distinguish operation occurrences and stay stable across attempts | Proposed | Specify workflow/operation identity and key encoding | Workflow ID plus step name alone collides for repeated same-named steps; no production key contract yet |
| Patch/fork behavior and subkeys have defined stability | Proposed | Explicit compatibility and fork policy | No acceptance evidence yet; copied history and newly executed fork work must be considered separately |
| Replay verification does not execute durable bodies or write history | Implemented | Verification gates | `replay_verifier::verification_writes_nothing_and_runs_nothing`; arbitrary workflow code outside durable calls still executes |
| Multi-position operations account only for records they actually own | Implemented | Wait accounting when polled | `replay_verifier::a_wait_still_in_flight_owns_its_recorded_deadline` and built/dropped-call tests; inventory every new multi-position operation |

## Evidence plan

Existing test names above refer to files under `tests/`. The timeout probe can
be reproduced by registering the example at the top, running it with an
`InMemoryProvider`, and calling `verify_replay` on its workflow ID. The checkpoint
probe requires a test-only provider fault before `record_step_result` stores its
row: return `Error::Db(sqlx::Error::PoolTimedOut)` once, then let subsequent
provider operations succeed. The observed terminal status is not a physical
Postgres-outage experiment. Both probes must become committed regression tests
with their fixes; neither is present on the pinned main.

Before a fix is accepted:

1. Observe its regression fail on the baseline for the intended reason.
2. Cover memory, SQLite, and Postgres where the protocol applies, including
   step, transaction, terminal workflow, and handle paths.
3. Cover the effect/record window, committed-but-unacknowledged writes,
   ownership races, and decoding failures where relevant.
4. Verify stable observations both directly and through subsequent workflow
   operations; trace matching alone misses data differences with the same trace.
5. Disable the relevant fix and confirm the regression fails again.

Specula guidance and TLA+ checks are separate work. Each model must identify its
assumptions, explored bounds, properties, counterexamples, and corresponding Rust
tests. No formal-model result is claimed by this document or by `verify_replay`.

## Guide corrections to carry with implementation

- `durability` currently says replay returns the same error; qualify the current
  limitation and replace it with the implemented stable-field contract.
- Its workflow-ID-plus-step-name idempotency advice needs an occurrence identity
  for repeated names. Do not promote that advice into StepCtx's key format.
- `determinism` describes placement refusal as occurring before the counter
  moves. Construction refusal does; a pending call refused when polled has
  already spent its position. Keep that distinction explicit.
- Retain the `transaction_on` exception until its actual interface changes.

These corrections belong beside the relevant user-facing behavior; this ledger
does not silently rewrite the public API or certify the existing broad wording.
