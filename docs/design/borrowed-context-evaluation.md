# Borrowed workflow context: integrated prototype review

Date: 2026-09-26. Decision status: prototype for review, not a release approval.

## Scope and baseline

This branch reapplies `proto/borrowed-context` (`b2f7fc4`, `c14159c`) onto
`origin/main` at `e0ed95a`, including the nesting guard (#214) and replay verifier
(#215). It exercises the real registry, erasure, macros, engine dispatch, child
workflows, transactions, and test suite. It does not change the checkpoint schema.

The official SDK comparison below is against the local `dbos-transact-rust`
checkout at `e70f7f75d0bfb5cb884437c986fd0f166024e98e`, not a claim about every
subsequent official release. Source locations are in `crates/dbos/src/`.

## Conclusion

Borrowing works in the integrated SDK and provides a specific useful guarantee:
safe Rust cannot hand the current workflow's borrowed context, or a durable
future borrowing it, to `tokio::spawn` or `spawn_local`, which require `'static`.
Removing `Clone` alone would not provide this guarantee.

That result does not justify declaring the whole redesign superior to the
official SDK, nor automatically shipping this branch. Capturing registration
closures need an adapter and boxing. Native transaction callbacks lose their
plain async-closure syntax. One legitimate transaction capture pattern still
fails: holding the outer context across an await in the callback's returned
future. Copying the metadata before returning the future works. These are costs,
not additional safety wins. In particular, this does **not** satisfy the earlier
handoff's stronger acceptance criterion that a callback can retain both the
outer context and its connection across await. The boxed interface fixes `Send`
and preserves retries, but only partially resolves that blocker.

Recommendation: retain explicit context as the durable capability boundary,
but treat adoption of **borrowed** context as a separate decision. Accept B1 only
if preventing ordinary detached-task escape is worth the callback migration for
the intended users. Do not require B1 in order to deliver step identity and
attempt metadata: `StepCtx` can be implemented with the existing owned context.
The prototype is sufficient to make that choice without further framework work.

## What changed

- The engine owns one context per run; workflow handlers borrow it. The public
  context has neither `Clone` nor a public constructor/provider escape hatch.
- Named async functions register directly through `WorkflowHandler<'a, I, O>`.
  Capturing closures use `workflow_fn` and `Box::pin`.
- Steps receive `StepCtx` by value. Its attempt is zero-based; `max_attempts`
  is a `u64` to represent `u32::MAX` retries plus the first attempt safely.
- The idempotency key is `durare:v1:` followed by a JSON pair of workflow ID and
  step position. Attempts are excluded. Different workflow IDs (including forks)
  get different keys. Compatible code and a deduplicating receiver are still
  necessary. This format is a prototype proposal, not an existing wire promise.
- Native transactions use repeatable `Fn` callbacks returning boxed `Send`
  futures. They return `PendingStep` and claim their position at construction.
  Both main and the inherited prototype documented poll-time allocation as an
  exception for native transactions. Boxing lets this prototype remove that
  exception; it was not a regression newly introduced by the old prototype.
- #214's synchronous-body and per-poll guards and #215's poll-time accounting of
  reserved wait positions remain in place.
- The inert cancellation token from the old prototype was removed. No cooperative
  cancellation API is claimed. Wiring cancellation remains separate engine work.
- The step macro discards the metadata argument with `_`, rather than `_step`,
  which could shadow a user's argument. A regression test covers this case.

## Comparison of the relevant guarantee

| Operation | Official at the pinned revision | This prototype |
| --- | --- | --- |
| Create and await a step in the workflow | Checkpointed | Checkpointed |
| Move creation and execution into a new Tokio task | No inherited task-local workflow context; step runs plainly | Borrowed workflow capability cannot satisfy `'static` |
| Build a step in the workflow, then poll it elsewhere | Placement checked on every poll; `StepBuiltElsewhere` | Detached task rejected at compile time; nested body crossing still checked at runtime |
| Construct an inner step in a step body | Plain nested call | Runtime refusal before position allocation |
| Concurrent in-scope helpers and child starts | Supported mechanisms | `join!`, borrowed helpers, and child starts exercised |
| Construct operations after nondeterministic wakeups | Borrowing does not solve this issue | Not prevented; ordering remains the user's responsibility |

Official evidence: `checkpoint.rs`, `StepPlacement::here` and `check_here`
(`Outside` plus no current context yields `StepDurability::Plain`); `step.rs`,
`a_step_outside_a_workflow_runs_plainly`, `a_nested_step_is_a_plain_call_and_allocates_no_id`,
and `a_step_polled_once_is_refused_when_it_moves`. This was source inspection in
this evaluation, not a new execution of the official SDK's test suite.

The official design supports calling the same step-shaped function outside a
workflow. Its fallback is an intentional semantic choice with a relocation
hazard, not evidence that its whole SDK is defective. Its existing runtime
placement checks must not be omitted from the comparison.

## Cost visible to users

Named functions remain straightforward:

```rust
async fn workflow(ctx: &DurableContext, input: Input) -> Result<Output> {
    helper(ctx, input).await
}
engine.register("workflow", workflow);
```

A closure that captures a service needs scaffolding:

```rust
engine.register("workflow", workflow_fn(move |ctx, input: Input| {
    let service = service.clone();
    Box::pin(async move {
        ctx.step("request", |_| async move { service.request(input).await }).await
    })
}));
```

A retryable native transaction needs its captures cloned per invocation:

```rust
ctx.transaction_on(&ds, "insert", move |conn| {
    let item = item.clone();
    Box::pin(async move {
        sqlx::query("INSERT INTO orders(item) VALUES (?)")
            .bind(item).execute(&mut *conn).await?;
        Ok(())
    })
}).await?;
```

For workflow metadata inside that callback, read `ctx.workflow_id().to_owned()`
before `Box::pin`, then move the resulting string into the future. Holding `ctx`
in the future is rejected by the callback's higher-ranked lifetime bound. The
`transaction_metadata_borrow` fixture documents this **usability limitation**.
Allowing that capture would require another interface design, such as supplying
a context argument with a lifetime tied to the connection; that design is not
silently introduced here.

The repository migration spans dozens of files, mostly closure registration and
step-call syntax. This is real migration work even though the execution engine
change is much smaller. Boxed closure handlers also add an inner allocation to
the existing erased handler wrapper. No performance benchmark was run.

## Evidence and limits

| Claim | Evidence | Limit |
| --- | --- | --- |
| A workflow borrow cannot enter a detached Tokio task | `spawn_async_move_step`, `spawn_local`, `clone_reference_spawn` compile-fail fixtures | Applies to the borrowed capability and APIs requiring `'static` |
| A pending step cannot escape that way either | `spawn_pending_step` compile-fail fixture | Borrow lifetime, not task identity, is enforced |
| Portable transactions cannot capture that borrow | `nested_transaction`, `step_in_transaction` compile-fail fixtures | Their `'static` callback bound also restricts legitimate captures |
| Ordinary in-scope concurrency and dependencies work | `borrowed_interface`, `compile_pass/join_helper_child`, existing child/queue/macro tests | Does not prove all application callback shapes |
| Borrowing is not a general concurrency proof | `compile_pass/scoped_thread_limit` | Scoped threads can borrow; no task-local propagation or deterministic scheduling is guaranteed |
| Native transaction retries remain repeatable | `datasource::retry_policy_reruns_on_fresh_transactions`, existing Postgres conflict tests | No `FnOnce` substitution |
| Native transaction positions follow construction | `datasource::native_transaction_claims_before_poll` | Test observed failure before the fix |
| Sync metadata reads and nested refusal coexist | `datasource::native_transaction_copies_metadata_and_refuses_nested_creation` | Metadata is copied before the returned future |
| Step metadata does not shadow user parameters | `step_macro_shapes::step_metadata_does_not_shadow_a_user_argument` | Test observed compilation failure before the fix |
| Attempt metadata and keys agree across retries | `borrowed_interface::step_metadata_tracks_attempts_without_changing_the_key` | A receiver must enforce idempotency |
| Keys remain the same in the effect/checkpoint crash window | Existing seven-boundary `crash_consistency` sweep, augmented to compare keys | In-process parked-task crash model; no claim of exactly-once external effects |
| Nested and replay behavior survived migration | `durable_call_nesting`, `step_position`, `replay_verifier` | Runtime guards and compatibility rules are still necessary |

The `#[step]` macro currently ignores `StepCtx`; callers needing attempt metadata
or a key use `ctx.step` directly. Full macro access and cancellation are not
finished release features. This prototype does not implement durable race,
stronger error typing, replay-safe arbitrary scheduling, or external exactly-once
side effects. None follows from a lifetime bound.

## Validation record

- Stable Rust 1.98: the complete unit/integration run passed 435 tests, including
  live Postgres, SQLite, both seven-boundary crash sweeps, and replay verification.
  The two new borrowed-interface tests and the new macro-shadowing regression
  were then run successfully on the final code.
- The final trybuild run, without snapshot-overwrite mode, passed seven rejection
  fixtures and two accepted fixtures. Diagnostics were inspected: the rejected
  cases fail for lifetime escape, including the separately labeled transaction
  usability limitation, rather than incidental missing APIs.
- Rust 1.88: all targets/features compiled; 47 focused integration tests plus
  both macro-shape tests passed. All 52 doctests passed, with four pre-existing
  ignored examples. The initial full run exposed one stale determinism example;
  it was migrated before the passing doctest run.
- Stable all-target/all-feature clippy with warnings denied and formatting passed.
  Documentation built on 1.88 with rustdoc warnings denied. Postgres-only and
  SQLite-only library/example clippy passed; the zero-backend build failed with
  the intended backend-required diagnostic.
- The native transaction construction test and macro-shadowing test were observed
  failing before their fixes. Mutating the key to include the attempt caused the
  key-stability test to fail; the mutation was restored and the test passed.
- Semver tooling recognizes `0.4.1 -> 0.5.0` as permitting a breaking change:
  zero compatibility checks ran and 254 were skipped. This is **not** evidence of
  source compatibility.

Database tests used a dedicated local Postgres database. The assessment does not
claim a hosted CI run, a performance result, or execution of the official SDK's
test suite.
