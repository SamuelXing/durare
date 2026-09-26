# Development plan

Reviewed baseline: `e0ed95aa0c66727ae3be2bb2a2ad9ce451be7471` (2026-09-26).
This plan separates implemented behavior, proposed work, and deferred choices.
Update a row when its code and evidence land, not when work merely starts.
The [recovery contract](design/recovery-contract.md) supplies the initial gap
ledger and acceptance requirements.

## Delivery rules

- Use one focused branch and PR per independently reviewable change. Separate
  behavior fixes from unrelated refactoring; do not merge the context prototype
  as a prerequisite for recovery work.
- Each API proposal starts with current user code, a concrete failure or cost,
  proposed code, alternatives, migration cost, and evidence. An abstract safety
  claim is not a sufficient reason for a breaking change.
- Observe regressions failing before fixes and use targeted mutation checks.
  Run applicable backend, toolchain, documentation, and CI checks. Documentation
  planning alone does not require inventing production tests.
- Review changes for reuse, unnecessary abstraction, efficiency, and whether
  they address the shared cause rather than one example.
- Keep code, documentation, commits, and PR descriptions in English. PR
  descriptions explain the broken behavior before implementation details.

## Existing foundation

| Work | Status | Keep or audit |
| --- | --- | --- |
| Construction-time positions (#210) | Merged | Keep position tests; native `transaction_on` remains a documented exception |
| Leaf and cross-body guards (#214) | Merged | Preserve refusal and legal sibling concurrency |
| Replay verifier (#215) | Merged | Preserve no-durable-work/write tests and multi-position accounting |
| Semver CI | Implemented | Run with the other five CI jobs; an allowed version bump is not proof of compatibility |
| Borrowed-context evaluation (#216) | Draft prototype | Retain as evidence; do not merge wholesale |

## Recovery correctness

| Work | Status | Dependencies and acceptance |
| --- | --- | --- |
| Contract and gap ledger | In review in this change | Pin source evidence, distinguish current guarantees from proposed fixes |
| Recorded-error semantics | Reproduced; not fixed | Define stable fields and versioned encoding; first return and replay agree; legacy/foreign records remain readable with explicit rollout limits |
| Infrastructure-failure recovery | Reproduced by provider injection; not fixed | Preserve failure origin; cover pre-commit and ambiguous commit failures without terminalizing them as business failures |
| Boundary audit | Pending | Check steps, both transaction protocols, workflow completion, handles, cancellation, and competing executions |

The error codec and engine-failure handling get separate PRs using one contract.
Neither requires borrowed context or generic `Error<E>`. Wire format and public
error variants must be specified in the implementation proposal rather than
implicitly selected by this planning document.

## Verification: model, implementation, and history

| Work | Status | Acceptance |
| --- | --- | --- |
| Specula guidance | Pending | State boundaries, persistent state, crash points, and properties tied to the ledger |
| Specula calibration | Pending | Test discovery of known position/context-escape problems; report prompts, misses, and limits |
| TLA+ models and checks | Pending | Separate identity/concurrency and checkpoint/recovery/error models; publish reproducible commands, bounds, assumptions, and unexplained counterexamples |
| Model-to-implementation checks | Pending | Turn applicable counterexamples into Rust regressions and target the enforcement with mutations |
| Real-history verification | Available; extend with fixes | Use `verify_replay` and direct value/error assertions; trace equality alone is insufficient |

Guidance and calibration can proceed while recovery fixes are developed. Models
of the new failure protocol depend on its contract. A model pass is not a proof
of arbitrary Rust code, and the replay verifier is not a model checker.

## API decisions and implementation

| Work | Status | Acceptance |
| --- | --- | --- |
| Owned context with runtime placement checks | Prototype pending | Compare the same spawn, pending-call, helper, join, native transaction, child, queue, and handle cases as #216 |
| Context ownership decision | Open | Show protection, bypass boundaries, real migration diffs, and costs; retain explicit context without assuming it must be borrowed |
| Selected context implementation | Pending decision | Small production PR(s), preserving the leaf guard; compile-time and runtime claims have separate evidence |
| Native transaction position consistency | Exception identified | Evaluate separately from borrowing; preserve repeatable retries and legitimate captures while resolving poll-time allocation |
| StepCtx | Prototype exists; production pending | Stable current identity and attempt metadata, macro ergonomics, receiver-enforced idempotency; does not depend on borrowing |
| Key stability | Contract pending | Repeated names, retries, patches, forks, and encoding are explicit; attempts do not change one operation's key; defer subkey helper until specified |
| Cooperative cancellation | Contract/wiring pending | Define signals and attempt/recovery lifetime; expose no inert token; cancellation never implies effect rollback |
| Plain-work select ergonomics | Pending | Compare `branch()`, tuples, and a macro on real call sites; reduce ceremony without changing persistence or loser semantics |

#216 demonstrates a narrow lifetime guarantee, but capturing registrations need
an adapter/boxing and a native transaction cannot retain the outer context across
await in one legitimate callback shape. That limitation is a cost, not another
safety success. Extract useful implementation/tests only after the relevant
production decision; a green prototype CI is not adoption approval.

## Readability and reviewability

After the correctness work, review error handling, replay, transactions, and
scope enforcement for clear control flow, duplicated logic, and misplaced
responsibility. Keep behavior changes separate from pure refactors. Organize
tests around observable behavior and recovery boundaries. Prefer a small diff
that explains itself over abstractions or file splits without a concrete benefit.

## Open issues and official SDK synchronization

Inventory every open issue against a pinned repository revision. Each needs a
reproduction or motivation, acceptance criteria, dependencies, and disposition:
implement, already resolved with evidence, defer with reason, or not applicable.
Inspect all issues; do not promise to implement every historical proposal as-is.

Use the official Python SDK as the main synchronization reference and other SDKs
for relevant protocol differences. Pin versions and compare DBOS schema, wire
formats, recovery behavior, and capabilities. Add compatibility fixtures and a
repeatable follow-up process. Preserve Rust-specific API choices where useful;
protocol compatibility does not require copying another language's surface.

Issue triage and source comparison can start before API work finishes. Implement
correctness and compatibility fixes before convenience features. Do not block a
release merely on making the open-issue count zero.

### Replay-verifier follow-ups retained from #215

- Assess hoisting the transaction replay consult shared by normal execution and
  verification; account for additional reads and cancellation behavior.
- Read operation positions/names without decoding unused outputs.
- Evaluate a write-refusing provider or capability so verification's read-only
  behavior depends less on distributed gate discipline.
- Correct Postgres-specific gating in adversity tests.

These are separate improvements unless investigation identifies a release-blocking
correctness defect. Register ownership/accounting rules for every multi-position
operation and test completed, incomplete, and built-but-unpolled histories.

## Performance and public comparison

Design benchmarks early; measure after the compared interfaces settle. Compare
representative official SDKs using pinned versions, equivalent persistence
guarantees, the same database/configuration, and reproducible workloads. Publish
commands, raw results, repeated runs, and limitations.

Measure throughput; p50/p95/p99 latency; CPU/memory at equal load; and replay and
recovery speed. Separate database-bound, external-I/O-bound, and SDK-overhead
workloads. Do not infer workflow performance from language microbenchmarks.

Use measured strengths in the README, whether speed, memory, or resource cost.
Include conditions beside numbers and link the full benchmark report. No blanket
"Rust is faster" claim is approved in advance of results.

## Documentation and release

- Maintain the contract ledger now, with proposed rows clearly labeled.
- Write `src/design.rs` for implemented user-facing behavior and an ADR under
  `docs/design/` for decisions and rejected alternatives.
- Explain DBOS schema compatibility and actual capabilities in the README;
  link source-pinned comparisons and measured performance when available.
- Provide a runnable migration example from 0.4.1 and a changelog describing
  position allocation, leaf refusal, and whichever API changes actually ship.
- Pass memory/SQLite/Postgres recovery checks, MSRV, feature matrix, formatting,
  Clippy, docs, and semver CI. Resolve confirmed correctness defects. Document
  model bounds and explain applicable counterexamples before claiming coverage.

Release 0.5.0 when those gates hold for its selected scope. Do not force borrowed
context into the release or make all issue cleanup and performance work release
prerequisites. Documentation must match implemented behavior.

## Deferred or excluded

- Generic `Error<E>`: not part of the current fixes; reconsider only for concrete
  typed-error needs or new evidence about common-path ergonomics.
- Durable race over `PendingStep`: a separate recovery protocol if needed.
- Typed manual workflow references, pending combinators, checkpointed handle
  results: later API work, not silently bundled into error handling.
- FFI experiments, broader cross-SDK matrices, broad mutation/coverage tooling:
  after the correctness priorities. Targeted mutation checks remain required.
- Specula as a public selling point: only after reproducible models and their
  implementation correspondence exist.
- Renaming `DurableContext`, async-std support, and the previously rejected FFI
  shape: excluded. The previous Issue 2 proposal remains unfiled.

## Working order

Contract -> recovery fixes and verification -> context decision and separate
StepCtx/select changes -> readability work -> issue implementation and SDK sync
-> performance reporting. Guidance, issue inventory, and benchmark design can
proceed alongside earlier work. Each production API choice gets its own concrete
review; approval of this plan does not merge #216.
