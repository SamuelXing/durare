# Execution layers

durare has two orchestration layers that are easy to conflate because both are
about "running workflows". They answer different questions, hold different
invariants, and meet at one narrow seam. Most of the correctness bugs fixed in
2026 were a decision made on the wrong side of that seam. This note names the
layers, the seam, and the rule for deciding which side a new behavior belongs to.

## The two layers

| | Workflow orchestration | Runtime orchestration |
|---|---|---|
| Question | What does this workflow do, and what did it decide last time? | Which process runs this execution, when, and what happens if it dies? |
| Vocabulary | steps, children, send/recv, events, sleep, positions, recorded outcomes, replay | tasks, ownership claims, queues, dispatchers, settlement, cancellation, providers |
| Invariant | a replay makes exactly the decisions the original run made | at most one live execution per workflow id, across all processes |
| Its world | the workflow's own code and input, plus what the database records | this process: its registry, queue configuration, version, connections |
| Code | `context.rs` (`DurableContext`, `PendingStep`, `Position`), `determinism` | `engine.rs` (`DurableEngine`, `Runtime`, dispatch, recovery, settlement), `queue.rs`, `schedule.rs`, `debounce.rs`, the providers |

The workflow layer never learns which process it runs on. The runtime layer
never learns what a workflow means; to it every execution is a task to be
finished exactly once.

## The seam

`execution.rs` is the only place the two layers touch. It owns two things.

**The durable-call protocol.** Every durable call claims a position, checks the
record at that position, runs only if nothing is recorded, then records. The
runtime guarantees the record is served before anything re-executes; the
workflow layer guarantees it claims positions in a repeatable order. Body-side
concurrency (`join!`, `select!`, `FuturesUnordered`) cannot disturb this because
positions are claimed at construction and durable calls are refused outside the
execution's task; [explicit-context.md](explicit-context.md) covers those rules.

**The control-error channel.** Two errors cross the seam upward, from runtime to
workflow, and neither is an outcome:

- `Error::RecoveryRequired` — this process cannot continue this execution.
  Nothing is recorded, the row stays `PENDING`, and the body's later durable
  calls are refused by the execution latch (`Execution::interrupt`,
  `Execution::check`). Settlement re-claims the row with backoff (100 ms doubling,
  capped at 5 s) up to the recovery-attempt cap, then parks it.
- `Error::ObservationFailed` — the outcome of an operation could not be
  observed. The execution stops without scheduling recovery, because a lost
  response is not proof the operation did not happen.

`Execution::record` classifies provider results into these channels
(`provider_error` → `recovery_error`); `Execution::interrupt` is the entry point
for the engine's own refusals. A body that catches a control error cannot branch
on it: its next durable call is refused.

## Which side does a new behavior belong to?

Ask what the result depends on.

| It depends on… | Layer | What to do with the result | Examples |
|---|---|---|---|
| the call's own arguments | workflow | refuse before claiming a position (deterministic: a replay refuses identically) | input fails to serialize |
| the call's arguments, in a way 0.5.0 already charged a position for | workflow | refuse after claiming; record nothing | empty name or queue, invalid deduplication options, delay without a queue |
| a fact observed through the database | workflow | record it as the step's outcome; replay serves it | `Reject` deduplication collision, `WorkflowTargetConflict`, a step's return value |
| this process's configuration or registry | runtime | interrupt with `RecoveryRequired`; record nothing | unregistered inline target or queue, remote target without a version, unsafe same-version routing |
| whether storage answered | runtime | `ObservationFailed`; no recovery | lost connection during a checkpoint write |

The database is the workflow layer's world; the process is the runtime's. The
test is mechanical: if a correctly configured process would get a different
result from the same call with the same history, the result is not the
workflow's to record.

## Runtime invariants, and how they are kept

- **One live execution per id.** Direct start uses the insert's created flag
  as the arbiter; dequeue flips `ENQUEUED → PENDING` with a conditional
  update; recovery claims with a compare-and-set on the row's executor and
  attempt count (`claim_for_recovery`). A loser observes through a polling
  handle and never runs the body.
- **One recorded outcome.** A terminal status is written only from `PENDING`;
  a late writer's update is a no-op and it adopts the recorded result.
- **One checkpoint per position.** `operation_outputs` is unique on
  `(workflow_uuid, function_id)`; a second writer at the same position gets
  `WorkflowConflict` and stops.
- **Recovered runs are dispatched, never run inline.** `recover()` spawns each
  run on the engine's task tracker and returns; a run parked on `recv` stalls
  nothing but itself.

## History

- #162 — recovery ran executions inline inside `recover()`: a runtime
  scheduling choice broke a workflow-level semantic (construction deadlocked on
  a parked `recv`).
- #163 — two processes could run one id: the runtime did not enforce the
  workflow layer's identity invariant.
- #210, #214, #220 — body concurrency reached the position counter: positions
  are now claimed at construction, scoped per poll, and refused in spawned tasks.
- #239 → #243 — "queue not registered" moved between record, catchable error,
  and interrupt over seven review rounds before the classification above was
  written down. `validate_child_arguments` / `validate_child_configuration` is
  that classification in code.
