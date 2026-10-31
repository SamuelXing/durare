# AGENTS.md

durare is a Rust durable-workflow SDK compatible with the DBOS system schema.
A workflow checkpoints every durable call to a `StateProvider` (in-memory,
SQLite, Postgres) and replays from those checkpoints after a crash. The
correctness property behind every change: a replay makes exactly the decisions
the original run made.

## Commands

CI runs these four gates; all must pass locally before a PR is opened:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features && cargo test --doc --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
```

Postgres tests are skipped unless `DATABASE_URL` is set. Use a throwaway
container on a non-default port so no other database is ever touched:

```bash
docker run -d --rm --name durare-pg -e POSTGRES_PASSWORD=durare -e POSTGRES_DB=durare_test -p 54329:5432 postgres:16
DATABASE_URL=postgres://postgres:durare@localhost:54329/durare_test cargo test --all-features --no-fail-fast
docker rm -f durare-pg
```

One integration test file: `cargo test --all-features --test <file>`. Lib unit
tests live in `src/*_tests.rs` and run with `--lib`. `pg_client_debounces` in
`tests/postgres.rs` is a known flake (#244): rerun once before investigating.

CI also runs `cargo semver-checks` against the crates.io release and the
feature matrix (`--no-default-features --features postgres` and `sqlite`); a
change to any `pub` item must keep both green.

## Repository map

- `src/engine.rs` — `DurableEngine`: dispatch, recovery, child starts
- `src/context.rs` — `DurableContext`: the durable calls a workflow body makes
- `src/execution.rs` — the per-execution latch (`RecoveryRequired`, `ObservationFailed`)
- `src/provider.rs` — the `StateProvider` trait; `memory.rs`, `sqlite.rs`, `postgres.rs` implement it
- `tests/` — integration tests; `tests/common/mod.rs` holds shared fixtures (`recorded`, `hermetic_pg_db`)
- `CONTRIBUTING.md`, `RELEASING.md`, `CHANGELOG.md`

## Conventions

- Every `pub` item is documented (`missing_docs` is enforced). Prefer runnable
  doc examples; use `no_run` for ones that need a database; never `ignore`.
- Behavior that touches storage is tested on every backend it affects. Write
  one `contract(db)` and run it on memory, SQLite and Postgres, as
  `tests/child_atomic.rs` does.
- Match the reference DBOS SDKs (Go, Python) on semantics, not only on names.
  Before stating "the reference SDKs do X", read the code on the `dbos-inc`
  upstream `main` branch and cite the file. When a case has no reference
  behavior, write the rule down first (condition → durable behavior), agree on
  it, then implement it once.
- Keep the SDK Rust-native. Name Go or Python in code or rustdoc only when the
  point is cross-SDK compatibility.
- Prose explains a constraint or a reason, not what the code does. Document
  observed behavior and concrete limits; no promotional claims. English only in
  code, docs, commits and PRs.
- User-visible changes get a `CHANGELOG.md` entry under `[Unreleased]` in the
  right section. A change to an existing API's behavior or error type is
  `Changed`, even when the PR also adds something.
- Plan or tracker numbering (phases, waves) never appears in code, file names
  or comments.

## Workflow

1. Branch from `origin/main` for one concern. One concern per PR; split
   otherwise.
2. For a bug, write the failing test first and watch it fail.
3. Run the four gates, plus the Postgres suite when storage is touched.
4. Review your own diff before opening the PR, from four angles: reuse (does a
   helper already exist?), simplification (derivable state, copy-paste, dead
   code), efficiency (hot-path cost), altitude (root cause, or a special case
   layered on shared code?). Fix what you find.
5. Open the PR and watch CI. When review changes the code, update the
   description so it describes the final state, not the history.

## Commits

Conventional Commits: `type(scope): imperative summary`, lowercase, at most 72
characters, standing alone in `git log`. Types: `feat`, `fix`, `docs`, `test`,
`refactor`, `perf`, `chore`, `ci`. The body explains why: the problem, why this
fix, and what was considered and rejected; wrap at 72 columns. No
`Co-Authored-By` or tool-attribution lines. Put `Closes #n` in the PR
description rather than the commit, so rebases do not spam the issue.

## Pull request description

Title: the commit subject. Body in this order, about 400 words unless the
change is a design change:

1. **What changed** — one paragraph a reader understands without the diff.
2. **Why** — the problem, concretely. A failing scenario or a short snippet
   beats prose.
3. **How** — only the non-obvious design choice; at most one snippet.
4. **Breaking / compatibility** — explicit, or "None".
5. **Tests** — what proves it, and which backends it ran on.
6. **Follow-ups** — what was deliberately left out, and why.

Self-sufficient: summarize the discussion that led here instead of linking to
it. Disclose shortcomings. No marketing language.

## Boundaries

- Never push to `main`, force-push a shared branch, or rewrite others' commits.
- Never `cargo publish`. Releases follow `RELEASING.md`: from merged `main`,
  tag first, `cargo package --list`, then publish.
- Never commit secrets, local database URLs, or scratch files (`*.db`, probe
  tests).
- Never run destructive git operations (`reset --hard`, `stash pop`, branch
  deletion) in a checkout you did not create; other sessions share them.
- Ask before changing the shape of a released `pub` trait. Unreleased shapes
  may change freely until the release that ships them.
