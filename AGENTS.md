# AGENTS.md — nuclio

Authoritative reference for AI coding agents working in the nuclio repository.
Human-curated; update when Makefile targets or conventions change.

---

## Project Overview

nuclio is a high-performance, open-source serverless event and data processing
platform. The codebase is primarily Go, with some Python and Shell. The main
integration branch is **`development`**; all feature work branches from it.
`master` tracks the latest stable release. Full contributor docs live in
[`docs/devel/contributing.md`](docs/devel/contributing.md).

---

## Build Commands

```sh
make modules       # fetch Go dependencies (run first in a clean checkout)
make build         # build all Docker images and the nuctl CLI
make nuctl-bin     # build nuctl binary only — no Docker required
make fmt lint      # format and lint; run before every commit
```

---

## Test Commands

```sh
make lint test-unit   # fast unit tests (~seconds) — required before every PR
make test             # local integration tests (~90 min, requires Docker)
```

**Notes**:
- Unit tests require build tag `test_unit`; integration tests use `test_integration`.
- Linter: `golangci-lint` v2.x — config is [`.golangci.yml`](.golangci.yml).
- `make lint` also verifies all test files carry the correct build-tag annotation.

---

## Coding Conventions

Distilled from [`docs/devel/coding-conventions.md`](docs/devel/coding-conventions.md).

### Naming
- Use **verbose, descriptive names**: `handleFunctionAdd` not `add`; `handler` not `hdlr`.
- Short names are acceptable only for struct receivers (e.g., `f` for a `Function` receiver).
- File variable suffix conventions:
  - `FileName` — base name only
  - `Dir` — directory path
  - `Path` — full absolute or relative path
  - `File` — `*os.File` object
  - `FileContents` — contents read from a file

### Structure
- All functionality lives in **instantiatable objects**; no package-level mutable state.
- Exported struct methods must appear **before** unexported ones.

### Testing
- Tests use **testify suites** (`github.com/stretchr/testify/suite`).
- All assertions go through `suite.Require().<Assertion>` (not `assert`).
- Test suite ordering: struct → `SetupSuite` → `SetupTest` → `TearDownTest` →
  `TearDownSuite` → test functions → helper functions.

---

## Branching & PR Conventions

1. **Fork** the repository; do **not** push feature branches to `nuclio/nuclio` directly.
2. Branch from `development` (not `master` or `main`).
3. Run `make fmt lint` before opening a PR.
4. PR description must explain **why** the change is needed, not just what changed.
5. GitHub Actions CI validates every PR — do not merge until all checks pass.

---

## Boundaries — Ask a Human First

| Action | Policy |
|--------|--------|
| Force-push to `development` or `master` | **Never** |
| Commit built binaries or Docker images | **Never** |
| Add a new Go module dependency | Ask first |
| Modify CI workflow files under `.github/` | Ask first |
| Introduce a new package-level variable with mutable state | Ask first |
