# AGENTS.md — Orientation Guide for AI Coding Agents

This file is the primary orientation document for AI coding agents working in this fork of the nuclio repository. Read this file before taking any action in the repo.

---

## What is nuclio

[nuclio](https://nuclio.io) is a high-performance, open-source **serverless framework** designed for real-time and data-driven applications. It is purpose-built for AI/ML inference, stream processing, and event-driven workloads where latency and throughput matter.

Key characteristics:

- **High performance** — functions are compiled and run as native processes (not spawned per-invocation), enabling sub-millisecond cold starts and high request throughput.
- **Kubernetes-native** — nuclio deploys functions as Kubernetes `Deployment` and `Service` objects, managed via a Custom Resource Definition (CRD).
- **Multi-platform** — runs on Kubernetes (the primary target), local Docker, and managed cloud environments.
- **Multi-language** — supports Go, Python, Java, NodeJS, .NET Core, Shell, and Ruby runtimes.
- **Rich trigger ecosystem** — functions can be triggered by HTTP, Kafka, Kinesis, RabbitMQ, NATS, cron, and more.
- **Iguazio integration** — deeply integrated with the Iguazio Data Science Platform (MLRun, V3IO), though usable standalone.

---

## Architecture Overview

nuclio's runtime architecture consists of five main components:

### Dashboard
A web UI and REST API server (written in Go, fronted by a React SPA) that lets users create, configure, deploy, and monitor functions. The Dashboard exposes the same API consumed by the `nuctl` CLI.

### nuctl
The command-line interface for nuclio. Wraps the same API as the Dashboard. Used for scripted and CI/CD deployments.

### Function CR (Custom Resource)
A Kubernetes Custom Resource (`Function`) that describes the desired state of a deployed function — its source code, runtime, resource limits, triggers, and configuration. Stored via the Kubernetes API server.

### Controller
A Kubernetes controller that watches Function Custom Resources and reconciles actual cluster state (Deployments, Services, ConfigMaps, Ingresses) to match the desired spec. Written in Go using the `controller-runtime` framework.

### Processor
The per-function runtime binary that is embedded inside each function's container image. The Processor:
- Reads the function configuration at startup.
- Connects to configured triggers (Kafka topic, HTTP port, etc.).
- Receives events from triggers and dispatches them to the user's handler code.
- Returns responses back through the trigger.

### Interaction flow

```
User / CI
  │
  ├─► Dashboard (REST API)  ──┐
  └─► nuctl (CLI)            │
                              ▼
                       Function CR  (Kubernetes API server)
                              │
                              ▼
                        Controller  (reconciles)
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
             Kubernetes             Processor
          Deployment/Service     (inside function
                                  container image)
                                       │
                                  Trigger sources
                               (HTTP, Kafka, cron …)
```

---

## Main Documentation Pages

Use these stable URLs for authoritative reference. Do not rely on branch-specific or versioned paths.

| Topic | URL |
|---|---|
| Getting started | https://docs.nuclio.io/en/stable/setup/ |
| Function concepts | https://docs.nuclio.io/en/stable/concepts/ |
| Triggers reference | https://docs.nuclio.io/en/stable/reference/triggers/ |
| Deployment (Kubernetes) | https://docs.nuclio.io/en/stable/setup/k8s/ |
| CLI reference (nuctl) | https://docs.nuclio.io/en/stable/reference/nuctl/ |
| Official GitHub (upstream) | https://github.com/nuclio/nuclio |

For in-repo documentation see the `docs/` directory.

---

## How to Contribute

### Branching conventions
- Always branch from `development` (the main integration branch for this fork).
- Name branches `feature/<short-slug>` or `fix/<short-slug>`.
- Never commit directly to `development` or `master`.

### Go version
Check `go.mod` at the repo root for the required Go toolchain version. Currently:

```
go 1.25.0
```

Install the matching version before building or running tests.

### Build
```bash
# Build all components
make build

# Build a specific component (e.g., dashboard, controller, processor)
make build-<component>
```

### Tests
```bash
# Unit tests
make test

# Integration tests (requires a running Kubernetes cluster)
make test-k8s
```

Always run `make test` before opening a PR. Integration tests are optional unless your change touches Kubernetes-specific behaviour.

### Linting
```bash
make lint
```

This runs `golangci-lint` with the project's configuration. Lint must pass before a PR can be merged. Fix all lint errors before committing.

### PR guidelines
- **Target branch:** `development` (not `master`).
- Fill out the pull-request template completely.
- Link the relevant issue or ticket in the PR description.
- Keep PRs small and focused on a single concern — prefer multiple small PRs over one large one.
- Do not mix refactoring with feature or bug-fix changes.

### Auto-generated files — do NOT edit manually
The following files/directories are produced by code-generation scripts and must not be edited by hand:

| Path | Generator |
|---|---|
| `pkg/platform/kube/apis/` | Kubernetes code-gen (`hack/scripts/`) |
| `go.sum` | `go mod tidy` — run the command, do not hand-edit |

If you need to update generated code, run the appropriate generator script and commit the result.

### Agent-specific rules
- Always run `make lint` and `make test` locally before committing.
- Never force-push to `development` or any other shared branch.
- Do not modify files outside the scope of your assigned ticket.
- When in doubt about whether a file is auto-generated, check `hack/scripts/` or `Makefile` for generation targets before editing.

---

## How to Deploy nuclio

### Kubernetes — Helm (recommended)

```bash
helm repo add nuclio https://nuclio.github.io/nuclio/charts
helm repo update
helm install nuclio nuclio/nuclio \
  --namespace nuclio \
  --create-namespace
```

### Kubernetes — raw manifests

```bash
kubectl apply -f https://raw.githubusercontent.com/nuclio/nuclio/master/hack/k8s/resources/nuclio-rbac.yaml
kubectl apply -f https://raw.githubusercontent.com/nuclio/nuclio/master/hack/k8s/resources/nuclio.yaml
```

### Local / Docker-based development

Run the Dashboard locally against a local Docker daemon (no Kubernetes required):

```bash
docker run \
  -p 8070:8070 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  nuclio/dashboard:stable-amd64
```

Then open `http://localhost:8070` to manage functions via the web UI.

For more environment-specific configurations (e.g., volume mounts, registry credentials, local registry setup) refer to:

- `hack/docker/` — Docker Compose files and helper scripts for local development
- `docs/setup/` — step-by-step setup guides for various environments
- `hack/scripts/` — automation scripts used in CI and local dev
