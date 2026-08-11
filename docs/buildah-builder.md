# Buildah Builder for Nuclio

Buildah is an opt-in container image builder for Nuclio's Kubernetes-based build pipeline. It is a supported alternative to the deprecated Kaniko builder and achieves feature parity with it.

---

## Enabling Buildah at the Platform Level

Set the `NUCLIO_CONTAINER_BUILDER_KIND` environment variable on the Nuclio controller/dashboard deployment to `buildah`:

```yaml
env:
  - name: NUCLIO_CONTAINER_BUILDER_KIND
    value: buildah
  - name: NUCLIO_BUILDAH_CONTAINER_IMAGE
    value: quay.io/buildah/stable:v1.36.0       # optional, this is the default
  - name: NUCLIO_BUILDAH_CONTAINER_IMAGE_PULL_POLICY
    value: IfNotPresent                           # optional, this is the default
```

Existing deployments using `kaniko` or `docker` are unaffected; no automatic migration occurs.

### Available Environment Variables

| Variable | Default | Description |
|---|---|---|
| `NUCLIO_CONTAINER_BUILDER_KIND` | `docker` | Set to `buildah` to enable Buildah |
| `NUCLIO_BUILDAH_CONTAINER_IMAGE` | `quay.io/buildah/stable:v1.36.0` | Buildah OCI image (pin this in production) |
| `NUCLIO_BUILDAH_CONTAINER_IMAGE_PULL_POLICY` | `IfNotPresent` | Image pull policy |
| `NUCLIO_BUILDAH_PRIVILEGED` | `false` | Set to `true` to run Buildah in privileged mode |
| `NUCLIO_DASHBOARD_JOB_NAME_PREFIX` | `kanikojob` | Prefix for Kubernetes Job names |
| `NUCLIO_KANIKO_JOB_DELETION_TIMEOUT` | `30m` | Time before build Jobs are cleaned up |
| `NUCLIO_KANIKO_INSECURE_PUSH_REGISTRY` | `false` | Disable TLS verification when pushing |
| `NUCLIO_KANIKO_INSECURE_PULL_REGISTRY` | `false` | Disable TLS verification when pulling |
| `NUCLIO_REGISTRY_CREDENTIALS_SECRET_NAME` | `` | Default Kubernetes secret for registry credentials |

---

## Selecting Buildah per Function

Builder selection is a platform-level setting. To use Buildah for a function, the platform must be configured with `NUCLIO_CONTAINER_BUILDER_KIND=buildah`. There is no per-function builder override; all functions on a Buildah-enabled platform use Buildah.

---

## Security Context: Rootless vs Privileged

### Default: Rootless (recommended)

By default, the Buildah container runs with:

```
runAsNonRoot: true
allowPrivilegeEscalation: false
```

This is the preferred mode. Most standard Kubernetes distributions support rootless Buildah without additional cluster configuration.

**Limitation:** Some managed Kubernetes clusters with restrictive Pod Security Admission (PSA) policies or that lack user namespace support may not allow rootless Buildah to function correctly (e.g., `buildah bud` may fail with permission errors when trying to unpack layers).

### Privileged Mode (fallback for restricted environments)

If rootless Buildah cannot be made to work in your cluster environment, set:

```yaml
env:
  - name: NUCLIO_BUILDAH_PRIVILEGED
    value: "true"
```

This configures the container with `privileged: true`. **Only use this when required**, and ensure your cluster security policy permits privileged pods. Consult your cluster administrator before enabling this mode.

---

## Registry Authentication

Buildah reuses Nuclio's existing registry credential mechanism. The Docker config JSON secret is mounted into the Buildah pod at `/tmp/.docker/config.json`, and the `DOCKER_CONFIG` environment variable is set to `/tmp/.docker`. This works regardless of whether Buildah runs as root or non-root.

Configure a registry credentials secret using:

```yaml
env:
  - name: NUCLIO_REGISTRY_CREDENTIALS_SECRET_NAME
    value: my-registry-secret
```

The secret must contain a `.dockerconfigjson` key (standard Kubernetes docker-registry secret format).

---

## Build Caching

### What works

Buildah supports local layer caching via the `--layers` flag. This flag is always passed to `buildah bud`, so intermediate build layers are cached in the Buildah pod's local storage for the duration of the build. This can speed up multi-step Dockerfiles where early layers rarely change.

### Known limitation: no remote cache repository

Kaniko supports `--cache-repo` to push and pull build cache layers to/from a remote registry. **Buildah does not natively support this pattern.** The `NUCLIO_DASHBOARD_KANIKO_CACHE_REPO` environment variable has no effect when using the Buildah builder.

If remote layer caching is critical for your build performance, continue using Kaniko until Buildah adds equivalent remote cache support.

---

## How Buildah Builds Work (Kubernetes Job)

Each function build dispatches a `batch/v1 Job`:

1. An `fetch-bundle` init container downloads the build context archive from the Nuclio dashboard.
2. An `extract-bundle` init container unpacks the archive to the shared volume.
3. The `buildah-executor` container runs:
   ```
   buildah bud --layers --file=<Dockerfile> --tag=<registry>/<image>:<tag> <context-dir>
   buildah push <registry>/<image>:<tag>
   ```
4. Nuclio polls the Job status and streams logs back to the function deployment flow, identical to how Kaniko Jobs are observed.

Jobs are cleaned up after `NUCLIO_KANIKO_JOB_DELETION_TIMEOUT` (default 30 minutes) to allow inspection after a build failure.

---

## Kaniko Compatibility

Buildah and Kaniko can coexist in the same Nuclio installation; only one builder is active per platform instance. Switching a platform from Kaniko to Buildah has no effect on already-deployed functions; it only affects new function builds.

To revert to Kaniko, set `NUCLIO_CONTAINER_BUILDER_KIND=kaniko`.
