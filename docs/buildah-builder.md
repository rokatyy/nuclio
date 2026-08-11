# Buildah Builder for Nuclio

Buildah is an opt-in container image builder available alongside the existing Kaniko and Docker builders. It is introduced as a non-breaking addition — all existing deployments continue to use their currently configured builder without any changes.

---

## Enabling Buildah

Set the `NUCLIO_CONTAINER_BUILDER_KIND` environment variable to `buildah` on the Nuclio dashboard deployment. This is the same environment variable used to select `kaniko`.

```yaml
env:
  - name: NUCLIO_CONTAINER_BUILDER_KIND
    value: buildah
```

No other top-level configuration schema changes are required.

---

## Configuration Reference

The following environment variables configure the Buildah builder. All have defaults and are optional.

| Variable | Default | Description |
|---|---|---|
| `NUCLIO_CONTAINER_BUILDER_KIND` | `docker` | Set to `buildah` to activate the Buildah builder. |
| `NUCLIO_BUILDAH_CONTAINER_IMAGE` | `quay.io/buildah/stable` | Buildah executor container image. Pin to a specific tag in production to avoid unexpected version drift. |
| `NUCLIO_BUILDAH_CONTAINER_IMAGE_PULL_POLICY` | `IfNotPresent` | Image pull policy for both the Buildah executor and busybox init containers. |
| `NUCLIO_BUSYBOX_CONTAINER_IMAGE` | `busybox:stable` | BusyBox image used in init containers to fetch and extract the build bundle. |
| `NUCLIO_KANIKO_JOB_DELETION_TIMEOUT` | `30m` | How long to keep the build Job around after completion before deletion (shared with Kaniko). |
| `NUCLIO_REGISTRY_CREDENTIALS_SECRET_NAME` | _(empty)_ | Default Kubernetes secret name containing Docker registry credentials. |
| `NUCLIO_KANIKO_INSECURE_PUSH_REGISTRY` | `false` | Disable TLS verification when pushing images (passes `--tls-verify=false` to `buildah push`). |
| `NUCLIO_KANIKO_INSECURE_PULL_REGISTRY` | `false` | Disable TLS verification when pulling base images during build (passes `--tls-verify=false` to `buildah bud`). |

---

## Example Platform Configuration Snippet

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nuclio-dashboard
spec:
  template:
    spec:
      containers:
        - name: nuclio-dashboard
          env:
            - name: NUCLIO_CONTAINER_BUILDER_KIND
              value: buildah
            - name: NUCLIO_BUILDAH_CONTAINER_IMAGE
              value: quay.io/buildah/stable:v1.36.0   # pin to a specific version
            - name: NUCLIO_REGISTRY_CREDENTIALS_SECRET_NAME
              value: registry-credentials
```

---

## Security Context (Rootless vs Privileged)

### Default: Rootless

By default the Buildah executor container runs with a rootless security context:

```
runAsNonRoot: true
allowPrivilegeEscalation: false
capabilities:
  drop: ["ALL"]
  add: ["SETUID", "SETGID"]
```

`SETUID` and `SETGID` are required for Buildah's user namespace mapping, which is the core mechanism that enables rootless container builds. This is the least-privileged mode available.

### Privileged Fallback

Rootless Buildah requires kernel support for unprivileged user namespaces (`user.max_user_namespaces > 0`). Some managed Kubernetes environments (notably older GKE, AKS, or EKS configurations, or clusters with strict PodSecurityAdmission policies that block `SETUID`/`SETGID`) may not permit this.

If your cluster cannot run rootless Buildah, you must grant the build pod privileged access by adding a custom `securityContext` to the function's build configuration or by relaxing the namespace's PodSecurityAdmission policy. Example privileged override for the executor pod:

```yaml
securityContext:
  privileged: true
```

**Operator note:** Enabling privileged mode is a cluster-level security decision. Document this clearly for your users and restrict which namespaces or service accounts are permitted to run privileged build pods.

#### Checking Kernel Support

```bash
cat /proc/sys/user/max_user_namespaces   # must be > 0
```

On a node that shows `0`, rootless Buildah will fail with a user namespace error. Configure the node or fall back to privileged mode.

---

## Registry Authentication

Buildah uses the Docker `config.json` credential format — the same format used by Kaniko. Registry credentials are mounted from a Kubernetes Secret via `--authfile /auth/config.json`.

The secret must contain a `.dockerconfigjson` key (standard Kubernetes `kubernetes.io/dockerconfigjson` secret type):

```bash
kubectl create secret docker-registry registry-credentials \
  --docker-server=registry.example.com \
  --docker-username=myuser \
  --docker-password=mypassword \
  --namespace=nuclio
```

Then set `NUCLIO_REGISTRY_CREDENTIALS_SECRET_NAME=registry-credentials`.

### Amazon ECR

ECR authentication is supported using the same pattern as Kaniko: an init container runs the AWS CLI to create repositories, and AWS credentials are mounted via a Kubernetes Secret named by `NUCLIO_KANIKO_REGISTRY_PROVIDER_AUTH_SECRET_NAME`. If no provider secret is configured, an instance role with ECR permissions is assumed.

---

## Build Caching

### What is supported

Buildah uses `buildah bud --layers` to enable layer caching. Each build layer is cached locally within the Job pod for the duration of the build, which avoids re-running unchanged Dockerfile instructions within a single build run.

### Known Limitation: No Remote Cache Registry

Kaniko supports `--cache-repo` to push and pull cached layers from a remote registry. **Buildah does not have an equivalent remote cache registry mechanism.**

This means:

- Layer cache does **not** persist between separate builds (each build starts with a cold layer cache).
- The `NUCLIO_DASHBOARD_KANIKO_CACHE_REPO` configuration option has no effect when using Buildah.
- Repeated builds of functions with large, unchanged base layers will re-pull those layers on every build.

For build environments where persistent inter-build caching is critical, Kaniko remains the preferred builder until Buildah gains equivalent remote cache support.

---

## Error Handling

| Scenario | Behavior |
|---|---|
| Buildah not configured at platform level | Returns `"buildah builder is not enabled on this platform"` before any Job is submitted |
| Build Job fails | Job logs are surfaced as an error (same as Kaniko) |
| Build Job times out | Timeout error with Job logs (same as Kaniko) |
| Registry push fails | Surfaced via `buildah push` exit code in Job logs |

---

## Keeping Kaniko Working

Buildah is additive. Setting `NUCLIO_CONTAINER_BUILDER_KIND=kaniko` continues to use Kaniko exactly as before. No migration occurs automatically, and no existing function configuration is modified when Buildah is enabled.
