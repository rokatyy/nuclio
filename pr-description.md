### 📝 Description

Add native Azure Container Registry (ACR) authentication support for Kaniko builds, using kaniko's built-in `acr-env` credential helper. This eliminates the need for pre-generated Docker login tokens when pushing images to ACR, analogous to the existing ECR support for AWS.

---

### 🛠️ Changes Made

- Added ACR URL detection via `matchACRUrl` (checks for `.azurecr.io`), following the same pattern as `matchECRUrl`
- Added `configureACRCredentialsMount` which:
  - Writes a `config.json` with `{"credHelpers":{"<registry>":"acr-env"}}` via a BusyBox init container to an emptyDir volume mounted at `/kaniko/.docker`
  - Mounts the Azure credentials secret (`AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`) via `envFrom` on the kaniko executor, using the existing `RegistryProviderSecretName` config field
  - Falls back to managed identity (e.g., Azure Workload Identity) when no secret is specified
- Refactored `configureSecretVolumeMount` from an if-else chain to a switch statement (linter requirement) and extracted the default docker registry secret logic into `configureDockerRegistrySecretMount`

---

### ✅ Checklist
- [ ] I updated the documentation (if applicable)
- [ ] I have tested the changes in this PR

---

### 🧪 Testing

- Verified compilation (`go build ./pkg/containerimagebuilderpusher/...`)
- Passed `go vet`
- Passed `make fmt` (0 linter issues)
- Manual verification of the generated Kaniko pod spec needed against a live ACR registry

---

### 🔗 References
- Ticket link:
- Design docs links:
- External links:
  - [Kaniko ACR authentication docs](https://github.com/GoogleContainerTools/kaniko#pushing-to-azure-container-registry)

---

### 🚨 Breaking Changes?

- [ ] Yes (explain below)
- [x] No

---

### 🔍️ Additional Notes

**Configuration**: ACR auth uses the same `RegistryProviderSecretName` field already used for ECR (`NUCLIO_KANIKO_REGISTRY_PROVIDER_AUTH_SECRET_NAME` env var / `dashboard.kaniko.registryProviderSecretName` Helm value). No new config fields were added.

**Required secret format** (different from ECR — keys are env var names, not a credentials file):
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: acr-credentials
type: Opaque
stringData:
  AZURE_CLIENT_ID: "<clientID>"
  AZURE_CLIENT_SECRET: "<clientSecret>"
  AZURE_TENANT_ID: "<tenantId>"
```

**Why init container + emptyDir instead of a ConfigMap resource**: Creating a ConfigMap would require lifecycle management (create before job, clean up after) and risk orphaned resources. The init container approach is self-contained within the pod, following the same pattern as the existing `fetch-bundle` and `extract-bundle` init containers.

**Companion changes needed in mlrun**: mlrun needs to pass the ACR credentials secret name and set the registry URL to the ACR hostname (e.g., `mycr.azurecr.io`).
