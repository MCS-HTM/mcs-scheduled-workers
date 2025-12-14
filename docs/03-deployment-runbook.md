# Deployment Runbook (ACA Jobs)

This runbook covers M1 (ingestion) and M1.5 (enrichment) using Azure Container Apps Jobs (ACA Jobs) with Managed Identity. No local Docker daemon is required; builds use ACR build. ACA cron here is **5-part** and `--trigger-type` is `Schedule` (not `Scheduled`).

## Prerequisites
- `az` CLI installed and logged in to the correct subscription.
- `az extension add --name containerapp` (preview version acceptable).
- Permissions to the resource group `rg-mcs-scheduled-workers-production`, ACR, ACA, Key Vault, and SQL as outlined in `02-azure-resources-and-rbac.md`.
- UAMI: `id-mcs-scheduled-workers-aca-prod-uks` (objectId and clientId available; no secrets).

## Golden path (copy/paste)
1. Build image in ACR (choose one of the jobs) using the commands below.
2. Update the job to the new image tag (`az containerapp job update --image ...`).
3. Start the job once (`az containerapp job start ...`).
4. Prove success via `az containerapp job execution list` and SQL/Log Analytics queries from `04-observability-and-proof.md`.

## Build and push images (ACR build)
Run from the repo root.

```powershell
# Heartbeat image
az acr build `
  --registry acrmcsschedwrkprod `
  --image job-mcs-aca-heartbeat-prod-uks:0.1.0 `
  --file functions/Dockerfile `
  functions

# GoAudits ingestion image
az acr build `
  --registry acrmcsschedwrkprod `
  --image job-goaudits-ingest-uks:0.1.0 `
  --file functions/Dockerfile.goaudits `
  functions

# GoAudits enrichment image
az acr build `
  --registry acrmcsschedwrkprod `
  --image job-goaudits-enrich-uks:0.1.0 `
  --file functions/Dockerfile.goaudits.enrichment `
  functions
```
Use semantic-ish tags (`0.1.x`) and update the job definitions to the new tag when promoting.
To update a job to a new image tag:
```powershell
az containerapp job update `
  --name job-goaudits-ingest-uks `
  --resource-group rg-mcs-scheduled-workers-production `
  --image acrmcsschedwrkprod.azurecr.io/job-goaudits-ingest-uks:<new-tag>
```
Repeat for `job-goaudits-enrich-uks` as needed.

## Create or update ACA Jobs
The UAMI is used both for pulling from ACR and for runtime SQL/Key Vault access.

### Heartbeat job
```powershell
$uamiId = (az identity show -g rg-mcs-scheduled-workers-production -n id-mcs-scheduled-workers-aca-prod-uks --query id -o tsv)
$uamiClientId = (az identity show -g rg-mcs-scheduled-workers-production -n id-mcs-scheduled-workers-aca-prod-uks --query clientId -o tsv)

az containerapp job create `
  --name job-mcs-aca-heartbeat-prod-uks `
  --resource-group rg-mcs-scheduled-workers-production `
  --environment cae-mcs-scheduled-workers-prod-uks `
  --trigger-type Schedule `
  --cron-expression "*/15 * * * *" `   # example 5-part schedule
  --replica-timeout 1800 `
  --replica-retry-limit 1 `
  --image acrmcsschedwrkprod.azurecr.io/job-mcs-aca-heartbeat-prod-uks:0.1.0 `
  --registry-server acrmcsschedwrkprod.azurecr.io `
  --registry-identity $uamiId `
  --mi-user-assigned $uamiId `
  --env-vars `
      SQL_SERVER="<sql-server-name>.database.windows.net" `
      SQL_DATABASE="<database-name>" `
      AZURE_CLIENT_ID=$uamiClientId
```
Use `az containerapp job update` with the same arguments (swap `create` for `update`) when changing images or env vars.

### GoAudits ingestion job
```powershell
az containerapp job create `
  --name job-goaudits-ingest-uks `
  --resource-group rg-mcs-scheduled-workers-production `
  --environment cae-mcs-scheduled-workers-prod-uks `
  --trigger-type Schedule `
  --cron-expression "0 * * * *" `      # steady-state (top of the hour)
  --replica-timeout 1800 `
  --replica-retry-limit 1 `
  --image acrmcsschedwrkprod.azurecr.io/job-goaudits-ingest-uks:0.1.0 `
  --registry-server acrmcsschedwrkprod.azurecr.io `
  --registry-identity $uamiId `
  --mi-user-assigned $uamiId `
  --env-vars `
      SQL_SERVER="<sql-server-name>.database.windows.net" `
      SQL_DATABASE="<database-name>" `
      AZURE_CLIENT_ID=$uamiClientId `
      KEYVAULT_URI="https://kv-mcsschedwrkprod-uks.vault.azure.net/" `
      GOAUDITS_BEARER_SECRET_NAME="goaudits-bearer-token" `
      GOAUDITS_AUDITSUMMARY_URL="https://api.goaudits.com/v1/audits/getauditsummary" `
      GOAUDITS_STATUS="Completed"
```
Optional debug/env overrides for GoAudits job (set only when needed): `GOAUDITS_START_DATE`, `GOAUDITS_END_DATE`, `GOAUDITS_FILTER_ID`.

### GoAudits enrichment job
```powershell
az containerapp job create `
  --name job-goaudits-enrich-uks `
  --resource-group rg-mcs-scheduled-workers-production `
  --environment cae-mcs-scheduled-workers-prod-uks `
  --trigger-type Schedule `
  --cron-expression "15 * * * *" `     # steady-state (15 past the hour)
  --replica-timeout 1800 `
  --replica-retry-limit 1 `
  --image acrmcsschedwrkprod.azurecr.io/job-goaudits-enrich-uks:0.1.0 `
  --registry-server acrmcsschedwrkprod.azurecr.io `
  --registry-identity $uamiId `
  --mi-user-assigned $uamiId `
  --env-vars `
      SQL_SERVER="<sql-server-name>.database.windows.net" `
      SQL_DATABASE="<database-name>" `
      AZURE_CLIENT_ID=$uamiClientId `
      KEYVAULT_URI="https://kv-mcsschedwrkprod-uks.vault.azure.net/" `
      GOAUDITS_BEARER_SECRET_NAME="goaudits-bearer-token" `
      GOAUDITS_AUDITDETAILS_URL="https://api.goaudits.com/v1/audits/getauditdetailsbyid" `
      GOAUDITS_ENRICH_BATCH_SIZE="50" `
      GOAUDITS_ENRICH_CONCURRENCY="1"
```

### Start a job run on demand
```powershell
az containerapp job start `
  --name job-goaudits-ingest-uks `
  --resource-group rg-mcs-scheduled-workers-production

az containerapp job start `
  --name job-goaudits-enrich-uks `
  --resource-group rg-mcs-scheduled-workers-production
```

## Secrets handling (GoAudits bearer token)
- The bearer token lives only in Key Vault.
- Set it via PowerShell (never paste in chat or commit to files):
  ```powershell
  $secure = Read-Host -AsSecureString "Enter GoAudits bearer token"
  $plain  = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
  az keyvault secret set `
    --vault-name kv-mcsschedwrkprod-uks `
    --name goaudits-bearer-token `
    --value $plain
  ```
  (Use `--name` to match `GOAUDITS_BEARER_SECRET_NAME` if you deviate from the default.)

## Environment variables (runtime)
- Common:
  - `SQL_SERVER` – e.g. `<server>.database.windows.net`
  - `SQL_DATABASE` – target database
  - `AZURE_CLIENT_ID` – clientId of `id-mcs-scheduled-workers-aca-prod-uks`
- GoAudits-specific:
  - `KEYVAULT_URI` – `https://kv-mcsschedwrkprod-uks.vault.azure.net/`
  - `GOAUDITS_BEARER_SECRET_NAME` – defaults to `goaudits-bearer-token`
  - `GOAUDITS_AUDITSUMMARY_URL` – defaults to the `getauditsummary` endpoint
  - `GOAUDITS_START_DATE` / `GOAUDITS_END_DATE` – optional overrides (YYYY-MM-DD)
  - `GOAUDITS_STATUS` – defaults to `Completed`
  - `GOAUDITS_FILTER_ID` – defaults to empty string
 - Enrichment:
   - `GOAUDITS_AUDITDETAILS_URL` – defaults to `https://api.goaudits.com/v1/audits/getauditdetailsbyid`
   - `GOAUDITS_ENRICH_BATCH_SIZE` – defaults to 50
   - `GOAUDITS_ENRICH_CONCURRENCY` – defaults to 1 (clamped 1..3)
