# Troubleshooting

Use this symptom → cause → fix guide. Logs are in Log Analytics; SQL shows canonical job status.

## CLI and scheduling gotchas
- `az containerapp job create/update` uses `--trigger-type Schedule` (exact casing). A common failure is `BadRequest` if “Scheduled” is used.
- Cron for ACA Jobs here is 5-part (e.g., `0 * * * *`, `15 * * * *`). Using 6-part will fail validation.

## ACR pull failures (Managed Identity)
- Symptom: ACA Job fails to start, logs mention auth/pull errors from ACR.
- Fix:
  - Ensure `AcrPull` is assigned to the UAMI on `acrmcsschedwrkprod.azurecr.io`.
  - Job definitions must specify both `--registry-identity <uamiId>` and `--mi-user-assigned <uamiId>`.
  - Confirm image tag exists in ACR (`az acr repository show-tags -n acrmcsschedwrkprod --repository job-goaudits-ingest-uks` or `job-goaudits-enrich-uks`).

## Key Vault `ForbiddenByRbac`
- Symptom: setting or reading secrets fails.
- Fix:
  - For humans: assign `Key Vault Secrets Officer` on `kv-mcsschedwrkprod-uks`.
  - For jobs/UAMI: assign `Key Vault Secrets User`.
  - Verify `KEYVAULT_URI` env var is set and points to the prod vault.

## GoAudits auth errors (401/403)
- Symptom: job stops, `authFatal: true` in counts or fatal log.
- Cause: expired/invalid bearer token or MI not allowed to read Key Vault.
- Fix: rotate the bearer token in Key Vault; confirm UAMI has `Key Vault Secrets User`.

## GoAudits non-auth API errors (ingestion or enrichment)
- Symptom: `failedCount` increments; logs show non-retryable error or repeated retries.
- Causes:
  - Wrong endpoint or body shape (verify ingestion uses `getauditsummary`, enrichment uses `getauditdetailsbyid` with Postman-style body).
  - Bad `audit_id` returning empty/non-detail rows (enrichment marks as failure; does not halt job).
- Fix:
  - Confirm endpoints/env vars.
  - Test in Postman with same body; adjust data range (`GOAUDITS_START_DATE`/`END_DATE`) or batch size (`GOAUDITS_ENRICH_BATCH_SIZE`).

## Timeouts or large data windows (ingestion)
- Symptom: retries/timeouts on ingestion.
- Fix:
  - Narrow the window with `GOAUDITS_START_DATE`/`GOAUDITS_END_DATE`.
  - Consider running multiple smaller backfills (see `08-backfill-runbook.md`).

## Enrichment returns empty/no detail rows
- Symptom: `failedCount` increases, message mentions no detail rows.
- Cause: minimal body would return empty; current body is full Postman-style. If still empty, check audit_id validity.
- Fix: verify audit IDs exist in GoAudits; rerun later or skip (idempotency ensures safety).

## Certificates missing
- Symptom: `certMissingCount` > 0 even with answers inserted.
- Cause: detail data lacks cert question or value.
- Fix: none required for pipeline; data consumer should handle blanks. Certs set later will not be double-processed because `ProcessedItems` is marked once answers exist.

## SQL auth/permission issues
- Errors like “failed to acquire managed identity access token” or “permission denied”:
  - Confirm `SQL_SERVER`, `SQL_DATABASE`, and `AZURE_CLIENT_ID` env vars are set on the job.
  - Ensure the MI user exists in SQL and has grants on `JobRunHistory`, `JobWatermark`, `ProcessedItems`, `GoAuditsReports`, `GoAuditsReportAnswers` (see `02-azure-resources-and-rbac.md`).
  - SQL firewall/network must allow traffic from ACA.

## Check job executions
- List executions:
  ```powershell
  az containerapp job execution list `
    --name job-goaudits-ingest-uks `
    --resource-group rg-mcs-scheduled-workers-production `
    -o table
  az containerapp job execution list `
    --name job-goaudits-enrich-uks `
    --resource-group rg-mcs-scheduled-workers-production `
    -o table
  ```
- Inspect logs via Log Analytics queries in `04-observability-and-proof.md`.

## Legacy Functions
- Legacy Function Apps are retired. If they restart unintentionally, stop them:
  ```powershell
  az functionapp stop --name func-mcs-scheduled-workers-prod-linux --resource-group rg-mcs-scheduled-workers-production
  az functionapp stop --name func-mcs-scheduled-workers-prod-uks --resource-group rg-mcs-scheduled-workers-production
  ```
