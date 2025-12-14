# Architecture Overview

## What this platform does
Managed, containerised scheduled jobs that ingest and enrich GoAudits data (M1 + M1.5) into Azure SQL using Managed Identity. All job output is structured JSON to Log Analytics for observability. Watermarks and idempotency are enforced in SQL tables to prevent duplicates and to resume safely.

## Why Azure Container Apps Jobs (ACA Jobs)
- Timer-driven Azure Functions were retired due to reliability and limited observability for long-running or external-API dependent tasks.
- ACA Jobs provide: container-level control, image-based deployments, Log Analytics integration, and first-class Managed Identity for SQL and Key Vault.

## Security model
- **No secrets in app settings** beyond non-sensitive config values.
- **Managed Identity** used for:
  - Azure SQL access (token-based auth, no SQL passwords).
  - Key Vault access (to retrieve the GoAudits bearer token only).
  - ACR image pulls.
- **Key Vault** stores the GoAudits bearer token. The token is never logged or embedded in images.

## High-level flow
```mermaid
flowchart LR
    Sched[ACA Job (schedule)] --> Ctn[Container / Node script]
    Ctn --> KV[(Key Vault\n(Managed Identity))]
    Ctn --> GA[GoAudits API]
    Ctn --> SQL[(Azure SQL\n(JobRunHistory,\nJobWatermark,\nProcessedItems,\nGoAuditsReports,\nGoAuditsReportAnswers))]
    Ctn --> Logs[Log Analytics\n(structured JSON logs)]
```

## Data lifecycle (M1 ingestion)
1. ACA Job (`job-goaudits-ingest-uks`) runs `src/jobs/goaudits-ingestion.js` from the repo.
2. Script authenticates to SQL via Managed Identity using the shared helper (`functions/src/shared/sql.js`).
3. GoAudits bearer token is fetched from Key Vault with Managed Identity.
4. Script calls the GoAudits “audit summary” endpoint with a watermark-driven window and normalises fields.
5. Inserts are wrapped in a SQL transaction with idempotency (`ProcessedItems`) and watermark updates (`JobWatermark`) only after successful ingest.
6. JobRunHistory captures start/end status; structured JSON logs land in Log Analytics.

## Data lifecycle (M1.5 enrichment)
1. ACA Job (`job-goaudits-enrich-uks`) runs `src/jobs/goaudits-enrichment.js`.
2. Selects GoAuditsReports needing enrichment (missing answers or certificate) excluding already processed items.
3. Fetches bearer token from Key Vault (Managed Identity) and calls GoAudits “details by id” with the full Postman-style request body merged with `audit_id`.
4. Parses detail rows (`RecordType='Detail'`), updates `GoAuditsReports.CertificationNumber` when present, and inserts answers into `GoAuditsReportAnswers` with idempotent keys.
5. Marks `ProcessedItems` when answers exist (even if certificate is blank) so blank-cert audits do not block progress.
6. JobRunHistory and structured JSON logs record status, counts (including `certMissingCount`), and errors.

## Persistent stores
- **SQL**: `JobRunHistory`, `JobWatermark`, `ProcessedItems`, `GoAuditsReports`, `GoAuditsReportAnswers`.
- **Log Analytics**: single source for container stdout/stderr across all jobs.
