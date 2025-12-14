# GoAudits Enrichment Behaviour (M1.5)

## Job identity and entry point
- Job name in SQL/logs: `GoAuditsEnrichment`
- Script: `functions/src/jobs/goaudits-enrichment.js`
- Runs in ACA Job `job-goaudits-enrich-uks` from image built by `functions/Dockerfile.goaudits.enrichment`.
- Steady-state schedule: cron `15 * * * *` (15 past each hour).

## Runtime configuration (env vars)
- Common: `SQL_SERVER`, `SQL_DATABASE`, `AZURE_CLIENT_ID`
- Key Vault: `KEYVAULT_URI`, `GOAUDITS_BEARER_SECRET_NAME`
- Endpoint: `GOAUDITS_AUDITDETAILS_URL` (defaults to `https://api.goaudits.com/v1/audits/getauditdetailsbyid`)
- Control: `GOAUDITS_ENRICH_BATCH_SIZE` (default 50), `GOAUDITS_ENRICH_CONCURRENCY` (default 1, clamped 1..3)

## Selection logic (what needs enrichment)
- Selects `TOP(@batchSize)` from `dbo.GoAuditsReports` where either:
  - `CertificationNumber IS NULL or ''`, **or**
  - no rows exist in `dbo.GoAuditsReportAnswers` for that report.
- Excludes any report already present in `dbo.ProcessedItems` with `JobName='GoAuditsEnrichment'`.
- Ordered newest first (`CompletedAtUtc DESC`).
- Batch size defaults to `GOAUDITS_ENRICH_BATCH_SIZE` = 50. Concurrency defaults to 1 (clamped 1..3).

## Request to GoAudits (details by id)
- Endpoint default: `https://api.goaudits.com/v1/audits/getauditdetailsbyid` (`GOAUDITS_AUDITDETAILS_URL` override).
- Request body merges `audit_id` with a Postman-style base payload:
  ```json
  {
    "archived": "",
    "audit_type_id": "",
    "auto_fail": "",
    "client_id": "",
    "custom_fields": "",
    "description": "",
    "start_date": "2024-01-01",
    "end_date": "2050-12-12",
    "file_type": "",
    "filetype": "",
    "filterId": "",
    "generated_on": "",
    "guid": "",
    "json": 0,
    "name": "",
    "parameters": "",
    "report_name": "",
    "role_code": "",
    "status": "",
    "store_id": "",
    "tags_ids": "",
    "template_name": "",
    "templateactive": true,
    "templateid": 0,
    "uid": "",
    "csv": 0,
    "csvflag": false,
    "jsonflag": true,
    "xlsx": 0,
    "xlsxflag": false,
    "audit_id": "<GoAuditsReportId as string>"
  }
  ```
- Auth: `Authorization: Bearer <token>` from Key Vault (`GOAUDITS_BEARER_SECRET_NAME`, `KEYVAULT_URI`) using Managed Identity.
- Retry policy: retry 429/5xx up to 5 attempts (30s timeout each, exponential backoff + jitter). 401/403 are fatal; other non-2xx or non-array responses are non-retryable and counted as per-report failures.
- If the response is empty or lacks any `RecordType='Detail'` rows, the report is treated as a non-retryable failure (increments failure count).

## Parsing rules
- Certification number: taken from a row where `QUESTION_ID == "1"` or `Question` contains “Certificate Number” (e.g., “MCS Certificate Number”). Trimmed; not logged.
- Answers: only `RecordType == "Detail"` rows are considered.
  - `QuestionKey`: prefer `QUESTION_ID`; fallback to normalized `Question` text (lowercase, whitespace collapsed, non-alphanumerics to `_`) hashed/truncated to `<=256` chars.
  - `QuestionText`: trimmed, max 1000 chars.
  - `AnswerValue`: string/number/bool -> string; object/array -> JSON string; null -> null (max 4000 chars stored).
  - `Section`: from `Section`, optionally combined with `GroupName` if present and not "N/A" (max 200 chars).
  - Deduplicated in-memory per `QuestionKey` so only one insert per key per report.

## SQL writes and idempotency
Per-report transaction:
1. Update `dbo.GoAuditsReports.CertificationNumber` if new cert is present and field is empty.
2. Insert answers into `dbo.GoAuditsReportAnswers (GoAuditsReportId, QuestionKey, AnswerValue, Section, QuestionText, JobRunId)`; ignore PK duplicates.
3. Check `answersExist` (`COUNT > 0`) and read current cert. If answers exist:
   - Increment `certMissingCount` when cert is still null/empty (blanks do not block progress).
   - Insert into `dbo.ProcessedItems (JobName='GoAuditsEnrichment', ItemKey=reportId, RunId=jobRunId)`; ignore PK duplicates.
4. Commit transaction. Per-report failures (non-auth) increment failure count; auth failures fail the whole job.

## JobRunHistory and logging
- On start: insert `JobRunHistory` with `Status='Running'`, `CorrelationId=RunId`.
- On completion: update `JobRunHistory` with `Status` (`Succeeded`/`Failed`) and a message summarising counts.
- Structured JSON log (single line): includes `jobName`, `jobRunId`, `completedAtUtc`, `status`, and `counts` (`selected`, `processed`, `certUpdatedCount`, `answersInsertedCount`, `markedProcessedCount`, `certMissingCount`, `failedCount`, `authFatal`). Errors logged without PII/payloads.

## Steady-state vs backfill
- Normal schedule: `15 * * * *`, batch 50, concurrency 1.
- For backfill, you may temporarily raise `GOAUDITS_ENRICH_BATCH_SIZE` and/or run on-demand more frequently; idempotency prevents duplicates but be mindful of API cost/limits. Reset to steady-state after the catch-up (see `08-backfill-runbook.md`).
