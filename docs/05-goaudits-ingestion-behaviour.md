# GoAudits Ingestion Behaviour (M1)

## Job identity and entry point
- Job name in SQL/logs: `GoAuditsIngestion`
- Script: `functions/src/jobs/goaudits-ingestion.js`
- Runs in ACA Job `job-goaudits-ingest-uks` from image built by `functions/Dockerfile.goaudits`.
- Steady-state schedule: cron `0 * * * *` (top of each hour).

## Request window and payload
- Watermark read from `dbo.JobWatermark` where `JobName='GoAuditsIngestion'`.
- `start_date` defaults to the watermark date (UTC, truncated to YYYY-MM-DD) unless `GOAUDITS_START_DATE` overrides.
- `end_date` defaults to today (UTC, YYYY-MM-DD) unless `GOAUDITS_END_DATE` overrides.
- API endpoint default: `https://api.goaudits.com/v1/audits/getauditsummary` (override via `GOAUDITS_AUDITSUMMARY_URL`).
- Request body (JSON):
  ```json
  {
    "start_date": "<from watermark or override>",
    "end_date": "<today or override>",
    "status": "Completed",
    "jsonflag": true,
    "filterId": ""
  }
  ```
  (Other optional keys are sent as empty/zero/false; token is in the `Authorization: Bearer <token>` header from Key Vault.)
- HTTP retry policy: retry on 429 or 5xx up to 5 attempts with exponential backoff + jitter; 30s request timeout; 401/403 or non-JSON responses fail fast with a fatal error.

## Response parsing
- Expected response: array of objects.
- ID mapping:
  - Primary: `item.ID`
  - Fallbacks: `Id`, `auditId`, `audit_id`, `id`, `reportId`, `report_id`
- Completed timestamp:
  - Primary: `Updated_On` (format `YYYY-MM-DD HH:mm:ss`, coerced to `YYYY-MM-DDTHH:mm:ssZ`)
  - Fallback: `EndTime`, `endTime`, `Date`, `date`
- CertificationNumber: left `NULL` (no reliable field provided).
- Items with missing ID or missing/unparseable completed time are skipped.
- Certificates are populated later by the enrichment job (`GoAuditsEnrichment`); blanks here are expected.

## Filtering and counts
- Items are eligible only if `completedAtUtc > watermark`.
- `counts`:
  - `fetched` = total items returned by the API
  - `eligible` = items passing ID and timestamp checks and newer than watermark
  - `ingested` = successfully inserted into `GoAuditsReports`
  - `skipped` = missing ID/time or older/equal to watermark
  - `alreadyProcessed` = duplicates detected via `ProcessedItems` PK
  - `pages` = always `1` (single call)
- For backfill: set `GOAUDITS_START_DATE` to the desired rewind date (or adjust `JobWatermark` per `08-backfill-runbook.md`) and run once; idempotency and ProcessedItems prevent duplicates.

## Idempotency and transactional writes
1. Start SQL transaction after API fetch.
2. For each eligible item:
   - Insert into `dbo.ProcessedItems (JobName, ItemKey, RunId)`. On PK violation (`JobName`, `ItemKey`), treat as already processed and continue.
   - If inserted, write to `dbo.GoAuditsReports (GoAuditsReportId, CompletedAtUtc, CertificationNumber, JobRunId)`.
3. After processing all items, update or insert `dbo.JobWatermark` to `max(CompletedAtUtc)` (or create the row if it did not exist).
4. Commit transaction. Watermark only advances after successful ingest.

## JobRunHistory and logging
- On start: insert `JobRunHistory` with `Status='Running'`.
- On success/failure: update `JobRunHistory` with status (`Succeeded`/`Failed`), message summarising counts, and `RunCompletedUtc`.
- Logs: single-line JSON to stdout/stderr with `jobName`, `jobRunId`, `startedAtUtc`, `completedAtUtc`, `status`, `counts`, `start_date`, `end_date`, and `error` on failures. Payload values are **not** logged; only the list of keys from the first item may be logged for debugging.

## Why fetched can be much greater than ingested
The GoAudits API can return multiple rows per audit (e.g., question-level or item-level entries). Ingestion deduplicates by the unique ID via `ProcessedItems` + `GoAuditsReportId`, so `fetched` can exceed `eligible` and `ingested`.
