# Repo Structure

The working code lives under `functions/` even though the platform now runs on ACA Jobs (not Azure Functions timers).

## Key directories
- `functions/src/shared/sql.js` – SQL helper using `DefaultAzureCredential` (honours `AZURE_CLIENT_ID` for user-assigned identity). Builds token-based config for SQL using `SQL_SERVER` and `SQL_DATABASE`, with connection pooling and retry-safe lazy initialisation.
- `functions/src/jobs/aca-heartbeat.js` – ACA heartbeat job. Writes a row to `dbo.JobRunHistory` with `JobName='ACAHeartbeat'` and structured JSON logging.
- `functions/src/jobs/goaudits-ingestion.js` – M1 GoAudits ingestion job. Uses Managed Identity for SQL and Key Vault, fetches the bearer token, calls the GoAudits API (`getauditsummary`), enforces watermark/idempotency, writes to `JobRunHistory`, `ProcessedItems`, `GoAuditsReports`, and `JobWatermark`.
- `functions/src/jobs/goaudits-enrichment.js` – M1.5 GoAudits enrichment job. Selects unenriched reports, calls the details endpoint (`getauditdetailsbyid`) with the full Postman-style body, updates `GoAuditsReports.CertificationNumber`, inserts answers into `GoAuditsReportAnswers`, and marks `ProcessedItems` when answers exist (tracks `certMissingCount`).
- `functions/Dockerfile` – container image for the heartbeat job (Node 20-slim, `npm ci --omit=dev`, runs `node src/jobs/aca-heartbeat.js`).
- `functions/Dockerfile.goaudits` – container image for GoAudits ingestion (same base/build flow, runs `node src/jobs/goaudits-ingestion.js`).
- `functions/Dockerfile.goaudits.enrichment` – container image for GoAudits enrichment (runs `node src/jobs/goaudits-enrichment.js`).
- `functions/infra/sql/001_m0_tables.sql` – core tables:
  - `dbo.JobWatermark (JobName PK, WatermarkUtc datetime2(0), UpdatedUtc datetime2(0) default SYSUTCDATETIME())`
  - `dbo.JobRunHistory (RunId uniqueidentifier PK default NEWID(), JobName nvarchar(100), RunStartedUtc datetime2(0) default SYSUTCDATETIME(), RunCompletedUtc datetime2(0) null, Status nvarchar(30), Message nvarchar(4000), CorrelationId nvarchar(100), CreatedUtc datetime2(0) default SYSUTCDATETIME())`
  - `dbo.ProcessedItems (JobName nvarchar(100), ItemKey nvarchar(200), ProcessedUtc datetime2(0) default SYSUTCDATETIME(), RunId uniqueidentifier null, PK(JobName, ItemKey))`
- `functions/infra/sql/002_m0_grant_function_mi.sql` – template T-SQL to create the MI user (`CREATE USER FROM EXTERNAL PROVIDER`) and grant least-privilege rights to the tables above.
- `functions/infra/sql/003_m1_goaudits_reports.sql` – creates `dbo.GoAuditsReports (GoAuditsReportId nvarchar(100) PK, CompletedAtUtc datetime2 NOT NULL, CertificationNumber nvarchar(100) NULL, JobRunId uniqueidentifier NOT NULL, IngestedAtUtc datetime2 default SYSUTCDATETIME())`.
- `dbo.GoAuditsReportAnswers` (M1.5) is expected in the target database with columns:
  - `GoAuditsReportId NVARCHAR(100)` (PK part, FK to GoAuditsReports)
  - `QuestionKey NVARCHAR(256)` (PK part)
  - `AnswerValue NVARCHAR(MAX)`
  - `Section NVARCHAR(200) NULL`
  - `QuestionText NVARCHAR(1000) NULL`
  - `JobRunId UNIQUEIDENTIFIER`
  - `IngestedAtUtc DATETIME2 default SYSUTCDATETIME()`
  - PK `(GoAuditsReportId, QuestionKey)`
  (Schema created in the target environment alongside the M1.5 rollout.)

## Jobs vs legacy Functions
- The old Azure Functions timer folders remain in the repo but are retired in production.
- ACA Jobs execute plain Node scripts from `functions/src/jobs/` using images built from the Dockerfiles. There is no Functions runtime involvement in production.
