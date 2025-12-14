# Backfill Runbook (GoAudits M1 + M1.5)

Goal: safely re-run ingestion from an earlier date and let enrichment catch up without duplicating data. Idempotency and ProcessedItems make reruns safe, but be mindful of API cost and scheduling.

## Prerequisites
- Confirm SQL and Key Vault RBAC as per `02-azure-resources-and-rbac.md`.
- Ensure ACA Jobs exist (`job-goaudits-ingest-uks`, `job-goaudits-enrich-uks`) and that you know the current image tags.

## Step 1: Rewind the ingestion watermark (if needed)
Set `JobWatermark` for `GoAuditsIngestion` to the desired start date (UTC). Example to backfill from 2025-10-01:
```sql
MERGE dbo.JobWatermark AS target
USING (SELECT 'GoAuditsIngestion' AS JobName, CAST('2025-10-01T00:00:00Z' AS DATETIME2) AS WatermarkUtc) AS src
ON target.JobName = src.JobName
WHEN MATCHED THEN UPDATE SET WatermarkUtc = src.WatermarkUtc, UpdatedUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT (JobName, WatermarkUtc) VALUES (src.JobName, src.WatermarkUtc);
```
(Alternatively, use `GOAUDITS_START_DATE` env var for a one-off run without changing the watermark row.)

## Step 2: Run ingestion on-demand
```powershell
az containerapp job start `
  --name job-goaudits-ingest-uks `
  --resource-group rg-mcs-scheduled-workers-production
```
Monitor with:
```powershell
az containerapp job execution list `
  --name job-goaudits-ingest-uks `
  --resource-group rg-mcs-scheduled-workers-production `
  -o table
```
Validate in SQL:
```sql
SELECT TOP (20) JobName, Status, RunStartedUtc, RunCompletedUtc, Message
FROM dbo.JobRunHistory
ORDER BY RunStartedUtc DESC;

SELECT COUNT(*) AS Reports FROM dbo.GoAuditsReports;
```

## Step 3: Speed up enrichment temporarily (optional)
- Bump batch size (temporary) via env var:
  ```powershell
  az containerapp job update `
    --name job-goaudits-enrich-uks `
    --resource-group rg-mcs-scheduled-workers-production `
    --environment-variables GOAUDITS_ENRICH_BATCH_SIZE=200 GOAUDITS_ENRICH_CONCURRENCY=2
  ```
- Optionally trigger multiple on-demand runs:
  ```powershell
  az containerapp job start `
    --name job-goaudits-enrich-uks `
    --resource-group rg-mcs-scheduled-workers-production
  ```
Idempotency prevents duplicates (ProcessedItems per job), but larger batches increase API load—monitor rate limits.

## Step 4: Monitor progress
SQL checks:
```sql
SELECT COUNT(*) AS Reports FROM dbo.GoAuditsReports;
SELECT COUNT(*) AS Answers FROM dbo.GoAuditsReportAnswers;
SELECT TOP (20) JobName, Status, RunStartedUtc, RunCompletedUtc, Message
FROM dbo.JobRunHistory
ORDER BY RunStartedUtc DESC;
```
Log Analytics (recent ingestion/enrichment logs):
```kusto
ContainerAppConsoleLogs_CL
| where ContainerJobName_s in ("job-goaudits-ingest-uks","job-goaudits-enrich-uks")
| top 50 by TimeGenerated desc
| project TimeGenerated, ContainerJobName_s, Log_s
```

## Step 5: Return to steady-state
- Reset enrichment batch/concurrency if modified:
  ```powershell
  az containerapp job update `
    --name job-goaudits-enrich-uks `
    --resource-group rg-mcs-scheduled-workers-production `
    --environment-variables GOAUDITS_ENRICH_BATCH_SIZE=50 GOAUDITS_ENRICH_CONCURRENCY=1
  ```
- Ensure cron schedules are back to:
  - Ingestion: `0 * * * *`
  - Enrichment: `15 * * * *`

## Safety notes
- ProcessedItems (per job) prevents double-inserts; watermark prevents re-reading old summaries unless intentionally rewound.
- Avoid lowering `GOAUDITS_START_DATE` and rewinding the watermark simultaneously unless needed—use one or the other.
- Auth failures (401/403) stop the job; other per-report errors are counted and do not halt the batch. Review `JobRunHistory.Message` and logs for `failedCount`/`certMissingCount`.
