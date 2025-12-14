# Observability and Proof of Life

## SQL evidence
Run against the target database.

- Recent job runs (heartbeat, ingestion, enrichment):
  ```sql
  SELECT TOP (50)
      JobName, Status, RunStartedUtc, RunCompletedUtc, Message, CorrelationId
  FROM dbo.JobRunHistory
  WHERE JobName IN ('ACAHeartbeat', 'GoAuditsIngestion', 'GoAuditsEnrichment')
  ORDER BY RunStartedUtc DESC;
  ```
- Current watermark for GoAudits:
  ```sql
  SELECT JobName, WatermarkUtc, UpdatedUtc
  FROM dbo.JobWatermark
  WHERE JobName = 'GoAuditsIngestion';
  ```
- Counts of ingested items and ledger entries:
  ```sql
  SELECT COUNT(*) AS GoAuditsReports FROM dbo.GoAuditsReports;
  SELECT COUNT(*) AS ProcessedItemsLedger
  FROM dbo.ProcessedItems
  WHERE JobName = 'GoAuditsIngestion';

  SELECT COUNT(*) AS GoAuditsReportAnswers FROM dbo.GoAuditsReportAnswers;
  SELECT COUNT(*) AS ProcessedEnrichment
  FROM dbo.ProcessedItems
  WHERE JobName = 'GoAuditsEnrichment';
  ```

## Log Analytics queries (structured JSON logs)
ACA Job console logs land in Log Analytics. Use `ContainerJobName_s` to filter.

- Heartbeat job logs:
  ```kusto
  ContainerAppConsoleLogs_CL
  | where ContainerJobName_s == "job-mcs-aca-heartbeat-prod-uks"
  | where Log_s contains "ACAHeartbeat"
  | project TimeGenerated, Log_s
  | top 50 by TimeGenerated desc
  ```
- GoAudits ingestion logs:
  ```kusto
  ContainerAppConsoleLogs_CL
  | where ContainerJobName_s == "job-goaudits-ingest-uks"
  | where Log_s contains "GoAuditsIngestion"
  | project TimeGenerated, Log_s
  | top 50 by TimeGenerated desc
  ```
- GoAudits enrichment logs:
  ```kusto
  ContainerAppConsoleLogs_CL
  | where ContainerJobName_s == "job-goaudits-enrich-uks"
  | where Log_s contains "GoAuditsEnrichment"
  | project TimeGenerated, Log_s
  | top 50 by TimeGenerated desc
  ```
  To tie a specific run end-to-end, filter on a known `jobRunId` from JobRunHistory/SQL:
  ```kusto
  let runId = "<jobRunId>";
  ContainerAppConsoleLogs_CL
  | where Log_s contains runId
  | project TimeGenerated, ContainerJobName_s, Log_s
  | order by TimeGenerated asc
  ```

## Structured log format (stdout/stderr)
All jobs emit single-line JSON. Fields you should see:
- `jobName`, `jobRunId`
- `startedAtUtc`, `completedAtUtc`
- `status` (`Succeeded` or `Failed`)
- `counts`:
  - Ingestion: `fetched`, `eligible`, `ingested`, `skipped`, `alreadyProcessed`, `pages`
  - Enrichment: `selected`, `processed`, `certUpdatedCount`, `answersInsertedCount`, `markedProcessedCount`, `certMissingCount`, `failedCount`, `authFatal`
  - Scoring: `selected`, `processed`, `skipped`, `alreadyProcessed`, `findingsInsertedCount`, `majorCountTotal`, `minorCountTotal`
- GoAudits ingestion also logs `start_date` and `end_date`
- GoAudits scoring also logs `ruleset.name` and `ruleset.version`
- `error` present on failures

### Example (success, GoAudits)
```
{"jobName":"GoAuditsIngestion","jobRunId":"<guid>","startedAtUtc":"2024-08-01T00:00:00.000Z","completedAtUtc":"2024-08-01T00:03:00.000Z","status":"Succeeded","counts":{"fetched":120,"eligible":50,"ingested":50,"skipped":70,"alreadyProcessed":0,"pages":1},"start_date":"2024-07-31","end_date":"2024-08-01"}
```

### Example (success, GoAudits enrichment)
```
{"jobName":"GoAuditsEnrichment","jobRunId":"<guid>","completedAtUtc":"2024-08-01T00:20:00.000Z","status":"Succeeded","counts":{"selected":50,"processed":48,"certUpdatedCount":30,"answersInsertedCount":480,"markedProcessedCount":48,"certMissingCount":18,"failedCount":2,"authFatal":false}}
```

### Example (failure, heartbeat)
```
{"jobName":"ACAHeartbeat","jobRunId":"<guid>","startedAtUtc":"2024-08-01T00:00:00.000Z","completedAtUtc":"2024-08-01T00:00:01.000Z","status":"Failed","counts":{},"error":"SQL_SERVER environment variable is not set."}
```

## Reporting / export helpers (SQL)
These are convenience queries for spot checks (not full BI models).
- Latest answers for a given report:
  ```sql
  SELECT ra.GoAuditsReportId, ra.QuestionKey, ra.QuestionText, ra.Section, ra.AnswerValue, ra.IngestedAtUtc
  FROM dbo.GoAuditsReportAnswers ra
  WHERE ra.GoAuditsReportId = '<report-id>'
  ORDER BY ra.QuestionKey;
  ```
- “PV flat” style extract (one row per report, answers pivoted by key) for small sets:
  ```sql
  DECLARE @keys TABLE (QuestionKey NVARCHAR(256));
  INSERT INTO @keys (QuestionKey)
  SELECT DISTINCT TOP (50) QuestionKey FROM dbo.GoAuditsReportAnswers; -- limit to avoid huge pivots

  DECLARE @cols NVARCHAR(MAX) = STRING_AGG(QUOTENAME(QuestionKey), ',') FROM @keys;
  DECLARE @sql NVARCHAR(MAX) = '
    SELECT *
    FROM (
      SELECT GoAuditsReportId, QuestionKey, AnswerValue
      FROM dbo.GoAuditsReportAnswers
      WHERE QuestionKey IN (SELECT QuestionKey FROM @keys)
    ) AS src
    PIVOT (MAX(AnswerValue) FOR QuestionKey IN (' + @cols + ')) AS p;';

  EXEC sp_executesql @sql, N'@keys TABLE (QuestionKey NVARCHAR(256)) READONLY', @keys=@keys;
  ```
  This is intentionally limited; for larger exports use an external tool/ETL.

## M2 – Scoring (GoAuditsScoring)

### SQL evidence
- Recent scoring runs:
  ```sql
  SELECT TOP (50)
      JobName, Status, RunStartedUtc, RunCompletedUtc, Message, CorrelationId
  FROM dbo.JobRunHistory
  WHERE JobName = 'GoAuditsScoring'
  ORDER BY RunStartedUtc DESC;
  ```
- Derived table counts:
  ```sql
  SELECT COUNT(*) AS Scores   FROM dbo.GoAuditsScores;
  SELECT COUNT(*) AS Findings FROM dbo.GoAuditsFindings;

  SELECT COUNT(*) AS ProcessedScoring
  FROM dbo.ProcessedItems
  WHERE JobName = 'GoAuditsScoring';
  ```
- Duplicate safety proof (should return 0 rows):
  ```sql
  SELECT TOP (20)
    GoAuditsReportId, RuleSetName, RuleSetVersion, COUNT(*) AS DuplicateRows
  FROM dbo.GoAuditsScores
  GROUP BY GoAuditsReportId, RuleSetName, RuleSetVersion
  HAVING COUNT(*) > 1
  ORDER BY COUNT(*) DESC;
  ```
- Progress proof (eligible vs scored vs remaining) with ruleset/version variables:
  ```sql
  DECLARE @RuleSetName    NVARCHAR(50) = N'PV';
  DECLARE @RuleSetVersion NVARCHAR(20) = N'v1';

  WITH Eligible AS (
    SELECT DISTINCT a.GoAuditsReportId
    FROM dbo.GoAuditsReportAnswers a
  ),
  Scored AS (
    SELECT s.GoAuditsReportId
    FROM dbo.GoAuditsScores s
    WHERE s.RuleSetName = @RuleSetName
      AND s.RuleSetVersion = @RuleSetVersion
  )
  SELECT
    (SELECT COUNT(*) FROM Eligible) AS EligibleReportsWithAnswers,
    (SELECT COUNT(*) FROM Scored)   AS ScoredReportsThisRuleSet,
    (SELECT COUNT(*) FROM Eligible e WHERE NOT EXISTS (SELECT 1 FROM Scored s WHERE s.GoAuditsReportId = e.GoAuditsReportId)) AS RemainingToScore;
  ```
- View samples (BI-ready):
  ```sql
  SELECT TOP (20) *
  FROM dbo.vw_GoAuditsReportScores
  ORDER BY ScoredAtUtc DESC;

  SELECT TOP (20) *
  FROM dbo.vw_GoAuditsFindingsDetail
  ORDER BY CreatedUtc DESC;
  ```

### Log Analytics queries (structured JSON logs)
Scoring job logs land in Log Analytics under `ContainerJobName_s == "job-goaudits-score-uks"`. Filter to JSON lines then `parse_json(Log_s)`.

- Raw last 50 lines:
  ```kusto
  ContainerAppConsoleLogs_CL
  | where ContainerJobName_s == "job-goaudits-score-uks"
  | project TimeGenerated, Log_s
  | top 50 by TimeGenerated desc
  ```
- Parsed end-of-run JSON summary (safe fields only):
  ```kusto
  ContainerAppConsoleLogs_CL
  | where ContainerJobName_s == "job-goaudits-score-uks"
  | where Log_s startswith "{"
  | extend j = parse_json(Log_s)
  | where tostring(j.jobName) == "GoAuditsScoring"
  | project
      TimeGenerated,
      jobRunId = tostring(j.jobRunId),
      status   = tostring(j.status),
      ruleSet  = tostring(j.ruleset.name),
      version  = tostring(j.ruleset.version),
      selected = toint(j.counts.selected),
      processed = toint(j.counts.processed),
      alreadyProcessed = toint(j.counts.alreadyProcessed),
      findingsInserted = toint(j.counts.findingsInsertedCount),
      majorTotal = toint(j.counts.majorCountTotal),
      minorTotal = toint(j.counts.minorCountTotal)
  | order by TimeGenerated desc
  ```
