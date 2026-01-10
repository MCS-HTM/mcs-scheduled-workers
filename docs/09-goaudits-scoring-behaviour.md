# GoAudits Scoring Behaviour (M2)

## Overview
- Scope: scoring and SQL outputs only (no emails, SharePoint, or Power BI build here).
- Inputs: answers already in SQL (`dbo.GoAuditsReportAnswers`); no direct API calls during scoring.
- Outputs: scores and findings in SQL plus a tracker-ready view.

## What gets scored
- The scoring job reads GoAudits answers from SQL and applies versioned JSON rules.
- Each ruleset/version is file-based (`functions/src/rules/<ruleset>.<version>.json`), so you can re-score with a newer version without altering historic results.

## Rulesets and versions
- PV: current version `v2`.
- HeatPump: current version `v3`.
- Versioning: numeric ordering of `vN` controls “latest” selection in reporting (v3 > v2 > v1).

## Important HeatPump v3 fix (double negative)
- Question keys `524/525/526` (“gap in external pipe insulation >1m”) are defect-style questions.
- HeatPump `v3` treats `YES` as non-compliant and `NO` as compliant to avoid false `Major` outcomes that occurred when `NO` was treated as non-compliant.

## Azure jobs (production)
- PV scoring: ACA Job `job-goaudits-score-uks`, cron `30 * * * *`, env `GOAUDITS_RULESET=PV`, `GOAUDITS_RULESET_VERSION=v2`, batch size default `100`.
- HeatPump scoring: ACA Job `job-goaudits-score-hp-uks`, cron `35 * * * *`, env `GOAUDITS_RULESET=HeatPump`, `GOAUDITS_RULESET_VERSION=v3`, batch size default `100`.
- Identity: both run with the UAMI (no secrets) and write `JobRunHistory`.

## Reporting output (tracker feed)
- View: `dbo.vw_GoAuditsAssessmentScoreFinal`.
- Includes both `GoAuditsReportId` and `AssessmentId` (same value) to avoid HP/PV mix-ups and handle multiple audits with the same cert/date.
- Picks the latest score per (report, ruleset) by numeric `vN` ordering of `RuleSetVersion`, then `ScoredAtUtc`.
- Columns: Address, Date of Assessment, Type, Certificate number, Score, WorstBucket, WorstQuestionKey, WorstFindingCode, WorstQuestionText, Major Non compliances, Minor Non compliances, RuleSetName, RuleSetVersion, plus the IDs above.

## Non-compliance text (finding-level)
- Each rule may provide severity-specific text: `finding.majorNonCompliantText` or `finding.minorNonCompliantText` (only one populated per rule).
- Persisted on `dbo.GoAuditsFindings` as `MajorNonCompliantText` / `MinorNonCompliantText` alongside the question-level finding.
- Scoring writes text on insert; if a finding already exists, it updates with `COALESCE(...)` to backfill text without changing `CreatedUtc`, `AnswerValue`, `FindingSeverity`, or `FindingCode`.

## Reporting rollup (non-compliance lists)
- `dbo.vw_GoAuditsAssessmentScoreFinal` includes `Major Non compliances` and `Minor Non compliances`.
- Each list is `STRING_AGG(..., CHAR(13)+CHAR(10)) WITHIN GROUP (ORDER BY QuestionKey)` over findings tied to the latest score row.
- Newline delimiter is CRLF (`CHAR(13)+CHAR(10)`).

## Backfill procedure (non-compliance text)
- Path of least resistance: delete scores/findings/processed markers for PV v2 + HeatPump v3, then rerun scoring until text is populated.
- Reference runbook SQL: `functions/infra/sql/009_m2_backfill_non_compliance_text_runbook.sql`.
- Proof queries (scores + missing text):
  ```sql
  SELECT RuleSetName, RuleSetVersion, COUNT(*) AS ScoreRows
  FROM dbo.GoAuditsScores
  WHERE (RuleSetName='HeatPump' AND RuleSetVersion='v3')
     OR (RuleSetName='PV' AND RuleSetVersion='v2')
  GROUP BY RuleSetName, RuleSetVersion;

  SELECT 'HeatPump v3' AS RuleSet, COUNT(*) TotalFindings,
  SUM(CASE WHEN (FindingSeverity='Major' AND MajorNonCompliantText IS NULL)
        OR (FindingSeverity='Minor' AND MinorNonCompliantText IS NULL) THEN 1 ELSE 0 END) AS FindingsMissingText
  FROM dbo.GoAuditsFindings
  WHERE RuleSetName='HeatPump' AND RuleSetVersion='v3';

  SELECT 'PV v2' AS RuleSet, COUNT(*) TotalFindings,
  SUM(CASE WHEN (FindingSeverity='Major' AND MajorNonCompliantText IS NULL)
        OR (FindingSeverity='Minor' AND MinorNonCompliantText IS NULL) THEN 1 ELSE 0 END) AS FindingsMissingText
  FROM dbo.GoAuditsFindings
  WHERE RuleSetName='PV' AND RuleSetVersion='v2';
  ```

## Proof / sign-off queries (copy/paste)
- Last 24h job runs (ingestion, enrichment, scoring):
  ```sql
  SELECT TOP (200)
      JobName, Status, RunStartedUtc, RunCompletedUtc, Message, CorrelationId
  FROM dbo.JobRunHistory
  WHERE JobName IN ('GoAuditsIngestion', 'GoAuditsEnrichment', 'GoAuditsScoring')
    AND RunStartedUtc >= DATEADD(day, -1, SYSUTCDATETIME())
  ORDER BY RunStartedUtc DESC;
  ```
- Counts (reports, answers, scores, findings):
  ```sql
  SELECT COUNT(*) AS Reports  FROM dbo.GoAuditsReports;
  SELECT COUNT(*) AS Answers  FROM dbo.GoAuditsReportAnswers;
  SELECT COUNT(*) AS Scores   FROM dbo.GoAuditsScores;
  SELECT COUNT(*) AS Findings FROM dbo.GoAuditsFindings;
  ```
- Coverage checks (expected: PV v2, HeatPump v3):
  ```sql
  SELECT RuleSetName, RuleSetVersion, COUNT(*) AS Scores
  FROM dbo.GoAuditsScores
  GROUP BY RuleSetName, RuleSetVersion
  ORDER BY RuleSetName, RuleSetVersion;
  ```
- Duplicate safety (scores should be unique per report/ruleset/version):
  ```sql
  SELECT GoAuditsReportId, RuleSetName, RuleSetVersion, COUNT(*) AS Dupes
  FROM dbo.GoAuditsScores
  GROUP BY GoAuditsReportId, RuleSetName, RuleSetVersion
  HAVING COUNT(*) > 1
  ORDER BY COUNT(*) DESC;
  ```
- Final tracker view sample (latest per report/ruleset):
  ```sql
  SELECT TOP (50) *
  FROM dbo.vw_GoAuditsAssessmentScoreFinal
  ORDER BY [Date of Assessment] DESC, RuleSetName;
  ```
