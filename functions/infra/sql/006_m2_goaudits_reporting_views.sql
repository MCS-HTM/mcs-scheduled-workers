/*
M2 – Reporting views for GoAudits scoring outputs
Creates lightweight views for BI/exports without pivoting answers.
*/

GO

-- Scores per report per ruleset/version
CREATE OR ALTER VIEW dbo.vw_GoAuditsReportScores
AS
SELECT
    s.GoAuditsReportId,
    r.CompletedAtUtc,
    r.CertificationNumber,
    s.RuleSetName,
    s.RuleSetVersion,
    s.MajorCount,
    s.MinorCount,
    s.Outcome,
    s.ScoreValue,
    s.ScoredAtUtc,
    s.JobRunId AS ScoringJobRunId,
    r.JobRunId AS IngestJobRunId,
    r.IngestedAtUtc
FROM dbo.GoAuditsScores s
INNER JOIN dbo.GoAuditsReports r
    ON r.GoAuditsReportId = s.GoAuditsReportId;

GO

-- Findings detail joined to report metadata and answer context
CREATE OR ALTER VIEW dbo.vw_GoAuditsFindingsDetail
AS
SELECT
    f.GoAuditsReportId,
    r.CompletedAtUtc,
    r.CertificationNumber,
    f.RuleSetName,
    f.RuleSetVersion,
    f.QuestionKey,
    a.Section,
    a.QuestionText,
    f.AnswerValue,
    f.FindingSeverity,
    f.FindingCode,
    f.CreatedUtc,
    f.JobRunId AS ScoringJobRunId
FROM dbo.GoAuditsFindings f
INNER JOIN dbo.GoAuditsReports r
    ON r.GoAuditsReportId = f.GoAuditsReportId
LEFT JOIN dbo.GoAuditsReportAnswers a
    ON a.GoAuditsReportId = f.GoAuditsReportId
   AND a.QuestionKey = f.QuestionKey;

GO
