/*
M2 – Update reporting view for final assessment scores
- Exposes the latest score per report/ruleset (numerically ordered versions, then score time)
- Includes both GoAuditsReportId and AssessmentId (same value)
*/

GO

CREATE OR ALTER VIEW dbo.vw_GoAuditsAssessmentScoreFinal
AS
WITH RankedScores AS (
    SELECT
        s.GoAuditsReportId,
        s.RuleSetName,
        s.RuleSetVersion,
        s.ScoreValue,
        s.Outcome,
        s.ScoredAtUtc,
        r.CompletedAtUtc,
        r.CertificationNumber,
        TRY_CONVERT(INT, CASE WHEN s.RuleSetVersion LIKE '[vV]%' THEN SUBSTRING(s.RuleSetVersion, 2, 10) ELSE s.RuleSetVersion END) AS RuleSetVersionNumber,
        ROW_NUMBER() OVER (
            PARTITION BY s.GoAuditsReportId, s.RuleSetName
            ORDER BY
                TRY_CONVERT(INT, CASE WHEN s.RuleSetVersion LIKE '[vV]%' THEN SUBSTRING(s.RuleSetVersion, 2, 10) ELSE s.RuleSetVersion END) DESC,
                s.ScoredAtUtc DESC
        ) AS rn
    FROM dbo.GoAuditsScores s
    INNER JOIN dbo.GoAuditsReports r
        ON r.GoAuditsReportId = s.GoAuditsReportId
),
LatestScores AS (
    SELECT *
    FROM RankedScores
    WHERE rn = 1
),
WorstFinding AS (
    SELECT
        f.GoAuditsReportId,
        f.RuleSetName,
        f.RuleSetVersion,
        f.QuestionKey,
        f.FindingCode,
        f.FindingSeverity,
        a.QuestionText,
        ROW_NUMBER() OVER (
            PARTITION BY f.GoAuditsReportId, f.RuleSetName, f.RuleSetVersion
            ORDER BY
                CASE f.FindingSeverity WHEN 'Major' THEN 1 WHEN 'Minor' THEN 2 ELSE 3 END,
                f.CreatedUtc DESC,
                f.QuestionKey
        ) AS rn
    FROM dbo.GoAuditsFindings f
    LEFT JOIN dbo.GoAuditsReportAnswers a
        ON a.GoAuditsReportId = f.GoAuditsReportId
       AND a.QuestionKey = f.QuestionKey
)
SELECT
    l.GoAuditsReportId,
    AssessmentId = l.GoAuditsReportId,
    Address = addr.Address,
    [Date of Assessment] = CAST(l.CompletedAtUtc AS DATE),
    [Type] = COALESCE(typ.AssessmentType, l.RuleSetName),
    [Certificate number] = l.CertificationNumber,
    Score = l.ScoreValue,
    WorstBucket = COALESCE(wf.FindingSeverity, l.Outcome),
    WorstQuestionKey = wf.QuestionKey,
    WorstFindingCode = wf.FindingCode,
    WorstQuestionText = wf.QuestionText,
    l.RuleSetName,
    l.RuleSetVersion
FROM LatestScores l
OUTER APPLY (
    SELECT TOP (1) AnswerValue AS Address
    FROM dbo.GoAuditsReportAnswers a
    WHERE a.GoAuditsReportId = l.GoAuditsReportId
      AND (a.QuestionText LIKE '%address%' OR a.QuestionKey LIKE '%address%')
) addr
OUTER APPLY (
    SELECT TOP (1) AnswerValue AS AssessmentType
    FROM dbo.GoAuditsReportAnswers a
    WHERE a.GoAuditsReportId = l.GoAuditsReportId
      AND (a.QuestionText LIKE '%type%' OR a.QuestionKey LIKE '%type%')
) typ
LEFT JOIN WorstFinding wf
    ON wf.GoAuditsReportId = l.GoAuditsReportId
   AND wf.RuleSetName = l.RuleSetName
   AND wf.RuleSetVersion = l.RuleSetVersion
   AND wf.rn = 1;

GO
