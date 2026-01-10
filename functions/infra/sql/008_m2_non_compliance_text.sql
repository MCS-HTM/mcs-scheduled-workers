/*
M2 – Add non-compliance text to findings and assessment rollup
*/

-- 1) GoAuditsFindings: add non-compliance text columns
IF COL_LENGTH('dbo.GoAuditsFindings', 'MajorNonCompliantText') IS NULL
BEGIN
    ALTER TABLE dbo.GoAuditsFindings
        ADD MajorNonCompliantText NVARCHAR(MAX) NULL;
END
GO

IF COL_LENGTH('dbo.GoAuditsFindings', 'MinorNonCompliantText') IS NULL
BEGIN
    ALTER TABLE dbo.GoAuditsFindings
        ADD MinorNonCompliantText NVARCHAR(MAX) NULL;
END
GO

-- 2) Reporting view: include major/minor non-compliance rollups
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
    [Major Non compliances] = majors.MajorNonCompliances,
    [Minor Non compliances] = minors.MinorNonCompliances,
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
OUTER APPLY (
    SELECT STRING_AGG(f.MajorNonCompliantText, CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY f.QuestionKey) AS MajorNonCompliances
    FROM dbo.GoAuditsFindings f
    WHERE f.GoAuditsReportId = l.GoAuditsReportId
      AND f.RuleSetName = l.RuleSetName
      AND f.RuleSetVersion = l.RuleSetVersion
      AND f.FindingSeverity = 'Major'
      AND f.MajorNonCompliantText IS NOT NULL
) majors
OUTER APPLY (
    SELECT STRING_AGG(f.MinorNonCompliantText, CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY f.QuestionKey) AS MinorNonCompliances
    FROM dbo.GoAuditsFindings f
    WHERE f.GoAuditsReportId = l.GoAuditsReportId
      AND f.RuleSetName = l.RuleSetName
      AND f.RuleSetVersion = l.RuleSetVersion
      AND f.FindingSeverity = 'Minor'
      AND f.MinorNonCompliantText IS NOT NULL
) minors
LEFT JOIN WorstFinding wf
    ON wf.GoAuditsReportId = l.GoAuditsReportId
   AND wf.RuleSetName = l.RuleSetName
   AND wf.RuleSetVersion = l.RuleSetVersion
   AND wf.rn = 1;

GO
