/*
M2 – GoAudits scoring outputs (findings and scores)
*/

-- 1) GoAuditsFindings
IF OBJECT_ID('dbo.GoAuditsFindings', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.GoAuditsFindings
    (
        GoAuditsReportId   NVARCHAR(100) NOT NULL,
        RuleSetName        NVARCHAR(50)  NOT NULL,
        RuleSetVersion     NVARCHAR(20)  NOT NULL,
        QuestionKey        NVARCHAR(256) NOT NULL,
        AnswerValue        NVARCHAR(MAX) NULL,
        FindingSeverity    NVARCHAR(10)  NOT NULL, -- Major / Minor
        FindingCode        NVARCHAR(50)  NULL,
        JobRunId           UNIQUEIDENTIFIER NOT NULL,
        CreatedUtc         DATETIME2(3) NOT NULL CONSTRAINT DF_GoAuditsFindings_CreatedUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_GoAuditsFindings PRIMARY KEY CLUSTERED (GoAuditsReportId, RuleSetName, RuleSetVersion, QuestionKey),
        CONSTRAINT FK_GoAuditsFindings_Report FOREIGN KEY (GoAuditsReportId) REFERENCES dbo.GoAuditsReports(GoAuditsReportId),
        CONSTRAINT CK_GoAuditsFindings_Severity CHECK (FindingSeverity IN ('Major','Minor'))
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_GoAuditsFindings_RuleSet_Severity'
      AND object_id = OBJECT_ID('dbo.GoAuditsFindings')
)
BEGIN
    CREATE INDEX IX_GoAuditsFindings_RuleSet_Severity
        ON dbo.GoAuditsFindings (RuleSetName, RuleSetVersion, FindingSeverity)
        INCLUDE (FindingCode, CreatedUtc);
END
GO

-- 2) GoAuditsScores
IF OBJECT_ID('dbo.GoAuditsScores', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.GoAuditsScores
    (
        GoAuditsReportId   NVARCHAR(100) NOT NULL,
        RuleSetName        NVARCHAR(50)  NOT NULL,
        RuleSetVersion     NVARCHAR(20)  NOT NULL,
        MajorCount         INT NOT NULL CONSTRAINT DF_GoAuditsScores_MajorCount DEFAULT (0),
        MinorCount         INT NOT NULL CONSTRAINT DF_GoAuditsScores_MinorCount DEFAULT (0),
        ScoreValue         NVARCHAR(50)  NULL,
        Outcome            NVARCHAR(20)  NOT NULL,
        JobRunId           UNIQUEIDENTIFIER NOT NULL,
        ScoredAtUtc        DATETIME2(3) NOT NULL CONSTRAINT DF_GoAuditsScores_ScoredAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_GoAuditsScores PRIMARY KEY CLUSTERED (GoAuditsReportId, RuleSetName, RuleSetVersion),
        CONSTRAINT FK_GoAuditsScores_Report FOREIGN KEY (GoAuditsReportId) REFERENCES dbo.GoAuditsReports(GoAuditsReportId)
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_GoAuditsScores_RuleSet_Outcome_ScoredAtUtc'
      AND object_id = OBJECT_ID('dbo.GoAuditsScores')
)
BEGIN
    CREATE INDEX IX_GoAuditsScores_RuleSet_Outcome_ScoredAtUtc
        ON dbo.GoAuditsScores (RuleSetName, RuleSetVersion, Outcome, ScoredAtUtc)
        INCLUDE (MajorCount, MinorCount, ScoreValue);
END
GO
