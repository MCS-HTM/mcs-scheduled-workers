/*
M1 – GoAudits ingestion (minimal metadata only)
*/

IF OBJECT_ID('dbo.GoAuditsReports', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.GoAuditsReports (
        GoAuditsReportId     NVARCHAR(100) NOT NULL,
        CompletedAtUtc       DATETIME2     NOT NULL,
        CertificationNumber  NVARCHAR(100) NULL,
        JobRunId             UNIQUEIDENTIFIER NOT NULL,
        IngestedAtUtc        DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_GoAuditsReports PRIMARY KEY (GoAuditsReportId)
    );
END;
