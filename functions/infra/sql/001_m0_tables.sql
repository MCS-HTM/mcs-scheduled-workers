/*
M0 Tables
- JobWatermark: per-job watermark state
- JobRunHistory: each execution of a job
- ProcessedItems: idempotency ledger (per job + item key)
*/

-- 1) JobWatermark
IF OBJECT_ID('dbo.JobWatermark', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.JobWatermark
    (
        JobName            NVARCHAR(100)  NOT NULL PRIMARY KEY,
        WatermarkUtc       DATETIME2(0)    NULL,
        UpdatedUtc         DATETIME2(0)    NOT NULL CONSTRAINT DF_JobWatermark_UpdatedUtc DEFAULT (SYSUTCDATETIME())
    );
END
GO

-- 2) JobRunHistory
IF OBJECT_ID('dbo.JobRunHistory', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.JobRunHistory
    (
        RunId              UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_JobRunHistory PRIMARY KEY DEFAULT (NEWID()),
        JobName            NVARCHAR(100)    NOT NULL,
        RunStartedUtc      DATETIME2(0)     NOT NULL CONSTRAINT DF_JobRunHistory_RunStartedUtc DEFAULT (SYSUTCDATETIME()),
        RunCompletedUtc    DATETIME2(0)     NULL,
        Status             NVARCHAR(30)     NOT NULL, -- e.g. Started | Succeeded | Failed
        Message            NVARCHAR(4000)   NULL,
        CorrelationId      NVARCHAR(100)    NULL,
        CreatedUtc         DATETIME2(0)     NOT NULL CONSTRAINT DF_JobRunHistory_CreatedUtc DEFAULT (SYSUTCDATETIME())
    );

    CREATE INDEX IX_JobRunHistory_JobName_RunStartedUtc
        ON dbo.JobRunHistory (JobName, RunStartedUtc DESC);
END
GO

-- 3) ProcessedItems (idempotency ledger)
IF OBJECT_ID('dbo.ProcessedItems', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.ProcessedItems
    (
        JobName            NVARCHAR(100)    NOT NULL,
        ItemKey            NVARCHAR(200)    NOT NULL,
        ProcessedUtc       DATETIME2(0)     NOT NULL CONSTRAINT DF_ProcessedItems_ProcessedUtc DEFAULT (SYSUTCDATETIME()),
        RunId              UNIQUEIDENTIFIER NULL,
        CONSTRAINT PK_ProcessedItems PRIMARY KEY (JobName, ItemKey)
    );

    CREATE INDEX IX_ProcessedItems_ProcessedUtc
        ON dbo.ProcessedItems (ProcessedUtc DESC);
END
GO
