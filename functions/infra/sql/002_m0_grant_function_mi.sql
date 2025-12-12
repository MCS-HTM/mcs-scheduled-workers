-- M0: Grant Function App Managed Identity access to write run history and manage watermark/ledger
-- Replace the value below with the Function App's Managed Identity PrincipalId (GUID)
DECLARE @PrincipalId UNIQUEIDENTIFIER = '47920823-cccb-48a5-b1df-d76aa7389def';
DECLARE @UserName SYSNAME = CONCAT('mi_func_mcs_scheduled_workers_prod_', REPLACE(CONVERT(NVARCHAR(36), @PrincipalId), '-', ''));

-- Create the user from external provider (Entra / Managed Identity)
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @UserName)
BEGIN
    DECLARE @sql NVARCHAR(MAX) = N'CREATE USER [' + @UserName + N'] FROM EXTERNAL PROVIDER;';
    EXEC (@sql);
END

-- Grant minimal permissions for M0
DECLARE @grantSql NVARCHAR(MAX) =
N'
GRANT SELECT, INSERT, UPDATE ON dbo.JobRunHistory TO [' + @UserName + N'];
GRANT SELECT, INSERT, UPDATE ON dbo.JobWatermark  TO [' + @UserName + N'];
GRANT SELECT, INSERT            ON dbo.ProcessedItems TO [' + @UserName + N'];
';

EXEC (@grantSql);
