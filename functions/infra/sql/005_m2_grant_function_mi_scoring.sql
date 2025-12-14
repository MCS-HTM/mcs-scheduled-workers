-- M2: Grant Managed Identity access for scoring outputs (findings and scores)
DECLARE @ManagedIdentityName SYSNAME = N'id-mcs-scheduled-workers-aca-prod-uks';
DECLARE @UserName SYSNAME = @ManagedIdentityName;
DECLARE @UserNameEscaped NVARCHAR(300) = REPLACE(@UserName, ']', ']]'); -- escape bracket if present

-- Create the user from external provider (Entra / Managed Identity)
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @UserName)
BEGIN
    DECLARE @sql NVARCHAR(MAX) = N'CREATE USER [' + @UserNameEscaped + N'] FROM EXTERNAL PROVIDER;';
    EXEC (@sql);
END

-- Grant additional permissions for M2 scoring outputs
DECLARE @grantSql NVARCHAR(MAX) =
N'
GRANT SELECT, INSERT              ON dbo.GoAuditsFindings TO [' + @UserNameEscaped + N'];
GRANT SELECT, INSERT, UPDATE     ON dbo.GoAuditsScores   TO [' + @UserNameEscaped + N'];
';

EXEC (@grantSql);
