# Azure Resources and RBAC

## Resources in use (production names)
- Resource group: `rg-mcs-scheduled-workers-production`
- Container Apps environment (UK South): `cae-mcs-scheduled-workers-prod-uks`
- Log Analytics workspace: `law-mcs-scheduled-workers-prod-uks`
- Azure Container Registry: `acrmcsschedwrkprod.azurecr.io`
- Key Vault: `kv-mcsschedwrkprod-uks`
- User-assigned managed identity (UAMI): `id-mcs-scheduled-workers-aca-prod-uks` (used for SQL, Key Vault, and ACR pulls)
- ACA jobs: `job-mcs-aca-heartbeat-prod-uks`, `job-goaudits-ingest-uks`, `job-goaudits-enrich-uks`
- Azure SQL database (server/database names provided via env vars at runtime; no passwords stored).
- Log Analytics holds all stdout/stderr from ACA Jobs (primary observability surface).

## RBAC requirements
- **ACR pull**: assign `AcrPull` to the UAMI against the registry.
  ```powershell
  $uamiId = "<uami-resource-id>"
  $acrId = (az acr show -n acrmcsschedwrkprod --query id -o tsv)
  az role assignment create --assignee $uamiId --role AcrPull --scope $acrId
  ```
- **Key Vault**:
  - UAMI: `Key Vault Secrets User`.
  - Humans who set the GoAudits secret: `Key Vault Secrets Officer`.
  ```powershell
  $kvId = (az keyvault show -n kv-mcsschedwrkprod-uks --query id -o tsv)
  az role assignment create --assignee $uamiId --role "Key Vault Secrets User" --scope $kvId
  # For humans:
  az role assignment create --assignee "<user-object-id>" --role "Key Vault Secrets Officer" --scope $kvId
  ```
- **SQL (Managed Identity login)**:
  Use T-SQL to create the MI user and grant least privilege using the Managed Identity name (no generated username). For ACA Jobs we use UAMI `id-mcs-scheduled-workers-aca-prod-uks`. Run `CREATE USER [id-mcs-scheduled-workers-aca-prod-uks] FROM EXTERNAL PROVIDER;` then apply GRANTs.
  ```sql
  DECLARE @ManagedIdentityName SYSNAME = N'id-mcs-scheduled-workers-aca-prod-uks';
  DECLARE @UserName SYSNAME = @ManagedIdentityName;
  DECLARE @UserNameEscaped NVARCHAR(300) = REPLACE(@UserName, ']', ']]');

  IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = @UserName)
  BEGIN
      DECLARE @sql NVARCHAR(MAX) = N'CREATE USER [' + @UserNameEscaped + N'] FROM EXTERNAL PROVIDER;';
      EXEC (@sql);
  END

  DECLARE @grantSql NVARCHAR(MAX) = N'
  GRANT SELECT, INSERT, UPDATE ON dbo.JobRunHistory   TO [' + @UserNameEscaped + N'];
  GRANT SELECT, INSERT, UPDATE ON dbo.JobWatermark    TO [' + @UserNameEscaped + N'];
  GRANT SELECT, INSERT         ON dbo.ProcessedItems  TO [' + @UserNameEscaped + N'];
  GRANT SELECT, INSERT         ON dbo.GoAuditsReports TO [' + @UserNameEscaped + N'];
  GRANT SELECT, INSERT         ON dbo.GoAuditsReportAnswers TO [' + @UserNameEscaped + N'];
  ';
  EXEC (@grantSql);
  ```

## Networking / firewall
- SQL must allow connectivity from ACA Jobs. Either configure private networking or enable the “Allow Azure services and resources to access this server” setting on the SQL server. No SQL credentials are used; access is token-based via Managed Identity.

## Why these resources
- ACA environment provides scheduled jobs with Log Analytics integration.
- ACR hosts images built from the repo’s Dockerfiles.
- Key Vault holds the GoAudits bearer token (only secrets location).
- UAMI centralises identity for SQL, Key Vault, and ACR pull.
