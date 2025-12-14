# mcs-scheduled-workers

Containerised scheduled worker platform for MCS. Milestones:
- **M0**: table + identity scaffolding (JobRunHistory, JobWatermark, ProcessedItems).
- **M1**: GoAudits ingestion (summary endpoint) landing into `GoAuditsReports`.
- **M1.5**: GoAudits enrichment (details endpoint) populating `GoAuditsReportAnswers` and certificate numbers.

Azure Functions timers were retired due to reliability/observability gaps; everything now runs as Azure Container Apps (ACA) Jobs with User Assigned Managed Identity (UAMI) for SQL, Key Vault, and ACR pulls. Logs go to Log Analytics. No secrets are baked into app settings.

## Reading order (docs)
Start here: `docs/README.md` (index). Key references:
- `docs/00-architecture-overview.md`
- `docs/01-repo-structure.md`
- `docs/02-azure-resources-and-rbac.md`
- `docs/03-deployment-runbook.md`
- `docs/04-observability-and-proof.md`
- `docs/05-goaudits-ingestion-behaviour.md`
- `docs/07-goaudits-enrichment-behaviour.md`
- `docs/08-backfill-runbook.md`
- `docs/06-troubleshooting.md`

## Quickstart: prove it’s working (production names shown as examples)
```powershell
# 1) Manually start ingestion (hourly cron is steady-state)
az containerapp job start `
  --name job-goaudits-ingest-uks `
  --resource-group rg-mcs-scheduled-workers-production

# 2) Manually start enrichment (runs at :15 past steady-state)
az containerapp job start `
  --name job-goaudits-enrich-uks `
  --resource-group rg-mcs-scheduled-workers-production

# 3) Check recent executions
az containerapp job execution list `
  --name job-goaudits-enrich-uks `
  --resource-group rg-mcs-scheduled-workers-production `
  -o table
```

SQL proof (run against the target DB with your preferred SQL client):
```sql
SELECT TOP (20) JobName, Status, RunStartedUtc, RunCompletedUtc, Message
FROM dbo.JobRunHistory
ORDER BY RunStartedUtc DESC;

SELECT COUNT(*) AS Reports FROM dbo.GoAuditsReports;
SELECT COUNT(*) AS Answers FROM dbo.GoAuditsReportAnswers;
```

Log Analytics proof (Kusto):
```kusto
ContainerAppConsoleLogs_CL
| where ContainerJobName_s in ("job-goaudits-ingest-uks","job-goaudits-enrich-uks")
| top 20 by TimeGenerated desc
| project TimeGenerated, ContainerJobName_s, Log_s
```

## What is running in production (names)
- Resource group: `rg-mcs-scheduled-workers-production`
- Container Apps environment (UK South): `cae-mcs-scheduled-workers-prod-uks`
- Log Analytics workspace: `law-mcs-scheduled-workers-prod-uks`
- ACR: `acrmcsschedwrkprod.azurecr.io`
- Key Vault: `kv-mcsschedwrkprod-uks`
- User-assigned managed identity (UAMI): `id-mcs-scheduled-workers-aca-prod-uks` (clientId only; no secrets stored here)
- ACA jobs:
  - `job-mcs-aca-heartbeat-prod-uks` (heartbeat)
  - `job-goaudits-ingest-uks` (GoAudits ingestion, cron: `0 * * * *`)
  - `job-goaudits-enrich-uks` (GoAudits enrichment, cron: `15 * * * *`)
- Legacy Azure Functions (stopped/retired):
  - `func-mcs-scheduled-workers-prod-linux`
  - `func-mcs-scheduled-workers-prod-uks`

## Non-goals / out of scope
- No scoring or downstream analytics in these milestones.
- No emails or notifications.
- No SharePoint integration.
- Only ingestion/enrichment of GoAudits data into SQL; downstream use is external to this repo.
