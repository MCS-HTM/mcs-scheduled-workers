# Documentation Index (reading order)

1) **Platform overview**
   - `00-architecture-overview.md` – what the platform does, why ACA Jobs, security model.
   - `01-repo-structure.md` – where the code and SQL live.
2) **Access and identity**
   - `02-azure-resources-and-rbac.md` – Azure resources, Managed Identity model, RBAC/grants.
3) **How to deploy/run**
   - `03-deployment-runbook.md` – build/push images, create/update jobs, schedules, env vars.
   - `08-backfill-runbook.md` – safe backfill steps (rewind watermark, rerun, re-enrich).
4) **Behaviour (what the jobs do)**
   - `10-goaudits-pipeline.md` – unified ingestion + enrichment + scoring pipeline (GoAuditsPipeline).
   - `05-goaudits-ingestion-behaviour.md` – M1 ingestion (getauditsummary).
   - `07-goaudits-enrichment-behaviour.md` – M1.5 enrichment (getauditdetailsbyid).
   - `09-goaudits-scoring-behaviour.md` – M2 scoring outputs, non-compliance text, reporting view, backfill.
5) **Operations**
   - `04-observability-and-proof.md` – SQL and Log Analytics proof queries.
   - `06-troubleshooting.md` – common failure modes and fixes.

Steady-state schedules:
- Pipeline: cron `0 * * * *` (replaces ingestion/enrichment in steady state).
- Ingestion/Enrichment: keep disabled for rollback.
