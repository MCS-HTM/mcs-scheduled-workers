# GoAudits Pipeline (M1-M2)

Job name: `GoAuditsPipeline`  
Script: `functions/src/jobs/goaudits-pipeline.js`  
Image entrypoint: `functions/Dockerfile.goaudits` (runs the pipeline script)

## What it does
- Pulls completed audits from GoAudits summary API since the `GoAuditsIngestion` watermark (or `GOAUDITS_START_DATE`/`GOAUDITS_END_DATE`).
- Inserts new reports into `dbo.GoAuditsReports` and updates `dbo.JobWatermark` after ingestion.
- Fetches audit details, upserts answers into `dbo.GoAuditsReportAnswers`, updates missing certificates, and marks `ProcessedItems` for `GoAuditsEnrichment`.
- Resolves the report ruleset (PV vs HeatPump) and scores into `dbo.GoAuditsScores` + `dbo.GoAuditsFindings`, marking `ProcessedItems` for `GoAuditsScoring`.
- Continues past per-report failures, logging stage failure counts.
- Optionally materialises `dbo.GoAuditsEmailOutbox` (no sending) when enabled.

## Ruleset detection
Order of preference:
1) `dbo.GoAuditsReports.RuleSetName` / `TechnologyType` / `AssessmentType` (if columns exist).
2) Details payload metadata.
3) Question-key overlap against PV/HeatPump rulesets.

## Pipeline env vars
- `GOAUDITS_PIPELINE_BATCH_SIZE` (default `50`)
- `GOAUDITS_START_DATE` / `GOAUDITS_END_DATE` (optional ISO UTC window overrides)
- `GOAUDITS_RULESET_MAP_JSON` (optional JSON mapping, e.g. `{"PV":"v2","HeatPump":"v3"}`)
- `GOAUDITS_PIPELINE_DRYRUN=true` (no SQL writes; logs actions)
- `GOAUDITS_PIPELINE_MATERIALISE_EMAIL=true` (run email outbox materialiser after scoring)
- `GOAUDITS_PIPELINE_MATERIALISE_EMAIL_SCOPE=batch|all` (default `all`; `batch` limits to report IDs processed in the pipeline run)
- Existing GoAudits auth/env vars remain required (`KEYVAULT_URI`, `GOAUDITS_BEARER_SECRET_NAME`, `GOAUDITS_AUDITSUMMARY_URL`, `GOAUDITS_AUDITDETAILS_URL`, etc.)

## Idempotency
- Watermark row: `JobName='GoAuditsIngestion'`
- Details guard: `ProcessedItems` with `JobName='GoAuditsEnrichment'`
- Scoring guard: `ProcessedItems` with key `GoAuditsReportId|RuleSetName|RuleSetVersion` and `JobName='GoAuditsScoring'`

## Deployment note
- Create ACA Job `job-goaudits-pipeline-uks` (or repoint `job-goaudits-ingest-uks`) to run the pipeline image.
- Keep the legacy ingestion/enrichment/scoring ACA jobs disabled for rollback.
