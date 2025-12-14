# M2 Scoring Status (discovery only)

## Job code
- `functions/src/jobs/goaudits-scoring.js` exists; file is currently untracked (no git history).

## SQL migrations mentioning scoring tables
- `functions/infra/sql/004_m2_goaudits_scoring_outputs.sql` creates `GoAuditsFindings` and `GoAuditsScores` (with indexes and constraints); untracked.
- `functions/infra/sql/005_m2_grant_function_mi_scoring.sql` grants scoring-table permissions to the managed identity; untracked.
- `functions/infra/sql/006_m2_goaudits_reporting_views.sql` defines reporting views over `GoAuditsScores` and `GoAuditsFindings`; untracked.

## Rules files
- `functions/src/rules/heatpump.v1.json`, `functions/src/rules/pv.v1.json`, `functions/src/rules/ruleset.schema.v1.json`, and `functions/src/rules/README.md` all exist; the directory is untracked.

## Dockerfile for scoring
- `functions/Dockerfile.goaudits.scoring` exists; file is untracked (no git history).

## Git history snapshot (last commit touching each path)
| Path | Last commit |
| --- | --- |
| functions/src/jobs/goaudits-scoring.js | Untracked (no commits) |
| functions/infra/sql/004_m2_goaudits_scoring_outputs.sql | Untracked (no commits) |
| functions/infra/sql/005_m2_grant_function_mi_scoring.sql | Untracked (no commits) |
| functions/infra/sql/006_m2_goaudits_reporting_views.sql | Untracked (no commits) |
| functions/src/rules/* | Untracked (no commits) |
| functions/Dockerfile.goaudits.scoring | Untracked (no commits) |
