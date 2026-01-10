/*
Runbook: Backfill non-compliance text for PV v2 and HeatPump v3
Manual steps only (not a migration).
*/

-- 1) Delete scores for the targeted rulesets
DELETE FROM dbo.GoAuditsScores
WHERE (RuleSetName = 'PV' AND RuleSetVersion = 'v2')
   OR (RuleSetName = 'HeatPump' AND RuleSetVersion = 'v3');

-- 2) Delete findings for the targeted rulesets
DELETE FROM dbo.GoAuditsFindings
WHERE (RuleSetName = 'PV' AND RuleSetVersion = 'v2')
   OR (RuleSetName = 'HeatPump' AND RuleSetVersion = 'v3');

-- 3) Delete processed-item markers so scoring can re-run
DELETE FROM dbo.ProcessedItems
WHERE JobName = 'GoAuditsScoring'
  AND (
    ItemKey LIKE '%|PV|v2'
    OR ItemKey LIKE '%|HeatPump|v3'
  );

-- 4) Re-run ACA scoring jobs until findings show non-compliance text populated.
