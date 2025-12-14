# Scoring Rules (M2 scaffold)

File naming: `<ruleset-lower>.<version>.json` (e.g., `pv.v1.json`, `heatpump.v1.json`). Each file declares `ruleSetName` and `ruleSetVersion` to match its name. Use a new file for new versions (e.g., `pv.v2.json`); do not mutate v1 when introducing new rules.

Question keys: `questionKey` must match `GoAuditsReportAnswers.QuestionKey`, which is the stable `QUESTION_ID` from GoAudits detail rows. Do not use transient UI labels or report-specific IDs.

Schema: see `ruleset.schema.v1.json` (Draft 2020-12). `additionalProperties` is false at top-level and for rule objects to keep configs strict.

Adding a new version:
1. Copy the previous version file to `<ruleset>.<newversion>.json`.
2. Update `ruleSetVersion` inside the new file.
3. Add/change rules; leave older versions untouched to keep backward compatibility.

Example rule snippet (illustrative; not present in the shipped files):
```json
{
  "ruleId": "pv-cert-present",
  "questionKey": "1",
  "enabled": true,
  "nonCompliantWhen": { "op": "missing" },
  "finding": { "severity": "Major", "message": "Certificate number is missing." }
}
```

Current rule files are placeholders with empty `rules` arrays; populate from scoring documents when available. Normalization defaults are set in each file to trim, case-fold, and treat empty as null.
