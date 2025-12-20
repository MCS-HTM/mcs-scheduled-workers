const { randomUUID } = require('crypto');
const fs = require('fs');
const path = require('path');
const { sql, getSqlPool } = require('../shared/sql');

const JOB_NAME = 'GoAuditsScoring';
const DEFAULT_RULESET_VERSION = 'v1';
const DEFAULT_BATCH_SIZE = 100;

function requireEnv(name) {
  const value = process.env[name];
  if (!value || !value.trim()) {
    throw new Error(`${name} environment variable is not set.`);
  }
  return value.trim();
}

function parseIntEnv(name, defaultValue) {
  const value = parseInt(process.env[name], 10);
  return Number.isFinite(value) && value > 0 ? value : defaultValue;
}

function loadRules(ruleset, version) {
  const fileName = `${ruleset.toLowerCase()}.${version}.json`;
  const filePath = path.join(__dirname, '..', 'rules', fileName);
  const raw = fs.readFileSync(filePath, 'utf8');
  const json = JSON.parse(raw);

  if (!json.ruleSetName || !json.ruleSetVersion || !Array.isArray(json.rules) || !json.scoring) {
    throw new Error('Rules file missing required fields (ruleSetName, ruleSetVersion, rules, scoring).');
  }

  if (String(json.ruleSetVersion) !== version) {
    throw new Error(`Rules version mismatch. Expected ${version}, found ${json.ruleSetVersion}.`);
  }

  if (String(json.ruleSetName).toLowerCase() !== ruleset.toLowerCase()) {
    throw new Error(`Rules name mismatch. Expected ${ruleset}, found ${json.ruleSetName}.`);
  }

  if (!Array.isArray(json.scoring.outcomeRules) || json.scoring.outcomeRules.length === 0 || !json.scoring.scoreValue) {
    throw new Error('Scoring configuration is incomplete (outcomeRules/scoreValue).');
  }

  return json;
}

function extractRulesetQuestionKeys(rulesDoc) {
  const keys = new Set();
  for (const rule of rulesDoc.rules ?? []) {
    for (const k of rule.questionKeysAny ?? []) keys.add(Number(k));
  }
  for (const k of rulesDoc.ignoreQuestionKeys ?? []) keys.add(Number(k));
  return keys;
}

function normalizeAnswer(value, options = {}) {
  const { trim = false, caseInsensitive = false, emptyIsNull = false } = options;

  if (value === null || value === undefined) {
    return null;
  }

  let result = String(value);
  if (trim) {
    result = result.trim();
  }
  if (emptyIsNull && result === '') {
    return null;
  }
  if (caseInsensitive) {
    result = result.toLowerCase();
  }
  return result;
}

function evaluateRule(rule, answerMap, defaultNorm) {
  if (rule.enabled === false) {
    return null;
  }

  const nc = rule.nonCompliantWhen || {};
  const normOpts = {
    trim: defaultNorm.trim || false,
    caseInsensitive: defaultNorm.caseInsensitive || false,
    emptyIsNull: defaultNorm.emptyIsNull || false,
  };

  if (typeof nc.trim === 'boolean') {
    normOpts.trim = nc.trim;
  }
  if (typeof nc.caseInsensitive === 'boolean') {
    normOpts.caseInsensitive = nc.caseInsensitive;
  }

  const answerRaw = answerMap.get(rule.questionKey) ?? null;
  const answerNorm = normalizeAnswer(answerRaw, normOpts);

  const op = nc.op;
  let isNonCompliant = false;

  switch (op) {
    case 'missing':
      isNonCompliant = answerNorm === null || answerNorm === '';
      break;
    case 'equals': {
      const expected = normalizeAnswer(nc.value, normOpts);
      isNonCompliant = expected !== null && answerNorm === expected;
      break;
    }
    case 'in': {
      const values = Array.isArray(nc.values) ? nc.values.map((v) => normalizeAnswer(v, normOpts)) : [];
      isNonCompliant = answerNorm !== null && values.includes(answerNorm);
      break;
    }
    default:
      throw new Error(`Unsupported op: ${op}`);
  }

  if (!isNonCompliant) {
    return null;
  }

  return {
    questionKey: rule.questionKey,
    answerValue: answerRaw === undefined ? null : answerRaw,
    severity: rule.finding.severity,
    code: rule.finding.code || null,
    message: rule.finding.message,
  };
}

function determineOutcome(scoring, majorCount, minorCount) {
  for (const rule of scoring.outcomeRules) {
    const when = rule.when || {};
    if (when.always === true) {
      return rule.outcome;
    }
    if (typeof when.majorCountGte === 'number' && majorCount >= when.majorCountGte) {
      return rule.outcome;
    }
    if (typeof when.minorCountGte === 'number' && minorCount >= when.minorCountGte) {
      return rule.outcome;
    }
  }
  return null;
}

function computeScoreValue(scoreValueConfig, outcome) {
  if (!scoreValueConfig) return null;
  const { type, from, fixedValue } = scoreValueConfig;

  if (from === 'fixed') {
    if (fixedValue === null || fixedValue === undefined) return null;
    return String(fixedValue);
  }

  if (from === 'outcome') {
    if (type === 'text') {
      return outcome;
    }
    if (type === 'numeric') {
      return outcome != null ? String(outcome) : null;
    }
  }

  return null;
}

async function insertJobRun(pool, runId, status, message) {
  const request = pool.request();
  request.input('runId', sql.UniqueIdentifier, runId);
  request.input('jobName', sql.NVarChar(100), JOB_NAME);
  request.input('status', sql.NVarChar(30), status);
  request.input('message', sql.NVarChar(4000), message || '');
  request.input('correlationId', sql.NVarChar(100), runId);

  await request.query(
    'INSERT INTO dbo.JobRunHistory (RunId, JobName, Status, Message, CorrelationId) VALUES (@runId, @jobName, @status, @message, @correlationId)'
  );
}

async function updateJobRun(pool, runId, status, message) {
  const request = pool.request();
  request.input('runId', sql.UniqueIdentifier, runId);
  request.input('status', sql.NVarChar(30), status);
  request.input('message', sql.NVarChar(4000), message || '');

  await request.query(
    'UPDATE dbo.JobRunHistory SET Status = @status, Message = @message, RunCompletedUtc = SYSUTCDATETIME() WHERE RunId = @runId'
  );
}

async function selectReports(pool, batchSize, rulesetName, rulesetVersion) {
  const request = pool.request();
  request.input('batchSize', sql.Int, batchSize);
  request.input('jobName', sql.NVarChar(100), JOB_NAME);
  request.input('rulesetKey', sql.NVarChar(200), `${rulesetName}|${rulesetVersion}`);

  const query = `
    SELECT TOP (@batchSize) r.GoAuditsReportId
    FROM dbo.GoAuditsReports r
    WHERE EXISTS (
        SELECT 1 FROM dbo.GoAuditsReportAnswers a WHERE a.GoAuditsReportId = r.GoAuditsReportId
    )
    AND NOT EXISTS (
        SELECT 1 FROM dbo.ProcessedItems p
        WHERE p.JobName = @jobName
          AND p.ItemKey = r.GoAuditsReportId + '|' + @rulesetKey
    )
    ORDER BY r.CompletedAtUtc DESC;
  `;

  const result = await request.query(query);
  return result.recordset.map((row) => row.GoAuditsReportId);
}

async function loadAnswers(pool, reportId) {
  const request = pool.request();
  request.input('reportId', sql.NVarChar(100), reportId);

  const result = await request.query(
    'SELECT QuestionKey, AnswerValue FROM dbo.GoAuditsReportAnswers WHERE GoAuditsReportId = @reportId'
  );

  const map = new Map();
  for (const row of result.recordset) {
    map.set(row.QuestionKey, row.AnswerValue);
  }
  return map;
}

async function processReport(pool, reportId, ruleset, version, rulesDoc, jobRunId, counts) {
  const transaction = new sql.Transaction(pool);
  await transaction.begin();

  const processedKey = `${reportId}|${ruleset}|${version}`;

  try {
    const procReq = new sql.Request(transaction);
    procReq.input('jobName', sql.NVarChar(100), JOB_NAME);
    procReq.input('itemKey', sql.NVarChar(200), processedKey);
    procReq.input('runId', sql.UniqueIdentifier, jobRunId);

    await procReq.query(
      'INSERT INTO dbo.ProcessedItems (JobName, ItemKey, RunId) VALUES (@jobName, @itemKey, @runId)'
    );
  } catch (error) {
    if (error && (error.number === 2627 || error.number === 2601)) {
      await transaction.rollback();
      counts.alreadyProcessed += 1;
      return;
    }
    await transaction.rollback();
    counts.failedCount += 1;
    return;
  }

  try {
    const answerMap = await loadAnswers(pool, reportId);
    const rulesetKeys = extractRulesetQuestionKeys(rulesDoc);
    let eligible = false;
    for (const key of answerMap.keys()) {
      const keyNumber = Number(key);
      if (!Number.isNaN(keyNumber) && rulesetKeys.has(keyNumber)) {
        eligible = true;
        break;
      }
    }
    if (!eligible) {
      await transaction.commit();
      counts.skippedNotEligible += 1;
      return;
    }
    const defaultNorm = rulesDoc.answerNormalization || {};
    const findings = [];
    let majorCount = 0;
    let minorCount = 0;

    for (const rule of rulesDoc.rules) {
      const finding = evaluateRule(rule, answerMap, defaultNorm);
      if (finding) {
        findings.push(finding);
        if (finding.severity === 'Major') {
          majorCount += 1;
        } else if (finding.severity === 'Minor') {
          minorCount += 1;
        }
      }
    }

    const outcome = determineOutcome(rulesDoc.scoring, majorCount, minorCount) || 'Unknown';
    const scoreValue = computeScoreValue(rulesDoc.scoring.scoreValue, outcome);

    // Insert findings (ignore PK duplicates)
    for (const f of findings) {
      const req = new sql.Request(transaction);
      req.input('reportId', sql.NVarChar(100), reportId);
      req.input('ruleSetName', sql.NVarChar(50), ruleset);
      req.input('ruleSetVersion', sql.NVarChar(20), version);
      req.input('questionKey', sql.NVarChar(256), f.questionKey);
      req.input('answerValue', sql.NVarChar(sql.MAX), f.answerValue);
      req.input('findingSeverity', sql.NVarChar(10), f.severity);
      req.input('findingCode', sql.NVarChar(50), f.code);
      req.input('jobRunId', sql.UniqueIdentifier, jobRunId);
      try {
        await req.query(
          'INSERT INTO dbo.GoAuditsFindings (GoAuditsReportId, RuleSetName, RuleSetVersion, QuestionKey, AnswerValue, FindingSeverity, FindingCode, JobRunId) VALUES (@reportId, @ruleSetName, @ruleSetVersion, @questionKey, @answerValue, @findingSeverity, @findingCode, @jobRunId)'
        );
        counts.findingsInsertedCount += 1;
      } catch (error) {
        if (!(error && (error.number === 2627 || error.number === 2601))) {
          throw error;
        }
      }
    }

    // Upsert score
    const scoreReq = new sql.Request(transaction);
    scoreReq.input('reportId', sql.NVarChar(100), reportId);
    scoreReq.input('ruleSetName', sql.NVarChar(50), ruleset);
    scoreReq.input('ruleSetVersion', sql.NVarChar(20), version);
    scoreReq.input('majorCount', sql.Int, majorCount);
    scoreReq.input('minorCount', sql.Int, minorCount);
    scoreReq.input('scoreValue', sql.NVarChar(50), scoreValue);
    scoreReq.input('outcome', sql.NVarChar(20), outcome);
    scoreReq.input('jobRunId', sql.UniqueIdentifier, jobRunId);

    try {
      await scoreReq.query(
        'INSERT INTO dbo.GoAuditsScores (GoAuditsReportId, RuleSetName, RuleSetVersion, MajorCount, MinorCount, ScoreValue, Outcome, JobRunId) VALUES (@reportId, @ruleSetName, @ruleSetVersion, @majorCount, @minorCount, @scoreValue, @outcome, @jobRunId)'
      );
    } catch (error) {
      if (error && (error.number === 2627 || error.number === 2601)) {
        await scoreReq.query(
          'UPDATE dbo.GoAuditsScores SET MajorCount=@majorCount, MinorCount=@minorCount, ScoreValue=@scoreValue, Outcome=@outcome, JobRunId=@jobRunId, ScoredAtUtc=SYSUTCDATETIME() WHERE GoAuditsReportId=@reportId AND RuleSetName=@ruleSetName AND RuleSetVersion=@ruleSetVersion'
        );
      } else {
        throw error;
      }
    }

    await transaction.commit();
    counts.processed += 1;
    counts.majorCountTotal += majorCount;
    counts.minorCountTotal += minorCount;
  } catch (error) {
    await transaction.rollback();
    counts.failedCount += 1;
  }
}

async function main() {
  const jobRunId = randomUUID();
  const startedAtUtc = new Date().toISOString();

  const counts = {
    selected: 0,
    processed: 0,
    skipped: 0,
    skippedNotEligible: 0,
    alreadyProcessed: 0,
    findingsInsertedCount: 0,
    majorCountTotal: 0,
    minorCountTotal: 0,
    failedCount: 0,
  };

  let pool;
  let status = 'Succeeded';
  let message = '';

  try {
    const rulesetName = requireEnv('GOAUDITS_RULESET');
    const rulesetVersion = process.env.GOAUDITS_RULESET_VERSION
      ? process.env.GOAUDITS_RULESET_VERSION.trim()
      : DEFAULT_RULESET_VERSION;
    const batchSize = parseIntEnv('GOAUDITS_SCORE_BATCH_SIZE', DEFAULT_BATCH_SIZE);

    const rulesDoc = loadRules(rulesetName, rulesetVersion);

    pool = await getSqlPool();
    await insertJobRun(pool, jobRunId, 'Running', `Scoring ${rulesetName} ${rulesetVersion}`);

    const reportIds = await selectReports(pool, batchSize, rulesetName, rulesetVersion);
    counts.selected = reportIds.length;

    for (const reportId of reportIds) {
      await processReport(pool, reportId, rulesetName, rulesetVersion, rulesDoc, jobRunId, counts);
    }

    const completedAtUtc = new Date().toISOString();
    message = `Selected=${counts.selected} Processed=${counts.processed} Skipped=${counts.skipped} SkippedNotEligible=${counts.skippedNotEligible} AlreadyProcessed=${counts.alreadyProcessed} FindingsInserted=${counts.findingsInsertedCount} Majors=${counts.majorCountTotal} Minors=${counts.minorCountTotal} Failed=${counts.failedCount}`;
    await updateJobRun(pool, jobRunId, status, message);

    console.log(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId,
        startedAtUtc,
        completedAtUtc,
        status,
        ruleset: { name: rulesetName, version: rulesetVersion },
        counts,
      })
    );
  } catch (error) {
    status = 'Failed';
    const completedAtUtc = new Date().toISOString();
    message = error && error.message ? error.message : 'Scoring job failed.';

    try {
      if (pool) {
        await updateJobRun(pool, jobRunId, status, message);
      }
    } catch {
      // best effort
    }

    console.error(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId,
        startedAtUtc,
        completedAtUtc,
        status,
        counts,
        error: message,
      })
    );
    process.exitCode = 1;
    return;
  }

  process.exitCode = 0;
}

main();
