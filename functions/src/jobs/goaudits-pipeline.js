const { randomUUID, createHash } = require('crypto');
const fs = require('fs');
const path = require('path');
const { DefaultAzureCredential } = require('@azure/identity');
const { SecretClient } = require('@azure/keyvault-secrets');
const { sql, getSqlPool } = require('../shared/sql');
const {
  materialiseEmailOutbox,
} = require('./goaudits-email-outbox-materialise');

const JOB_NAME = 'GoAuditsPipeline';
const INGESTION_JOB_NAME = 'GoAuditsIngestion';
const ENRICHMENT_JOB_NAME = 'GoAuditsEnrichment';
const SCORING_JOB_NAME = 'GoAuditsScoring';

const DEFAULT_AUDIT_URL =
  'https://api.goaudits.com/v1/audits/getauditsummary';
const DEFAULT_DETAILS_URL =
  'https://api.goaudits.com/v1/audits/getauditdetailsbyid';
const DEFAULT_SECRET_NAME = 'goaudits-bearer-token';
const DEFAULT_BATCH_SIZE = 50;
const MAX_MESSAGE_LENGTH = 4000;
const REQUEST_TIMEOUT_MS = 30000;
const MAX_RETRIES = 5;
const DETAILS_CONCURRENCY = 3;
const DEFAULT_RULESET_MAP = { PV: 'v2', HeatPump: 'v3' };
const DETAILS_REQUEST_BASE = {
  archived: '',
  audit_type_id: '',
  auto_fail: '',
  client_id: '',
  custom_fields: '',
  description: '',
  start_date: '2024-01-01',
  end_date: '2050-12-12',
  file_type: '',
  filetype: '',
  filterId: '',
  generated_on: '',
  guid: '',
  json: 0,
  name: '',
  parameters: '',
  report_name: '',
  role_code: '',
  status: '',
  store_id: '',
  tags_ids: '',
  template_name: '',
  templateactive: true,
  templateid: 0,
  uid: '',
  csv: 0,
  csvflag: false,
  jsonflag: true,
  xlsx: 0,
  xlsxflag: false,
};

const credential = new DefaultAzureCredential({
  managedIdentityClientId: process.env.AZURE_CLIENT_ID,
});

const rulesDocCache = new Map();
const rulesetQuestionKeyCache = new Map();
let rulesetMapCache = null;
let keyValidationLogCount = 0;

const RULESET_HINT_KEYS = [
  'rulesetname',
  'ruleset',
  'rule_set_name',
  'rule_set',
  'technologytype',
  'techtype',
  'assessmenttype',
  'assessment_type',
  'templatename',
  'template_name',
  'template',
  'audit_type',
  'audittype',
  'auditname',
  'checklist',
  'checklistname',
];

function truncateMessage(message) {
  if (!message) {
    return '';
  }

  return message.length > MAX_MESSAGE_LENGTH
    ? message.slice(0, MAX_MESSAGE_LENGTH)
    : message;
}

function truncate(str, max) {
  if (str == null) return null;
  const s = String(str);
  return s.length > max ? s.slice(0, max) : s;
}

function formatDateOnly(date) {
  return date.toISOString().slice(0, 10);
}

function getEnv(name, defaultValue) {
  const value = process.env[name];
  return value && value.trim().length > 0 ? value : defaultValue;
}

function parsePositiveInt(value, defaultValue) {
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : defaultValue;
}

function parseBoolEnv(name, defaultValue = false) {
  const value = process.env[name];
  if (!value) return defaultValue;
  const normalized = value.trim().toLowerCase();
  return normalized === 'true' || normalized === '1' || normalized === 'yes';
}

function parseOptionalDateEnv(name, options = {}) {
  const raw = getEnv(name, '');
  if (!raw) return null;
  const trimmed = raw.trim();
  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`${name} environment variable was not a valid ISO date.`);
  }
  if (options.endOfDay && /^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return new Date(
      Date.UTC(
        parsed.getUTCFullYear(),
        parsed.getUTCMonth(),
        parsed.getUTCDate(),
        23,
        59,
        59,
        999
      )
    );
  }
  return parsed;
}

function getFirstDefined(item, keys) {
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(item, key) && item[key] != null) {
      return item[key];
    }
  }
  return undefined;
}

function parseCompletedAt(value) {
  if (!value) {
    return null;
  }

  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function parseCompletedAtValue(item) {
  const updatedOn = getFirstDefined(item, ['Updated_On', 'updated_on']);
  if (updatedOn) {
    const formatted =
      typeof updatedOn === 'string'
        ? `${updatedOn.replace(' ', 'T')}Z`
        : updatedOn;
    const parsed = parseCompletedAt(formatted);
    if (parsed) {
      return parsed;
    }
  }

  const fallback = getFirstDefined(item, ['EndTime', 'endTime', 'Date', 'date']);
  return parseCompletedAt(fallback);
}

function normalizeQuestionKey(questionId, questionText) {
  if (questionId && String(questionId).trim()) {
    return String(questionId).trim();
  }

  if (!questionText) return null;
  const normalized =
    String(questionText)
      .toLowerCase()
      .replace(/\s+/g, ' ')
      .trim()
      .replace(/[^a-z0-9]+/g, '_')
      .replace(/^_+|_+$/g, '') || null;

  if (!normalized) {
    return null;
  }

  if (normalized.length <= 256) {
    return normalized;
  }

  const hash = createHash('sha1').update(normalized).digest('hex');
  const prefix = normalized.slice(0, 256 - hash.length - 1);
  return `${prefix}_${hash}`;
}

function normalizeQuestionKeyValue(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const normalized = String(value).trim();
  return normalized ? normalized : null;
}

function normalizeQuestionKeySet(values) {
  const set = new Set();
  if (!values) {
    return set;
  }
  for (const value of values) {
    const normalized = normalizeQuestionKeyValue(value);
    if (normalized) {
      set.add(normalized);
    }
  }
  return set;
}

async function getBearerToken() {
  const keyVaultUri = process.env.KEYVAULT_URI;
  const secretName = getEnv(
    'GOAUDITS_BEARER_SECRET_NAME',
    DEFAULT_SECRET_NAME
  );

  if (!keyVaultUri) {
    throw new Error('KEYVAULT_URI environment variable is not set.');
  }

  const client = new SecretClient(keyVaultUri, credential);
  const secret = await client.getSecret(secretName);

  if (!secret || !secret.value) {
    throw new Error('GoAudits bearer token was empty.');
  }

  return secret.value;
}

async function fetchPage(url, token, payload) {
  const headers = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${token}`,
  };

  const body = JSON.stringify(payload);
  let attempt = 0;

  while (attempt < MAX_RETRIES) {
    attempt += 1;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers,
        body,
        signal: controller.signal,
      });

      if (response.status === 401 || response.status === 403) {
        const error = new Error(
          `Authorization failed with status ${response.status}.`
        );
        error.fatal = true;
        throw error;
      }

      if (
        response.status === 429 ||
        (response.status >= 500 && response.status < 600)
      ) {
        if (attempt >= MAX_RETRIES) {
          throw new Error(
            `GoAudits API returned ${response.status} after ${attempt} attempts.`
          );
        }

        const delay =
          Math.min(1000 * 2 ** (attempt - 1), 8000) +
          Math.floor(Math.random() * 300);

        await new Promise((resolve) => setTimeout(resolve, delay));
        continue;
      }

      if (!response.ok) {
        const error = new Error(
          `GoAudits API returned ${response.status} ${response.statusText} for ${url}.`
        );
        error.fatal = true;
        throw error;
      }

      const parsed = await response.json().catch(() => {
        const error = new Error('GoAudits API response was not valid JSON.');
        error.fatal = true;
        throw error;
      });

      if (Array.isArray(parsed)) {
        return { items: parsed, keys: parsed[0] ? Object.keys(parsed[0]) : [] };
      }

      return { items: [], keys: Object.keys(parsed || {}) };
    } catch (error) {
      if ((error && error.fatal) || attempt >= MAX_RETRIES) {
        throw error;
      }

      const delay =
        Math.min(1000 * 2 ** (attempt - 1), 8000) +
        Math.floor(Math.random() * 300);
      await new Promise((resolve) => setTimeout(resolve, delay));
    } finally {
      clearTimeout(timeout);
    }
  }

  return { items: [], keys: [] };
}

async function collectAudits(token, payload) {
  const url = getEnv('GOAUDITS_AUDITSUMMARY_URL', DEFAULT_AUDIT_URL);
  const { items, keys } = await fetchPage(url, token, payload);
  return { items, keys, pages: 1 };
}

async function fetchWithRetry(url, token, bodyObj) {
  const headers = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${token}`,
  };

  const body = JSON.stringify(bodyObj);
  let attempt = 0;

  while (attempt < MAX_RETRIES) {
    attempt += 1;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers,
        body,
        signal: controller.signal,
      });

      if (response.status === 401 || response.status === 403) {
        const error = new Error(
          `Authorization failed with status ${response.status}.`
        );
        error.fatal = true;
        throw error;
      }

      if (
        response.status === 429 ||
        (response.status >= 500 && response.status < 600)
      ) {
        if (attempt >= MAX_RETRIES) {
          throw new Error(
            `GoAudits details API returned ${response.status} after ${attempt} attempts.`
          );
        }

        const delay =
          Math.min(1000 * 2 ** (attempt - 1), 8000) +
          Math.floor(Math.random() * 300);
        await new Promise((resolve) => setTimeout(resolve, delay));
        continue;
      }

      if (!response.ok) {
        const error = new Error(
          `GoAudits details API returned ${response.status} ${response.statusText} for ${url}.`
        );
        error.fatal = response.status === 401 || response.status === 403;
        error.retryable = false;
        throw error;
      }

      const parsed = await response.json().catch(() => {
        const error = new Error(
          'GoAudits details API response was not valid JSON.'
        );
        error.retryable = false;
        throw error;
      });

      if (!Array.isArray(parsed)) {
        const error = new Error(
          'GoAudits details API response was not an array.'
        );
        error.retryable = false;
        throw error;
      }

      return parsed;
    } catch (error) {
      if (error && error.fatal) {
        throw error;
      }
      if (error && error.retryable === false) {
        throw error;
      }
      if (attempt >= MAX_RETRIES) {
        throw error;
      }

      const delay =
        Math.min(1000 * 2 ** (attempt - 1), 8000) +
        Math.floor(Math.random() * 300);
      await new Promise((resolve) => setTimeout(resolve, delay));
    } finally {
      clearTimeout(timeout);
    }
  }

  throw new Error('Unexpected retry loop exit');
}

function buildPayload(startDate, endDate) {
  return {
    start_date: formatDateOnly(startDate),
    end_date: formatDateOnly(endDate),
    status: getEnv('GOAUDITS_STATUS', 'Completed'),
    jsonflag: true,
    filterId: getEnv('GOAUDITS_FILTER_ID', ''),
    locationId: '',
    userId: '',
    checklistId: '',
    timezoneOffset: 0,
    includePdf: false,
  };
}

function summarizeCounts(counts) {
  return [
    `Fetched=${counts.fetched}`,
    `Eligible=${counts.eligible}`,
    `Skipped=${counts.skipped}`,
    `Selected=${counts.selected}`,
    `Ingested=${counts.ingested}`,
    `IngestAlreadyProcessed=${counts.ingestAlreadyProcessed}`,
    `IngestFailed=${counts.ingestFailedCount}`,
    `DetailsProcessed=${counts.detailsProcessed}`,
    `DetailsAlreadyProcessed=${counts.detailsAlreadyProcessed}`,
    `AnswersInserted=${counts.answersInsertedCount}`,
    `CertUpdated=${counts.certUpdatedCount}`,
    `MarkedProcessed=${counts.markedProcessedCount}`,
    `DetailsFailed=${counts.detailsFailedCount}`,
    `ScoreProcessed=${counts.scoreProcessed}`,
    `ScoreAlreadyProcessed=${counts.scoreAlreadyProcessed}`,
    `SkippedNotEligible=${counts.skippedNotEligible}`,
    `FindingsInserted=${counts.findingsInsertedCount}`,
    `Majors=${counts.majorCountTotal}`,
    `Minors=${counts.minorCountTotal}`,
    `ScoreFailed=${counts.scoreFailedCount}`,
    `EmailOutboxInserted=${counts.emailOutboxInserted}`,
    `EmailOutboxSkipped=${counts.emailOutboxSkippedAlreadyExists}`,
    `EmailOutboxMissingRecipient=${counts.emailOutboxMissingRecipient}`,
  ].join(' ');
}

async function insertJobRun(pool, runId, status, message, correlationId) {
  const request = pool.request();
  request.input('runId', sql.UniqueIdentifier, runId);
  request.input('jobName', sql.NVarChar(100), JOB_NAME);
  request.input('status', sql.NVarChar(30), status);
  request.input('message', sql.NVarChar(4000), truncateMessage(message));
  request.input('correlationId', sql.NVarChar(100), correlationId);

  await request.query(
    'INSERT INTO dbo.JobRunHistory (RunId, JobName, Status, Message, CorrelationId) VALUES (@runId, @jobName, @status, @message, @correlationId)'
  );
}

async function updateJobRun(pool, runId, status, message) {
  const request = pool.request();
  request.input('runId', sql.UniqueIdentifier, runId);
  request.input('status', sql.NVarChar(30), status);
  request.input('message', sql.NVarChar(4000), truncateMessage(message));

  await request.query(
    'UPDATE dbo.JobRunHistory SET Status = @status, Message = @message, RunCompletedUtc = SYSUTCDATETIME() WHERE RunId = @runId'
  );
}

async function getWatermark(pool) {
  const request = pool.request();
  request.input('jobName', sql.NVarChar(100), INGESTION_JOB_NAME);
  const result = await request.query(
    'SELECT WatermarkUtc FROM dbo.JobWatermark WHERE JobName = @jobName'
  );

  if (!result.recordset || result.recordset.length === 0) {
    return { watermark: new Date(0), exists: false };
  }

  const watermark = result.recordset[0].WatermarkUtc;
  return {
    watermark: watermark ? new Date(watermark) : new Date(0),
    exists: true,
  };
}

async function updateWatermark(pool, watermarkUtc) {
  const request = pool.request();
  request.input('jobName', sql.NVarChar(100), INGESTION_JOB_NAME);
  request.input('watermarkUtc', sql.DateTime2, watermarkUtc);

  await request.query(
    `
IF EXISTS (SELECT 1 FROM dbo.JobWatermark WHERE JobName = @jobName)
  UPDATE dbo.JobWatermark SET WatermarkUtc = @watermarkUtc, UpdatedUtc = SYSUTCDATETIME() WHERE JobName = @jobName;
ELSE
  INSERT INTO dbo.JobWatermark (JobName, WatermarkUtc) VALUES (@jobName, @watermarkUtc);
    `.trim()
  );
}

async function isProcessed(pool, jobName, itemKey) {
  const request = pool.request();
  request.input('jobName', sql.NVarChar(100), jobName);
  request.input('itemKey', sql.NVarChar(200), itemKey);
  const result = await request.query(
    'SELECT TOP (1) 1 AS Found FROM dbo.ProcessedItems WHERE JobName = @jobName AND ItemKey = @itemKey'
  );
  return !!(result.recordset && result.recordset.length > 0);
}

async function getCurrentCertificationNumber(pool, reportId) {
  const request = pool.request();
  request.input('reportId', sql.NVarChar(100), reportId);
  const result = await request.query(
    'SELECT CertificationNumber FROM dbo.GoAuditsReports WHERE GoAuditsReportId = @reportId'
  );
  return result.recordset && result.recordset[0]
    ? result.recordset[0].CertificationNumber
    : null;
}

async function loadReportMetadataColumns(pool) {
  const wanted = [
    'RuleSetName',
    'TechnologyType',
    'AssessmentType',
    'TemplateName',
    'Template',
  ];
  const query = `
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dbo'
      AND TABLE_NAME = 'GoAuditsReports'
      AND COLUMN_NAME IN (${wanted.map((c) => `'${c}'`).join(', ')});
  `;
  const result = await pool.request().query(query);
  const set = new Set();
  for (const row of result.recordset || []) {
    set.add(row.COLUMN_NAME);
  }
  return set;
}

async function loadReportRow(pool, reportId, columnSet) {
  if (!columnSet || columnSet.size === 0) {
    return { GoAuditsReportId: reportId };
  }

  const columns = ['GoAuditsReportId'];
  for (const column of columnSet) {
    columns.push(`[${column}]`);
  }

  const request = pool.request();
  request.input('reportId', sql.NVarChar(100), reportId);
  const result = await request.query(
    `SELECT ${columns.join(', ')} FROM dbo.GoAuditsReports WHERE GoAuditsReportId = @reportId`
  );
  return result.recordset && result.recordset[0]
    ? result.recordset[0]
    : { GoAuditsReportId: reportId };
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

function loadRules(ruleset, version) {
  const fileName = `${ruleset.toLowerCase()}.${version}.json`;
  const filePath = path.join(__dirname, '..', 'rules', fileName);
  const raw = fs.readFileSync(filePath, 'utf8');
  const json = JSON.parse(raw);

  if (
    !json.ruleSetName ||
    !json.ruleSetVersion ||
    !Array.isArray(json.rules) ||
    !json.scoring
  ) {
    throw new Error(
      'Rules file missing required fields (ruleSetName, ruleSetVersion, rules, scoring).'
    );
  }

  if (String(json.ruleSetVersion) !== version) {
    throw new Error(
      `Rules version mismatch. Expected ${version}, found ${json.ruleSetVersion}.`
    );
  }

  if (String(json.ruleSetName).toLowerCase() !== ruleset.toLowerCase()) {
    throw new Error(
      `Rules name mismatch. Expected ${ruleset}, found ${json.ruleSetName}.`
    );
  }

  if (
    !Array.isArray(json.scoring.outcomeRules) ||
    json.scoring.outcomeRules.length === 0 ||
    !json.scoring.scoreValue
  ) {
    throw new Error(
      'Scoring configuration is incomplete (outcomeRules/scoreValue).'
    );
  }

  return json;
}

function getRulesDoc(ruleset, version) {
  const key = `${ruleset}|${version}`;
  if (rulesDocCache.has(key)) {
    return rulesDocCache.get(key);
  }
  const doc = loadRules(ruleset, version);
  rulesDocCache.set(key, doc);
  return doc;
}

function extractRulesetQuestionKeys(rulesDoc) {
  const keys = new Set();
  for (const rule of rulesDoc.rules ?? []) {
    for (const k of rule.questionKeysAny ?? []) {
      const normalized = normalizeQuestionKeyValue(k);
      if (normalized) {
        keys.add(normalized);
      }
    }
  }
  for (const k of rulesDoc.ignoreQuestionKeys ?? []) {
    const normalized = normalizeQuestionKeyValue(k);
    if (normalized) {
      keys.add(normalized);
    }
  }
  return keys;
}

function getRulesetQuestionKeys(ruleset, version) {
  const key = `${ruleset}|${version}`;
  if (rulesetQuestionKeyCache.has(key)) {
    return rulesetQuestionKeyCache.get(key);
  }
  const rulesDoc = getRulesDoc(ruleset, version);
  const keys = extractRulesetQuestionKeys(rulesDoc);
  rulesetQuestionKeyCache.set(key, keys);
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
      const values = Array.isArray(nc.values)
        ? nc.values.map((v) => normalizeAnswer(v, normOpts))
        : [];
      isNonCompliant = answerNorm !== null && values.includes(answerNorm);
      break;
    }
    default:
      throw new Error(`Unsupported op: ${op}`);
  }

  if (!isNonCompliant) {
    return null;
  }

  const severity = rule.finding.severity;
  const majorNonCompliantText =
    severity === 'Major' ? rule.finding?.majorNonCompliantText ?? null : null;
  const minorNonCompliantText =
    severity === 'Minor' ? rule.finding?.minorNonCompliantText ?? null : null;

  return {
    questionKey: rule.questionKey,
    answerValue: answerRaw === undefined ? null : answerRaw,
    severity,
    code: rule.finding.code || null,
    message: rule.finding.message,
    majorNonCompliantText,
    minorNonCompliantText,
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

function loadRulesetMap() {
  const raw = getEnv('GOAUDITS_RULESET_MAP_JSON', '');
  if (!raw) {
    return { ...DEFAULT_RULESET_MAP };
  }

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw new Error('GOAUDITS_RULESET_MAP_JSON was not valid JSON.');
  }

  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error('GOAUDITS_RULESET_MAP_JSON must be a JSON object.');
  }

  const map = { ...DEFAULT_RULESET_MAP };
  for (const [key, value] of Object.entries(parsed)) {
    const normalizedKey = String(key || '').trim();
    const normalizedValue = String(value || '').trim();
    if (!normalizedKey || !normalizedValue) {
      continue;
    }
    map[normalizedKey] = normalizedValue;
  }

  return map;
}

function getRulesetMap() {
  if (!rulesetMapCache) {
    rulesetMapCache = loadRulesetMap();
  }
  return rulesetMapCache;
}

function getRulesetVersion(map, rulesetName) {
  if (!map || !rulesetName) return null;
  if (map[rulesetName]) return map[rulesetName];

  const lower = rulesetName.toLowerCase();
  for (const [key, value] of Object.entries(map)) {
    if (key.toLowerCase() === lower) {
      return value;
    }
  }
  return null;
}

function detectRulesetNameFromText(value) {
  if (!value) return null;
  const text = String(value).trim().toLowerCase();
  if (!text) return null;

  if (text === 'pv' || /\bpv\b/.test(text) || text.includes('photovoltaic') || text.includes('solar')) {
    return 'PV';
  }
  if (
    text === 'hp' ||
    text.includes('heat pump') ||
    text.includes('heatpump') ||
    text.includes('heat_pump')
  ) {
    return 'HeatPump';
  }

  return null;
}

function collectCandidateStrings(obj) {
  if (!obj || typeof obj !== 'object') return [];
  const candidates = [];
  const keyMap = {};
  for (const [key, value] of Object.entries(obj)) {
    if (value == null) continue;
    keyMap[key.toLowerCase()] = value;
  }

  for (const key of RULESET_HINT_KEYS) {
    if (Object.prototype.hasOwnProperty.call(keyMap, key)) {
      const str = String(keyMap[key]).trim();
      if (str) {
        candidates.push(str);
      }
    }
  }
  return candidates;
}

function extractTemplateName(obj) {
  if (!obj || typeof obj !== 'object') return null;
  const keyMap = {};
  for (const [key, value] of Object.entries(obj)) {
    if (value == null) continue;
    keyMap[key.toLowerCase()] = value;
  }
  for (const key of ['templatename', 'template_name', 'template']) {
    if (Object.prototype.hasOwnProperty.call(keyMap, key)) {
      const str = String(keyMap[key]).trim();
      if (str) {
        return str;
      }
    }
  }
  return null;
}

function extractQuestionKeys(detailsPayload) {
  const keys = new Set();
  if (Array.isArray(detailsPayload)) {
    for (const row of detailsPayload) {
      if (!row || typeof row !== 'object') continue;
      const raw = getFirstDefined(row, [
        'QUESTION_ID',
        'QuestionId',
        'question_id',
        'QuestionID',
        'questionId',
      ]);
      const normalized = normalizeQuestionKeyValue(raw);
      if (normalized) {
        keys.add(normalized);
      }
    }
  }

  if (detailsPayload && Array.isArray(detailsPayload.answerKeys)) {
    for (const raw of detailsPayload.answerKeys) {
      const normalized = normalizeQuestionKeyValue(raw);
      if (normalized) {
        keys.add(normalized);
      }
    }
  }

  return keys;
}

function countRulesetHits(questionKeys, rulesetName, rulesetVersion) {
  if (!questionKeys || questionKeys.size === 0) {
    return 0;
  }
  const rulesetKeys = getRulesetQuestionKeys(rulesetName, rulesetVersion);
  let hits = 0;
  for (const key of questionKeys) {
    const normalized = normalizeQuestionKeyValue(key);
    if (normalized && rulesetKeys.has(normalized)) {
      hits += 1;
    }
  }
  return hits;
}

function inferRulesetFromQuestionKeys(questionKeys) {
  const normalizedKeys = normalizeQuestionKeySet(questionKeys);
  if (normalizedKeys.size === 0) {
    return null;
  }

  const map = getRulesetMap();
  const pvVersion = getRulesetVersion(map, 'PV');
  const hpVersion = getRulesetVersion(map, 'HeatPump');
  const pvHits = pvVersion
    ? countRulesetHits(normalizedKeys, 'PV', pvVersion)
    : 0;
  const hpHits = hpVersion
    ? countRulesetHits(normalizedKeys, 'HeatPump', hpVersion)
    : 0;

  if (pvHits === 0 && hpHits === 0) {
    return null;
  }

  if (pvHits === hpHits) {
    return null;
  }

  return pvHits > hpHits ? 'PV' : 'HeatPump';
}

function maybeLogKeyValidation(
  validateKeys,
  jobRunId,
  reportId,
  answerMap,
  rulesetInfo
) {
  if (!validateKeys || keyValidationLogCount >= 3 || !answerMap) {
    return;
  }

  keyValidationLogCount += 1;

  try {
    const answerKeySet = normalizeQuestionKeySet(answerMap.keys());
    const map = getRulesetMap();
    const pvVersion = getRulesetVersion(map, 'PV');
    const hpVersion = getRulesetVersion(map, 'HeatPump');
    const pvHits = pvVersion
      ? countRulesetHits(answerKeySet, 'PV', pvVersion)
      : 0;
    const hpHits = hpVersion
      ? countRulesetHits(answerKeySet, 'HeatPump', hpVersion)
      : 0;
    const resolvedRuleset =
      rulesetInfo && rulesetInfo.ruleSetName && rulesetInfo.ruleSetVersion
        ? `${rulesetInfo.ruleSetName}|${rulesetInfo.ruleSetVersion}`
        : null;

    console.log(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId,
        reportId,
        stage: 'validate-keys',
        totalDistinctAnswerKeys: answerKeySet.size,
        pvHits,
        hpHits,
        resolvedRuleset,
      })
    );
  } catch (error) {
    console.log(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId,
        reportId,
        stage: 'validate-keys',
        error: 'validation_failed',
        detail: error && error.message ? error.message : 'Unknown error',
      })
    );
  }
}

function finalizeRuleset(ruleSetName, templateName) {
  const map = getRulesetMap();
  const ruleSetVersion = getRulesetVersion(map, ruleSetName);
  if (!ruleSetVersion) {
    return null;
  }
  const result = { ruleSetName, ruleSetVersion };
  if (templateName) {
    result.templateName = templateName;
  }
  return result;
}

function resolveRuleset(reportRow, detailsPayload) {
  let templateName = extractTemplateName(reportRow);

  const reportCandidates = collectCandidateStrings(reportRow);
  for (const value of reportCandidates) {
    const detected = detectRulesetNameFromText(value);
    if (detected) {
      return finalizeRuleset(detected, templateName);
    }
  }

  if (Array.isArray(detailsPayload)) {
    for (const row of detailsPayload) {
      if (!templateName) {
        templateName = extractTemplateName(row) || templateName;
      }
      const detailCandidates = collectCandidateStrings(row);
      for (const value of detailCandidates) {
        const detected = detectRulesetNameFromText(value);
        if (detected) {
          return finalizeRuleset(detected, templateName);
        }
      }
    }
  }

  const questionKeys = extractQuestionKeys(detailsPayload);
  const inferred = inferRulesetFromQuestionKeys(questionKeys);
  if (inferred) {
    return finalizeRuleset(inferred, templateName);
  }

  return null;
}

function extractCertificate(rows) {
  for (const row of rows) {
    const questionId = row && row.QUESTION_ID ? String(row.QUESTION_ID).trim() : '';
    const questionText = row && row.Question ? String(row.Question) : '';
    const answer = row ? row.Answer : null;

    if (questionId === '1' || /certificate number/i.test(questionText || '')) {
      if (answer != null && String(answer).trim() !== '') {
        return truncate(String(answer).trim(), 100);
      }
    }
  }
  return null;
}

function extractAnswers(rows, reportId) {
  const answers = new Map();

  for (const row of rows) {
    if (!row || row.RecordType !== 'Detail') {
      continue;
    }

    const questionText = row.Question ? String(row.Question).trim() : '';
    const answerRaw = row.Answer;

    if (
      !questionText &&
      (answerRaw === null ||
        answerRaw === undefined ||
        String(answerRaw).trim() === '')
    ) {
      continue;
    }

    const questionKey = normalizeQuestionKey(row.QUESTION_ID, questionText);
    if (!questionKey) {
      continue;
    }

    let section = row.Section ? String(row.Section).trim() : null;
    if (
      row.GroupName &&
      String(row.GroupName).trim() &&
      String(row.GroupName).trim() !== 'N/A'
    ) {
      const combined = `${section || ''}${section ? ' | ' : ''}${String(
        row.GroupName
      ).trim()}`;
      section = truncate(combined, 200) || section;
    }

    let answerValue = null;
    if (answerRaw === null || answerRaw === undefined) {
      answerValue = null;
    } else if (
      typeof answerRaw === 'string' ||
      typeof answerRaw === 'number' ||
      typeof answerRaw === 'boolean'
    ) {
      answerValue = truncate(String(answerRaw), 4000);
    } else {
      try {
        answerValue = truncate(JSON.stringify(answerRaw), 4000);
      } catch {
        answerValue = null;
      }
    }

    if (!answers.has(questionKey)) {
      answers.set(questionKey, {
        reportId,
        questionKey,
        answerValue,
        section,
        questionText: truncate(questionText, 1000) || null,
      });
    }
  }

  return Array.from(answers.values());
}

function buildAnswerMap(answers) {
  const map = new Map();
  for (const answer of answers) {
    map.set(answer.questionKey, answer.answerValue);
  }
  return map;
}

async function ingestReports(pool, jobRunId, items, counts, dryRun) {
  const failedReportIds = new Set();
  let maxCompletedAtUtc = null;

  for (const item of items) {
    if (!maxCompletedAtUtc || item.completedAtUtc > maxCompletedAtUtc) {
      maxCompletedAtUtc = item.completedAtUtc;
    }

    if (dryRun) {
      const alreadyProcessed = await isProcessed(
        pool,
        INGESTION_JOB_NAME,
        item.reportId
      );
      if (alreadyProcessed) {
        counts.ingestAlreadyProcessed += 1;
      } else {
        counts.ingested += 1;
      }
      continue;
    }

    const transaction = new sql.Transaction(pool);
    await transaction.begin();

    try {
      const processedRequest = new sql.Request(transaction);
      processedRequest.input('jobName', sql.NVarChar(100), INGESTION_JOB_NAME);
      processedRequest.input('itemKey', sql.NVarChar(200), item.reportId);
      processedRequest.input('runId', sql.UniqueIdentifier, jobRunId);

      try {
        await processedRequest.query(
          'INSERT INTO dbo.ProcessedItems (JobName, ItemKey, RunId) VALUES (@jobName, @itemKey, @runId)'
        );
      } catch (error) {
        if (error && (error.number === 2627 || error.number === 2601)) {
          counts.ingestAlreadyProcessed += 1;
          await transaction.rollback();
          continue;
        }
        throw error;
      }

      const reportRequest = new sql.Request(transaction);
      reportRequest.input('reportId', sql.NVarChar(100), item.reportId);
      reportRequest.input(
        'completedAtUtc',
        sql.DateTime2,
        item.completedAtUtc
      );
      reportRequest.input(
        'certificationNumber',
        sql.NVarChar(100),
        item.certificationNumber
      );
      reportRequest.input('jobRunId', sql.UniqueIdentifier, jobRunId);

      try {
        await reportRequest.query(
          'INSERT INTO dbo.GoAuditsReports (GoAuditsReportId, CompletedAtUtc, CertificationNumber, JobRunId) VALUES (@reportId, @completedAtUtc, @certificationNumber, @jobRunId)'
        );
        counts.ingested += 1;
      } catch (error) {
        if (error && (error.number === 2627 || error.number === 2601)) {
          counts.ingestAlreadyProcessed += 1;
        } else {
          throw error;
        }
      }

      await transaction.commit();
    } catch (error) {
      await transaction.rollback();
      counts.ingestFailedCount += 1;
      failedReportIds.add(item.reportId);
    }
  }

  return { maxCompletedAtUtc, failedReportIds };
}

async function processDetails(
  pool,
  token,
  reportId,
  jobRunId,
  counts,
  detailsUrl,
  dryRun
) {
  try {
    const alreadyProcessed = await isProcessed(
      pool,
      ENRICHMENT_JOB_NAME,
      reportId
    );
    if (alreadyProcessed) {
      counts.detailsAlreadyProcessed += 1;
      return { detailsPayload: null, answerMap: null, failed: false };
    }

    const rows = await fetchWithRetry(detailsUrl, token, {
      ...DETAILS_REQUEST_BASE,
      audit_id: String(reportId),
    });

    const hasDetail =
      Array.isArray(rows) &&
      rows.some((row) => row && row.RecordType === 'Detail');
    if (!Array.isArray(rows) || rows.length === 0 || !hasDetail) {
      counts.detailsFailedCount += 1;
      return { detailsPayload: rows, answerMap: null, failed: true };
    }

    const cert = extractCertificate(rows);
    const answers = extractAnswers(rows, reportId);
    const answerMap = buildAnswerMap(answers);

    if (dryRun) {
      const currentCert = await getCurrentCertificationNumber(pool, reportId);
      const certNow = cert || currentCert;

      if (cert && (!currentCert || String(currentCert).trim() === '')) {
        counts.certUpdatedCount += 1;
      }

      counts.answersInsertedCount += answers.length;
      if (answers.length > 0) {
        counts.markedProcessedCount += 1;
        if (!certNow || String(certNow).trim() === '') {
          counts.certMissingCount += 1;
        }
      }

      counts.detailsProcessed += 1;
      return { detailsPayload: rows, answerMap, failed: false };
    }

    const transaction = new sql.Transaction(pool);
    await transaction.begin();

    try {
      if (cert) {
        const requestUpdate = new sql.Request(transaction);
        requestUpdate.input('reportId', sql.NVarChar(100), reportId);
        requestUpdate.input('cert', sql.NVarChar(100), cert);

        const updateResult = await requestUpdate.query(
          'UPDATE dbo.GoAuditsReports SET CertificationNumber=@cert WHERE GoAuditsReportId=@reportId AND (CertificationNumber IS NULL OR CertificationNumber = \'\')'
        );
        counts.certUpdatedCount +=
          updateResult &&
          updateResult.rowsAffected &&
          updateResult.rowsAffected[0] > 0
            ? 1
            : 0;
      }

      for (const answer of answers) {
        const req = new sql.Request(transaction);
        req.input('reportId', sql.NVarChar(100), answer.reportId);
        req.input('questionKey', sql.NVarChar(256), answer.questionKey);
        req.input('answerValue', sql.NVarChar(4000), answer.answerValue);
        req.input('section', sql.NVarChar(200), answer.section);
        req.input('questionText', sql.NVarChar(1000), answer.questionText);
        req.input('jobRunId', sql.UniqueIdentifier, jobRunId);

        try {
          await req.query(
            'INSERT INTO dbo.GoAuditsReportAnswers (GoAuditsReportId, QuestionKey, AnswerValue, Section, QuestionText, JobRunId) VALUES (@reportId, @questionKey, @answerValue, @section, @questionText, @jobRunId)'
          );
          counts.answersInsertedCount += 1;
        } catch (error) {
          if (!(error && (error.number === 2627 || error.number === 2601))) {
            throw error;
          }
        }
      }

      const reqCheck = new sql.Request(transaction);
      reqCheck.input('reportId', sql.NVarChar(100), reportId);
      const checkResult = await reqCheck.query(
        `
          SELECT
            (SELECT COUNT(1) FROM dbo.GoAuditsReportAnswers WHERE GoAuditsReportId=@reportId) AS AnswerCount,
            (SELECT CertificationNumber FROM dbo.GoAuditsReports WHERE GoAuditsReportId=@reportId) AS CertNow;
        `
      );

      const answersExist = checkResult.recordset[0].AnswerCount > 0;
      const certNow = checkResult.recordset[0].CertNow;

      if (answersExist && (!certNow || String(certNow).trim() === '')) {
        counts.certMissingCount += 1;
      }

      if (answersExist) {
        const reqProcessed = new sql.Request(transaction);
        reqProcessed.input('jobName', sql.NVarChar(100), ENRICHMENT_JOB_NAME);
        reqProcessed.input('itemKey', sql.NVarChar(200), reportId);
        reqProcessed.input('runId', sql.UniqueIdentifier, jobRunId);
        try {
          await reqProcessed.query(
            'INSERT INTO dbo.ProcessedItems (JobName, ItemKey, RunId) VALUES (@jobName, @itemKey, @runId)'
          );
          counts.markedProcessedCount += 1;
        } catch (error) {
          if (!(error && (error.number === 2627 || error.number === 2601))) {
            throw error;
          }
        }
      }

      await transaction.commit();
      counts.detailsProcessed += 1;
      return { detailsPayload: rows, answerMap: null, failed: false };
    } catch (error) {
      await transaction.rollback();
      counts.detailsFailedCount += 1;
      return { detailsPayload: rows, answerMap: null, failed: true };
    }
  } catch (error) {
    if (error && error.fatal) {
      throw error;
    }
    counts.detailsFailedCount += 1;
    return { detailsPayload: null, answerMap: null, failed: true };
  }
}

async function scoreReport(
  pool,
  reportId,
  rulesetName,
  rulesetVersion,
  rulesDoc,
  jobRunId,
  answerMap,
  counts
) {
  const transaction = new sql.Transaction(pool);
  await transaction.begin();

  const processedKey = `${reportId}|${rulesetName}|${rulesetVersion}`;

  try {
    const procReq = new sql.Request(transaction);
    procReq.input('jobName', sql.NVarChar(100), SCORING_JOB_NAME);
    procReq.input('itemKey', sql.NVarChar(200), processedKey);
    procReq.input('runId', sql.UniqueIdentifier, jobRunId);

    await procReq.query(
      'INSERT INTO dbo.ProcessedItems (JobName, ItemKey, RunId) VALUES (@jobName, @itemKey, @runId)'
    );
  } catch (error) {
    if (error && (error.number === 2627 || error.number === 2601)) {
      await transaction.rollback();
      counts.scoreAlreadyProcessed += 1;
      return;
    }
    await transaction.rollback();
    counts.scoreFailedCount += 1;
    return;
  }

  try {
    const rulesetKeys = getRulesetQuestionKeys(rulesetName, rulesetVersion);
    let eligible = false;
    for (const key of answerMap.keys()) {
      const normalized = normalizeQuestionKeyValue(key);
      if (normalized && rulesetKeys.has(normalized)) {
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

    const outcome =
      determineOutcome(rulesDoc.scoring, majorCount, minorCount) || 'Unknown';
    const scoreValue = computeScoreValue(rulesDoc.scoring.scoreValue, outcome);

    for (const f of findings) {
      const req = new sql.Request(transaction);
      req.input('reportId', sql.NVarChar(100), reportId);
      req.input('ruleSetName', sql.NVarChar(50), rulesetName);
      req.input('ruleSetVersion', sql.NVarChar(20), rulesetVersion);
      req.input('questionKey', sql.NVarChar(256), f.questionKey);
      req.input('answerValue', sql.NVarChar(sql.MAX), f.answerValue);
      req.input('findingSeverity', sql.NVarChar(10), f.severity);
      req.input('findingCode', sql.NVarChar(50), f.code);
      req.input(
        'majorNonCompliantText',
        sql.NVarChar(sql.MAX),
        f.majorNonCompliantText
      );
      req.input(
        'minorNonCompliantText',
        sql.NVarChar(sql.MAX),
        f.minorNonCompliantText
      );
      req.input('jobRunId', sql.UniqueIdentifier, jobRunId);
      try {
        await req.query(
          'INSERT INTO dbo.GoAuditsFindings (GoAuditsReportId, RuleSetName, RuleSetVersion, QuestionKey, AnswerValue, FindingSeverity, FindingCode, MajorNonCompliantText, MinorNonCompliantText, JobRunId) VALUES (@reportId, @ruleSetName, @ruleSetVersion, @questionKey, @answerValue, @findingSeverity, @findingCode, @majorNonCompliantText, @minorNonCompliantText, @jobRunId)'
        );
        counts.findingsInsertedCount += 1;
      } catch (error) {
        if (error && (error.number === 2627 || error.number === 2601)) {
          await req.query(
            `UPDATE dbo.GoAuditsFindings
SET
  MajorNonCompliantText = COALESCE(MajorNonCompliantText, @majorNonCompliantText),
  MinorNonCompliantText = COALESCE(MinorNonCompliantText, @minorNonCompliantText)
WHERE GoAuditsReportId = @reportId
  AND RuleSetName      = @ruleSetName
  AND RuleSetVersion   = @ruleSetVersion
  AND QuestionKey      = @questionKey;`
          );
        } else {
          throw error;
        }
      }
    }

    const scoreReq = new sql.Request(transaction);
    scoreReq.input('reportId', sql.NVarChar(100), reportId);
    scoreReq.input('ruleSetName', sql.NVarChar(50), rulesetName);
    scoreReq.input('ruleSetVersion', sql.NVarChar(20), rulesetVersion);
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
    counts.scoreProcessed += 1;
    counts.majorCountTotal += majorCount;
    counts.minorCountTotal += minorCount;
  } catch (error) {
    await transaction.rollback();
    counts.scoreFailedCount += 1;
  }
}

function scoreReportDryRun(rulesDoc, answerMap, counts) {
  const rulesetKeys = extractRulesetQuestionKeys(rulesDoc);
  let eligible = false;
  for (const key of answerMap.keys()) {
    const normalized = normalizeQuestionKeyValue(key);
    if (normalized && rulesetKeys.has(normalized)) {
      eligible = true;
      break;
    }
  }

  if (!eligible) {
    counts.skippedNotEligible += 1;
    return;
  }

  const defaultNorm = rulesDoc.answerNormalization || {};
  let majorCount = 0;
  let minorCount = 0;
  let findingsInserted = 0;

  for (const rule of rulesDoc.rules) {
    const finding = evaluateRule(rule, answerMap, defaultNorm);
    if (finding) {
      findingsInserted += 1;
      if (finding.severity === 'Major') {
        majorCount += 1;
      } else if (finding.severity === 'Minor') {
        minorCount += 1;
      }
    }
  }

  const outcome =
    determineOutcome(rulesDoc.scoring, majorCount, minorCount) || 'Unknown';
  computeScoreValue(rulesDoc.scoring.scoreValue, outcome);

  counts.scoreProcessed += 1;
  counts.findingsInsertedCount += findingsInserted;
  counts.majorCountTotal += majorCount;
  counts.minorCountTotal += minorCount;
}

async function processReport(
  pool,
  token,
  jobRunId,
  item,
  counts,
  detailsUrl,
  dryRun,
  validateKeys,
  reportColumnSet
) {
  const reportId = item.reportId;

  const detailsResult = await processDetails(
    pool,
    token,
    reportId,
    jobRunId,
    counts,
    detailsUrl,
    dryRun
  );

  const reportRow = await loadReportRow(pool, reportId, reportColumnSet);
  const reportContext = {
    ...item.rawItem,
    ...reportRow,
  };

  let answerMap = detailsResult.answerMap;
  let rulesetInfo;

  try {
    rulesetInfo = resolveRuleset(reportContext, detailsResult.detailsPayload);
  } catch (error) {
    counts.scoreFailedCount += 1;
    console.log(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId,
        reportId,
        stage: 'scoring',
        error: 'ruleset_resolution_failed',
        detail: error && error.message ? error.message : 'Unknown error',
      })
    );
    return;
  }

  if (!rulesetInfo) {
    if (!answerMap) {
      answerMap = await loadAnswers(pool, reportId);
    }
    if (answerMap && answerMap.size > 0) {
      try {
        rulesetInfo = resolveRuleset(reportContext, {
          answerKeys: Array.from(answerMap.keys()),
        });
      } catch (error) {
        counts.scoreFailedCount += 1;
        console.log(
          JSON.stringify({
            jobName: JOB_NAME,
            jobRunId,
            reportId,
            stage: 'scoring',
            error: 'ruleset_resolution_failed',
            detail: error && error.message ? error.message : 'Unknown error',
          })
        );
        return;
      }
    }
  }

  if (!answerMap) {
    answerMap = await loadAnswers(pool, reportId);
  }

  maybeLogKeyValidation(
    validateKeys,
    jobRunId,
    reportId,
    answerMap,
    rulesetInfo
  );

  if (!rulesetInfo || !rulesetInfo.ruleSetName || !rulesetInfo.ruleSetVersion) {
    counts.skippedNotEligible += 1;
    console.log(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId,
        reportId,
        stage: 'scoring',
        skippedReason: 'ruleset_not_resolved',
      })
    );
    return;
  }

  if (!answerMap || answerMap.size === 0) {
    counts.skippedNotEligible += 1;
    console.log(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId,
        reportId,
        stage: 'scoring',
        skippedReason: 'no_answers',
      })
    );
    return;
  }

  const processedKey = `${reportId}|${rulesetInfo.ruleSetName}|${rulesetInfo.ruleSetVersion}`;
  if (dryRun) {
    const alreadyProcessed = await isProcessed(
      pool,
      SCORING_JOB_NAME,
      processedKey
    );
    if (alreadyProcessed) {
      counts.scoreAlreadyProcessed += 1;
      return;
    }
  }

  let rulesDoc;
  try {
    rulesDoc = getRulesDoc(
      rulesetInfo.ruleSetName,
      rulesetInfo.ruleSetVersion
    );
  } catch (error) {
    counts.scoreFailedCount += 1;
    console.log(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId,
        reportId,
        stage: 'scoring',
        error: 'ruleset_load_failed',
        detail: error && error.message ? error.message : 'Unknown error',
      })
    );
    return;
  }

  if (dryRun) {
    scoreReportDryRun(rulesDoc, answerMap, counts);
    return;
  }

  await scoreReport(
    pool,
    reportId,
    rulesetInfo.ruleSetName,
    rulesetInfo.ruleSetVersion,
    rulesDoc,
    jobRunId,
    answerMap,
    counts
  );
}

async function main() {
  const jobRunId = randomUUID();
  const startedAtUtc = new Date().toISOString();
  const correlationId =
    process.env.ACA_JOB_RUN_ID || process.env.HOSTNAME || jobRunId;

  const dryRun = parseBoolEnv('GOAUDITS_PIPELINE_DRYRUN', false);
  const validateKeys = parseBoolEnv('GOAUDITS_PIPELINE_VALIDATE_KEYS', false);
  const materialiseEmail =
    process.env.GOAUDITS_PIPELINE_MATERIALISE_EMAIL === 'true';
  const materialiseScope = getEnv(
    'GOAUDITS_PIPELINE_MATERIALISE_EMAIL_SCOPE',
    'all'
  )
    .trim()
    .toLowerCase();

  const counts = {
    fetched: 0,
    eligible: 0,
    skipped: 0,
    selected: 0,
    ingested: 0,
    ingestAlreadyProcessed: 0,
    ingestFailedCount: 0,
    detailsProcessed: 0,
    detailsAlreadyProcessed: 0,
    answersInsertedCount: 0,
    certUpdatedCount: 0,
    markedProcessedCount: 0,
    certMissingCount: 0,
    detailsFailedCount: 0,
    scoreProcessed: 0,
    scoreAlreadyProcessed: 0,
    skippedNotEligible: 0,
    findingsInsertedCount: 0,
    majorCountTotal: 0,
    minorCountTotal: 0,
    scoreFailedCount: 0,
    emailOutboxInserted: 0,
    emailOutboxSkippedAlreadyExists: 0,
    emailOutboxMissingRecipient: 0,
    pages: 0,
  };

  let pool;
  let status = 'Succeeded';
  let message = '';
  let completedAtUtc;
  let startDate;
  let endDate;

  try {
    pool = await getSqlPool();
    if (!dryRun) {
      await insertJobRun(
        pool,
        jobRunId,
        'Running',
        'Starting GoAudits pipeline',
        correlationId
      );
    }
    getRulesetMap();

    const reportColumnSet = await loadReportMetadataColumns(pool);
    const { watermark, exists: watermarkExists } = await getWatermark(pool);

    const startOverride = parseOptionalDateEnv('GOAUDITS_START_DATE');
    const endOverride = parseOptionalDateEnv('GOAUDITS_END_DATE', {
      endOfDay: true,
    });

    if (startOverride && endOverride && endOverride < startOverride) {
      throw new Error('GOAUDITS_END_DATE must be after GOAUDITS_START_DATE.');
    }

    const lowerBound = startOverride || watermark;
    const upperBound = endOverride || null;
    const payload = buildPayload(
      startOverride || watermark,
      endOverride || new Date()
    );
    startDate = payload.start_date;
    endDate = payload.end_date;

    const bearerToken = await getBearerToken();
    const { items, keys, pages } = await collectAudits(
      bearerToken,
      payload
    );
    counts.pages = pages;

    if (Array.isArray(items)) {
      counts.fetched = items.length;
    }

    if (keys && keys.length > 0) {
      console.log(
        JSON.stringify({
          jobName: JOB_NAME,
          jobRunId,
          info: 'First item keys',
          keys,
        })
      );
    }

    const eligible = [];
    for (const item of items || []) {
      const reportId =
        getFirstDefined(item, [
          'ID',
          'Id',
          'auditId',
          'audit_id',
          'id',
          'reportId',
          'report_id',
        ]) || null;

      const completedAt = parseCompletedAtValue(item);

      const certificationNumber =
        getFirstDefined(item, [
          'certificationNumber',
          'certification_number',
          'certification',
          'certNumber',
          'cert_number',
        ]) || null;

      if (!reportId || !completedAt) {
        counts.skipped += 1;
        continue;
      }

      if (completedAt <= lowerBound) {
        counts.skipped += 1;
        continue;
      }

      if (upperBound && completedAt > upperBound) {
        counts.skipped += 1;
        continue;
      }

      const normalizedCompleted = new Date(completedAt);
      eligible.push({
        reportId: String(reportId),
        completedAtUtc: normalizedCompleted,
        certificationNumber: certificationNumber
          ? String(certificationNumber)
          : null,
        rawItem: item,
      });
    }

    counts.eligible = eligible.length;

    eligible.sort((a, b) => {
      if (a.completedAtUtc.getTime() !== b.completedAtUtc.getTime()) {
        return a.completedAtUtc - b.completedAtUtc;
      }
      return String(a.reportId).localeCompare(String(b.reportId));
    });

    const batchSize = parsePositiveInt(
      process.env.GOAUDITS_PIPELINE_BATCH_SIZE,
      DEFAULT_BATCH_SIZE
    );
    let selected = eligible.slice(0, batchSize);
    if (selected.length > 0 && eligible.length > selected.length) {
      const lastTime = selected[selected.length - 1].completedAtUtc.getTime();
      let index = selected.length;
      while (
        index < eligible.length &&
        eligible[index].completedAtUtc.getTime() === lastTime
      ) {
        selected.push(eligible[index]);
        index += 1;
      }
    }
    counts.selected = selected.length;

    if (selected.length > batchSize) {
      console.log(
        JSON.stringify({
          jobName: JOB_NAME,
          jobRunId,
          info: 'Expanded batch to include tied completion timestamps',
          batchSize,
          selected: selected.length,
        })
      );
    }

    if (selected.length === 0) {
      completedAtUtc = new Date().toISOString();
      message = summarizeCounts(counts);
      if (!dryRun) {
        await updateJobRun(pool, jobRunId, status, message);
      }

      console.log(
        JSON.stringify({
          jobName: JOB_NAME,
          jobRunId,
          startedAtUtc,
          completedAtUtc,
          status,
          counts,
          start_date: startDate,
          end_date: endDate,
          dryRun,
        })
      );
      process.exit(0);
      return;
    }

    const { maxCompletedAtUtc, failedReportIds } = await ingestReports(
      pool,
      jobRunId,
      selected,
      counts,
      dryRun
    );

    if (
      !dryRun &&
      (maxCompletedAtUtc || !watermarkExists) &&
      counts.ingestFailedCount === 0
    ) {
      let nextWatermark = watermark;
      if (maxCompletedAtUtc && maxCompletedAtUtc > watermark) {
        nextWatermark = maxCompletedAtUtc;
      }
      await updateWatermark(pool, nextWatermark);
    }

    const detailsUrl = getEnv(
      'GOAUDITS_AUDITDETAILS_URL',
      DEFAULT_DETAILS_URL
    );

    const queue = selected.filter(
      (item) => !failedReportIds.has(item.reportId)
    );
    const batchReportIds = Array.from(
      new Set(queue.map((item) => item.reportId))
    );
    const workers = new Array(DETAILS_CONCURRENCY).fill(0).map(async () => {
      while (queue.length) {
        const report = queue.shift();
        await processReport(
          pool,
          bearerToken,
          jobRunId,
          report,
          counts,
          detailsUrl,
          dryRun,
          validateKeys,
          reportColumnSet
        );
      }
    });

    await Promise.all(workers);

    if (materialiseEmail) {
      if (dryRun) {
        console.log(
          JSON.stringify({
            jobName: JOB_NAME,
            jobRunId,
            stage: 'email-outbox',
            skippedReason: 'dryrun',
          })
        );
      } else if (materialiseScope === 'batch' && batchReportIds.length === 0) {
        console.log(
          JSON.stringify({
            jobName: JOB_NAME,
            jobRunId,
            stage: 'email-outbox',
            skippedReason: 'no_batch_ids',
          })
        );
      } else {
        const scope = materialiseScope === 'batch' ? 'batch' : 'all';
        const result = await materialiseEmailOutbox(pool, {
          scope,
          reportIds: scope === 'batch' ? batchReportIds : null,
        });
        counts.emailOutboxInserted += result.inserted || 0;
        counts.emailOutboxSkippedAlreadyExists +=
          result.skippedAlreadyExists || 0;
        counts.emailOutboxMissingRecipient += result.missingRecipient || 0;
      }
    }

    completedAtUtc = new Date().toISOString();
    message = summarizeCounts(counts);
    if (!dryRun) {
      await updateJobRun(pool, jobRunId, status, message);
    }

    console.log(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId,
        startedAtUtc,
        completedAtUtc,
        status,
        counts,
        start_date: startDate,
        end_date: endDate,
        dryRun,
      })
    );
    process.exit(0);
  } catch (error) {
    status = 'Failed';
    completedAtUtc = new Date().toISOString();
    const safeMessage = truncateMessage(
      error && error.message ? error.message : 'GoAudits pipeline failed.'
    );
    message = `${summarizeCounts(counts)} | Error: ${safeMessage}`;

    if (pool && !dryRun) {
      try {
        await updateJobRun(pool, jobRunId, status, message);
      } catch {
        // best effort
      }
    }

    console.error(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId,
        startedAtUtc,
        completedAtUtc,
        status,
        counts,
        error: safeMessage,
        start_date: startDate,
        end_date: endDate,
        dryRun,
      })
    );
    process.exit(1);
  }
}

main();
