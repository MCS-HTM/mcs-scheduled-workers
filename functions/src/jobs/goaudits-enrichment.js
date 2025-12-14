const { randomUUID, createHash } = require('crypto');
const { DefaultAzureCredential } = require('@azure/identity');
const { SecretClient } = require('@azure/keyvault-secrets');
const { sql, getSqlPool } = require('../shared/sql');

const JOB_NAME = 'GoAuditsEnrichment';
const DEFAULT_DETAILS_URL =
  'https://api.goaudits.com/v1/audits/getauditdetailsbyid';
const DEFAULT_BATCH_SIZE = 50;
const MAX_RETRIES = 5;
const REQUEST_TIMEOUT_MS = 30000;
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

function truncate(str, max) {
  if (str == null) return null;
  const s = String(str);
  return s.length > max ? s.slice(0, max) : s;
}

function getEnv(name, fallback) {
  const value = process.env[name];
  return value && value.trim().length > 0 ? value : fallback;
}

function parsePositiveInt(value, defaultValue) {
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : defaultValue;
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
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

async function getBearerToken() {
  const keyVaultUri = process.env.KEYVAULT_URI;
  const secretName = getEnv('GOAUDITS_BEARER_SECRET_NAME', 'goaudits-bearer-token');

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
        const error = new Error('GoAudits details API response was not valid JSON.');
        error.retryable = false;
        throw error;
      });

      if (!Array.isArray(parsed)) {
        const error = new Error('GoAudits details API response was not an array.');
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

async function selectBatch(pool, batchSize) {
  const request = pool.request();
  request.input('batchSize', sql.Int, batchSize);
  request.input('jobName', sql.NVarChar(100), JOB_NAME);

  const query = `
    SELECT TOP (@batchSize) r.GoAuditsReportId
    FROM dbo.GoAuditsReports r
    WHERE
      (
        r.CertificationNumber IS NULL OR r.CertificationNumber = ''
        OR NOT EXISTS (
          SELECT 1 FROM dbo.GoAuditsReportAnswers a WHERE a.GoAuditsReportId = r.GoAuditsReportId
        )
      )
      AND NOT EXISTS (
        SELECT 1 FROM dbo.ProcessedItems p
        WHERE p.JobName = @jobName AND p.ItemKey = r.GoAuditsReportId
      )
    ORDER BY r.CompletedAtUtc DESC;
  `;

  const result = await request.query(query);
  return result.recordset.map((row) => row.GoAuditsReportId);
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

    if (!questionText && (answerRaw === null || answerRaw === undefined || String(answerRaw).trim() === '')) {
      continue;
    }

    const questionKey = normalizeQuestionKey(row.QUESTION_ID, questionText);
    if (!questionKey) {
      continue;
    }

    let section = row.Section ? String(row.Section).trim() : null;
    if (row.GroupName && String(row.GroupName).trim() && String(row.GroupName).trim() !== 'N/A') {
      const combined = `${section || ''}${section ? ' | ' : ''}${String(row.GroupName).trim()}`;
      section = truncate(combined, 200) || section;
    }

    let answerValue = null;
    if (answerRaw === null || answerRaw === undefined) {
      answerValue = null;
    } else if (typeof answerRaw === 'string' || typeof answerRaw === 'number' || typeof answerRaw === 'boolean') {
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

async function processReport(pool, token, reportId, jobRunId, counts, detailsUrl) {
  try {
    const rows = await fetchWithRetry(detailsUrl, token, {
      ...DETAILS_REQUEST_BASE,
      audit_id: String(reportId),
    });

    const hasDetail = Array.isArray(rows) && rows.some((row) => row && row.RecordType === 'Detail');
    if (!Array.isArray(rows) || rows.length === 0 || !hasDetail) {
      const error = new Error('No detail rows returned for report.');
      error.retryable = false;
      throw error;
    }

    const cert = extractCertificate(rows);
    const answers = extractAnswers(rows, reportId);

    const transaction = new sql.Transaction(pool);
    await transaction.begin();

    const requestUpdate = new sql.Request(transaction);
    requestUpdate.input('reportId', sql.NVarChar(100), reportId);
    requestUpdate.input('cert', sql.NVarChar(100), cert);

    if (cert) {
      const updateResult = await requestUpdate.query(
        'UPDATE dbo.GoAuditsReports SET CertificationNumber=@cert WHERE GoAuditsReportId=@reportId AND (CertificationNumber IS NULL OR CertificationNumber = \'\')'
      );
      counts.certUpdatedCount +=
        updateResult && updateResult.rowsAffected && updateResult.rowsAffected[0] > 0 ? 1 : 0;
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
      reqProcessed.input('jobName', sql.NVarChar(100), JOB_NAME);
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
    counts.processed += 1;
  } catch (error) {
    if (error && error.fatal) {
      throw error;
    }
    counts.failedCount += 1;
  }
}

async function run() {
  const jobRunId = randomUUID();
  const startedAtUtc = new Date().toISOString();
  const counts = {
    selected: 0,
    processed: 0,
    certUpdatedCount: 0,
    answersInsertedCount: 0,
    markedProcessedCount: 0,
    failedCount: 0,
    authFatal: false,
    certMissingCount: 0,
  };

  let pool;
  let status = 'Succeeded';
  let errorMessage = '';

  try {
    pool = await getSqlPool();
    await insertJobRun(pool, jobRunId, 'Running', 'Starting GoAudits enrichment');

    console.log(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId,
        startedAtUtc,
      })
    );

    const batchSize = parsePositiveInt(
      process.env.GOAUDITS_ENRICH_BATCH_SIZE,
      DEFAULT_BATCH_SIZE
    );
    const concurrency = clamp(
      parsePositiveInt(process.env.GOAUDITS_ENRICH_CONCURRENCY, 1),
      1,
      3
    );
    const detailsUrl = getEnv(
      'GOAUDITS_AUDITDETAILS_URL',
      DEFAULT_DETAILS_URL
    );

    const reportIds = await selectBatch(pool, batchSize);
    counts.selected = reportIds.length;
    if (reportIds.length === 0) {
      errorMessage = 'No reports require enrichment.';
      await updateJobRun(pool, jobRunId, 'Succeeded', errorMessage);
      console.log(
        JSON.stringify({
          jobName: JOB_NAME,
          jobRunId,
          completedAtUtc: new Date().toISOString(),
          status,
          counts,
          message: errorMessage,
        })
      );
      process.exit(0);
    }

    const token = await getBearerToken();

    const queue = [...reportIds];
    const workers = new Array(concurrency).fill(0).map(async () => {
      while (queue.length) {
        const reportId = queue.shift();
        await processReport(pool, token, reportId, jobRunId, counts, detailsUrl);
      }
    });

    await Promise.all(workers);

    const completedAtUtc = new Date().toISOString();
    const message = `Selected=${counts.selected} Processed=${counts.processed} CertUpdated=${counts.certUpdatedCount} AnswersInserted=${counts.answersInsertedCount} MarkedProcessed=${counts.markedProcessedCount} CertMissing=${counts.certMissingCount} Failed=${counts.failedCount}`;
    await updateJobRun(pool, jobRunId, status, message);

    console.log(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId,
        completedAtUtc,
        status,
        counts,
      })
    );
    process.exit(0);
  } catch (error) {
    status = 'Failed';
    const completedAtUtc = new Date().toISOString();
    if (error && error.fatal) {
      counts.authFatal = true;
    }
    errorMessage =
      error && error.message
        ? error.message
        : 'GoAudits enrichment failed.';

    try {
      if (pool) {
        await updateJobRun(pool, jobRunId, status, errorMessage);
      }
    } catch {
      // best effort
    }

    console.error(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId,
        completedAtUtc,
        status,
        counts,
        error: errorMessage,
      })
    );
    process.exit(1);
  }
}

run();
