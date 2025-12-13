const { randomUUID } = require('crypto');
const { DefaultAzureCredential } = require('@azure/identity');
const { SecretClient } = require('@azure/keyvault-secrets');
const { sql, getSqlPool } = require('../shared/sql');

const JOB_NAME = 'GoAuditsIngestion';
const DEFAULT_AUDIT_URL =
  'https://api.goaudits.com/v1/audits/getauditsummary';
const DEFAULT_SECRET_NAME = 'goaudits-bearer-token';
const MAX_MESSAGE_LENGTH = 4000;
const REQUEST_TIMEOUT_MS = 30000;
const MAX_RETRIES = 5;

const credential = new DefaultAzureCredential({
  managedIdentityClientId: process.env.AZURE_CLIENT_ID,
});

function truncateMessage(message) {
  if (!message) {
    return '';
  }

  return message.length > MAX_MESSAGE_LENGTH
    ? message.slice(0, MAX_MESSAGE_LENGTH)
    : message;
}

function formatDateOnly(date) {
  return date.toISOString().slice(0, 10);
}

function getEnv(name, defaultValue) {
  const value = process.env[name];
  return value && value.trim().length > 0 ? value : defaultValue;
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
  request.input('jobName', sql.NVarChar(100), JOB_NAME);
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
    const timeout = setTimeout(
      () => controller.abort(),
      REQUEST_TIMEOUT_MS
    );

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

function buildPayload(watermark) {
  const startOverride = getEnv('GOAUDITS_START_DATE', '');
  const endOverride = getEnv('GOAUDITS_END_DATE', '');
  const startDate = startOverride || formatDateOnly(watermark);
  const endDate = endOverride || formatDateOnly(new Date());

  return {
    start_date: startDate,
    end_date: endDate,
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
  return `Fetched=${counts.fetched} Eligible=${counts.eligible} Ingested=${counts.ingested} Skipped=${counts.skipped} AlreadyProcessed=${counts.alreadyProcessed}`;
}

async function ingestIntoSql(
  pool,
  runId,
  items,
  maxCompletedAtUtc,
  counts,
  watermarkExists,
  currentWatermark
) {
  const transaction = new sql.Transaction(pool);
  await transaction.begin();

  try {
    for (const item of items) {
      const processedRequest = new sql.Request(transaction);
      processedRequest.input('jobName', sql.NVarChar(100), JOB_NAME);
      processedRequest.input('itemKey', sql.NVarChar(200), item.reportId);
      processedRequest.input('runId', sql.UniqueIdentifier, runId);

      try {
        await processedRequest.query(
          'INSERT INTO dbo.ProcessedItems (JobName, ItemKey, RunId) VALUES (@jobName, @itemKey, @runId)'
        );
      } catch (error) {
        // PK violation means already processed
        if (error && (error.number === 2627 || error.number === 2601)) {
          counts.alreadyProcessed += 1;
          continue;
        }
        throw error;
      }

      const reportRequest = new sql.Request(transaction);
      reportRequest.input(
        'reportId',
        sql.NVarChar(100),
        item.reportId
      );
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
      reportRequest.input('jobRunId', sql.UniqueIdentifier, runId);

      await reportRequest.query(
        'INSERT INTO dbo.GoAuditsReports (GoAuditsReportId, CompletedAtUtc, CertificationNumber, JobRunId) VALUES (@reportId, @completedAtUtc, @certificationNumber, @jobRunId)'
      );

      counts.ingested += 1;
    }

    if (maxCompletedAtUtc || !watermarkExists) {
      const watermarkRequest = new sql.Request(transaction);
      watermarkRequest.input('jobName', sql.NVarChar(100), JOB_NAME);
      watermarkRequest.input(
        'watermarkUtc',
        sql.DateTime2,
        maxCompletedAtUtc || currentWatermark
      );

      await watermarkRequest.query(
        `
IF EXISTS (SELECT 1 FROM dbo.JobWatermark WHERE JobName = @jobName)
  UPDATE dbo.JobWatermark SET WatermarkUtc = @watermarkUtc, UpdatedUtc = SYSUTCDATETIME() WHERE JobName = @jobName;
ELSE
  INSERT INTO dbo.JobWatermark (JobName, WatermarkUtc) VALUES (@jobName, @watermarkUtc);
      `.trim()
      );
    }

    await transaction.commit();
  } catch (error) {
    await transaction.rollback();
    throw error;
  }
}

async function main() {
  const runId = randomUUID();
  const startedAtUtc = new Date().toISOString();
  const correlationId =
    process.env.ACA_JOB_RUN_ID || process.env.HOSTNAME || runId;

  const counts = {
    fetched: 0,
    eligible: 0,
    ingested: 0,
    skipped: 0,
    alreadyProcessed: 0,
    pages: 0,
  };

  let pool;
  let status = 'Succeeded';
  let message = '';
  let completedAtUtc;
  let payload;
  let startDate;
  let endDate;

  try {
    pool = await getSqlPool();
    await insertJobRun(pool, runId, 'Running', 'Starting GoAudits ingestion', correlationId);

    const { watermark, exists: watermarkExists } = await getWatermark(pool);
    const bearerToken = await getBearerToken();

    payload = buildPayload(watermark);
    startDate = payload.start_date;
    endDate = payload.end_date;

    const { items, keys, pages } = await collectAudits(bearerToken, payload);
    counts.pages = pages;

    let maxCompletedAtUtc = null;
    const toIngest = [];

    if (Array.isArray(items)) {
      counts.fetched = items.length;
    }

    if (keys && keys.length > 0) {
      console.log(
        JSON.stringify({
          jobName: JOB_NAME,
          jobRunId: runId,
          info: 'First item keys',
          keys,
        })
      );
    }

    for (const item of items) {
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

      if (completedAt <= watermark) {
        counts.skipped += 1;
        continue;
      }

      const normalizedCompleted = new Date(completedAt);
      toIngest.push({
        reportId: String(reportId),
        completedAtUtc: normalizedCompleted,
        certificationNumber: certificationNumber
          ? String(certificationNumber)
          : null,
      });

      if (!maxCompletedAtUtc || normalizedCompleted > maxCompletedAtUtc) {
        maxCompletedAtUtc = normalizedCompleted;
      }
    }

    counts.eligible = toIngest.length;

    if (toIngest.length > 0 || !watermarkExists) {
      await ingestIntoSql(
        pool,
        runId,
        toIngest,
        maxCompletedAtUtc,
        counts,
        watermarkExists,
        watermark
      );
    }

    completedAtUtc = new Date().toISOString();
    message = summarizeCounts(counts);
    await updateJobRun(pool, runId, 'Succeeded', message);

    console.log(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId: runId,
        startedAtUtc,
        completedAtUtc,
        status,
        counts,
        start_date: startDate,
        end_date: endDate,
      })
    );
    process.exit(0);
  } catch (error) {
    status = 'Failed';
    completedAtUtc = new Date().toISOString();
    const safeMessage = truncateMessage(
      error && error.message ? error.message : 'GoAudits ingestion failed.'
    );
    message = `${summarizeCounts(counts)} | Error: ${safeMessage}`;

    if (pool) {
      try {
        await updateJobRun(pool, runId, 'Failed', message);
      } catch (updateError) {
        console.error(
          JSON.stringify({
            jobName: JOB_NAME,
            jobRunId: runId,
            error: 'Failed to record failure in JobRunHistory',
          })
        );
      }
    }

    console.error(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId: runId,
        startedAtUtc,
        completedAtUtc,
        status,
        counts,
        error: safeMessage,
        start_date: startDate,
        end_date: endDate,
      })
    );
    process.exit(1);
  }
}

main();
