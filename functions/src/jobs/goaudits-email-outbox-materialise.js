const { randomUUID } = require('crypto');
const { sql, getSqlPool } = require('../shared/sql');

const JOB_NAME = 'GoAuditsEmailOutboxMaterialise';
const DEFAULT_BATCH_SIZE = 2000;

function parseIntEnv(name, defaultValue) {
  const value = parseInt(process.env[name], 10);
  return Number.isFinite(value) && value > 0 ? value : defaultValue;
}

function truncate(str, max) {
  if (str == null) return '';
  const s = String(str);
  return s.length > max ? s.slice(0, max) : s;
}

function normalizeReportIds(reportIds) {
  if (!Array.isArray(reportIds)) {
    return [];
  }
  const set = new Set();
  for (const id of reportIds) {
    if (id === null || id === undefined) continue;
    const normalized = String(id).trim();
    if (normalized) {
      set.add(normalized);
    }
  }
  return Array.from(set);
}

function buildBatchInsertSql(reportIds) {
  if (!reportIds || reportIds.length === 0) {
    return { sql: '', paramNames: [] };
  }
  const paramNames = reportIds.map((_, index) => `batchId${index}`);
  const values = paramNames.map((name) => `(@${name})`).join(', ');
  const sql = `INSERT INTO #BatchIds (GoAuditsReportId) VALUES ${values};`;
  return { sql, paramNames };
}

function buildMaterialiseSql(options) {
  const { useBatchIds, batchInsertSql } = options;
  const batchJoin = useBatchIds
    ? 'JOIN #BatchIds ids ON ids.GoAuditsReportId = s.GoAuditsReportId'
    : '';
  const batchPrefix = useBatchIds
    ? `CREATE TABLE #BatchIds (GoAuditsReportId NVARCHAR(50) NOT NULL PRIMARY KEY);
${batchInsertSql}`
    : '';
  const skippedSql = useBatchIds
    ? `
SELECT @SkippedAlreadyExists = COUNT(1)
FROM dbo.GoAuditsScores s
JOIN #BatchIds ids
  ON ids.GoAuditsReportId = s.GoAuditsReportId
WHERE EXISTS
(
    SELECT 1
    FROM dbo.GoAuditsEmailOutbox o
    WHERE o.GoAuditsReportId = s.GoAuditsReportId
      AND o.RuleSetName      = s.RuleSetName
      AND o.RuleSetVersion   = s.RuleSetVersion
);`
    : '';

  return `
${batchPrefix}
DECLARE @SkippedAlreadyExists INT = 0;
DECLARE @MissingRecipient INT = 0;
${skippedSql}
;WITH Src AS
(
  SELECT TOP (@batchSize)
      s.GoAuditsReportId,
      s.RuleSetName,
      s.RuleSetVersion,
      r.CertificationNumber AS CertificateNumber,
      ins.Email             AS RecipientEmail,
      ins.Name              AS CompanyName,
      CASE
          WHEN s.RuleSetName = 'PV'       AND s.RuleSetVersion = 'v2' THEN 'GoAudits_PV_v2_Result'
          WHEN s.RuleSetName = 'HeatPump' AND s.RuleSetVersion = 'v3' THEN 'GoAudits_HeatPump_v3_Result'
          ELSE CONCAT('GoAudits_', s.RuleSetName, '_', s.RuleSetVersion, '_Result')
      END AS TemplateName
  FROM dbo.GoAuditsScores s
  JOIN dbo.GoAuditsReports r
      ON r.GoAuditsReportId = s.GoAuditsReportId
  LEFT JOIN dbo.Installation inst
      ON inst.CertificateNumber COLLATE Latin1_General_CI_AS
       = r.CertificationNumber COLLATE Latin1_General_CI_AS
  LEFT JOIN dbo.Installer ins
      ON ins.ID = inst.InstallerID
  ${batchJoin}
  WHERE NOT EXISTS
  (
      SELECT 1
      FROM dbo.GoAuditsEmailOutbox o
      WHERE o.GoAuditsReportId = s.GoAuditsReportId
        AND o.RuleSetName      = s.RuleSetName
        AND o.RuleSetVersion   = s.RuleSetVersion
  )
  ORDER BY s.GoAuditsReportId DESC
)
INSERT INTO dbo.GoAuditsEmailOutbox
(
    Status,
    AttemptCount,
    LastAttemptUtc,
    SucceededUtc,
    ExternalId,
    GoAuditsReportId,
    RuleSetName,
    RuleSetVersion,
    CertificateNumber,
    RecipientEmail,
    CompanyName,
    TemplateName,
    PdfReceivedUtc
)
SELECT
    'Pending',
    0,
    NULL,
    NULL,
    NULL,
    GoAuditsReportId,
    RuleSetName,
    RuleSetVersion,
    CertificateNumber,
    RecipientEmail,
    CompanyName,
    TemplateName,
    NULL
FROM Src;

SELECT
  @@ROWCOUNT AS InsertedRows,
  @SkippedAlreadyExists AS SkippedAlreadyExists,
  @MissingRecipient AS MissingRecipient;
  `.trim();
}

async function insertJobRun(pool, runId, status, message) {
  const request = pool.request();
  request.input('runId', sql.UniqueIdentifier, runId);
  request.input('jobName', sql.NVarChar(100), JOB_NAME);
  request.input('status', sql.NVarChar(30), status);
  request.input('message', sql.NVarChar(4000), truncate(message, 4000));
  request.input('correlationId', sql.NVarChar(100), runId);

  await request.query(
    'INSERT INTO dbo.JobRunHistory (RunId, JobName, Status, Message, CorrelationId) VALUES (@runId, @jobName, @status, @message, @correlationId)'
  );
}

async function updateJobRun(pool, runId, status, message) {
  const request = pool.request();
  request.input('runId', sql.UniqueIdentifier, runId);
  request.input('status', sql.NVarChar(30), status);
  request.input('message', sql.NVarChar(4000), truncate(message, 4000));

  await request.query(
    'UPDATE dbo.JobRunHistory SET Status=@status, Message=@message, RunCompletedUtc=SYSUTCDATETIME() WHERE RunId=@runId'
  );
}

async function materialiseEmailOutbox(pool, options = {}) {
  const batchSize = Number.isFinite(options.batchSize) && options.batchSize > 0
    ? options.batchSize
    : parseIntEnv('GOAUDITS_EMAIL_OUTBOX_BATCH_SIZE', DEFAULT_BATCH_SIZE);
  const scope = String(options.scope || 'all').trim().toLowerCase();
  const reportIds = normalizeReportIds(options.reportIds);
  const useBatchIds = scope === 'batch' && reportIds.length > 0;

  if (scope === 'batch' && reportIds.length === 0) {
    return { inserted: 0, skippedAlreadyExists: 0, missingRecipient: 0 };
  }

  const request = pool.request();
  request.input('batchSize', sql.Int, batchSize);

  let batchInsertSql = '';
  let paramNames = [];
  if (useBatchIds) {
    const batchInsert = buildBatchInsertSql(reportIds);
    batchInsertSql = batchInsert.sql;
    paramNames = batchInsert.paramNames;
    for (let i = 0; i < reportIds.length; i += 1) {
      request.input(paramNames[i], sql.NVarChar(50), reportIds[i]);
    }
  }

  const sqlText = buildMaterialiseSql({ useBatchIds, batchInsertSql });
  const result = useBatchIds ? await request.batch(sqlText) : await request.query(sqlText);
  const recordset = result?.recordset || result?.recordsets?.[0] || [];
  const row = recordset[0] || {};

  return {
    inserted: row.InsertedRows ?? 0,
    skippedAlreadyExists: row.SkippedAlreadyExists ?? 0,
    missingRecipient: row.MissingRecipient ?? 0,
  };
}

async function main() {
  const runId = randomUUID();
  const startedAtUtc = new Date().toISOString();
  const batchSize = parseIntEnv('GOAUDITS_EMAIL_OUTBOX_BATCH_SIZE', DEFAULT_BATCH_SIZE);

  let pool;
  let status = 'Succeeded';
  let message = '';
  let inserted = 0;
  let skippedAlreadyExists = 0;
  let missingRecipient = 0;

  try {
    pool = await getSqlPool();
    await insertJobRun(pool, runId, 'Running', `Materialising email outbox (batchSize=${batchSize})`);

    const result = await materialiseEmailOutbox(pool, { batchSize });
    inserted = result.inserted;
    skippedAlreadyExists = result.skippedAlreadyExists;
    missingRecipient = result.missingRecipient;

    message = `Inserted=${inserted}`;
    await updateJobRun(pool, runId, status, message);

    console.log(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId: runId,
        startedAtUtc,
        completedAtUtc: new Date().toISOString(),
        status,
        inserted,
        skippedAlreadyExists,
        missingRecipient,
      })
    );
    process.exit(0);
  } catch (err) {
    status = 'Failed';
    message = err && err.message ? err.message : 'Email outbox materialiser failed.';
    try {
      if (pool) await updateJobRun(pool, runId, status, message);
    } catch {
      // best effort
    }
    console.error(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId: runId,
        startedAtUtc,
        completedAtUtc: new Date().toISOString(),
        status,
        error: message,
      })
    );
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = { materialiseEmailOutbox };
