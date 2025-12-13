const { randomUUID } = require('crypto');
const { execute, sql } = require('../shared/sql');

const JOB_NAME = 'ACAHeartbeat';
const MAX_MESSAGE_LENGTH = 4000;

function truncateMessage(message) {
  if (!message) {
    return '';
  }

  return message.length > MAX_MESSAGE_LENGTH
    ? message.slice(0, MAX_MESSAGE_LENGTH)
    : message;
}

async function insertJobRun(runId, status, message, correlationId) {
  return execute(
    'INSERT INTO dbo.JobRunHistory (RunId, JobName, Status, Message, CorrelationId, RunCompletedUtc) VALUES (@runId, @jobName, @status, @message, @correlationId, SYSUTCDATETIME())',
    [
      { name: 'runId', type: sql.UniqueIdentifier, value: runId },
      { name: 'jobName', type: sql.NVarChar(100), value: JOB_NAME },
      { name: 'status', type: sql.NVarChar(30), value: status },
      { name: 'message', type: sql.NVarChar(4000), value: truncateMessage(message) },
      { name: 'correlationId', type: sql.NVarChar(100), value: correlationId },
    ]
  );
}

async function main() {
  const runId = randomUUID();
  const startedAtUtc = new Date().toISOString();
  const correlationId = process.env.ACA_JOB_RUN_ID || process.env.HOSTNAME || runId;

  try {
    await insertJobRun(runId, 'Succeeded', 'Heartbeat OK', correlationId);

    const completedAtUtc = new Date().toISOString();
    console.log(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId: runId,
        startedAtUtc,
        completedAtUtc,
        status: 'Succeeded',
        counts: {},
      })
    );
    process.exit(0);
  } catch (error) {
    const safeMessage = truncateMessage(
      error && error.message ? error.message : 'Heartbeat SQL insert failed.'
    );
    let failureRecorded = true;

    try {
      await insertJobRun(runId, 'Failed', safeMessage, correlationId);
    } catch (secondaryError) {
      failureRecorded = false;
    }

    const completedAtUtc = new Date().toISOString();
    console.error(
      JSON.stringify({
        jobName: JOB_NAME,
        jobRunId: runId,
        startedAtUtc,
        completedAtUtc,
        status: 'Failed',
        counts: {},
        error: safeMessage,
        recordedFailure: failureRecorded,
      })
    );
    process.exit(1);
  }
}

main();
