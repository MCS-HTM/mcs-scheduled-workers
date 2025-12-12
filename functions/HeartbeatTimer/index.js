const { randomUUID } = require('crypto');
const { execute, sql } = require('../src/shared/sql');

const JOB_NAME = 'Heartbeat';
const MAX_MESSAGE_LENGTH = 4000;

function truncateMessage(message) {
  if (!message) {
    return '';
  }

  return message.length > MAX_MESSAGE_LENGTH
    ? message.slice(0, MAX_MESSAGE_LENGTH)
    : message;
}

module.exports = async function (context, myTimer) {
  const runId = randomUUID();
  const timestampUtc = new Date().toISOString();

  const heartbeat = {
    jobName: JOB_NAME,
    runId,
    invocationId: context.invocationId,
    scheduleStatus: myTimer && myTimer.scheduleStatus ? myTimer.scheduleStatus : null,
    timestampUtc,
  };

  if (myTimer && myTimer.isPastDue) {
    heartbeat.isPastDue = true;
  }

  context.log(heartbeat);

  const insertRow = (status, message) =>
    execute(
      'INSERT INTO dbo.JobRunHistory (RunId, JobName, Status, Message, CorrelationId, RunCompletedUtc) VALUES (@runId, @jobName, @status, @message, @correlationId, SYSUTCDATETIME())',
      [
        { name: 'runId', type: sql.UniqueIdentifier, value: runId },
        { name: 'jobName', type: sql.NVarChar(100), value: JOB_NAME },
        { name: 'status', type: sql.NVarChar(30), value: status },
        { name: 'message', type: sql.NVarChar(4000), value: truncateMessage(message) },
        { name: 'correlationId', type: sql.NVarChar(100), value: context.invocationId },
      ]
    );

  try {
    await insertRow('Succeeded', 'Heartbeat OK');
  } catch (error) {
    const errorMessage = truncateMessage(error && error.message ? error.message : 'Heartbeat SQL insert failed.');
    context.log.error('Heartbeat failed to write JobRunHistory', error);

    try {
      await insertRow('Failed', errorMessage);
    } catch (secondaryError) {
      context.log.error('Heartbeat failed to record failure status', secondaryError);
    }
  }
};
