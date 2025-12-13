const crypto = require("crypto");
const sql = require("../src/shared/sql");

module.exports = async function (context, myTimer) {
  const runId = crypto.randomUUID();

  context.log(JSON.stringify({
    jobName: "GoAuditsIngestion",
    event: "start",
    runId,
    isPastDue: !!myTimer?.isPastDue,
    timestampUtc: new Date().toISOString()
  }));

  try {
    await sql.execute(`
      INSERT INTO dbo.JobRunHistory
        (RunId, JobName, RunStartedUtc, RunCompletedUtc, Status, Message)
      VALUES
        (@runId, 'GoAuditsIngestion', SYSUTCDATETIME(), SYSUTCDATETIME(), 'Succeeded', 'M1 scaffold ran (no API yet)')
    `, { runId });

    context.log(JSON.stringify({
      jobName: "GoAuditsIngestion",
      event: "end",
      runId,
      status: "Succeeded"
    }));
  } catch (err) {
    context.log.error(JSON.stringify({
      jobName: "GoAuditsIngestion",
      event: "error",
      runId,
      message: err?.message || String(err)
    }));

    // Record failure too
    try {
      await sql.execute(`
        INSERT INTO dbo.JobRunHistory
          (RunId, JobName, RunStartedUtc, RunCompletedUtc, Status, Message)
        VALUES
          (@runId, 'GoAuditsIngestion', SYSUTCDATETIME(), SYSUTCDATETIME(), 'Failed', @msg)
      `, { runId, msg: (err?.message || String(err)).slice(0, 4000) });
    } catch (_) {}

    throw err;
  }
};
