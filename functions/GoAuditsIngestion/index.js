module.exports = async function (context, myTimer) {
  context.log({
    jobName: "GoAuditsIngestion",
    message: "Function loaded (no logic yet)",
    timestampUtc: new Date().toISOString()
  });
};
