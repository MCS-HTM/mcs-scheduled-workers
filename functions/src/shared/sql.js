const sql = require('mssql');
const { DefaultAzureCredential } = require('@azure/identity');

const SQL_SCOPE = 'https://database.windows.net/.default';
const credential = new DefaultAzureCredential({
  managedIdentityClientId: process.env.AZURE_CLIENT_ID,
});

let poolPromise;

async function getAccessToken() {
  try {
    const accessToken = await credential.getToken(SQL_SCOPE);

    if (!accessToken || !accessToken.token) {
      throw new Error('Managed identity token response was empty.');
    }

    return accessToken.token;
  } catch (error) {
    const detail = error && error.message ? ` Details: ${error.message}` : '';
    throw new Error(
      `Failed to acquire managed identity access token for SQL.${detail}`
    );
  }
}

function getConfig(accessToken) {
  const server = process.env.SQL_SERVER;
  const database = process.env.SQL_DATABASE;

  if (!server) {
    throw new Error('SQL_SERVER environment variable is not set.');
  }

  if (!database) {
    throw new Error('SQL_DATABASE environment variable is not set.');
  }

  return {
    server,
    database,
    options: {
      encrypt: true,
      trustServerCertificate: false,
    },
    authentication: {
      type: 'azure-active-directory-access-token',
      options: {
        token: accessToken,
      },
    },
    pool: {
      max: 3,
      min: 0,
      idleTimeoutMillis: 30000,
    },
  };
}

async function getPool() {
  if (poolPromise) {
    return poolPromise;
  }

  // Validate environment variables before attempting to acquire a token
  getConfig('env-check');

  poolPromise = (async () => {
    const accessToken = await getAccessToken();
    const pool = await new sql.ConnectionPool(getConfig(accessToken)).connect();

    pool.on('error', (err) => {
      console.error('SQL pool error', err);
    });

    return pool;
  })();

  poolPromise = poolPromise.catch((error) => {
    poolPromise = undefined;
    throw error;
  });

  return poolPromise;
}

async function execute(query, params = []) {
  const pool = await getPool();
  const request = pool.request();

  for (const param of params) {
    request.input(param.name, param.type, param.value);
  }

  return request.query(query);
}

module.exports = {
  sql,
  execute,
  getPool,
  getSqlPool: getPool,
};
