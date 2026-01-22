const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const { getPool, sql } = require('../shared/sql');

function parseArgs() {
  const args = process.argv.slice(2);
  const out = {};

  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith('--')) {
      const key = args[i].replace('--', '');
      out[key] = args[i + 1];
      i++;
    }
  }

  return out;
}

function toNumber(value) {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toInt(value) {
  const n = toNumber(value);
  return Number.isFinite(n) ? Math.round(n) : null;
}

async function main() {
  const args = parseArgs();

  if (!args.input) {
    throw new Error('Missing --input "<path-to-csv>"');
  }

  const inputPath = path.resolve(args.input);
  if (!fs.existsSync(inputPath)) {
    throw new Error(`Input file not found: ${inputPath}`);
  }

  const csvText = fs.readFileSync(inputPath, 'utf8');
  const rows = parse(csvText, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });

  const pool = await getPool();

  let updated = 0;
  let skipped = 0;
  let failed = 0;

  for (const row of rows) {
    const cert = (row.CertificateNumber || row['\ufeffCertificateNumber'] || '')
      .toString()
      .trim();

    if (!cert) {
      skipped++;
      continue;
    }

    const request = pool.request();
    request.input('CertificateNumber', sql.NVarChar(100), cert);
    request.input('Coordinator', sql.NVarChar(200), row.Coordinator || null);

    request.input('ClosestVerifier', sql.NVarChar(200), row.ClosestVerifier || null);
    request.input('ClosestTimeMinutes', sql.Int, toInt(row.ClosestTimeMinutes));
    request.input('ClosestDistanceMiles', sql.Float, toNumber(row.ClosestDistanceMiles));

    request.input('SecondClosestVerifier', sql.NVarChar(200), row.SecondClosestVerifier || null);
    request.input('SecondClosestTimeMinutes', sql.Int, toInt(row.SecondClosestTimeMinutes));
    request.input('SecondClosestDistanceMiles', sql.Float, toNumber(row.SecondClosestDistanceMiles));

    request.input('ThirdClosestVerifier', sql.NVarChar(200), row.ThirdClosestVerifier || null);
    request.input('ThirdClosestTimeMinutes', sql.Int, toInt(row.ThirdClosestTimeMinutes));
    request.input('ThirdClosestDistanceMiles', sql.Float, toNumber(row.ThirdClosestDistanceMiles));

    try {
      const result = await request.query(`
        UPDATE dbo.Trustmark_Installations
        SET
          Coordinator = @Coordinator,
          ClosestVerifier = @ClosestVerifier,
          ClosestTimeMinutes = @ClosestTimeMinutes,
          ClosestDistanceMiles = @ClosestDistanceMiles,
          SecondClosestVerifier = @SecondClosestVerifier,
          SecondClosestTimeMinutes = @SecondClosestTimeMinutes,
          SecondClosestDistanceMiles = @SecondClosestDistanceMiles,
          ThirdClosestVerifier = @ThirdClosestVerifier,
          ThirdClosestTimeMinutes = @ThirdClosestTimeMinutes,
          ThirdClosestDistanceMiles = @ThirdClosestDistanceMiles,
          Status = 'completed',
          UpdatedDate = SYSUTCDATETIME()
        WHERE CertificateNumber = @CertificateNumber;
      `);

      const affected = Array.isArray(result?.rowsAffected)
        ? result.rowsAffected[0] || 0
        : 0;

      if (affected > 0) {
        updated += affected;
      } else {
        skipped++;
      }
    } catch (error) {
      failed++;
      console.error(JSON.stringify({
        msg: 'failed to apply csv row',
        CertificateNumber: cert,
        error: error?.message || String(error),
      }));
    }
  }

  console.log(JSON.stringify({
    msg: 'csv apply complete',
    total: rows.length,
    updated,
    skipped,
    failed,
  }));
}

main().catch((err) => {
  console.error(JSON.stringify({
    msg: 'trustmark-apply-csv failed',
    error: err?.message || String(err),
  }));
  process.exit(1);
});
