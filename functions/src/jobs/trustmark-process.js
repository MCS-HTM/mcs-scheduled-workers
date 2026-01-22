/**
 * Trustmark local processing job
 *
 * Reads a Trustmark CSV export, de-duplicates by CertificateNumber,
 * calculates closest / 2nd / 3rd verifiers by travel time using Azure Maps,
 * and writes a processed CSV to ./out
 *
 * Auth:
 *  - Azure SQL via az login (DefaultAzureCredential)
 *  - Azure Maps via AZURE_MAPS_KEY
 */

const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const pLimit = require('p-limit');

const { getPool } = require('../shared/sql'); // existing helper

// ----------------------
// Configuration
// ----------------------

const MAPS_KEY = process.env.AZURE_MAPS_KEY;
const CONCURRENCY = 5; // Azure Maps call limit

// ----------------------
// Helpers
// ----------------------

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

function requireEnv(name) {
  if (!process.env[name]) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return process.env[name];
}

function getColumnValue(row, name) {
  if (!row) return undefined;
  if (Object.prototype.hasOwnProperty.call(row, name)) {
    return row[name];
  }
  const bomName = `\ufeff${name}`;
  if (Object.prototype.hasOwnProperty.call(row, bomName)) {
    return row[bomName];
  }
  return undefined;
}

async function markInstallationError(pool, certificateNumber) {
  if (!certificateNumber) return;
  const certEsc = certificateNumber.replace(/'/g, "''");
  const q = `
    UPDATE dbo.Trustmark_Installations
    SET
      Status = 'error',
      UpdatedDate = SYSUTCDATETIME()
    WHERE CertificateNumber = '${certEsc}';
  `;

  try {
    await pool.request().query(q);
  } catch (error) {
    console.error(JSON.stringify({
      msg: 'failed to mark installation error',
      CertificateNumber: certificateNumber,
      error: error?.message || String(error)
    }));
  }
}

// ----------------------
// Main
// ----------------------

async function main() {
  const args = parseArgs();

  if (!args.input) {
    throw new Error('Missing --input "<path-to-csv>"');
  }

  const inputPath = path.resolve(args.input);
  const outDir = path.resolve(args.out || './out');

  requireEnv('AZURE_MAPS_KEY');
  requireEnv('SQL_SERVER');
  requireEnv('SQL_DATABASE');

  console.log(JSON.stringify({
    msg: 'trustmark-process starting',
    inputPath,
    outDir
  }));

  if (!fs.existsSync(inputPath)) {
    throw new Error(`Input file not found: ${inputPath}`);
  }

  if (!fs.existsSync(outDir)) {
    fs.mkdirSync(outDir, { recursive: true });
  }

  // 1) Read CSV (robust parsing, supports quotes/commas)
  const csvText = fs.readFileSync(inputPath, 'utf8');

  const records = parse(csvText, {
    columns: true,            // first row -> keys
    skip_empty_lines: true,
    trim: true
  });

  console.log(JSON.stringify({ msg: 'csv loaded', rows: records.length }));

  // 2) Map Trustmark columns -> canonical field names used by our process
  // Input headers (confirmed):
  // - MCS Certificate Number, Project Type, Unique Measure Reference, Measure Category, Measure Type,
  // - Owner Name, Owner Contact Number, Owner Email,
  // - Address Line 1, Address Line 2, Address Line 3, City, Postcode

  function mapRow(r) {
    return {
      CertificateNumber: (getColumnValue(r, 'MCS Certificate Number') || '').toString().trim(),
      ProjectType: (getColumnValue(r, 'Project Type') || '').toString().trim(),
      UniqueMeasureReference: (getColumnValue(r, 'Unique Measure Reference') || '').toString().trim(),
      MeasureCategory: (getColumnValue(r, 'Measure Category') || '').toString().trim(),
      MeasureType: (getColumnValue(r, 'Measure Type') || '').toString().trim(),
      OwnerName: (getColumnValue(r, 'Owner Name') || '').toString().trim(),
      OwnerContactNumber: (getColumnValue(r, 'Owner Contact Number') || '').toString().trim(),
      OwnerEmailAddress: (getColumnValue(r, 'Owner Email') || '').toString().trim(),
      AddressLine1: (getColumnValue(r, 'Address Line 1') || '').toString().trim(),
      AddressLine2: (getColumnValue(r, 'Address Line 2') || '').toString().trim(),
      AddressLine3: (getColumnValue(r, 'Address Line 3') || '').toString().trim(),
      City: (getColumnValue(r, 'City') || '').toString().trim(),
      Postcode: (getColumnValue(r, 'Postcode') || '').toString().trim()
    };
  }

  const mapped = records.map(mapRow);

  // 3) Filter out rows missing CertificateNumber
  const withCert = mapped.filter(r => r.CertificateNumber);
  const missingCert = mapped.length - withCert.length;

  // 4) De-duplicate by CertificateNumber (keep first occurrence)
  const seen = new Set();
  const deduped = [];
  let dupCount = 0;

  for (const r of withCert) {
    if (seen.has(r.CertificateNumber)) {
      dupCount++;
      continue;
    }
    seen.add(r.CertificateNumber);
    deduped.push(r);
  }

  console.log(JSON.stringify({
    msg: 'dedupe complete',
    totalRows: mapped.length,
    missingCert,
    uniqueCertificates: deduped.length,
    duplicateRowsSkipped: dupCount
  }));

    // 5) Load verifier definitions from SQL
  const pool = await getPool();

  const verifiersRes = await pool.request().query(`
    SELECT VerifierName, HomePostcode, Coordinator
    FROM dbo.Trustmark_VerifierDefinitions
  `);

  const verifiers = (verifiersRes.recordset || []).map(v => ({
    VerifierName: (v.VerifierName || '').toString().trim(),
    HomePostcode: (v.HomePostcode || '').toString().trim(),
    Coordinator: (v.Coordinator || '').toString().trim()
  })).filter(v => v.VerifierName && v.HomePostcode && v.Coordinator);

  if (verifiers.length === 0) {
    throw new Error('No verifier definitions found in dbo.Trustmark_VerifierDefinitions.');
  }

  console.log(JSON.stringify({
    msg: 'verifiers loaded',
    verifiers: verifiers.length
  }));

    // 6) Incremental: skip certificates already present in dbo.Trustmark_Installations
  // We only process certificates not yet in the table.
  const certList = deduped.map(r => r.CertificateNumber);

  // Chunk to avoid overly large IN() clauses
  const chunkSize = 500;
  const existing = new Map();

  for (let i = 0; i < certList.length; i += chunkSize) {
    const chunk = certList.slice(i, i + chunkSize);

    // Build a safely-quoted list (CertificateNumber is alphanumeric; still escape quotes)
    const inList = chunk
      .map(c => `'${c.replace(/'/g, "''")}'`)
      .join(',');

    const q = `
      SELECT CertificateNumber, Status
      FROM dbo.Trustmark_Installations
      WHERE CertificateNumber IN (${inList})
    `;

    const res = await pool.request().query(q);
    for (const row of (res.recordset || [])) {
      if (!row.CertificateNumber) continue;
      const cert = row.CertificateNumber.toString().trim();
      const status = row.Status ? row.Status.toString().trim().toLowerCase() : '';
      existing.set(cert, status);
    }
  }

  const toProcess = deduped.filter(r => existing.get(r.CertificateNumber) !== 'completed');

  console.log(JSON.stringify({
    msg: 'incremental filter complete',
    uniqueCertificates: deduped.length,
    alreadyInDb: existing.size,
    toProcess: toProcess.length
  }));

  if (toProcess.length === 0) {
    console.log(JSON.stringify({ msg: 'no new certificates to process; exiting' }));
    return;
  }

    // 7) Insert new installations into SQL (Status='new')
  // We insert only for toProcess, and still guard with IF NOT EXISTS for safety.
  let inserted = 0;

  for (const r of toProcess) {
    const certEsc = r.CertificateNumber.replace(/'/g, "''");

    const q = `
      IF NOT EXISTS (SELECT 1 FROM dbo.Trustmark_Installations WHERE CertificateNumber = '${certEsc}')
      BEGIN
        INSERT INTO dbo.Trustmark_Installations
        (
          CertificateNumber,
          Status,
          ProjectType,
          UniqueMeasureReference,
          MeasureCategory,
          MeasureType,
          OwnerName,
          OwnerContactNumber,
          OwnerEmailAddress,
          AddressLine1,
          AddressLine2,
          AddressLine3,
          City,
          Postcode
        )
        VALUES
        (
          '${certEsc}',
          'new',
          '${r.ProjectType.replace(/'/g, "''")}',
          '${r.UniqueMeasureReference.replace(/'/g, "''")}',
          '${r.MeasureCategory.replace(/'/g, "''")}',
          '${r.MeasureType.replace(/'/g, "''")}',
          '${r.OwnerName.replace(/'/g, "''")}',
          '${r.OwnerContactNumber.replace(/'/g, "''")}',
          '${r.OwnerEmailAddress.replace(/'/g, "''")}',
          '${r.AddressLine1.replace(/'/g, "''")}',
          '${r.AddressLine2.replace(/'/g, "''")}',
          '${r.AddressLine3.replace(/'/g, "''")}',
          '${r.City.replace(/'/g, "''")}',
          '${r.Postcode.replace(/'/g, "''")}'
        );
      END
    `;

    await pool.request().query(q);
    inserted++;
  }

  console.log(JSON.stringify({
    msg: 'inserted new installations',
    inserted
  }));

    // 8) Azure Maps helpers (with caching)
  const mapsKey = MAPS_KEY;
  const limit = pLimit(CONCURRENCY);

  const geocodeCache = new Map(); // postcode -> { lat, lon } | null
  async function geocodePostcode(postcode) {
    const pc = (postcode || '').toString().trim().toUpperCase();
    if (!pc) return null;

    if (geocodeCache.has(pc)) return geocodeCache.get(pc);

    const url =
      `https://atlas.microsoft.com/search/address/json` +
      `?api-version=1.0` +
      `&subscription-key=${encodeURIComponent(mapsKey)}` +
      `&countrySet=GB` +
      `&query=${encodeURIComponent(pc)}`;

    const resp = await fetch(url);
    if (!resp.ok) {
      const txt = await resp.text().catch(() => '');
      throw new Error(`Azure Maps geocode failed (${resp.status}): ${txt}`);
    }

    const json = await resp.json();
    const pos = json?.results?.[0]?.position;
    const result = (pos && typeof pos.lat === 'number' && typeof pos.lon === 'number')
      ? { lat: pos.lat, lon: pos.lon }
      : null;

    geocodeCache.set(pc, result);
    return result;
  }

  async function routeCarMinutesMiles(from, to) {
    if (!from || !to) return null;

    const query =
      `${from.lat},${from.lon}:` +
      `${to.lat},${to.lon}`;

    const url =
      `https://atlas.microsoft.com/route/directions/json` +
      `?api-version=1.0` +
      `&subscription-key=${encodeURIComponent(mapsKey)}` +
      `&travelMode=car` +
      `&query=${encodeURIComponent(query)}`;

    const resp = await fetch(url);
    if (!resp.ok) {
      const txt = await resp.text().catch(() => '');
      throw new Error(`Azure Maps route failed (${resp.status}): ${txt}`);
    }

    const json = await resp.json();
    const summary = json?.routes?.[0]?.summary;
    if (!summary) return null;

    const seconds = summary.travelTimeInSeconds;
    const meters = summary.lengthInMeters;

    if (typeof seconds !== 'number' || typeof meters !== 'number') return null;

    const minutes = Math.round(seconds / 60);
    const miles = meters / 1609.344;

    return {
      minutes,
      miles
    };
  }

    // 9) Prepare output CSV (streamed)
  const now = new Date();
  const pad = (n) => n.toString().padStart(2, '0');
  const stamp =
    `${now.getUTCFullYear()}${pad(now.getUTCMonth() + 1)}${pad(now.getUTCDate())}_` +
    `${pad(now.getUTCHours())}${pad(now.getUTCMinutes())}`;

  const outCsvPath = path.join(outDir, `Processed_Trustmark_${stamp}.csv`);
  const outLogPath = path.join(outDir, `RunLog_${stamp}.json`);

  const columns = [
    'CertificateNumber',
    'ProjectType',
    'UniqueMeasureReference',
    'MeasureCategory',
    'MeasureType',
    'OwnerName',
    'OwnerContactNumber',
    'OwnerEmailAddress',
    'AddressLine1',
    'AddressLine2',
    'AddressLine3',
    'City',
    'Postcode',
    'Coordinator',
    'ClosestVerifier',
    'ClosestTimeMinutes',
    'ClosestDistanceMiles',
    'SecondClosestVerifier',
    'SecondClosestTimeMinutes',
    'SecondClosestDistanceMiles',
    'ThirdClosestVerifier',
    'ThirdClosestTimeMinutes',
    'ThirdClosestDistanceMiles'
  ];

  function csvEscape(v) {
    if (v === null || v === undefined) return '';
    const s = v.toString();
    // Quote if contains comma, quote, or newline
    if (/[",\r\n]/.test(s)) {
      return `"${s.replace(/"/g, '""')}"`;
    }
    return s;
  }

  const outStream = fs.createWriteStream(outCsvPath, { encoding: 'utf8' });
  outStream.write(`${columns.join(',')}\r\n`);

  function writeLine(line) {
    if (outStream.write(line)) {
      return Promise.resolve();
    }
    return new Promise((resolve) => outStream.once('drain', resolve));
  }

    // 10) Compute closest / 2nd / 3rd for each installation
  let processed = 0;
  let errored = 0;
  let updated = 0;

  for (const inst of toProcess) {
    try {
      const dest = await geocodePostcode(inst.Postcode);
      if (!dest) {
        console.error(JSON.stringify({
          msg: 'installation geocode missing',
          CertificateNumber: inst.CertificateNumber,
          Postcode: inst.Postcode
        }));
        await markInstallationError(pool, inst.CertificateNumber);
        errored++;
        continue;
      }

      // For each verifier, geocode (cached) then route (limited concurrency)
      const tasks = verifiers.map(v => limit(async () => {
        try {
          const from = await geocodePostcode(v.HomePostcode);
          if (!from) return null;

          const route = await routeCarMinutesMiles(from, dest);
          if (!route) return null;

          return {
            VerifierName: v.VerifierName,
            Coordinator: v.Coordinator,
            TimeMinutes: route.minutes,
            DistanceMiles: route.miles
          };
        } catch (error) {
          console.error(JSON.stringify({
            msg: 'verifier route failed',
            CertificateNumber: inst.CertificateNumber,
            VerifierName: v.VerifierName,
            HomePostcode: v.HomePostcode,
            error: error?.message || String(error)
          }));
          return null;
        }
      }));

      const results = (await Promise.all(tasks)).filter(Boolean);

      if (results.length === 0) {
        console.error(JSON.stringify({
          msg: 'no route results',
          CertificateNumber: inst.CertificateNumber,
          Postcode: inst.Postcode
        }));
        await markInstallationError(pool, inst.CertificateNumber);
        errored++;
        continue;
      }

      // Sort by time asc, then distance asc
      results.sort((a, b) => (a.TimeMinutes - b.TimeMinutes) || (a.DistanceMiles - b.DistanceMiles));

      const first = results[0] || {};
      const second = results[1] || {};
      const third = results[2] || {};

      const coordinator = first.Coordinator || '';

      // Build output row (canonical)
      const outputRow = {
        CertificateNumber: inst.CertificateNumber,
        ProjectType: inst.ProjectType,
        UniqueMeasureReference: inst.UniqueMeasureReference,
        MeasureCategory: inst.MeasureCategory,
        MeasureType: inst.MeasureType,
        OwnerName: inst.OwnerName,
        OwnerContactNumber: inst.OwnerContactNumber,
        OwnerEmailAddress: inst.OwnerEmailAddress,
        AddressLine1: inst.AddressLine1,
        AddressLine2: inst.AddressLine2,
        AddressLine3: inst.AddressLine3,
        City: inst.City,
        Postcode: inst.Postcode,
        Coordinator: coordinator,

        ClosestVerifier: first.VerifierName || '',
        ClosestTimeMinutes: Number.isFinite(first.TimeMinutes) ? first.TimeMinutes : '',
        ClosestDistanceMiles: Number.isFinite(first.DistanceMiles) ? Number(first.DistanceMiles.toFixed(2)) : '',

        SecondClosestVerifier: second.VerifierName || '',
        SecondClosestTimeMinutes: Number.isFinite(second.TimeMinutes) ? second.TimeMinutes : '',
        SecondClosestDistanceMiles: Number.isFinite(second.DistanceMiles) ? Number(second.DistanceMiles.toFixed(2)) : '',

        ThirdClosestVerifier: third.VerifierName || '',
        ThirdClosestTimeMinutes: Number.isFinite(third.TimeMinutes) ? third.TimeMinutes : '',
        ThirdClosestDistanceMiles: Number.isFinite(third.DistanceMiles) ? Number(third.DistanceMiles.toFixed(2)) : ''
      };

      const certEsc = outputRow.CertificateNumber.replace(/'/g, "''");

      const q = `
        UPDATE dbo.Trustmark_Installations
        SET
          Coordinator = '${(outputRow.Coordinator || '').replace(/'/g, "''")}',

          ClosestVerifier = '${(outputRow.ClosestVerifier || '').replace(/'/g, "''")}',
          ClosestTimeMinutes = ${Number.isFinite(outputRow.ClosestTimeMinutes) ? outputRow.ClosestTimeMinutes : 'NULL'},
          ClosestDistanceMiles = ${Number.isFinite(outputRow.ClosestDistanceMiles) ? outputRow.ClosestDistanceMiles : 'NULL'},

          SecondClosestVerifier = '${(outputRow.SecondClosestVerifier || '').replace(/'/g, "''")}',
          SecondClosestTimeMinutes = ${Number.isFinite(outputRow.SecondClosestTimeMinutes) ? outputRow.SecondClosestTimeMinutes : 'NULL'},
          SecondClosestDistanceMiles = ${Number.isFinite(outputRow.SecondClosestDistanceMiles) ? outputRow.SecondClosestDistanceMiles : 'NULL'},

          ThirdClosestVerifier = '${(outputRow.ThirdClosestVerifier || '').replace(/'/g, "''")}',
          ThirdClosestTimeMinutes = ${Number.isFinite(outputRow.ThirdClosestTimeMinutes) ? outputRow.ThirdClosestTimeMinutes : 'NULL'},
          ThirdClosestDistanceMiles = ${Number.isFinite(outputRow.ThirdClosestDistanceMiles) ? outputRow.ThirdClosestDistanceMiles : 'NULL'},

          Status = 'completed',
          UpdatedDate = SYSUTCDATETIME()
        WHERE CertificateNumber = '${certEsc}';
      `;

      await pool.request().query(q);
      updated++;

      const vals = columns.map(c => csvEscape(outputRow[c]));
      await writeLine(`${vals.join(',')}\r\n`);

      processed++;
      console.log(JSON.stringify({ msg: 'processed certificate', CertificateNumber: inst.CertificateNumber }));
    } catch (e) {
      console.error(JSON.stringify({
        msg: 'processing error',
        CertificateNumber: inst.CertificateNumber,
        error: e.message
      }));
      await markInstallationError(pool, inst.CertificateNumber);
      errored++;
    }
  }

  await new Promise((resolve) => outStream.end(resolve));

  console.log(JSON.stringify({
    msg: 'routing complete',
    processed,
    errored,
    updated
  }));

  const runSummary = {
    inputPath,
    outDir,
    outCsvPath,
    totalRows: records.length,
    uniqueCertificates: deduped.length,
    processed: processed,
    errored: errored,
    updated: updated,
    verifiers: verifiers.length,
    geocodeCacheSize: geocodeCache.size
  };

  fs.writeFileSync(outLogPath, JSON.stringify(runSummary, null, 2), 'utf8');

  console.log(JSON.stringify({
    msg: 'files written',
    outCsvPath,
    outLogPath
  }));

}

main()
  .then(() => {
    console.log(JSON.stringify({ msg: 'trustmark-process completed' }));
    process.exit(0);
  })
  .catch(err => {
    console.error(JSON.stringify({
      msg: 'trustmark-process failed',
      error: err.message,
      stack: err.stack
    }));
    process.exit(1);
  });
