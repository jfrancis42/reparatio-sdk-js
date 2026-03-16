/**
 * Reparatio JavaScript SDK — runnable examples.
 *
 * Each example is a self-contained async function. Run all of them:
 *
 *     node examples/examples.mjs
 *
 * A valid REPARATIO_API_KEY environment variable is required to run these
 * examples (except formats(), which needs no key).
 */

import { inflateRawSync } from "zlib";
import { Reparatio, ReparatioError } from "../dist/index.js";

// ── Shared configuration ──────────────────────────────────────────────────────

const API_KEY  = process.env.REPARATIO_API_KEY  ?? "EXAMPLE-EXAMPLE-EXAMPLE";

/** Wrap a Node.js Buffer as a Blob the SDK accepts. */
function toBlob(buf) {
  return new Blob([buf], { type: "application/octet-stream" });
}

function client() {
  return new Reparatio(API_KEY);
}

function sep(title) {
  console.log(`\n${"─".repeat(60)}`);
  console.log(`  ${title}`);
  console.log("─".repeat(60));
}

// ── Minimal ZIP builder (no external deps) ────────────────────────────────────
//
// Produces a valid ZIP archive from an array of { name, data } entries where
// data is a Buffer or Uint8Array.  Files are stored uncompressed (method 0).

function buildZip(entries) {
  const localHeaders = [];
  const centralDir   = [];
  let offset = 0;

  for (const { name, data } of entries) {
    const nameBuf  = Buffer.from(name, "utf8");
    const dataBuf  = Buffer.isBuffer(data) ? data : Buffer.from(data);
    const crc      = crc32(dataBuf);
    const size     = dataBuf.length;

    // Local file header
    const lh = Buffer.alloc(30 + nameBuf.length);
    lh.writeUInt32LE(0x04034b50, 0);  // signature
    lh.writeUInt16LE(20, 4);           // version needed
    lh.writeUInt16LE(0, 6);            // flags
    lh.writeUInt16LE(0, 8);            // compression (stored)
    lh.writeUInt16LE(0, 10);           // mod time
    lh.writeUInt16LE(0, 12);           // mod date
    lh.writeUInt32LE(crc, 14);         // CRC-32
    lh.writeUInt32LE(size, 18);        // compressed size
    lh.writeUInt32LE(size, 22);        // uncompressed size
    lh.writeUInt16LE(nameBuf.length, 26); // filename length
    lh.writeUInt16LE(0, 28);           // extra field length
    nameBuf.copy(lh, 30);

    localHeaders.push(lh);
    localHeaders.push(dataBuf);

    // Central directory record
    const cd = Buffer.alloc(46 + nameBuf.length);
    cd.writeUInt32LE(0x02014b50, 0);  // signature
    cd.writeUInt16LE(20, 4);           // version made by
    cd.writeUInt16LE(20, 6);           // version needed
    cd.writeUInt16LE(0, 8);            // flags
    cd.writeUInt16LE(0, 10);           // compression
    cd.writeUInt16LE(0, 12);           // mod time
    cd.writeUInt16LE(0, 14);           // mod date
    cd.writeUInt32LE(crc, 16);         // CRC-32
    cd.writeUInt32LE(size, 20);        // compressed size
    cd.writeUInt32LE(size, 24);        // uncompressed size
    cd.writeUInt16LE(nameBuf.length, 28); // filename length
    cd.writeUInt16LE(0, 30);           // extra length
    cd.writeUInt16LE(0, 32);           // file comment length
    cd.writeUInt16LE(0, 34);           // disk number start
    cd.writeUInt16LE(0, 36);           // internal attributes
    cd.writeUInt32LE(0, 38);           // external attributes
    cd.writeUInt32LE(offset, 42);      // offset of local header
    nameBuf.copy(cd, 46);

    centralDir.push(cd);
    offset += lh.length + dataBuf.length;
  }

  const cdBuf   = Buffer.concat(centralDir);
  const cdSize  = cdBuf.length;
  const cdOffset = offset;

  // End of central directory record
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0); // signature
  eocd.writeUInt16LE(0, 4);           // disk number
  eocd.writeUInt16LE(0, 6);           // start disk
  eocd.writeUInt16LE(entries.length, 8);  // entries on disk
  eocd.writeUInt16LE(entries.length, 10); // total entries
  eocd.writeUInt32LE(cdSize, 12);         // central dir size
  eocd.writeUInt32LE(cdOffset, 16);       // central dir offset
  eocd.writeUInt16LE(0, 20);              // comment length

  return Buffer.concat([...localHeaders, cdBuf, eocd]);
}

/** CRC-32 for ZIP. */
function crc32(buf) {
  const table = crc32.table ?? (crc32.table = (() => {
    const t = new Uint32Array(256);
    for (let i = 0; i < 256; i++) {
      let c = i;
      for (let j = 0; j < 8; j++) c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
      t[i] = c;
    }
    return t;
  })());
  let crc = 0xffffffff;
  for (let i = 0; i < buf.length; i++) crc = table[(crc ^ buf[i]) & 0xff] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}

/**
 * Minimal ZIP reader — returns Map<name, Buffer>.
 * Handles both stored (method 0) and deflated (method 8) entries.
 */
function readZip(buf) {
  const files = new Map();
  let i = 0;
  while (i < buf.length - 4) {
    const sig = buf.readUInt32LE(i);
    if (sig !== 0x04034b50) { i++; continue; }
    const method    = buf.readUInt16LE(i + 8);
    const fnLen     = buf.readUInt16LE(i + 26);
    const extraLen  = buf.readUInt16LE(i + 28);
    const compSize  = buf.readUInt32LE(i + 18);
    const name      = buf.slice(i + 30, i + 30 + fnLen).toString("utf8");
    const dataStart = i + 30 + fnLen + extraLen;
    const compData  = buf.slice(dataStart, dataStart + compSize);
    const data = method === 8 ? inflateRawSync(compData) : compData;
    files.set(name, data);
    i = dataStart + compSize;
  }
  return files;
}

// ── Example 1: formats() ─────────────────────────────────────────────────────

async function exFormats() {
  sep("1. formats() — list supported input/output formats");

  // No API key required
  const c = new Reparatio();
  const f = await c.formats();

  console.log(`Input formats  (${f.input.length}): ${f.input.slice(0, 8).join(", ")} …`);
  console.log(`Output formats (${f.output.length}): ${f.output.slice(0, 8).join(", ")} …`);

  if (!f.input.includes("csv"))     throw new Error("csv not in input formats");
  if (!f.output.includes("parquet")) throw new Error("parquet not in output formats");
  console.log("PASS");
}

// ── Example 2: me() ──────────────────────────────────────────────────────────

async function exMe() {
  sep("2. me() — account / subscription details");

  const me = await client().me();

  console.log(`Email:      ${me.email}`);
  console.log(`Plan:       ${me.plan}`);
  console.log(`Active:     ${me.active}`);
  console.log(`API access: ${me.api_access}`);

  if (!me.active)     throw new Error("account not active");
  if (!me.api_access) throw new Error("api_access is false");
  console.log("PASS");
}

// ── Example 3: inspect() — CSV (inline Buffer) ────────────────────────────────

async function exInspectCsv() {
  sep("3. inspect() — CSV (inline Buffer)");

  const blob = toBlob(Buffer.from("country,county\nEngland,Kent\nEngland,Essex\nWales,Gwent\n"));
  const result = await client().inspect(blob, "county_uk.csv");

  console.log(`Filename:  ${result.filename}`);
  console.log(`Rows:      ${result.rows}`);
  console.log(`Encoding:  ${result.detected_encoding}`);
  console.log(`Columns (${result.columns.length}):`);
  for (const col of result.columns) {
    console.log(`  ${col.name.padEnd(25)} ${col.dtype.padEnd(15)} nulls=${col.null_count}`);
  }
  console.log(`Preview row 0: ${JSON.stringify(result.preview[0])}`);

  if (result.rows <= 0)    throw new Error("Expected rows > 0");
  if (result.columns.length <= 0) throw new Error("Expected columns > 0");
  console.log("PASS");
}

// ── Example 4: inspect() — raw Buffer (in-memory CSV) ────────────────────────

async function exInspectBuffer() {
  sep("4. inspect() — raw Buffer (in-memory CSV)");

  const csvBytes = Buffer.from("id,name,score\n1,Alice,95\n2,Bob,87\n3,Carol,92\n");
  const result = await client().inspect(toBlob(csvBytes), "scores.csv");

  console.log(`Rows:    ${result.rows}`);
  console.log(`Columns: ${result.columns.map(c => c.name).join(", ")}`);
  console.log(`Preview: ${JSON.stringify(result.preview)}`);

  if (result.rows !== 3) throw new Error(`Expected 3 rows, got ${result.rows}`);
  const names = result.columns.map(c => c.name);
  if (JSON.stringify(names) !== JSON.stringify(["id", "name", "score"])) {
    throw new Error(`Unexpected columns: ${names}`);
  }
  console.log("PASS");
}

// ── Example 5: inspect() — TSV (inline Buffer) ────────────────────────────────

async function exInspectExcel() {
  sep("5. inspect() — TSV (inline Buffer)");

  const blob = toBlob(Buffer.from("id\tname\tscore\n1\tAlice\t95\n2\tBob\t87\n3\tCarol\t92\n"));
  const result = await client().inspect(blob, "scores.tsv");

  console.log(`Filename: ${result.filename}`);
  console.log(`Rows:     ${result.rows}`);
  console.log(`Columns:  ${result.columns.map(c => c.name).join(", ")}`);

  if (result.rows <= 0) throw new Error("Expected rows > 0");
  console.log("PASS");
}

// ── Example 6: convert() — CSV → Parquet ─────────────────────────────────────

async function exConvertCsvToParquet() {
  sep("6. convert() — CSV → Parquet (verify PAR1 magic bytes)");

  const blob = toBlob(Buffer.from("country,county\nEngland,Kent\nEngland,Essex\nWales,Gwent\n"));
  const out = await client().convert(blob, "parquet", "county_uk.csv");

  console.log(`Output filename: ${out.filename}`);
  console.log(`Output size:     ${out.data.byteLength.toLocaleString()} bytes`);

  if (!out.filename.endsWith(".parquet")) throw new Error(`Bad filename: ${out.filename}`);
  if (out.data.byteLength === 0)          throw new Error("Empty output");

  const magic = Buffer.from(out.data.slice(0, 4)).toString("ascii");
  if (magic !== "PAR1") throw new Error(`Not a Parquet file — magic bytes: ${magic}`);
  console.log(`Magic bytes: ${magic} ✓`);
  console.log("PASS");
}

// ── Example 7: convert() — CSV → JSON Lines ──────────────────────────────────

async function exConvertExcelToJsonl() {
  sep("7. convert() — CSV → JSON Lines");

  const blob = toBlob(Buffer.from("id\tname\tscore\n1\tAlice\t95\n2\tBob\t87\n3\tCarol\t92\n"));
  const out = await client().convert(blob, "jsonl", "scores.tsv");

  const text  = Buffer.from(out.data).toString("utf8");
  const lines = text.split("\n").filter(l => l.trim());

  console.log(`Output filename: ${out.filename}`);
  console.log(`Lines:           ${lines.length}`);
  console.log(`First record:    ${lines[0]}`);

  if (!out.filename.endsWith(".jsonl")) throw new Error(`Bad filename: ${out.filename}`);
  if (lines.length === 0)              throw new Error("No lines in output");
  JSON.parse(lines[0]); // must be valid JSON
  console.log("PASS");
}

// ── Example 8: convert() — select + rename columns + gzip output ─────────────

async function exConvertSelectColumns() {
  sep("8. convert() — select columns and gzip output");

  const csvData = Buffer.from("country,county\nEngland,Kent\nEngland,Essex\nWales,Gwent\n");

  // Inspect first to discover available columns
  const info = await client().inspect(toBlob(csvData), "county_uk.csv");
  const colNames = info.columns.map(c => c.name);
  console.log(`Available columns: ${colNames.join(", ")}`);

  // Select only the first column and compress
  const selected = colNames.slice(0, 1);
  const out = await client().convert(
    toBlob(csvData),
    "csv.gz",
    "county_uk.csv",
    { selectColumns: selected },
  );

  console.log(`Output filename: ${out.filename}`);
  console.log(`Output size:     ${out.data.byteLength.toLocaleString()} bytes (compressed)`);

  if (!out.filename.endsWith(".csv.gz")) throw new Error(`Bad filename: ${out.filename}`);
  if (out.data.byteLength === 0)         throw new Error("Empty output");
  console.log("PASS");
}

// ── Example 9: convert() — deduplicate + sample ───────────────────────────────

async function exConvertDeduplicateSample() {
  sep("9. convert() — deduplicate rows + 50% sample");

  // Build a CSV with deliberate duplicates
  const rows = ["name,value", ...Array(10).fill(["Alice,1", "Alice,1", "Bob,2", "Bob,2"]).flat()];
  const csvBytes = Buffer.from(rows.join("\n"));

  // Confirm raw row count
  const infoBlob = toBlob(csvBytes);
  const info = await client().inspect(infoBlob, "dupes.csv");
  console.log(`Raw rows (with dupes): ${info.rows}`);

  const out = await client().convert(
    toBlob(csvBytes),
    "csv",
    "dupes.csv",
    { deduplicate: true, sampleFrac: 0.5 },
  );

  const resultRows = Buffer.from(out.data).toString("utf8").split("\n").filter(l => l.trim());
  console.log(`After dedup+sample:    ${resultRows.length - 1} data rows`);

  if (resultRows.length < 2) throw new Error("Expected at least header + 1 data row");
  console.log("PASS");
}

// ── Example 10: convert() — castColumns type overrides ───────────────────────

async function exConvertCastColumns() {
  sep("10. convert() — override column types with castColumns");

  const csvBytes = Buffer.from(
    "id,amount,event_date\n" +
    "1,19.99,2025-01-15\n" +
    "2,34.50,2025-02-20\n" +
    "3,7.00,2025-03-01\n",
  );

  const out = await client().convert(
    toBlob(csvBytes),
    "parquet",
    "sales.csv",
    {
      castColumns: {
        id:         { type: "Int32" },
        amount:     { type: "Float64" },
        event_date: { type: "Date", format: "%Y-%m-%d" },
      },
    },
  );

  console.log(`Output filename: ${out.filename}`);
  console.log(`Output size:     ${out.data.byteLength.toLocaleString()} bytes`);

  const magic = Buffer.from(out.data.slice(0, 4)).toString("ascii");
  if (magic !== "PAR1") throw new Error(`Not a Parquet file — magic bytes: ${magic}`);

  // Round-trip inspect to verify types
  const info = await client().inspect(toBlob(Buffer.from(out.data)), out.filename);
  const typeMap = Object.fromEntries(info.columns.map(c => [c.name, c.dtype]));
  console.log(`Column types: ${JSON.stringify(typeMap)}`);

  if (!["Int32", "Int64"].includes(typeMap["id"])) {
    throw new Error(`Unexpected id type: ${typeMap["id"]}`);
  }
  if (typeMap["amount"] !== "Float64") {
    throw new Error(`Unexpected amount type: ${typeMap["amount"]}`);
  }
  if (!["Date", "String"].includes(typeMap["event_date"])) {
    throw new Error(`Unexpected event_date type: ${typeMap["event_date"]}`);
  }
  console.log("PASS");
}

// ── Example 11: query() — SQL aggregation ────────────────────────────────────

async function exQuery() {
  sep("11. query() — SQL aggregation against a CSV file");

  const csvBytes = Buffer.from(
    "region,product,revenue\n" +
    "North,Widget,100\n" +
    "South,Widget,200\n" +
    "North,Gadget,150\n" +
    "South,Gadget,300\n" +
    "North,Widget,120\n",
  );

  const out = await client().query(
    toBlob(csvBytes),
    "SELECT region, SUM(revenue) AS total FROM data GROUP BY region ORDER BY total DESC",
    "sales.csv",
    { format: "json" },
  );

  const result = JSON.parse(Buffer.from(out.data).toString("utf8"));
  console.log(`Query result: ${JSON.stringify(result)}`);

  if (result.length !== 2) throw new Error(`Expected 2 rows, got ${result.length}`);
  if (result[0].region !== "South") throw new Error(`Expected South first, got ${result[0].region}`);
  if (result[0].total !== 500)      throw new Error(`Expected total=500, got ${result[0].total}`);
  console.log("PASS");
}

// ── Example 12: append() — stack three in-memory CSVs ────────────────────────

async function exAppend() {
  sep("12. append() — stack three CSVs vertically");

  const jan = Buffer.from("date,region,revenue\n2025-01-01,North,100\n2025-01-02,South,200\n");
  const feb = Buffer.from("date,region,revenue\n2025-02-01,North,150\n2025-02-02,South,180\n");
  const mar = Buffer.from("date,region,revenue\n2025-03-01,North,120\n2025-03-02,South,210\n");

  const out = await client().append(
    [
      { file: toBlob(jan), filename: "jan.csv" },
      { file: toBlob(feb), filename: "feb.csv" },
      { file: toBlob(mar), filename: "mar.csv" },
    ],
    "csv",
  );

  const lines = Buffer.from(out.data).toString("utf8").split("\n").filter(l => l.trim());
  console.log(`Output filename: ${out.filename}`);
  console.log(`Total rows (incl. header): ${lines.length}`);
  console.log(`Header: ${lines[0]}`);

  if (lines.length !== 7) throw new Error(`Expected 7 rows (1 header + 6 data), got ${lines.length}`);
  console.log("PASS");
}

// ── Example 13: merge() — inner join two CSVs on a key column ────────────────

async function exMergeJoin() {
  sep("13. merge() — inner join two CSVs on a key column");

  const ordersCsv = Buffer.from(
    "order_id,customer_id,amount\n" +
    "1001,C1,50.00\n" +
    "1002,C2,75.00\n" +
    "1003,C1,30.00\n" +
    "1004,C3,90.00\n",
  );
  const customersCsv = Buffer.from(
    "customer_id,name,city\n" +
    "C1,Alice,Boston\n" +
    "C2,Bob,Chicago\n",
  );

  const out = await client().merge(
    toBlob(ordersCsv),
    toBlob(customersCsv),
    { op: "inner", format: "csv", on: "customer_id" },
    "orders.csv",
    "customers.csv",
  );

  const lines = Buffer.from(out.data).toString("utf8").split("\n").filter(l => l.trim());
  console.log(`Output filename: ${out.filename}`);
  console.log(`Result rows (incl. header): ${lines.length}`);
  console.log(`Header: ${lines[0]}`);
  for (const row of lines.slice(1)) console.log(`  ${row}`);

  // C3 has no matching customer → inner join yields 3 matched rows + 1 header
  if (lines.length !== 4) throw new Error(`Expected 4 rows, got ${lines.length}`);
  console.log("PASS");
}

// ── Example 14: batchConvert() — ZIP of CSVs → ZIP of Parquets ───────────────

async function exBatchConvert() {
  sep("14. batchConvert() — ZIP of CSVs → ZIP of Parquet files");

  const zipBuf = buildZip([
    { name: "sales_jan.csv", data: Buffer.from("date,amount\n2025-01-01,100\n2025-01-02,200\n") },
    { name: "sales_feb.csv", data: Buffer.from("date,amount\n2025-02-01,150\n2025-02-02,180\n") },
  ]);

  const out = await client().batchConvert(toBlob(zipBuf), "parquet", "monthly.zip");

  // Unpack returned ZIP
  const outBuf = Buffer.from(out.data);
  const files  = readZip(outBuf);

  console.log(`Output files: ${[...files.keys()].join(", ")}`);
  for (const [name, data] of files) {
    const magic = data.slice(0, 4).toString("ascii");
    if (magic !== "PAR1") throw new Error(`${name} is not valid Parquet — magic: ${magic}`);
    console.log(`  ${name}: ${data.length.toLocaleString()} bytes — valid Parquet`);
  }

  if (out.errors.length > 0) {
    console.log(`Errors: ${JSON.stringify(out.errors)}`);
  }

  if (files.size !== 2) throw new Error(`Expected 2 files, got ${files.size}`);
  console.log("PASS");
}

// ── Example 15: error handling — bad key throws ReparatioError ───────────────

async function exErrorHandling() {
  sep("15. error handling — bad key throws ReparatioError");

  const badClient = new Reparatio("rp_invalid");
  try {
    await badClient.convert(
      toBlob(Buffer.from("a,b\n1,2\n")),
      "parquet",
      "test.csv",
    );
    throw new Error("Should have thrown ReparatioError");
  } catch (err) {
    if (!(err instanceof ReparatioError)) throw new Error(`Expected ReparatioError, got ${err}`);
    console.log(`Bad key caught: ${err.name} (HTTP ${err.status}): ${err.message}`);
    if (err.status !== 401 && err.status !== 403) {
      throw new Error(`Expected 401 or 403, got ${err.status}`);
    }
  }

  console.log("PASS");
}

// ── Runner ────────────────────────────────────────────────────────────────────

const EXAMPLES = [
  exFormats,
  exMe,
  exInspectCsv,
  exInspectBuffer,
  exInspectExcel,
  exConvertCsvToParquet,
  exConvertExcelToJsonl,
  exConvertSelectColumns,
  exConvertDeduplicateSample,
  exConvertCastColumns,
  exQuery,
  exAppend,
  exMergeJoin,
  exBatchConvert,
  exErrorHandling,
];

let passed = 0;
const failed = [];

for (const fn of EXAMPLES) {
  try {
    await fn();
    passed++;
  } catch (err) {
    failed.push({ name: fn.name, err });
    console.log(`  FAIL: ${err.message ?? err}`);
  }
}

console.log(`\n${"─".repeat(60)}`);
console.log(`  Results: ${passed}/${EXAMPLES.length} passed`);
console.log("─".repeat(60));

if (failed.length > 0) {
  for (const { name, err } of failed) {
    console.log(`  FAIL  ${name}: ${err.message ?? err}`);
  }
  process.exit(1);
} else {
  console.log("  All examples passed.");
}
