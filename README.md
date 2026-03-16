# reparatio — JavaScript / TypeScript SDK

> **Alpha software.** The API surface may change without notice between versions.

JavaScript/TypeScript SDK for the [Reparatio](https://reparatio.app) data conversion API.

Works in **Node.js 18+** (uses the built-in `fetch` and `FormData`) and modern browsers.

**See also:** [reparatio-cli](https://github.com/jfrancis42/reparatio-cli) · [reparatio-sdk](https://github.com/jfrancis42/reparatio-sdk) (Python)

---

## Installation

```bash
npm install reparatio
# or
pnpm add reparatio
# or
yarn add reparatio
```

---

## Quick start

```typescript
import { Reparatio } from "reparatio";

const client = new Reparatio("rp_YOUR_KEY");

// Inspect a file (no key required)
const info = await client.inspect(file);
console.log(info.rows_total, info.columns);

// Convert a file
const { data, filename } = await client.convert(file, "parquet");
// data is an ArrayBuffer — write to disk or trigger a browser download
```

---

## Authentication

```typescript
// Pass the key directly
const client = new Reparatio("rp_YOUR_KEY");

// Or use the REPARATIO_API_KEY environment variable (Node.js)
const client = new Reparatio();
```

---

## API Reference

### `new Reparatio(apiKey?, baseUrl?, timeoutMs?)`

| Parameter | Default | Description |
|---|---|---|
| `apiKey` | `REPARATIO_API_KEY` env var | Your `rp_...` API key |
| `baseUrl` | `https://reparatio.app` | Override the API base URL |
| `timeoutMs` | `120000` | HTTP timeout in milliseconds |

---

### `client.formats()`

List all supported input and output formats. No API key required.

```typescript
const { input, output } = await client.formats();
```

---

### `client.me()`

Return subscription and usage details for the current API key.

```typescript
const me = await client.me();
console.log(me.plan, me.active, me.request_count);
```

---

### `client.inspect(file, filename?, options?)`

Inspect a file: encoding, row count, column types, null counts, unique counts, and a data preview. No API key required.

```typescript
const result = await client.inspect(file, "sales.csv", {
  previewRows: 20,
  fixEncoding: true,
});
console.log(result.detected_encoding);   // e.g. "utf-8"
console.log(result.detected_delimiter);  // e.g. ","
result.columns.forEach(c => console.log(c.name, c.dtype));
```

**Options:**

| Option | Default | Description |
|---|---|---|
| `noHeader` | `false` | Treat first row as data |
| `fixEncoding` | `true` | Repair encoding |
| `previewRows` | `8` | Number of preview rows (1–100) |
| `delimiter` | auto | Custom delimiter |
| `sheet` | first | Sheet or table name |

---

### `client.convert(file, targetFormat, filename?, options?)`

Convert a file to any supported output format.

```typescript
// Basic conversion
const { data, filename } = await client.convert(file, "parquet");

// With options
const result = await client.convert(file, "parquet", "sales.csv", {
  selectColumns: ["date", "region", "revenue"],
  castColumns: {
    price:  { type: "Float64" },
    date:   { type: "Date", format: "%d/%m/%Y" },
  },
  nullValues: ["N/A", "NULL", "-"],
  rowFilter: 'region = "EU" AND amount > 100',
  deduplicate: true,
});
```

**Options:**

| Option | Description |
|---|---|
| `noHeader` | Treat first row as data |
| `fixEncoding` | Repair encoding (default `true`) |
| `delimiter` | Custom delimiter for CSV-like input |
| `sheet` | Sheet or table name |
| `selectColumns` | Columns to include in output |
| `deduplicate` | Remove duplicate rows |
| `sampleN` | Random sample of N rows |
| `sampleFrac` | Random sample fraction (e.g. `0.1`) |
| `rowFilter` | SQL WHERE fragment (e.g. `'amount > 100'`) |
| `castColumns` | Column type overrides (see below) |
| `nullValues` | Strings to treat as null (e.g. `["N/A", "NULL"]`) |
| `geometryColumn` | WKT geometry column for GeoJSON output |

**`castColumns` types:** `String`, `Int8`–`Int64`, `UInt8`–`UInt64`, `Float32`, `Float64`, `Boolean`, `Date` (optionally with `format`), `Datetime` (optionally with `format`), `Time`.

---

### `client.append(files, targetFormat, options?)`

Stack rows from two or more files vertically. Column mismatches are filled with null.

```typescript
const { data } = await client.append(
  [
    { file: jan, filename: "jan.csv" },
    { file: feb, filename: "feb.csv" },
    { file: mar, filename: "mar.csv" },
  ],
  "parquet",
);
```

---

### `client.merge(file1, file2, options, filename1?, filename2?)`

Merge or join two files.

```typescript
// Left join on customer_id
const { data } = await client.merge(orders, customers, {
  op: "left",
  format: "parquet",
  on: "customer_id",
});

// Stack rows
const { data } = await client.merge(jan, feb, { op: "append", format: "csv" });
```

**`op` values:** `"append"` · `"left"` · `"right"` · `"outer"` · `"inner"`

---

### `client.query(file, sql, filename?, options?)`

Run a SQL query against a file. The table is always named `data`.

```typescript
const { data } = await client.query(
  file,
  "SELECT region, SUM(revenue) AS total FROM data GROUP BY region ORDER BY total DESC",
  "events.parquet",
  { format: "csv" },
);
```

---

### `client.batchConvert(zipFile, targetFormat, filename?, options?)`

Convert every file inside a ZIP archive to a common format. Returns a ZIP of converted files.

```typescript
const { data, errors } = await client.batchConvert(zipFile, "parquet");
if (errors.length) {
  console.warn("Skipped files:", errors);
}
```

---

## Supported formats

### Input
CSV, TSV, CSV.GZ, CSV.BZ2, CSV.ZST, CSV.ZIP, TSV.GZ, TSV.BZ2, TSV.ZST, TSV.ZIP, GZ, ZIP, BZ2, ZST, Excel (.xlsx/.xls), ODS, JSON, JSON.GZ, JSON.BZ2, JSON.ZST, JSON.ZIP, JSON Lines, GeoJSON, Parquet, Feather, Arrow, ORC, Avro, SQLite, YAML, BSON, SRT, VTT, HTML, Markdown, XML, SQL dump, PDF

### Output
CSV, TSV, CSV.GZ, CSV.BZ2, CSV.ZST, CSV.ZIP, TSV.GZ, TSV.BZ2, TSV.ZST, TSV.ZIP, Excel (.xlsx), ODS, JSON, JSON.GZ, JSON.BZ2, JSON.ZST, JSON.ZIP, JSON Lines, JSON Lines.GZ, JSON Lines.BZ2, JSON Lines.ZST, JSON Lines.ZIP, GeoJSON, GeoJSON.GZ, GeoJSON.BZ2, GeoJSON.ZST, GeoJSON.ZIP, Parquet, Feather, Arrow, ORC, Avro, SQLite, YAML, BSON, SRT, VTT

---

## Error handling

All methods throw `ReparatioError` on API errors:

```typescript
import { Reparatio, ReparatioError } from "reparatio";

try {
  const result = await client.convert(file, "parquet");
} catch (e) {
  if (e instanceof ReparatioError) {
    console.error(`API error ${e.status}: ${e.message}`);
  }
}
```

---

## Running the Examples

The repository includes 15 runnable examples covering every API method.

```bash
# clone and install
git clone https://github.com/jfrancis42/reparatio-sdk-js
cd reparatio-sdk-js
npm install

# run all examples
REPARATIO_API_KEY=rp_... \
node examples/examples.mjs

# run a single example
node -e "import('./examples/examples.mjs').then(m => m.exFormats())"
```

The examples require a valid `REPARATIO_API_KEY` environment variable (except `exFormats()`, which needs no key).

---

## Privacy

Files are sent to the Reparatio API at `reparatio.app` for processing.
Files are handled in memory and never stored — see the [Privacy Policy](https://reparatio.app).
