/**
 * Reparatio JavaScript/TypeScript SDK
 * https://reparatio.app
 */

const DEFAULT_BASE_URL = "https://reparatio.app";

// ── Types ─────────────────────────────────────────────────────────────────────

export class ReparatioError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "ReparatioError";
  }
}

export interface ColumnInfo {
  name: string;
  dtype: string;
  null_count: number;
  unique_count: number;
}

export interface InspectResult {
  filename: string;
  detected_encoding: string;
  detected_delimiter: string | null;
  rows: number;
  sheets: string[];
  columns: ColumnInfo[];
  preview: Record<string, unknown>[];
}

export interface MeResult {
  email: string;
  plan: string;
  expires_at: string | null;
  api_access: boolean;
  active: boolean;
  request_count: number;
  data_bytes_total: number;
}

export interface FormatsResult {
  input: string[];
  output: string[];
}

export interface ConvertOptions {
  /** Output format (e.g. "parquet", "csv", "xlsx"). Inferred from outputPath extension if omitted. */
  format?: string;
  noHeader?: boolean;
  fixEncoding?: boolean;
  delimiter?: string;
  sheet?: string;
  /** Column names to include in output. */
  selectColumns?: string[];
  deduplicate?: boolean;
  sampleN?: number;
  sampleFrac?: number;
  /** WHERE-style row filter expression (SQL fragment). */
  rowFilter?: string;
  /** Column type overrides: `{ price: { type: "Float64" }, date: { type: "Date", format: "%d/%m/%Y" } }` */
  castColumns?: Record<string, { type: string; format?: string }>;
  /** Strings to treat as null (e.g. ["N/A", "NULL", "-"]). */
  nullValues?: string[];
  /** WKT geometry column for GeoJSON output. */
  geometryColumn?: string;
}

export interface AppendOptions {
  noHeader?: boolean;
  fixEncoding?: boolean;
}

export interface MergeOptions {
  /** Join operation: "append" | "left" | "right" | "outer" | "inner" */
  op: "append" | "left" | "right" | "outer" | "inner";
  format: string;
  /** Column(s) to join on (not needed for "append"). */
  on?: string | string[];
  noHeader?: boolean;
  fixEncoding?: boolean;
  geometryColumn?: string;
}

export interface QueryOptions {
  format?: string;
  noHeader?: boolean;
  fixEncoding?: boolean;
  delimiter?: string;
  sheet?: string;
}

export interface BatchConvertOptions extends ConvertOptions {
  castColumns?: Record<string, { type: string; format?: string }>;
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async function _raiseForStatus(res: Response): Promise<void> {
  if (res.ok) return;
  let detail: string;
  try {
    const body = await res.clone().json();
    detail = body.detail ?? res.statusText;
  } catch {
    detail = res.statusText;
  }
  throw new ReparatioError(res.status, detail);
}

function _filenameFromResponse(res: Response, fallback: string): string {
  const cd = res.headers.get("content-disposition") ?? "";
  const match = cd.match(/filename="([^"]+)"/);
  return match ? match[1] : fallback;
}

// ── Client ────────────────────────────────────────────────────────────────────

export class Reparatio {
  private readonly baseUrl: string;
  private readonly headers: Record<string, string>;
  private readonly timeoutMs: number;

  /**
   * @param apiKey   Your `rp_...` API key. Falls back to `REPARATIO_API_KEY` env var.
   * @param baseUrl  Override the API base URL (default: https://reparatio.app).
   * @param timeoutMs HTTP timeout in milliseconds (default: 120 000).
   */
  constructor(
    apiKey?: string,
    baseUrl: string = DEFAULT_BASE_URL,
    timeoutMs: number = 120_000,
  ) {
    const key =
      apiKey ??
      (typeof process !== "undefined" ? process.env.REPARATIO_API_KEY : undefined) ??
      "";
    this.baseUrl  = baseUrl.replace(/\/$/, "");
    this.headers  = key ? { "X-API-Key": key } : {};
    this.timeoutMs = timeoutMs;
  }

  private _url(path: string): string {
    return `${this.baseUrl}${path}`;
  }

  private async _fetch(url: string, init: RequestInit): Promise<Response> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const res = await fetch(url, { ...init, signal: controller.signal });
      return res;
    } finally {
      clearTimeout(timer);
    }
  }

  // ── formats ──────────────────────────────────────────────────────────────

  /** List all supported input and output formats. No API key required. */
  async formats(): Promise<FormatsResult> {
    const res = await this._fetch(this._url("/api/v1/formats"), {
      headers: this.headers,
    });
    await _raiseForStatus(res);
    return res.json();
  }

  // ── me ───────────────────────────────────────────────────────────────────

  /** Return subscription and usage details for the current API key. */
  async me(): Promise<MeResult> {
    const res = await this._fetch(this._url("/api/v1/me"), {
      headers: this.headers,
    });
    await _raiseForStatus(res);
    return res.json();
  }

  // ── inspect ──────────────────────────────────────────────────────────────

  /**
   * Inspect a file: return schema, encoding, row count, and a data preview.
   * No API key required.
   *
   * @param file     A `File` object (browser) or `Blob`.
   * @param filename Filename (required when passing a `Blob`).
   */
  async inspect(
    file: File | Blob,
    filename?: string,
    options: {
      noHeader?: boolean;
      fixEncoding?: boolean;
      previewRows?: number;
      delimiter?: string;
      sheet?: string;
    } = {},
  ): Promise<InspectResult> {
    const fd = new FormData();
    const fname = filename ?? (file instanceof File ? file.name : "file");
    fd.append("file", file, fname);
    if (options.noHeader)             fd.append("no_header",    "true");
    if (options.fixEncoding === false) fd.append("fix_encoding", "false");
    if (options.previewRows)          fd.append("preview_rows", String(options.previewRows));
    if (options.delimiter)            fd.append("delimiter",     options.delimiter);
    if (options.sheet)                fd.append("sheet",         options.sheet);

    const res = await this._fetch(this._url("/api/v1/inspect"), {
      method: "POST",
      headers: this.headers,
      body: fd,
    });
    await _raiseForStatus(res);
    return res.json();
  }

  // ── convert ──────────────────────────────────────────────────────────────

  /**
   * Convert a file to a different format.
   * Returns the converted file as an `ArrayBuffer`.
   *
   * @param file         A `File` object (browser) or `Blob`.
   * @param targetFormat Output format (e.g. `"parquet"`, `"csv"`, `"xlsx"`).
   * @param filename     Filename (required when passing a `Blob`).
   */
  async convert(
    file: File | Blob,
    targetFormat: string,
    filename?: string,
    options: ConvertOptions = {},
  ): Promise<{ data: ArrayBuffer; filename: string }> {
    const fd = new FormData();
    const fname = filename ?? (file instanceof File ? file.name : "file");
    fd.append("file",          file, fname);
    fd.append("target_format", targetFormat);
    if (options.noHeader)             fd.append("no_header",    "true");
    if (options.fixEncoding === false) fd.append("fix_encoding", "false");
    if (options.delimiter)            fd.append("delimiter",    options.delimiter);
    if (options.sheet)                fd.append("sheet",        options.sheet);
    if (options.deduplicate)          fd.append("deduplicate",  "true");
    if (options.selectColumns?.length)
      fd.append("select_columns", JSON.stringify(options.selectColumns));
    if (options.sampleN)              fd.append("sample_n",    String(options.sampleN));
    else if (options.sampleFrac)      fd.append("sample_frac", String(options.sampleFrac));
    if (options.rowFilter)            fd.append("row_filter",   options.rowFilter);
    if (options.castColumns)          fd.append("cast_columns", JSON.stringify(options.castColumns));
    if (options.nullValues?.length)   fd.append("null_values",  JSON.stringify(options.nullValues));
    if (options.geometryColumn)       fd.append("geometry_column", options.geometryColumn);

    const res = await this._fetch(this._url("/api/v1/convert"), {
      method: "POST",
      headers: this.headers,
      body: fd,
    });
    await _raiseForStatus(res);
    const outFilename = _filenameFromResponse(res, `output.${targetFormat}`);
    return { data: await res.arrayBuffer(), filename: outFilename };
  }

  // ── append ───────────────────────────────────────────────────────────────

  /**
   * Stack rows from two or more files vertically (union / append).
   * Column mismatches are filled with null.
   */
  async append(
    files: Array<{ file: File | Blob; filename?: string }>,
    targetFormat: string,
    options: AppendOptions = {},
  ): Promise<{ data: ArrayBuffer; filename: string }> {
    if (files.length < 2) throw new Error("At least 2 files required for append");
    const fd = new FormData();
    for (const f of files) {
      const fname = f.filename ?? (f.file instanceof File ? f.file.name : "file");
      fd.append("files", f.file, fname);
    }
    fd.append("target_format", targetFormat);
    if (options.noHeader)             fd.append("no_header",    "true");
    if (options.fixEncoding === false) fd.append("fix_encoding", "false");

    const res = await this._fetch(this._url("/api/v1/append"), {
      method: "POST",
      headers: this.headers,
      body: fd,
    });
    await _raiseForStatus(res);
    const outFilename = _filenameFromResponse(res, `appended.${targetFormat}`);
    return { data: await res.arrayBuffer(), filename: outFilename };
  }

  // ── merge ────────────────────────────────────────────────────────────────

  /**
   * Merge or join two files.
   */
  async merge(
    file1: File | Blob,
    file2: File | Blob,
    options: MergeOptions,
    filename1?: string,
    filename2?: string,
  ): Promise<{ data: ArrayBuffer; filename: string }> {
    const fd = new FormData();
    fd.append("file1", file1, filename1 ?? (file1 instanceof File ? file1.name : "file1"));
    fd.append("file2", file2, filename2 ?? (file2 instanceof File ? file2.name : "file2"));
    fd.append("operation",     options.op);
    fd.append("target_format", options.format);
    if (options.on) {
      const cols = Array.isArray(options.on) ? options.on.join(",") : options.on;
      fd.append("join_on", cols);
    }
    if (options.noHeader)             fd.append("no_header",    "true");
    if (options.fixEncoding === false) fd.append("fix_encoding", "false");
    if (options.geometryColumn)       fd.append("geometry_column", options.geometryColumn);

    const res = await this._fetch(this._url("/api/v1/merge"), {
      method: "POST",
      headers: this.headers,
      body: fd,
    });
    await _raiseForStatus(res);
    const outFilename = _filenameFromResponse(res, `merged.${options.format}`);
    return { data: await res.arrayBuffer(), filename: outFilename };
  }

  // ── query ────────────────────────────────────────────────────────────────

  /**
   * Run a SQL query against a file. The table is always named `data`.
   */
  async query(
    file: File | Blob,
    sql: string,
    filename?: string,
    options: QueryOptions = {},
  ): Promise<{ data: ArrayBuffer; filename: string }> {
    const fd = new FormData();
    const fname = filename ?? (file instanceof File ? file.name : "file");
    fd.append("file",          file, fname);
    fd.append("sql",           sql);
    fd.append("target_format", options.format ?? "csv");
    if (options.noHeader)             fd.append("no_header",    "true");
    if (options.fixEncoding === false) fd.append("fix_encoding", "false");
    if (options.delimiter)            fd.append("delimiter",    options.delimiter);
    if (options.sheet)                fd.append("sheet",        options.sheet);

    const res = await this._fetch(this._url("/api/v1/query"), {
      method: "POST",
      headers: this.headers,
      body: fd,
    });
    await _raiseForStatus(res);
    const outFilename = _filenameFromResponse(res, `query.${options.format ?? "csv"}`);
    return { data: await res.arrayBuffer(), filename: outFilename };
  }

  // ── batch-convert ────────────────────────────────────────────────────────

  /**
   * Convert every file inside a ZIP archive to a common format.
   * Returns a ZIP of converted files as an `ArrayBuffer`.
   * Files that cannot be parsed are skipped; errors are in the
   * `X-Reparatio-Errors` response header.
   */
  async batchConvert(
    zipFile: File | Blob,
    targetFormat: string,
    filename?: string,
    options: BatchConvertOptions = {},
  ): Promise<{ data: ArrayBuffer; filename: string; errors: Array<{ file: string; error: string }> }> {
    const fd = new FormData();
    const fname = filename ?? (zipFile instanceof File ? zipFile.name : "batch.zip");
    fd.append("zip_file",      zipFile, fname);
    fd.append("target_format", targetFormat);
    if (options.noHeader)             fd.append("no_header",    "true");
    if (options.fixEncoding === false) fd.append("fix_encoding", "false");
    if (options.delimiter)            fd.append("delimiter",    options.delimiter);
    if (options.deduplicate)          fd.append("deduplicate",  "true");
    if (options.selectColumns?.length)
      fd.append("select_columns", JSON.stringify(options.selectColumns));
    if (options.sampleN)              fd.append("sample_n",    String(options.sampleN));
    else if (options.sampleFrac)      fd.append("sample_frac", String(options.sampleFrac));
    if (options.castColumns)          fd.append("cast_columns", JSON.stringify(options.castColumns));

    const res = await this._fetch(this._url("/api/v1/batch-convert"), {
      method: "POST",
      headers: this.headers,
      body: fd,
    });
    await _raiseForStatus(res);

    let errors: Array<{ file: string; error: string }> = [];
    const errHeader = res.headers.get("X-Reparatio-Errors");
    if (errHeader) {
      try { errors = JSON.parse(decodeURIComponent(errHeader)); } catch { /* ignore */ }
    }

    const outFilename = _filenameFromResponse(res, "converted.zip");
    return { data: await res.arrayBuffer(), filename: outFilename, errors };
  }
}
