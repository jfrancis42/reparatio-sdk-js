/**
 * Reparatio JavaScript/TypeScript SDK
 * https://reparatio.app
 */
const DEFAULT_BASE_URL = "https://reparatio.app";
// ── Types ─────────────────────────────────────────────────────────────────────
export class ReparatioError extends Error {
    constructor(status, message) {
        super(message);
        this.status = status;
        this.name = "ReparatioError";
    }
}
// ── Helpers ───────────────────────────────────────────────────────────────────
async function _raiseForStatus(res) {
    if (res.ok)
        return;
    let detail;
    try {
        const body = await res.clone().json();
        detail = body.detail ?? res.statusText;
    }
    catch {
        detail = res.statusText;
    }
    throw new ReparatioError(res.status, detail);
}
function _filenameFromResponse(res, fallback) {
    const cd = res.headers.get("content-disposition") ?? "";
    const match = cd.match(/filename="([^"]+)"/);
    return match ? match[1] : fallback;
}
// ── Client ────────────────────────────────────────────────────────────────────
export class Reparatio {
    /**
     * @param apiKey   Your `rp_...` API key. Falls back to `REPARATIO_API_KEY` env var.
     * @param baseUrl  Override the API base URL (default: https://reparatio.app).
     * @param timeoutMs HTTP timeout in milliseconds (default: 120 000).
     */
    constructor(apiKey, baseUrl = DEFAULT_BASE_URL, timeoutMs = 120000) {
        const key = apiKey ??
            (typeof process !== "undefined" ? process.env.REPARATIO_API_KEY : undefined) ??
            "";
        this.baseUrl = baseUrl.replace(/\/$/, "");
        this.headers = key ? { "X-API-Key": key } : {};
        this.timeoutMs = timeoutMs;
    }
    _url(path) {
        return `${this.baseUrl}${path}`;
    }
    async _fetch(url, init) {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), this.timeoutMs);
        try {
            const res = await fetch(url, { ...init, signal: controller.signal });
            return res;
        }
        finally {
            clearTimeout(timer);
        }
    }
    // ── formats ──────────────────────────────────────────────────────────────
    /** List all supported input and output formats. No API key required. */
    async formats() {
        const res = await this._fetch(this._url("/api/v1/formats"), {
            headers: this.headers,
        });
        await _raiseForStatus(res);
        return res.json();
    }
    // ── me ───────────────────────────────────────────────────────────────────
    /** Return subscription and usage details for the current API key. */
    async me() {
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
    async inspect(file, filename, options = {}) {
        const fd = new FormData();
        const fname = filename ?? (file instanceof File ? file.name : "file");
        fd.append("file", file, fname);
        if (options.noHeader)
            fd.append("no_header", "true");
        if (options.fixEncoding === false)
            fd.append("fix_encoding", "false");
        if (options.previewRows)
            fd.append("preview_rows", String(options.previewRows));
        if (options.delimiter)
            fd.append("delimiter", options.delimiter);
        if (options.sheet)
            fd.append("sheet", options.sheet);
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
    async convert(file, targetFormat, filename, options = {}) {
        const fd = new FormData();
        const fname = filename ?? (file instanceof File ? file.name : "file");
        fd.append("file", file, fname);
        fd.append("target_format", targetFormat);
        if (options.noHeader)
            fd.append("no_header", "true");
        if (options.fixEncoding === false)
            fd.append("fix_encoding", "false");
        if (options.delimiter)
            fd.append("delimiter", options.delimiter);
        if (options.sheet)
            fd.append("sheet", options.sheet);
        if (options.deduplicate)
            fd.append("deduplicate", "true");
        if (options.selectColumns?.length)
            fd.append("select_columns", JSON.stringify(options.selectColumns));
        if (options.sampleN)
            fd.append("sample_n", String(options.sampleN));
        else if (options.sampleFrac)
            fd.append("sample_frac", String(options.sampleFrac));
        if (options.rowFilter)
            fd.append("row_filter", options.rowFilter);
        if (options.castColumns)
            fd.append("cast_columns", JSON.stringify(options.castColumns));
        if (options.nullValues?.length)
            fd.append("null_values", JSON.stringify(options.nullValues));
        if (options.geometryColumn)
            fd.append("geometry_column", options.geometryColumn);
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
    async append(files, targetFormat, options = {}) {
        if (files.length < 2)
            throw new Error("At least 2 files required for append");
        const fd = new FormData();
        for (const f of files) {
            const fname = f.filename ?? (f.file instanceof File ? f.file.name : "file");
            fd.append("files", f.file, fname);
        }
        fd.append("target_format", targetFormat);
        if (options.noHeader)
            fd.append("no_header", "true");
        if (options.fixEncoding === false)
            fd.append("fix_encoding", "false");
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
    async merge(file1, file2, options, filename1, filename2) {
        const fd = new FormData();
        fd.append("file1", file1, filename1 ?? (file1 instanceof File ? file1.name : "file1"));
        fd.append("file2", file2, filename2 ?? (file2 instanceof File ? file2.name : "file2"));
        fd.append("operation", options.op);
        fd.append("target_format", options.format);
        if (options.on) {
            const cols = Array.isArray(options.on) ? options.on.join(",") : options.on;
            fd.append("join_on", cols);
        }
        if (options.noHeader)
            fd.append("no_header", "true");
        if (options.fixEncoding === false)
            fd.append("fix_encoding", "false");
        if (options.geometryColumn)
            fd.append("geometry_column", options.geometryColumn);
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
    async query(file, sql, filename, options = {}) {
        const fd = new FormData();
        const fname = filename ?? (file instanceof File ? file.name : "file");
        fd.append("file", file, fname);
        fd.append("sql", sql);
        fd.append("target_format", options.format ?? "csv");
        if (options.noHeader)
            fd.append("no_header", "true");
        if (options.fixEncoding === false)
            fd.append("fix_encoding", "false");
        if (options.delimiter)
            fd.append("delimiter", options.delimiter);
        if (options.sheet)
            fd.append("sheet", options.sheet);
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
    async batchConvert(zipFile, targetFormat, filename, options = {}) {
        const fd = new FormData();
        const fname = filename ?? (zipFile instanceof File ? zipFile.name : "batch.zip");
        fd.append("zip_file", zipFile, fname);
        fd.append("target_format", targetFormat);
        if (options.noHeader)
            fd.append("no_header", "true");
        if (options.fixEncoding === false)
            fd.append("fix_encoding", "false");
        if (options.delimiter)
            fd.append("delimiter", options.delimiter);
        if (options.deduplicate)
            fd.append("deduplicate", "true");
        if (options.selectColumns?.length)
            fd.append("select_columns", JSON.stringify(options.selectColumns));
        if (options.sampleN)
            fd.append("sample_n", String(options.sampleN));
        else if (options.sampleFrac)
            fd.append("sample_frac", String(options.sampleFrac));
        if (options.castColumns)
            fd.append("cast_columns", JSON.stringify(options.castColumns));
        const res = await this._fetch(this._url("/api/v1/batch-convert"), {
            method: "POST",
            headers: this.headers,
            body: fd,
        });
        await _raiseForStatus(res);
        let errors = [];
        const errHeader = res.headers.get("X-Reparatio-Errors");
        if (errHeader) {
            try {
                errors = JSON.parse(decodeURIComponent(errHeader));
            }
            catch { /* ignore */ }
        }
        const outFilename = _filenameFromResponse(res, "converted.zip");
        return { data: await res.arrayBuffer(), filename: outFilename, errors };
    }
}
//# sourceMappingURL=index.js.map