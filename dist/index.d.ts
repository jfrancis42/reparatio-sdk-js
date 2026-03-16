/**
 * Reparatio JavaScript/TypeScript SDK
 * https://reparatio.app
 */
export declare class ReparatioError extends Error {
    readonly status: number;
    constructor(status: number, message: string);
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
    castColumns?: Record<string, {
        type: string;
        format?: string;
    }>;
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
    castColumns?: Record<string, {
        type: string;
        format?: string;
    }>;
}
export declare class Reparatio {
    private readonly baseUrl;
    private readonly headers;
    private readonly timeoutMs;
    /**
     * @param apiKey   Your `rp_...` API key. Falls back to `REPARATIO_API_KEY` env var.
     * @param baseUrl  Override the API base URL (default: https://reparatio.app).
     * @param timeoutMs HTTP timeout in milliseconds (default: 120 000).
     */
    constructor(apiKey?: string, baseUrl?: string, timeoutMs?: number);
    private _url;
    private _fetch;
    /** List all supported input and output formats. No API key required. */
    formats(): Promise<FormatsResult>;
    /** Return subscription and usage details for the current API key. */
    me(): Promise<MeResult>;
    /**
     * Inspect a file: return schema, encoding, row count, and a data preview.
     * No API key required.
     *
     * @param file     A `File` object (browser) or `Blob`.
     * @param filename Filename (required when passing a `Blob`).
     */
    inspect(file: File | Blob, filename?: string, options?: {
        noHeader?: boolean;
        fixEncoding?: boolean;
        previewRows?: number;
        delimiter?: string;
        sheet?: string;
    }): Promise<InspectResult>;
    /**
     * Convert a file to a different format.
     * Returns the converted file as an `ArrayBuffer`.
     *
     * @param file         A `File` object (browser) or `Blob`.
     * @param targetFormat Output format (e.g. `"parquet"`, `"csv"`, `"xlsx"`).
     * @param filename     Filename (required when passing a `Blob`).
     */
    convert(file: File | Blob, targetFormat: string, filename?: string, options?: ConvertOptions): Promise<{
        data: ArrayBuffer;
        filename: string;
    }>;
    /**
     * Stack rows from two or more files vertically (union / append).
     * Column mismatches are filled with null.
     */
    append(files: Array<{
        file: File | Blob;
        filename?: string;
    }>, targetFormat: string, options?: AppendOptions): Promise<{
        data: ArrayBuffer;
        filename: string;
    }>;
    /**
     * Merge or join two files.
     */
    merge(file1: File | Blob, file2: File | Blob, options: MergeOptions, filename1?: string, filename2?: string): Promise<{
        data: ArrayBuffer;
        filename: string;
    }>;
    /**
     * Run a SQL query against a file. The table is always named `data`.
     */
    query(file: File | Blob, sql: string, filename?: string, options?: QueryOptions): Promise<{
        data: ArrayBuffer;
        filename: string;
    }>;
    /**
     * Convert every file inside a ZIP archive to a common format.
     * Returns a ZIP of converted files as an `ArrayBuffer`.
     * Files that cannot be parsed are skipped; errors are in the
     * `X-Reparatio-Errors` response header.
     */
    batchConvert(zipFile: File | Blob, targetFormat: string, filename?: string, options?: BatchConvertOptions): Promise<{
        data: ArrayBuffer;
        filename: string;
        errors: Array<{
            file: string;
            error: string;
        }>;
    }>;
}
//# sourceMappingURL=index.d.ts.map