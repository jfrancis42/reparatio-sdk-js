/**
 * Tests for the Reparatio JS/TS SDK.
 *
 * Mocks the global `fetch` so no real network calls are made.
 * Run with:  npm test  (requires `npm install` first)
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  Reparatio,
  ReparatioError,
  type FormatsResult,
  type InspectResult,
  type MeResult,
} from "../src/index.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Build a real Response object for use as a fetch mock return value. */
function makeResponse(
  status: number,
  body: unknown = null,
  headers: Record<string, string> = {},
): Response {
  const init: ResponseInit = { status, headers };
  if (body === null) return new Response(null, init);
  const bodyStr = body instanceof Uint8Array ? body : JSON.stringify(body);
  return new Response(bodyStr as BodyInit, {
    ...init,
    headers: {
      "content-type":
        body instanceof Uint8Array ? "application/octet-stream" : "application/json",
      ...headers,
    },
  });
}

/** Build a binary Response (e.g. for converted file downloads). */
function makeBinaryResponse(
  status: number,
  bytes: Uint8Array,
  headers: Record<string, string> = {},
): Response {
  return new Response(bytes, { status, headers });
}

const CSV_BYTES = new TextEncoder().encode("id,name\n1,Alice\n2,Bob\n");
const PARQUET_MAGIC = new Uint8Array([0x50, 0x41, 0x52, 0x31, 0x00]); // PAR1\0

const FORMATS_DATA: FormatsResult = {
  input: ["csv", "parquet", "xlsx", "json"],
  output: ["csv", "parquet", "xlsx"],
};

const INSPECT_DATA: InspectResult = {
  filename: "data.csv",
  detected_encoding: "utf-8",
  detected_delimiter: ",",
  rows_total: 500,
  sheets: [],
  columns: [
    { name: "id", dtype: "Int64", null_count: 0, unique_count: 500 },
    { name: "name", dtype: "Utf8", null_count: 1, unique_count: 499 },
  ],
  preview: [{ id: "1", name: "Alice" }],
};

const ME_DATA: MeResult = {
  email: "user@example.com",
  plan: "pro",
  expires_at: "2026-12-31T00:00:00Z",
  api_access: true,
  active: true,
  request_count: 42,
  data_bytes_total: 1_000_000,
};

// ---------------------------------------------------------------------------
// Setup: stub global fetch before every test, restore after
// ---------------------------------------------------------------------------

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchMock = vi.fn();
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  delete process.env.REPARATIO_API_KEY;
});

// ---------------------------------------------------------------------------
// ReparatioError
// ---------------------------------------------------------------------------

describe("ReparatioError", () => {
  it("extends Error", () => {
    const err = new ReparatioError(401, "Unauthorized");
    expect(err).toBeInstanceOf(Error);
    expect(err).toBeInstanceOf(ReparatioError);
  });

  it("exposes status and message", () => {
    const err = new ReparatioError(422, "Parse error");
    expect(err.status).toBe(422);
    expect(err.message).toBe("Parse error");
  });

  it("has name ReparatioError", () => {
    expect(new ReparatioError(500, "oops").name).toBe("ReparatioError");
  });

  it("can be caught as Error", () => {
    expect(() => { throw new ReparatioError(403, "Forbidden"); }).toThrow(Error);
  });
});

// ---------------------------------------------------------------------------
// Constructor / auth
// ---------------------------------------------------------------------------

describe("Reparatio constructor", () => {
  it("sets X-API-Key header from explicit key", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, FORMATS_DATA));
    const client = new Reparatio("rp_explicit_key");
    await client.formats();
    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect((init.headers as Record<string, string>)["X-API-Key"]).toBe("rp_explicit_key");
  });

  it("reads key from REPARATIO_API_KEY env var", async () => {
    process.env.REPARATIO_API_KEY = "rp_from_env";
    fetchMock.mockResolvedValue(makeResponse(200, FORMATS_DATA));
    const client = new Reparatio();
    await client.formats();
    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect((init.headers as Record<string, string>)["X-API-Key"]).toBe("rp_from_env");
  });

  it("explicit key takes precedence over env var", async () => {
    process.env.REPARATIO_API_KEY = "rp_env";
    fetchMock.mockResolvedValue(makeResponse(200, FORMATS_DATA));
    const client = new Reparatio("rp_explicit");
    await client.formats();
    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect((init.headers as Record<string, string>)["X-API-Key"]).toBe("rp_explicit");
  });

  it("omits X-API-Key when no key provided and no env var", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, FORMATS_DATA));
    const client = new Reparatio();
    await client.formats();
    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect((init.headers as Record<string, string>)["X-API-Key"]).toBeUndefined();
  });

  it("strips trailing slash from baseUrl", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, FORMATS_DATA));
    const client = new Reparatio("rp_key", "https://reparatio.app/");
    await client.formats();
    const [url] = fetchMock.mock.calls[0] as [string];
    expect(url).toBe("https://reparatio.app/api/v1/formats");
    expect(url).not.toContain("//api");
  });

  it("uses custom baseUrl", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, FORMATS_DATA));
    const client = new Reparatio("rp_key", "http://localhost:8000");
    await client.formats();
    const [url] = fetchMock.mock.calls[0] as [string];
    expect(url).toContain("http://localhost:8000");
  });
});

// ---------------------------------------------------------------------------
// formats()
// ---------------------------------------------------------------------------

describe("formats()", () => {
  it("returns FormatsResult on success", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, FORMATS_DATA));
    const client = new Reparatio("rp_key");
    const result = await client.formats();
    expect(result.input).toContain("csv");
    expect(result.output).toContain("parquet");
  });

  it("calls GET /api/v1/formats", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, FORMATS_DATA));
    await new Reparatio("rp_key").formats();
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain("/api/v1/formats");
    expect(init.method).toBeUndefined(); // GET (default)
  });

  it("throws ReparatioError on 401", async () => {
    fetchMock.mockResolvedValue(makeResponse(401, { detail: "Invalid API key" }));
    await expect(new Reparatio("rp_bad").formats()).rejects.toSatisfy(
      (e: unknown) => e instanceof ReparatioError && e.status === 401,
    );
  });

  it("throws ReparatioError on 500", async () => {
    fetchMock.mockResolvedValue(makeResponse(500, { detail: "Internal error" }));
    await expect(new Reparatio("rp_key").formats()).rejects.toBeInstanceOf(ReparatioError);
  });

  it("handles non-JSON error body gracefully", async () => {
    fetchMock.mockResolvedValue(new Response("Bad Gateway", { status: 502 }));
    await expect(new Reparatio("rp_key").formats()).rejects.toSatisfy(
      (e: unknown) => e instanceof ReparatioError && e.status === 502,
    );
  });
});

// ---------------------------------------------------------------------------
// me()
// ---------------------------------------------------------------------------

describe("me()", () => {
  it("returns MeResult on success", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, ME_DATA));
    const result = await new Reparatio("rp_key").me();
    expect(result.email).toBe("user@example.com");
    expect(result.plan).toBe("pro");
    expect(result.active).toBe(true);
    expect(result.api_access).toBe(true);
    expect(result.request_count).toBe(42);
  });

  it("calls GET /api/v1/me", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, ME_DATA));
    await new Reparatio("rp_key").me();
    const [url] = fetchMock.mock.calls[0] as [string];
    expect(url).toContain("/api/v1/me");
  });

  it("throws on 401", async () => {
    fetchMock.mockResolvedValue(makeResponse(401, { detail: "Unauthorized" }));
    await expect(new Reparatio("rp_bad").me()).rejects.toSatisfy(
      (e: unknown) => e instanceof ReparatioError && e.status === 401,
    );
  });

  it("throws on 403", async () => {
    fetchMock.mockResolvedValue(makeResponse(403, { detail: "Forbidden" }));
    await expect(new Reparatio("rp_key").me()).rejects.toSatisfy(
      (e: unknown) => e instanceof ReparatioError && e.status === 403,
    );
  });
});

// ---------------------------------------------------------------------------
// inspect()
// ---------------------------------------------------------------------------

describe("inspect()", () => {
  it("returns InspectResult on success", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, INSPECT_DATA));
    const file = new Blob([CSV_BYTES], { type: "text/csv" });
    const result = await new Reparatio("rp_key").inspect(file, "data.csv");
    expect(result.filename).toBe("data.csv");
    expect(result.rows_total).toBe(500);
    expect(result.columns).toHaveLength(2);
    expect(result.columns[0].name).toBe("id");
  });

  it("calls POST /api/v1/inspect", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, INSPECT_DATA));
    await new Reparatio("rp_key").inspect(new Blob([CSV_BYTES]), "data.csv");
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain("/api/v1/inspect");
    expect(init.method).toBe("POST");
    expect(init.body).toBeInstanceOf(FormData);
  });

  it("uses File.name when no explicit filename given", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, INSPECT_DATA));
    const file = new File([CSV_BYTES], "upload.csv", { type: "text/csv" });
    await new Reparatio("rp_key").inspect(file);
    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    const fd = init.body as FormData;
    const fileField = fd.get("file") as File;
    expect(fileField.name).toBe("upload.csv");
  });

  it("appends no_header=true when noHeader option set", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, INSPECT_DATA));
    await new Reparatio("rp_key").inspect(new Blob([CSV_BYTES]), "d.csv", { noHeader: true });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("no_header")).toBe("true");
  });

  it("appends fix_encoding=false when fixEncoding=false", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, INSPECT_DATA));
    await new Reparatio("rp_key").inspect(new Blob([CSV_BYTES]), "d.csv", { fixEncoding: false });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("fix_encoding")).toBe("false");
  });

  it("does NOT append fix_encoding when fixEncoding is true (default)", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, INSPECT_DATA));
    await new Reparatio("rp_key").inspect(new Blob([CSV_BYTES]), "d.csv");
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("fix_encoding")).toBeNull();
  });

  it("appends preview_rows when set", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, INSPECT_DATA));
    await new Reparatio("rp_key").inspect(new Blob([CSV_BYTES]), "d.csv", { previewRows: 20 });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("preview_rows")).toBe("20");
  });

  it("appends delimiter when set", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, INSPECT_DATA));
    await new Reparatio("rp_key").inspect(new Blob([CSV_BYTES]), "d.csv", { delimiter: "|" });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("delimiter")).toBe("|");
  });

  it("throws ReparatioError on 422", async () => {
    fetchMock.mockResolvedValue(makeResponse(422, { detail: "Cannot parse file" }));
    await expect(
      new Reparatio("rp_key").inspect(new Blob([new Uint8Array([0, 1, 2])]), "garbage.bin"),
    ).rejects.toSatisfy((e: unknown) => e instanceof ReparatioError && e.status === 422);
  });
});

// ---------------------------------------------------------------------------
// convert()
// ---------------------------------------------------------------------------

describe("convert()", () => {
  it("returns ArrayBuffer and filename on success", async () => {
    fetchMock.mockResolvedValue(
      makeBinaryResponse(200, PARQUET_MAGIC, {
        "content-disposition": 'attachment; filename="data.parquet"',
      }),
    );
    const result = await new Reparatio("rp_key").convert(
      new Blob([CSV_BYTES]), "parquet", "data.csv",
    );
    expect(result.data).toBeInstanceOf(ArrayBuffer);
    expect(result.filename).toBe("data.parquet");
  });

  it("uses fallback filename when no content-disposition", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    const result = await new Reparatio("rp_key").convert(
      new Blob([CSV_BYTES]), "parquet", "data.csv",
    );
    expect(result.filename).toBe("output.parquet");
  });

  it("sends target_format in FormData", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").convert(new Blob([CSV_BYTES]), "parquet", "data.csv");
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("target_format")).toBe("parquet");
  });

  it("sends select_columns as JSON array", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").convert(new Blob([CSV_BYTES]), "parquet", "d.csv", {
      selectColumns: ["id", "name"],
    });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(JSON.parse(fd.get("select_columns") as string)).toEqual(["id", "name"]);
  });

  it("sends cast_columns as JSON object", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").convert(new Blob([CSV_BYTES]), "parquet", "d.csv", {
      castColumns: { price: { type: "Float64" }, date: { type: "Date", format: "%d/%m/%Y" } },
    });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(JSON.parse(fd.get("cast_columns") as string)).toEqual({
      price: { type: "Float64" },
      date: { type: "Date", format: "%d/%m/%Y" },
    });
  });

  it("sends null_values as JSON array", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").convert(new Blob([CSV_BYTES]), "parquet", "d.csv", {
      nullValues: ["N/A", "NULL", "-"],
    });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(JSON.parse(fd.get("null_values") as string)).toEqual(["N/A", "NULL", "-"]);
  });

  it("sends deduplicate=true", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").convert(new Blob([CSV_BYTES]), "csv", "d.csv", {
      deduplicate: true,
    });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("deduplicate")).toBe("true");
  });

  it("sends sample_n when set", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").convert(new Blob([CSV_BYTES]), "csv", "d.csv", { sampleN: 100 });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("sample_n")).toBe("100");
    expect(fd.get("sample_frac")).toBeNull(); // not sent when sampleN provided
  });

  it("sends sample_frac when sampleN not set", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").convert(new Blob([CSV_BYTES]), "csv", "d.csv", { sampleFrac: 0.1 });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("sample_frac")).toBe("0.1");
    expect(fd.get("sample_n")).toBeNull();
  });

  it("sends no_header=true", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").convert(new Blob([CSV_BYTES]), "csv", "d.csv", { noHeader: true });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("no_header")).toBe("true");
  });

  it("sends fix_encoding=false", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").convert(new Blob([CSV_BYTES]), "csv", "d.csv", {
      fixEncoding: false,
    });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("fix_encoding")).toBe("false");
  });

  it("sends geometry_column when set", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").convert(new Blob([CSV_BYTES]), "geojson", "d.csv", {
      geometryColumn: "wkt",
    });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("geometry_column")).toBe("wkt");
  });

  it("uses File.name when no explicit filename", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    const file = new File([CSV_BYTES], "upload.csv");
    await new Reparatio("rp_key").convert(file, "parquet");
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    const fileField = fd.get("file") as File;
    expect(fileField.name).toBe("upload.csv");
  });

  it("throws ReparatioError 402 on insufficient plan", async () => {
    fetchMock.mockResolvedValue(makeResponse(402, { detail: "Requires Professional plan" }));
    await expect(
      new Reparatio("rp_key").convert(new Blob([CSV_BYTES]), "parquet", "d.csv"),
    ).rejects.toSatisfy((e: unknown) => e instanceof ReparatioError && e.status === 402);
  });

  it("throws ReparatioError 413 on file too large", async () => {
    fetchMock.mockResolvedValue(makeResponse(413, { detail: "File too large" }));
    await expect(
      new Reparatio("rp_key").convert(new Blob([CSV_BYTES]), "parquet", "d.csv"),
    ).rejects.toSatisfy((e: unknown) => e instanceof ReparatioError && e.status === 413);
  });

  it("throws ReparatioError 422 on parse error", async () => {
    fetchMock.mockResolvedValue(makeResponse(422, { detail: "Cannot parse file" }));
    await expect(
      new Reparatio("rp_key").convert(new Blob([new Uint8Array([0, 1, 2])]), "csv", "garbage.bin"),
    ).rejects.toSatisfy((e: unknown) => e instanceof ReparatioError && e.status === 422);
  });

  it("throws ReparatioError on 500 with non-JSON body", async () => {
    fetchMock.mockResolvedValue(new Response("Internal Server Error", { status: 500 }));
    await expect(
      new Reparatio("rp_key").convert(new Blob([CSV_BYTES]), "parquet", "d.csv"),
    ).rejects.toSatisfy((e: unknown) => e instanceof ReparatioError && e.status === 500);
  });
});

// ---------------------------------------------------------------------------
// append()
// ---------------------------------------------------------------------------

describe("append()", () => {
  it("returns ArrayBuffer and filename on success", async () => {
    fetchMock.mockResolvedValue(
      makeBinaryResponse(200, PARQUET_MAGIC, {
        "content-disposition": 'attachment; filename="appended.parquet"',
      }),
    );
    const files = [
      { file: new Blob([CSV_BYTES]), filename: "jan.csv" },
      { file: new Blob([CSV_BYTES]), filename: "feb.csv" },
    ];
    const result = await new Reparatio("rp_key").append(files, "parquet");
    expect(result.data).toBeInstanceOf(ArrayBuffer);
    expect(result.filename).toBe("appended.parquet");
  });

  it("uses fallback filename when no content-disposition", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    const files = [
      { file: new Blob([CSV_BYTES]), filename: "a.csv" },
      { file: new Blob([CSV_BYTES]), filename: "b.csv" },
    ];
    const result = await new Reparatio("rp_key").append(files, "parquet");
    expect(result.filename).toBe("appended.parquet");
  });

  it("throws Error when fewer than 2 files provided", async () => {
    await expect(
      new Reparatio("rp_key").append(
        [{ file: new Blob([CSV_BYTES]), filename: "only.csv" }],
        "parquet",
      ),
    ).rejects.toThrow("At least 2 files required");
  });

  it("throws Error on empty file list", async () => {
    await expect(new Reparatio("rp_key").append([], "parquet")).rejects.toThrow();
  });

  it("appends all files to FormData", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    const files = [
      { file: new Blob([CSV_BYTES]), filename: "jan.csv" },
      { file: new Blob([CSV_BYTES]), filename: "feb.csv" },
      { file: new Blob([CSV_BYTES]), filename: "mar.csv" },
    ];
    await new Reparatio("rp_key").append(files, "parquet");
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.getAll("files")).toHaveLength(3);
  });

  it("sends target_format", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    const files = [
      { file: new Blob([CSV_BYTES]), filename: "a.csv" },
      { file: new Blob([CSV_BYTES]), filename: "b.csv" },
    ];
    await new Reparatio("rp_key").append(files, "parquet");
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("target_format")).toBe("parquet");
  });

  it("uses File.name when no filename given", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    const f1 = new File([CSV_BYTES], "jan.csv");
    const f2 = new File([CSV_BYTES], "feb.csv");
    await new Reparatio("rp_key").append([{ file: f1 }, { file: f2 }], "parquet");
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    const uploaded = fd.getAll("files") as File[];
    expect(uploaded.map((f) => f.name)).toEqual(["jan.csv", "feb.csv"]);
  });

  it("sends no_header=true", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    const files = [
      { file: new Blob([CSV_BYTES]), filename: "a.csv" },
      { file: new Blob([CSV_BYTES]), filename: "b.csv" },
    ];
    await new Reparatio("rp_key").append(files, "parquet", { noHeader: true });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("no_header")).toBe("true");
  });
});

// ---------------------------------------------------------------------------
// merge()
// ---------------------------------------------------------------------------

describe("merge()", () => {
  it("returns ArrayBuffer and filename on success", async () => {
    fetchMock.mockResolvedValue(
      makeBinaryResponse(200, PARQUET_MAGIC, {
        "content-disposition": 'attachment; filename="orders_left_customers.parquet"',
      }),
    );
    const result = await new Reparatio("rp_key").merge(
      new Blob([CSV_BYTES]),
      new Blob([CSV_BYTES]),
      { op: "left", format: "parquet", on: "id" },
      "orders.csv",
      "customers.csv",
    );
    expect(result.filename).toBe("orders_left_customers.parquet");
    expect(result.data).toBeInstanceOf(ArrayBuffer);
  });

  it("sends operation and target_format", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").merge(
      new Blob([CSV_BYTES]),
      new Blob([CSV_BYTES]),
      { op: "inner", format: "csv" },
    );
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("operation")).toBe("inner");
    expect(fd.get("target_format")).toBe("csv");
  });

  it("sends string join_on directly", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").merge(
      new Blob([CSV_BYTES]),
      new Blob([CSV_BYTES]),
      { op: "left", format: "parquet", on: "customer_id" },
    );
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("join_on")).toBe("customer_id");
  });

  it("joins array join_on with comma", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").merge(
      new Blob([CSV_BYTES]),
      new Blob([CSV_BYTES]),
      { op: "left", format: "parquet", on: ["region", "year"] },
    );
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("join_on")).toBe("region,year");
  });

  it("omits join_on when not provided", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").merge(
      new Blob([CSV_BYTES]),
      new Blob([CSV_BYTES]),
      { op: "append", format: "parquet" },
    );
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("join_on")).toBeNull();
  });

  it("uses fallback filename", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    const result = await new Reparatio("rp_key").merge(
      new Blob([CSV_BYTES]),
      new Blob([CSV_BYTES]),
      { op: "append", format: "csv" },
    );
    expect(result.filename).toBe("merged.csv");
  });

  it("sends geometry_column", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").merge(
      new Blob([CSV_BYTES]),
      new Blob([CSV_BYTES]),
      { op: "left", format: "geojson", geometryColumn: "wkt" },
    );
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("geometry_column")).toBe("wkt");
  });
});

// ---------------------------------------------------------------------------
// query()
// ---------------------------------------------------------------------------

describe("query()", () => {
  const SQL = "SELECT region, SUM(revenue) FROM data GROUP BY region";

  it("returns ArrayBuffer and filename on success", async () => {
    fetchMock.mockResolvedValue(
      makeBinaryResponse(200, CSV_BYTES, {
        "content-disposition": 'attachment; filename="events_query.csv"',
      }),
    );
    const result = await new Reparatio("rp_key").query(
      new Blob([PARQUET_MAGIC]), SQL, "events.parquet",
    );
    expect(result.data).toBeInstanceOf(ArrayBuffer);
    expect(result.filename).toBe("events_query.csv");
  });

  it("sends sql in FormData", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, CSV_BYTES));
    await new Reparatio("rp_key").query(new Blob([CSV_BYTES]), SQL, "d.csv");
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("sql")).toBe(SQL);
  });

  it("defaults target_format to csv", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, CSV_BYTES));
    await new Reparatio("rp_key").query(new Blob([CSV_BYTES]), SQL, "d.csv");
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("target_format")).toBe("csv");
  });

  it("sends custom format", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, PARQUET_MAGIC));
    await new Reparatio("rp_key").query(new Blob([CSV_BYTES]), SQL, "d.csv", { format: "parquet" });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("target_format")).toBe("parquet");
  });

  it("uses fallback filename when no content-disposition", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, CSV_BYTES));
    const result = await new Reparatio("rp_key").query(new Blob([CSV_BYTES]), SQL, "d.csv");
    expect(result.filename).toBe("query.csv");
  });

  it("sends delimiter and sheet", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, CSV_BYTES));
    await new Reparatio("rp_key").query(new Blob([CSV_BYTES]), SQL, "d.csv", {
      delimiter: "|",
      sheet: "Data",
    });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("delimiter")).toBe("|");
    expect(fd.get("sheet")).toBe("Data");
  });

  it("calls POST /api/v1/query", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, CSV_BYTES));
    await new Reparatio("rp_key").query(new Blob([CSV_BYTES]), SQL, "d.csv");
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toContain("/api/v1/query");
    expect(init.method).toBe("POST");
  });
});

// ---------------------------------------------------------------------------
// batchConvert()
// ---------------------------------------------------------------------------

describe("batchConvert()", () => {
  const ZIP_MAGIC = new Uint8Array([0x50, 0x4b, 0x03, 0x04]);

  it("returns ArrayBuffer, filename, and empty errors on success", async () => {
    fetchMock.mockResolvedValue(
      makeBinaryResponse(200, ZIP_MAGIC, {
        "content-disposition": 'attachment; filename="converted.zip"',
      }),
    );
    const result = await new Reparatio("rp_key").batchConvert(
      new Blob([ZIP_MAGIC]), "parquet", "data.zip",
    );
    expect(result.data).toBeInstanceOf(ArrayBuffer);
    expect(result.filename).toBe("converted.zip");
    expect(result.errors).toEqual([]);
  });

  it("decodes X-Reparatio-Errors header", async () => {
    const errors = [{ file: "bad.bin", error: "Cannot parse" }];
    fetchMock.mockResolvedValue(
      makeBinaryResponse(200, ZIP_MAGIC, {
        "X-Reparatio-Errors": encodeURIComponent(JSON.stringify(errors)),
      }),
    );
    const result = await new Reparatio("rp_key").batchConvert(
      new Blob([ZIP_MAGIC]), "parquet", "data.zip",
    );
    expect(result.errors).toEqual(errors);
  });

  it("returns empty errors when no error header", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, ZIP_MAGIC));
    const result = await new Reparatio("rp_key").batchConvert(
      new Blob([ZIP_MAGIC]), "parquet", "data.zip",
    );
    expect(result.errors).toEqual([]);
  });

  it("handles malformed error header gracefully", async () => {
    fetchMock.mockResolvedValue(
      makeBinaryResponse(200, ZIP_MAGIC, {
        "X-Reparatio-Errors": "not valid json {{{{",
      }),
    );
    const result = await new Reparatio("rp_key").batchConvert(
      new Blob([ZIP_MAGIC]), "parquet", "data.zip",
    );
    expect(result.errors).toEqual([]); // graceful fallback
  });

  it("sends select_columns as JSON array", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, ZIP_MAGIC));
    await new Reparatio("rp_key").batchConvert(new Blob([ZIP_MAGIC]), "parquet", "d.zip", {
      selectColumns: ["id", "name"],
    });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(JSON.parse(fd.get("select_columns") as string)).toEqual(["id", "name"]);
  });

  it("sends deduplicate=true", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, ZIP_MAGIC));
    await new Reparatio("rp_key").batchConvert(new Blob([ZIP_MAGIC]), "parquet", "d.zip", {
      deduplicate: true,
    });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(fd.get("deduplicate")).toBe("true");
  });

  it("sends cast_columns as JSON", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, ZIP_MAGIC));
    await new Reparatio("rp_key").batchConvert(new Blob([ZIP_MAGIC]), "parquet", "d.zip", {
      castColumns: { price: { type: "Float64" } },
    });
    const fd = (fetchMock.mock.calls[0] as [string, RequestInit])[1].body as FormData;
    expect(JSON.parse(fd.get("cast_columns") as string)).toEqual({ price: { type: "Float64" } });
  });

  it("uses fallback filename converted.zip", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, ZIP_MAGIC));
    const result = await new Reparatio("rp_key").batchConvert(
      new Blob([ZIP_MAGIC]), "parquet", "d.zip",
    );
    expect(result.filename).toBe("converted.zip");
  });

  it("calls POST /api/v1/batch-convert", async () => {
    fetchMock.mockResolvedValue(makeBinaryResponse(200, ZIP_MAGIC));
    await new Reparatio("rp_key").batchConvert(new Blob([ZIP_MAGIC]), "parquet", "d.zip");
    const [url] = fetchMock.mock.calls[0] as [string];
    expect(url).toContain("/api/v1/batch-convert");
  });
});

// ---------------------------------------------------------------------------
// AbortController / timeout wiring (structural check)
// ---------------------------------------------------------------------------

describe("timeout / abort wiring", () => {
  it("passes signal to fetch", async () => {
    fetchMock.mockResolvedValue(makeResponse(200, FORMATS_DATA));
    await new Reparatio("rp_key").formats();
    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(init.signal).toBeInstanceOf(AbortSignal);
  });
});
