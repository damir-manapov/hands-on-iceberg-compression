/**
 * bench.ts — Generate, load & measure Iceberg/Parquet tables across codecs & levels using Trino
 * Run:  npx ts-node bench.ts
 *
 * What it does:
 * 1) Creates a base schema (config-driven) + one table per (codec, level) variant
 * 2) Loads the same data volume into each table with batched INSERT..SELECT using sequence()
 * 3) (optional) CALL system.optimize(...) to normalize file sizes
 * 4) Measures storage from $files (+ optional manifests) and prints CSV + console table
 */

import * as fs from "node:fs";
import * as path from "node:path";

/* =========================
   0) CONFIG
   ========================= */

type NumberSpec = { kind: "int" | "bigint" | "double"; min: number; max: number; nullable?: number };
type StringSpec = { kind: "string"; length: number; nullable?: number };
type DateSpec =
  | { kind: "date"; start: string; end: string; nullable?: number }
  | { kind: "timestamp"; start: string; end: string; nullable?: number };
type EnumSpec<T extends "string" | "int" | "bigint"> = {
  kind: "enum";
  base: T;
  values: (T extends "string" ? string : number)[];
  nullable?: number;
};
type ArraySpec =
  | { kind: "array"; element: Exclude<FieldSpec, ArraySpec>; minLen: number; maxLen: number; nullable?: number };
type FieldSpec = NumberSpec | StringSpec | DateSpec | EnumSpec<any> | ArraySpec;

type TableConfig = {
  catalog: string;
  schema: string;
  tableBase: string;  // base name used for variants, e.g. "events"
  format?: "PARQUET" | "ORC" | "AVRO";
  partitioning?: string[];
  tableProperties?: Record<string, string | number | boolean>;
  columns: Record<string, FieldSpec>;
  idColumn?: string;
};

const CONFIG: TableConfig = {
  catalog: process.env.TRINO_CATALOG ?? "iceberg",
  schema: process.env.TRINO_SCHEMA ?? "lab",
  tableBase: process.env.TABLE_BASE ?? "events",
  format: "PARQUET",
  partitioning: ["date(created_at)"],
//   tableProperties: {
//     "write.target-file-size-bytes": 512 * 1024 * 1024, // 512 MB
//   },
  columns: {
    id:           { kind: "bigint", min: 1, max: 1, nullable: 0 },
    user_name:    { kind: "string", length: 16, nullable: 0 },
    amount:       { kind: "double", min: 0, max: 10000, nullable: 0.05 },
    created_at:   { kind: "timestamp", start: "2024-01-01", end: "2025-01-01", nullable: 0 },
    status:       { kind: "enum", base: "string", values: ["new","paid","delivered","canceled"], nullable: 0 },
    tags:         { kind: "array", element: { kind: "enum", base: "string", values: ["alpha","beta","gamma","delta"] }, minLen: 1, maxLen: 3, nullable: 0.1 },
    country:      { kind: "enum", base: "string", values: ["KZ","US","DE","TR","PL"], nullable: 0.02 },
    age:          { kind: "int", min: 18, max: 78, nullable: 0.03 },
    note:         { kind: "string", length: 8, nullable: 0.1 },
  },
  idColumn: "id",
};

// Which codecs & levels to test
// Use [] for levels if the codec ignores levels (e.g., snappy); script still creates a single table with level "00".
const CODECS: Array<{ codec: "zstd" | "gzip" | "snappy" | "lz4"; levels: number[] }> = [
  // { codec: "zstd",   levels: [6] },
  { codec: "zstd",   levels: [1, 3, 6, 9] },
  { codec: "gzip",   levels: [1, 6, 9] },
  { codec: "snappy", levels: [0] },    // no levels; keep 0 as placeholder
  // { codec: "lz4", levels: [0] },    // enable if your build supports LZ4
];

// Load plan
const LOAD = {
  startId: Number(process.env.START_ID ?? "1"),
  totalRows: Number(process.env.TOTAL_ROWS ?? `${100_000}`),   // e.g., 10M
  batchRows: Number(process.env.BATCH_ROWS ?? `${100_000}`),    // e.g., 5M per insert
  concurrency: Number(process.env.CONCURRENCY ?? "4"),
  createBaseSchema: (process.env.CREATE_BASE_SCHEMA ?? "true") === "true",
  compactAfterLoad: (process.env.COMPACT_AFTER_LOAD ?? "true") === "true",
  checkpointDir: process.env.CHECKPOINT_DIR ?? ".checkpoints",
  includeManifestBytes: (process.env.MEASURE_INCLUDE_MANIFESTS ?? "true") === "true",
  resultsCsv: process.env.RESULTS_CSV ?? "results_sizes.csv",
};

/* =========================
   1) Trino client (query + execute)
   ========================= */

type TrinoConfig = {
  host: string;
  port: number;
  catalog: string;
  schema: string;
  user: string;
  source?: string;
  basicAuth?: { username: string; password: string };
};

class TrinoClient {
  constructor(private cfg: TrinoConfig) {}
  private baseUrl() { return `${this.cfg.host}:${this.cfg.port}`; }
  private headers(): Record<string, string> {
    const h: Record<string, string> = {
      "X-Trino-User": this.cfg.user,
      "X-Trino-Catalog": this.cfg.catalog,
      "X-Trino-Schema": this.cfg.schema,
    };
    if (this.cfg.source) h["X-Trino-Source"] = this.cfg.source;
    if (this.cfg.basicAuth) {
      const { username, password } = this.cfg.basicAuth;
      h["Authorization"] = `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`;
    }
    return h;
  }
  async execute(sql: string): Promise<void> {
    await this._run(sql, false);
  }
  async query<T = any>(sql: string): Promise<T[]> {
    const all = await this._run(sql, true);
    return all as T[];
  }
  private async _run<T = any>(sql: string, collect: boolean): Promise<T[]> {
    const res = await fetch(`${this.baseUrl()}/v1/statement`, {
      method: "POST",
      headers: { "Content-Type": "text/plain; charset=utf-8", ...this.headers() },
      body: sql,
    });
    if (!res.ok) throw new Error(`Trino POST ${res.status}: ${await res.text()}`);
    let payload: any = await res.json();
    const rows: any[] = [];
    let colNames: string[] | null = payload.columns?.map((c: any) => c.name) ?? null;
    const rowsArr: any[][] = [];

    const take = (p: any) => {
      if (collect) {
        // (1) learn columns as soon as they show up
        if (!colNames && p.columns) {
          colNames = p.columns.map((c: any) => c.name);
        }
        // (2) collect any data page we get
        if (p.data && Array.isArray(p.data)) {
          for (const r of p.data) rowsArr.push(r);
        }
      }
    };
    take(payload);

    while (true) {
      if (payload.error) throw new Error(`Trino error: ${payload.error.message}`);
      if (!payload.nextUri) break;
      const poll = await fetch(payload.nextUri, { headers: this.headers() });
      if (!poll.ok) throw new Error(`Trino poll ${poll.status}: ${await poll.text()}`);
      payload = await poll.json();
      if (!colNames && payload.columns) {
        // late columns; extremely rare
      }
      take(payload);
      await new Promise(r => setTimeout(r, 150));
    }

    // Map arrays → objects if we have column names; else return arrays
    if (colNames) {
      return rowsArr.map(r => {
        const obj: any = {};
        if (colNames) {
          for (let i = 0; i < colNames.length; i++) obj[colNames[i]] = r[i];
        }
        return obj as T;
      });
    } else {
      // No columns (e.g., for some non-SELECT statements) — return empty
      return [] as T[];
    }
  }
}

/* =========================
   2) SQL helpers for schema + data gen
   ========================= */

function sqlTypeOf(spec: FieldSpec): string {
  switch (spec.kind) {
    case "int": return "integer";
    case "bigint": return "bigint";
    case "double": return "double";
    case "string": return "varchar";
    case "date": return "date";
    case "timestamp": return "timestamp(3)";
    case "enum":
      if (spec.base === "string") return "varchar";
      if (spec.base === "int") return "integer";
      if (spec.base === "bigint") return "bigint";
      throw new Error("Unsupported enum base");
    case "array": return `array(${sqlTypeOf(spec.element)})`;
    default: throw new Error("Unknown spec");
  }
}
function wrapNullable(expr: string, p?: number) {
  return !p || p <= 0 ? expr : `CASE WHEN random() < ${p} THEN NULL ELSE (${expr}) END`;
}
function intExpr(min: number, max: number) {
  const span = Math.max(0, Math.floor(max - min));
  return `${min} + CAST(floor(random()*${span + 1}) AS integer)`;
}
function doubleExpr(min: number, max: number) {
  const span = max - min;
  return `${min} + (random()*${span})`;
}
function randStringExpr(len: number) {
  return `substr(regexp_replace(CAST(uuid() AS varchar), '-', ''), 1, ${len})`;
}
function enumExpr(spec: EnumSpec<"string"|"int"|"bigint">) {
  const arr = spec.values.map(v => (spec.base === "string" ? `'${String(v).replace(/'/g,"''")}'` : `${v}`)).join(", ");
  const n = spec.values.length;
  return `element_at(ARRAY[${arr}], 1 + CAST(floor(random()*${n}) AS integer))`;
}
function daysBetween(start: string, end: string): number {
  const s = new Date(start + "T00:00:00Z").getTime();
  const e = new Date(end   + "T00:00:00Z").getTime();
  return Math.max(0, Math.round((e - s) / (24*3600*1000)));
}
function timestampExpr(start: string, end: string) {
  return `from_unixtime(CAST(to_unixtime(TIMESTAMP '${start}') + rand() * (to_unixtime(TIMESTAMP '${end}') - to_unixtime(TIMESTAMP '${start}')) AS bigint))`;
}
function dateExpr(start: string, end: string) {
  const dspan = daysBetween(start, end);
  return `date_add('day', CAST(floor(random()*${dspan + 1}) AS integer), DATE '${start}')`;
}
function columnExpr(spec: FieldSpec): string {
  switch (spec.kind) {
    case "int": return wrapNullable(intExpr(spec.min, spec.max), spec.nullable);
    case "bigint": return wrapNullable(intExpr(spec.min, spec.max), spec.nullable).replace(/\binteger\b/g, "bigint");
    case "double": return wrapNullable(`round(${doubleExpr(spec.min, spec.max)}, 6)`, spec.nullable);
    case "string": return wrapNullable(randStringExpr(spec.length), spec.nullable);
    case "date": return wrapNullable(dateExpr(spec.start, spec.end), spec.nullable);
    case "timestamp": return wrapNullable(timestampExpr(spec.start, spec.end), spec.nullable);
    case "enum": return wrapNullable(enumExpr(spec), spec.nullable);
    case "array": {
      const minLen = Math.max(0, spec.minLen);
      const span = Math.max(0, spec.maxLen - minLen);
      const nExpr = `${minLen} + CAST(floor(random()*${span + 1}) AS integer)`;
      const elem = columnExpr(spec.element);
      const arrExpr = `transform(sequence(1, ${nExpr}), x -> ${elem})`;
      return wrapNullable(arrExpr, spec.nullable);
    }
    default: throw new Error("Unknown spec");
  }
}

function createSchemaSQL(cfg: TableConfig) {
  return `CREATE SCHEMA IF NOT EXISTS ${cfg.catalog}.${cfg.schema}`;
}
function createBaseTableSQL(cfg: TableConfig) {
  const fq = `${cfg.catalog}.${cfg.schema}.${cfg.tableBase}_base`;
  const cols = Object.entries(cfg.columns).map(([n,s]) => `  ${n} ${sqlTypeOf(s)}`).join(",\n");
  return `CREATE TABLE IF NOT EXISTS ${fq} (\n${cols}\n)`;
}

function createVariantTableSQLs(cfg: TableConfig, name: string, codec: string, level?: number): string[] {
  const fq = `${cfg.catalog}.${cfg.schema}.${name}`;
  const props: string[] = [];
  const extraProps: [string, string][] = [];
  if (cfg.format) props.push(`format = '${cfg.format}'`);
  // if (cfg.partitioning?.length) props.push(`partitioning = ARRAY[${cfg.partitioning.map(p => `'${p}'`).join(", ")}]`); // FIXME
  if (cfg.tableProperties) {
    for (const [k,v] of Object.entries(cfg.tableProperties)) {
      const val = typeof v === "string" ? `'${v}'` : (typeof v === "boolean" ? (v ? "true":"false") : `${v}`);
      props.push(`"${k}" = ${val}`);
    }
  }
  extraProps.push(["write.parquet.compression-codec", codec]);
  if (typeof level === "number") extraProps.push(["write.parquet.compression-level", level.toString()]);

  // There is some problems setting extra_properties via WITH in CRETE statement, so setting them by ALTER stmnt
  const alterByExtraProps = extraProps.map(([key, value]) => `ALTER TABLE ${fq} SET PROPERTIES extra_properties = map_from_entries(ARRAY[ROW('${key}', '${value}')])`);
  
  return [
    `CREATE TABLE IF NOT EXISTS ${fq} (LIKE ${cfg.catalog}.${cfg.schema}.${cfg.tableBase}_base)
WITH (
  ${props.join(",\n  ")}
)`,
    ...alterByExtraProps,
  ];
}

function buildInsertSQL(rangeStart: number, rangeEnd: number, cfg: TableConfig, tableName: string) {
  const fq = `${cfg.catalog}.${cfg.schema}.${tableName}`;
  const idCol = cfg.idColumn && cfg.columns[cfg.idColumn] ? cfg.idColumn : undefined;

  // <= 10k elements per sequence()
  const BLOCK = 10_000;
  const selectCols = Object.entries(cfg.columns).map(([name,spec]) => {
    if (idCol && name === idCol) {
      const typeCast = sqlTypeOf(spec);
      return `  CAST(id AS ${typeCast}) AS ${name}`;
    }
    return `  ${columnExpr(spec)} AS ${name}`;
  }).join(",\n");

  return `
INSERT INTO ${fq}
WITH
  params AS (
    SELECT ${rangeStart} AS start_id, ${rangeEnd} AS end_id
  ),
  blocks AS (
    -- number of full/partial 10k blocks to cover [start..end]
    SELECT sequence(
      0,
      CAST(ceil( (end_id - start_id + 1) / ${BLOCK} ) AS integer) - 1
    ) AS b
    FROM params
  ),
  block_idx AS (
    SELECT x AS block
    FROM blocks, UNNEST(b) AS t(x)
  ),
  gen AS (
    -- produce up to 10k ids per block, trimmed to end_id
    SELECT
      start_id + block*${BLOCK} + offset - 1 AS id
    FROM params
    CROSS JOIN block_idx
    CROSS JOIN UNNEST(sequence(1, ${BLOCK})) AS u(offset)
    WHERE start_id + block*${BLOCK} + offset - 1 <= end_id
  )
SELECT
${selectCols}
FROM gen`;
}

/* =========================
   3) Orchestration helpers
   ========================= */

class Limiter {
  private active = 0; private q: (()=>void)[] = [];
  constructor(private n: number) {}
  async run<T>(fn: ()=>Promise<T>) {
    if (this.active >= this.n) await new Promise<void>(r => this.q.push(r));
    this.active++;
    try { return await fn(); }
    finally { this.active--; const nxt = this.q.shift(); if (nxt) nxt(); }
  }
}
function makeBatches(startId: number, totalRows: number, batchRows: number) {
  if (batchRows <= 0) throw new Error("batchRows must be > 0");
  const out: { index: number; start: number; end: number }[] = [];
  const cnt = Math.ceil(totalRows / batchRows);
  for (let i=0;i<cnt;i++) {
    const start = startId + i*batchRows;
    const end = Math.min(start + batchRows - 1, startId + totalRows - 1);
    out.push({ index: i, start, end });
  }
  return out;
}
function ensureDir(p: string) { if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true }); }

async function loadTable(client: TrinoClient, limiter: Limiter, cfg: TableConfig, tableName: string, startId: number, totalRows: number, batchRows: number, cpFile: string) {
  const batches = makeBatches(startId, totalRows, batchRows);
  let cp: { completedBatches: number[] } = { completedBatches: [] };
  try { cp = JSON.parse(fs.readFileSync(cpFile, "utf-8")); } catch {}
  const pending = batches.filter(b => !cp.completedBatches.includes(b.index));

  await Promise.all(pending.map(b => limiter.run(async () => {
    const label = `${tableName} #${b.index} [${humanNumber(b.start)}..${humanNumber(b.end)}]`;
    const sql = buildInsertSQL(b.start, b.end, cfg, tableName);
    const t0 = Date.now();
    try {
      console.log(`→ INSERT ${label}`);
      await client.execute(sql);
      console.log(`✔ OK ${label} in ${((Date.now()-t0)/1000).toFixed(1)}s`);
      cp.completedBatches.push(b.index);
      cp.completedBatches.sort((a,z)=>a-z);
      fs.writeFileSync(cpFile, JSON.stringify(cp), "utf-8");
    } catch (e: any) {
      const message = `✖ FAIL ${label}: ${e?.message || e}\n\nSql:${sql}`
      console.error(message);
      throw new Error(message);
    }
  })));
}

async function optimizeTable(client: TrinoClient, fq: string) {
  const sql = `ALTER TABLE ${fq} EXECUTE optimize`;
  try {
    await client.execute(sql);
  } catch (e: any) {
    console.warn(`(optimize skipped for ${fq}): ${e?.message || e}, sql: ${sql}`);
    throw new Error(e);
  }
}

type SizeRow = { table_name: string; codec: string; level: number; rows: number; data_bytes: number; bytes_per_row: number; manifest_bytes?: number; total_bytes?: number };

async function measureSizes(client: TrinoClient, cfg: TableConfig, name: string, codec: string, level: number): Promise<SizeRow> {
  const filesTbl = `"${name}$files"`;
  const baseSQL = `SELECT SUM(file_size_in_bytes) AS data_bytes, SUM(record_count) AS rows, (SUM(file_size_in_bytes) / NULLIF(SUM(record_count),0)) AS bytes_per_row FROM ${cfg.catalog}.${cfg.schema}.${filesTbl}`;
  const filesRes = await client.query<{data_bytes: number; rows: number; bytes_per_row: number}>(baseSQL);
  const data_bytes = Number(filesRes?.[0]?.data_bytes ?? 0);
  const rows = Number(filesRes?.[0]?.rows ?? 0);
  const bytes_per_row = Number(filesRes?.[0]?.bytes_per_row ?? 0);

  if (!LOAD.includeManifestBytes) {
    return { table_name: name, codec, level, rows, data_bytes, bytes_per_row };
  }

  // manifests (optional)
  let manifest_bytes = 0;
  try {
    const m = await client.query<{b: number}>(`
      SELECT COALESCE(SUM(length),0) AS b
      FROM ${cfg.catalog}.${cfg.schema}."${name}$manifests"
    `);
    manifest_bytes = Number(m?.[0]?.b ?? 0);
  } catch (e: any) {
    throw new Error(e);
  }
  const total_bytes = data_bytes + manifest_bytes;
  return { table_name: name, codec, level, rows, data_bytes, bytes_per_row, manifest_bytes, total_bytes };
}

function humanSize(bytes: number | null | undefined): string {
  if (bytes == null) return "";
  if (bytes === 0) return "0 B";
  const thresh = 1024;
  const units = ["B", "KB", "MB", "GB", "TB"];
  let u = 0;
  let b = bytes;
  while (b >= thresh && u < units.length - 1) {
    b /= thresh;
    u++;
  }
  return `${b.toFixed(1)} ${units[u]}`;
}

const humanNumber = (num: number | null | undefined): string => num?.toLocaleString("en-US").replaceAll(',', '_') ?? '';

/* =========================
   4) MAIN
   ========================= */

async function main() {
  // Trino connection
  const trino: TrinoConfig = {
    host: process.env.TRINO_HOST ?? "http://localhost",
    port: Number(process.env.TRINO_PORT ?? "8080"),
    catalog: CONFIG.catalog,
    schema: CONFIG.schema,
    user: process.env.TRINO_USER ?? "bench",
    source: "ts-bench",
    ...(process.env.TRINO_USERNAME && process.env.TRINO_PASSWORD
      ? { basicAuth: { username: process.env.TRINO_USERNAME, password: process.env.TRINO_PASSWORD } }
      : {}),
  };
  const client = new TrinoClient(trino);
  const limiter = new Limiter(LOAD.concurrency);

  // 1) Base schema + base table
  if (LOAD.createBaseSchema) {
    console.log("Ensuring schema + base table exist…");
    await client.execute(createSchemaSQL(CONFIG));
    await client.execute(createBaseTableSQL(CONFIG));
  }

  ensureDir(LOAD.checkpointDir);

  // 2) Create variants and load
  const variants: Array<{ name: string; codec: string; level: number }> = [];
  for (const { codec, levels } of CODECS) {
    for (const level of levels) {
      const suffix = `${codec}_l${String(level).padStart(2,"0")}`;
      const name = `${CONFIG.tableBase}_${suffix}`;
      console.log(`Creating table ${name} (codec=${codec}, level=${level})…`);
      const variantTableSQLs = createVariantTableSQLs(CONFIG, name, codec, level);
      for (const sql of variantTableSQLs) {
        await client.execute(sql);
      }
      const cpFile = path.join(LOAD.checkpointDir, `.cp_${name}.json`);
      const t0 = Date.now();
      await loadTable(client, limiter, CONFIG, name, LOAD.startId, LOAD.totalRows, LOAD.batchRows, cpFile);
      console.log(`Load finished for ${name} in ${((Date.now()-t0)/1000).toFixed(1)}s`);

      if (LOAD.compactAfterLoad) {
        await optimizeTable(client, `${CONFIG.catalog}.${CONFIG.schema}.${name}`);
      }

      variants.push({ name, codec, level });
    }
  }

  // 3) Measure sizes
  const results: SizeRow[] = [];
  for (const v of variants) {
    const r = await measureSizes(client, CONFIG, v.name, v.codec, v.level);
    results.push(r);
  }

  // 4) Print results & CSV
  results.sort((a,b) => (a.codec === b.codec ? a.level - b.level : a.codec.localeCompare(b.codec)));

  console.log("\nRESULTS (bytes rounded):");
  const pretty = results.map(r => ({
    table: r.table_name,
    codec: r.codec,
    level: r.level,
    rows: humanNumber(r.rows),
    data_bytes: Math.round(r.data_bytes),
    data_human: humanSize(r.data_bytes),
    bytes_per_row: Math.round(r.bytes_per_row),
    ...(LOAD.includeManifestBytes ? {
      manifest_bytes: Math.round(r.manifest_bytes || 0),
      manifest_human: humanSize(r.manifest_bytes || 0),
      total_bytes: Math.round(r.total_bytes || r.data_bytes),
      total_human: humanSize(r.total_bytes || r.data_bytes),
    } : {})
  }));
  console.table(pretty);

  const headers = [
    "table_name","codec","level","rows","data_bytes","data_human","bytes_per_row",
    ...(LOAD.includeManifestBytes ? ["manifest_bytes","manifest_human","total_bytes","total_human"] : [])
  ];
  const lines = [headers.join(",")].concat(results.map(r => [
    r.table_name, r.codec, String(r.level), humanNumber(r.rows),
    String(r.data_bytes), humanSize(r.data_bytes), String(r.bytes_per_row),
    ...(LOAD.includeManifestBytes ? [
      String(r.manifest_bytes ?? 0),
      humanSize(r.manifest_bytes ?? 0),
      String(r.total_bytes ?? r.data_bytes),
      humanSize(r.total_bytes ?? r.data_bytes),
    ] : [])
  ].join(",")));
  fs.writeFileSync(LOAD.resultsCsv, lines.join("\n"), "utf-8");
  console.log(`\nSaved CSV: ${LOAD.resultsCsv}`);
}

main().catch(e => { console.error(e); process.exit(1); });
