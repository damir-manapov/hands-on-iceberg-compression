import { EnumSpec, FieldSpec, TableConfig } from "./types";

function sqlTypeOf(spec: FieldSpec): string {
  switch (spec.kind) {
    case "int":
      return "integer";
    case "bigint":
      return "bigint";
    case "double":
      return "double";
    case "string":
      return "varchar";
    case "date":
      return "date";
    case "timestamp":
      return "timestamp(3)";
    case "enum":
      if (spec.base === "string") return "varchar";
      if (spec.base === "int") return "integer";
      if (spec.base === "bigint") return "bigint";
      throw new Error("Unsupported enum base");
    case "array":
      return `array(${sqlTypeOf(spec.element)})`;
    default:
      throw new Error("Unknown spec");
  }
}
function wrapNullable(expr: string, p?: number) {
  return !p || p <= 0
    ? expr
    : `CASE WHEN random() < ${p} THEN NULL ELSE (${expr}) END`;
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
function enumExpr(spec: EnumSpec<"string" | "int" | "bigint">) {
  const arr = spec.values
    .map(v =>
      spec.base === "string" ? `'${String(v).replace(/'/g, "''")}'` : `${v}`
    )
    .join(", ");
  const n = spec.values.length;
  return `element_at(ARRAY[${arr}], 1 + CAST(floor(random()*${n}) AS integer))`;
}
function daysBetween(start: string, end: string): number {
  const s = new Date(start + "T00:00:00Z").getTime();
  const e = new Date(end + "T00:00:00Z").getTime();
  return Math.max(0, Math.round((e - s) / (24 * 3600 * 1000)));
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
    case "int":
      return wrapNullable(intExpr(spec.min, spec.max), spec.nullable);
    case "bigint":
      return wrapNullable(intExpr(spec.min, spec.max), spec.nullable).replace(
        /\binteger\b/g,
        "bigint"
      );
    case "double":
      return wrapNullable(
        `round(${doubleExpr(spec.min, spec.max)}, 6)`,
        spec.nullable
      );
    case "string":
      return wrapNullable(randStringExpr(spec.length), spec.nullable);
    case "date":
      return wrapNullable(dateExpr(spec.start, spec.end), spec.nullable);
    case "timestamp":
      return wrapNullable(timestampExpr(spec.start, spec.end), spec.nullable);
    case "enum":
      return wrapNullable(enumExpr(spec), spec.nullable);
    case "array": {
      const minLen = Math.max(0, spec.minLen);
      const span = Math.max(0, spec.maxLen - minLen);
      const nExpr = `${minLen} + CAST(floor(random()*${span + 1}) AS integer)`;
      const elem = columnExpr(spec.element);
      const arrExpr = `transform(sequence(1, ${nExpr}), x -> ${elem})`;
      return wrapNullable(arrExpr, spec.nullable);
    }
    default:
      throw new Error("Unknown spec");
  }
}

export function createSchemaSQL(cfg: TableConfig) {
  return `CREATE SCHEMA IF NOT EXISTS ${cfg.catalog}.${cfg.schema}`;
}

export function createBaseTableSQL(cfg: TableConfig) {
  const fq = `${cfg.catalog}.${cfg.schema}.${cfg.tableBase}_base`;
  const cols = Object.entries(cfg.columns)
    .map(([n, s]) => `  ${n} ${sqlTypeOf(s)}`)
    .join(",\n");
  return `CREATE TABLE IF NOT EXISTS ${fq} (\n${cols}\n)`;
}

export function createSelectExamplesSQL(cfg: TableConfig, tableName: string) {
  const fq = `${cfg.catalog}.${cfg.schema}.${tableName}`;
  return `SELECT * FROM ${fq} LIMIT 1`;
}

export function createVariantTableSQLs(
  cfg: TableConfig,
  name: string,
  codec: string,
  level?: number
): string[] {
  const fq = `${cfg.catalog}.${cfg.schema}.${name}`;
  const props: string[] = [];
  const extraProps: [string, string][] = [];
  if (cfg.format) props.push(`format = '${cfg.format}'`);
  // if (cfg.partitioning?.length) props.push(`partitioning = ARRAY[${cfg.partitioning.map(p => `'${p}'`).join(", ")}]`); // FIXME
  if (cfg.tableProperties) {
    for (const [k, v] of Object.entries(cfg.tableProperties)) {
      const val =
        typeof v === "string"
          ? `'${v}'`
          : typeof v === "boolean"
            ? v
              ? "true"
              : "false"
            : `${v}`;
      props.push(`"${k}" = ${val}`);
    }
  }
  extraProps.push(["write.parquet.compression-codec", codec]);
  if (typeof level === "number")
    extraProps.push(["write.parquet.compression-level", level.toString()]);

  // There is some problems setting extra_properties via WITH in CRETE statement, so setting them by ALTER stmnt
  const alterByExtraProps = extraProps.map(
    ([key, value]) =>
      `ALTER TABLE ${fq} SET PROPERTIES extra_properties = map_from_entries(ARRAY[ROW('${key}', '${value}')])`
  );

  return [
    `CREATE TABLE IF NOT EXISTS ${fq} (LIKE ${cfg.catalog}.${cfg.schema}.${cfg.tableBase}_base)
WITH (
  ${props.join(",\n  ")}
)`,
    ...alterByExtraProps,
  ];
}

export function buildInsertSQL(
  rangeStart: number,
  rangeEnd: number,
  cfg: TableConfig,
  tableName: string
) {
  const fq = `${cfg.catalog}.${cfg.schema}.${tableName}`;
  const idCol =
    cfg.idColumn && cfg.columns[cfg.idColumn] ? cfg.idColumn : undefined;

  // <= 10k elements per sequence()
  const BLOCK = 10_000;
  const selectCols = Object.entries(cfg.columns)
    .map(([name, spec]) => {
      if (idCol && name === idCol) {
        const typeCast = sqlTypeOf(spec);
        return `  CAST(id AS ${typeCast}) AS ${name}`;
      }
      return `  ${columnExpr(spec)} AS ${name}`;
    })
    .join(",\n");

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
