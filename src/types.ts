type NumberSpec = {
  kind: "int" | "bigint" | "double";
  min: number;
  max: number;
  nullable?: number;
};
type StringSpec = { kind: "string"; length: number; nullable?: number };
type DateSpec =
  | { kind: "date"; start: string; end: string; nullable?: number }
  | { kind: "timestamp"; start: string; end: string; nullable?: number };
export type EnumSpec<T extends "string" | "int" | "bigint"> = {
  kind: "enum";
  base: T;
  values: (T extends "string" ? string : number)[];
  nullable?: number;
};
type ArraySpec = {
  kind: "array";
  element: Exclude<FieldSpec, ArraySpec>;
  minLen: number;
  maxLen: number;
  nullable?: number;
};
export type FieldSpec =
  | NumberSpec
  | StringSpec
  | DateSpec
  | EnumSpec<any>
  | ArraySpec;

export type TableConfig = {
  catalog: string;
  schema: string;
  tableBase: string; // base name used for variants, e.g. "events"
  format?: "PARQUET" | "ORC" | "AVRO";
  partitioning?: string[];
  tableProperties?: Record<string, string | number | boolean>;
  columns: Record<string, FieldSpec>;
  idColumn?: string;
};

export type TrinoConfig = {
  host: string;
  port: number;
  catalog: string;
  schema: string;
  user: string;
  source?: string;
  basicAuth?: { username: string; password: string };
};

export type SizeRow = {
  table_name: string;
  codec: string;
  level: number;
  rows: number;
  data_bytes: number;
  bytes_per_row: number;
  manifest_bytes?: number;
  total_bytes?: number;
};
