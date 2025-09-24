// Load plan
export const LOAD = {
  startId: Number(process.env.START_ID ?? "1"),
  totalRows: Number(process.env.TOTAL_ROWS ?? `${100_000_000}`),   // e.g., 10M
  // totalRows: Number(process.env.TOTAL_ROWS ?? `${10_000_000}`),   // e.g., 10M
  // totalRows: Number(process.env.TOTAL_ROWS ?? `${20_000_000}`),   // e.g., 10M
  // totalRows: Number(process.env.TOTAL_ROWS ?? `${1_000_000}`),   // e.g., 10M
  batchRows: Number(process.env.BATCH_ROWS ?? `${100_000}`),    // e.g., 5M per insert
  concurrency: Number(process.env.CONCURRENCY ?? "4"),
  createBaseSchema: (process.env.CREATE_BASE_SCHEMA ?? "true") === "true",
  compactAfterLoad: (process.env.COMPACT_AFTER_LOAD ?? "true") === "true",
  checkpointDir: process.env.CHECKPOINT_DIR ?? ".checkpoints",
  includeManifestBytes: (process.env.MEASURE_INCLUDE_MANIFESTS ?? "true") === "true",
  resultsCsv: process.env.RESULTS_CSV ?? "results_sizes.csv",
};

// 1ml -> 300mb
// 10ml -> 3gb
// 100ml -> 30gb
// milliard -> 300gb
