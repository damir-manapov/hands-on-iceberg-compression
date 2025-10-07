// Load plan
export const LOAD = {
  startId: Number(process.env.START_ID ?? "1"),
  // totalRows: Number(process.env.TOTAL_ROWS ?? `${1_000_000_000}`),   // e.g., 10M
  totalRows: Number(process.env.TOTAL_ROWS ?? `${100_000_000}`),
  // totalRows: Number(process.env.TOTAL_ROWS ?? `${10_000_000}`),
  // totalRows: Number(process.env.TOTAL_ROWS ?? `${1_000_000}`),
  // totalRows: Number(process.env.TOTAL_ROWS ?? `${100_000}`),
  batchRows: Number(process.env.BATCH_ROWS ?? `${100_000}`), // e.g., 5M per insert
  // batchRows: Number(process.env.BATCH_ROWS ?? `${1_000_000}`), // e.g., 5M per insert
  concurrency: Number(process.env.CONCURRENCY ?? "4"),
  createBaseSchema: (process.env.CREATE_BASE_SCHEMA ?? "true") === "true",
  compactAfterLoad: (process.env.COMPACT_AFTER_LOAD ?? "true") === "true",
  checkpointDir: process.env.CHECKPOINT_DIR ?? ".checkpoints",
  includeManifestBytes:
    (process.env.MEASURE_INCLUDE_MANIFESTS ?? "true") === "true",
  resultsCsv: process.env.RESULTS_CSV ?? "results_sizes.csv",
};

// wide table:
// 1ml -> 300mb, 1.1min
// 10ml -> 3gb, 7min
// 100ml -> 30gb, 50min
// billion -> 300gb, ~10h

// narrow table:
// 1ml -> 30mb, 6sec
// 10ml -> 300mb, 40sec
// 100ml -> 2.9gb, 2.7min
// billion -> 29gb, 26min
