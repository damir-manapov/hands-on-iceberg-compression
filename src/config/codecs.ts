// Which codecs & levels to test
// Use [] for levels if the codec ignores levels (e.g., snappy); script still creates a single table with level "00".
export const CODECS: Array<{
  codec: "zstd" | "gzip" | "snappy" | "lz4";
  levels: number[];
}> = [
  { codec: "zstd", levels: [6] },
  // { codec: "zstd",   levels: [1, 3, 6, 9] },
  // { codec: "gzip",   levels: [1, 6, 9] },
  // { codec: "snappy", levels: [0] },    // no levels; keep 0 as placeholder
  // // { codec: "lz4", levels: [0] },    // enable if your build supports LZ4
];
