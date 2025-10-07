import * as fs from "node:fs";

export function ensureDir(p: string) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

export function humanSize(bytes: number | null | undefined): string {
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

export const humanNumber = (num: number | null | undefined): string =>
  num?.toLocaleString("en-US").replaceAll(",", "_") ?? "";

export function makeBatches(
  startId: number,
  totalRows: number,
  batchRows: number
): { index: number; start: number; end: number }[] {
  if (batchRows <= 0) throw new Error("batchRows must be > 0");
  const out: { index: number; start: number; end: number }[] = [];
  const cnt = Math.ceil(totalRows / batchRows);
  for (let i = 0; i < cnt; i++) {
    const start = startId + i * batchRows;
    const end = Math.min(start + batchRows - 1, startId + totalRows - 1);
    out.push({ index: i, start, end });
  }
  return out;
}

// Simple seeded PRNG (Mulberry32)
function mulberry32(seed: number = 1) {
  return function () {
    let t = (seed += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Function to generate one deterministic random string
function randomString(rng: () => number, length: number): string {
  const chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let result = "";
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(rng() * chars.length));
  }
  return result;
}

// Generate an array of deterministic random strings
export function generateRandomStrings(
  count: number,
  length: number,
  seed: number = 1
): string[] {
  const rng = mulberry32(seed);
  return Array.from({ length: count }, () => randomString(rng, length));
}
