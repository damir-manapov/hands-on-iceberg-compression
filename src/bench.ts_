import fs from "node:fs";
import path from "node:path";
import { runExperiment } from "./measure";

type Case = { label: string; size: number; strategy: "A" | "B" };
type Config = { cases: Case[]; warmupIters: number; measureIters: number };

const ROOT = process.cwd();
const cfg: Config = JSON.parse(fs.readFileSync(path.join(ROOT, "bench/config.json"), "utf8"));

function timeOne(fn: () => void): number {
  const t0 = process.hrtime.bigint();
  fn();
  const t1 = process.hrtime.bigint();
  return Number(t1 - t0) / 1e6; // ms
}

function stats(samples: number[]) {
  const mean = samples.reduce((a, b) => a + b, 0) / samples.length;
  const sorted = [...samples].sort((a, b) => a - b);
  const p95 = sorted[Math.floor(0.95 * (sorted.length - 1))];
  return { mean, p95, min: sorted[0], max: sorted[sorted.length - 1] };
}

const results: Record<string, any>[] = [];
for (const c of cfg.cases) {
  // warmup
  for (let i = 0; i < cfg.warmupIters; i++) runExperiment(c.size, c.strategy);

  const samples: number[] = [];
  for (let i = 0; i < cfg.measureIters; i++) {
    const ms = timeOne(() => runExperiment(c.size, c.strategy));
    samples.push(ms);
  }
  const s = stats(samples);
  results.push({
    label: c.label,
    size: c.size,
    strategy: c.strategy,
    samples,
    ...s
  });
}

fs.mkdirSync(path.join(ROOT, "bench"), { recursive: true });
fs.writeFileSync(path.join(ROOT, "bench/results.json"), JSON.stringify({ results, meta: { date: new Date().toISOString() } }, null, 2));
console.log("Wrote bench/results.json");
