import fs from "node:fs";
import path from "node:path";
import { ChartJSNodeCanvas } from "chartjs-node-canvas";

const ROOT = process.cwd();
const outDir = path.join(ROOT, "assets");
const dataPath = path.join(ROOT, "bench/results.json");

type Row = {
  label: string; strategy: string; mean: number; p95: number;
};

const { results } = JSON.parse(fs.readFileSync(dataPath, "utf8")) as { results: Row[] };

const width = 1000;
const height = 500;
const chart = new ChartJSNodeCanvas({ width, height, type: 'svg' });

const labels = results.map(r => r.label);
const meanSeries = results.map(r => r.mean);
const p95Series  = results.map(r => r.p95);

const cfg = {
  type: "bar",
  data: {
    labels,
    datasets: [
      { label: "Mean (ms)", data: meanSeries },
      { label: "P95 (ms)",  data: p95Series }
    ]
  },
  options: {
    responsive: false,
    plugins: {
      title: {
        display: true,
        text: "Benchmark results (lower is better)"
      },
      legend: { position: "top" }
    },
    scales: {
      y: { beginAtZero: true, title: { display: true, text: "Milliseconds" } }
    }
  }
} as const;

fs.mkdirSync(outDir, { recursive: true });
const svg = chart.renderToBufferSync(cfg as any, 'image/svg+xml');
fs.writeFileSync("assets/benchmarks.svg", svg);
console.log("Wrote assets/benchmarks.png");
