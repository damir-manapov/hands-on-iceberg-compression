// make-charts.ts
import fs from "node:fs";
import path from "node:path";
import { parse } from "csv-parse/sync";

// Chart.js v4 + node-canvas wrapper
import {
  Chart,
  BarController,
  BarElement,
  CategoryScale,
  LinearScale,
  Legend,
  Title,
  Tooltip,
} from "chart.js";
import { ChartJSNodeCanvas } from "chartjs-node-canvas";

// Register the pieces we use
Chart.register(
  BarController,
  BarElement,
  CategoryScale,
  LinearScale,
  Legend,
  Title,
  Tooltip
);

const ROOT = process.cwd();
const inCsv = path.join(ROOT, "results_sizes.csv"); // adjust if needed
const outDir = path.join(ROOT, "assets");
fs.mkdirSync(outDir, { recursive: true });

// ---- CSV types & helpers ----
type Rec = {
  table_name: string;
  codec: string;
  level: string | number;
  rows?: string | number;
  data_bytes?: string | number;
  bytes_per_row?: string | number;
  total_bytes?: string | number; // may be missing if you didn’t include manifests
};

const toNum = (v: any): number => {
  if (v === null || v === undefined || v === "") return NaN;
  if (typeof v === "number") return v;
  // strip quotes/commas if present
  return Number(String(v).replace(/,/g, "").trim());
};

// ---- Load CSV ----
const csvText = fs.readFileSync(inCsv, "utf8");
const rows = parse(csvText, {
  columns: true,
  skip_empty_lines: true,
}) as Rec[];

if (rows.length === 0) {
  console.error("No rows found in results_sizes.csv");
  process.exit(1);
}

// Normalize/parse
const data = rows.map(r => ({
  table: r.table_name,
  codec: r.codec,
  level: String(r.level ?? ""),
  rows: toNum(r.rows),
  dataBytes: toNum(r.total_bytes ?? r.data_bytes), // prefer total_bytes if present
  bpr: toNum(r.bytes_per_row),
}));

// Sort by codec then level (numeric where possible)
data.sort((a, b) => {
  if (a.codec !== b.codec) return a.codec.localeCompare(b.codec);
  const al = Number(a.level),
    bl = Number(b.level);
  if (!Number.isNaN(al) && !Number.isNaN(bl)) return al - bl;
  return String(a.level).localeCompare(String(b.level));
});

// Labels look like: zstd:l06 (events_zstd_l06)
const labels = data.map(
  d => `${d.codec}:${d.level.toString().padStart(2, "0")}`
);

// Build datasets grouped by codec (nice for side-by-side comparison)
const codecs = Array.from(new Set(data.map(d => d.codec)));

const seriesBPR = codecs.map(codec => {
  const vals = data.filter(d => d.codec === codec).map(d => d.bpr);
  return { label: `${codec} bytes/row`, data: vals };
});

const seriesBytes = codecs.map(codec => {
  const vals = data.filter(d => d.codec === codec).map(d => d.dataBytes);
  return { label: `${codec} total bytes`, data: vals };
});

// Chart settings
const width = 1200;
const height = 600;
const chart = new ChartJSNodeCanvas({ width, height, type: "svg" });

// Reusable render
async function renderBar(
  filename: string,
  title: string,
  yTitle: string,
  datasets: { label: string; data: (number | null | undefined)[] }[],
  logScale = false
) {
  const cfg = {
    type: "bar" as const,
    data: {
      labels,
      datasets: datasets.map(ds => ({
        label: ds.label,
        data: ds.data,
      })),
    },
    options: {
      responsive: false,
      plugins: {
        title: { display: true, text: title },
        legend: { position: "top" as const },
        tooltip: {
          callbacks: {
            label: (ctx: any) => {
              const v = ctx.parsed.y;
              if (v == null || Number.isNaN(v))
                return `${ctx.dataset.label}: n/a`;
              if (yTitle.toLowerCase().includes("bytes"))
                return `${ctx.dataset.label}: ${humanSize(v)}`;
              return `${ctx.dataset.label}: ${prettyNumber(v)}`;
            },
          },
        },
      },
      scales: {
        x: { ticks: { autoSkip: false, maxRotation: 60, minRotation: 30 } },
        y: {
          beginAtZero: true,
          type: logScale ? ("logarithmic" as const) : ("linear" as const),
          title: { display: true, text: yTitle },
        },
      },
    },
  };

  const svg = chart.renderToBufferSync(cfg as any, "image/svg+xml");
  const full = path.join(outDir, filename);
  console.log(full);
  fs.writeFileSync(full, svg);
  console.log(`Wrote ${path.relative(ROOT, full)}`);
}

// Pretty helpers (same style you asked for earlier)
function prettyNumber(n: number): string {
  if (n == null || Number.isNaN(n)) return "n/a";
  return n.toLocaleString("en-US");
}
function humanSize(bytes: number): string {
  if (bytes == null || Number.isNaN(bytes)) return "n/a";
  if (bytes === 0) return "0 B";
  const thresh = 1024;
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let u = 0;
  let b = bytes;
  while (b >= thresh && u < units.length - 1) {
    b /= thresh;
    u++;
  }
  return `${b.toFixed(1)} ${units[u]}`;
}

// Render charts
(async () => {
  // 1) Bytes per row (linear)
  await renderBar(
    "bytes_per_row.svg",
    "Bytes per Row (lower is better)",
    "Bytes per row",
    seriesBPR,
    false
  );

  // 2) Total data bytes (log scale helps if you compare gzip vs zstd at different levels)
  await renderBar(
    "total_data_bytes.svg",
    "Total Data Bytes per Table",
    "Bytes (log)",
    seriesBytes,
    true
  );
})();
