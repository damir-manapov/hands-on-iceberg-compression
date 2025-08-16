export function runExperiment(size: number, strategy: "A" | "B") {
  // TODO: replace with your real logic
  let acc = 0;
  if (strategy === "A") {
    for (let i = 0; i < size; i++) acc += (i ^ (i >>> 3)) & 0xff;
  } else {
    for (let i = 0; i < size; i++) acc += (i * 2654435761) >>> 0;
  }
  return acc;
}
