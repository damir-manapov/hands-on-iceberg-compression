export class Limiter {
  private active = 0;
  private q: (() => void)[] = [];
  constructor(private n: number) {}
  async run<T>(fn: () => Promise<T>) {
    if (this.active >= this.n) await new Promise<void>(r => this.q.push(r));
    this.active++;
    try {
      return await fn();
    } finally {
      this.active--;
      const nxt = this.q.shift();
      if (nxt) nxt();
    }
  }
}
