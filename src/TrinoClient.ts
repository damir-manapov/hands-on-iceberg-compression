import { TrinoConfig } from "./types";

export class TrinoClient {
  constructor(private cfg: TrinoConfig) {}
  private baseUrl() {
    return `${this.cfg.host}:${this.cfg.port}`;
  }
  private headers(): Record<string, string> {
    const h: Record<string, string> = {
      "X-Trino-User": this.cfg.user,
      "X-Trino-Catalog": this.cfg.catalog,
      "X-Trino-Schema": this.cfg.schema,
    };
    if (this.cfg.source) h["X-Trino-Source"] = this.cfg.source;
    if (this.cfg.basicAuth) {
      const { username, password } = this.cfg.basicAuth;
      h["Authorization"] =
        `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`;
    }
    return h;
  }
  async execute(sql: string): Promise<void> {
    await this._run(sql, false);
  }
  async query<T = any>(sql: string): Promise<T[]> {
    const all = await this._run(sql, true);
    return all as T[];
  }
  private async _run<T = any>(sql: string, collect: boolean): Promise<T[]> {
    const res = await fetch(`${this.baseUrl()}/v1/statement`, {
      method: "POST",
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
        ...this.headers(),
      },
      body: sql,
    });
    if (!res.ok)
      throw new Error(`Trino POST ${res.status}: ${await res.text()}`);
    let payload: any = await res.json();
    let colNames: string[] | null =
      payload.columns?.map((c: any) => c.name) ?? null;
    const rowsArr: any[][] = [];

    const take = (p: any) => {
      if (collect) {
        // (1) learn columns as soon as they show up
        if (!colNames && p.columns) {
          colNames = p.columns.map((c: any) => c.name);
        }
        // (2) collect any data page we get
        if (p.data && Array.isArray(p.data)) {
          for (const r of p.data) rowsArr.push(r);
        }
      }
    };
    take(payload);

    while (payload.nextUri) {
      if (payload.error)
        throw new Error(`Trino error: ${payload.error.message}`);
      const poll = await fetch(payload.nextUri, { headers: this.headers() });
      if (!poll.ok)
        throw new Error(`Trino poll ${poll.status}: ${await poll.text()}`);
      payload = await poll.json();
      if (!colNames && payload.columns) {
        // late columns; extremely rare
      }
      take(payload);
      await new Promise(r => setTimeout(r, 150));
    }

    // Map arrays → objects if we have column names; else return arrays
    if (colNames) {
      return rowsArr.map(r => {
        const obj: any = {};
        if (colNames) {
          for (let i = 0; i < colNames.length; i++) obj[colNames[i]] = r[i];
        }
        return obj as T;
      });
    } else {
      // No columns (e.g., for some non-SELECT statements) — return empty
      return [] as T[];
    }
  }
}
