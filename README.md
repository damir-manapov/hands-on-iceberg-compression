# Hands on iceberg compression codecs and their compression levels through Trino

Project compares storage size with different copmression codecs and levels, also it gives you ability to play with trino.

It cotains docker compose to get all up and runnig, generates charts and gives you ability to define shema with stings, numbers, dates and random elements from an array.

* Start environment `yarn compose:up`
* Define compression codecs and their respective compression levels: `src/config/codecs.ts`
* Define your schema: `src/config/tableConfig.ts`
* Run bench: `yarn bench` or `yarn rebench` if you would like to start from scratch deleting checkpoints
* Generate charts (uses svg to not bloat git with binary data): `yarn plot`
* Lint code: `yarn lint` (TypeScript type checking) or `yarn lint:fix` (ESLint with auto-fix)
* Stop environment with volumes deletion: `yarn compose:clean`
* Clear `.checkpoints` if you want to rerun generation: `yarn rmcheckpoints`
* Del environments and restart from scratch: `yarn compose:clean && yarn compose:up && sleep 5 && yarn rebench`

## Docker Compose via yarn scripts

You can manage the stack using convenient yarn scripts (wrapping `docker compose`):

* Start: `yarn compose:up`
* Stop: `yarn compose:down`
* Restart: `yarn compose:restart`
* Logs (follow): `yarn compose:logs`
* Status: `yarn compose:ps`
* Clean teardown (remove volumes/orphans): `yarn compose:clean`
* Execute in container: `yarn compose:exec -it trino trino`
* Base command: `yarn compose:base` (for custom docker compose commands)

# Start environment
```sh
yarn compose:up
```

## Dashboards / endpoints

* Trino: http://localhost:8080
* MinIO console: http://localhost:9001 (user/pass from env)

# Working with iceberg through Trino
* Enter trino console in container: `yarn compose:exec -it trino trino`
* Check Trino works: `SELECT 1;`
* Check Trino node version: `SELECT node_version FROM system.runtime.nodes;`
* `SHOW CATALOGS;`
* `CREATE SCHEMA IF NOT EXISTS iceberg.experiment_schema;`
* `SHOW SCHEMAS FROM iceberg;`
* `CREATE TABLE IF NOT EXISTS iceberg.experiment_schema.experiment_table (id bigint);`
* `SHOW TABLES FROM iceberg.experiment_schema;`
* `SHOW TABLES FROM iceberg.lab;`
* `SHOW CREATE TABLE iceberg.experiment_schema.experiment_table;`
* `INSERT INTO iceberg.experiment_schema.experiment_table VALUES (1);`
* `SELECT * FROM iceberg.experiment_schema.experiment_table;`
* Set simple property: `ALTER TABLE iceberg.experiment_schema.experiment_table SET PROPERTIES format = 'PARQUET';`
* Set compression-codec property through extra_properties: `ALTER TABLE iceberg.experiment_schema.experiment_table SET PROPERTIES extra_properties = map_from_entries(ARRAY[ROW('write.parquet.compression-codec', 'zstd')]);`
* Set compression-level property through extra_properties: `ALTER TABLE iceberg.experiment_schema.experiment_table SET PROPERTIES extra_properties = map_from_entries(ARRAY[ROW('write.parquet.compression-level', '9')]);`
* Show properties of a table: `SELECT * FROM iceberg.experiment_schema."experiment_table$properties";`
* Create table with compression props set through extra_properties `CREATE TABLE IF NOT EXISTS iceberg.experiment_schema.experiment_table_2 (id bigint) WITH (extra_properties = map_from_entries(ARRAY[ROW('write.parquet.compression-codec', 'zstd'), ROW('write.parquet.compression-level', '9')]));`
* Show table props: `SELECT * FROM iceberg.experiment_schema."experiment_table$properties";`
* Show table props: `SELECT * FROM iceberg.experiment_schema."experiment_table_2$properties";`
* Show table props: `SELECT * FROM iceberg.lab."events_zstd_l01$properties";`
* Optimize: `CALL iceberg.system.optimize('lab.events_zstd_l06');`
ALTER TABLE iceberg.lab.events_zstd_l06 EXECUTE optimize;

## Troubleshooting

* Down with volume remove: `yarn compose:clean`
* All logs: `yarn compose:logs`
* Service logs: `yarn compose:base logs [service]`
* Last 10 log lines: `yarn compose:base logs [service] | tail -10`
* Logs of the container: `yarn compose:base logs trino`
* Force recreate container: `yarn compose:up --force-recreate trino`
* Check iceberg.properties inside Trino: `yarn compose:exec -it trino sh -lc 'grep -n "allowed-extra" -n /etc/trino/catalog/iceberg.properties; echo; cat /etc/trino/catalog/iceberg.properties'`
* Check Trino session catalog: `yarn compose:exec -it trino trino --execute "SHOW CATALOGS"`
* Show tables in iceberg schema: `yarn compose:exec -it trino trino --execute "SHOW TABLES FROM iceberg.lab"`
* Try to create trino iceberg catalog manually with compression: ```
yarn compose:exec -it trino trino --execute "
CREATE SCHEMA IF NOT EXISTS iceberg.lab;
CREATE TABLE IF NOT EXISTS iceberg.lab._prop_test (id bigint)
WITH (
  \"write.parquet.compression-codec\" = 'zstd'
)"```
* Check Trino node version: `yarn compose:exec -it trino trino --execute "SELECT node_version FROM system.runtime.nodes"`
* Check table properties: `yarn compose:exec -it trino trino --execute "SHOW CREATE TABLE iceberg.lab.events_base"`

WITH (partitioning = ARRAY['month(order_date)', 'bucket(account_number, 10)', 'country']);
WITH (sorted_by = ARRAY['order_date']);

# Development

* Type check: `yarn lint`
* Lint and fix: `yarn lint:fix`
* Format code: `yarn format`
* Check formatting: `yarn format:check`

# Benchmark

* Run: `yarn bench`
* Generate charts: `yarn plot`

![bytes_per_row](./assets/bytes_per_row.svg)
![total_data_bytes](./assets/total_data_bytes.svg)

## Algorithms
You need [Mermaid Preview](https://marketplace.visualstudio.com/items?itemName=vstirbu.vscode-mermaid-preview) extension to see gparhs in [vsc](https://code.visualstudio.com/)

### Example graph
```mermaid
graph LR
    A[Square Rect] -- Link text --> B((Circle))
    A --> C(Round Rect)
    B --> D{Rhombus}
    C --> D
```
