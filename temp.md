
Enter trino cli: `docker compose exec -it trino trino`

Investigate schema:
```sql
SELECT 1;
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.lab;
SELECT COUNT(*) FROM iceberg.lab."events_zstd_l06";
DESCRIBE iceberg.lab."events_zstd_l06";
SHOW COLUMNS FROM iceberg.lab."events_zstd_l06";
```

examples of rows:
```
row_1 timestamp(6): 2024-10-08 08:48:24.000000, 2024-03-19 03:16:10.000000, 2024-01-08 04:15:21.000000
row_3 varchar: d020edf, c8630ca, 7845ded
row_5 varchar: e, 2, d, 3, b
row_16 nullabe integer: 46, NULL, 72
row_32 varchar: 117e80a49bb84d4eb197e1593020c94d, 6f0fa550ccb74c4dbf01e7dd1df0d837, ebafbcd2566a48eaaf7e1261acb96a4c
```

### Environment

Docker compose on laptop (12 cores, 32 RAM)

### How to read stats from trinoc cli

Example: `2.21 [100M rows, 2.33GiB] [45.2M rows/s, 1.06GiB/s]`

* `2.21` → total elapsed time (seconds).
* `[100M rows, 2.33GiB]` → query processed 100 million rows, ~2.33 GiB of uncompressed data.
* `[45.2M rows/s, 1.06GiB/s]` → throughput: ~45.2 million rows/sec, 1.06 GiB/sec.

### Requests and their results

```sql
SELECT * FROM iceberg.lab."events_zstd_l06" LIMIT 10;

-- 7 letters text (varchar), not nullable
SELECT COUNT(*) FROM iceberg.lab."events_zstd_l06" WHERE row_3 = '433b0fa';
10m: 1 - 0.36 [10M rows, 47.4MiB] [27.9M rows/s, 132MiB/s]
100m: 0 - 2.02 [100M rows, 475MiB] [49.4M rows/s, 235MiB/s]

SELECT * FROM iceberg.lab."events_zstd_l06" WHERE row_3 = 'd020edf' LIMIT 10;
10m: 0.39 [10M rows, 273MiB] [25.6M rows/s, 699MiB/s]
100m: 1.44 [100M rows, 2.3GiB] [69.5M rows/s, 1.6GiB/s]

-- 1 letters text (varchar), not nullable
SELECT COUNT(*) FROM iceberg.lab."events_zstd_l06" WHERE row_5 = 'e';
10m: 623666 - 0.28 [10M rows, 8.28MiB] [35.7M rows/s, 29.6MiB/s]
100m: 6247324 - 0.67 [100M rows, 83.7MiB] [149M rows/s, 124MiB/s]

SELECT * FROM iceberg.lab."events_zstd_l06" WHERE row_5 = 'e' LIMIT 10;
10m: 1.18 [103K rows, 914MiB] [87.2K rows/s, 777MiB/s]
100m: 1.10 [219K rows, 1.97GiB] [199K rows/s, 1.79GiB/s]

-- date (timestamp(6)), not nullable

-- eq
SELECT COUNT(*) FROM iceberg.lab."events_zstd_l06" WHERE row_1 = TIMESTAMP '2024-11-04 22:30:13.000000';
10m: 1 rows - 0.36 [10M rows, 64MiB] [27.5M rows/s, 176MiB/s]
100m: 3 rows - 0.80 [100M rows, 641MiB] [124M rows/s, 797MiB/s]

SELECT * FROM iceberg.lab."events_zstd_l06" WHERE row_1 = TIMESTAMP '2024-11-04 22:30:13.000000' LIMIT 10;
10m: 0.46 [10M rows, 273MiB] [21.7M rows/s, 594MiB/s]
100m: 1.30 [100M rows, 2.59GiB] [77.2M rows/s, 2GiB/s]

-- gt
SELECT COUNT(*) FROM iceberg.lab."events_zstd_l06" WHERE row_1 > TIMESTAMP '2024-11-01';
10m: 1666914 - 0.36 [10M rows, 64MiB] [28M rows/s, 179MiB/s]
100m: 0.93 [100M rows, 641MiB] [107M rows/s, 686MiB/s]

-- gt lt
SELECT COUNT(*) FROM iceberg.lab."events_zstd_l06" WHERE row_1 > TIMESTAMP '2024-11-01' AND row_1 < TIMESTAMP '2024-11-20';
10m: 518893 rows - 0.31 [10M rows, 64MiB] [32.8M rows/s, 210MiB/s]
100m: 5192952 rows - 0.86 [100M rows, 641MiB] [116M rows/s, 746MiB/s]

-- between
SELECT COUNT(*) FROM iceberg.lab."events_zstd_l06" WHERE row_1 BETWEEN TIMESTAMP '2024-11-01' AND TIMESTAMP '2024-11-20';
100m: 5192960 rows - 0.86 [100M rows, 641MiB] [116M rows/s, 744MiB/s]

-- text (varchar)

-- like
SELECT COUNT(*) FROM iceberg.lab."events_zstd_l06" WHERE row_32 LIKE '%50ccb74%';
100m: 5 rows - 3.37 [100M rows, 1.74GiB] [29.7M rows/s, 530MiB/s]

SELECT COUNT(*) FROM iceberg.lab."events_zstd_l06" WHERE row_32 LIKE '%50ccb%';
100m: 1850 rows - 2.49 [100M rows, 1.74GiB] [40.1M rows/s, 716MiB/s]

SELECT COUNT(*) FROM iceberg.lab."events_zstd_l06" WHERE row_32 LIKE '%50c%';
100m: 585805 rows - 2.66 [100M rows, 1.74GiB] [37.6M rows/s, 670MiB/s]

-- compositions

-- like and between
SELECT COUNT(*) FROM iceberg.lab."events_zstd_l06" WHERE row_32 LIKE '%50c%' AND row_1 BETWEEN TIMESTAMP '2024-11-01' AND TIMESTAMP '2024-11-20';
100m: 30502 rows - 2.21 [100M rows, 2.33GiB] [45.2M rows/s, 1.06GiB/s]


-- like, between and gt on int
SELECT COUNT(*) FROM iceberg.lab."events_zstd_l06" WHERE row_32 LIKE '%50c%' AND row_1 BETWEEN TIMESTAMP '2024-11-01' AND TIMESTAMP '2024-11-20' AND row_16 < 80;
100m: 20861 rows - 2.46 [100M rows, 2.42GiB] [40.7M rows/s, 1006MiB/s]
```

# Selects under constraints on cpu / mem

Let's try to constrain trino and perform requests. We will restart containers before each batch so cache won't help, every batch will start cold.

Request: `SELECT COUNT(*) FROM iceberg.lab."events_zstd_l06" WHERE row_32 LIKE '%50c%' AND row_1 BETWEEN TIMESTAMP '2024-11-01' AND TIMESTAMP '2024-11-20' AND row_16 < 80;`

Environment: `Docker compose on laptop (12 cores, 32 RAM)`

```sh
for m in 2 4 8 12; do
  docker compose down --remove-orphans
  docker compose up -d
  docker update --cpus=$m $(docker compose ps -q trino)
  sleep 5
  echo ">>> Cpu: "$m""
  alias query="docker compose exec -it trino trino --progress --execute \"SELECT COUNT(*) FROM iceberg.lab.\"events_zstd_l06\" WHERE row_32 LIKE '%50c%' AND row_1 BETWEEN TIMESTAMP '2024-11-01' AND TIMESTAMP '2024-11-20' AND row_16 < 80;\""
  query
  query
  query
  sleep 5
done
```

## Results

### mem_limit: 2g

total mem usage by container `~2GB`

params: `-Xms1G -Xmx1G query.max-memory-per-node=512MB query.max-memory=1GB query.max-total-memory=1GB memory.heap-headroom-per-node=512MB`

```
>>> Cpu: 2
"20861" 1:53 [100M rows, 2.42GiB] [883K rows/s, 21.9MiB/s]
"20861" 58.91 [100M rows, 2.42GiB] [1.7M rows/s, 42MiB/s]
"20861" 51.07 [100M rows, 2.42GiB] [1.96M rows/s, 48.5MiB/s]

>>> Cpu: 4
"20861" 53.71 [100M rows, 2.42GiB] [1.86M rows/s, 46.1MiB/s]
"20861" 31.49 [100M rows, 2.42GiB] [3.18M rows/s, 78.6MiB/s]
"20861" 26.14 [100M rows, 2.42GiB] [3.83M rows/s, 94.7MiB/s]

>>> Cpu: 8
"20861" 19.80 [100M rows, 2.42GiB] [5.05M rows/s, 125MiB/s]
"20861" 8.31 [100M rows, 2.42GiB] [12M rows/s, 298MiB/s]
"20861" 7.15 [100M rows, 2.42GiB] [14M rows/s, 346MiB/s]

>>> Cpu: 12
"20861" 16.08 [100M rows, 2.42GiB] [6.22M rows/s, 154MiB/s]
"20861" 7.02 [100M rows, 2.42GiB] [14.2M rows/s, 352MiB/s]
"20861" 6.49 [100M rows, 2.42GiB] [15.4M rows/s, 381MiB/s]
```

### mem_limit: 4g

total mem usage by container `~4.6GB`

params: `-Xms4G -Xmx4G query.max-memory-per-node=3GB query.max-memory=4GB query.max-total-memory=4GB memory.heap-headroom-per-node=1GB`

```
>>> Cpu: 2
"20861" 1:29 [100M rows, 2.42GiB] [1.13M rows/s, 27.9MiB/s]
"20861" 52.48 [100M rows, 2.42GiB] [1.91M rows/s, 47.2MiB/s]
"20861" 45.42 [100M rows, 2.42GiB] [2.2M rows/s, 54.5MiB/s]

>>> Cpu: 4
"20861" 43.77 [100M rows, 2.42GiB] [2.28M rows/s, 56.5MiB/s]
"20861" 23.80 [100M rows, 2.42GiB] [4.2M rows/s, 104MiB/s]
"20861" 21.96 [100M rows, 2.42GiB] [4.55M rows/s, 113MiB/s]

>>> Cpu: 8
"20861" 22.08 [100M rows, 2.42GiB] [4.53M rows/s, 112MiB/s]
"20861" 8.07 [100M rows, 2.42GiB] [12.4M rows/s, 307MiB/s]
"20861" 7.14 [100M rows, 2.42GiB] [14M rows/s, 347MiB/s]

>>> Cpu: 12
"20861" 13.97 [100M rows, 2.42GiB] [7.16M rows/s, 177MiB/s]
"20861" 5.51 [100M rows, 2.42GiB] [18.1M rows/s, 449MiB/s]
"20861" 4.73 [100M rows, 2.42GiB] [21.2M rows/s, 524MiB/s]
```

### no mem limit

total mem usage by container `~13.8GB`

```
>>> Cpu: 2
"20861" 1:31 [100M rows, 2.42GiB] [1.1M rows/s, 27.1MiB/s]
"20861" 56.29 [100M rows, 2.42GiB] [1.78M rows/s, 44MiB/s]
"20861" 39.05 [100M rows, 2.42GiB] [2.56M rows/s, 63.4MiB/s]

>>> Cpu: 4
"20861" 48.62 [100M rows, 2.42GiB] [2.06M rows/s, 50.9MiB/s]
"20861" 22.79 [100M rows, 2.42GiB] [4.39M rows/s, 109MiB/s]
"20861" 21.27 [100M rows, 2.42GiB] [4.7M rows/s, 116MiB/s]

>>> Cpu: 8
"20861" 12.87 [100M rows, 2.42GiB] [7.77M rows/s, 192MiB/s]
"20861" 5.01 [100M rows, 2.42GiB] [20M rows/s, 494MiB/s]
"20861" 4.76 [100M rows, 2.42GiB] [21M rows/s, 520MiB/s]

>>> Cpu: 12
"20861" 7.42 [100M rows, 2.42GiB] [13.5M rows/s, 334MiB/s]
"20861" 5.28 [100M rows, 2.42GiB] [19M rows/s, 469MiB/s]
"20861" 4.54 [100M rows, 2.42GiB] [22M rows/s, 545MiB/s]
```

## Conclusion
In this case there is no differemce between `4G limit` and `no limit` and almost no difference to `2G limit`. Guess we could see the difference if there is a few concurrent queries. At the same time cpu limit influences performance heavily




















 Column |     Type     | Extra | Comment
--------+--------------+-------+---------
 id     | bigint       |       |
 row_1  | timestamp(6) |       |
 row_2  | timestamp(6) |       |
 row_3  | varchar      |       |
 row_4  | varchar      |       |
 row_5  | varchar      |       |
 row_6  | varchar      |       |
 row_7  | varchar      |       |
 row_8  | varchar      |       |
 row_9  | integer      |       |
 row_10 | integer      |       |
 row_11 | integer      |       |
 row_12 | integer      |       |
 row_13 | integer      |       |
 row_14 | integer      |       |
 row_15 | integer      |       |
 row_16 | integer      |       |
 row_17 | integer      |       |
 row_18 | integer      |       |
 row_19 | integer      |       |
 row_20 | integer      |       |
 row_21 | integer      |       |
 row_22 | integer      |       |
 row_23 | integer      |       |
 row_24 | integer      |       |
 row_25 | integer      |       |
 row_26 | integer      |       |
 row_27 | integer      |       |
 row_28 | varchar      |       |
 row_29 | varchar      |       |
 row_30 | varchar      |       |
 row_31 | varchar      |       |
 row_32 | varchar      |       |
 row_33 | varchar      |       |
 row_34 | varchar      |       |