# Apache Iceberg Practice

Last session you saw what an embedded analytics engine (DuckDB) does for you on medium data. Today we add the missing piece: **a table format**. Same Parquet files on disk, but now with ACID, time travel, and schema evolution managed by **Apache Iceberg**.

Following the same "no JVM, no server" thread we picked up in session 11, we'll build everything in **PyIceberg**: the Iceberg client that runs in pure Python on top of PyArrow. No Spark, no catalog server, no Hadoop classpath. Just `pip install` and go. The very last exercise points at how the same warehouse opens up to a heavier engine (DuckDB) for free, which is the whole point of an open table format.

## Setup

```bash
cd 12-iceberg-lakehouse
docker compose up -d --build
```

Open your browser at **http://localhost:8888**.

---

## Data

We're reusing the **NYC Yellow Taxi 2024** dataset from session 11. Three months are enough for today; the goal is to write multiple snapshots and observe what happens, not to test scale.

```bash
cp ../11-duckdb-right-sizing/data/yellow_tripdata_2024-0[1-3].parquet ./data/
```

If you don't have those files yet, download them from the public TLC bucket:

```bash
mkdir -p data
for m in 01 02 03; do
    curl -L -o "data/yellow_tripdata_2024-${m}.parquet" \
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-${m}.parquet"
done
```

Inside the Jupyter container these live at `/app/data/yellow_tripdata_2024-MM.parquet`.

---

## Part 1: Iceberg Fundamentals (45 min)

### Exercise 1: A Catalog of Your Own (5 min)

The catalog is the one piece of state that turns a directory of Parquet files into a transactional table. It maps `table_name → current metadata.json`. Today we'll use the simplest catalog there is: a **SQLite database file** as the backing store, `pyiceberg`'s `SqlCatalog`. Same idea as a Hive metastore but with a one-line setup.

**Q1.** Create a Jupyter notebook under `/app/notebooks/`. In the first cell, build a fresh PyIceberg catalog under `/app/data/warehouse/`. List its namespaces (it should be empty), then create a namespace called `taxi`.

<details>
<summary>Hint</summary>

`pyiceberg.catalog.load_catalog(name, **props)`. The `type` is `"sql"`, the `uri` is a `sqlite:///` URI to a file inside the warehouse, and `warehouse` is a `file://` URL pointing at the directory where data and metadata will live. Both can be the same directory.

</details>

<details>
<summary>Answer</summary>

```python
import os, shutil

WAREHOUSE = "/app/data/warehouse"
shutil.rmtree(WAREHOUSE, ignore_errors=True)   # start clean each run
os.makedirs(WAREHOUSE)

from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "local",
    type="sql",
    uri=f"sqlite:///{WAREHOUSE}/catalog.db",
    warehouse=f"file://{WAREHOUSE}",
)

print("namespaces:", list(catalog.list_namespaces()))   # []
catalog.create_namespace("taxi")
print("namespaces:", list(catalog.list_namespaces()))   # [('taxi',)]
```

From the host you can see what was actually created:

```bash
ls data/warehouse/         # catalog.db
```

That `catalog.db` SQLite file **is** the catalog. Crack it open with stock `sqlite3` to see what's inside:

```python
import sqlite3
con = sqlite3.connect(f"{WAREHOUSE}/catalog.db")

# What tables does pyiceberg's SqlCatalog use?
print("tables in catalog.db:")
for (name,) in con.execute("SELECT name FROM sqlite_master WHERE type='table'"):
    print(f"  {name}")

# Inspect the namespace we just created
print("\niceberg_namespace_properties:")
for row in con.execute("SELECT * FROM iceberg_namespace_properties"):
    print(f"  {row}")

# iceberg_tables is empty for now (no tables yet) but here's its schema
print("\niceberg_tables columns:")
print(" ", [c[1] for c in con.execute("PRAGMA table_info(iceberg_tables)")])
```

You'll see two tables; `iceberg_tables` and `iceberg_namespace_properties`, and the schema of `iceberg_tables` is the punchline:

```
iceberg_tables columns: ['catalog_name', 'table_namespace', 'table_name',
                         'metadata_location', 'previous_metadata_location']
```

That `metadata_location` column is **the one piece of state the catalog tracks**: for each table, *where is the current metadata.json?* Everything else (snapshots, schemas, files) lives in object storage. After exercise 2 the row will show up; come back here and re-run the query to see it land.

The whole "central catalog service" is one SQLite file. In production the same role is played by AWS Glue, Hive Metastore, Polaris, or a REST endpoint, but the protocol is the same: *for each table, where is the current metadata.json?*

</details>

---

### Exercise 2: Your First Iceberg Table (10 min)

Iceberg infers its schema from a PyArrow schema. We can hand it the schema of an existing Parquet file and use **append** to land that file's data as the first snapshot.

**Q2.** Read January 2024 with PyArrow. Create an Iceberg table `taxi.trips` using the Parquet's schema. Append the data. Verify the row count by scanning the table back, and inspect the snapshot summary.

<details>
<summary>Hint</summary>

- `pyarrow.parquet.read_table(path)` gives you a `pa.Table`.
- `catalog.create_table(identifier, schema)` accepts a PyArrow schema directly.
- `table.append(pa_table)` writes new data files and commits a snapshot.
- `table.scan().to_arrow()` reads the table back as a PyArrow Table.
- `table.snapshots()` returns the list of snapshots; each has a `summary`.

</details>

<details>
<summary>Answer</summary>

```python
import pyarrow.parquet as pq

month1 = pq.read_table("/app/data/yellow_tripdata_2024-01.parquet")
print(f"month 1: {month1.num_rows:,} rows, {len(month1.schema)} columns")

table = catalog.create_table("taxi.trips", schema=month1.schema)
table.append(month1)

# read it back
back = table.scan().to_arrow()
print(f"in iceberg table: {back.num_rows:,} rows")

# inspect the snapshot
for s in table.snapshots():
    sm = s.summary
    print(f"  snapshot {s.snapshot_id}")
    print(f"    operation:    {sm.operation.value}")
    print(f"    added rows:   {sm.additional_properties['added-records']}")
    print(f"    added files:  {sm.additional_properties['added-data-files']}")
```

A typical run prints something like:

```
month 1: 2,964,624 rows, 19 columns
in iceberg table: 2,964,624 rows
  snapshot 7201650108055357207
    operation:    append
    added rows:   2964624
    added files:  1
```

So PyIceberg wrote **one** Parquet file (the data was small enough not to be split) and committed **one** snapshot. The snapshot doesn't contain the data; it's a small JSON entry that *points at* the data file. We'll see that in a minute.

</details>

---

### Exercise 3: A Second Snapshot (5 min)

ACID and time travel come from **commits**, not from individual writes. Every `append` produces a new snapshot, and the old snapshot stays around so you can still query the table as it looked before.

**Q3.** Append February's data. Use `table.inspect.history()` to view the full history.

<details>
<summary>Answer</summary>

```python
month2 = pq.read_table("/app/data/yellow_tripdata_2024-02.parquet")
table.append(month2)

print(f"row count now: {table.scan().to_arrow().num_rows:,}")

# inspect.history() returns a PyArrow Table ;  clean tabular form
print(table.inspect.history())
```

Expected output (timestamps and IDs will differ):

```
row count now: 5,972,150
pyarrow.Table
made_current_at: timestamp[ms] not null
snapshot_id: int64 not null
parent_id: int64
is_current_ancestor: bool not null
----
made_current_at:    [[2026-05-10 15:20:08, 2026-05-10 15:20:09]]
snapshot_id:        [[7201650108055357207, 9159361329493901782]]
parent_id:          [[null, 7201650108055357207]]
is_current_ancestor:[[true, true]]
```

Two snapshots, the second one's `parent_id` points at the first. That linked list is the table's history; both snapshots are valid points to query. We'll do exactly that in Exercise 8.

</details>

---

### Exercise 4: What Lives on Disk (10 min)

The most useful thing about a SqlCatalog with a `file://` warehouse is that the *entire table* lives in a directory you can browse with normal shell commands. No service to query, no API, just files.

**Q4.** From your host (or a Jupyter cell with `!`), run `tree /app/data/warehouse` and identify five things:

1. Where is `catalog.db`?
2. Where are the Parquet **data files**?
3. Where are the **`metadata.json`** files? How many are there and why?
4. Where are the **manifest list** files (`snap-*.avro`)?
5. Where are the **manifest** files (`*-m0.avro`)?

<details>
<summary>Answer</summary>

```sh
!tree /app/data/warehouse
```

A typical layout:

```
/app/data/warehouse
├── catalog.db
└── taxi
    └── trips
        ├── data
        │   ├── 00000-0-70704009-...parquet      ← snapshot 1's data
        │   └── 00000-0-abd8c0a9-...parquet      ← snapshot 2's data
        └── metadata
            ├── 00000-...metadata.json           ← table created (empty)
            ├── 00001-...metadata.json           ← after 1st append
            ├── 00002-...metadata.json           ← after 2nd append (current)
            ├── 70704009-...m0.avro              ← manifest for snapshot 1
            ├── abd8c0a9-...m0.avro              ← manifest for snapshot 2
            ├── snap-7201...avro                 ← manifest list for snapshot 1
            └── snap-9159...avro                 ← manifest list for snapshot 2
```

Why **three** `metadata.json` files for **two** appends? Because creating the table itself was a separate commit (the empty table, schema only). Every commit produces a new `metadata.json`; the catalog's only job is to remember which one is current. Old `metadata.json` files are kept and that's how time travel works.

The Parquet files are the same Parquet files you'd get from `df.write.parquet()` ;  Iceberg doesn't transform them. What makes the directory a *table* is the metadata: the manifest list says which manifests belong to which snapshot, the manifests say which data files belong to a manifest, and the `metadata.json` says which manifest list is current.

</details>

---

### Exercise 5: What Iceberg Tracks for You (8 min)

You don't have to crack open Avro files by hand to see what Iceberg knows. The `table.inspect` namespace exposes the metadata as PyArrow tables ;  same flavour as Spark's `SELECT * FROM table.snapshots`.

**Q5.** Print three inspection tables and explain what each one tells you:

1. `table.inspect.snapshots()` ;  every snapshot with its summary.
2. `table.inspect.files()` ;  every data file currently referenced.
3. `table.inspect.manifests()` ;  every manifest in the current snapshot.

<details>
<summary>Hint</summary>

The result of each `inspect.*()` call is a `pyarrow.Table`. Either print it directly or pick a few columns with `.select([...])` to keep the output readable.

</details>

<details>
<summary>Answer</summary>

```python
print("=== snapshots ===")
print(table.inspect.snapshots().select(
    ["committed_at", "snapshot_id", "parent_id", "operation", "summary"]
))

print("\n=== current data files ===")
print(table.inspect.files().select(
    ["file_path", "record_count", "file_size_in_bytes"]
).to_pylist())

print("\n=== manifests in the current snapshot ===")
print(table.inspect.manifests().select(
    ["path", "added_data_files_count", "existing_data_files_count"]
).to_pylist())
```

What you should see:

- **Two snapshots**, each with a summary including `added-records`, `added-data-files`, etc.
- **Two data files** in the current snapshot; month 1's file (carried forward from snapshot 1) plus month 2's new file.
- **Two manifests**; each append wrote its own manifest. As you append more data, manifests accumulate; the `rewrite_manifests` procedure (Spark-side) compacts them.

This is the same data you saw in `metadata.json` and the manifest Avros, just exposed as a query-able PyArrow Table. Useful for debugging ("which file holds this row?"), capacity planning ("how big is each manifest getting?"), and auditing ("when did this snapshot land?").

</details>

---

### Exercise 6: Predicate Pushdown via `row_filter` (7 min)

In session 11 you saw Parquet predicate pushdown via DuckDB's SQL engine. PyIceberg exposes the same mechanism directly through its scan API: `table.scan(row_filter=...)` pushes the filter into the file readers, which use the per-file min/max stats to skip data files entirely, and inside the surviving files the per-row-group stats to skip row groups.

**Q6.** Use `row_filter` to find trips with `fare_amount > 200`. Time it against an unfiltered scan that does the same filter in Python afterwards. The two should return the same number of rows; one should be much faster.

<details>
<summary>Hint</summary>

`row_filter` accepts a SQL-like string (`"fare_amount > 200"`). The result of `.to_arrow()` is a `pa.Table`; you can also chain `.to_pandas()` if you want a DataFrame.

</details>

<details>
<summary>Answer</summary>

```python
import time, pyarrow.compute as pc

# warm the page cache so we're comparing CPU/decode work, not disk I/O
table.scan().to_arrow()

# 1. row_filter only; pushdown the predicate, but read all 19 columns
t0 = time.perf_counter()
a = table.scan(row_filter="fare_amount > 200").to_arrow()
t1 = time.perf_counter() - t0

# 2. full scan, filter in Python afterwards
t0 = time.perf_counter()
full = table.scan().to_arrow()
b = full.filter(pc.greater(full["fare_amount"], 200))
t2 = time.perf_counter() - t0

# 3. column pruning only; read just one column, no row filter
t0 = time.perf_counter()
c = table.scan(selected_fields=("fare_amount",)).to_arrow()
t3 = time.perf_counter() - t0

# 4. column pruning + row_filter; both pushdowns stacked
t0 = time.perf_counter()
d = table.scan(row_filter="fare_amount > 200",
               selected_fields=("fare_amount",)).to_arrow()
t4 = time.perf_counter() - t0

print(f"row_filter only             : {t1:.3f}s  ({a.num_rows:,} rows × {len(a.schema)} cols)")
print(f"full scan + python filter   : {t2:.3f}s  ({b.num_rows:,} rows)")
print(f"selected_fields only        : {t3:.3f}s  ({c.num_rows:,} rows × {len(c.schema)} col)")
print(f"row_filter + selected_fields: {t4:.3f}s  ({d.num_rows:,} rows × {len(d.schema)} col)")
```

A typical run (numbers depend on cache state and machine; yours will differ):

```
row_filter only             : 0.20s  (~2,000 rows × 19 cols)
full scan + python filter   : 0.14s  (~2,000 rows)
selected_fields only        : 0.03s  (~6M rows × 1 col)
row_filter + selected_fields: 0.04s  (~2,000 rows × 1 col)
```

Two things may surprise you:

**1. `row_filter` alone is not dramatically faster than scan-then-filter-in-Python** on hot data. Why? `row_filter` mostly saves *I/O*; it skips Parquet row groups whose min/max stats say they can't match. When the table is already in the page cache, that I/O is essentially free, and Iceberg still decodes the 19 other columns whose values you didn't filter on. PyArrow's vectorised `pc.greater` on the in-memory Arrow table is fast enough to compete.

**2. `selected_fields` (column pruning) is the much bigger lever**; ~5× faster than a full scan. Parquet is columnar: reading 1 of 19 columns means touching ~1/19th of the bytes. That's a real, predictable win regardless of cache.

Stacking both is what you actually want in production: only read the columns the query needs, only read the row groups whose stats overlap the filter. The advantage you *don't* see clearly here is the one that dominates at scale: on a 1 TB table that doesn't fit in memory, `row_filter` will skip whole Parquet files based on manifest stats and never open them. On 6M rows hot in cache, the per-file overhead is hard to amortise.

**The lesson**: predicate pushdown is real, but it shows up most clearly when (a) the data is large or cold, (b) you also prune columns, and (c) the filter is selective enough to let the engine drop entire files via manifest stats. On medium, hot data, *column pruning is doing most of the work*; and that's true for plain Parquet too. Iceberg adds the manifest-level skipping on top.

</details>

---

## Part 2: Schema Evolution, Time Travel, Multi-Engine (45 min)

### Exercise 7: Schema Evolution (10 min)

Iceberg tracks columns by **immutable numeric ID**, not by name. That's why you can rename, drop, or reorder columns without rewriting any data files. Adding a column is even simpler ;  old files just don't have it, and reads get NULL for those rows.

**Q7.** Add a new `tip_pct` column to the `taxi.trips` table using `union_by_name`. After the schema change, verify two things:

1. The new column exists in `table.schema()` with a fresh field ID.
2. **None of the Parquet files in `data/` were rewritten** (check `os.path.getmtime` before and after).

<details>
<summary>Hint</summary>

- `pyarrow.Schema.append(field)` returns a new schema with one extra field.
- `with table.update_schema() as us: us.union_by_name(new_schema)` is the schema evolution API; adds any fields that exist in `new_schema` but not in the table.
- The Parquet files are at `/app/data/warehouse/taxi/trips/data/`.

</details>

<details>
<summary>Answer</summary>

```python
import os, glob
import pyarrow as pa

DATA_DIR = "/app/data/warehouse/taxi/trips/data"

before = {p: os.path.getmtime(p) for p in glob.glob(f"{DATA_DIR}/*.parquet")}
print("before:")
for p, mt in before.items():
    print(f"  {os.path.basename(p)}  mtime={mt}")

# Build a schema that includes the new column
new_schema = month1.schema.append(pa.field("tip_pct", pa.float64()))

with table.update_schema() as us:
    us.union_by_name(new_schema)

print("\nschema now:")
for f in table.schema().fields:
    print(f"  id={f.field_id:<3} {f.name:<25} {f.field_type}")

after = {p: os.path.getmtime(p) for p in glob.glob(f"{DATA_DIR}/*.parquet")}
print("\nfiles unchanged?", before == after)
```

Expected:

```
schema now:
  id=1   VendorID                  int
  id=2   tpep_pickup_datetime      timestamptz
  ...
  id=19  Airport_fee               double
  id=20  tip_pct                   double          ← new
files unchanged? True
```

The `tip_pct` column got the next available field ID (20). The existing Parquet files weren't touched: when a query selects `tip_pct` from the old data, Iceberg notices that `field_id=20` isn't present in those files and returns NULL.

This is what the deck means by "metadata-only operation"; ALTER TABLE on a 100 GB table is a sub-second commit, not an hours-long rewrite.

New files can have the new field and won't be a problem.

</details>

---

### Exercise 8: Time Travel (8 min)

Every snapshot is a complete view of the table at a point in time. You can pin a scan to any snapshot in the history ;  this is what "time travel" means in Iceberg.

**Q8.** Find the snapshot ID of the **first** append (the one that landed January's data only). Use `table.scan(snapshot_id=...)` to read the table as of that snapshot. The row count should match January alone, not January + February.

<details>
<summary>Hint</summary>

`table.snapshots()` returns snapshots in commit order (oldest first). `table.scan(snapshot_id=...)` accepts an explicit snapshot ID; without it, the scan uses the current snapshot.

</details>

<details>
<summary>Answer</summary>

```python
snaps = list(table.snapshots())
first_id, second_id = snaps[0].snapshot_id, snaps[1].snapshot_id

t_old = table.scan(snapshot_id=first_id).to_arrow()
t_now = table.scan().to_arrow()

print(f"at snapshot {first_id}: {t_old.num_rows:,} rows")
print(f"at current snapshot: {t_now.num_rows:,} rows")
```

Expected:

```
at snapshot 7201650108055357207: 2,964,624 rows
at current snapshot: 5,972,150 rows
```

The current scan sees both January and February. The historical scan sees only January, exactly as the table looked right after the first append, even though we've since modified it. **No data was copied to enable this**; the historical snapshot just refers to the manifest list it was created with, and that manifest list still references January's Parquet file.

That's why time travel on Iceberg is essentially free: the engine follows pointers that already exist, instead of restoring from a backup.

</details>

---

### Exercise 9: Rollback (10 min)

Time travel is for *reading* the past. **Rollback** changes which snapshot is "current", so subsequent writes branch from there. This is how you undo an accidental commit without having to recover from a backup.

**Q9.** Roll the table back to the first snapshot (January only) using `manage_snapshots().rollback_to_snapshot(...)`. Verify the current scan now returns ~3M rows. Then check `table.inspect.history()` ;  how many entries does it show now, and which one is `is_current_ancestor=true`?

<details>
<summary>Hint</summary>

`with table.manage_snapshots() as ms: ms.rollback_to_snapshot(snapshot_id)` is the API. After rollback, the snapshot you rolled back to becomes the current one; the previous "current" snapshot is still in the table but is no longer an ancestor.

</details>

<details>
<summary>Answer</summary>

```python
target = snaps[0].snapshot_id

with table.manage_snapshots() as ms:
    ms.rollback_to_snapshot(target)

print(f"after rollback: {table.scan().to_arrow().num_rows:,} rows")
print()
print(table.inspect.history())
```

Expected:

```
after rollback: 2,964,624 rows

made_current_at         snapshot_id            parent_id              is_current_ancestor
2026-05-10 15:20:08     7201650108055357207    null                   true
2026-05-10 15:20:09     9159361329493901782    7201650108055357207    false
2026-05-10 15:20:30     7201650108055357207    null                   true   ← rollback
```

Three history entries:

1. The original January append.
2. The February append (no longer an ancestor of the current snapshot).
3. The rollback; the current snapshot pointer was set back to snapshot 1.

**Rollback didn't delete February's snapshot.** It just changed which snapshot is current. February's data files are still there; if you wanted, you could time-travel to that snapshot ID and read them. They're just no longer in the table's "live" history.

This is the same property that makes Iceberg safe for concurrent writes: changing the current pointer is **the** atomic operation, and old snapshots never disappear unless explicitly expired.

> Note: PyIceberg doesn't yet expose `expire_snapshots` (the operation that actually deletes orphaned data files to reclaim disk). That's done via Spark procedures in production today.

</details>

---

### Exercise 10: Multi-Engine; DuckDB Reads the Same Warehouse (10 min)

Open table format = same files, any engine. Let's prove it. We'll point DuckDB's iceberg extension at the warehouse PyIceberg has been writing to. Same Parquet files. Same `metadata.json`. Different process, different engine, no copy.

**Q10.** From the same notebook, install DuckDB's Iceberg extension and run a SQL aggregation against the `taxi.trips` table directly. Compare the result with the same aggregation done via PyIceberg + PyArrow.

<details>
<summary>Hint</summary>

- `duckdb.sql("INSTALL iceberg; LOAD iceberg;")`
- `SET unsafe_enable_version_guessing=true;` is needed because we're using a SQL catalog DuckDB doesn't know about, so it scans the metadata directory to find the latest `metadata.json`. In production you'd configure DuckDB with the catalog's REST endpoint instead.
- The path to the table for `iceberg_scan(...)` is the table's location: `/app/data/warehouse/taxi/trips`.

</details>

<details>
<summary>Answer</summary>

Execute first the installation:

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute("SET unsafe_enable_version_guessing=true;")
```

Query:

```python
print(con.sql("""
    SELECT PULocationID,
           COUNT(*)            AS trips,
           AVG(fare_amount)    AS avg_fare,
           SUM(total_amount)   AS revenue
    FROM iceberg_scan('/app/data/warehouse/taxi/trips')
    GROUP BY PULocationID
    ORDER BY trips DESC
    LIMIT 5
"""))
```

Expected:

```
┌──────────────┬────────┬───────────┬───────────────┐
│ PULocationID │ trips  │ avg_fare  │    revenue    │
├──────────────┼────────┼───────────┼───────────────┤
│          161 │ 290557 │     15.35 │  6,512,000.00 │
│          237 │ 283508 │     12.31 │  5,114,000.00 │
│          132 │ 272040 │     59.45 │ 19,400,000.00 │
│          236 │ 270465 │     12.84 │  5,098,000.00 │
│          162 │ 212655 │     14.92 │  4,632,000.00 │
└──────────────┴────────┴───────────┴───────────────┘
```

The numbers reflect *whatever the current snapshot is*; if you ran exercise 9, that's January only (~3M rows). If you ran exercise 8 first and skipped the rollback, it's both months.

What just happened: **DuckDB read the exact same Parquet files PyIceberg wrote**, using the manifest list and manifests to know which files to open and which can be skipped. Two completely independent libraries, one warehouse on disk, perfect agreement. This is what an open table format buys you. Replace DuckDB with Spark, Trino, Snowflake, BigQuery, or PyIceberg-with-a-different-catalog and the same property holds.

A real production setup wouldn't use `unsafe_enable_version_guessing`. Instead, both engines would talk to the same **REST catalog** (Polaris, Lakekeeper, AWS Glue's Iceberg endpoint, Snowflake Open Catalog) which atomically tracks the current `metadata.json` pointer. The files on disk look identical; only the catalog conversation changes.

</details>

---

### Exercise 11 (bonus): Hidden Partitioning + Partition Evolution (10 min)

If we have time. The deck mentioned two ideas that look like magic until you do them once:

- **Hidden partitioning**: define a transform like `days(timestamp_column)` once, and any query filtering on `timestamp_column` gets partition pruning automatically (no separate `pickup_day` column to remember).
- **Partition evolution**: change the partitioning *after* the table exists, without rewriting any data.

Both happen via the same `update_spec` API.

**Q11.** Create a fresh table `taxi.trips_pe` (no partitioning). Then *evolve* the partition spec to add a `days(tpep_pickup_datetime)` transform. Append a slice of January's data. Look at `data/`: what directory structure appeared, and what does it tell you about the underlying timestamps?

> ⚠️ DuckDB's Iceberg extension currently has trouble with date-partitioned Iceberg tables. The exercise here is about *writing* and *inspecting* them via PyIceberg; reading via DuckDB may error.

<details>
<summary>Hint</summary>

```python
from pyiceberg.transforms import DayTransform

with ptable.update_spec() as us:
    us.add_field("tpep_pickup_datetime", DayTransform(), "pickup_day")
```

`update_spec` is the partition-evolution context manager. `add_field(source_column, transform, partition_name)` adds a partition field referencing an existing column.

</details>

<details>
<summary>Answer</summary>

```python
from pyiceberg.transforms import DayTransform

# Fresh table, no partitioning yet
ptable = catalog.create_table("taxi.trips_pe", schema=month1.schema)

# Evolve the partition spec: from "no partitions" to "days(tpep_pickup_datetime)"
with ptable.update_spec() as us:
    us.add_field("tpep_pickup_datetime", DayTransform(), "pickup_day")

print("partition spec now:", ptable.spec())

# Now append a slice and inspect the on-disk layout
ptable.append(month1.slice(0, 50_000))

import subprocess
print(subprocess.check_output([
    "ls", "/app/data/warehouse/taxi/trips_pe/data"
]).decode())
```

Expected:

```
partition spec now: [ 1000: pickup_day: day(2) ]

pickup_day=2009-01-01
pickup_day=2023-12-31
pickup_day=2024-01-01
pickup_day=2024-01-02
... (one dir per day) ...
```

Two things to notice:

**1. Hidden partitioning.** We never added a `pickup_day` column to the data; Iceberg derived it from `tpep_pickup_datetime` using the `DayTransform`. A future query like `WHERE tpep_pickup_datetime BETWEEN '2024-01-15' AND '2024-01-16'` will prune to ~2 partitions automatically. Users filter on the *source* column; the partition is invisible to them. Compare classic Hive: you'd have a separate `pickup_day STRING` column and a filter on `tpep_pickup_datetime` would do a full scan because Hive can't connect the two.

**2. Real-world data quality.** Partition directories include `pickup_day=2009-01-01` and `pickup_day=2023-12-31` ;  those are bad timestamps in the raw TLC dataset (data-entry errors, GPS clock issues), the same ones we filtered out in session 11 with a `WHERE tpep_pickup_datetime >= '2024-01-01'`. Iceberg dutifully partitions them; in production you'd filter at write time.

If you wanted to roll this back further ;  say, change `day` to `hour` granularity ;  you'd `update_spec` again. **Old data stays in day-partitions; new data lands in hour-partitions.** No rewrite. That's partition evolution.

</details>

---

## Summary

| Topic | What you practiced |
|-------|-------------------|
| Catalog | `SqlCatalog` over SQLite ;  what the catalog actually stores |
| Table creation | Inferring an Iceberg schema from a PyArrow schema |
| Append + snapshots | One commit per write, snapshot history as a linked list |
| On-disk layout | `data/` Parquet files + `metadata/` JSON + Avro manifests |
| Inspect tables | `table.inspect.{history, snapshots, files, manifests}` |
| Predicate pushdown | `table.scan(row_filter=...)` skips files via min/max stats |
| Schema evolution | `union_by_name` is metadata-only; data files unchanged |
| Time travel | `table.scan(snapshot_id=...)` to read any historical commit |
| Rollback | `manage_snapshots.rollback_to_snapshot()` flips the current pointer |
| Multi-engine | DuckDB and PyIceberg reading the same warehouse |
| Hidden partitioning | `DayTransform` + auto-pruning on the source column |

> **The core idea**: a table format like Iceberg turns "a directory of Parquet files" into "a transactional, time-travellable, schema-evolving table". The data is still just Parquet; the ACID and history come from a small amount of metadata that any engine can read.

Next session: **Cloud-native big data** ;  separation of storage and compute, serverless query engines (Athena, BigQuery), and the modern data architecture story end-to-end.

---

## Cleanup

```bash
docker compose down
```

To wipe the warehouse and start clean next time:

```bash
rm -rf data/warehouse
```

The raw taxi Parquet files in `data/` stay; you'll likely want them again.
