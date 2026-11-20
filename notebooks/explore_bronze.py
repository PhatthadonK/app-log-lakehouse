# %% [markdown]
# # Explore bronze + time travel demo
# Run cell by cell in VS Code / Jupyter (the "# %%" markers are cells),
# or run the whole file: python notebooks/explore_bronze.py
#
# Stop consume_to_bronze.py first: a DuckDB-file catalog allows only one process at a time.

# %%
import os

from pyducklake import Catalog

LAKE_DIR = os.getenv("LAKE_DIR", "lakehouse")
catalog = Catalog("lake", os.path.join(LAKE_DIR, "catalog.ducklake"),
                  data_path=os.path.join(LAKE_DIR, "data") + "/")
table = catalog.load_table(("bronze", "raw_events"))
con = catalog.connection
BRONZE = "lake.bronze.raw_events"


def sql(query):
    return con.sql(query).df()


# %% 1. How many rows? (pyducklake scan API)
print("rows in bronze:", table.scan().count())
print(table.scan().select("event_id", "user_id", "event_type", "event_ts").with_limit(5).to_pandas())

# %% 2. Duplicates: total rows vs distinct event_id
print(sql(f"""
    SELECT count(*)                                  AS total_rows,
           count(DISTINCT event_id)                  AS distinct_events,
           count(*) - count(DISTINCT event_id)       AS duplicate_rows
    FROM {BRONZE}
"""))

# %% 3. Nulls and unparseable payloads
print(sql(f"""
    SELECT count(*) FILTER (WHERE user_id IS NULL) AS null_user_id,
           count(*) FILTER (WHERE parse_error)     AS parse_errors
    FROM {BRONZE}
"""))

# %% 4. Messy casing in event_type
print(sql(f"""
    SELECT event_type, count(*) AS n
    FROM {BRONZE}
    GROUP BY event_type
    ORDER BY lower(event_type), event_type
"""))

# %% 5. Messy timestamp formats (just look at the patterns)
print(sql(f"""
    SELECT CASE
             WHEN event_ts LIKE '____-__-__T%'          THEN 'ISO 8601'
             WHEN event_ts LIKE '____/__/__ __:__:__'   THEN 'YYYY/MM/DD'
             WHEN regexp_full_match(event_ts, '[0-9]+') THEN 'epoch seconds'
             ELSE 'other'
           END AS ts_format,
           count(*) AS n
    FROM {BRONZE}
    GROUP BY ts_format
    ORDER BY n DESC
"""))

# %% 6. Spread across Kafka partitions
print(sql(f"""
    SELECT kafka_partition, count(*) AS n, min(kafka_offset) AS min_offset, max(kafka_offset) AS max_offset
    FROM {BRONZE}
    GROUP BY kafka_partition
    ORDER BY kafka_partition
"""))

# %% 7. Data files written (small files problem check)
files = table.inspect().files()
print("parquet files:", files.num_rows)
print(files.to_pandas().head())

# %% [markdown]
# ## Time travel

# %% 8. Snapshot history with commit messages
snapshots = sorted(table.snapshots(), key=lambda s: s.snapshot_id)
for s in snapshots:
    print(s.snapshot_id, s.timestamp, s.commit_message)

loads = [s for s in snapshots if (s.commit_message or "").startswith("bronze:")]
print(f"\n{len(loads)} bronze load(s) found")

# %% 9. Read the table as it was after the FIRST load vs now
if loads:
    first = loads[0]
    then_count = table.scan().with_snapshot(first.snapshot_id).count()
    now_count = table.scan().count()
    print(f"after first load (snapshot {first.snapshot_id}): {then_count:,} rows")
    print(f"now: {now_count:,} rows")

    # Same thing in SQL
    print(sql(f"SELECT count(*) AS rows_at_first_load FROM {BRONZE} AT (VERSION => {first.snapshot_id})"))

# %% 10. Exactly which rows did the LAST load add?
if loads:
    last = loads[-1]
    added = table.table_insertions(start_snapshot=last.snapshot_id, end_snapshot=last.snapshot_id)
    print(f"snapshot {last.snapshot_id}: '{last.commit_message}'")
    print(f"rows inserted by this load: {added.num_rows:,}")
    print(added.to_pandas()[["event_id", "event_type", "kafka_partition", "kafka_offset"]].head())