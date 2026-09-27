import duckdb

DB = duckdb.connect()

files = [
    "intermediate/verified_baseline_candidates.parquet",
    "intermediate/recovery7_candidates.parquet",
    "intermediate/recovery10_candidates.parquet",
    "intermediate/recovery11_candidates.parquet",
]

for f in files:
    print("=" * 80)
    print(f)
    print("=" * 80)

    rows = DB.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{f}')"
    ).fetchall()

    for row in rows:
        print(row)