from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parent.parent
PARQUET_DIR = ROOT / "parquet"

con = duckdb.connect()

files = [
    "train_source1_normalized.parquet",
    "train_source2_normalized.parquet",
    "train_source3_normalized.parquet",
    "test_source1_normalized.parquet",
    "test_source2_normalized.parquet",
    "test_source3_normalized.parquet",
]

print("=" * 75)
print("COUNTRY DISTRIBUTION")
print("=" * 75)

for filename in files:
    path = PARQUET_DIR / filename

    print(f"\n{filename}")

    result = con.execute(f"""
        SELECT
            country_norm,
            COUNT(*) AS row_count
        FROM read_parquet('{path}')
        GROUP BY country_norm
        ORDER BY row_count DESC
    """).fetchall()

    total = sum(row[1] for row in result)

    for country, count in result:
        percentage = count * 100.0 / total
        print(f"  {country!r:20} {count:>12,}  ({percentage:6.2f}%)")

    print(f"  TOTAL                {total:>12,}")

con.close()