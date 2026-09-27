import duckdb
import time

con = duckdb.connect()
con.execute("SET memory_limit='6GB'")
con.execute("SET threads=2")
con.execute("SET preserve_insertion_order=false")

start = time.time()

BASE = "intermediate/train_candidate_pairs.parquet"
R15 = "intermediate/train_candidates/r15_address_prefix5_len10_freq50.parquet"
OUTPUT = "intermediate/train_candidate_pairs_r15.parquet"

print("=" * 90)
print("BUILDING COMBINED TRAIN CANDIDATES: BASE + R15")
print("=" * 90)

print("\nCounting base candidates...")

base_count = con.execute(f"""
SELECT COUNT(*)
FROM read_parquet('{BASE}')
""").fetchone()[0]

print(f"Base candidates: {base_count:,}")

print("\nCounting R15 candidates...")

r15_count = con.execute(f"""
SELECT COUNT(*)
FROM read_parquet('{R15}')
""").fetchone()[0]

print(f"R15 new candidates: {r15_count:,}")

print("\nCombining and removing duplicates...")

con.execute(f"""
COPY (
    SELECT
        source1_entity_id,
        candidate_entity_id
    FROM read_parquet('{BASE}')

    UNION

    SELECT
        source1_entity_id,
        candidate_entity_id
    FROM read_parquet('{R15}')
)
TO '{OUTPUT}'
(FORMAT PARQUET, COMPRESSION ZSTD);
""")

combined_count = con.execute(f"""
SELECT COUNT(*)
FROM read_parquet('{OUTPUT}')
""").fetchone()[0]

print("\n" + "=" * 90)
print("COMBINED CANDIDATE SUMMARY")
print("=" * 90)

print(f"Base candidates       : {base_count:,}")
print(f"R15 candidates        : {r15_count:,}")
print(f"Combined candidates   : {combined_count:,}")
print(f"Added candidates      : {combined_count - base_count:,}")

increase = (combined_count - base_count) / base_count * 100

print(f"Candidate increase    : {increase:.2f}%")

print(f"\nOutput: {OUTPUT}")
print(f"Elapsed: {(time.time() - start) / 60:.2f} minutes")
print("=" * 90)