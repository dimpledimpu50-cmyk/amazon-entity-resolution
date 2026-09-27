import duckdb
import os
import time

INPUT_FILE = "intermediate/train_match_features.parquet"
OUTPUT_FILE = "intermediate/ml_training_sample.parquet"

con = duckdb.connect()

con.execute("SET memory_limit='6GB'")
con.execute("SET threads=2")
con.execute("SET preserve_insertion_order=false")

start = time.time()

print("=" * 80)
print("CREATING ML TRAINING SAMPLE - FAST VERSION")
print("=" * 80)

# ------------------------------------------------------------------
# Count positives
# ------------------------------------------------------------------

positive_count = con.execute(f"""
    SELECT COUNT(*)
    FROM read_parquet('{INPUT_FILE}')
    WHERE label = 1
""").fetchone()[0]

print(f"Positive rows: {positive_count:,}")

# We want approximately 3 negatives for every positive.
# Instead of reservoir sampling, use a deterministic hash filter.
#
# 76.43M negatives / 19.38M desired negatives ~= 25.35%
#
# hash(source1 + candidate) gives a deterministic selection
# without maintaining a large reservoir in memory.

print("Selecting approximately 25.35% of negative pairs...")
print("This avoids the slow reservoir sampling.")

query = f"""
COPY (
    SELECT
        source1_entity_id,
        candidate_entity_id,
        label,
        country_equal,
        exact_name,
        exact_compact_name,
        name_prefix4,
        name_prefix5,
        name_suffix4,
        name_contains,
        name_length_diff,
        exact_address,
        exact_compact_address,
        address_prefix5,
        same_house,
        address_length_diff,
        prefix4_same_house,
        prefix4_address_prefix,
        exact_name_same_house,
        exact_name_exact_address
    FROM read_parquet('{INPUT_FILE}')
    WHERE label = 1

    UNION ALL

    SELECT
        source1_entity_id,
        candidate_entity_id,
        label,
        country_equal,
        exact_name,
        exact_compact_name,
        name_prefix4,
        name_prefix5,
        name_suffix4,
        name_contains,
        name_length_diff,
        exact_address,
        exact_compact_address,
        address_prefix5,
        same_house,
        address_length_diff,
        prefix4_same_house,
        prefix4_address_prefix,
        exact_name_same_house,
        exact_name_exact_address
    FROM read_parquet('{INPUT_FILE}')
    WHERE label = 0
      AND MOD(
            HASH(
                source1_entity_id || '|' || candidate_entity_id
            ),
            10000
          ) < 2535
)
TO '{OUTPUT_FILE}'
(FORMAT PARQUET, COMPRESSION ZSTD);
"""

print("\nWriting training sample...")
con.execute(query)

# ------------------------------------------------------------------
# Inspect
# ------------------------------------------------------------------

rows = con.execute(f"""
    SELECT COUNT(*)
    FROM read_parquet('{OUTPUT_FILE}')
""").fetchone()[0]

positives = con.execute(f"""
    SELECT COUNT(*)
    FROM read_parquet('{OUTPUT_FILE}')
    WHERE label = 1
""").fetchone()[0]

negatives = con.execute(f"""
    SELECT COUNT(*)
    FROM read_parquet('{OUTPUT_FILE}')
    WHERE label = 0
""").fetchone()[0]

columns = con.execute(f"""
    DESCRIBE
    SELECT *
    FROM read_parquet('{OUTPUT_FILE}')
""").fetchall()

elapsed = (time.time() - start) / 60
size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)

print(f"Rows      : {rows:,}")
print(f"Positive  : {positives:,}")
print(f"Negative  : {negatives:,}")
print(f"Columns   : {len(columns)}")
print(f"File size : {size_mb:.1f} MB")
print(f"Elapsed   : {elapsed:.2f} minutes")

print("\nImportant columns:")

for col in columns[:3]:
    print(f"  {col[0]}")