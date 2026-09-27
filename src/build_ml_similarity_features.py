import duckdb
import os
import time

INPUT_FILE = "intermediate/ml_training_sample.parquet"
OUTPUT_FILE = "intermediate/ml_training_similarity.parquet"

con = duckdb.connect()

con.execute("SET memory_limit='6GB'")
con.execute("SET threads=2")
con.execute("SET preserve_insertion_order=false")

start = time.time()

print("=" * 80)
print("BUILDING ML SIMILARITY FEATURES")
print("=" * 80)

print("\nInput: 25.8M training pairs")
print("Features:")
print("  - Name Jaro-Winkler")
print("  - Name Levenshtein similarity")
print("  - Name length ratio")
print("  - Address Jaro-Winkler")
print("  - Address Levenshtein similarity")
print("  - Address length ratio")
print()

query = f"""
COPY (
    SELECT
        m.*,

        -- ============================================================
        -- NAME SIMILARITY
        -- ============================================================

        CASE
            WHEN length(s1.name_norm) = 0
              OR length(c.name_norm) = 0
            THEN 0.0
            ELSE jaro_winkler_similarity(
                s1.name_norm,
                c.name_norm
            )
        END AS name_jaro,

        CASE
            WHEN GREATEST(
                length(s1.name_norm),
                length(c.name_norm)
            ) = 0
            THEN 0.0
            ELSE
                1.0 -
                CAST(
                    levenshtein(
                        s1.name_norm,
                        c.name_norm
                    ) AS DOUBLE
                )
                /
                GREATEST(
                    length(s1.name_norm),
                    length(c.name_norm)
                )
        END AS name_levenshtein,

        CASE
            WHEN GREATEST(
                length(s1.name_norm),
                length(c.name_norm)
            ) = 0
            THEN 0.0
            ELSE
                CAST(
                    LEAST(
                        length(s1.name_norm),
                        length(c.name_norm)
                    ) AS DOUBLE
                )
                /
                GREATEST(
                    length(s1.name_norm),
                    length(c.name_norm)
                )
        END AS name_length_ratio,

        -- ============================================================
        -- ADDRESS SIMILARITY
        -- ============================================================

        CASE
            WHEN length(s1.address_norm) = 0
              OR length(c.address_norm) = 0
            THEN 0.0
            ELSE jaro_winkler_similarity(
                s1.address_norm,
                c.address_norm
            )
        END AS address_jaro,

        CASE
            WHEN GREATEST(
                length(s1.address_norm),
                length(c.address_norm)
            ) = 0
            THEN 0.0
            ELSE
                1.0 -
                CAST(
                    levenshtein(
                        s1.address_norm,
                        c.address_norm
                    ) AS DOUBLE
                )
                /
                GREATEST(
                    length(s1.address_norm),
                    length(c.address_norm)
                )
        END AS address_levenshtein,

        CASE
            WHEN GREATEST(
                length(s1.address_norm),
                length(c.address_norm)
            ) = 0
            THEN 0.0
            ELSE
                CAST(
                    LEAST(
                        length(s1.address_norm),
                        length(c.address_norm)
                    ) AS DOUBLE
                )
                /
                GREATEST(
                    length(s1.address_norm),
                    length(c.address_norm)
                )
        END AS address_length_ratio

    FROM read_parquet('{INPUT_FILE}') m

    JOIN read_parquet(
        'parquet/train_source1_normalized.parquet'
    ) s1
        ON m.source1_entity_id = s1.entity_id

    JOIN (
        SELECT
            entity_id,
            name_norm,
            address_norm
        FROM read_parquet(
            'parquet/train_source2_normalized.parquet'
        )

        UNION ALL

        SELECT
            entity_id,
            name_norm,
            address_norm
        FROM read_parquet(
            'parquet/train_source3_normalized.parquet'
        )
    ) c
        ON m.candidate_entity_id = c.entity_id

) TO '{OUTPUT_FILE}'
(FORMAT PARQUET, COMPRESSION ZSTD);
"""

print("Running DuckDB...")
print("This is the expensive step.")
print("Please let it run.")

con.execute(query)

# ------------------------------------------------------------------
# Verify
# ------------------------------------------------------------------

rows = con.execute(f"""
    SELECT COUNT(*)
    FROM read_parquet('{OUTPUT_FILE}')
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
print(f"Columns   : {len(columns)}")
print(f"File size : {size_mb:.1f} MB")
print(f"Elapsed   : {elapsed:.2f} minutes")

print("\nNew similarity features:")

for name in [
    "name_jaro",
    "name_levenshtein",
    "name_length_ratio",
    "address_jaro",
    "address_levenshtein",
    "address_length_ratio"
]:
    print(f"  {name}")