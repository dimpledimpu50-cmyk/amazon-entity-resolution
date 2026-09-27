import duckdb
import time
import os

start = time.time()

DB = duckdb.connect()

DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")
DB.execute("SET preserve_insertion_order=false")

CANDIDATES = "intermediate/test_candidate_pairs.parquet"
S1 = "parquet/test_source1_normalized.parquet"
S2 = "parquet/test_source2_normalized.parquet"
S3 = "parquet/test_source3_normalized.parquet"

OUT = "intermediate/test_match_features.parquet"

print("=" * 90)
print("BUILDING TEST MATCH FEATURES")
print("=" * 90)

# ------------------------------------------------------------
# 1. Load candidate pairs
# ------------------------------------------------------------

print("\nLoading candidate pairs...")

DB.execute(f"""
CREATE OR REPLACE TEMP TABLE candidates AS
SELECT DISTINCT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet('{CANDIDATES}')
""")

candidate_count = DB.execute("""
SELECT COUNT(*) FROM candidates
""").fetchone()[0]

print(f"Candidate pairs: {candidate_count:,}")


# ------------------------------------------------------------
# 2. Load Test Source 1
# ------------------------------------------------------------

print("\nPreparing Test Source 1...")

DB.execute(f"""
CREATE OR REPLACE TEMP TABLE s1 AS
SELECT
    entity_id,
    name_norm,
    name_compact,
    address_norm,
    address_compact,
    country_norm,

    regexp_extract(
        address_norm,
        '[0-9]+'
    ) AS house_number,

    length(name_compact) AS name_len,
    length(address_compact) AS address_len

FROM read_parquet('{S1}')
""")


# ------------------------------------------------------------
# 3. Load Test Source 2 + Source 3
# ------------------------------------------------------------

print("Preparing Test Source 2 + Source 3...")

DB.execute(f"""
CREATE OR REPLACE TEMP TABLE s23 AS

SELECT
    entity_id,
    name_norm,
    name_compact,
    address_norm,
    address_compact,
    country_norm,

    regexp_extract(
        address_norm,
        '[0-9]+'
    ) AS house_number,

    length(name_compact) AS name_len,
    length(address_compact) AS address_len

FROM read_parquet('{S2}')

UNION ALL

SELECT
    entity_id,
    name_norm,
    name_compact,
    address_norm,
    address_compact,
    country_norm,

    regexp_extract(
        address_norm,
        '[0-9]+'
    ) AS house_number,

    length(name_compact) AS name_len,
    length(address_compact) AS address_len

FROM read_parquet('{S3}')
""")


# ------------------------------------------------------------
# 4. Generate features
# ------------------------------------------------------------

print("\nGenerating test features...")
print("This is the main DuckDB operation and may take several minutes.")

DB.execute(f"""
COPY (

    SELECT

        c.source1_entity_id,
        c.candidate_entity_id,


        -- ---------------------------------------------------
        -- COUNTRY
        -- ---------------------------------------------------

        CASE
            WHEN a.country_norm = b.country_norm
            THEN 1
            ELSE 0
        END AS country_equal,


        -- ---------------------------------------------------
        -- NAME FEATURES
        -- ---------------------------------------------------

        CASE
            WHEN a.name_norm = b.name_norm
            THEN 1
            ELSE 0
        END AS exact_name,

        CASE
            WHEN a.name_compact = b.name_compact
            THEN 1
            ELSE 0
        END AS exact_compact_name,

        CASE
            WHEN left(a.name_compact, 4)
               = left(b.name_compact, 4)
            THEN 1
            ELSE 0
        END AS name_prefix4,

        CASE
            WHEN left(a.name_compact, 5)
               = left(b.name_compact, 5)
            THEN 1
            ELSE 0
        END AS name_prefix5,

        CASE
            WHEN right(a.name_compact, 4)
               = right(b.name_compact, 4)
            THEN 1
            ELSE 0
        END AS name_suffix4,

        CASE
            WHEN a.name_compact LIKE '%' || b.name_compact || '%'
              OR b.name_compact LIKE '%' || a.name_compact || '%'
            THEN 1
            ELSE 0
        END AS name_contains,

        abs(
            a.name_len - b.name_len
        ) AS name_length_diff,


        -- ---------------------------------------------------
        -- ADDRESS FEATURES
        -- ---------------------------------------------------

        CASE
            WHEN a.address_norm = b.address_norm
            THEN 1
            ELSE 0
        END AS exact_address,

        CASE
            WHEN a.address_compact = b.address_compact
            THEN 1
            ELSE 0
        END AS exact_compact_address,

        CASE
            WHEN left(a.address_compact, 5)
               = left(b.address_compact, 5)
            THEN 1
            ELSE 0
        END AS address_prefix5,

        CASE
            WHEN a.house_number <> ''
             AND b.house_number <> ''
             AND a.house_number = b.house_number
            THEN 1
            ELSE 0
        END AS same_house,

        abs(
            a.address_len - b.address_len
        ) AS address_length_diff,


        -- ---------------------------------------------------
        -- COMBINED SIGNALS
        -- ---------------------------------------------------

        CASE
            WHEN left(a.name_compact, 4)
               = left(b.name_compact, 4)
             AND a.house_number <> ''
             AND b.house_number <> ''
             AND a.house_number = b.house_number
            THEN 1
            ELSE 0
        END AS prefix4_same_house,

        CASE
            WHEN left(a.name_compact, 4)
               = left(b.name_compact, 4)
             AND left(a.address_compact, 5)
               = left(b.address_compact, 5)
            THEN 1
            ELSE 0
        END AS prefix4_address_prefix,

        CASE
            WHEN a.name_compact = b.name_compact
             AND a.house_number = b.house_number
            THEN 1
            ELSE 0
        END AS exact_name_same_house,

        CASE
            WHEN a.name_compact = b.name_compact
             AND a.address_compact = b.address_compact
            THEN 1
            ELSE 0
        END AS exact_name_exact_address


    FROM candidates c

    JOIN s1 a
      ON c.source1_entity_id = a.entity_id

    JOIN s23 b
      ON c.candidate_entity_id = b.entity_id

) TO '{OUT}'
(FORMAT PARQUET, COMPRESSION ZSTD)
""")

print("\nFeature file created.")

size_mb = os.path.getsize(OUT) / (1024 * 1024)

print(f"Output: {OUT}")
print(f"Size: {size_mb:.1f} MB")


# ------------------------------------------------------------
# 5. Basic statistics
# ------------------------------------------------------------

print("\nChecking feature rows...")

stats = DB.execute(f"""
SELECT COUNT(*)
FROM read_parquet('{OUT}')
""").fetchone()

print(f"Feature rows: {stats[0]:,}")

print()
print("=" * 90)
print(f"DONE - elapsed {(time.time() - start) / 60:.2f} minutes")
print("=" * 90)