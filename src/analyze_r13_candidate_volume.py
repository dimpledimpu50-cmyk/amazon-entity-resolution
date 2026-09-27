import duckdb

DB = duckdb.connect()

print("=" * 80)
print("R13 CANDIDATE VOLUME DIAGNOSTIC")
print("=" * 80)

S1_EVAL = "intermediate/blocking_eval_s1.parquet"
SOURCE2 = "parquet/train_source2_normalized.parquet"
SOURCE3 = "parquet/train_source3_normalized.parquet"

# ============================================================================
# 1. Evaluation S1
# ============================================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW s1 AS
SELECT
    entity_id AS source1_entity_id,
    country_norm,
    name_compact,
    address_norm,
    address_compact,

    left(name_compact, 4) AS name_prefix4,

    TRY_CAST(
        regexp_extract(address_norm, '[0-9]+')
        AS BIGINT
    ) AS house_number,

    left(address_compact, 5) AS address_prefix5

FROM read_parquet('{S1_EVAL}')
""")


# ============================================================================
# 2. Source 2 + Source 3
# ============================================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW source_data AS

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_compact,
    address_norm,
    address_compact,

    left(name_compact, 4) AS name_prefix4,

    TRY_CAST(
        regexp_extract(address_norm, '[0-9]+')
        AS BIGINT
    ) AS house_number,

    left(address_compact, 5) AS address_prefix5

FROM read_parquet('{SOURCE2}')

UNION ALL

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_compact,
    address_norm,
    address_compact,

    left(name_compact, 4) AS name_prefix4,

    TRY_CAST(
        regexp_extract(address_norm, '[0-9]+')
        AS BIGINT
    ) AS house_number,

    left(address_compact, 5) AS address_prefix5

FROM read_parquet('{SOURCE3}')
""")


# ============================================================================
# 3. Basic source statistics
# ============================================================================

print()
print("Calculating source statistics...")

source_count = DB.execute("""
SELECT COUNT(*) FROM source_data
""").fetchone()[0]

print(f"Total S2 + S3 rows: {source_count:,}")


# ============================================================================
# 4. Count candidates from PREFIX4 + HOUSE
# ============================================================================

print()
print("Testing: country + prefix4 + same house...")

prefix_house = DB.execute("""
SELECT COUNT(*)
FROM s1 s
JOIN source_data c
    ON s.country_norm = c.country_norm
   AND s.name_prefix4 = c.name_prefix4
   AND s.name_prefix4 <> ''
   AND s.house_number IS NOT NULL
   AND c.house_number IS NOT NULL
   AND s.house_number = c.house_number
""").fetchone()[0]

print(f"Prefix4 + house candidates: {prefix_house:,}")


# ============================================================================
# 5. Count candidates from PREFIX4 + ADDRESS PREFIX5
# ============================================================================

print()
print("Testing: country + prefix4 + address prefix5...")

prefix_address = DB.execute("""
SELECT COUNT(*)
FROM s1 s
JOIN source_data c
    ON s.country_norm = c.country_norm
   AND s.name_prefix4 = c.name_prefix4
   AND s.name_prefix4 <> ''
   AND s.address_prefix5 <> ''
   AND c.address_prefix5 <> ''
   AND s.address_prefix5 = c.address_prefix5
""").fetchone()[0]

print(f"Prefix4 + address-prefix5 candidates: {prefix_address:,}")


# ============================================================================
# 6. Count UNION candidate volume
# ============================================================================

print()
print("Testing UNION of both blockers...")

union_count = DB.execute("""
SELECT COUNT(*)
FROM (
    SELECT
        s.source1_entity_id,
        c.candidate_entity_id

    FROM s1 s
    JOIN source_data c
        ON s.country_norm = c.country_norm
       AND s.name_prefix4 = c.name_prefix4
       AND s.name_prefix4 <> ''
       AND s.house_number IS NOT NULL
       AND c.house_number IS NOT NULL
       AND s.house_number = c.house_number

    UNION

    SELECT
        s.source1_entity_id,
        c.candidate_entity_id

    FROM s1 s
    JOIN source_data c
        ON s.country_norm = c.country_norm
       AND s.name_prefix4 = c.name_prefix4
       AND s.name_prefix4 <> ''
       AND s.address_prefix5 <> ''
       AND c.address_prefix5 <> ''
       AND s.address_prefix5 = c.address_prefix5
) x
""").fetchone()[0]

print(f"Combined unique candidates: {union_count:,}")


# ============================================================================
# 7. Number of S1 entities receiving candidates
# ============================================================================

print()
print("Calculating per-S1 distribution...")

DB.execute("""
CREATE OR REPLACE TEMP VIEW r13_candidates AS

SELECT
    s.source1_entity_id,
    c.candidate_entity_id

FROM s1 s
JOIN source_data c
    ON s.country_norm = c.country_norm
   AND s.name_prefix4 = c.name_prefix4
   AND s.name_prefix4 <> ''
   AND (
        (
            s.house_number IS NOT NULL
            AND c.house_number IS NOT NULL
            AND s.house_number = c.house_number
        )
        OR
        (
            s.address_prefix5 <> ''
            AND c.address_prefix5 <> ''
            AND s.address_prefix5 = c.address_prefix5
        )
   )
""")


stats = DB.execute("""
SELECT
    COUNT(*) AS candidate_count,
    COUNT(DISTINCT source1_entity_id) AS s1_count,
    AVG(cnt) AS avg_candidates,
    MEDIAN(cnt) AS median_candidates,
    QUANTILE_CONT(cnt, 0.95) AS p95_candidates,
    MAX(cnt) AS max_candidates
FROM (
    SELECT
        source1_entity_id,
        COUNT(*) AS cnt
    FROM r13_candidates
    GROUP BY source1_entity_id
) x
""").fetchone()

print()
print("=" * 80)
print("R13 VOLUME RESULTS")
print("=" * 80)

print(f"Total candidates : {stats[0]:,}")
print(f"S1 with candidates: {stats[1]:,}")
print(f"Average          : {stats[2]:.2f}")
print(f"Median           : {stats[3]:.2f}")
print(f"P95              : {stats[4]:.2f}")
print(f"Maximum          : {stats[5]:,}")


# ============================================================================
# 8. Candidate distribution
# ============================================================================

print()
print("=" * 80)
print("CANDIDATE COUNT DISTRIBUTION")
print("=" * 80)

distribution = DB.execute("""
SELECT
    CASE
        WHEN cnt = 1 THEN '1'
        WHEN cnt BETWEEN 2 AND 5 THEN '2-5'
        WHEN cnt BETWEEN 6 AND 10 THEN '6-10'
        WHEN cnt BETWEEN 11 AND 25 THEN '11-25'
        WHEN cnt BETWEEN 26 AND 50 THEN '26-50'
        WHEN cnt BETWEEN 51 AND 100 THEN '51-100'
        WHEN cnt BETWEEN 101 AND 250 THEN '101-250'
        WHEN cnt BETWEEN 251 AND 500 THEN '251-500'
        ELSE '500+'
    END AS bucket,

    COUNT(*) AS s1_count

FROM (
    SELECT
        source1_entity_id,
        COUNT(*) AS cnt
    FROM r13_candidates
    GROUP BY source1_entity_id
) x

GROUP BY bucket

ORDER BY
    CASE bucket
        WHEN '1' THEN 1
        WHEN '2-5' THEN 2
        WHEN '6-10' THEN 3
        WHEN '11-25' THEN 4
        WHEN '26-50' THEN 5
        WHEN '51-100' THEN 6
        WHEN '101-250' THEN 7
        WHEN '251-500' THEN 8
        ELSE 9
    END
""").fetchall()

for bucket, count in distribution:
    print(f"{bucket:<10}: {count:,}")


print()
print("=" * 80)
print("R13 VOLUME DIAGNOSTIC COMPLETE")
print("=" * 80)
print()
print("Do NOT generate or save R13 candidates yet.")
print("Send me these results first.")