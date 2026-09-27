import duckdb
import time

DB = duckdb.connect()

DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")

FEATURES = "intermediate/train_match_features.parquet"

S1 = "parquet/train_source1_normalized.parquet"
S2 = "parquet/train_source2_normalized.parquet"
S3 = "parquet/train_source3_normalized.parquet"

print("=" * 100)
print("NORMALIZED FUZZY DISTANCE ANALYSIS")
print("=" * 100)

start = time.time()

query = f"""
WITH source_entities AS (

    SELECT
        entity_id,
        name_compact
    FROM read_parquet('{S2}')

    UNION ALL

    SELECT
        entity_id,
        name_compact
    FROM read_parquet('{S3}')
),

base AS (

    SELECT
        f.label,
        f.same_house,
        f.address_prefix5,

        s1.name_compact AS name1,
        s2.name_compact AS name2,

        levenshtein(
            s1.name_compact,
            s2.name_compact
        ) AS edit_distance,

        GREATEST(
            length(s1.name_compact),
            length(s2.name_compact)
        ) AS max_name_length

    FROM read_parquet('{FEATURES}') AS f

    JOIN read_parquet('{S1}') AS s1
        ON f.source1_entity_id = s1.entity_id

    JOIN source_entities AS s2
        ON f.candidate_entity_id = s2.entity_id

    WHERE

        f.name_prefix4 = 1

        AND f.name_length_diff <= 5

        AND (
            f.same_house = 1
            OR f.address_prefix5 = 1
        )

        -- Remove deterministic tiers

        AND NOT (
            f.exact_compact_name = 1
            AND f.same_house = 1
        )

        AND NOT (
            f.name_prefix5 = 1
            AND f.same_house = 1
            AND f.address_prefix5 = 1
        )

        AND NOT (
            f.name_prefix4 = 1
            AND f.same_house = 1
            AND f.address_prefix5 = 1
        )

        AND NOT (
            f.name_contains = 1
            AND f.same_house = 1
        )
),

signals AS (

    SELECT
        *,
        CASE
            WHEN max_name_length > 0
            THEN CAST(edit_distance AS DOUBLE)
                 / max_name_length
            ELSE 1.0
        END AS normalized_distance

    FROM base
)

SELECT
    'normalized <= 0.10 + house' AS rule,
    COUNT(*) AS candidates,
    COALESCE(SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END), 0)
        AS true_pairs
FROM signals
WHERE normalized_distance <= 0.10
  AND same_house = 1

UNION ALL

SELECT
    'normalized <= 0.15 + house',
    COUNT(*),
    COALESCE(SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END), 0)
FROM signals
WHERE normalized_distance <= 0.15
  AND same_house = 1

UNION ALL

SELECT
    'normalized <= 0.20 + house',
    COUNT(*),
    COALESCE(SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END), 0)
FROM signals
WHERE normalized_distance <= 0.20
  AND same_house = 1

UNION ALL

SELECT
    'normalized <= 0.25 + house',
    COUNT(*),
    COALESCE(SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END), 0)
FROM signals
WHERE normalized_distance <= 0.25
  AND same_house = 1

UNION ALL

SELECT
    'normalized <= 0.30 + house',
    COUNT(*),
    COALESCE(SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END), 0)
FROM signals
WHERE normalized_distance <= 0.30
  AND same_house = 1

UNION ALL

SELECT
    'normalized <= 0.15 + address',
    COUNT(*),
    COALESCE(SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END), 0)
FROM signals
WHERE normalized_distance <= 0.15
  AND address_prefix5 = 1

UNION ALL

SELECT
    'normalized <= 0.20 + address',
    COUNT(*),
    COALESCE(SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END), 0)
FROM signals
WHERE normalized_distance <= 0.20
  AND address_prefix5 = 1

UNION ALL

SELECT
    'normalized <= 0.25 + address',
    COUNT(*),
    COALESCE(SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END), 0)
FROM signals
WHERE normalized_distance <= 0.25
  AND address_prefix5 = 1

UNION ALL

SELECT
    'normalized <= 0.20 + house OR address',
    COUNT(*),
    COALESCE(SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END), 0)
FROM signals
WHERE normalized_distance <= 0.20
  AND (
      same_house = 1
      OR address_prefix5 = 1
  )

UNION ALL

SELECT
    'normalized <= 0.25 + house OR address',
    COUNT(*),
    COALESCE(SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END), 0)
FROM signals
WHERE normalized_distance <= 0.25
  AND (
      same_house = 1
      OR address_prefix5 = 1
  )

ORDER BY candidates
"""

rows = DB.execute(query).fetchall()

print()
print(
    f"{'RULE':<42}"
    f"{'CANDIDATES':>16}"
    f"{'TRUE':>16}"
    f"{'PRECISION':>15}"
)

print("-" * 92)

for rule, candidates, true_pairs in rows:

    candidates = candidates or 0
    true_pairs = true_pairs or 0

    precision = (
        true_pairs / candidates * 100
        if candidates > 0
        else 0
    )

    print(
        f"{rule:<42}"
        f"{candidates:>16,}"
        f"{true_pairs:>16,}"
        f"{precision:>14.2f}%"
    )

print()
print("=" * 100)
print("DONE")
print("=" * 100)

print(f"Elapsed: {(time.time() - start) / 60:.2f} minutes")

print("=" * 100)