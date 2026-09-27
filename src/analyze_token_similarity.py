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
print("TOKEN SIMILARITY ANALYSIS")
print("=" * 100)

start = time.time()

query = f"""
WITH source_entities AS (

    SELECT
        entity_id,
        name_norm
    FROM read_parquet('{S2}')

    UNION ALL

    SELECT
        entity_id,
        name_norm
    FROM read_parquet('{S3}')
),

base AS (

    SELECT
        f.label,
        f.same_house,
        f.address_prefix5,

        s1.name_norm AS name1,
        s2.name_norm AS name2

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

tokenized AS (

    SELECT
        *,
        regexp_split_to_array(
            trim(name1),
            '\\s+'
        ) AS tokens1,

        regexp_split_to_array(
            trim(name2),
            '\\s+'
        ) AS tokens2

    FROM base
),

similarity AS (

    SELECT
        *,

        -- Number of tokens in each name
        len(tokens1) AS tokens1_count,
        len(tokens2) AS tokens2_count,

        -- Shared token count
        len(
            list_intersect(tokens1, tokens2)
        ) AS shared_tokens

    FROM tokenized
),

metrics AS (

    SELECT
        *,

        -- Jaccard:
        -- intersection / union
        CASE
            WHEN (
                len(tokens1)
                + len(tokens2)
                - shared_tokens
            ) > 0
            THEN
                CAST(shared_tokens AS DOUBLE)
                /
                (
                    len(tokens1)
                    + len(tokens2)
                    - shared_tokens
                )
            ELSE 0
        END AS jaccard,

        -- Overlap coefficient:
        -- intersection / smaller token set
        CASE
            WHEN LEAST(
                len(tokens1),
                len(tokens2)
            ) > 0
            THEN
                CAST(shared_tokens AS DOUBLE)
                /
                LEAST(
                    len(tokens1),
                    len(tokens2)
                )
            ELSE 0
        END AS overlap

    FROM similarity
)

SELECT
    'shared tokens >= 1 + house' AS rule,
    COUNT(*) AS candidates,
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    ) AS true_pairs

FROM metrics

WHERE shared_tokens >= 1
  AND same_house = 1

UNION ALL

SELECT
    'shared tokens >= 2 + house',
    COUNT(*),
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    )

FROM metrics

WHERE shared_tokens >= 2
  AND same_house = 1

UNION ALL

SELECT
    'shared tokens >= 3 + house',
    COUNT(*),
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    )

FROM metrics

WHERE shared_tokens >= 3
  AND same_house = 1

UNION ALL

SELECT
    'Jaccard >= 0.25 + house',
    COUNT(*),
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    )

FROM metrics

WHERE jaccard >= 0.25
  AND same_house = 1

UNION ALL

SELECT
    'Jaccard >= 0.50 + house',
    COUNT(*),
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    )

FROM metrics

WHERE jaccard >= 0.50
  AND same_house = 1

UNION ALL

SELECT
    'Jaccard >= 0.75 + house',
    COUNT(*),
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    )

FROM metrics

WHERE jaccard >= 0.75
  AND same_house = 1

UNION ALL

SELECT
    'Overlap >= 0.50 + house',
    COUNT(*),
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    )

FROM metrics

WHERE overlap >= 0.50
  AND same_house = 1

UNION ALL

SELECT
    'Overlap >= 0.75 + house',
    COUNT(*),
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    )

FROM metrics

WHERE overlap >= 0.75
  AND same_house = 1

UNION ALL

SELECT
    'Overlap = 1 + house',
    COUNT(*),
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    )

FROM metrics

WHERE overlap = 1
  AND same_house = 1

UNION ALL

SELECT
    'Jaccard >= 0.50 + address',
    COUNT(*),
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    )

FROM metrics

WHERE jaccard >= 0.50
  AND address_prefix5 = 1

UNION ALL

SELECT
    'Overlap >= 0.75 + address',
    COUNT(*),
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    )

FROM metrics

WHERE overlap >= 0.75
  AND address_prefix5 = 1

ORDER BY candidates
"""

rows = DB.execute(query).fetchall()

print()
print(
    f"{'RULE':<40}"
    f"{'CANDIDATES':>16}"
    f"{'TRUE':>16}"
    f"{'PRECISION':>15}"
)

print("-" * 90)

for rule, candidates, true_pairs in rows:

    candidates = candidates or 0
    true_pairs = true_pairs or 0

    precision = (
        true_pairs / candidates * 100
        if candidates > 0
        else 0
    )

    print(
        f"{rule:<40}"
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