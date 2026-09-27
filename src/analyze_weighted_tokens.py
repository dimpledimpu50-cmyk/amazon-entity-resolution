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
print("WEIGHTED TOKEN SIMILARITY ANALYSIS")
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

-- ---------------------------------------------------------------
-- Build token frequencies in Source 2 + Source 3
-- ---------------------------------------------------------------

source_tokens AS (

    SELECT DISTINCT
        entity_id,
        unnest(
            regexp_split_to_array(
                trim(name_norm),
                '\\s+'
            )
        ) AS token

    FROM source_entities

    WHERE name_norm IS NOT NULL
),

token_frequency AS (

    SELECT
        token,
        COUNT(*) AS freq

    FROM source_tokens

    WHERE length(token) >= 3

    GROUP BY token
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

-- ---------------------------------------------------------------
-- Calculate weighted shared-token score
--
-- weight = 1 / sqrt(frequency)
--
-- Rare tokens therefore matter more.
-- ---------------------------------------------------------------

weighted AS (

    SELECT
        t.*,

        (
            SELECT COALESCE(
                SUM(
                    1.0 /
                    sqrt(
                        GREATEST(tf.freq, 1)
                    )
                ),
                0
            )

            FROM unnest(t.tokens1) AS a(token)

            JOIN token_frequency tf
                ON tf.token = a.token

            WHERE
                a.token IN (
                    SELECT unnest(t.tokens2)
                )
        ) AS shared_weight,

        (
            SELECT COALESCE(
                SUM(
                    1.0 /
                    sqrt(
                        GREATEST(tf.freq, 1)
                    )
                ),
                0
            )

            FROM unnest(t.tokens1) AS a(token)

            JOIN token_frequency tf
                ON tf.token = a.token
        ) AS total_weight_1,

        (
            SELECT COALESCE(
                SUM(
                    1.0 /
                    sqrt(
                        GREATEST(tf.freq, 1)
                    )
                ),
                0
            )

            FROM unnest(t.tokens2) AS a(token)

            JOIN token_frequency tf
                ON tf.token = a.token
        ) AS total_weight_2

    FROM tokenized t
),

metrics AS (

    SELECT
        *,

        CASE
            WHEN (
                total_weight_1
                + total_weight_2
                - shared_weight
            ) > 0

            THEN
                shared_weight /
                (
                    total_weight_1
                    + total_weight_2
                    - shared_weight
                )

            ELSE 0
        END AS weighted_jaccard,

        CASE
            WHEN LEAST(
                total_weight_1,
                total_weight_2
            ) > 0

            THEN
                shared_weight /
                LEAST(
                    total_weight_1,
                    total_weight_2
                )

            ELSE 0
        END AS weighted_overlap

    FROM weighted
)

SELECT
    'weighted Jaccard >= 0.50 + house' AS rule,
    COUNT(*) AS candidates,
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    ) AS true_pairs

FROM metrics

WHERE weighted_jaccard >= 0.50
  AND same_house = 1

UNION ALL

SELECT
    'weighted Jaccard >= 0.60 + house',
    COUNT(*),
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    )

FROM metrics

WHERE weighted_jaccard >= 0.60
  AND same_house = 1

UNION ALL

SELECT
    'weighted Jaccard >= 0.70 + house',
    COUNT(*),
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    )

FROM metrics

WHERE weighted_jaccard >= 0.70
  AND same_house = 1

UNION ALL

SELECT
    'weighted Jaccard >= 0.80 + house',
    COUNT(*),
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    )

FROM metrics

WHERE weighted_jaccard >= 0.80
  AND same_house = 1

UNION ALL

SELECT
    'weighted overlap >= 0.75 + house',
    COUNT(*),
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    )

FROM metrics

WHERE weighted_overlap >= 0.75
  AND same_house = 1

UNION ALL

SELECT
    'weighted overlap >= 0.85 + house',
    COUNT(*),
    COALESCE(
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END),
        0
    )

FROM metrics

WHERE weighted_overlap >= 0.85
  AND same_house = 1

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