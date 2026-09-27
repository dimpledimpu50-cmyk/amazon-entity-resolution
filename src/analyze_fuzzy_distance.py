import duckdb
import time

DB = duckdb.connect()

# Memory-efficient settings
DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")

FEATURES = "intermediate/train_match_features.parquet"

S1 = "parquet/train_source1_normalized.parquet"
S2 = "parquet/train_source2_normalized.parquet"
S3 = "parquet/train_source3_normalized.parquet"

print("=" * 100)
print("FUZZY DISTANCE ANALYSIS")
print("=" * 100)

start = time.time()

query = f"""
WITH source_entities AS (

    -- Source 2
    SELECT
        entity_id,
        name_compact
    FROM read_parquet('{S2}')

    UNION ALL

    -- Source 3
    SELECT
        entity_id,
        name_compact
    FROM read_parquet('{S3}')
),

candidates AS (

    SELECT
        f.source1_entity_id,
        f.candidate_entity_id,
        f.label,

        f.name_prefix4,
        f.name_prefix5,
        f.same_house,
        f.address_prefix5,
        f.name_contains,
        f.exact_compact_name,

        f.name_length_diff,

        levenshtein(
            s1.name_compact,
            s2.name_compact
        ) AS name_edit

    FROM read_parquet('{FEATURES}') AS f

    JOIN read_parquet('{S1}') AS s1
        ON f.source1_entity_id = s1.entity_id

    JOIN source_entities AS s2
        ON f.candidate_entity_id = s2.entity_id

    WHERE

        -- ---------------------------------------------------------
        -- Structural pre-filter
        -- ---------------------------------------------------------
        f.name_prefix4 = 1

        AND f.name_length_diff <= 5

        AND (
            f.same_house = 1
            OR f.address_prefix5 = 1
        )

        -- ---------------------------------------------------------
        -- Remove pairs already handled by strong deterministic tiers
        -- ---------------------------------------------------------

        -- Tier 2
        AND NOT (
            f.exact_compact_name = 1
            AND f.same_house = 1
        )

        -- Tier 3
        AND NOT (
            f.name_prefix5 = 1
            AND f.same_house = 1
            AND f.address_prefix5 = 1
        )

        -- Tier 4
        AND NOT (
            f.name_prefix4 = 1
            AND f.same_house = 1
            AND f.address_prefix5 = 1
        )

        -- Tier 5
        AND NOT (
            f.name_contains = 1
            AND f.same_house = 1
        )
)

SELECT

    COUNT(*) AS candidates,

    SUM(
        CASE
            WHEN label = 1 THEN 1
            ELSE 0
        END
    ) AS true_pairs,

    -- -------------------------------------------------------------
    -- Edit distance <= 1
    -- -------------------------------------------------------------

    SUM(
        CASE
            WHEN name_edit <= 1 THEN 1
            ELSE 0
        END
    ) AS d1_candidates,

    SUM(
        CASE
            WHEN name_edit <= 1
                 AND label = 1
            THEN 1
            ELSE 0
        END
    ) AS d1_true,

    -- -------------------------------------------------------------
    -- Edit distance <= 2
    -- -------------------------------------------------------------

    SUM(
        CASE
            WHEN name_edit <= 2 THEN 1
            ELSE 0
        END
    ) AS d2_candidates,

    SUM(
        CASE
            WHEN name_edit <= 2
                 AND label = 1
            THEN 1
            ELSE 0
        END
    ) AS d2_true,

    -- -------------------------------------------------------------
    -- Edit distance <= 3
    -- -------------------------------------------------------------

    SUM(
        CASE
            WHEN name_edit <= 3 THEN 1
            ELSE 0
        END
    ) AS d3_candidates,

    SUM(
        CASE
            WHEN name_edit <= 3
                 AND label = 1
            THEN 1
            ELSE 0
        END
    ) AS d3_true,

    -- -------------------------------------------------------------
    -- Edit distance <= 4
    -- -------------------------------------------------------------

    SUM(
        CASE
            WHEN name_edit <= 4 THEN 1
            ELSE 0
        END
    ) AS d4_candidates,

    SUM(
        CASE
            WHEN name_edit <= 4
                 AND label = 1
            THEN 1
            ELSE 0
        END
    ) AS d4_true,

    -- -------------------------------------------------------------
    -- Edit distance <= 5
    -- -------------------------------------------------------------

    SUM(
        CASE
            WHEN name_edit <= 5 THEN 1
            ELSE 0
        END
    ) AS d5_candidates,

    SUM(
        CASE
            WHEN name_edit <= 5
                 AND label = 1
            THEN 1
            ELSE 0
        END
    ) AS d5_true

FROM candidates
"""

print("\nRunning Levenshtein analysis...")
print("This may take a few minutes because names are being compared.\n")

row = DB.execute(query).fetchone()

(
    total,
    total_true,

    d1_candidates,
    d1_true,

    d2_candidates,
    d2_true,

    d3_candidates,
    d3_true,

    d4_candidates,
    d4_true,

    d5_candidates,
    d5_true
) = row


# =================================================================
# Display results
# =================================================================

print("=" * 100)
print("RESULTS")
print("=" * 100)

print()
print(f"Structural fuzzy candidates : {total:,}")
print(f"True pairs                  : {total_true:,}")

print()

print(
    f"{'EDIT DISTANCE':<20}"
    f"{'CANDIDATES':>18}"
    f"{'TRUE':>18}"
    f"{'PRECISION':>16}"
)

print("-" * 72)


results = [
    ("<= 1", d1_candidates, d1_true),
    ("<= 2", d2_candidates, d2_true),
    ("<= 3", d3_candidates, d3_true),
    ("<= 4", d4_candidates, d4_true),
    ("<= 5", d5_candidates, d5_true),
]

for distance, candidates, true_pairs in results:

    precision = (
        true_pairs / candidates * 100
        if candidates > 0
        else 0
    )

    print(
        f"{distance:<20}"
        f"{candidates:>18,}"
        f"{true_pairs:>18,}"
        f"{precision:>15.2f}%"
    )


print()
print("=" * 100)
print("DONE")
print("=" * 100)

elapsed = (time.time() - start) / 60

print(f"Elapsed: {elapsed:.2f} minutes")
print("=" * 100)