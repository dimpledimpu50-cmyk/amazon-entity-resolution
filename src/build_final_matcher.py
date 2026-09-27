import duckdb
import time

DB = duckdb.connect()

DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")

FEATURES = "intermediate/train_match_features.parquet"
OUTPUT = "intermediate/train_final_matches.parquet"

start = time.time()

print("=" * 100)
print("BUILDING FINAL MATCHER")
print("=" * 100)


query = f"""
COPY (

    WITH scored AS (

        SELECT
            source1_entity_id,
            candidate_entity_id,
            label,

            /*
            ============================================================
            TIER 1 — EXACT NAME + EXACT ADDRESS
            ============================================================
            */

            CASE
                WHEN exact_name = 1
                 AND exact_address = 1
                THEN 1
                ELSE 0
            END AS tier1,

            /*
            ============================================================
            TIER 2 — EXACT NAME + SAME HOUSE
            ============================================================
            */

            CASE
                WHEN exact_name = 1
                 AND same_house = 1
                THEN 1
                ELSE 0
            END AS tier2,

            /*
            ============================================================
            TIER 3 — PREFIX5 + HOUSE + ADDRESS PREFIX
            ============================================================
            */

            CASE
                WHEN name_prefix5 = 1
                 AND same_house = 1
                 AND address_prefix5 = 1
                THEN 1
                ELSE 0
            END AS tier3,

            /*
            ============================================================
            TIER 4 — PREFIX4 + HOUSE + ADDRESS PREFIX
            ============================================================
            */

            CASE
                WHEN name_prefix4 = 1
                 AND same_house = 1
                 AND address_prefix5 = 1
                THEN 1
                ELSE 0
            END AS tier4,

            /*
            ============================================================
            TIER 5 — NAME CONTAINMENT + SAME HOUSE
            ============================================================
            */

            CASE
                WHEN name_contains = 1
                 AND same_house = 1
                THEN 1
                ELSE 0
            END AS tier5,

            /*
            ============================================================
            EXTRA STRONG SIGNAL
            ============================================================
            */

            CASE
                WHEN exact_compact_name = 1
                 AND same_house = 1
                THEN 1
                ELSE 0
            END AS exact_compact_house,

            /*
            ============================================================
            MODERATE SIGNALS
            ============================================================
            */

            CASE
                WHEN name_prefix4 = 1
                 AND same_house = 1
                THEN 1
                ELSE 0
            END AS prefix4_house,

            CASE
                WHEN name_prefix5 = 1
                 AND same_house = 1
                THEN 1
                ELSE 0
            END AS prefix5_house,

            CASE
                WHEN name_prefix4 = 1
                 AND address_prefix5 = 1
                THEN 1
                ELSE 0
            END AS prefix4_address,

            /*
            ============================================================
            WEAK SIGNALS — ONLY USED IN COMBINATION
            ============================================================
            */

            CASE
                WHEN name_prefix4 = 1
                 AND name_length_diff <= 2
                THEN 1
                ELSE 0
            END AS prefix4_short_diff,

            CASE
                WHEN name_prefix5 = 1
                 AND name_length_diff <= 2
                THEN 1
                ELSE 0
            END AS prefix5_short_diff

        FROM read_parquet('{FEATURES}')
    ),

    final_decision AS (

        SELECT
            source1_entity_id,
            candidate_entity_id,
            label,

            CASE

                /*
                --------------------------------------------------------
                HIGHEST CONFIDENCE
                --------------------------------------------------------
                */

                WHEN tier1 = 1
                    THEN 'TIER_1_EXACT_NAME_ADDRESS'

                WHEN tier2 = 1
                    THEN 'TIER_2_EXACT_NAME_HOUSE'

                WHEN exact_compact_house = 1
                    THEN 'TIER_2_COMPACT_NAME_HOUSE'

                /*
                --------------------------------------------------------
                STRONG FUZZY + ADDRESS
                --------------------------------------------------------
                */

                WHEN tier3 = 1
                    THEN 'TIER_3_PREFIX5_HOUSE_ADDRESS'

                WHEN tier4 = 1
                    THEN 'TIER_4_PREFIX4_HOUSE_ADDRESS'

                WHEN tier5 = 1
                    THEN 'TIER_5_CONTAINS_HOUSE'

                /*
                --------------------------------------------------------
                ADDITIONAL CONSERVATIVE RULE
                --------------------------------------------------------
               
                --------------------------------------------------------
                DO NOT ACCEPT ADDRESS-ONLY OR NAME-ONLY MATCHES
                --------------------------------------------------------
                */

                ELSE NULL

            END AS match_rule

        FROM scored
    )

    SELECT
        source1_entity_id,
        candidate_entity_id,
        label,
        match_rule

    FROM final_decision

    WHERE match_rule IS NOT NULL

)
TO '{OUTPUT}'
(FORMAT PARQUET);
"""


print("Running matcher...")

DB.execute(query)

print(f"Saved: {OUTPUT}")

# ------------------------------------------------------------
# Evaluate
# ------------------------------------------------------------

stats = DB.execute(f"""
SELECT
    COUNT(*) AS predicted_matches,

    SUM(
        CASE
            WHEN label = 1 THEN 1
            ELSE 0
        END
    ) AS true_matches,

    SUM(
        CASE
            WHEN label = 0 THEN 1
            ELSE 0
        END
    ) AS false_matches

FROM read_parquet('{OUTPUT}')
""").fetchone()

predicted, true_matches, false_matches = stats

precision = (
    true_matches / predicted
    if predicted > 0
    else 0
)

total_true = DB.execute(f"""
SELECT SUM(label)
FROM read_parquet('{FEATURES}')
""").fetchone()[0]

recall = (
    true_matches / total_true
    if total_true > 0
    else 0
)

if precision + recall > 0:
    f05 = (
        1.25 * precision * recall
        /
        (0.25 * precision + recall)
    )
else:
    f05 = 0


print()
print("=" * 100)
print("FINAL MATCHER RESULTS")
print("=" * 100)

print(f"Candidate pairs        : {total_true:,} true pairs in blocker")
print(f"Predicted matches      : {predicted:,}")
print(f"True matches           : {true_matches:,}")
print(f"False matches          : {false_matches:,}")
print(f"Precision              : {precision * 100:.2f}%")
print(f"Recall                 : {recall * 100:.2f}%")
print(f"F0.5                   : {f05:.4f}")

print()
print("MATCH RULE DISTRIBUTION")
print("-" * 100)

rows = DB.execute(f"""
SELECT
    match_rule,
    COUNT(*) AS candidates,
    SUM(label) AS true_matches,
    ROUND(
        100.0 * SUM(label) / COUNT(*),
        2
    ) AS precision
FROM read_parquet('{OUTPUT}')
GROUP BY match_rule
ORDER BY candidates DESC
""").fetchall()

for rule, count, true_count, p in rows:
    print(
        f"{rule:<40}"
        f"{count:>15,}"
        f"{true_count:>15,}"
        f"{p:>10.2f}%"
    )

print()
print("=" * 100)
print("DONE")
print("=" * 100)

print(f"Elapsed: {(time.time() - start) / 60:.2f} minutes")