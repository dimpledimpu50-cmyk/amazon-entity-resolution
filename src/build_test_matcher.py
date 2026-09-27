import duckdb
import time

DB = duckdb.connect()

DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")
DB.execute("SET preserve_insertion_order=false")

FEATURES = "intermediate/test_match_features.parquet"
OUTPUT = "intermediate/test_final_matches.parquet"

start = time.time()

print("=" * 100)
print("BUILDING FINAL TEST MATCHER")
print("=" * 100)


query = f"""
COPY (

    WITH scored AS (

        SELECT
            source1_entity_id,
            candidate_entity_id,

            CASE
                WHEN exact_name = 1
                 AND exact_address = 1
                THEN 1
                ELSE 0
            END AS tier1,

            CASE
                WHEN exact_name = 1
                 AND same_house = 1
                THEN 1
                ELSE 0
            END AS tier2,

            CASE
                WHEN name_prefix5 = 1
                 AND same_house = 1
                 AND address_prefix5 = 1
                THEN 1
                ELSE 0
            END AS tier3,

            CASE
                WHEN name_prefix4 = 1
                 AND same_house = 1
                 AND address_prefix5 = 1
                THEN 1
                ELSE 0
            END AS tier4,

            CASE
                WHEN name_contains = 1
                 AND same_house = 1
                THEN 1
                ELSE 0
            END AS tier5,

            CASE
                WHEN exact_compact_name = 1
                 AND same_house = 1
                THEN 1
                ELSE 0
            END AS exact_compact_house

        FROM read_parquet('{FEATURES}')
    ),

    final_decision AS (

        SELECT
            source1_entity_id,
            candidate_entity_id,

            CASE

                WHEN tier1 = 1
                    THEN 'TIER_1_EXACT_NAME_ADDRESS'

                WHEN tier2 = 1
                    THEN 'TIER_2_EXACT_NAME_HOUSE'

                WHEN exact_compact_house = 1
                    THEN 'TIER_2_COMPACT_NAME_HOUSE'

                WHEN tier3 = 1
                    THEN 'TIER_3_PREFIX5_HOUSE_ADDRESS'

                WHEN tier4 = 1
                    THEN 'TIER_4_PREFIX4_HOUSE_ADDRESS'

                WHEN tier5 = 1
                    THEN 'TIER_5_CONTAINS_HOUSE'

                ELSE NULL

            END AS match_rule

        FROM scored
    )

    SELECT
        source1_entity_id,
        candidate_entity_id,
        match_rule

    FROM final_decision

    WHERE match_rule IS NOT NULL

)
TO '{OUTPUT}'
(FORMAT PARQUET, COMPRESSION ZSTD);
"""


print("Running matcher...")
DB.execute(query)

print(f"Saved: {OUTPUT}")


# ------------------------------------------------------------
# Statistics
# ------------------------------------------------------------

stats = DB.execute(f"""
SELECT
    COUNT(*) AS predicted_matches,
    COUNT(DISTINCT source1_entity_id) AS matched_s1
FROM read_parquet('{OUTPUT}')
""").fetchone()

predicted_matches, matched_s1 = stats


total_s1 = DB.execute("""
SELECT COUNT(*)
FROM read_parquet('parquet/test_source1_normalized.parquet')
""").fetchone()[0]


print()
print("=" * 100)
print("FINAL TEST MATCHER RESULTS")
print("=" * 100)

print(f"Test S1 entities       : {total_s1:,}")
print(f"Predicted match pairs  : {predicted_matches:,}")
print(f"S1 entities matched    : {matched_s1:,}")
print(f"S1 entities unmatched  : {total_s1 - matched_s1:,}")

print()
print("MATCH RULE DISTRIBUTION")
print("-" * 100)

rows = DB.execute(f"""
SELECT
    match_rule,
    COUNT(*) AS matches,
    COUNT(DISTINCT source1_entity_id) AS s1_entities
FROM read_parquet('{OUTPUT}')
GROUP BY match_rule
ORDER BY matches DESC
""").fetchall()

for rule, count, s1_count in rows:
    print(
        f"{rule:<40}"
        f"{count:>15,}"
        f"{s1_count:>15,}"
    )

print()
print("=" * 100)
print("DONE")
print("=" * 100)

print(f"Elapsed: {(time.time() - start) / 60:.2f} minutes")