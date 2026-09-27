import duckdb
import time

con = duckdb.connect()
con.execute("SET memory_limit='6GB'")
con.execute("SET threads=2")
con.execute("SET preserve_insertion_order=false")

start = time.time()

CANDIDATES = "intermediate/train_candidate_pairs_r15.parquet"
S1 = "parquet/train_source1_normalized.parquet"
S2 = "parquet/train_source2_normalized.parquet"
S3 = "parquet/train_source3_normalized.parquet"

OUTPUT = "intermediate/train_r15_similarity_features.parquet"

print("=" * 90)
print("BUILDING R15 SIMILARITY FEATURES")
print("=" * 90)

candidate_count = con.execute(f"""
SELECT COUNT(*)
FROM read_parquet('{CANDIDATES}')
""").fetchone()[0]

print(f"\nCandidate pairs: {candidate_count:,}")

print("\nBuilding features...")

con.execute(f"""
COPY (
    SELECT
        c.source1_entity_id,
        c.candidate_entity_id,

        CASE
            WHEN s1.country_norm = s23.country_norm
            THEN 1 ELSE 0
        END AS country_equal,

        CASE
            WHEN s1.name_norm = s23.name_norm
            THEN 1 ELSE 0
        END AS exact_name,

        CASE
            WHEN s1.name_compact = s23.name_compact
            THEN 1 ELSE 0
        END AS exact_compact_name,

        CASE
            WHEN LEFT(s1.name_compact, 4) =
                 LEFT(s23.name_compact, 4)
            THEN 1 ELSE 0
        END AS name_prefix4,

        CASE
            WHEN LEFT(s1.name_compact, 5) =
                 LEFT(s23.name_compact, 5)
            THEN 1 ELSE 0
        END AS name_prefix5,

        CASE
            WHEN RIGHT(s1.name_compact, 4) =
                 RIGHT(s23.name_compact, 4)
            THEN 1 ELSE 0
        END AS name_suffix4,

        CASE
            WHEN POSITION(s1.name_compact IN s23.name_compact) > 0
              OR POSITION(s23.name_compact IN s1.name_compact) > 0
            THEN 1 ELSE 0
        END AS name_contains,

        ABS(
            LENGTH(s1.name_compact) -
            LENGTH(s23.name_compact)
        ) AS name_length_diff,

        CASE
            WHEN s1.address_norm = s23.address_norm
            THEN 1 ELSE 0
        END AS exact_address,

        CASE
            WHEN s1.address_compact = s23.address_compact
            THEN 1 ELSE 0
        END AS exact_compact_address,

        CASE
            WHEN LEFT(s1.address_compact, 5) =
                 LEFT(s23.address_compact, 5)
            THEN 1 ELSE 0
        END AS address_prefix5,

        CASE
            WHEN regexp_extract(s1.address_compact, '[0-9]+')
               =
               regexp_extract(s23.address_compact, '[0-9]+')
            AND regexp_extract(s1.address_compact, '[0-9]+') <> ''
            THEN 1 ELSE 0
        END AS same_house,

        ABS(
            LENGTH(s1.address_compact) -
            LENGTH(s23.address_compact)
        ) AS address_length_diff,

        CASE
            WHEN LEFT(s1.name_compact, 4) =
                 LEFT(s23.name_compact, 4)
             AND
                 regexp_extract(s1.address_compact, '[0-9]+')
                 =
                 regexp_extract(s23.address_compact, '[0-9]+')
             AND
                 regexp_extract(s1.address_compact, '[0-9]+') <> ''
            THEN 1 ELSE 0
        END AS prefix4_same_house,

        CASE
            WHEN LEFT(s1.name_compact, 4) =
                 LEFT(s23.name_compact, 4)
             AND
                 LEFT(s1.address_compact, 5) =
                 LEFT(s23.address_compact, 5)
            THEN 1 ELSE 0
        END AS prefix4_address_prefix,

        CASE
            WHEN s1.name_compact = s23.name_compact
             AND
                 regexp_extract(s1.address_compact, '[0-9]+')
                 =
                 regexp_extract(s23.address_compact, '[0-9]+')
             AND
                 regexp_extract(s1.address_compact, '[0-9]+') <> ''
            THEN 1 ELSE 0
        END AS exact_name_same_house,

        CASE
            WHEN s1.name_compact = s23.name_compact
             AND s1.address_compact = s23.address_compact
            THEN 1 ELSE 0
        END AS exact_name_exact_address,

        jaro_winkler_similarity(
            s1.name_compact,
            s23.name_compact
        ) AS name_jaro,

        levenshtein(
            s1.name_compact,
            s23.name_compact
        ) AS name_levenshtein,

        CASE
            WHEN GREATEST(
                LENGTH(s1.name_compact),
                LENGTH(s23.name_compact)
            ) = 0
            THEN 1.0
            ELSE LEAST(
                LENGTH(s1.name_compact),
                LENGTH(s23.name_compact)
            ) * 1.0
            /
            GREATEST(
                LENGTH(s1.name_compact),
                LENGTH(s23.name_compact)
            )
        END AS name_length_ratio,

        jaro_winkler_similarity(
            s1.address_compact,
            s23.address_compact
        ) AS address_jaro,

        levenshtein(
            s1.address_compact,
            s23.address_compact
        ) AS address_levenshtein,

        CASE
            WHEN GREATEST(
                LENGTH(s1.address_compact),
                LENGTH(s23.address_compact)
            ) = 0
            THEN 1.0
            ELSE LEAST(
                LENGTH(s1.address_compact),
                LENGTH(s23.address_compact)
            ) * 1.0
            /
            GREATEST(
                LENGTH(s1.address_compact),
                LENGTH(s23.address_compact)
            )
        END AS address_length_ratio

    FROM read_parquet('{CANDIDATES}') c

    JOIN read_parquet('{S1}') s1
        ON c.source1_entity_id = s1.entity_id

    JOIN (
        SELECT * FROM read_parquet('{S2}')
        UNION ALL
        SELECT * FROM read_parquet('{S3}')
    ) s23
        ON c.candidate_entity_id = s23.entity_id

) TO '{OUTPUT}'
(FORMAT PARQUET, COMPRESSION ZSTD);
""")

rows = con.execute(f"""
SELECT COUNT(*)
FROM read_parquet('{OUTPUT}')
""").fetchone()[0]

print("\n" + "=" * 90)
print("FEATURE BUILD COMPLETE")
print("=" * 90)

print(f"Feature rows: {rows:,}")
print(f"Output: {OUTPUT}")
print(f"Elapsed: {(time.time() - start) / 60:.2f} minutes")
print("=" * 90)