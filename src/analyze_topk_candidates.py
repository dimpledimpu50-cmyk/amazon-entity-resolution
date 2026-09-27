import duckdb
import time

DB = duckdb.connect()

DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")
DB.execute("SET preserve_insertion_order=false")

CANDIDATES = "intermediate/train_candidate_pairs.parquet"
S1 = "parquet/train_source1_normalized.parquet"
S2 = "parquet/train_source2_normalized.parquet"
S3 = "parquet/train_source3_normalized.parquet"
GT = "dataset/train/train_ground_truth.tsv"

start = time.time()

print("=" * 90)
print("TOP-K CHEAP CANDIDATE ANALYSIS")
print("=" * 90)


# ================================================================
# 1. Candidates
# ================================================================

print("\nLoading candidates...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW candidates AS
SELECT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet('{CANDIDATES}')
""")

candidate_count = DB.execute("""
SELECT COUNT(*) FROM candidates
""").fetchone()[0]

print(f"Candidates: {candidate_count:,}")


# ================================================================
# 2. Source 1
# ================================================================

print("Loading Source 1...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW s1 AS
SELECT
    entity_id AS source1_entity_id,
    name_norm,
    name_compact,
    address_norm,
    address_compact
FROM read_parquet('{S1}')
""")


# ================================================================
# 3. Source 2 + Source 3
# ================================================================

print("Loading Source 2 + Source 3...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW source23 AS

SELECT
    entity_id AS candidate_entity_id,
    name_norm,
    name_compact,
    address_norm,
    address_compact
FROM read_parquet('{S2}')

UNION ALL

SELECT
    entity_id AS candidate_entity_id,
    name_norm,
    name_compact,
    address_norm,
    address_compact
FROM read_parquet('{S3}')
""")


# ================================================================
# 4. Ground truth
# ================================================================

print("Loading ground truth...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW gt AS
SELECT
    source1_entity_id,
    unnest(string_split(matched_entity_ids, ',')) AS candidate_entity_id
FROM read_csv(
    '{GT}',
    delim='\\t',
    header=true,
    columns={{
        'source1_entity_id': 'VARCHAR',
        'matched_entity_ids': 'VARCHAR'
    }}
)
WHERE matched_entity_ids IS NOT NULL
  AND matched_entity_ids <> ''
""")

gt_count = DB.execute("""
SELECT COUNT(*)
FROM gt
""").fetchone()[0]

print(f"Ground-truth pairs: {gt_count:,}")


# ================================================================
# 5. Calculate cheap ranking score
# ================================================================

print("\nCalculating cheap ranking score...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE scored AS

SELECT
    c.source1_entity_id,
    c.candidate_entity_id,

    (
        CASE
            WHEN s1.name_norm IS NOT NULL
             AND s.name_norm IS NOT NULL
             AND s1.name_norm = s.name_norm
            THEN 5 ELSE 0
        END

        +

        CASE
            WHEN s1.name_compact IS NOT NULL
             AND s.name_compact IS NOT NULL
             AND s1.name_compact = s.name_compact
            THEN 5 ELSE 0
        END

        +

        CASE
            WHEN s1.address_compact IS NOT NULL
             AND s.address_compact IS NOT NULL
             AND s1.address_compact = s.address_compact
            THEN 5 ELSE 0
        END

        +

        CASE
            WHEN regexp_extract(s1.address_norm, '[0-9]+')
               = regexp_extract(s.address_norm, '[0-9]+')
             AND regexp_extract(s1.address_norm, '[0-9]+') <> ''
            THEN 2 ELSE 0
        END

        +

        CASE
            WHEN length(s1.name_compact) >= 4
             AND length(s.name_compact) >= 4
             AND left(s1.name_compact, 4)
                 = left(s.name_compact, 4)
            THEN 2 ELSE 0
        END

        +

        CASE
            WHEN length(s1.name_compact) >= 5
             AND length(s.name_compact) >= 5
             AND left(s1.name_compact, 5)
                 = left(s.name_compact, 5)
            THEN 2 ELSE 0
        END

        +

        CASE
            WHEN length(s1.name_compact) >= 5
             AND length(s.name_compact) >= 5
             AND (
                 position(s1.name_compact IN s.name_compact) > 0
                 OR
                 position(s.name_compact IN s1.name_compact) > 0
             )
            THEN 2 ELSE 0
        END

        +

        CASE
            WHEN abs(
                length(coalesce(s1.name_compact, ''))
                -
                length(coalesce(s.name_compact, ''))
            ) <= 2
            THEN 1 ELSE 0
        END

        +

        CASE
            WHEN abs(
                length(coalesce(s1.address_compact, ''))
                -
                length(coalesce(s.address_compact, ''))
            ) <= 10
            THEN 1 ELSE 0
        END
    ) AS cheap_score,

    -- Extra tie-breaking signals.
    -- These do NOT change the primary score.
    CASE
        WHEN s1.name_compact IS NOT NULL
         AND s.name_compact IS NOT NULL
         AND s1.name_compact = s.name_compact
        THEN 1 ELSE 0
    END AS exact_name,

    CASE
        WHEN s1.address_compact IS NOT NULL
         AND s.address_compact IS NOT NULL
         AND s1.address_compact = s.address_compact
        THEN 1 ELSE 0
    END AS exact_address

FROM candidates c

JOIN s1
    ON c.source1_entity_id = s1.source1_entity_id

JOIN source23 s
    ON c.candidate_entity_id = s.candidate_entity_id
""")


print("Cheap scores calculated.")


# ================================================================
# 6. Rank candidates within each S1
# ================================================================

print("\nRanking candidates per Source 1 entity...")
print("This may take several minutes because there are 82.9M rows.")

DB.execute("""
CREATE OR REPLACE TEMP TABLE ranked AS

SELECT
    source1_entity_id,
    candidate_entity_id,
    cheap_score,

    ROW_NUMBER() OVER (
        PARTITION BY source1_entity_id
        ORDER BY
            cheap_score DESC,
            exact_name DESC,
            exact_address DESC,
            candidate_entity_id
    ) AS rank

FROM scored
""")


ranked_count = DB.execute("""
SELECT COUNT(*)
FROM ranked
""").fetchone()[0]

print(f"Ranked candidates: {ranked_count:,}")


# ================================================================
# 7. Mark true pairs
# ================================================================

print("\nMarking ground-truth pairs...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE gt_pairs AS
SELECT DISTINCT
    source1_entity_id,
    candidate_entity_id
FROM gt
""")

DB.execute("""
CREATE OR REPLACE TEMP TABLE ranked_marked AS

SELECT
    r.source1_entity_id,
    r.candidate_entity_id,
    r.rank,

    CASE
        WHEN g.source1_entity_id IS NOT NULL
        THEN TRUE
        ELSE FALSE
    END AS is_true

FROM ranked r

LEFT JOIN gt_pairs g
    ON r.source1_entity_id = g.source1_entity_id
   AND r.candidate_entity_id = g.candidate_entity_id
""")


# ================================================================
# 8. Top-K evaluation
# ================================================================

print("\nCalculating Top-K recall...")

k_values = [1, 2, 3, 5, 10, 15, 20, 25, 50, 100, 200]

print()
print("=" * 100)
print("TOP-K RESULTS")
print("=" * 100)

print(
    f"{'K':>8}"
    f"{'Candidates':>20}"
    f"{'True pairs':>20}"
    f"{'Recall':>14}"
    f"{'Precision':>14}"
)

print("-" * 100)

for k in k_values:

    candidates = DB.execute(f"""
        SELECT COUNT(*)
        FROM ranked_marked
        WHERE rank <= {k}
    """).fetchone()[0]

    true_pairs = DB.execute(f"""
        SELECT COUNT(*)
        FROM ranked_marked
        WHERE rank <= {k}
          AND is_true
    """).fetchone()[0]

    recall = true_pairs / gt_count * 100
    precision = (
        true_pairs / candidates * 100
        if candidates else 0
    )

    print(
        f"{k:>8}"
        f"{candidates:>20,}"
        f"{true_pairs:>20,}"
        f"{recall:>13.2f}%"
        f"{precision:>13.2f}%"
    )


# ================================================================
# 9. Check maximum true matches per S1
# ================================================================

print("\nChecking one-to-many ground truth...")

max_matches = DB.execute("""
SELECT
    MAX(match_count)
FROM (
    SELECT
        source1_entity_id,
        COUNT(*) AS match_count
    FROM gt_pairs
    GROUP BY source1_entity_id
)
""").fetchone()[0]

print(f"Maximum true matches for one S1: {max_matches}")


# ================================================================
# 10. Check how many S1 entities have > K true matches
# ================================================================

print()
print("=" * 100)
print("GROUND-TRUTH MATCH COUNT DISTRIBUTION")
print("=" * 100)

distribution = DB.execute("""
SELECT
    match_count,
    COUNT(*) AS s1_entities
FROM (
    SELECT
        source1_entity_id,
        COUNT(*) AS match_count
    FROM gt_pairs
    GROUP BY source1_entity_id
)
GROUP BY match_count
ORDER BY match_count
""").fetchall()

for match_count, entities in distribution:
    print(
        f"{match_count:>4} true matches -> "
        f"{entities:>12,} S1 entities"
    )


# ================================================================
# 11. Save ranked candidates for possible next step
# ================================================================

print("\nSaving ranked candidate information...")

DB.execute("""
COPY (
    SELECT
        source1_entity_id,
        candidate_entity_id,
        cheap_score,
        rank
    FROM ranked
)
TO 'intermediate/ranked_candidates.parquet'
(FORMAT PARQUET)
""")

print("Saved:")
print("intermediate/ranked_candidates.parquet")


# ================================================================
# Done
# ================================================================

print()
print("=" * 90)
print("DONE")
print("=" * 90)

print(
    f"Elapsed time: {(time.time() - start) / 60:.2f} minutes"
)