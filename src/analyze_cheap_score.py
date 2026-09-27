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
print("OPTIMIZED CHEAP SCORE ANALYSIS")
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
    country_norm,
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
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact
FROM read_parquet('{S2}')

UNION ALL

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
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
SELECT COUNT(*) FROM gt
""").fetchone()[0]

print(f"Ground-truth pairs: {gt_count:,}")


# ================================================================
# 5. Create scored candidates
# ================================================================

print("\nCreating cheap scores...")
print("This is the main processing step...")

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
    ) AS cheap_score

FROM candidates c

JOIN s1
    ON c.source1_entity_id = s1.source1_entity_id

JOIN source23 s
    ON c.candidate_entity_id = s.candidate_entity_id
""")


scored_count = DB.execute("""
SELECT COUNT(*) FROM scored
""").fetchone()[0]

print(f"Scored candidates: {scored_count:,}")


# ================================================================
# 6. Mark true pairs using a temporary indexed-style table
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
CREATE OR REPLACE TEMP TABLE scored_marked AS
SELECT
    s.cheap_score,
    CASE
        WHEN g.source1_entity_id IS NOT NULL
        THEN TRUE
        ELSE FALSE
    END AS is_true
FROM scored s
LEFT JOIN gt_pairs g
    ON s.source1_entity_id = g.source1_entity_id
   AND s.candidate_entity_id = g.candidate_entity_id
""")

print("Ground-truth marking complete.")


# ================================================================
# 7. Score distribution
# ================================================================

print("\n")
print("=" * 90)
print("SCORE DISTRIBUTION")
print("=" * 90)

distribution = DB.execute("""
SELECT
    cheap_score,
    COUNT(*) AS candidates,
    SUM(CASE WHEN is_true THEN 1 ELSE 0 END) AS true_pairs
FROM scored_marked
GROUP BY cheap_score
ORDER BY cheap_score
""").fetchall()

print(
    f"{'Score':>8}"
    f"{'Candidates':>20}"
    f"{'True pairs':>20}"
    f"{'Precision':>14}"
)

print("-" * 70)

for score, candidates, true_pairs in distribution:

    precision = (
        true_pairs / candidates * 100
        if candidates else 0
    )

    print(
        f"{score:>8}"
        f"{candidates:>20,}"
        f"{true_pairs:>20,}"
        f"{precision:>13.2f}%"
    )


# ================================================================
# 8. Calculate cumulative threshold statistics in ONE query
# ================================================================

print("\nCalculating threshold statistics...")

thresholds = DB.execute("""
WITH threshold_values AS (
    SELECT range AS threshold
    FROM range(
        0,
        (SELECT MAX(cheap_score) FROM scored_marked) + 1
    )
),

stats AS (
    SELECT
        t.threshold,

        COUNT(*) FILTER (
            WHERE s.cheap_score >= t.threshold
        ) AS candidates,

        COUNT(*) FILTER (
            WHERE s.cheap_score >= t.threshold
              AND s.is_true
        ) AS true_pairs

    FROM threshold_values t
    CROSS JOIN scored_marked s

    GROUP BY t.threshold
)

SELECT
    threshold,
    candidates,
    true_pairs,
    true_pairs * 100.0 / {gt_count} AS recall,
    CASE
        WHEN candidates > 0
        THEN true_pairs * 100.0 / candidates
        ELSE 0
    END AS precision
FROM stats
ORDER BY threshold
""".format(gt_count=gt_count)).fetchall()


# ================================================================
# 9. Print threshold table
# ================================================================

print("\n")
print("=" * 105)
print("THRESHOLD ANALYSIS")
print("=" * 105)

print(
    f"{'Score >= ':>10}"
    f"{'Candidates':>20}"
    f"{'True pairs':>20}"
    f"{'Recall':>14}"
    f"{'Precision':>14}"
)

print("-" * 105)

for threshold, candidates, true_pairs, recall, precision in thresholds:

    print(
        f"{threshold:>10}"
        f"{candidates:>20,}"
        f"{true_pairs:>20,}"
        f"{recall:>13.2f}%"
        f"{precision:>13.2f}%"
    )


# ================================================================
# 10. Final
# ================================================================

print("\n")
print("=" * 90)
print("DONE")
print("=" * 90)

print(
    f"Elapsed time: {(time.time() - start) / 60:.2f} minutes"
)