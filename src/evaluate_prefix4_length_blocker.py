import duckdb
import time

start = time.time()

DB = duckdb.connect()

DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")
DB.execute("SET preserve_insertion_order=false")

S1 = "parquet/train_source1_normalized.parquet"
S2 = "parquet/train_source2_normalized.parquet"
S3 = "parquet/train_source3_normalized.parquet"

print("=" * 90)
print("PREFIX4 + NAME LENGTH BLOCKER ANALYSIS")
print("=" * 90)


# ============================================================
# 1. Source 1
# ============================================================

print("\nLoading Source 1...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW s1 AS
SELECT
    entity_id AS source1_entity_id,
    country_norm,
    name_compact,
    length(name_compact) AS name_len,
    left(name_compact, 4) AS name_prefix4
FROM read_parquet('{S1}')
WHERE name_compact IS NOT NULL
  AND length(name_compact) >= 4
""")


# ============================================================
# 2. Source 2 + Source 3
# ============================================================

print("Loading Source 2 + Source 3...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW source23 AS

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_compact,
    length(name_compact) AS name_len,
    left(name_compact, 4) AS name_prefix4
FROM read_parquet('{S2}')
WHERE name_compact IS NOT NULL
  AND length(name_compact) >= 4

UNION ALL

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_compact,
    length(name_compact) AS name_len,
    left(name_compact, 4) AS name_prefix4
FROM read_parquet('{S3}')
WHERE name_compact IS NOT NULL
  AND length(name_compact) >= 4
""")


# ============================================================
# 3. Candidate count
# ============================================================

print("\nCounting prefix4 + length<=2 candidates...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE blocker_candidates AS

SELECT
    a.source1_entity_id,
    b.candidate_entity_id

FROM s1 a

JOIN source23 b
    ON a.country_norm = b.country_norm
   AND a.name_prefix4 = b.name_prefix4
   AND abs(a.name_len - b.name_len) <= 2
""")


count = DB.execute("""
SELECT COUNT(*)
FROM blocker_candidates
""").fetchone()[0]

s1_count = DB.execute("""
SELECT COUNT(DISTINCT source1_entity_id)
FROM blocker_candidates
""").fetchone()[0]

stats = DB.execute("""
SELECT
    AVG(cnt),
    median(cnt),
    quantile_cont(cnt, 0.95),
    MAX(cnt)
FROM (
    SELECT
        source1_entity_id,
        COUNT(*) AS cnt
    FROM blocker_candidates
    GROUP BY source1_entity_id
)
""").fetchone()

print()
print("=" * 90)
print("CANDIDATE VOLUME")
print("=" * 90)

print(f"Candidates:       {count:,}")
print(f"S1 covered:       {s1_count:,}")
print(f"Average per S1:   {stats[0]:.2f}")
print(f"Median per S1:    {stats[1]:.2f}")
print(f"P95 per S1:       {stats[2]:.2f}")
print(f"Maximum per S1:   {stats[3]:,}")


# ============================================================
# 4. Compare against existing blocker
# ============================================================

print("\nComparing with existing candidate set...")

DB.execute("""
CREATE OR REPLACE TEMP VIEW existing AS
SELECT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet(
    'intermediate/train_candidate_pairs.parquet'
)
""")


new_count = DB.execute("""
SELECT COUNT(*)
FROM blocker_candidates b
LEFT JOIN existing e
    ON b.source1_entity_id = e.source1_entity_id
   AND b.candidate_entity_id = e.candidate_entity_id
WHERE e.candidate_entity_id IS NULL
""").fetchone()[0]

overlap = count - new_count

print(f"Total blocker candidates: {count:,}")
print(f"Already existing:          {overlap:,}")
print(f"NEW candidates:             {new_count:,}")


# ============================================================
# 5. Save candidate file
# ============================================================

print("\nSaving diagnostic candidate file...")

DB.execute("""
COPY blocker_candidates
TO 'intermediate/prefix4_length2_candidates.parquet'
(FORMAT PARQUET)
""")

print(
    "Saved: intermediate/prefix4_length2_candidates.parquet"
)


print("\n" + "=" * 90)
print("DONE")
print("=" * 90)

print(
    f"Elapsed time: {(time.time() - start) / 60:.2f} minutes"
)