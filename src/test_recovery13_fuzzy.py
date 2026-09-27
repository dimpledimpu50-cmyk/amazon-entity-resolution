import duckdb

DB = duckdb.connect()

print("=" * 80)
print("R13 — SELECTIVE FUZZY NAME + ADDRESS BLOCKER")
print("=" * 80)

S1_EVAL = "intermediate/blocking_eval_s1.parquet"
TRUE_PAIRS = "intermediate/blocking_eval_true_pairs.parquet"

BASELINE = "intermediate/verified_baseline_candidates.parquet"
R7 = "intermediate/recovery7_candidates.parquet"
R10 = "intermediate/recovery10_candidates.parquet"
R11 = "intermediate/recovery11_candidates.parquet"

SOURCE2 = "parquet/train_source2_normalized.parquet"
SOURCE3 = "parquet/train_source3_normalized.parquet"

OUTPUT = "intermediate/recovery13_candidates.parquet"


# ============================================================================
# 1. Evaluation S1
# ============================================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW s1 AS
SELECT
    entity_id AS source1_entity_id,
    country_norm,
    name_compact,
    address_norm,
    address_compact,

    left(name_compact, 4) AS name_prefix4,

    TRY_CAST(
        regexp_extract(address_norm, '[0-9]+')
        AS BIGINT
    ) AS house_number,

    left(address_compact, 5) AS address_prefix5

FROM read_parquet('{S1_EVAL}')
""")


# ============================================================================
# 2. Source 2 + Source 3
# ============================================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW source_data AS

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_compact,
    address_norm,
    address_compact,

    left(name_compact, 4) AS name_prefix4,

    TRY_CAST(
        regexp_extract(address_norm, '[0-9]+')
        AS BIGINT
    ) AS house_number,

    left(address_compact, 5) AS address_prefix5

FROM read_parquet('{SOURCE2}')

UNION ALL

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_compact,
    address_norm,
    address_compact,

    left(name_compact, 4) AS name_prefix4,

    TRY_CAST(
        regexp_extract(address_norm, '[0-9]+')
        AS BIGINT
    ) AS house_number,

    left(address_compact, 5) AS address_prefix5

FROM read_parquet('{SOURCE3}')
""")


# ============================================================================
# 3. Generate restricted structural candidates
#
# IMPORTANT:
# Edit distance is NOT calculated against all 10M rows.
# It is calculated only after the cheap blocking conditions.
# ============================================================================

print()
print("Generating structural candidates...")

DB.execute("""
CREATE OR REPLACE TEMP VIEW structural_candidates AS

SELECT DISTINCT
    s.source1_entity_id,
    c.candidate_entity_id,
    s.name_compact AS s1_name_compact,
    c.name_compact AS candidate_name_compact

FROM s1 s

JOIN source_data c
    ON s.country_norm = c.country_norm
   AND s.name_prefix4 = c.name_prefix4
   AND s.name_prefix4 <> ''

   AND (
        (
            s.house_number IS NOT NULL
            AND c.house_number IS NOT NULL
            AND s.house_number = c.house_number
        )
        OR
        (
            s.address_prefix5 <> ''
            AND c.address_prefix5 <> ''
            AND s.address_prefix5 = c.address_prefix5
        )
   )
""")

structural_count = DB.execute("""
SELECT COUNT(*)
FROM structural_candidates
""").fetchone()[0]

print(f"Structural candidates: {structural_count:,}")


# ============================================================================
# 4. Apply fuzzy name condition
# ============================================================================

print()
print("Applying name edit distance <= 3...")

DB.execute("""
CREATE OR REPLACE TEMP VIEW r13_candidates AS

SELECT
    source1_entity_id,
    candidate_entity_id

FROM structural_candidates

WHERE editdist3(
    s1_name_compact,
    candidate_name_compact
) <= 3
""")

r13_count = DB.execute("""
SELECT COUNT(*)
FROM r13_candidates
""").fetchone()[0]

r13_s1 = DB.execute("""
SELECT COUNT(DISTINCT source1_entity_id)
FROM r13_candidates
""").fetchone()[0]

print(f"R13 candidates: {r13_count:,}")
print(f"S1 with R13 candidates: {r13_s1:,}")


# ============================================================================
# 5. Save R13
# ============================================================================

print()
print("Saving R13 candidates...")

DB.execute(f"""
COPY r13_candidates
TO '{OUTPUT}'
(FORMAT PARQUET)
""")

print(f"Saved: {OUTPUT}")


# ============================================================================
# 6. Load true pairs
# ============================================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW true_pairs AS

SELECT
    s1_id AS source1_entity_id,
    candidate_id AS candidate_entity_id

FROM read_parquet('{TRUE_PAIRS}')
""")


# ============================================================================
# 7. Current blocker union
# ============================================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW current_blocking AS

SELECT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet('{BASELINE}')

UNION

SELECT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet('{R7}')

UNION

SELECT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet('{R10}')

UNION

SELECT
    s1_id AS source1_entity_id,
    candidate_id AS candidate_entity_id
FROM read_parquet('{R11}')
""")


# ============================================================================
# 8. Current baseline statistics
# ============================================================================

baseline_candidates = DB.execute("""
SELECT COUNT(*)
FROM current_blocking
""").fetchone()[0]

baseline_true = DB.execute("""
SELECT COUNT(*)
FROM true_pairs t
JOIN current_blocking b
    ON t.source1_entity_id = b.source1_entity_id
   AND t.candidate_entity_id = b.candidate_entity_id
""").fetchone()[0]

total_true = DB.execute("""
SELECT COUNT(*)
FROM true_pairs
""").fetchone()[0]


# ============================================================================
# 9. R13 standalone true pairs
# ============================================================================

r13_true = DB.execute("""
SELECT COUNT(*)
FROM true_pairs t
JOIN r13_candidates r
    ON t.source1_entity_id = r.source1_entity_id
   AND t.candidate_entity_id = r.candidate_entity_id
""").fetchone()[0]


# ============================================================================
# 10. NEW candidates beyond current blocker
# ============================================================================

new_candidates = DB.execute("""
SELECT COUNT(*)
FROM r13_candidates r
LEFT JOIN current_blocking b
    ON r.source1_entity_id = b.source1_entity_id
   AND r.candidate_entity_id = b.candidate_entity_id
WHERE b.source1_entity_id IS NULL
""").fetchone()[0]


# ============================================================================
# 11. NEW true pairs beyond current blocker
# ============================================================================

new_true_pairs = DB.execute("""
SELECT COUNT(*)
FROM true_pairs t
JOIN r13_candidates r
    ON t.source1_entity_id = r.source1_entity_id
   AND t.candidate_entity_id = r.candidate_entity_id

LEFT JOIN current_blocking b
    ON t.source1_entity_id = b.source1_entity_id
   AND t.candidate_entity_id = b.candidate_entity_id

WHERE b.source1_entity_id IS NULL
""").fetchone()[0]


# ============================================================================
# 12. Combined candidates
# ============================================================================

combined_candidates = DB.execute("""
SELECT COUNT(*)
FROM (
    SELECT
        source1_entity_id,
        candidate_entity_id
    FROM current_blocking

    UNION

    SELECT
        source1_entity_id,
        candidate_entity_id
    FROM r13_candidates
) x
""").fetchone()[0]


# ============================================================================
# 13. Combined true pairs
# ============================================================================

combined_true = DB.execute("""
SELECT COUNT(*)
FROM true_pairs t

JOIN (
    SELECT
        source1_entity_id,
        candidate_entity_id
    FROM current_blocking

    UNION

    SELECT
        source1_entity_id,
        candidate_entity_id
    FROM r13_candidates
) x

ON t.source1_entity_id = x.source1_entity_id
AND t.candidate_entity_id = x.candidate_entity_id
""").fetchone()[0]


# ============================================================================
# 14. Final statistics
# ============================================================================

baseline_recall = baseline_true / total_true * 100
r13_recall = r13_true / total_true * 100
combined_recall = combined_true / total_true * 100

print()
print("=" * 80)
print("R13 RESULTS")
print("=" * 80)

print(f"Total true pairs              : {total_true:,}")

print()
print("CURRENT BASELINE")
print(f"Candidates                    : {baseline_candidates:,}")
print(f"True pairs found              : {baseline_true:,}")
print(f"Recall                        : {baseline_recall:.2f}%")

print()
print("R13 STANDALONE")
print(f"Candidates                    : {r13_count:,}")
print(f"S1 with candidates            : {r13_s1:,}")
print(f"True pairs found              : {r13_true:,}")
print(f"Recall                        : {r13_recall:.2f}%")

print()
print("R13 ADDITIONAL VALUE")
print(f"New candidates                : {new_candidates:,}")
print(f"New true pairs                : {new_true_pairs:,}")

if new_true_pairs > 0:
    print(
        f"Candidates / new true pair   : "
        f"{new_candidates / new_true_pairs:.2f}"
    )
else:
    print("Candidates / new true pair   : N/A")

print()
print("COMBINED")
print(f"Candidates                    : {combined_candidates:,}")
print(f"True pairs found              : {combined_true:,}")
print(f"Recall                        : {combined_recall:.2f}%")
print(f"Remaining missed              : {total_true - combined_true:,}")

print()
print("=" * 80)
print("R13 COMPLETE")
print("=" * 80)