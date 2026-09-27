import duckdb

DB = duckdb.connect()

S1 = "intermediate/blocking_eval_s1.parquet"
SRC = """
    SELECT entity_id, name_norm, country_norm
    FROM read_parquet([
        'parquet/train_source2_normalized.parquet',
        'parquet/train_source3_normalized.parquet'
    ])
"""

BASE = "intermediate/verified_baseline_candidates.parquet"
TRUE_PAIRS = "intermediate/blocking_eval_true_pairs.parquet"

# ------------------------------------------------------------
# Load fixed sample + source
# ------------------------------------------------------------

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW s1 AS
SELECT *
FROM read_parquet('{S1}')
""")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW src AS
{SRC}
""")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW true_pairs AS
SELECT
    s1_id AS source1_entity_id,
    candidate_id AS candidate_entity_id
FROM read_parquet('{TRUE_PAIRS}')
""")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW base AS
SELECT *
FROM read_parquet('{BASE}')
""")

# ------------------------------------------------------------
# Generic words to remove
# ------------------------------------------------------------

GENERIC = """
    private|limited|ltd|pvt|pvtltd|private limited|
    company|co|inc|incorporated|corporation|corp|
    llc|llp|pllc|lp|
    services|service|center|centre|group|associates|
    partners|holdings|international|industries|industry|
    enterprises|enterprise|solutions|technologies|technology|
    trading|products|product|management|foundation|
    clinic|medical|healthcare|health|systems|system|
    consulting|consultants
"""

# ------------------------------------------------------------
# Build core compact name
# ------------------------------------------------------------

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW s1_keys AS
SELECT
    entity_id,
    country_norm,

    regexp_replace(
        regexp_replace(
            lower(name_norm),
            '(^| )({GENERIC})( |$)',
            ' ',
            'g'
        ),
        '[^a-z0-9]',
        '',
        'g'
    ) AS core_compact

FROM s1
""")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW src_keys AS
SELECT
    entity_id,
    country_norm,

    regexp_replace(
        regexp_replace(
            lower(name_norm),
            '(^| )({GENERIC})( |$)',
            ' ',
            'g'
        ),
        '[^a-z0-9]',
        '',
        'g'
    ) AS core_compact

FROM src
""")

# ------------------------------------------------------------
# 4-character prefix
# ------------------------------------------------------------

DB.execute("""
CREATE OR REPLACE TEMP VIEW src_prefix_freq AS
SELECT
    country_norm,
    substr(core_compact, 1, 4) AS prefix4,
    COUNT(*) AS freq
FROM src_keys
WHERE length(core_compact) >= 4
GROUP BY country_norm, prefix4
""")

# ------------------------------------------------------------
# Generate R6 candidates
# ------------------------------------------------------------

DB.execute("""
CREATE OR REPLACE TEMP VIEW r6 AS
SELECT
    s.entity_id AS source1_entity_id,
    c.entity_id AS candidate_entity_id

FROM s1_keys s
JOIN src_keys c
    ON s.country_norm = c.country_norm
   AND substr(s.core_compact, 1, 4)
       = substr(c.core_compact, 1, 4)

JOIN src_prefix_freq f
    ON f.country_norm = c.country_norm
   AND f.prefix4 = substr(c.core_compact, 1, 4)

WHERE length(s.core_compact) >= 4
  AND length(c.core_compact) >= 4
  AND f.freq <= 100
""")

# ------------------------------------------------------------
# Statistics
# ------------------------------------------------------------

r6_candidates = DB.execute("""
SELECT COUNT(*) FROM r6
""").fetchone()[0]

r6_s1 = DB.execute("""
SELECT COUNT(DISTINCT source1_entity_id)
FROM r6
""").fetchone()[0]

r6_avg = DB.execute("""
SELECT AVG(cnt)
FROM (
    SELECT source1_entity_id, COUNT(*) cnt
    FROM r6
    GROUP BY source1_entity_id
)
""").fetchone()[0]

r6_median = DB.execute("""
SELECT median(cnt)
FROM (
    SELECT source1_entity_id, COUNT(*) cnt
    FROM r6
    GROUP BY source1_entity_id
)
""").fetchone()[0]

r6_p95 = DB.execute("""
SELECT quantile_cont(cnt, 0.95)
FROM (
    SELECT source1_entity_id, COUNT(*) cnt
    FROM r6
    GROUP BY source1_entity_id
)
""").fetchone()[0]

r6_max = DB.execute("""
SELECT MAX(cnt)
FROM (
    SELECT source1_entity_id, COUNT(*) cnt
    FROM r6
    GROUP BY source1_entity_id
)
""").fetchone()[0]

# ------------------------------------------------------------
# True pairs found
# ------------------------------------------------------------

r6_found = DB.execute("""
SELECT COUNT(*)
FROM true_pairs t
JOIN r6 r
  ON t.source1_entity_id = r.source1_entity_id
 AND t.candidate_entity_id = r.candidate_entity_id
""").fetchone()[0]

# ------------------------------------------------------------
# New true pairs beyond verified baseline
# ------------------------------------------------------------

r6_new = DB.execute("""
SELECT COUNT(*)
FROM true_pairs t
JOIN r6 r
  ON t.source1_entity_id = r.source1_entity_id
 AND t.candidate_entity_id = r.candidate_entity_id
LEFT JOIN base b
  ON b.source1_entity_id = r.source1_entity_id
 AND b.candidate_entity_id = r.candidate_entity_id
WHERE b.source1_entity_id IS NULL
""").fetchone()[0]

true_total = DB.execute("""
SELECT COUNT(*) FROM true_pairs
""").fetchone()[0]

base_found = DB.execute("""
SELECT COUNT(*)
FROM true_pairs t
JOIN base b
  ON t.source1_entity_id = b.source1_entity_id
 AND t.candidate_entity_id = b.candidate_entity_id
""").fetchone()[0]

combined_candidates = DB.execute("""
SELECT COUNT(*)
FROM (
    SELECT source1_entity_id, candidate_entity_id
    FROM base

    UNION

    SELECT source1_entity_id, candidate_entity_id
    FROM r6
)
""").fetchone()[0]

combined_found = DB.execute("""
SELECT COUNT(*)
FROM true_pairs t
JOIN (
    SELECT source1_entity_id, candidate_entity_id
    FROM base

    UNION

    SELECT source1_entity_id, candidate_entity_id
    FROM r6
) x
  ON t.source1_entity_id = x.source1_entity_id
 AND t.candidate_entity_id = x.candidate_entity_id
""").fetchone()[0]

additional_candidates = combined_candidates - DB.execute("""
SELECT COUNT(*) FROM base
""").fetchone()[0]

remaining = true_total - combined_found

print("=" * 70)
print("RECOVERY #6 — CORE NAME 4-CHAR PREFIX")
print("=" * 70)

print()
print("R6 candidates        :", f"{r6_candidates:,}")
print("S1 with R6 candidates:", f"{r6_s1:,}")
print("Average / S1        :", round(r6_avg, 2))
print("Median / S1         :", round(r6_median, 2))
print("P95 / S1            :", round(r6_p95, 2))
print("Max / S1            :", r6_max)

print()
print("True pairs           :", f"{true_total:,}")
print("R6 found             :", f"{r6_found:,}")
print("R6 recall            :", f"{100*r6_found/true_total:.2f}%")

print()
print("Verified baseline    :", f"{base_found:,}")
print("Baseline recall      :", f"{100*base_found/true_total:.2f}%")

print()
print("NEW true pairs       :", f"{r6_new:,}")
print("Additional candidates:", f"{additional_candidates:,}")

if r6_new > 0:
    print(
        "Candidates / new pair:",
        round(additional_candidates / r6_new, 2)
    )

print()
print("Combined candidates  :", f"{combined_candidates:,}")
print("Combined found       :", f"{combined_found:,}")
print("Combined recall      :", f"{100*combined_found/true_total:.2f}%")
print("Remaining missed     :", f"{remaining:,}")

print("=" * 70)
print("DONE")
print("=" * 70)