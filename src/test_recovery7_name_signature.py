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


# ============================================================
# LOAD DATA
# ============================================================

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


# ============================================================
# GENERIC WORDS
# ============================================================

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


# ============================================================
# CORE NAME
# ============================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW s1_core AS
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
    ) AS core

FROM s1
""")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW src_core AS
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
    ) AS core

FROM src
""")


# ============================================================
# SIGNATURES
#
# Example:
#
# dalegoldman
#
# first4 = dale
# last4  = dman
# length = 11
#
# This can tolerate some internal character changes.
# ============================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW s1_sig AS
SELECT
    entity_id,
    country_norm,
    core,

    substr(core, 1, 4) AS first4,

    CASE
        WHEN length(core) >= 4
        THEN substr(core, length(core) - 3, 4)
        ELSE core
    END AS last4,

    length(core) AS name_len

FROM s1_core
WHERE length(core) >= 5
""")

DB.execute("""
CREATE OR REPLACE TEMP VIEW src_sig AS
SELECT
    entity_id,
    country_norm,
    core,

    substr(core, 1, 4) AS first4,

    CASE
        WHEN length(core) >= 4
        THEN substr(core, length(core) - 3, 4)
        ELSE core
    END AS last4,

    length(core) AS name_len

FROM src_core
WHERE length(core) >= 5
""")


# ============================================================
# SOURCE FREQUENCY
#
# Require the first4 + last4 combination to be selective.
# ============================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW sig_freq AS
SELECT
    country_norm,
    first4,
    last4,
    COUNT(*) AS freq
FROM src_sig
GROUP BY
    country_norm,
    first4,
    last4
""")


# ============================================================
# R7 CANDIDATES
#
# Same:
#   country
#   first 4 characters
#   last 4 characters
#
# And:
#   name length difference <= 2
#
# Frequency <= 25
# ============================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW r7 AS
SELECT
    s.entity_id AS source1_entity_id,
    c.entity_id AS candidate_entity_id

FROM s1_sig s

JOIN src_sig c
    ON s.country_norm = c.country_norm
   AND s.first4 = c.first4
   AND s.last4 = c.last4
   AND abs(s.name_len - c.name_len) <= 2

JOIN sig_freq f
    ON f.country_norm = c.country_norm
   AND f.first4 = c.first4
   AND f.last4 = c.last4

WHERE f.freq <= 25
""")


# ============================================================
# BASIC STATS
# ============================================================

r7_candidates = DB.execute("""
SELECT COUNT(*)
FROM r7
""").fetchone()[0]

r7_s1 = DB.execute("""
SELECT COUNT(DISTINCT source1_entity_id)
FROM r7
""").fetchone()[0]

r7_avg = DB.execute("""
SELECT AVG(cnt)
FROM (
    SELECT
        source1_entity_id,
        COUNT(*) AS cnt
    FROM r7
    GROUP BY source1_entity_id
)
""").fetchone()[0] or 0

r7_median = DB.execute("""
SELECT median(cnt)
FROM (
    SELECT
        source1_entity_id,
        COUNT(*) AS cnt
    FROM r7
    GROUP BY source1_entity_id
)
""").fetchone()[0] or 0

r7_p95 = DB.execute("""
SELECT quantile_cont(cnt, 0.95)
FROM (
    SELECT
        source1_entity_id,
        COUNT(*) AS cnt
    FROM r7
    GROUP BY source1_entity_id
)
""").fetchone()[0] or 0

r7_max = DB.execute("""
SELECT MAX(cnt)
FROM (
    SELECT
        source1_entity_id,
        COUNT(*) AS cnt
    FROM r7
    GROUP BY source1_entity_id
)
""").fetchone()[0] or 0


# ============================================================
# TRUE PAIRS FOUND BY R7
# ============================================================

true_total = DB.execute("""
SELECT COUNT(*)
FROM true_pairs
""").fetchone()[0]

r7_found = DB.execute("""
SELECT COUNT(*)
FROM true_pairs t
JOIN r7 r
    ON t.source1_entity_id = r.source1_entity_id
   AND t.candidate_entity_id = r.candidate_entity_id
""").fetchone()[0]


# ============================================================
# BASELINE
# ============================================================

base_count = DB.execute("""
SELECT COUNT(*)
FROM base
""").fetchone()[0]

base_found = DB.execute("""
SELECT COUNT(*)
FROM true_pairs t
JOIN base b
    ON t.source1_entity_id = b.source1_entity_id
   AND t.candidate_entity_id = b.candidate_entity_id
""").fetchone()[0]


# ============================================================
# NEW TRUE PAIRS FROM R7
#
# True pairs found by R7 but NOT already present in baseline.
# ============================================================

new_true = DB.execute("""
SELECT COUNT(*)
FROM true_pairs t

JOIN r7 r
    ON t.source1_entity_id = r.source1_entity_id
   AND t.candidate_entity_id = r.candidate_entity_id

LEFT JOIN base b
    ON b.source1_entity_id = r.source1_entity_id
   AND b.candidate_entity_id = r.candidate_entity_id

WHERE b.source1_entity_id IS NULL
""").fetchone()[0]


# ============================================================
# BASELINE + R7
# ============================================================

combined_count = DB.execute("""
SELECT COUNT(*)
FROM (
    SELECT
        source1_entity_id,
        candidate_entity_id
    FROM base

    UNION

    SELECT
        source1_entity_id,
        candidate_entity_id
    FROM r7
)
""").fetchone()[0]

additional_candidates = combined_count - base_count


# ============================================================
# TRUE PAIRS FOUND BY BASELINE + R7
# ============================================================

combined_found = DB.execute("""
SELECT COUNT(*)
FROM true_pairs t

JOIN (
    SELECT
        source1_entity_id,
        candidate_entity_id
    FROM base

    UNION

    SELECT
        source1_entity_id,
        candidate_entity_id
    FROM r7
) x

    ON t.source1_entity_id = x.source1_entity_id
   AND t.candidate_entity_id = x.candidate_entity_id
""").fetchone()[0]

remaining = true_total - combined_found


# ============================================================
# SAVE R7 CANDIDATES
#
# IMPORTANT:
# The actual R7 relation is named "r7".
# ============================================================

DB.execute("""
COPY r7
TO 'intermediate/recovery7_candidates.parquet'
(FORMAT PARQUET)
""")


# ============================================================
# OUTPUT
# ============================================================

print("=" * 70)
print("RECOVERY #7 — TYPO-TOLERANT NAME SIGNATURE")
print("=" * 70)

print()

print(
    "R7 candidates         :",
    f"{r7_candidates:,}"
)

print(
    "S1 with R7 candidates :",
    f"{r7_s1:,}"
)

print(
    "Average / S1          :",
    round(r7_avg, 2)
)

print(
    "Median / S1           :",
    round(r7_median, 2)
)

print(
    "P95 / S1              :",
    round(r7_p95, 2)
)

print(
    "Max / S1              :",
    r7_max
)

print()

print(
    "True pairs            :",
    f"{true_total:,}"
)

print(
    "R7 found              :",
    f"{r7_found:,}"
)

print(
    "R7 recall             :",
    f"{100 * r7_found / true_total:.2f}%"
)

print()

print(
    "Verified baseline     :",
    f"{base_found:,}"
)

print(
    "Baseline recall       :",
    f"{100 * base_found / true_total:.2f}%"
)

print()

print(
    "NEW true pairs        :",
    f"{new_true:,}"
)

print(
    "Additional candidates :",
    f"{additional_candidates:,}"
)

if new_true > 0:
    print(
        "Candidates / new pair:",
        round(additional_candidates / new_true, 2)
    )
else:
    print(
        "Candidates / new pair: N/A"
    )

print()

print(
    "Combined candidates   :",
    f"{combined_count:,}"
)

print(
    "Combined found        :",
    f"{combined_found:,}"
)

print(
    "Combined recall       :",
    f"{100 * combined_found / true_total:.2f}%"
)

print(
    "Remaining missed      :",
    f"{remaining:,}"
)

print()

print(
    "Saved R7 candidates   :"
)

print(
    "  intermediate/recovery7_candidates.parquet"
)

print("=" * 70)
print("DONE")
print("=" * 70)