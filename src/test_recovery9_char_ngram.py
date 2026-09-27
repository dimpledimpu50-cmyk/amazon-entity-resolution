import duckdb
import time
import os

# ============================================================
# PATHS
# ============================================================

BASE = r"C:\Users\DIMPLE K B\Documents\amazon-project\amazon_entity_resolution"

S1_SAMPLE = os.path.join(
    BASE, "intermediate", "blocking_eval_s1.parquet"
)

TRUE_PAIRS = os.path.join(
    BASE, "intermediate", "blocking_eval_true_pairs.parquet"
)

S2 = os.path.join(
    BASE, "parquet", "train_source2_normalized.parquet"
)

S3 = os.path.join(
    BASE, "parquet", "train_source3_normalized.parquet"
)

BASELINE = os.path.join(
    BASE, "intermediate", "verified_baseline_candidates.parquet"
)

R9_OUTPUT = os.path.join(
    BASE, "intermediate", "recovery9_v2_candidates.parquet"
)


# ============================================================
# SETTINGS
# ============================================================

NGRAM_SIZE = 3

# Only relatively rare 3-grams are useful.
MAX_GRAM_FREQ = 50

# Require two independent 3-gram matches.
MIN_SHARED_GRAMS = 2

# Avoid pairing extremely different-length names.
MAX_LENGTH_DIFF = 12


# ============================================================
# DUCKDB
# ============================================================

con = duckdb.connect()

con.execute("""
SET memory_limit='10GB';
SET threads=4;
SET preserve_insertion_order=false;
""")

start = time.time()

print("=" * 70)
print("R9-v2 OPTIMIZED - SELECTIVE CHARACTER 3-GRAM BLOCKING")
print("=" * 70)

print(f"N-gram size:             {NGRAM_SIZE}")
print(f"Maximum gram frequency:  {MAX_GRAM_FREQ}")
print(f"Minimum shared grams:    {MIN_SHARED_GRAMS}")
print(f"Maximum length diff:     {MAX_LENGTH_DIFF}")


# ============================================================
# 1. LOAD FIXED S1 SAMPLE
# ============================================================

print("\n[1/8] Loading fixed S1 sample...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE s1 AS

SELECT
    entity_id AS source1_entity_id,
    country_norm,
    name_norm,
    name_compact

FROM read_parquet('{S1_SAMPLE}');
""")

s1_count = con.execute("""
SELECT COUNT(*)
FROM s1;
""").fetchone()[0]

print(f"S1 sample rows: {s1_count:,}")


# ============================================================
# 2. LOAD SOURCE 2 + SOURCE 3
# ============================================================

print("\n[2/8] Loading S2 + S3...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE source AS

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_norm,
    name_compact

FROM read_parquet('{S2}')

UNION ALL

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_norm,
    name_compact

FROM read_parquet('{S3}');
""")

source_count = con.execute("""
SELECT COUNT(*)
FROM source;
""").fetchone()[0]

print(f"S2 + S3 rows: {source_count:,}")


# ============================================================
# 3. CREATE CORE BUSINESS NAMES
# ============================================================

print("\n[3/8] Creating core business names...")

generic_pattern = (
    r'\b('
    r'private|limited|ltd|pvt|pvtltd|'
    r'company|co|inc|incorporated|'
    r'corporation|corp|llc|llp|pllc|lp|'
    r'services|service|center|centre|group|'
    r'associates|partners|holdings|international|'
    r'industries|industry|enterprises|enterprise|'
    r'solutions|technologies|technology|trading|'
    r'products|product|management|foundation|'
    r'clinic|medical|healthcare|health|'
    r'systems|system|consulting|consultants'
    r')\b'
)


con.execute(f"""
CREATE OR REPLACE TEMP TABLE s1_core AS

SELECT
    source1_entity_id,
    country_norm,

    regexp_replace(
        regexp_replace(
            lower(name_norm),
            '{generic_pattern}',
            ' ',
            'g'
        ),
        '[^a-z0-9]+',
        '',
        'g'
    ) AS core_name

FROM s1

WHERE name_norm IS NOT NULL;
""")


con.execute(f"""
CREATE OR REPLACE TEMP TABLE source_core AS

SELECT
    candidate_entity_id,
    country_norm,

    regexp_replace(
        regexp_replace(
            lower(name_norm),
            '{generic_pattern}',
            ' ',
            'g'
        ),
        '[^a-z0-9]+',
        '',
        'g'
    ) AS core_name

FROM source

WHERE name_norm IS NOT NULL;
""")


# ============================================================
# 4. GENERATE SELECTIVE S1 3-GRAMS
# ============================================================

print("\n[4/8] Generating selective S1 3-grams...")

# Instead of every 3-gram, retain:
#
#   1. beginning 3-gram
#   2. middle 3-gram
#   3. ending 3-gram
#   4. a few interior positions
#
# This keeps the S1 side small while still tolerating
# spacing/typo changes.

con.execute(f"""
CREATE OR REPLACE TEMP TABLE s1_grams AS

WITH base AS (

    SELECT
        source1_entity_id,
        country_norm,
        core_name,
        length(core_name) AS name_length

    FROM s1_core

    WHERE length(core_name) >= {NGRAM_SIZE}
),

positions AS (

    SELECT
        source1_entity_id,
        country_norm,
        name_length,

        1 AS pos

    FROM base

    UNION ALL

    SELECT
        source1_entity_id,
        country_norm,
        name_length,

        CAST(
            floor((name_length - 2) / 2)
            AS INTEGER
        )

    FROM base

    WHERE name_length >= 7

    UNION ALL

    SELECT
        source1_entity_id,
        country_norm,
        name_length,

        name_length - 2

    FROM base

    UNION ALL

    SELECT
        source1_entity_id,
        country_norm,
        name_length,

        4

    FROM base

    WHERE name_length >= 9

    UNION ALL

    SELECT
        source1_entity_id,
        country_norm,
        name_length,

        name_length - 5

    FROM base

    WHERE name_length >= 9
)

SELECT DISTINCT
    p.source1_entity_id,
    p.country_norm,
    p.name_length AS s1_name_length,

    substr(
        b.core_name,
        p.pos,
        {NGRAM_SIZE}
    ) AS gram

FROM positions p

INNER JOIN base b
    ON b.source1_entity_id =
       p.source1_entity_id

WHERE
    p.pos >= 1
    AND p.pos <= p.name_length - 2;
""")


s1_gram_count = con.execute("""
SELECT COUNT(*)
FROM s1_grams;
""").fetchone()[0]

print(
    f"Selective S1 3-gram rows: "
    f"{s1_gram_count:,}"
)


# ============================================================
# 5. FIND RARE SOURCE 3-GRAMS
# ============================================================

print("\n[5/8] Finding rare source 3-grams...")

print(
    "Scanning S2 + S3 for 3-gram frequencies..."
)

# Here we still scan source names, but only retain
# frequency information for rare grams.

con.execute(f"""
CREATE OR REPLACE TEMP TABLE source_gram_frequency AS

SELECT
    country_norm,
    gram,
    COUNT(*) AS gram_freq

FROM (

    SELECT DISTINCT
        country_norm,
        candidate_entity_id,

        substr(
            core_name,
            r.i,
            {NGRAM_SIZE}
        ) AS gram

    FROM source_core s

    CROSS JOIN LATERAL range(
        1,
        length(s.core_name) - {NGRAM_SIZE} + 2
    ) r(i)

    WHERE
        length(s.core_name) >= {NGRAM_SIZE}
)

GROUP BY
    country_norm,
    gram

HAVING
    COUNT(*) <= {MAX_GRAM_FREQ};
""")


rare_gram_count = con.execute("""
SELECT COUNT(*)
FROM source_gram_frequency;
""").fetchone()[0]

print(
    f"Rare source 3-grams: "
    f"{rare_gram_count:,}"
)


# ============================================================
# 6. BUILD SELECTIVE SOURCE INDEX
# ============================================================

print("\n[6/8] Building selective source index...")

# Only source records containing a rare gram are retained.

con.execute(f"""
CREATE OR REPLACE TEMP TABLE source_gram_index AS

SELECT DISTINCT

    s.country_norm,
    s.candidate_entity_id,

    length(s.core_name)
        AS source_name_length,

    substr(
        s.core_name,
        r.i,
        {NGRAM_SIZE}
    ) AS gram

FROM source_core s

CROSS JOIN LATERAL range(
    1,
    length(s.core_name) - {NGRAM_SIZE} + 2
) r(i)

INNER JOIN source_gram_frequency f

    ON f.country_norm = s.country_norm

   AND f.gram =
       substr(
           s.core_name,
           r.i,
           {NGRAM_SIZE}
       )

WHERE
    length(s.core_name) >= {NGRAM_SIZE};
""")


index_count = con.execute("""
SELECT COUNT(*)
FROM source_gram_index;
""").fetchone()[0]

print(
    f"Selective source index rows: "
    f"{index_count:,}"
)


# ============================================================
# 7. GENERATE CANDIDATES
# ============================================================

print("\n[7/8] Generating R9-v2 candidates...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE r9_candidates AS

WITH gram_matches AS (

    SELECT

        s.source1_entity_id,
        idx.candidate_entity_id,

        MAX(s.s1_name_length)
            AS s1_name_length,

        MAX(idx.source_name_length)
            AS source_name_length,

        COUNT(*) AS shared_grams

    FROM s1_grams s

    INNER JOIN source_gram_index idx

        ON idx.country_norm =
           s.country_norm

       AND idx.gram =
           s.gram

    GROUP BY
        s.source1_entity_id,
        idx.candidate_entity_id
)

SELECT

    source1_entity_id,
    candidate_entity_id

FROM gram_matches

WHERE

    shared_grams >= {MIN_SHARED_GRAMS}

    AND ABS(
        s1_name_length -
        source_name_length
    ) <= {MAX_LENGTH_DIFF};
""")


# ============================================================
# CANDIDATE STATISTICS
# ============================================================

r9_candidates = con.execute("""
SELECT COUNT(*)
FROM r9_candidates;
""").fetchone()[0]

r9_s1 = con.execute("""
SELECT COUNT(DISTINCT source1_entity_id)
FROM r9_candidates;
""").fetchone()[0]

print(
    f"R9-v2 candidates: "
    f"{r9_candidates:,}"
)

print(
    f"S1 with R9-v2 candidates: "
    f"{r9_s1:,}"
)


stats = con.execute("""
SELECT
    AVG(cnt),
    MEDIAN(cnt),
    QUANTILE_CONT(cnt, 0.95),
    MAX(cnt)

FROM (

    SELECT
        source1_entity_id,
        COUNT(*) AS cnt

    FROM r9_candidates

    GROUP BY source1_entity_id
);
""").fetchone()

if stats[0] is not None:

    print(
        f"Average / S1: "
        f"{stats[0]:.2f}"
    )

    print(
        f"Median / S1: "
        f"{stats[1]:.2f}"
    )

    print(
        f"P95 / S1: "
        f"{stats[2]:.2f}"
    )

    print(
        f"Max / S1: "
        f"{stats[3]:,}"
    )


# ============================================================
# 8. EVALUATE
# ============================================================

print("\n[8/8] Evaluating R9-v2...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE true_pairs AS

SELECT
    s1_id AS source1_entity_id,
    candidate_id AS candidate_entity_id

FROM read_parquet('{TRUE_PAIRS}');
""")


true_count = con.execute("""
SELECT COUNT(*)
FROM true_pairs;
""").fetchone()[0]


# ------------------------------------------------------------
# R9-v2 recall
# ------------------------------------------------------------

r9_found = con.execute("""
SELECT COUNT(*)

FROM true_pairs t

INNER JOIN r9_candidates r

    ON r.source1_entity_id =
       t.source1_entity_id

   AND r.candidate_entity_id =
       t.candidate_entity_id;
""").fetchone()[0]


r9_recall = (
    r9_found / true_count
    if true_count
    else 0
)


# ============================================================
# BASELINE
# ============================================================

con.execute(f"""
CREATE OR REPLACE TEMP TABLE baseline AS

SELECT *
FROM read_parquet('{BASELINE}');
""")


baseline_candidates = con.execute("""
SELECT COUNT(*)
FROM baseline;
""").fetchone()[0]


baseline_found = con.execute("""
SELECT COUNT(*)

FROM true_pairs t

INNER JOIN baseline b

    ON b.source1_entity_id =
       t.source1_entity_id

   AND b.candidate_entity_id =
       t.candidate_entity_id;
""").fetchone()[0]


baseline_recall = (
    baseline_found / true_count
    if true_count
    else 0
)


# ============================================================
# NEW TRUE PAIRS
# ============================================================

new_found = con.execute("""
SELECT COUNT(*)

FROM true_pairs t

INNER JOIN r9_candidates r

    ON r.source1_entity_id =
       t.source1_entity_id

   AND r.candidate_entity_id =
       t.candidate_entity_id

LEFT JOIN baseline b

    ON b.source1_entity_id =
       t.source1_entity_id

   AND b.candidate_entity_id =
       t.candidate_entity_id

WHERE
    b.source1_entity_id IS NULL;
""").fetchone()[0]


# ============================================================
# ADDITIONAL CANDIDATES
# ============================================================

additional_candidates = con.execute("""
SELECT COUNT(*)

FROM r9_candidates r

LEFT JOIN baseline b

    ON b.source1_entity_id =
       r.source1_entity_id

   AND b.candidate_entity_id =
       r.candidate_entity_id

WHERE
    b.source1_entity_id IS NULL;
""").fetchone()[0]


# ============================================================
# COMBINED
# ============================================================

combined_candidates = (
    baseline_candidates +
    additional_candidates
)

combined_found = (
    baseline_found +
    new_found
)

combined_recall = (
    combined_found / true_count
    if true_count
    else 0
)

remaining_missed = (
    true_count -
    combined_found
)


# ============================================================
# REPORT
# ============================================================

print("\n")
print("=" * 70)
print("R9-v2 RESULTS")
print("=" * 70)

print(
    f"True pairs:                 "
    f"{true_count:,}"
)

print(
    f"R9-v2 candidates:           "
    f"{r9_candidates:,}"
)

print(
    f"R9-v2 S1 with candidates:   "
    f"{r9_s1:,}"
)

print(
    f"R9-v2 found:                "
    f"{r9_found:,}"
)

print(
    f"R9-v2 recall:               "
    f"{r9_recall * 100:.2f}%"
)

print("\n" + "-" * 70)
print("VERIFIED BASELINE")
print("-" * 70)

print(
    f"Baseline candidates:        "
    f"{baseline_candidates:,}"
)

print(
    f"Baseline found:             "
    f"{baseline_found:,}"
)

print(
    f"Baseline recall:            "
    f"{baseline_recall * 100:.2f}%"
)

print("\n" + "-" * 70)
print("INCREMENTAL VALUE OF R9-v2")
print("-" * 70)

print(
    f"NEW true pairs:              "
    f"{new_found:,}"
)

print(
    f"Additional candidates:      "
    f"{additional_candidates:,}"
)

if new_found:

    print(
        f"Candidates / new pair:      "
        f"{additional_candidates / new_found:.2f}"
    )

else:

    print(
        "Candidates / new pair:      "
        "infinite"
    )

print(
    f"Combined candidates:        "
    f"{combined_candidates:,}"
)

print(
    f"Combined found:             "
    f"{combined_found:,}"
)

print(
    f"Combined recall:            "
    f"{combined_recall * 100:.2f}%"
)

print(
    f"Remaining missed:           "
    f"{remaining_missed:,}"
)


# ============================================================
# SAVE
# ============================================================

print("\nSaving R9-v2 candidates...")

con.execute(f"""
COPY r9_candidates
TO '{R9_OUTPUT}'
(FORMAT PARQUET);
""")

print(
    f"Saved: {R9_OUTPUT}"
)


# ============================================================
# TIME
# ============================================================

elapsed = time.time() - start

print("\n" + "=" * 70)

print(
    f"Total time: "
    f"{elapsed / 60:.2f} minutes "
    f"({elapsed:.1f} seconds)"
)

print("=" * 70)