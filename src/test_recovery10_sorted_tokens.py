import duckdb
import os
import time

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

OUTPUT = os.path.join(
    BASE, "intermediate", "recovery10_candidates.parquet"
)


# ============================================================
# SETTINGS
# ============================================================

MAX_SIGNATURE_FREQ = 50
MIN_LONG_TOKEN_LENGTH = 5


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
print("R10 - SORTED INFORMATIVE TOKEN BLOCKING")
print("=" * 70)

print(
    f"Maximum signature frequency: "
    f"{MAX_SIGNATURE_FREQ}"
)

print(
    f"Minimum long token length:   "
    f"{MIN_LONG_TOKEN_LENGTH}"
)


# ============================================================
# GENERIC / LEGAL WORDS
# ============================================================

GENERIC_PATTERN = (
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


# ============================================================
# 1. LOAD S1
# ============================================================

print("\n[1/8] Loading fixed S1 sample...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE s1 AS

SELECT
    entity_id AS source1_entity_id,
    country_norm,
    name_norm

FROM read_parquet('{S1_SAMPLE}');
""")

s1_count = con.execute("""
SELECT COUNT(*)
FROM s1;
""").fetchone()[0]

print(
    f"S1 sample rows: {s1_count:,}"
)


# ============================================================
# 2. LOAD SOURCE 2 + SOURCE 3
# ============================================================

print("\n[2/8] Loading S2 + S3...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE source AS

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_norm

FROM read_parquet('{S2}')

UNION ALL

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_norm

FROM read_parquet('{S3}');
""")

source_count = con.execute("""
SELECT COUNT(*)
FROM source;
""").fetchone()[0]

print(
    f"S2 + S3 rows: {source_count:,}"
)


# ============================================================
# 3. CREATE S1 SORTED TOKEN SIGNATURES
# ============================================================

print("\n[3/8] Creating S1 sorted-token signatures...")


con.execute(f"""
CREATE OR REPLACE TEMP TABLE s1_signatures AS

SELECT

    source1_entity_id,
    country_norm,

    (
        SELECT string_agg(
            token,
            ' ' ORDER BY token
        )

        FROM unnest(
            regexp_extract_all(
                regexp_replace(
                    lower(name_norm),
                    '{GENERIC_PATTERN}',
                    ' ',
                    'g'
                ),
                '[a-z0-9]+'
            )
        ) AS t(token)

        WHERE length(token) >= 3

    ) AS signature,

    (
        SELECT COUNT(*)

        FROM unnest(
            regexp_extract_all(
                regexp_replace(
                    lower(name_norm),
                    '{GENERIC_PATTERN}',
                    ' ',
                    'g'
                ),
                '[a-z0-9]+'
            )
        ) AS t(token)

        WHERE
            length(token) >=
            {MIN_LONG_TOKEN_LENGTH}

    ) AS long_token_count

FROM s1

WHERE name_norm IS NOT NULL;
""")


s1_sig_count = con.execute("""
SELECT COUNT(*)
FROM s1_signatures
WHERE
    signature IS NOT NULL
    AND signature != ''
    AND long_token_count >= 1;
""").fetchone()[0]

print(
    f"Usable S1 signatures: "
    f"{s1_sig_count:,}"
)


# ============================================================
# 4. CREATE SOURCE SORTED TOKEN SIGNATURES
# ============================================================

print("\n[4/8] Creating source signatures...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE source_signatures AS

SELECT

    candidate_entity_id,
    country_norm,

    (
        SELECT string_agg(
            token,
            ' ' ORDER BY token
        )

        FROM unnest(
            regexp_extract_all(
                regexp_replace(
                    lower(name_norm),
                    '{GENERIC_PATTERN}',
                    ' ',
                    'g'
                ),
                '[a-z0-9]+'
            )
        ) AS t(token)

        WHERE length(token) >= 3

    ) AS signature,

    (
        SELECT COUNT(*)

        FROM unnest(
            regexp_extract_all(
                regexp_replace(
                    lower(name_norm),
                    '{GENERIC_PATTERN}',
                    ' ',
                    'g'
                ),
                '[a-z0-9]+'
            )
        ) AS t(token)

        WHERE
            length(token) >=
            {MIN_LONG_TOKEN_LENGTH}

    ) AS long_token_count

FROM source

WHERE name_norm IS NOT NULL;
""")


# ============================================================
# 5. COUNT SOURCE SIGNATURE FREQUENCY
# ============================================================

print("\n[5/8] Calculating source signature frequencies...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE signature_frequency AS

SELECT
    country_norm,
    signature,
    COUNT(*) AS signature_freq

FROM source_signatures

WHERE
    signature IS NOT NULL
    AND signature != ''
    AND long_token_count >= 1

GROUP BY
    country_norm,
    signature

HAVING
    COUNT(*) <= {MAX_SIGNATURE_FREQ};
""")


rare_signature_count = con.execute("""
SELECT COUNT(*)
FROM signature_frequency;
""").fetchone()[0]

print(
    f"Rare signatures: "
    f"{rare_signature_count:,}"
)


# ============================================================
# 6. BUILD R10 CANDIDATES
# ============================================================

print("\n[6/8] Generating R10 candidates...")

con.execute("""
CREATE OR REPLACE TEMP TABLE r10_candidates AS

SELECT DISTINCT

    s.source1_entity_id,
    src.candidate_entity_id

FROM s1_signatures s

INNER JOIN signature_frequency f

    ON f.country_norm =
       s.country_norm

   AND f.signature =
       s.signature

INNER JOIN source_signatures src

    ON src.country_norm =
       s.country_norm

   AND src.signature =
       s.signature

WHERE
    s.signature IS NOT NULL
    AND s.signature != ''
    AND s.long_token_count >= 1;
""")


r10_candidates = con.execute("""
SELECT COUNT(*)
FROM r10_candidates;
""").fetchone()[0]

r10_s1 = con.execute("""
SELECT COUNT(DISTINCT source1_entity_id)
FROM r10_candidates;
""").fetchone()[0]

print(
    f"R10 candidates: "
    f"{r10_candidates:,}"
)

print(
    f"S1 with R10 candidates: "
    f"{r10_s1:,}"
)


# ============================================================
# CANDIDATE DISTRIBUTION
# ============================================================

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

    FROM r10_candidates

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
# 7. EVALUATE AGAINST TRUE PAIRS
# ============================================================

print("\n[7/8] Evaluating R10...")

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


r10_found = con.execute("""
SELECT COUNT(*)

FROM true_pairs t

INNER JOIN r10_candidates r

    ON r.source1_entity_id =
       t.source1_entity_id

   AND r.candidate_entity_id =
       t.candidate_entity_id;
""").fetchone()[0]


r10_recall = (
    r10_found / true_count
    if true_count
    else 0
)


# ============================================================
# 8. COMPARE WITH VERIFIED BASELINE
# ============================================================

print("\n[8/8] Comparing with verified baseline...")

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


# ------------------------------------------------------------
# NEW TRUE PAIRS
# ------------------------------------------------------------

new_true_pairs = con.execute("""
SELECT COUNT(*)

FROM true_pairs t

INNER JOIN r10_candidates r

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


# ------------------------------------------------------------
# ADDITIONAL CANDIDATES
# ------------------------------------------------------------

additional_candidates = con.execute("""
SELECT COUNT(*)

FROM r10_candidates r

LEFT JOIN baseline b

    ON b.source1_entity_id =
       r.source1_entity_id

   AND b.candidate_entity_id =
       r.candidate_entity_id

WHERE
    b.source1_entity_id IS NULL;
""").fetchone()[0]


combined_candidates = (
    baseline_candidates +
    additional_candidates
)

combined_found = (
    baseline_found +
    new_true_pairs
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
# RESULTS
# ============================================================

print("\n")
print("=" * 70)
print("R10 RESULTS")
print("=" * 70)

print(
    f"True pairs:                 "
    f"{true_count:,}"
)

print(
    f"R10 candidates:             "
    f"{r10_candidates:,}"
)

print(
    f"R10 S1 with candidates:     "
    f"{r10_s1:,}"
)

print(
    f"R10 found:                  "
    f"{r10_found:,}"
)

print(
    f"R10 recall:                 "
    f"{r10_recall * 100:.2f}%"
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
print("INCREMENTAL VALUE OF R10")
print("-" * 70)

print(
    f"NEW true pairs:              "
    f"{new_true_pairs:,}"
)

print(
    f"Additional candidates:      "
    f"{additional_candidates:,}"
)

if new_true_pairs > 0:

    print(
        f"Candidates / new pair:      "
        f"{additional_candidates / new_true_pairs:.2f}"
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

print("\nSaving R10 candidates...")

con.execute(f"""
COPY r10_candidates
TO '{OUTPUT}'
(FORMAT PARQUET);
""")

print(
    f"Saved: {OUTPUT}"
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

con.close()