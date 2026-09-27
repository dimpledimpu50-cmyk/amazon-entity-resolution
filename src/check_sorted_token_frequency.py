import duckdb
import os
import time

BASE = r"C:\Users\DIMPLE K B\Documents\amazon-project\amazon_entity_resolution"

MISSED = os.path.join(
    BASE, "intermediate", "missed_true_pairs.parquet"
)

S2 = os.path.join(
    BASE, "parquet", "train_source2_normalized.parquet"
)

S3 = os.path.join(
    BASE, "parquet", "train_source3_normalized.parquet"
)

con = duckdb.connect()

con.execute("""
SET memory_limit='10GB';
SET threads=4;
SET preserve_insertion_order=false;
""")

start = time.time()

print("=" * 70)
print("SORTED-TOKEN FREQUENCY DIAGNOSTIC")
print("=" * 70)


# ============================================================
# GENERIC / LEGAL WORDS
# ============================================================

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


# ============================================================
# 1. LOAD MISSED PAIRS
# ============================================================

print("\n[1/6] Loading missed pairs...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE missed AS

SELECT
    source1_entity_id,
    candidate_entity_id,
    s1_name,
    candidate_name,
    country

FROM read_parquet('{MISSED}');
""")

missed_count = con.execute("""
SELECT COUNT(*)
FROM missed;
""").fetchone()[0]

print(f"Missed pairs: {missed_count:,}")


# ============================================================
# 2. CREATE S1 / CANDIDATE SIGNATURES
# ============================================================

print("\n[2/6] Creating missed-pair signatures...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE missed_signatures AS

SELECT

    source1_entity_id,
    candidate_entity_id,
    country,

    (
        SELECT string_agg(
            token,
            ' ' ORDER BY token
        )

        FROM unnest(
            regexp_extract_all(
                regexp_replace(
                    lower(s1_name),
                    '{generic_pattern}',
                    ' ',
                    'g'
                ),
                '[a-z0-9]+'
            )
        ) AS t(token)

        WHERE length(token) >= 3
    ) AS s1_signature,

    (
        SELECT string_agg(
            token,
            ' ' ORDER BY token
        )

        FROM unnest(
            regexp_extract_all(
                regexp_replace(
                    lower(candidate_name),
                    '{generic_pattern}',
                    ' ',
                    'g'
                ),
                '[a-z0-9]+'
            )
        ) AS t(token)

        WHERE length(token) >= 3
    ) AS candidate_signature

FROM missed;
""")


# ============================================================
# 3. KEEP ONLY EXACT SIGNATURE MATCHES
# ============================================================

print("\n[3/6] Finding exact sorted-token matches...")

con.execute("""
CREATE OR REPLACE TEMP TABLE recoverable AS

SELECT

    source1_entity_id,
    candidate_entity_id,
    country,
    s1_signature AS signature

FROM missed_signatures

WHERE
    s1_signature IS NOT NULL
    AND candidate_signature IS NOT NULL
    AND s1_signature != ''
    AND candidate_signature != ''
    AND s1_signature = candidate_signature;
""")


recoverable_count = con.execute("""
SELECT COUNT(*)
FROM recoverable;
""").fetchone()[0]

print(
    f"Recoverable missed pairs: "
    f"{recoverable_count:,}"
)


# ============================================================
# 4. BUILD SOURCE SIGNATURES
# ============================================================

print("\n[4/6] Building S2 + S3 sorted-token frequencies...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE source AS

SELECT
    entity_id AS candidate_entity_id,
    country_norm AS country,
    name_norm

FROM read_parquet('{S2}')

UNION ALL

SELECT
    entity_id AS candidate_entity_id,
    country_norm AS country,
    name_norm

FROM read_parquet('{S3}');
""")


print("Generating source signatures...")


con.execute(f"""
CREATE OR REPLACE TEMP TABLE source_signatures AS

SELECT DISTINCT

    candidate_entity_id,
    country,

    (
        SELECT string_agg(
            token,
            ' ' ORDER BY token
        )

        FROM unnest(
            regexp_extract_all(
                regexp_replace(
                    lower(name_norm),
                    '{generic_pattern}',
                    ' ',
                    'g'
                ),
                '[a-z0-9]+'
            )
        ) AS t(token)

        WHERE length(token) >= 3
    ) AS signature

FROM source

WHERE name_norm IS NOT NULL;
""")


# ============================================================
# 5. SIGNATURE FREQUENCIES
# ============================================================

print("\n[5/6] Calculating signature frequencies...")


con.execute("""
CREATE OR REPLACE TEMP TABLE signature_frequency AS

SELECT
    country,
    signature,
    COUNT(*) AS freq

FROM source_signatures

WHERE
    signature IS NOT NULL
    AND signature != ''

GROUP BY
    country,
    signature;
""")


# ============================================================
# 6. EVALUATE THRESHOLDS
# ============================================================

print("\n[6/6] Evaluating frequency thresholds...")

thresholds = [1, 5, 10, 25, 50, 100, 200, 500]

print("\n")
print("=" * 70)
print("SORTED-TOKEN RECOVERY BY SOURCE FREQUENCY")
print("=" * 70)

print(
    f"{'Threshold':>12} "
    f"{'Recovered':>12} "
    f"{'Percent':>12} "
    f"{'Candidate Pairs':>18}"
)

print("-" * 70)


for threshold in thresholds:

    recovered = con.execute(f"""
        SELECT COUNT(*)

        FROM recoverable r

        INNER JOIN signature_frequency f

            ON f.country = r.country
           AND f.signature = r.signature

        WHERE f.freq <= {threshold};
    """).fetchone()[0]


    # Approximate candidate volume contributed by
    # these signatures across the entire source.
    candidate_pairs = con.execute(f"""
        SELECT COALESCE(
            SUM(f.freq),
            0
        )

        FROM (
            SELECT DISTINCT
                country,
                signature

            FROM recoverable
        ) r

        INNER JOIN signature_frequency f

            ON f.country = r.country
           AND f.signature = r.signature

        WHERE f.freq <= {threshold};
    """).fetchone()[0]


    pct = (
        recovered / recoverable_count * 100
        if recoverable_count
        else 0
    )


    print(
        f"{threshold:>12} "
        f"{recovered:>12,} "
        f"{pct:>11.2f}% "
        f"{candidate_pairs:>18,}"
    )


# ============================================================
# EXTRA: HOW MANY SIGNATURES?
# ============================================================

print("\n" + "=" * 70)
print("SIGNATURE SUMMARY")
print("=" * 70)

summary = con.execute("""
SELECT

    COUNT(*) AS unique_signatures,

    COUNT(*) FILTER (
        WHERE freq = 1
    ) AS freq_1,

    COUNT(*) FILTER (
        WHERE freq <= 5
    ) AS freq_5,

    COUNT(*) FILTER (
        WHERE freq <= 10
    ) AS freq_10,

    COUNT(*) FILTER (
        WHERE freq <= 25
    ) AS freq_25,

    COUNT(*) FILTER (
        WHERE freq <= 50
    ) AS freq_50,

    COUNT(*) FILTER (
        WHERE freq <= 100
    ) AS freq_100

FROM signature_frequency;
""").fetchone()


print(
    f"Unique source signatures: "
    f"{summary[0]:,}"
)

print(
    f"Frequency <= 1:           "
    f"{summary[1]:,}"
)

print(
    f"Frequency <= 5:           "
    f"{summary[2]:,}"
)

print(
    f"Frequency <= 10:          "
    f"{summary[3]:,}"
)

print(
    f"Frequency <= 25:          "
    f"{summary[4]:,}"
)

print(
    f"Frequency <= 50:          "
    f"{summary[5]:,}"
)

print(
    f"Frequency <= 100:         "
    f"{summary[6]:,}"
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