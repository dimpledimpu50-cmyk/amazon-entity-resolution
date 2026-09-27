import duckdb

DB = duckdb.connect()

DB.execute("PRAGMA threads=2")
DB.execute("PRAGMA memory_limit='4GB'")

S2 = "parquet/train_source2_normalized.parquet"
S3 = "parquet/train_source3_normalized.parquet"

BLOCKING_EVAL = "intermediate/blocking_eval_s1.parquet"
TRUE_PAIRS = "intermediate/blocking_eval_true_pairs.parquet"
BASELINE = "intermediate/verified_baseline_candidates.parquet"


print("=" * 70)
print("FAST MISSED-PAIR ANALYSIS")
print("=" * 70)


# =========================================================
# 1. LOAD FIXED S1 SAMPLE
# =========================================================

print("\nLoading fixed S1 sample...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE s1 AS
SELECT
    entity_id,
    name_norm,
    address_norm,
    country_norm
FROM read_parquet(?)
""", [BLOCKING_EVAL])


# =========================================================
# 2. LOAD SOURCE 2 + SOURCE 3
# =========================================================

print("Loading Source 2 + Source 3...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE src AS

SELECT
    entity_id,
    name_norm,
    address_norm,
    country_norm
FROM read_parquet(?)

UNION ALL

SELECT
    entity_id,
    name_norm,
    address_norm,
    country_norm
FROM read_parquet(?)
""", [S2, S3])


# =========================================================
# 3. LOAD TRUE PAIRS
# =========================================================

DB.execute("""
CREATE OR REPLACE TEMP TABLE true_pairs AS
SELECT
    s1_id AS source1_entity_id,
    candidate_id AS candidate_entity_id
FROM read_parquet(?)
""", [TRUE_PAIRS])


true_count = DB.execute("""
SELECT COUNT(*)
FROM true_pairs
""").fetchone()[0]


# =========================================================
# 4. LOAD VERIFIED BASELINE
# =========================================================

print("Loading verified baseline...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE baseline AS
SELECT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet(?)
""", [BASELINE])


base_count = DB.execute("""
SELECT COUNT(*)
FROM baseline
""").fetchone()[0]


base_found = DB.execute("""
SELECT COUNT(*)
FROM true_pairs t
JOIN baseline b
  ON t.source1_entity_id = b.source1_entity_id
 AND t.candidate_entity_id = b.candidate_entity_id
""").fetchone()[0]


print()
print("=" * 70)
print("BASELINE CHECK")
print("=" * 70)

print("Baseline candidates :", f"{base_count:,}")
print("True pairs          :", f"{true_count:,}")
print("Found               :", f"{base_found:,}")
print("Recall              :", f"{base_found / true_count * 100:.2f}%")


# =========================================================
# 5. SANITY CHECK
# =========================================================

if base_count != 553491 or base_found != 28560:

    print()
    print("ERROR: VERIFIED BASELINE DOES NOT MATCH.")
    print("Expected candidates : 553,491")
    print("Expected found      : 28,560")
    print("STOP.")

    raise SystemExit(1)


print()
print("BASELINE VERIFIED.")


# =========================================================
# 6. FIND MISSED TRUE PAIRS
# =========================================================

print()
print("Finding missed true pairs...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE missed AS

SELECT
    t.source1_entity_id,
    t.candidate_entity_id
FROM true_pairs t

LEFT JOIN baseline b
  ON t.source1_entity_id = b.source1_entity_id
 AND t.candidate_entity_id = b.candidate_entity_id

WHERE b.source1_entity_id IS NULL
""")


missed_count = DB.execute("""
SELECT COUNT(*)
FROM missed
""").fetchone()[0]


print("Missed true pairs:", f"{missed_count:,}")


# =========================================================
# 7. RETRIEVE MISSED PAIR DETAILS
# =========================================================

print("\nRetrieving missed pair details...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE missed_details AS

SELECT

    m.source1_entity_id,
    m.candidate_entity_id,

    s.name_norm AS s1_name,
    x.name_norm AS candidate_name,

    s.address_norm AS s1_address,
    x.address_norm AS candidate_address,

    s.country_norm AS country

FROM missed m

JOIN s1 s
  ON m.source1_entity_id = s.entity_id

JOIN src x
  ON m.candidate_entity_id = x.entity_id
""")


# =========================================================
# 8. BASIC STRUCTURAL ANALYSIS
# =========================================================

print()
print("=" * 70)
print("MISSED-PAIR ANALYSIS")
print("=" * 70)


def show(label, query):

    n = DB.execute(query).fetchone()[0]

    print(
        f"{label:<40}: "
        f"{n:,} ({n / missed_count * 100:.2f}%)"
    )


show(
    "Same exact normalized name",
    """
    SELECT COUNT(*)
    FROM missed_details
    WHERE lower(s1_name) = lower(candidate_name)
    """
)


show(
    "Same exact address",
    """
    SELECT COUNT(*)
    FROM missed_details
    WHERE lower(coalesce(s1_address,'')) =
          lower(coalesce(candidate_address,''))
      AND coalesce(s1_address,'') <> ''
    """
)


show(
    "Same first numeric address",
    """
    SELECT COUNT(*)
    FROM missed_details
    WHERE
        regexp_extract(coalesce(s1_address,''),'[0-9]+')
        =
        regexp_extract(coalesce(candidate_address,''),'[0-9]+')
      AND regexp_extract(coalesce(s1_address,''),'[0-9]+') <> ''
    """
)


show(
    "Name containment",
    """
    SELECT COUNT(*)
    FROM missed_details
    WHERE
        (
            lower(s1_name) LIKE '%' || lower(candidate_name) || '%'
            OR
            lower(candidate_name) LIKE '%' || lower(s1_name) || '%'
        )
      AND length(s1_name) >= 5
      AND length(candidate_name) >= 5
    """
)


# =========================================================
# 9. NAME PREFIX/SUFFIX CHARACTERISTICS
# =========================================================

show(
    "Same first 5 name characters",
    """
    SELECT COUNT(*)
    FROM missed_details
    WHERE
        length(s1_name) >= 5
        AND length(candidate_name) >= 5
        AND substr(lower(s1_name),1,5)
            =
            substr(lower(candidate_name),1,5)
    """
)


show(
    "Same first 4 name characters",
    """
    SELECT COUNT(*)
    FROM missed_details
    WHERE
        length(s1_name) >= 4
        AND length(candidate_name) >= 4
        AND substr(lower(s1_name),1,4)
            =
            substr(lower(candidate_name),1,4)
    """
)


# =========================================================
# 10. ADDRESS CHARACTERISTICS
# =========================================================

show(
    "Same first 5 address characters",
    """
    SELECT COUNT(*)
    FROM missed_details
    WHERE
        length(s1_address) >= 5
        AND length(candidate_address) >= 5
        AND substr(lower(s1_address),1,5)
            =
            substr(lower(candidate_address),1,5)
    """
)


# =========================================================
# 11. PRINT SAMPLE MISSES
# =========================================================

print()
print("=" * 70)
print("SAMPLE MISSED TRUE PAIRS")
print("=" * 70)

rows = DB.execute("""
SELECT
    source1_entity_id,
    candidate_entity_id,
    country,
    s1_name,
    candidate_name,
    s1_address,
    candidate_address
FROM missed_details
LIMIT 50
""").fetchall()


for i, row in enumerate(rows, 1):

    (
        s1_id,
        candidate_id,
        country,
        s1_name,
        candidate_name,
        s1_address,
        candidate_address
    ) = row

    print()
    print(f"[{i}]")
    print("S1 ID        :", s1_id)
    print("Candidate ID :", candidate_id)
    print("Country      :", country)
    print("S1 Name      :", s1_name)
    print("Candidate    :", candidate_name)
    print("S1 Address   :", s1_address)
    print("Candidate    :", candidate_address)


# =========================================================
# 12. SAVE MISSED PAIRS
# =========================================================

DB.execute("""
COPY missed_details
TO 'intermediate/missed_true_pairs.parquet'
(FORMAT PARQUET)
""")


print()
print("=" * 70)
print("DONE")
print("=" * 70)

print(
    "Saved:",
    "intermediate/missed_true_pairs.parquet"
)