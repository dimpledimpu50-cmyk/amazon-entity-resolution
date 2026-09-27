from pathlib import Path
import duckdb


# ============================================================
# ANALYZE REMAINING MISSED PAIRS AFTER R11
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

BLOCKING_EVAL = (
    BASE_DIR
    / "intermediate"
    / "blocking_eval_s1.parquet"
)

TRUE_PAIRS = (
    BASE_DIR
    / "intermediate"
    / "blocking_eval_true_pairs.parquet"
)

BASE_CANDIDATES = (
    BASE_DIR
    / "intermediate"
    / "verified_baseline_candidates.parquet"
)

R7_CANDIDATES = (
    BASE_DIR
    / "intermediate"
    / "recovery7_candidates.parquet"
)

R10_CANDIDATES = (
    BASE_DIR
    / "intermediate"
    / "recovery10_candidates.parquet"
)

R11_CANDIDATES = (
    BASE_DIR
    / "intermediate"
    / "recovery11_candidates.parquet"
)


DB = duckdb.connect()


print("=" * 80)
print("REMAINING MISSED-PAIR ANALYSIS AFTER R11")
print("=" * 80)


# ============================================================
# 1. LOAD S1
# ============================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW s1 AS

SELECT
    entity_id AS s1_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact

FROM read_parquet('{BLOCKING_EVAL}')
""")


# ============================================================
# 2. LOAD SOURCE 2 + SOURCE 3
# ============================================================

print()
print("Loading Source 2 + Source 3...")

S2 = BASE_DIR / "parquet" / "train_source2_normalized.parquet"
S3 = BASE_DIR / "parquet" / "train_source3_normalized.parquet"

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW sources AS

SELECT
    entity_id AS candidate_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact

FROM read_parquet('{S2}')

UNION ALL

SELECT
    entity_id AS candidate_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact

FROM read_parquet('{S3}')
""")


# ============================================================
# 3. LOAD TRUE PAIRS
# ============================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW true_pairs AS

SELECT
    s1_id AS source1_entity_id,
    candidate_id AS candidate_entity_id

FROM read_parquet('{TRUE_PAIRS}')
""")


# ============================================================
# 4. BUILD CURRENT BLOCKING UNION
# ============================================================

print("Building current blocking union...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW candidates AS

SELECT DISTINCT
    source1_entity_id AS s1_id,
    candidate_entity_id AS candidate_id

FROM read_parquet('{BASE_CANDIDATES}')

UNION

SELECT DISTINCT
    source1_entity_id AS s1_id,
    candidate_entity_id AS candidate_id

FROM read_parquet('{R7_CANDIDATES}')

UNION

SELECT DISTINCT
    source1_entity_id AS s1_id,
    candidate_entity_id AS candidate_id

FROM read_parquet('{R10_CANDIDATES}')

UNION

SELECT DISTINCT
    s1_id,
    candidate_id

FROM read_parquet('{R11_CANDIDATES}')
""")


# ============================================================
# 5. GET MISSED TRUE PAIRS
# ============================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW missed AS

SELECT
    t.source1_entity_id AS s1_id,
    t.candidate_entity_id AS candidate_id

FROM true_pairs t

WHERE NOT EXISTS
(
    SELECT 1

    FROM candidates c

    WHERE
        c.s1_id = t.source1_entity_id
        AND c.candidate_id = t.candidate_entity_id
)
""")


missed_count = DB.execute("""
SELECT COUNT(*)
FROM missed
""").fetchone()[0]


print()
print(f"Remaining missed true pairs: {missed_count:,}")


# ============================================================
# 6. CREATE MISSED PAIR DETAILS
# ============================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW missed_details AS

SELECT
    m.s1_id,
    m.candidate_id,

    s.country_norm,

    s.name_norm AS s1_name,
    c.name_norm AS candidate_name,

    s.name_compact AS s1_name_compact,
    c.name_compact AS candidate_name_compact,

    s.address_norm AS s1_address,
    c.address_norm AS candidate_address,

    s.address_compact AS s1_address_compact,
    c.address_compact AS candidate_address_compact

FROM missed m

JOIN s1 s
    ON m.s1_id = s.s1_id

JOIN sources c
    ON m.candidate_id = c.candidate_id
""")


# ============================================================
# 7. BASIC NAME METRICS
# ============================================================

print()
print("=" * 80)
print("NAME-BASED PATTERNS")
print("=" * 80)


# Exact compact name
exact_compact = DB.execute("""
SELECT COUNT(*)

FROM missed_details

WHERE
    s1_name_compact <> ''
    AND candidate_name_compact <> ''
    AND s1_name_compact = candidate_name_compact
""").fetchone()[0]


# Containment
containment = DB.execute("""
SELECT COUNT(*)

FROM missed_details

WHERE
    LENGTH(s1_name_compact) >= 5
    AND LENGTH(candidate_name_compact) >= 5

    AND
    (
        POSITION(s1_name_compact IN candidate_name_compact) > 0
        OR
        POSITION(candidate_name_compact IN s1_name_compact) > 0
    )
""").fetchone()[0]


# Same first 3
prefix3 = DB.execute("""
SELECT COUNT(*)

FROM missed_details

WHERE
    LENGTH(s1_name_compact) >= 3
    AND LENGTH(candidate_name_compact) >= 3
    AND LEFT(s1_name_compact, 3)
        =
        LEFT(candidate_name_compact, 3)
""").fetchone()[0]


# Same first 4
prefix4 = DB.execute("""
SELECT COUNT(*)

FROM missed_details

WHERE
    LENGTH(s1_name_compact) >= 4
    AND LENGTH(candidate_name_compact) >= 4
    AND LEFT(s1_name_compact, 4)
        =
        LEFT(candidate_name_compact, 4)
""").fetchone()[0]


# Same first 5
prefix5 = DB.execute("""
SELECT COUNT(*)

FROM missed_details

WHERE
    LENGTH(s1_name_compact) >= 5
    AND LENGTH(candidate_name_compact) >= 5
    AND LEFT(s1_name_compact, 5)
        =
        LEFT(candidate_name_compact, 5)
""").fetchone()[0]


# Same last 4
suffix4 = DB.execute("""
SELECT COUNT(*)

FROM missed_details

WHERE
    LENGTH(s1_name_compact) >= 4
    AND LENGTH(candidate_name_compact) >= 4
    AND RIGHT(s1_name_compact, 4)
        =
        RIGHT(candidate_name_compact, 4)
""").fetchone()[0]


# Length difference <= 2
length_diff = DB.execute("""
SELECT COUNT(*)

FROM missed_details

WHERE
    ABS(
        LENGTH(s1_name_compact)
        -
        LENGTH(candidate_name_compact)
    ) <= 2
""").fetchone()[0]


print(
    f"Exact compact name:          "
    f"{exact_compact:,} "
    f"({exact_compact / missed_count * 100:.2f}%)"
)

print(
    f"Name containment:             "
    f"{containment:,} "
    f"({containment / missed_count * 100:.2f}%)"
)

print(
    f"Same first 3 chars:           "
    f"{prefix3:,} "
    f"({prefix3 / missed_count * 100:.2f}%)"
)

print(
    f"Same first 4 chars:           "
    f"{prefix4:,} "
    f"({prefix4 / missed_count * 100:.2f}%)"
)

print(
    f"Same first 5 chars:           "
    f"{prefix5:,} "
    f"({prefix5 / missed_count * 100:.2f}%)"
)

print(
    f"Same last 4 chars:            "
    f"{suffix4:,} "
    f"({suffix4 / missed_count * 100:.2f}%)"
)

print(
    f"Name length difference <=2:   "
    f"{length_diff:,} "
    f"({length_diff / missed_count * 100:.2f}%)"
)


# ============================================================
# 8. ADDRESS METRICS
# ============================================================

print()
print("=" * 80)
print("ADDRESS-BASED PATTERNS")
print("=" * 80)


exact_address = DB.execute("""
SELECT COUNT(*)

FROM missed_details

WHERE
    s1_address_compact <> ''
    AND candidate_address_compact <> ''
    AND s1_address_compact = candidate_address_compact
""").fetchone()[0]


# First 5 address chars
address_prefix5 = DB.execute("""
SELECT COUNT(*)

FROM missed_details

WHERE
    LENGTH(s1_address_compact) >= 5
    AND LENGTH(candidate_address_compact) >= 5
    AND LEFT(s1_address_compact, 5)
        =
        LEFT(candidate_address_compact, 5)
""").fetchone()[0]


# Canonical first numeric component
DB.execute("""
CREATE OR REPLACE TEMP VIEW missed_numeric AS

SELECT
    *,
    TRY_CAST(
        REGEXP_EXTRACT(
            s1_address,
            '[0-9]+'
        )
        AS BIGINT
    ) AS s1_house,

    TRY_CAST(
        REGEXP_EXTRACT(
            candidate_address,
            '[0-9]+'
        )
        AS BIGINT
    ) AS candidate_house

FROM missed_details
""")


same_house = DB.execute("""
SELECT COUNT(*)

FROM missed_numeric

WHERE
    s1_house IS NOT NULL
    AND candidate_house IS NOT NULL
    AND s1_house = candidate_house
""").fetchone()[0]


print(
    f"Exact compact address:        "
    f"{exact_address:,} "
    f"({exact_address / missed_count * 100:.2f}%)"
)

print(
    f"Same first 5 address chars:   "
    f"{address_prefix5:,} "
    f"({address_prefix5 / missed_count * 100:.2f}%)"
)

print(
    f"Same canonical house number:  "
    f"{same_house:,} "
    f"({same_house / missed_count * 100:.2f}%)"
)


# ============================================================
# 9. COMBINED NAME + ADDRESS SIGNALS
# ============================================================

print()
print("=" * 80)
print("COMBINED SIGNALS")
print("=" * 80)


prefix4_house = DB.execute("""
SELECT COUNT(*)

FROM missed_numeric

WHERE
    LENGTH(s1_name_compact) >= 4
    AND LENGTH(candidate_name_compact) >= 4

    AND LEFT(s1_name_compact, 4)
        =
        LEFT(candidate_name_compact, 4)

    AND s1_house IS NOT NULL
    AND candidate_house IS NOT NULL
    AND s1_house = candidate_house
""").fetchone()[0]


prefix4_address5 = DB.execute("""
SELECT COUNT(*)

FROM missed_details

WHERE
    LENGTH(s1_name_compact) >= 4
    AND LENGTH(candidate_name_compact) >= 4

    AND LEFT(s1_name_compact, 4)
        =
        LEFT(candidate_name_compact, 4)

    AND LENGTH(s1_address_compact) >= 5
    AND LENGTH(candidate_address_compact) >= 5

    AND LEFT(s1_address_compact, 5)
        =
        LEFT(candidate_address_compact, 5)
""").fetchone()[0]


print(
    f"Prefix4 + same house:         "
    f"{prefix4_house:,} "
    f"({prefix4_house / missed_count * 100:.2f}%)"
)

print(
    f"Prefix4 + address prefix5:    "
    f"{prefix4_address5:,} "
    f"({prefix4_address5 / missed_count * 100:.2f}%)"
)


# ============================================================
# 10. CHARACTER-LEVEL NAME SIMILARITY
#
# We use DuckDB's edit_distance.
# This is only diagnostic, NOT candidate generation yet.
# ============================================================

print()
print("=" * 80)
print("EDIT-DISTANCE DIAGNOSTIC")
print("=" * 80)


DB.execute("""
CREATE OR REPLACE TEMP VIEW name_distance AS

SELECT
    *,
    editdist3(
        s1_name_compact,
        candidate_name_compact
    ) AS edit_dist

FROM missed_details

WHERE
    s1_name_compact <> ''
    AND candidate_name_compact <> ''
""")


edit1 = DB.execute("""
SELECT COUNT(*)
FROM name_distance
WHERE edit_dist <= 1
""").fetchone()[0]


edit2 = DB.execute("""
SELECT COUNT(*)
FROM name_distance
WHERE edit_dist <= 2
""").fetchone()[0]


edit3 = DB.execute("""
SELECT COUNT(*)
FROM name_distance
WHERE edit_dist <= 3
""").fetchone()[0]


edit4 = DB.execute("""
SELECT COUNT(*)
FROM name_distance
WHERE edit_dist <= 4
""").fetchone()[0]


print(
    f"Edit distance <=1:            "
    f"{edit1:,} "
    f"({edit1 / missed_count * 100:.2f}%)"
)

print(
    f"Edit distance <=2:            "
    f"{edit2:,} "
    f"({edit2 / missed_count * 100:.2f}%)"
)

print(
    f"Edit distance <=3:            "
    f"{edit3:,} "
    f"({edit3 / missed_count * 100:.2f}%)"
)

print(
    f"Edit distance <=4:            "
    f"{edit4:,} "
    f"({edit4 / missed_count * 100:.2f}%)"
)


# ============================================================
# 11. NAME TOKEN OVERLAP
# ============================================================

print()
print("=" * 80)
print("NAME TOKEN OVERLAP")
print("=" * 80)


DB.execute("""
CREATE OR REPLACE TEMP VIEW name_tokens AS

SELECT
    s1_id,
    candidate_id,

    COUNT(DISTINCT s1_token) AS shared_name_tokens

FROM
(
    SELECT
        d.s1_id,
        d.candidate_id,
        s1_token,
        candidate_token

    FROM missed_details d

    CROSS JOIN UNNEST(
        STRING_SPLIT(
            REGEXP_REPLACE(
                d.s1_name_compact,
                '[^a-z0-9]+',
                ' ',
                'g'
            ),
            ' '
        )
    ) AS a(s1_token)

    CROSS JOIN UNNEST(
        STRING_SPLIT(
            REGEXP_REPLACE(
                d.candidate_name_compact,
                '[^a-z0-9]+',
                ' ',
                'g'
            ),
            ' '
        )
    ) AS b(candidate_token)

    WHERE
        LENGTH(s1_token) >= 4
        AND LENGTH(candidate_token) >= 4
        AND s1_token = candidate_token
)

GROUP BY
    s1_id,
    candidate_id
""")


name_token_1 = DB.execute("""
SELECT COUNT(*)
FROM name_tokens
WHERE shared_name_tokens >= 1
""").fetchone()[0]


name_token_2 = DB.execute("""
SELECT COUNT(*)
FROM name_tokens
WHERE shared_name_tokens >= 2
""").fetchone()[0]


name_token_3 = DB.execute("""
SELECT COUNT(*)
FROM name_tokens
WHERE shared_name_tokens >= 3
""").fetchone()[0]


print(
    f">=1 shared name token:         "
    f"{name_token_1:,} "
    f"({name_token_1 / missed_count * 100:.2f}%)"
)

print(
    f">=2 shared name tokens:        "
    f"{name_token_2:,} "
    f"({name_token_2 / missed_count * 100:.2f}%)"
)

print(
    f">=3 shared name tokens:        "
    f"{name_token_3:,} "
    f"({name_token_3 / missed_count * 100:.2f}%)"
)


# ============================================================
# 12. SHOW STRONG CANDIDATE PATTERNS
# ============================================================

print()
print("=" * 80)
print("HIGH-VALUE MISSED PAIR EXAMPLES")
print("=" * 80)


DB.execute("""
CREATE OR REPLACE TEMP VIEW examples AS

SELECT
    d.s1_id,
    d.candidate_id,

    d.s1_name,
    d.candidate_name,

    d.s1_address,
    d.candidate_address,

    editdist3(
        d.s1_name_compact,
        d.candidate_name_compact
    ) AS edit_dist

FROM missed_details d

WHERE
    (
        LENGTH(d.s1_name_compact) >= 4
        AND LENGTH(d.candidate_name_compact) >= 4
        AND LEFT(d.s1_name_compact, 4)
            =
            LEFT(d.candidate_name_compact, 4)
    )

ORDER BY
    edit_dist ASC

LIMIT 30
""")


rows = DB.execute("""
SELECT
    s1_id,
    candidate_id,
    s1_name,
    candidate_name,
    s1_address,
    candidate_address,
    edit_dist

FROM examples
""").fetchall()


for row in rows:

    (
        s1_id,
        candidate_id,
        s1_name,
        candidate_name,
        s1_address,
        candidate_address,
        edit_dist
    ) = row

    print()
    print("-" * 80)
    print(f"S1 ID:        {s1_id}")
    print(f"Candidate ID: {candidate_id}")
    print(f"S1 name:      {s1_name}")
    print(f"Cand name:    {candidate_name}")
    print(f"Edit dist:    {edit_dist}")
    print(f"S1 address:   {s1_address}")
    print(f"Cand address: {candidate_address}")


# ============================================================
# 13. SUMMARY
# ============================================================

print()
print("=" * 80)
print("SUMMARY")
print("=" * 80)

print()
print(f"Remaining missed pairs: {missed_count:,}")

print()
print("NAME")
print(
    f"  Containment:          "
    f"{containment / missed_count * 100:.2f}%"
)
print(
    f"  Prefix 3:             "
    f"{prefix3 / missed_count * 100:.2f}%"
)
print(
    f"  Prefix 4:             "
    f"{prefix4 / missed_count * 100:.2f}%"
)
print(
    f"  Prefix 5:             "
    f"{prefix5 / missed_count * 100:.2f}%"
)
print(
    f"  Edit <= 2:            "
    f"{edit2 / missed_count * 100:.2f}%"
)
print(
    f"  Edit <= 3:            "
    f"{edit3 / missed_count * 100:.2f}%"
)
print(
    f"  >=2 shared tokens:    "
    f"{name_token_2 / missed_count * 100:.2f}%"
)

print()
print("ADDRESS")
print(
    f"  Same house number:    "
    f"{same_house / missed_count * 100:.2f}%"
)
print(
    f"  Exact address:        "
    f"{exact_address / missed_count * 100:.2f}%"
)

print()
print("COMBINED")
print(
    f"  Prefix4 + house:      "
    f"{prefix4_house / missed_count * 100:.2f}%"
)
print(
    f"  Prefix4 + addr5:      "
    f"{prefix4_address5 / missed_count * 100:.2f}%"
)

print()
print("=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)

DB.close()