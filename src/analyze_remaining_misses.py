import duckdb

DB = duckdb.connect()

S1 = "intermediate/blocking_eval_s1.parquet"
SRC = """
    SELECT entity_id, name_norm, address_norm, country_norm
    FROM read_parquet([
        'parquet/train_source2_normalized.parquet',
        'parquet/train_source3_normalized.parquet'
    ])
"""

BASE = "intermediate/verified_baseline_candidates.parquet"
R7 = "intermediate/recovery7_candidates.parquet"
R10 = "intermediate/recovery10_candidates.parquet"
TRUE = "intermediate/blocking_eval_true_pairs.parquet"


# ============================================================
# LOAD
# ============================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW s1 AS
SELECT
    entity_id,
    name_norm,
    address_norm,
    country_norm
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
FROM read_parquet('{TRUE}')
""")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW base AS
SELECT *
FROM read_parquet('{BASE}')
""")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW r7 AS
SELECT *
FROM read_parquet('{R7}')
""")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW r10 AS
SELECT *
FROM read_parquet('{R10}')
""")


# ============================================================
# CURRENT COMBINED CANDIDATES
# ============================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW current_candidates AS

SELECT
    source1_entity_id,
    candidate_entity_id
FROM base

UNION

SELECT
    source1_entity_id,
    candidate_entity_id
FROM r7

UNION

SELECT
    source1_entity_id,
    candidate_entity_id
FROM r10
""")


# ============================================================
# REMAINING MISSED TRUE PAIRS
# ============================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW remaining AS

SELECT
    t.source1_entity_id,
    t.candidate_entity_id,

    s.name_norm AS s1_name,
    c.name_norm AS candidate_name,

    s.address_norm AS s1_address,
    c.address_norm AS candidate_address,

    s.country_norm AS country

FROM true_pairs t

JOIN s1 s
    ON t.source1_entity_id = s.entity_id

JOIN src c
    ON t.candidate_entity_id = c.entity_id

LEFT JOIN current_candidates cc
    ON t.source1_entity_id = cc.source1_entity_id
   AND t.candidate_entity_id = cc.candidate_entity_id

WHERE cc.source1_entity_id IS NULL
""")


# ============================================================
# TOTAL
# ============================================================

remaining_count = DB.execute("""
SELECT COUNT(*)
FROM remaining
""").fetchone()[0]

print("=" * 75)
print("REMAINING MISSED-PAIR ANALYSIS")
print("=" * 75)

print()
print(
    "Remaining missed true pairs:",
    f"{remaining_count:,}"
)

print()


# ============================================================
# NAME NORMALIZATION HELPERS
# ============================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW analyzed AS

SELECT
    *,

    regexp_replace(
        lower(coalesce(s1_name, '')),
        '[^a-z0-9]',
        '',
        'g'
    ) AS s1_name_compact,

    regexp_replace(
        lower(coalesce(candidate_name, '')),
        '[^a-z0-9]',
        '',
        'g'
    ) AS candidate_name_compact,

    regexp_replace(
        lower(coalesce(s1_address, '')),
        '[^a-z0-9]',
        '',
        'g'
    ) AS s1_address_compact,

    regexp_replace(
        lower(coalesce(candidate_address, '')),
        '[^a-z0-9]',
        '',
        'g'
    ) AS candidate_address_compact

FROM remaining
""")


# ============================================================
# 1. EXACT COMPACT NAME
# ============================================================

exact_name = DB.execute("""
SELECT COUNT(*)
FROM analyzed
WHERE
    s1_name_compact <> ''
    AND candidate_name_compact <> ''
    AND s1_name_compact = candidate_name_compact
""").fetchone()[0]

print(
    "1. Exact compact name:",
    f"{exact_name:,}",
    f"({100 * exact_name / remaining_count:.2f}%)"
)


# ============================================================
# 2. NAME CONTAINMENT
# ============================================================

name_containment = DB.execute("""
SELECT COUNT(*)
FROM analyzed
WHERE
    length(s1_name_compact) >= 5
    AND length(candidate_name_compact) >= 5
    AND (
        strpos(s1_name_compact, candidate_name_compact) > 0
        OR
        strpos(candidate_name_compact, s1_name_compact) > 0
    )
""").fetchone()[0]

print(
    "2. Name containment:",
    f"{name_containment:,}",
    f"({100 * name_containment / remaining_count:.2f}%)"
)


# ============================================================
# 3. SAME FIRST 3 CHARACTERS
# ============================================================

first3 = DB.execute("""
SELECT COUNT(*)
FROM analyzed
WHERE
    length(s1_name_compact) >= 3
    AND length(candidate_name_compact) >= 3
    AND substr(s1_name_compact, 1, 3)
        =
        substr(candidate_name_compact, 1, 3)
""").fetchone()[0]

print(
    "3. Same first 3 name chars:",
    f"{first3:,}",
    f"({100 * first3 / remaining_count:.2f}%)"
)


# ============================================================
# 4. SAME FIRST 4 CHARACTERS
# ============================================================

first4 = DB.execute("""
SELECT COUNT(*)
FROM analyzed
WHERE
    length(s1_name_compact) >= 4
    AND length(candidate_name_compact) >= 4
    AND substr(s1_name_compact, 1, 4)
        =
        substr(candidate_name_compact, 1, 4)
""").fetchone()[0]

print(
    "4. Same first 4 name chars:",
    f"{first4:,}",
    f"({100 * first4 / remaining_count:.2f}%)"
)


# ============================================================
# 5. SAME FIRST 5 CHARACTERS
# ============================================================

first5 = DB.execute("""
SELECT COUNT(*)
FROM analyzed
WHERE
    length(s1_name_compact) >= 5
    AND length(candidate_name_compact) >= 5
    AND substr(s1_name_compact, 1, 5)
        =
        substr(candidate_name_compact, 1, 5)
""").fetchone()[0]

print(
    "5. Same first 5 name chars:",
    f"{first5:,}",
    f"({100 * first5 / remaining_count:.2f}%)"
)


# ============================================================
# 6. SAME LAST 4 CHARACTERS
# ============================================================

last4 = DB.execute("""
SELECT COUNT(*)
FROM analyzed
WHERE
    length(s1_name_compact) >= 4
    AND length(candidate_name_compact) >= 4
    AND right(s1_name_compact, 4)
        =
        right(candidate_name_compact, 4)
""").fetchone()[0]

print(
    "6. Same last 4 name chars:",
    f"{last4:,}",
    f"({100 * last4 / remaining_count:.2f}%)"
)


# ============================================================
# 7. SAME NAME LENGTH ±2
# ============================================================

similar_length = DB.execute("""
SELECT COUNT(*)
FROM analyzed
WHERE
    abs(
        length(s1_name_compact)
        -
        length(candidate_name_compact)
    ) <= 2
""").fetchone()[0]

print(
    "7. Name length difference <=2:",
    f"{similar_length:,}",
    f"({100 * similar_length / remaining_count:.2f}%)"
)


# ============================================================
# 8. SAME FIRST NUMERIC ADDRESS
# ============================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW numeric_analysis AS

SELECT
    *,

    TRY_CAST(
        regexp_extract(
            coalesce(s1_address, ''),
            '[0-9]+'
        )
        AS BIGINT
    ) AS s1_house,

    TRY_CAST(
        regexp_extract(
            coalesce(candidate_address, ''),
            '[0-9]+'
        )
        AS BIGINT
    ) AS candidate_house

FROM analyzed
""")

same_house = DB.execute("""
SELECT COUNT(*)
FROM numeric_analysis
WHERE
    s1_house IS NOT NULL
    AND candidate_house IS NOT NULL
    AND s1_house = candidate_house
""").fetchone()[0]

print(
    "8. Same canonical house number:",
    f"{same_house:,}",
    f"({100 * same_house / remaining_count:.2f}%)"
)


# ============================================================
# 9. SAME FIRST 5 ADDRESS CHARACTERS
# ============================================================

address_first5 = DB.execute("""
SELECT COUNT(*)
FROM numeric_analysis
WHERE
    length(s1_address_compact) >= 5
    AND length(candidate_address_compact) >= 5
    AND substr(s1_address_compact, 1, 5)
        =
        substr(candidate_address_compact, 1, 5)
""").fetchone()[0]

print(
    "9. Same first 5 address chars:",
    f"{address_first5:,}",
    f"({100 * address_first5 / remaining_count:.2f}%)"
)


# ============================================================
# 10. SAME EXACT ADDRESS
# ============================================================

exact_address = DB.execute("""
SELECT COUNT(*)
FROM numeric_analysis
WHERE
    s1_address_compact <> ''
    AND candidate_address_compact <> ''
    AND s1_address_compact = candidate_address_compact
""").fetchone()[0]

print(
    "10. Exact compact address:",
    f"{exact_address:,}",
    f"({100 * exact_address / remaining_count:.2f}%)"
)


# ============================================================
# 11. NAME PREFIX + SAME HOUSE
# ============================================================

prefix_house = DB.execute("""
SELECT COUNT(*)
FROM numeric_analysis
WHERE
    length(s1_name_compact) >= 4
    AND length(candidate_name_compact) >= 4
    AND substr(s1_name_compact, 1, 4)
        =
        substr(candidate_name_compact, 1, 4)

    AND s1_house IS NOT NULL
    AND candidate_house IS NOT NULL
    AND s1_house = candidate_house
""").fetchone()[0]

print(
    "11. First 4 name chars + same house:",
    f"{prefix_house:,}",
    f"({100 * prefix_house / remaining_count:.2f}%)"
)


# ============================================================
# 12. NAME PREFIX + ADDRESS PREFIX
# ============================================================

prefix_address = DB.execute("""
SELECT COUNT(*)
FROM numeric_analysis
WHERE
    length(s1_name_compact) >= 4
    AND length(candidate_name_compact) >= 4
    AND substr(s1_name_compact, 1, 4)
        =
        substr(candidate_name_compact, 1, 4)

    AND length(s1_address_compact) >= 5
    AND length(candidate_address_compact) >= 5
    AND substr(s1_address_compact, 1, 5)
        =
        substr(candidate_address_compact, 1, 5)
""").fetchone()[0]

print(
    "12. First 4 name + first 5 address:",
    f"{prefix_address:,}",
    f"({100 * prefix_address / remaining_count:.2f}%)"
)


# ============================================================
# SAMPLE MISSED PAIRS
# ============================================================

print()
print("=" * 75)
print("SAMPLE REMAINING MISSED PAIRS")
print("=" * 75)

samples = DB.execute("""
SELECT
    source1_entity_id,
    candidate_entity_id,
    s1_name,
    candidate_name,
    s1_address,
    candidate_address,
    country
FROM numeric_analysis
ORDER BY
    source1_entity_id,
    candidate_entity_id
LIMIT 50
""").fetchall()

for row in samples:

    (
        s1_id,
        candidate_id,
        s1_name,
        candidate_name,
        s1_address,
        candidate_address,
        country
    ) = row

    print()
    print("-" * 75)

    print(
        "S1 ID       :",
        s1_id
    )

    print(
        "Candidate   :",
        candidate_id
    )

    print(
        "Country     :",
        country
    )

    print(
        "S1 name     :",
        s1_name
    )

    print(
        "Candidate   :",
        candidate_name
    )

    print(
        "S1 address  :",
        s1_address
    )

    print(
        "Candidate   :",
        candidate_address
    )


# ============================================================
# SAVE REMAINING MISSED PAIRS
# ============================================================

DB.execute("""
COPY numeric_analysis
TO 'intermediate/remaining_missed_pairs_r7_r10.parquet'
(FORMAT PARQUET)
""")


print()
print("=" * 75)
print("SAVED")
print("=" * 75)

print(
    "intermediate/remaining_missed_pairs_r7_r10.parquet"
)

print("=" * 75)
print("DONE")
print("=" * 75)