import duckdb
import pandas as pd
from rapidfuzz import fuzz

# =========================================================
# CONFIG
# =========================================================

DB = duckdb.connect()

DB.execute("PRAGMA threads=2")
DB.execute("PRAGMA memory_limit='4GB'")

S1 = "parquet/train_source1_normalized.parquet"
S2 = "parquet/train_source2_normalized.parquet"
S3 = "parquet/train_source3_normalized.parquet"

BLOCKING_EVAL = "intermediate/blocking_eval_s1.parquet"
TRUE_PAIRS = "intermediate/blocking_eval_true_pairs.parquet"

OUTPUT = "intermediate/fuzzy_miss_diagnostics.parquet"


# =========================================================
# 1. LOAD FIXED 10K S1 SAMPLE
# =========================================================

print("=" * 70)
print("STEP 1: Loading fixed evaluation sample")
print("=" * 70)

DB.execute("""
CREATE OR REPLACE TEMP TABLE s1_raw AS
SELECT
    entity_id,
    name_norm,
    address_norm,
    country_norm
FROM read_parquet(?)
""", [BLOCKING_EVAL])

print("S1 evaluation sample loaded.")


# =========================================================
# 2. LOAD SOURCE 2 + SOURCE 3
# =========================================================

print()
print("=" * 70)
print("STEP 2: Loading Source 2 + Source 3")
print("=" * 70)

DB.execute("""
CREATE OR REPLACE TEMP TABLE src_raw AS

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

print("Source 2 + Source 3 loaded.")


# =========================================================
# 3. CREATE V4 KEYS
# =========================================================
#
# Important:
# We explicitly select columns.
# We DO NOT use ALTER TABLE or UPDATE.
# This prevents the previous "column already exists" problem.
# =========================================================

print()
print("=" * 70)
print("STEP 3: Creating V4 blocking keys")
print("=" * 70)

DB.execute("""
CREATE OR REPLACE TEMP TABLE s1_keys AS
WITH base AS (
    SELECT
        entity_id,
        name_norm,
        address_norm,
        country_norm
    FROM s1_raw
),

name_fixed AS (
    SELECT
        *,
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        lower(coalesce(name_norm, '')),
                                        '\\bprivate limited\\b',
                                        'pvt ltd'
                                    ),
                                    '\\bprivate ltd\\b',
                                    'pvt ltd'
                                ),
                                '\\bpvt limited\\b',
                                'pvt ltd'
                            ),
                            '\\bprivate\\b',
                            'pvt'
                        ),
                        '\\blimited\\b',
                        'ltd'
                    ),
                    '\\bincorporated\\b',
                    'inc'
                ),
                '\\bcorporation\\b',
                'corp'
            ),
            '\\bcompany\\b',
            'co'
        ) AS name_std
    FROM base
)

SELECT
    entity_id,
    name_norm,
    address_norm,
    country_norm,

    name_std,

    regexp_replace(
        name_std,
        '[^a-z0-9]',
        '',
        'g'
    ) AS name_compact,

    regexp_extract(
        name_std,
        '^([^ ]+ +[^ ]+)',
        1
    ) AS name_first_two,

    regexp_replace(
        regexp_replace(
            lower(coalesce(address_norm, '')),
            '\\bstreet\\b',
            'st',
            'g'
        ),
        '\\broad\\b',
        'rd',
        'g'
    ) AS address_std,

    regexp_extract(
        coalesce(address_norm, ''),
        '[0-9]+'
    ) AS house_number

FROM name_fixed
""")

DB.execute("""
CREATE OR REPLACE TEMP TABLE src_keys AS
WITH base AS (
    SELECT
        entity_id,
        name_norm,
        address_norm,
        country_norm
    FROM src_raw
),

name_fixed AS (
    SELECT
        *,
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        lower(coalesce(name_norm, '')),
                                        '\\bprivate limited\\b',
                                        'pvt ltd'
                                    ),
                                    '\\bprivate ltd\\b',
                                    'pvt ltd'
                                ),
                                '\\bpvt limited\\b',
                                'pvt ltd'
                            ),
                            '\\bprivate\\b',
                            'pvt'
                        ),
                        '\\blimited\\b',
                        'ltd'
                    ),
                    '\\bincorporated\\b',
                    'inc'
                ),
                '\\bcorporation\\b',
                'corp'
            ),
            '\\bcompany\\b',
            'co'
        ) AS name_std
    FROM base
)

SELECT
    entity_id,
    name_norm,
    address_norm,
    country_norm,

    name_std,

    regexp_replace(
        name_std,
        '[^a-z0-9]',
        '',
        'g'
    ) AS name_compact,

    regexp_extract(
        name_std,
        '^([^ ]+ +[^ ]+)',
        1
    ) AS name_first_two,

    regexp_replace(
        regexp_replace(
            lower(coalesce(address_norm, '')),
            '\\bstreet\\b',
            'st',
            'g'
        ),
        '\\broad\\b',
        'rd',
        'g'
    ) AS address_std,

    regexp_extract(
        coalesce(address_norm, ''),
        '[0-9]+'
    ) AS house_number

FROM name_fixed
""")

print("V4 keys created.")


# =========================================================
# 4. V4 FREQUENCY TABLES
# =========================================================

print()
print("=" * 70)
print("STEP 4: Creating V4 frequency tables")
print("=" * 70)

DB.execute("""
CREATE OR REPLACE TEMP TABLE name_first_two_freq AS
SELECT
    country_norm,
    name_first_two,
    COUNT(*) AS cnt
FROM src_keys
WHERE name_first_two <> ''
GROUP BY
    country_norm,
    name_first_two
""")

DB.execute("""
CREATE OR REPLACE TEMP TABLE house_freq AS
SELECT
    country_norm,
    house_number,
    COUNT(*) AS cnt
FROM src_keys
WHERE house_number <> ''
GROUP BY
    country_norm,
    house_number
""")

print("Frequency tables created.")


# =========================================================
# 5. V4 CANDIDATES
# =========================================================

print()
print("=" * 70)
print("STEP 5: Building V4 candidates")
print("=" * 70)

DB.execute("""
CREATE OR REPLACE TEMP TABLE v4_candidates AS

-- -------------------------------------------------------
-- V4 blocker 1:
-- country + exact standardized name
-- -------------------------------------------------------
SELECT DISTINCT
    s.entity_id AS source1_entity_id,
    x.entity_id AS candidate_entity_id

FROM s1_keys s

JOIN src_keys x
  ON s.country_norm = x.country_norm
 AND s.name_std = x.name_std


UNION


-- -------------------------------------------------------
-- V4 blocker 2:
-- country + exact compact name
-- -------------------------------------------------------
SELECT DISTINCT
    s.entity_id AS source1_entity_id,
    x.entity_id AS candidate_entity_id

FROM s1_keys s

JOIN src_keys x
  ON s.country_norm = x.country_norm
 AND s.name_compact = x.name_compact


UNION


-- -------------------------------------------------------
-- V4 blocker 3:
-- country + first two name tokens
-- only when source frequency <= 100
-- -------------------------------------------------------
SELECT DISTINCT
    s.entity_id AS source1_entity_id,
    x.entity_id AS candidate_entity_id

FROM s1_keys s

JOIN src_keys x
  ON s.country_norm = x.country_norm
 AND s.name_first_two = x.name_first_two

JOIN name_first_two_freq f
  ON x.country_norm = f.country_norm
 AND x.name_first_two = f.name_first_two

WHERE s.name_first_two <> ''
  AND f.cnt <= 100


UNION


-- -------------------------------------------------------
-- V4 blocker 4:
-- country + exact standardized address
-- -------------------------------------------------------
SELECT DISTINCT
    s.entity_id AS source1_entity_id,
    x.entity_id AS candidate_entity_id

FROM s1_keys s

JOIN src_keys x
  ON s.country_norm = x.country_norm
 AND s.address_std = x.address_std

WHERE s.address_std <> ''
  AND x.address_std <> ''


UNION


-- -------------------------------------------------------
-- V4 blocker 5:
-- country + house number
-- only when house number frequency <= 200
-- -------------------------------------------------------
SELECT DISTINCT
    s.entity_id AS source1_entity_id,
    x.entity_id AS candidate_entity_id

FROM s1_keys s

JOIN src_keys x
  ON s.country_norm = x.country_norm
 AND s.house_number = x.house_number

JOIN house_freq f
  ON x.country_norm = f.country_norm
 AND x.house_number = f.house_number

WHERE s.house_number <> ''
  AND x.house_number <> ''
  AND f.cnt <= 200
""")

v4_count = DB.execute("""
SELECT COUNT(*)
FROM v4_candidates
""").fetchone()[0]

print("V4 candidates:", f"{v4_count:,}")


# =========================================================
# 6. RECOVERY BLOCKER #1
# =========================================================

print()
print("=" * 70)
print("STEP 6: Building Recovery #1")
print("=" * 70)

# ---------------------------------------------------------
# S1 address tokens
# ---------------------------------------------------------

DB.execute("""
CREATE OR REPLACE TEMP TABLE s1_addr_tokens AS

SELECT DISTINCT
    entity_id AS source1_entity_id,
    country_norm,

    regexp_extract(
        coalesce(address_norm, ''),
        '[0-9]+'
    ) AS house_number,

    token

FROM (
    SELECT
        entity_id,
        address_norm,
        country_norm,

        unnest(
            string_split(
                regexp_replace(
                    lower(coalesce(address_norm, '')),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                ),
                ' '
            )
        ) AS token

    FROM s1_keys
)

WHERE length(token) >= 5
  AND NOT regexp_matches(token, '^[0-9]+$')
  AND token <> ''
""")

# ---------------------------------------------------------
# Source address tokens
# ---------------------------------------------------------

DB.execute("""
CREATE OR REPLACE TEMP TABLE src_addr_tokens AS

SELECT DISTINCT
    entity_id AS candidate_entity_id,
    country_norm,

    regexp_extract(
        coalesce(address_norm, ''),
        '[0-9]+'
    ) AS house_number,

    token

FROM (
    SELECT
        entity_id,
        address_norm,
        country_norm,

        unnest(
            string_split(
                regexp_replace(
                    lower(coalesce(address_norm, '')),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                ),
                ' '
            )
        ) AS token

    FROM src_keys
)

WHERE length(token) >= 5
  AND NOT regexp_matches(token, '^[0-9]+$')
  AND token <> ''
""")

# ---------------------------------------------------------
# Source token frequencies
# ---------------------------------------------------------

DB.execute("""
CREATE OR REPLACE TEMP TABLE src_addr_token_freq AS

SELECT
    country_norm,
    house_number,
    token,
    COUNT(*) AS cnt

FROM src_addr_tokens

GROUP BY
    country_norm,
    house_number,
    token
""")

# ---------------------------------------------------------
# Recovery #1 candidates
# ---------------------------------------------------------

DB.execute("""
CREATE OR REPLACE TEMP TABLE r1_candidates AS

SELECT DISTINCT
    s.source1_entity_id,
    x.candidate_entity_id

FROM s1_addr_tokens s

JOIN src_addr_tokens x
  ON s.country_norm = x.country_norm
 AND s.house_number = x.house_number
 AND s.token = x.token

JOIN src_addr_token_freq f
  ON x.country_norm = f.country_norm
 AND x.house_number = f.house_number
 AND x.token = f.token

WHERE s.house_number <> ''
  AND x.house_number <> ''
  AND f.cnt <= 50
""")

r1_count = DB.execute("""
SELECT COUNT(*)
FROM r1_candidates
""").fetchone()[0]

print("Recovery #1 candidates:", f"{r1_count:,}")


# =========================================================
# 7. COMBINE V4 + R1
# =========================================================

print()
print("=" * 70)
print("STEP 7: Combining V4 + Recovery #1")
print("=" * 70)

DB.execute("""
CREATE OR REPLACE TEMP TABLE base_candidates AS

SELECT
    source1_entity_id,
    candidate_entity_id
FROM v4_candidates

UNION

SELECT
    source1_entity_id,
    candidate_entity_id
FROM r1_candidates
""")

base_count = DB.execute("""
SELECT COUNT(*)
FROM base_candidates
""").fetchone()[0]

print("V4 + R1 candidates:", f"{base_count:,}")


# =========================================================
# 8. LOAD FIXED TRUE PAIRS
# =========================================================

print()
print("=" * 70)
print("STEP 8: Loading fixed true pairs")
print("=" * 70)

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

print("True pairs:", f"{true_count:,}")


# =========================================================
# 9. FIND MISSED TRUE PAIRS
# =========================================================

print()
print("=" * 70)
print("STEP 9: Finding true pairs missed by V4 + R1")
print("=" * 70)

missed = DB.execute("""
SELECT
    t.source1_entity_id,
    t.candidate_entity_id,

    s.name_norm AS s1_name,
    x.name_norm AS candidate_name,

    s.address_norm AS s1_address,
    x.address_norm AS candidate_address,

    s.country_norm

FROM true_pairs t

LEFT JOIN base_candidates b
  ON t.source1_entity_id = b.source1_entity_id
 AND t.candidate_entity_id = b.candidate_entity_id

JOIN s1_keys s
  ON t.source1_entity_id = s.entity_id

JOIN src_keys x
  ON t.candidate_entity_id = x.entity_id

WHERE b.source1_entity_id IS NULL
""").fetchdf()

print()
print("Remaining missed true pairs:", f"{len(missed):,}")


# =========================================================
# 10. FUZZY DIAGNOSTIC
# =========================================================

print()
print("=" * 70)
print("STEP 10: RapidFuzz diagnostic")
print("=" * 70)

print(
    "Calculating similarity ONLY on the missed true pairs..."
)


def safe(value):
    if pd.isna(value):
        return ""
    return str(value)


missed["s1_name"] = missed["s1_name"].map(safe)
missed["candidate_name"] = missed["candidate_name"].map(safe)

missed["s1_address"] = missed["s1_address"].map(safe)
missed["candidate_address"] = missed["candidate_address"].map(safe)


# ---------------------------------------------------------
# NAME METRICS
# ---------------------------------------------------------

missed["name_ratio"] = [
    fuzz.ratio(a, b)
    for a, b in zip(
        missed["s1_name"],
        missed["candidate_name"]
    )
]

missed["name_token_sort"] = [
    fuzz.token_sort_ratio(a, b)
    for a, b in zip(
        missed["s1_name"],
        missed["candidate_name"]
    )
]

missed["name_token_set"] = [
    fuzz.token_set_ratio(a, b)
    for a, b in zip(
        missed["s1_name"],
        missed["candidate_name"]
    )
]

missed["name_wratio"] = [
    fuzz.WRatio(a, b)
    for a, b in zip(
        missed["s1_name"],
        missed["candidate_name"]
    )
]


# ---------------------------------------------------------
# ADDRESS METRICS
# ---------------------------------------------------------

missed["address_ratio"] = [
    fuzz.ratio(a, b)
    for a, b in zip(
        missed["s1_address"],
        missed["candidate_address"]
    )
]

missed["address_token_set"] = [
    fuzz.token_set_ratio(a, b)
    for a, b in zip(
        missed["s1_address"],
        missed["candidate_address"]
    )
]

missed["address_wratio"] = [
    fuzz.WRatio(a, b)
    for a, b in zip(
        missed["s1_address"],
        missed["candidate_address"]
    )
]


# =========================================================
# 11. NAME THRESHOLD SUMMARY
# =========================================================

print()
print("=" * 70)
print("NAME SIMILARITY")
print("=" * 70)

for column in [
    "name_ratio",
    "name_token_sort",
    "name_token_set",
    "name_wratio",
]:

    print()
    print(column)

    for threshold in [95, 90, 85, 80, 75, 70]:

        count = int(
            (missed[column] >= threshold).sum()
        )

        pct = (
            count / len(missed) * 100
            if len(missed) > 0
            else 0
        )

        print(
            f"  >= {threshold}: "
            f"{count:5d} "
            f"({pct:6.2f}%)"
        )


# =========================================================
# 12. ADDRESS THRESHOLD SUMMARY
# =========================================================

print()
print("=" * 70)
print("ADDRESS SIMILARITY")
print("=" * 70)

for column in [
    "address_ratio",
    "address_token_set",
    "address_wratio",
]:

    print()
    print(column)

    for threshold in [95, 90, 85, 80, 75, 70]:

        count = int(
            (missed[column] >= threshold).sum()
        )

        pct = (
            count / len(missed) * 100
            if len(missed) > 0
            else 0
        )

        print(
            f"  >= {threshold}: "
            f"{count:5d} "
            f"({pct:6.2f}%)"
        )


# =========================================================
# 13. COMBINED CONDITIONS
# =========================================================

print()
print("=" * 70)
print("COMBINED NAME + ADDRESS")
print("=" * 70)

conditions = [

    (
        "name_wratio >= 90",
        missed["name_wratio"] >= 90
    ),

    (
        "name_wratio >= 90 AND address_wratio >= 70",

        (missed["name_wratio"] >= 90)
        &
        (missed["address_wratio"] >= 70)
    ),

    (
        "name_wratio >= 85 AND address_wratio >= 70",

        (missed["name_wratio"] >= 85)
        &
        (missed["address_wratio"] >= 70)
    ),

    (
        "name_token_set >= 90 AND address_wratio >= 70",

        (missed["name_token_set"] >= 90)
        &
        (missed["address_wratio"] >= 70)
    ),

    (
        "name_token_set >= 85 AND address_token_set >= 75",

        (missed["name_token_set"] >= 85)
        &
        (missed["address_token_set"] >= 75)
    ),
]


for label, mask in conditions:

    count = int(mask.sum())

    pct = (
        count / len(missed) * 100
        if len(missed) > 0
        else 0
    )

    print(
        f"{label:<60}"
        f"{count:5d} "
        f"({pct:6.2f}%)"
    )


# =========================================================
# 14. TOP NAME SIMILARITY CASES
# =========================================================

print()
print("=" * 70)
print("TOP 30 MISSED TRUE PAIRS BY NAME WRatio")
print("=" * 70)

name_columns = [
    "source1_entity_id",
    "candidate_entity_id",
    "s1_name",
    "candidate_name",
    "name_wratio",
    "name_token_set",
    "address_wratio",
]

print(
    missed
    .sort_values(
        ["name_wratio", "address_wratio"],
        ascending=False
    )[name_columns]
    .head(30)
    .to_string(index=False)
)


# =========================================================
# 15. TOP ADDRESS SIMILARITY CASES
# =========================================================

print()
print("=" * 70)
print("TOP 30 MISSED TRUE PAIRS BY ADDRESS WRatio")
print("=" * 70)

address_columns = [
    "source1_entity_id",
    "candidate_entity_id",
    "s1_address",
    "candidate_address",
    "address_wratio",
    "name_wratio",
    "name_token_set",
]

print(
    missed
    .sort_values(
        ["address_wratio", "name_wratio"],
        ascending=False
    )[address_columns]
    .head(30)
    .to_string(index=False)
)


# =========================================================
# 16. SAVE RESULTS
# =========================================================

print()
print("=" * 70)
print("STEP 16: Saving diagnostic results")
print("=" * 70)

missed.to_parquet(
    OUTPUT,
    index=False
)

print("Saved:", OUTPUT)
print("Rows:", f"{len(missed):,}")


# =========================================================
# DONE
# =========================================================

print()
print("=" * 70)
print("DIAGNOSTIC COMPLETE")
print("=" * 70)