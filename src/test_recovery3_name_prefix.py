import duckdb

DB = duckdb.connect()

DB.execute("PRAGMA threads=2")
DB.execute("PRAGMA memory_limit='4GB'")

S2 = "parquet/train_source2_normalized.parquet"
S3 = "parquet/train_source3_normalized.parquet"

BLOCKING_EVAL = "intermediate/blocking_eval_s1.parquet"
TRUE_PAIRS = "intermediate/blocking_eval_true_pairs.parquet"


# =========================================================
# 1. LOAD FIXED SAMPLE
# =========================================================

print("=" * 70)
print("STEP 1: Loading fixed evaluation sample")
print("=" * 70)

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

print()
print("=" * 70)
print("STEP 2: Loading Source 2 + Source 3")
print("=" * 70)

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

print("Source loaded.")


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

print("True pairs:", f"{true_count:,}")


# =========================================================
# 4. CREATE NAME TOKENS
# =========================================================

print()
print("=" * 70)
print("STEP 3: Creating informative name tokens")
print("=" * 70)

# Generic/legal words which should not be used as blocking keys.
GENERIC = """
('private',
 'limited',
 'ltd',
 'pvt',
 'pvtltd',
 'company',
 'co',
 'inc',
 'incorporated',
 'corporation',
 'corp',
 'llc',
 'llp',
 'pllc',
 'lp',
 'services',
 'service',
 'center',
 'centre',
 'group',
 'associates',
 'partners',
 'holdings',
 'international',
 'industries',
 'industry',
 'enterprises',
 'enterprise',
 'solutions',
 'technologies',
 'technology',
 'trading',
 'products',
 'product',
 'management',
 'foundation',
 'clinic',
 'medical',
 'healthcare',
 'health',
 'systems',
 'system',
 'consulting',
 'consultants')
"""


# ---------------------------------------------------------
# S1 tokens
# ---------------------------------------------------------

DB.execute(f"""
CREATE OR REPLACE TEMP TABLE s1_name_tokens AS

SELECT DISTINCT
    entity_id AS source1_entity_id,
    country_norm,
    token,
    substr(token, 1, 3) AS prefix3

FROM (
    SELECT
        entity_id,
        country_norm,

        unnest(
            string_split(
                regexp_replace(
                    lower(coalesce(name_norm, '')),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                ),
                ' '
            )
        ) AS token

    FROM s1
)

WHERE length(token) >= 5
  AND token NOT IN {GENERIC}
  AND token <> ''
""")


# ---------------------------------------------------------
# Source tokens
# ---------------------------------------------------------

print("Creating source name tokens...")

DB.execute(f"""
CREATE OR REPLACE TEMP TABLE src_name_tokens AS

SELECT DISTINCT
    entity_id AS candidate_entity_id,
    country_norm,
    token,
    substr(token, 1, 3) AS prefix3

FROM (
    SELECT
        entity_id,
        country_norm,

        unnest(
            string_split(
                regexp_replace(
                    lower(coalesce(name_norm, '')),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                ),
                ' '
            )
        ) AS token

    FROM src
)

WHERE length(token) >= 5
  AND token NOT IN {GENERIC}
  AND token <> ''
""")


# =========================================================
# 5. SOURCE PREFIX FREQUENCY
# =========================================================

print()
print("=" * 70)
print("STEP 4: Calculating prefix frequencies")
print("=" * 70)

DB.execute("""
CREATE OR REPLACE TEMP TABLE prefix_freq AS

SELECT
    country_norm,
    prefix3,
    COUNT(*) AS cnt

FROM src_name_tokens

GROUP BY
    country_norm,
    prefix3
""")


# =========================================================
# 6. RECOVERY #3 CANDIDATES
# =========================================================

print()
print("=" * 70)
print("STEP 5: Building Recovery #3 candidates")
print("=" * 70)

DB.execute("""
CREATE OR REPLACE TEMP TABLE r3_candidates AS

SELECT DISTINCT

    s.source1_entity_id,
    x.candidate_entity_id

FROM s1_name_tokens s

JOIN src_name_tokens x

  ON s.country_norm = x.country_norm
 AND s.prefix3 = x.prefix3

JOIN prefix_freq f

  ON x.country_norm = f.country_norm
 AND x.prefix3 = f.prefix3

WHERE f.cnt <= 200
""")

r3_count = DB.execute("""
SELECT COUNT(*)
FROM r3_candidates
""").fetchone()[0]

r3_s1 = DB.execute("""
SELECT COUNT(DISTINCT source1_entity_id)
FROM r3_candidates
""").fetchone()[0]

print()
print("Recovery #3 candidates:", f"{r3_count:,}")
print("S1 with R3 candidates:", f"{r3_s1:,}")


# =========================================================
# 7. CANDIDATE DISTRIBUTION
# =========================================================

stats = DB.execute("""
SELECT
    AVG(cnt),
    MEDIAN(cnt),
    QUANTILE_CONT(cnt, 0.95),
    MAX(cnt)

FROM (
    SELECT
        source1_entity_id,
        COUNT(*) AS cnt

    FROM r3_candidates

    GROUP BY source1_entity_id
)
""").fetchone()

print()
print("R3 candidate distribution:")
print("  Average :", round(stats[0], 2))
print("  Median  :", round(stats[1], 2))
print("  P95     :", round(stats[2], 2))
print("  Max     :", stats[3])


# =========================================================
# 8. R3 RECALL
# =========================================================

r3_found = DB.execute("""
SELECT COUNT(*)

FROM true_pairs t

JOIN r3_candidates r

  ON t.source1_entity_id = r.source1_entity_id
 AND t.candidate_entity_id = r.candidate_entity_id
""").fetchone()[0]

r3_recall = r3_found / true_count * 100

print()
print("=" * 70)
print("RECOVERY #3 RESULTS")
print("=" * 70)

print("True pairs :", f"{true_count:,}")
print("Found      :", f"{r3_found:,}")
print("Recall     :", f"{r3_recall:.2f}%")


# =========================================================
# 9. RECREATE V4
# =========================================================

print()
print("=" * 70)
print("STEP 6: Building V4 baseline")
print("=" * 70)

DB.execute("""
CREATE OR REPLACE TEMP TABLE s1_keys AS

WITH base AS (

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

    FROM s1
)

SELECT
    *,
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

FROM base
""")


DB.execute("""
CREATE OR REPLACE TEMP TABLE src_keys AS

WITH base AS (

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

    FROM src
)

SELECT
    *,
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

FROM base
""")


# =========================================================
# 10. V4 FREQUENCIES
# =========================================================

DB.execute("""
CREATE OR REPLACE TEMP TABLE name_first_two_freq AS

SELECT
    country_norm,
    name_first_two,
    COUNT(*) AS cnt

FROM src_keys

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

GROUP BY
    country_norm,
    house_number
""")


# =========================================================
# 11. V4 CANDIDATES
# =========================================================

DB.execute("""
CREATE OR REPLACE TEMP TABLE v4_candidates AS

SELECT DISTINCT
    s.entity_id AS source1_entity_id,
    x.entity_id AS candidate_entity_id

FROM s1_keys s

JOIN src_keys x
  ON s.country_norm = x.country_norm
 AND s.name_std = x.name_std


UNION


SELECT DISTINCT
    s.entity_id,
    x.entity_id

FROM s1_keys s

JOIN src_keys x
  ON s.country_norm = x.country_norm
 AND s.name_compact = x.name_compact


UNION


SELECT DISTINCT
    s.entity_id,
    x.entity_id

FROM s1_keys s

JOIN src_keys x
  ON s.country_norm = x.country_norm
 AND s.name_first_two = x.name_first_two

JOIN name_first_two_freq f
  ON x.country_norm = f.country_norm
 AND x.name_first_two = f.name_first_two

WHERE f.cnt <= 100


UNION


SELECT DISTINCT
    s.entity_id,
    x.entity_id

FROM s1_keys s

JOIN src_keys x
  ON s.country_norm = x.country_norm
 AND s.address_std = x.address_std

WHERE s.address_std <> ''
  AND x.address_std <> ''


UNION


SELECT DISTINCT
    s.entity_id,
    x.entity_id

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


# =========================================================
# 12. RECREATE R1
# =========================================================

print("Building Recovery #1...")

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
        country_norm,
        address_norm,

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
        country_norm,
        address_norm,

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


# =========================================================
# 13. BASE = V4 + R1
# =========================================================

print("Combining V4 + R1...")

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
# 14. BASE + R3
# =========================================================

print()
print("=" * 70)
print("STEP 7: Evaluating V4 + R1 + R3")
print("=" * 70)

DB.execute("""
CREATE OR REPLACE TEMP TABLE combined_candidates AS

SELECT
    source1_entity_id,
    candidate_entity_id

FROM base_candidates

UNION

SELECT
    source1_entity_id,
    candidate_entity_id

FROM r3_candidates
""")


combined_count = DB.execute("""
SELECT COUNT(*)
FROM combined_candidates
""").fetchone()[0]


base_found = DB.execute("""
SELECT COUNT(*)

FROM true_pairs t

JOIN base_candidates b

  ON t.source1_entity_id = b.source1_entity_id
 AND t.candidate_entity_id = b.candidate_entity_id
""").fetchone()[0]


combined_found = DB.execute("""
SELECT COUNT(*)

FROM true_pairs t

JOIN combined_candidates c

  ON t.source1_entity_id = c.source1_entity_id
 AND t.candidate_entity_id = c.candidate_entity_id
""").fetchone()[0]


new_found = combined_found - base_found

base_recall = base_found / true_count * 100
combined_recall = combined_found / true_count * 100


# =========================================================
# 15. FINAL RESULTS
# =========================================================

print()
print("=" * 70)
print("FINAL RECOVERY #3 RESULTS")
print("=" * 70)

print()
print("BASELINE V4 + R1")
print("-----------------")
print("Candidates :", f"{base_count:,}")
print("Found      :", f"{base_found:,}")
print("Recall     :", f"{base_recall:.2f}%")

print()
print("RECOVERY #3")
print("------------")
print("R3 candidates :", f"{r3_count:,}")
print("R3 found      :", f"{r3_found:,}")
print("R3 recall     :", f"{r3_recall:.2f}%")

print()
print("COMBINED V4 + R1 + R3")
print("----------------------")
print("Candidates :", f"{combined_count:,}")
print("Found      :", f"{combined_found:,}")
print("New pairs  :", f"{new_found:,}")
print("Recall     :", f"{combined_recall:.2f}%")

print()
print("Remaining missed pairs:")
print(
    f"{true_count - combined_found:,}"
)

print()
print("=" * 70)
print("EFFICIENCY")
print("=" * 70)

if new_found > 0:

    extra_candidates = combined_count - base_count

    print(
        "Additional candidates:",
        f"{extra_candidates:,}"
    )

    print(
        "New true pairs:",
        f"{new_found:,}"
    )

    print(
        "Candidates per new true pair:",
        f"{extra_candidates / new_found:.2f}"
    )

else:

    print("R3 recovered no new true pairs.")

print()
print("DONE")