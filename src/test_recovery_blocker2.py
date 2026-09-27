from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parent.parent

PARQUET_DIR = ROOT / "parquet"
INTERMEDIATE = ROOT / "intermediate"

S2 = PARQUET_DIR / "train_source2_normalized.parquet"
S3 = PARQUET_DIR / "train_source3_normalized.parquet"

FIXED_S1 = INTERMEDIATE / "blocking_eval_s1.parquet"
FIXED_TRUE = INTERMEDIATE / "blocking_eval_true_pairs.parquet"

con = duckdb.connect()

con.execute("SET memory_limit='4GB'")
con.execute("SET threads=2")
con.execute("SET preserve_insertion_order=false")

print("=" * 80)
print("RECOVERY BLOCKER #2 - RARE NAME TOKEN")
print("=" * 80)


# ============================================================
# 1. FIXED S1 + GROUND TRUTH
# ============================================================

con.execute(f"""
CREATE OR REPLACE TEMP TABLE s1 AS
SELECT *
FROM read_parquet('{FIXED_S1}')
""")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE true_pairs AS
SELECT
    s1_id,
    candidate_id
FROM read_parquet('{FIXED_TRUE}')
""")


true_count = con.execute("""
SELECT COUNT(*)
FROM true_pairs
""").fetchone()[0]


# ============================================================
# 2. SOURCE DATA
# ============================================================

con.execute(f"""
CREATE OR REPLACE TEMP TABLE sources AS

SELECT
    entity_id,
    country_norm,
    name_norm,
    address_norm
FROM read_parquet('{S2}')

UNION ALL

SELECT
    entity_id,
    country_norm,
    name_norm,
    address_norm
FROM read_parquet('{S3}')
""")


# ============================================================
# 3. V4 KEYS
# ============================================================

def_key = """
    regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            lower(name_norm),
                                            '\\bprivate limited\\b',
                                            'pvt ltd',
                                            'g'
                                        ),
                                        '\\bprivate ltd\\b',
                                        'pvt ltd',
                                        'g'
                                    ),
                                    '\\bpvt limited\\b',
                                    'pvt ltd',
                                    'g'
                                ),
                                '\\bprivate\\b',
                                'pvt',
                                'g'
                            ),
                            '\\blimited\\b',
                            'ltd',
                            'g'
                        ),
                        '\\bincorporated\\b',
                        'inc',
                        'g'
                    ),
                    '\\bcorporation\\b',
                    'corp',
                    'g'
                ),
                '\\bcompany\\b',
                'co',
                'g'
            ),
            '\\bco\\.?\\b',
            'co',
            'g'
        ),
        '\\s+',
        ' ',
        'g'
    )
"""


con.execute(f"""
CREATE OR REPLACE TEMP TABLE s1_keys AS

SELECT
    entity_id,
    country_norm,
    name_norm,
    address_norm,

    {def_key} AS name_std,

    regexp_replace(
        {def_key},
        '\\s+',
        '',
        'g'
    ) AS name_compact,

    CASE
        WHEN array_length(string_split(name_norm, ' ')) >= 2
        THEN split_part(name_norm, ' ', 1)
             || ' '
             || split_part(name_norm, ' ', 2)
        ELSE name_norm
    END AS name_first_two,

    regexp_replace(
        lower(address_norm),
        '\\b(street|st)\\b',
        'st',
        'g'
    ) AS address_std,

    regexp_extract(address_norm, '[0-9]+') AS house_number

FROM s1
""")


con.execute(f"""
CREATE OR REPLACE TEMP TABLE source_keys AS

SELECT
    entity_id,
    country_norm,
    name_norm,
    address_norm,

    {def_key} AS name_std,

    regexp_replace(
        {def_key},
        '\\s+',
        '',
        'g'
    ) AS name_compact,

    CASE
        WHEN array_length(string_split(name_norm, ' ')) >= 2
        THEN split_part(name_norm, ' ', 1)
             || ' '
             || split_part(name_norm, ' ', 2)
        ELSE name_norm
    END AS name_first_two,

    regexp_replace(
        lower(address_norm),
        '\\b(street|st)\\b',
        'st',
        'g'
    ) AS address_std,

    regexp_extract(address_norm, '[0-9]+') AS house_number

FROM sources
""")


# ============================================================
# 4. V4 FREQUENCIES
# ============================================================

con.execute("""
CREATE OR REPLACE TEMP TABLE first_two_freq AS

SELECT
    country_norm,
    name_first_two,
    COUNT(*) AS freq

FROM source_keys

WHERE name_first_two <> ''

GROUP BY
    country_norm,
    name_first_two
""")


con.execute("""
CREATE OR REPLACE TEMP TABLE house_freq AS

SELECT
    country_norm,
    house_number,
    COUNT(*) AS freq

FROM source_keys

WHERE house_number <> ''

GROUP BY
    country_norm,
    house_number
""")


# ============================================================
# 5. V4 CANDIDATES
# ============================================================

print("\n[1] Building V4 candidates...")

con.execute("""
CREATE OR REPLACE TEMP TABLE v4_candidates AS

SELECT
    s.entity_id AS s1_id,
    c.entity_id AS candidate_id

FROM s1_keys s

JOIN source_keys c
  ON s.country_norm = c.country_norm
 AND s.name_std = c.name_std
 AND s.name_std <> ''

UNION

SELECT
    s.entity_id,
    c.entity_id

FROM s1_keys s

JOIN source_keys c
  ON s.country_norm = c.country_norm
 AND s.name_compact = c.name_compact
 AND s.name_compact <> ''

UNION

SELECT
    s.entity_id,
    c.entity_id

FROM s1_keys s

JOIN source_keys c
  ON s.country_norm = c.country_norm
 AND s.name_first_two = c.name_first_two
 AND s.name_first_two <> ''

JOIN first_two_freq f
  ON c.country_norm = f.country_norm
 AND c.name_first_two = f.name_first_two

WHERE f.freq <= 100

UNION

SELECT
    s.entity_id,
    c.entity_id

FROM s1_keys s

JOIN source_keys c
  ON s.country_norm = c.country_norm
 AND s.address_std = c.address_std
 AND s.address_std <> ''

UNION

SELECT
    s.entity_id,
    c.entity_id

FROM s1_keys s

JOIN source_keys c
  ON s.country_norm = c.country_norm
 AND s.house_number = c.house_number
 AND s.house_number <> ''

JOIN house_freq f
  ON c.country_norm = f.country_norm
 AND c.house_number = f.house_number

WHERE f.freq <= 200
""")


# ============================================================
# 6. RECOVERY #1
# ============================================================

print("[2] Building Recovery #1...")

con.execute("""
CREATE OR REPLACE TEMP TABLE s1_addr AS

SELECT
    entity_id,
    country_norm,
    address_norm,
    regexp_extract(address_norm, '[0-9]+') AS house_number

FROM s1
""")


con.execute("""
CREATE OR REPLACE TEMP TABLE source_addr AS

SELECT
    entity_id,
    country_norm,
    address_norm,
    regexp_extract(address_norm, '[0-9]+') AS house_number

FROM sources
""")


con.execute("""
CREATE OR REPLACE TEMP TABLE s1_addr_tokens AS

SELECT DISTINCT
    a.entity_id,
    a.country_norm,
    a.house_number,
    lower(trim(t.token)) AS token

FROM s1_addr a

CROSS JOIN UNNEST(
    string_split(
        regexp_replace(
            a.address_norm,
            '[^a-zA-Z0-9]+',
            ' ',
            'g'
        ),
        ' '
    )
) AS t(token)

WHERE length(trim(t.token)) >= 5
  AND NOT regexp_matches(
      trim(t.token),
      '^[0-9]+$'
  )
""")


con.execute("""
CREATE OR REPLACE TEMP TABLE source_addr_tokens AS

SELECT DISTINCT
    a.entity_id,
    a.country_norm,
    a.house_number,
    lower(trim(t.token)) AS token

FROM source_addr a

CROSS JOIN UNNEST(
    string_split(
        regexp_replace(
            a.address_norm,
            '[^a-zA-Z0-9]+',
            ' ',
            'g'
        ),
        ' '
    )
) AS t(token)

WHERE length(trim(t.token)) >= 5
  AND NOT regexp_matches(
      trim(t.token),
      '^[0-9]+$'
  )
""")


con.execute("""
CREATE OR REPLACE TEMP TABLE addr_token_freq AS

SELECT
    country_norm,
    house_number,
    token,
    COUNT(DISTINCT entity_id) AS freq

FROM source_addr_tokens

WHERE house_number <> ''
  AND token <> ''

GROUP BY
    country_norm,
    house_number,
    token
""")


con.execute("""
CREATE OR REPLACE TEMP TABLE recovery1_candidates AS

SELECT DISTINCT

    s.entity_id AS s1_id,
    c.entity_id AS candidate_id

FROM s1_addr_tokens s

JOIN source_addr_tokens c
  ON s.country_norm = c.country_norm
 AND s.house_number = c.house_number
 AND s.token = c.token

JOIN addr_token_freq f
  ON c.country_norm = f.country_norm
 AND c.house_number = f.house_number
 AND c.token = f.token

WHERE s.house_number <> ''
  AND s.token <> ''
  AND f.freq <= 50
""")


# ============================================================
# 7. RECOVERY #2 NAME TOKENS
# ============================================================

print("[3] Building Recovery #2 name tokens...")


# Generic/legal words are deliberately excluded.
# They are too common and produce huge candidate sets.

con.execute("""
CREATE OR REPLACE TEMP TABLE generic_words AS

SELECT *
FROM (
    VALUES
        ('private'),
        ('limited'),
        ('ltd'),
        ('pvt'),
        ('pvtltd'),
        ('private limited'),
        ('company'),
        ('co'),
        ('inc'),
        ('incorporated'),
        ('corporation'),
        ('corp'),
        ('llc'),
        ('llp'),
        ('pllc'),
        ('lp'),
        ('services'),
        ('service'),
        ('center'),
        ('centre'),
        ('group'),
        ('associates'),
        ('partners'),
        ('holdings'),
        ('international'),
        ('industries'),
        ('industry'),
        ('enterprises'),
        ('enterprise'),
        ('solutions'),
        ('technologies'),
        ('technology'),
        ('trading'),
        ('products'),
        ('product'),
        ('management'),
        ('foundation'),
        ('clinic'),
        ('medical'),
        ('healthcare'),
        ('health'),
        ('systems'),
        ('system'),
        ('consulting'),
        ('consultants')
) AS t(word)
""")


# Source name tokens

con.execute("""
CREATE OR REPLACE TEMP TABLE source_name_tokens AS

SELECT DISTINCT
    s.entity_id,
    s.country_norm,
    lower(trim(t.token)) AS token

FROM sources s

CROSS JOIN UNNEST(
    string_split(
        regexp_replace(
            s.name_norm,
            '[^a-zA-Z0-9]+',
            ' ',
            'g'
        ),
        ' '
    )
) AS t(token)

LEFT JOIN generic_words g
  ON lower(trim(t.token)) = g.word

WHERE length(trim(t.token)) >= 5
  AND g.word IS NULL
""")


# S1 name tokens

con.execute("""
CREATE OR REPLACE TEMP TABLE s1_name_tokens AS

SELECT DISTINCT
    s.entity_id,
    s.country_norm,
    lower(trim(t.token)) AS token

FROM s1 s

CROSS JOIN UNNEST(
    string_split(
        regexp_replace(
            s.name_norm,
            '[^a-zA-Z0-9]+',
            ' ',
            'g'
        ),
        ' '
    )
) AS t(token)

LEFT JOIN generic_words g
  ON lower(trim(t.token)) = g.word

WHERE length(trim(t.token)) >= 5
  AND g.word IS NULL
""")


# Frequency of each token in the source data.

con.execute("""
CREATE OR REPLACE TEMP TABLE name_token_freq AS

SELECT
    country_norm,
    token,
    COUNT(DISTINCT entity_id) AS freq

FROM source_name_tokens

GROUP BY
    country_norm,
    token
""")


print("[4] Building Recovery #2 candidates...")


# Rare informative token:
# frequency <= 100.
#
# Same country is mandatory.
#
# A token such as "kishore" can connect:
#   kishore traders private limited
#   private kishore traders limited
#
# while very common tokens are rejected.

con.execute("""
CREATE OR REPLACE TEMP TABLE recovery2_candidates AS

SELECT DISTINCT

    s.entity_id AS s1_id,
    c.entity_id AS candidate_id

FROM s1_name_tokens s

JOIN source_name_tokens c
  ON s.country_norm = c.country_norm
 AND s.token = c.token

JOIN name_token_freq f
  ON c.country_norm = f.country_norm
 AND c.token = f.token

WHERE f.freq <= 100
""")


# ============================================================
# 8. COMBINED BASE
# ============================================================

print("[5] Combining V4 + Recovery #1 + Recovery #2...")


con.execute("""
CREATE OR REPLACE TEMP TABLE base_candidates AS

SELECT s1_id, candidate_id
FROM v4_candidates

UNION

SELECT s1_id, candidate_id
FROM recovery1_candidates
""")


con.execute("""
CREATE OR REPLACE TEMP TABLE combined_candidates AS

SELECT s1_id, candidate_id
FROM base_candidates

UNION

SELECT s1_id, candidate_id
FROM recovery2_candidates
""")


# ============================================================
# 9. COUNTS
# ============================================================

v4_count = con.execute("""
SELECT COUNT(*)
FROM v4_candidates
""").fetchone()[0]


r1_count = con.execute("""
SELECT COUNT(*)
FROM recovery1_candidates
""").fetchone()[0]


r2_count = con.execute("""
SELECT COUNT(*)
FROM recovery2_candidates
""").fetchone()[0]


base_count = con.execute("""
SELECT COUNT(*)
FROM base_candidates
""").fetchone()[0]


combined_count = con.execute("""
SELECT COUNT(*)
FROM combined_candidates
""").fetchone()[0]


# ============================================================
# 10. TRUE PAIRS FOUND
# ============================================================

v4_found = con.execute("""
SELECT COUNT(*)

FROM true_pairs t

JOIN v4_candidates c
  ON t.s1_id = c.s1_id
 AND t.candidate_id = c.candidate_id
""").fetchone()[0]


r1_found = con.execute("""
SELECT COUNT(*)

FROM true_pairs t

JOIN recovery1_candidates c
  ON t.s1_id = c.s1_id
 AND t.candidate_id = c.candidate_id
""").fetchone()[0]


r2_found = con.execute("""
SELECT COUNT(*)

FROM true_pairs t

JOIN recovery2_candidates c
  ON t.s1_id = c.s1_id
 AND t.candidate_id = c.candidate_id
""").fetchone()[0]


base_found = con.execute("""
SELECT COUNT(*)

FROM true_pairs t

JOIN base_candidates c
  ON t.s1_id = c.s1_id
 AND t.candidate_id = c.candidate_id
""").fetchone()[0]


combined_found = con.execute("""
SELECT COUNT(*)

FROM true_pairs t

JOIN combined_candidates c
  ON t.s1_id = c.s1_id
 AND t.candidate_id = c.candidate_id
""").fetchone()[0]


# ============================================================
# 11. NEW TRUE PAIRS FROM RECOVERY #2
# ============================================================

r2_new_true = con.execute("""
SELECT COUNT(*)

FROM true_pairs t

JOIN recovery2_candidates r
  ON t.s1_id = r.s1_id
 AND t.candidate_id = r.candidate_id

LEFT JOIN base_candidates b
  ON t.s1_id = b.s1_id
 AND t.candidate_id = b.candidate_id

WHERE b.s1_id IS NULL
""").fetchone()[0]


# ============================================================
# 12. REMAINING MISSES
# ============================================================

remaining = true_count - combined_found


# ============================================================
# 13. CANDIDATE OVERLAP
# ============================================================

r2_overlap_base = con.execute("""
SELECT COUNT(*)

FROM recovery2_candidates r

JOIN base_candidates b
  ON r.s1_id = b.s1_id
 AND r.candidate_id = b.candidate_id
""").fetchone()[0]


r2_new_candidates = combined_count - base_count


# ============================================================
# 14. DISTRIBUTION
# ============================================================

dist = con.execute("""
SELECT
    AVG(cnt),
    MEDIAN(cnt),
    QUANTILE_CONT(cnt, 0.95),
    MAX(cnt)

FROM (
    SELECT
        s1_id,
        COUNT(*) AS cnt

    FROM combined_candidates

    GROUP BY s1_id
)
""").fetchone()


# ============================================================
# 15. RESULTS
# ============================================================

print("\n")
print("=" * 80)
print("RECOVERY #2 RESULTS")
print("=" * 80)

print("\nCandidate counts")
print("-" * 80)

print(f"V4 candidates                  : {v4_count:,}")
print(f"Recovery #1 candidates         : {r1_count:,}")
print(f"Recovery #2 candidates         : {r2_count:,}")
print(f"V4 + Recovery #1               : {base_count:,}")
print(f"Final combined candidates      : {combined_count:,}")
print(f"New candidates from Recovery2  : {r2_new_candidates:,}")
print(f"Recovery2 overlap with base    : {r2_overlap_base:,}")


print("\nTrue-pair coverage")
print("-" * 80)

print(f"Total true pairs               : {true_count:,}")
print(f"V4 found                       : {v4_found:,}")
print(f"Recovery #1 found              : {r1_found:,}")
print(f"Recovery #2 found              : {r2_found:,}")
print(f"V4 + Recovery #1 found         : {base_found:,}")
print(f"V4 + R1 + R2 found             : {combined_found:,}")
print(f"NEW true pairs from Recovery2  : {r2_new_true:,}")
print(f"Remaining true pairs            : {remaining:,}")


print("\nRecall")
print("-" * 80)

print(
    f"V4 recall                      : "
    f"{v4_found * 100.0 / true_count:.2f}%"
)

print(
    f"V4 + Recovery #1 recall        : "
    f"{base_found * 100.0 / true_count:.2f}%"
)

print(
    f"V4 + R1 + R2 recall            : "
    f"{combined_found * 100.0 / true_count:.2f}%"
)


print("\nCombined candidate distribution")
print("-" * 80)

print(f"Average/S1                     : {dist[0]:.2f}")
print(f"Median                         : {dist[1]:.2f}")
print(f"P95                            : {dist[2]:.2f}")
print(f"Maximum                        : {dist[3]:,}")


print("=" * 80)

con.close()