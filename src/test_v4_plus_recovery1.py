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
print("V4 + RECOVERY BLOCKER #1")
print("=" * 80)


# ============================================================
# 1. FIXED S1
# ============================================================

con.execute(f"""
CREATE OR REPLACE TEMP TABLE sample_s1 AS
SELECT *
FROM read_parquet('{FIXED_S1}')
""")


# ============================================================
# 2. TRUE PAIRS
# ============================================================

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
# 3. SOURCE DATA
# ============================================================

con.execute(f"""
CREATE OR REPLACE TEMP TABLE sources AS

SELECT entity_id, country_norm, name_norm, address_norm
FROM read_parquet('{S2}')

UNION ALL

SELECT entity_id, country_norm, name_norm, address_norm
FROM read_parquet('{S3}')
""")


# ============================================================
# 4. V4 STANDARDIZATION
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


# ============================================================
# 5. V4 KEYS
# ============================================================

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
        THEN
            split_part(name_norm, ' ', 1)
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

FROM sample_s1
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
        THEN
            split_part(name_norm, ' ', 1)
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
# 6. V4 FREQUENCIES
# ============================================================

con.execute("""
CREATE OR REPLACE TEMP TABLE first_two_freq AS
SELECT
    country_norm,
    name_first_two,
    COUNT(*) AS freq
FROM source_keys
WHERE name_first_two <> ''
GROUP BY country_norm, name_first_two
""")


con.execute("""
CREATE OR REPLACE TEMP TABLE house_freq AS
SELECT
    country_norm,
    house_number,
    COUNT(*) AS freq
FROM source_keys
WHERE house_number <> ''
GROUP BY country_norm, house_number
""")


# ============================================================
# 7. V4 CANDIDATES
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
# 8. RECOVERY #1 TOKENS
# ============================================================

print("[2] Building Recovery #1 candidates...")


con.execute("""
CREATE OR REPLACE TEMP TABLE s1_addr AS
SELECT
    entity_id,
    country_norm,
    address_norm,
    regexp_extract(address_norm, '[0-9]+') AS house_number
FROM sample_s1
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
CREATE OR REPLACE TEMP TABLE s1_tokens AS

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
CREATE OR REPLACE TEMP TABLE source_tokens AS

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
CREATE OR REPLACE TEMP TABLE token_freq AS

SELECT
    country_norm,
    house_number,
    token,
    COUNT(DISTINCT entity_id) AS freq

FROM source_tokens

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

FROM s1_tokens s

JOIN source_tokens c
  ON s.country_norm = c.country_norm
 AND s.house_number = c.house_number
 AND s.token = c.token

JOIN token_freq f
  ON c.country_norm = f.country_norm
 AND c.house_number = f.house_number
 AND c.token = f.token

WHERE s.house_number <> ''
  AND s.token <> ''
  AND f.freq <= 50
""")


# ============================================================
# 9. UNION V4 + RECOVERY
# ============================================================

print("[3] Combining candidates...")

con.execute("""
CREATE OR REPLACE TEMP TABLE combined_candidates AS

SELECT s1_id, candidate_id
FROM v4_candidates

UNION

SELECT s1_id, candidate_id
FROM recovery1_candidates
""")


# ============================================================
# 10. BASIC COUNTS
# ============================================================

v4_count = con.execute("""
SELECT COUNT(*)
FROM v4_candidates
""").fetchone()[0]


recovery_count = con.execute("""
SELECT COUNT(*)
FROM recovery1_candidates
""").fetchone()[0]


combined_count = con.execute("""
SELECT COUNT(*)
FROM combined_candidates
""").fetchone()[0]


new_candidates = combined_count - v4_count


# ============================================================
# 11. RECALL
# ============================================================

v4_found = con.execute("""
SELECT COUNT(*)

FROM true_pairs t

INNER JOIN v4_candidates c
  ON t.s1_id = c.s1_id
 AND t.candidate_id = c.candidate_id
""").fetchone()[0]


recovery_found = con.execute("""
SELECT COUNT(*)

FROM true_pairs t

INNER JOIN recovery1_candidates c
  ON t.s1_id = c.s1_id
 AND t.candidate_id = c.candidate_id
""").fetchone()[0]


combined_found = con.execute("""
SELECT COUNT(*)

FROM true_pairs t

INNER JOIN combined_candidates c
  ON t.s1_id = c.s1_id
 AND t.candidate_id = c.candidate_id
""").fetchone()[0]


# ============================================================
# 12. NEW TRUE PAIRS FROM RECOVERY
# ============================================================

new_true_pairs = con.execute("""
SELECT COUNT(*)

FROM true_pairs t

INNER JOIN recovery1_candidates r
  ON t.s1_id = r.s1_id
 AND t.candidate_id = r.candidate_id

LEFT JOIN v4_candidates v
  ON t.s1_id = v.s1_id
 AND t.candidate_id = v.candidate_id

WHERE v.s1_id IS NULL
""").fetchone()[0]


# ============================================================
# 13. OVERLAP
# ============================================================

overlap = con.execute("""
SELECT COUNT(*)

FROM v4_candidates v

INNER JOIN recovery1_candidates r
  ON v.s1_id = r.s1_id
 AND v.candidate_id = r.candidate_id
""").fetchone()[0]


# ============================================================
# 14. DISTRIBUTION
# ============================================================

distribution = con.execute("""
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
# 15. OUTPUT
# ============================================================

print("\n" + "=" * 80)
print("V4 + RECOVERY #1 RESULTS")
print("=" * 80)

print("\nCandidate counts")

print(f"V4 candidates              : {v4_count:,}")
print(f"Recovery #1 candidates     : {recovery_count:,}")
print(f"Combined candidates        : {combined_count:,}")
print(f"Additional candidates     : {new_candidates:,}")
print(f"Overlap                    : {overlap:,}")

print("\nTrue-pair coverage")

print(f"Total true pairs           : {true_count:,}")
print(f"V4 found                   : {v4_found:,}")
print(f"Recovery #1 found          : {recovery_found:,}")
print(f"Combined found             : {combined_found:,}")
print(f"NEW true pairs recovered   : {new_true_pairs:,}")

print("\nRecall")

print(f"V4 recall                  : {v4_found * 100.0 / true_count:.2f}%")
print(
    f"Recovery #1 recall         : "
    f"{recovery_found * 100.0 / true_count:.2f}%"
)
print(
    f"Combined recall            : "
    f"{combined_found * 100.0 / true_count:.2f}%"
)

print("\nCombined candidate distribution")

print(f"Average/S1                 : {distribution[0]:.2f}")
print(f"Median                     : {distribution[1]:.2f}")
print(f"P95                        : {distribution[2]:.2f}")
print(f"Maximum                    : {distribution[3]:,}")

print("=" * 80)

con.close()