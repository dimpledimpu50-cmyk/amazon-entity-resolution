from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parent.parent

PARQUET_DIR = ROOT / "parquet"
INTERMEDIATE = ROOT / "intermediate"

S1 = INTERMEDIATE / "blocking_eval_s1.parquet"
TRUE = INTERMEDIATE / "blocking_eval_true_pairs.parquet"

S2 = PARQUET_DIR / "train_source2_normalized.parquet"
S3 = PARQUET_DIR / "train_source3_normalized.parquet"

con = duckdb.connect()

con.execute("SET memory_limit='4GB'")
con.execute("SET threads=2")
con.execute("SET preserve_insertion_order=false")


# ============================================================
# 1. FIXED DATA
# ============================================================

con.execute(f"""
CREATE OR REPLACE TEMP TABLE s1 AS
SELECT *
FROM read_parquet('{S1}')
""")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE true_pairs AS
SELECT s1_id, candidate_id
FROM read_parquet('{TRUE}')
""")


# ============================================================
# 2. SOURCE DATA
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
# 3. RECREATE V4 CANDIDATES
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


con.execute("""
CREATE OR REPLACE TEMP TABLE v4_candidates AS

SELECT s.entity_id AS s1_id, c.entity_id AS candidate_id
FROM s1_keys s
JOIN source_keys c
  ON s.country_norm = c.country_norm
 AND s.name_std = c.name_std
 AND s.name_std <> ''

UNION

SELECT s.entity_id, c.entity_id
FROM s1_keys s
JOIN source_keys c
  ON s.country_norm = c.country_norm
 AND s.name_compact = c.name_compact
 AND s.name_compact <> ''

UNION

SELECT s.entity_id, c.entity_id
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

SELECT s.entity_id, c.entity_id
FROM s1_keys s
JOIN source_keys c
  ON s.country_norm = c.country_norm
 AND s.address_std = c.address_std
 AND s.address_std <> ''

UNION

SELECT s.entity_id, c.entity_id
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
# 4. RECOVERY #1
# ============================================================

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
AND NOT regexp_matches(trim(t.token), '^[0-9]+$')
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
AND NOT regexp_matches(trim(t.token), '^[0-9]+$')
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

GROUP BY country_norm, house_number, token
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
# 5. FIND REMAINING MISSES
# ============================================================

con.execute("""
CREATE OR REPLACE TEMP TABLE combined_candidates AS

SELECT s1_id, candidate_id
FROM v4_candidates

UNION

SELECT s1_id, candidate_id
FROM recovery1_candidates
""")


con.execute("""
CREATE OR REPLACE TEMP TABLE missed AS

SELECT
    t.s1_id,
    t.candidate_id

FROM true_pairs t

LEFT JOIN combined_candidates c
  ON t.s1_id = c.s1_id
 AND t.candidate_id = c.candidate_id

WHERE c.s1_id IS NULL
""")


missed_count = con.execute("""
SELECT COUNT(*)
FROM missed
""").fetchone()[0]

print("=" * 80)
print(f"REMAINING MISSED TRUE PAIRS: {missed_count:,}")
print("=" * 80)


# ============================================================
# 6. JOIN ACTUAL RECORDS
# ============================================================

con.execute("""
CREATE OR REPLACE TEMP TABLE missed_details AS

SELECT
    m.s1_id,
    m.candidate_id,

    s.country_norm AS s1_country,
    s.name_norm AS s1_name,
    s.address_norm AS s1_address,

    c.name_norm AS candidate_name,
    c.address_norm AS candidate_address

FROM missed m

JOIN s1 s
  ON m.s1_id = s.entity_id

JOIN sources c
  ON m.candidate_id = c.entity_id
""")


# ============================================================
# 7. PRINT RANDOM SAMPLE
# ============================================================

print("\nSAMPLE OF REMAINING MISSES")
print("=" * 80)

rows = con.execute("""
SELECT
    s1_id,
    candidate_id,
    s1_name,
    candidate_name,
    s1_address,
    candidate_address
FROM missed_details
USING SAMPLE 40
""").fetchall()


for i, row in enumerate(rows, 1):

    s1_id, cid, s1_name, cand_name, s1_addr, cand_addr = row

    print(f"\n--- CASE {i} ---")
    print(f"S1 ID       : {s1_id}")
    print(f"Candidate ID: {cid}")
    print(f"S1 name     : {s1_name}")
    print(f"Cand name   : {cand_name}")
    print(f"S1 address  : {s1_addr}")
    print(f"Cand address: {cand_addr}")


# ============================================================
# 8. SAVE SAMPLE
# ============================================================

output = INTERMEDIATE / "remaining_missed_pairs_sample.tsv"

con.execute(f"""
COPY (
    SELECT
        s1_id,
        candidate_id,
        s1_name,
        candidate_name,
        s1_address,
        candidate_address
    FROM missed_details
    USING SAMPLE 200
)
TO '{output}'
WITH (
    FORMAT CSV,
    DELIMITER '\\t',
    HEADER
)
""")

print("\nSaved sample:")
print(output)

con.close()