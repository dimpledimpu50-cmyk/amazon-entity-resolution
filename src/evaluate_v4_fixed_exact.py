from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parent.parent
PARQUET_DIR = ROOT / "parquet"
INTERMEDIATE = ROOT / "intermediate"

S1 = PARQUET_DIR / "train_source1_normalized.parquet"
S2 = PARQUET_DIR / "train_source2_normalized.parquet"
S3 = PARQUET_DIR / "train_source3_normalized.parquet"

FIXED_S1 = INTERMEDIATE / "blocking_eval_s1.parquet"
FIXED_TRUE = INTERMEDIATE / "blocking_eval_true_pairs.parquet"

con = duckdb.connect()

# RAM-safe
con.execute("SET memory_limit='4GB'")
con.execute("SET threads TO 2")
con.execute("SET preserve_insertion_order=false")


# ============================================================
# 1. FIXED S1 SAMPLE
# ============================================================

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE sample_s1 AS
    SELECT *
    FROM read_parquet('{FIXED_S1}')
""")


# ============================================================
# 2. SOURCES
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
# 3. FIXED GROUND TRUTH
# ============================================================

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE true_pairs AS
    SELECT
        s1_id AS source1_entity_id,
        candidate_id AS candidate_entity_id
    FROM read_parquet('{FIXED_TRUE}')
""")


# ============================================================
# 4. STANDARDIZED KEYS
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

        split_part(address_norm, ' ', 1) AS addr_first,

        regexp_extract(address_norm, '[0-9]+') AS house_number,

        country_norm
            || '|'
            || regexp_extract(address_norm, '[0-9]+')
            AS house_key

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

        split_part(address_norm, ' ', 1) AS addr_first,

        regexp_extract(address_norm, '[0-9]+') AS house_number,

        country_norm
            || '|'
            || regexp_extract(address_norm, '[0-9]+')
            AS house_key

    FROM sources
""")


# ============================================================
# 5. FREQUENCIES
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
# 6. EXACT ORIGINAL V4 CANDIDATE LOGIC
# ============================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE candidates AS

    -- --------------------------------------------------------
    -- 1. Standardized exact name
    -- --------------------------------------------------------

    SELECT
        s.entity_id AS source1_entity_id,
        c.entity_id AS candidate_entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.name_std = c.name_std
     AND s.name_std <> ''

    UNION

    -- --------------------------------------------------------
    -- 2. Compact standardized name
    -- --------------------------------------------------------

    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.name_compact = c.name_compact
     AND s.name_compact <> ''

    UNION

    -- --------------------------------------------------------
    -- 3. First two name tokens
    -- Frequency <= 100
    -- --------------------------------------------------------

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

    -- --------------------------------------------------------
    -- 4. Exact standardized address
    -- --------------------------------------------------------

    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.address_std = c.address_std
     AND s.address_std <> ''

    UNION

    -- --------------------------------------------------------
    -- 5. Country + house number
    -- Frequency <= 200
    -- --------------------------------------------------------

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
# 7. STATISTICS
# ============================================================

stats = con.execute("""
    SELECT
        COUNT(*) AS total_pairs,
        COUNT(DISTINCT source1_entity_id) AS s1_count
    FROM candidates
""").fetchone()


distribution = con.execute("""
    SELECT
        AVG(cnt),
        MEDIAN(cnt),
        QUANTILE_CONT(cnt, 0.95),
        MAX(cnt)
    FROM (
        SELECT
            source1_entity_id,
            COUNT(*) AS cnt
        FROM candidates
        GROUP BY source1_entity_id
    )
""").fetchone()


# ============================================================
# 8. RECALL
# ============================================================

recall = con.execute("""
    SELECT
        COUNT(*) AS total_true,
        COUNT(c.candidate_entity_id) AS found,
        COUNT(c.candidate_entity_id) * 100.0 / COUNT(*) AS recall
    FROM true_pairs t
    LEFT JOIN candidates c
      ON t.source1_entity_id = c.source1_entity_id
     AND t.candidate_entity_id = c.candidate_entity_id
""").fetchone()


# ============================================================
# 9. OUTPUT
# ============================================================

print("=" * 75)
print("EXACT V4 BLOCKING — FIXED 10,000 S1")
print("=" * 75)

print("\nCandidate statistics")
print(f"Total candidate pairs : {stats[0]:,}")
print(f"S1 with candidates    : {stats[1]:,}")
print(f"Average candidates    : {distribution[0]:.2f}")
print(f"Median candidates     : {distribution[1]:.2f}")
print(f"95th percentile       : {distribution[2]:.2f}")
print(f"Maximum candidates    : {distribution[3]:,}")

print("\nBlocking recall")
print(f"Total true pairs      : {recall[0]:,}")
print(f"Found by blocker      : {recall[1]:,}")
print(f"Blocking recall       : {recall[2]:.2f}%")

print("\nFixed evaluation set")
print("S1 records            : 10,000")
print("True pairs            : 34,264")

print("\nExpected original V4")
print("Candidate pairs       : 391,315")
print("Blocking recall       : 60.17%")

con.close()