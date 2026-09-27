from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parent.parent
PARQUET_DIR = ROOT / "parquet"

S1 = PARQUET_DIR / "train_source1_normalized.parquet"
S2 = PARQUET_DIR / "train_source2_normalized.parquet"
S3 = PARQUET_DIR / "train_source3_normalized.parquet"
GT = ROOT / "dataset" / "train" / "train_ground_truth.tsv"

con = duckdb.connect()
con.execute("SET threads TO 4")


# ============================================================
# 1. SAMPLE
# ============================================================

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE sample_s1 AS
    SELECT *
    FROM read_parquet('{S1}')
    ORDER BY entity_id
    LIMIT 10000
""")


# ============================================================
# 2. SOURCE 2 + SOURCE 3
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
# 3. GROUND TRUTH
# ============================================================

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE ground_truth AS
    SELECT
        source1_entity_id,
        matched_entity_ids
    FROM read_csv(
        '{GT}',
        delim='\\t',
        header=true,
        auto_detect=true
    )
    WHERE source1_entity_id IN (
        SELECT entity_id
        FROM sample_s1
    )
""")


con.execute("""
    CREATE OR REPLACE TEMP TABLE true_pairs AS
    SELECT
        g.source1_entity_id,
        TRIM(x) AS candidate_entity_id
    FROM ground_truth g,
    UNNEST(
        CASE
            WHEN g.matched_entity_ids IS NULL
              OR TRIM(g.matched_entity_ids) = ''
            THEN []
            ELSE STRING_SPLIT(g.matched_entity_ids, ',')
        END
    ) AS t(x)
""")


# ============================================================
# 4. BASIC KEYS
# ============================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE s1_keys AS
    SELECT
        entity_id,
        country_norm,
        name_norm,
        address_norm,

        -- Standardized legal suffixes
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
            '\\s+',
            ' ',
            'g'
        ) AS name_std,

        regexp_replace(
            regexp_replace(
                lower(address_norm),
                '\\b(street|st)\\b',
                'st',
                'g'
            ),
            '\\b(road|rd)\\b',
            'rd',
            'g'
        ) AS address_std,

        regexp_extract(address_norm, '[0-9]+') AS house_number

    FROM sample_s1
""")


con.execute("""
    CREATE OR REPLACE TEMP TABLE source_keys AS
    SELECT
        entity_id,
        country_norm,
        name_norm,
        address_norm,

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
            '\\s+',
            ' ',
            'g'
        ) AS name_std,

        regexp_replace(
            regexp_replace(
                lower(address_norm),
                '\\b(street|st)\\b',
                'st',
                'g'
            ),
            '\\b(road|rd)\\b',
            'rd',
            'g'
        ) AS address_std,

        regexp_extract(address_norm, '[0-9]+') AS house_number

    FROM sources
""")


# ============================================================
# 5. NAME 3-GRAMS
# ============================================================

# Generate overlapping character 3-grams.
#
# Example:
# "sanders"
#
# san
# and
# nde
# der
# ers
#
# Spaces are removed so small formatting differences matter less.

con.execute("""
    CREATE OR REPLACE TEMP TABLE s1_name_grams AS
    SELECT DISTINCT
        s.entity_id AS source1_entity_id,
        s.country_norm,
        SUBSTRING(
            regexp_replace(s.name_std, '[^a-z0-9]', '', 'g'),
            i,
            3
        ) AS gram
    FROM s1_keys s
    CROSS JOIN range(
        1,
        100
    ) r(i)
    WHERE LENGTH(
        regexp_replace(s.name_std, '[^a-z0-9]', '', 'g')
    ) >= 3
      AND i <= LENGTH(
        regexp_replace(s.name_std, '[^a-z0-9]', '', 'g')
      ) - 2
""")


con.execute("""
    CREATE OR REPLACE TEMP TABLE source_name_grams AS
    SELECT DISTINCT
        s.entity_id AS candidate_entity_id,
        s.country_norm,
        SUBSTRING(
            regexp_replace(s.name_std, '[^a-z0-9]', '', 'g'),
            i,
            3
        ) AS gram
    FROM source_keys s
    CROSS JOIN range(
        1,
        100
    ) r(i)
    WHERE LENGTH(
        regexp_replace(s.name_std, '[^a-z0-9]', '', 'g')
    ) >= 3
      AND i <= LENGTH(
        regexp_replace(s.name_std, '[^a-z0-9]', '', 'g')
      ) - 2
""")


# ============================================================
# 6. 3-GRAM FREQUENCY
# ============================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE gram_freq AS
    SELECT
        country_norm,
        gram,
        COUNT(*) AS freq
    FROM source_name_grams
    WHERE gram <> ''
    GROUP BY country_norm, gram
""")


# ============================================================
# 7. NAME GRAM CANDIDATES
# ============================================================

# Only use moderately rare grams.
#
# If a gram appears in >500 source records,
# it is too generic for blocking.

con.execute("""
    CREATE OR REPLACE TEMP TABLE gram_candidates AS
    SELECT DISTINCT
        s.source1_entity_id,
        c.candidate_entity_id
    FROM s1_name_grams s
    JOIN source_name_grams c
      ON s.country_norm = c.country_norm
     AND s.gram = c.gram

    JOIN gram_freq f
      ON c.country_norm = f.country_norm
     AND c.gram = f.gram

    WHERE f.freq <= 500
""")


# ============================================================
# 8. ADDRESS TOKENS
# ============================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE s1_address_tokens AS
    SELECT DISTINCT
        s.entity_id AS source1_entity_id,
        s.country_norm,
        token
    FROM s1_keys s,
    UNNEST(
        STRING_SPLIT(s.address_std, ' ')
    ) AS t(token)
    WHERE LENGTH(token) >= 4
      AND token NOT IN (
          'street',
          'road',
          'avenue',
          'main',
          'north',
          'south',
          'east',
          'west'
      )
""")


con.execute("""
    CREATE OR REPLACE TEMP TABLE source_address_tokens AS
    SELECT DISTINCT
        s.entity_id AS candidate_entity_id,
        s.country_norm,
        token
    FROM source_keys s,
    UNNEST(
        STRING_SPLIT(s.address_std, ' ')
    ) AS t(token)
    WHERE LENGTH(token) >= 4
      AND token NOT IN (
          'street',
          'road',
          'avenue',
          'main',
          'north',
          'south',
          'east',
          'west'
      )
""")


# ============================================================
# 9. ADDRESS TOKEN FREQUENCY
# ============================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE address_token_freq AS
    SELECT
        country_norm,
        token,
        COUNT(*) AS freq
    FROM source_address_tokens
    GROUP BY country_norm, token
""")


# ============================================================
# 10. ADDRESS TOKEN PAIR
# ============================================================

# Instead of blocking on one address token,
# create pairs of tokens.
#
# Example:
#
# "4506 caven street indianapolis"
#
# can produce:
#
# caven + indianapolis
# 4506 + caven
# caven + street
#
# We exclude very common tokens.

con.execute("""
    CREATE OR REPLACE TEMP TABLE s1_address_pairs AS
    SELECT DISTINCT
        a.source1_entity_id,
        a.country_norm,

        CASE
            WHEN a.token < b.token
            THEN a.token || '|' || b.token
            ELSE b.token || '|' || a.token
        END AS token_pair

    FROM s1_address_tokens a
    JOIN s1_address_tokens b
      ON a.source1_entity_id = b.source1_entity_id
     AND a.token < b.token
""")


con.execute("""
    CREATE OR REPLACE TEMP TABLE source_address_pairs AS
    SELECT DISTINCT
        a.candidate_entity_id,
        a.country_norm,

        CASE
            WHEN a.token < b.token
            THEN a.token || '|' || b.token
            ELSE b.token || '|' || a.token
        END AS token_pair

    FROM source_address_tokens a
    JOIN source_address_tokens b
      ON a.candidate_entity_id = b.candidate_entity_id
     AND a.token < b.token
""")


# ============================================================
# 11. ADDRESS PAIR FREQUENCY
# ============================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE address_pair_freq AS
    SELECT
        country_norm,
        token_pair,
        COUNT(*) AS freq
    FROM source_address_pairs
    GROUP BY country_norm, token_pair
""")


# ============================================================
# 12. ADDRESS PAIR CANDIDATES
# ============================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE address_pair_candidates AS
    SELECT DISTINCT
        s.source1_entity_id,
        c.candidate_entity_id
    FROM s1_address_pairs s
    JOIN source_address_pairs c
      ON s.country_norm = c.country_norm
     AND s.token_pair = c.token_pair

    JOIN address_pair_freq f
      ON c.country_norm = f.country_norm
     AND c.token_pair = f.token_pair

    WHERE f.freq <= 200
""")


# ============================================================
# 13. V4 BASELINE
# ============================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE v4_candidates AS

    -- Standardized name

    SELECT
        s.entity_id AS source1_entity_id,
        c.entity_id AS candidate_entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.name_std = c.name_std
     AND s.name_std <> ''

    UNION

    -- Standardized address

    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.address_std = c.address_std
     AND s.address_std <> ''

    UNION

    -- House number

    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.house_number = c.house_number
     AND s.house_number <> ''
""")


# ============================================================
# 14. FUNCTION TO PRINT STATS
# ============================================================

def print_stats(name, table):

    stats = con.execute(f"""
        SELECT
            COUNT(*) AS total_pairs,
            COUNT(DISTINCT source1_entity_id) AS s1_count
        FROM {table}
    """).fetchone()

    distribution = con.execute(f"""
        SELECT
            AVG(cnt),
            MEDIAN(cnt),
            QUANTILE_CONT(cnt, 0.95),
            MAX(cnt)
        FROM (
            SELECT
                source1_entity_id,
                COUNT(*) AS cnt
            FROM {table}
            GROUP BY source1_entity_id
        )
    """).fetchone()

    recall = con.execute(f"""
        SELECT
            COUNT(*) AS total_true,
            COUNT(c.candidate_entity_id) AS found,
            COUNT(c.candidate_entity_id) * 100.0 / COUNT(*) AS recall
        FROM true_pairs t
        LEFT JOIN {table} c
          ON t.source1_entity_id = c.source1_entity_id
         AND t.candidate_entity_id = c.candidate_entity_id
    """).fetchone()

    print("\n" + "-" * 75)
    print(name)
    print("-" * 75)

    print(f"Candidate pairs : {stats[0]:,}")
    print(f"S1 with cand.   : {stats[1]:,}")
    print(f"Average/S1      : {distribution[0]:.2f}")
    print(f"Median          : {distribution[1]:.2f}")
    print(f"95th percentile : {distribution[2]:.2f}")
    print(f"Maximum         : {distribution[3]:,}")

    print(f"Recall          : {recall[2]:.2f}%")

    return recall[2]


# ============================================================
# 15. INDIVIDUAL EXPERIMENTS
# ============================================================

print("=" * 80)
print("V5 BLOCKING DIAGNOSTIC — 10,000 S1")
print("=" * 80)

print("\nEXPERIMENTS")


# ------------------------------------------------------------
# V4
# ------------------------------------------------------------

print_stats(
    "V4 BASELINE",
    "v4_candidates"
)


# ------------------------------------------------------------
# Name 3-gram only
# ------------------------------------------------------------

print_stats(
    "NAME 3-GRAM ONLY",
    "gram_candidates"
)


# ------------------------------------------------------------
# Address token pair only
# ------------------------------------------------------------

print_stats(
    "ADDRESS TOKEN-PAIR ONLY",
    "address_pair_candidates"
)


# ============================================================
# 16. V4 + NAME GRAM
# ============================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE v4_name_candidates AS

    SELECT *
    FROM v4_candidates

    UNION

    SELECT *
    FROM gram_candidates
""")

print_stats(
    "V4 + NAME 3-GRAM",
    "v4_name_candidates"
)


# ============================================================
# 17. V4 + ADDRESS PAIR
# ============================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE v4_address_candidates AS

    SELECT *
    FROM v4_candidates

    UNION

    SELECT *
    FROM address_pair_candidates
""")

print_stats(
    "V4 + ADDRESS TOKEN-PAIR",
    "v4_address_candidates"
)


# ============================================================
# 18. FULL V5
# ============================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE v5_candidates AS

    SELECT *
    FROM v4_candidates

    UNION

    SELECT *
    FROM gram_candidates

    UNION

    SELECT *
    FROM address_pair_candidates
""")

print_stats(
    "V5 FULL UNION",
    "v5_candidates"
)


print("\n" + "=" * 80)
print("DONE")
print("=" * 80)

con.close()