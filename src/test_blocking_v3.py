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
# 1. SAMPLE 10,000 S1
# ============================================================

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE sample_s1 AS
    SELECT *
    FROM read_parquet('{S1}')
    ORDER BY entity_id
    LIMIT 10000
""")


# ============================================================
# 2. LOAD SOURCE 2 + SOURCE 3
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
# 3. GROUND TRUTH
# ============================================================

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE ground_truth AS
    SELECT source1_entity_id, matched_entity_ids
    FROM read_csv(
        '{GT}',
        delim='\\t',
        header=true,
        auto_detect=true
    )
    WHERE source1_entity_id IN (
        SELECT entity_id FROM sample_s1
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
# 4. CREATE BLOCKING KEYS
# ============================================================

# Name:
# - normalized
# - compact
# - first 4 characters
# - first token
#
# Address:
# - normalized
# - compact
# - first meaningful token
# - numeric token signature
#
# We will later restrict some keys by frequency.

con.execute("""
    CREATE OR REPLACE TEMP TABLE s1_keys AS
    SELECT
        entity_id,
        country_norm,
        name_norm,
        address_norm,

        -- Exact normalized name
        name_norm AS name_key,

        -- Remove spaces from name
        regexp_replace(name_norm, '\\s+', '', 'g') AS name_compact_key,

        -- First 4 characters of normalized name
        LEFT(
            regexp_replace(name_norm, '[^a-z0-9]', '', 'g'),
            4
        ) AS name_prefix4,

        -- First token of name
        SPLIT_PART(name_norm, ' ', 1) AS name_first_token,

        -- Exact normalized address
        address_norm AS address_key,

        -- Compact address
        regexp_replace(address_norm, '\\s+', '', 'g') AS address_compact_key,

        -- First address token
        SPLIT_PART(address_norm, ' ', 1) AS address_first_token,

        -- Numeric signature from address
        regexp_extract_all(address_norm, '[0-9]+') AS address_numbers

    FROM sample_s1
""")


con.execute("""
    CREATE OR REPLACE TEMP TABLE source_keys AS
    SELECT
        entity_id,
        country_norm,
        name_norm,
        address_norm,

        name_norm AS name_key,

        regexp_replace(name_norm, '\\s+', '', 'g') AS name_compact_key,

        LEFT(
            regexp_replace(name_norm, '[^a-z0-9]', '', 'g'),
            4
        ) AS name_prefix4,

        SPLIT_PART(name_norm, ' ', 1) AS name_first_token,

        address_norm AS address_key,

        regexp_replace(address_norm, '\\s+', '', 'g') AS address_compact_key,

        SPLIT_PART(address_norm, ' ', 1) AS address_first_token,

        regexp_extract_all(address_norm, '[0-9]+') AS address_numbers

    FROM sources
""")


# ============================================================
# 5. KEY FREQUENCIES
# ============================================================

# We only want selective keys.
# Generic keys such as "the", "medical", "main", etc.
# can create huge candidate sets.

con.execute("""
    CREATE OR REPLACE TEMP TABLE name_prefix_freq AS
    SELECT
        country_norm,
        name_prefix4,
        COUNT(*) AS freq
    FROM source_keys
    WHERE name_prefix4 <> ''
    GROUP BY country_norm, name_prefix4
""")


con.execute("""
    CREATE OR REPLACE TEMP TABLE name_first_freq AS
    SELECT
        country_norm,
        name_first_token,
        COUNT(*) AS freq
    FROM source_keys
    WHERE name_first_token <> ''
    GROUP BY country_norm, name_first_token
""")


con.execute("""
    CREATE OR REPLACE TEMP TABLE address_first_freq AS
    SELECT
        country_norm,
        address_first_token,
        COUNT(*) AS freq
    FROM source_keys
    WHERE address_first_token <> ''
    GROUP BY country_norm, address_first_token
""")


# ============================================================
# 6. BUILD CANDIDATES
# ============================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE candidates AS

    -- --------------------------------------------------------
    -- BLOCK 1: Exact normalized name
    -- --------------------------------------------------------
    SELECT
        s.entity_id AS source1_entity_id,
        c.entity_id AS candidate_entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.name_key = c.name_key
     AND s.name_key <> ''

    UNION

    -- --------------------------------------------------------
    -- BLOCK 2: Compact name
    -- Helps with spacing differences
    -- --------------------------------------------------------
    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.name_compact_key = c.name_compact_key
     AND s.name_compact_key <> ''

    UNION

    -- --------------------------------------------------------
    -- BLOCK 3: Name prefix
    -- Only allow relatively rare prefixes
    -- --------------------------------------------------------
    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.name_prefix4 = c.name_prefix4
     AND s.name_prefix4 <> ''
    JOIN name_prefix_freq f
      ON c.country_norm = f.country_norm
     AND c.name_prefix4 = f.name_prefix4
    WHERE f.freq <= 100

    UNION

    -- --------------------------------------------------------
    -- BLOCK 4: First name token
    -- Only rare first tokens
    -- --------------------------------------------------------
    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.name_first_token = c.name_first_token
     AND s.name_first_token <> ''
    JOIN name_first_freq f
      ON c.country_norm = f.country_norm
     AND c.name_first_token = f.name_first_token
    WHERE f.freq <= 100

    UNION

    -- --------------------------------------------------------
    -- BLOCK 5: Exact address
    -- --------------------------------------------------------
    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.address_key = c.address_key
     AND s.address_key <> ''

    UNION

    -- --------------------------------------------------------
    -- BLOCK 6: Compact address
    -- --------------------------------------------------------
    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.address_compact_key = c.address_compact_key
     AND s.address_compact_key <> ''

    UNION

    -- --------------------------------------------------------
    -- BLOCK 7: Rare first address token
    -- --------------------------------------------------------
    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.address_first_token = c.address_first_token
     AND s.address_first_token <> ''
    JOIN address_first_freq f
      ON c.country_norm = f.country_norm
     AND c.address_first_token = f.address_first_token
    WHERE f.freq <= 100
""")


# ============================================================
# 7. CANDIDATE STATISTICS
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
# 8. BLOCKING RECALL
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
print("V3 MULTI-KEY BLOCKING — 10,000 S1")
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

print("\nMissed examples:")

missed = con.execute("""
    SELECT
        t.source1_entity_id,
        t.candidate_entity_id
    FROM true_pairs t
    LEFT JOIN candidates c
      ON t.source1_entity_id = c.source1_entity_id
     AND t.candidate_entity_id = c.candidate_entity_id
    WHERE c.candidate_entity_id IS NULL
    LIMIT 15
""").fetchall()

for row in missed:
    print(f"  {row[0]} -> {row[1]}")


con.close()