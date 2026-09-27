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

# =========================================================
# 1. Sample
# =========================================================

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE sample_s1 AS
    SELECT *
    FROM read_parquet('{S1}')
    ORDER BY entity_id
    LIMIT 10000
""")

# =========================================================
# 2. Source records
# =========================================================

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE sources AS

    SELECT entity_id, country_norm, name_norm, address_norm
    FROM read_parquet('{S2}')

    UNION ALL

    SELECT entity_id, country_norm, name_norm, address_norm
    FROM read_parquet('{S3}')
""")

# =========================================================
# 3. Ground truth
# =========================================================

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE ground_truth AS
    SELECT source1_entity_id, matched_entity_ids
    FROM read_csv(
        '{GT}',
        delim='\t',
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

# =========================================================
# 4. Create standardized fields
# =========================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE s1_std AS
    SELECT
        *,
        lower(
            regexp_replace(
                translate(
                    name_norm,
                    'éèêëáàâäíìîïóòôöúùûüçñ',
                    'eeeeaaaaiiiioooouuuucn'
                ),
                '\\s+',
                ' ',
                'g'
            )
        ) AS name_std,

        regexp_replace(
            lower(
                regexp_replace(
                    address_norm,
                    '\\b(street|st)\\b', 'st', 'g'
                )
            ),
            '\\b(road|rd)\\b', 'rd', 'g'
        ) AS address_std
    FROM sample_s1
""")

con.execute("""
    CREATE OR REPLACE TEMP TABLE source_std AS
    SELECT
        *,
        lower(
            regexp_replace(
                translate(
                    name_norm,
                    'éèêëáàâäíìîïóòôöúùûüçñ',
                    'eeeeaaaaiiiioooouuuucn'
                ),
                '\\s+',
                ' ',
                'g'
            )
        ) AS name_std,

        regexp_replace(
            lower(
                regexp_replace(
                    address_norm,
                    '\\b(street|st)\\b', 'st', 'g'
                )
            ),
            '\\b(road|rd)\\b', 'rd', 'g'
        ) AS address_std
    FROM sources
""")

# =========================================================
# 5. Blocking keys
# =========================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE candidates AS

    -- Exact standardized name
    SELECT DISTINCT
        s.entity_id AS source1_entity_id,
        c.entity_id AS candidate_entity_id
    FROM s1_std s
    JOIN source_std c
      ON s.country_norm = c.country_norm
     AND s.name_std = c.name_std
     AND s.name_std <> ''

    UNION

    -- Exact standardized address
    SELECT DISTINCT
        s.entity_id AS source1_entity_id,
        c.entity_id AS candidate_entity_id
    FROM s1_std s
    JOIN source_std c
      ON s.country_norm = c.country_norm
     AND s.address_std = c.address_std
     AND s.address_std <> ''
""")

# =========================================================
# 6. Statistics
# =========================================================

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

# =========================================================
# 7. Recall
# =========================================================

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

print("=" * 75)
print("STANDARDIZED BLOCKING — 10,000 S1")
print("=" * 75)

print("\nStandardized name + standardized address")

print(f"Total candidate pairs : {stats[0]:,}")
print(f"S1 with candidates    : {stats[1]:,}")
print(f"Average candidates    : {distribution[0]:.2f}")
print(f"Median candidates     : {distribution[1]:.2f}")
print(f"95th percentile       : {distribution[2]:.2f}")
print(f"Maximum candidates    : {distribution[3]:,}")

print("\nBlocking recall")
print(f"Total true pairs     : {recall[0]:,}")
print(f"Found by blocker     : {recall[1]:,}")
print(f"Blocking recall      : {recall[2]:.2f}%")

# =========================================================
# 8. Missed examples
# =========================================================

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
    LIMIT 10
""").fetchall()

for row in missed:
    print(f"  {row[0]} -> {row[1]}")

con.close()