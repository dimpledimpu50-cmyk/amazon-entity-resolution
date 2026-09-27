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
# 1. Sample S1
# =========================================================

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE sample_s1 AS
    SELECT *
    FROM read_parquet('{S1}')
    ORDER BY entity_id
    LIMIT 10000
""")

# =========================================================
# 2. Candidate sources
# =========================================================

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE candidates_source AS

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

# =========================================================
# 3. Ground truth
# =========================================================

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE ground_truth AS
    SELECT
        source1_entity_id,
        matched_entity_ids
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
# 4. S1 address tokens
# =========================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE s1_address_tokens AS
    SELECT DISTINCT
        s.entity_id AS source1_entity_id,
        s.country_norm,
        TRIM(token) AS token
    FROM sample_s1 s,
    UNNEST(STRING_SPLIT(s.address_norm, ' ')) AS t(token)
    WHERE LENGTH(TRIM(token)) >= 4
""")

# =========================================================
# 5. Candidate address tokens
# =========================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE candidate_address_tokens AS
    SELECT DISTINCT
        c.entity_id AS candidate_entity_id,
        c.country_norm,
        TRIM(token) AS token
    FROM candidates_source c,
    UNNEST(STRING_SPLIT(COALESCE(c.address_norm, ''), ' ')) AS t(token)
    WHERE LENGTH(TRIM(token)) >= 4
""")

# =========================================================
# 6. Token frequency
# =========================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE address_token_frequency AS
    SELECT
        country_norm,
        token,
        COUNT(*) AS freq
    FROM candidate_address_tokens
    GROUP BY country_norm, token
""")

# =========================================================
# 7. Address-token candidates
#
# Ignore extremely common address tokens.
# =========================================================

con.execute("""
    CREATE OR REPLACE TEMP TABLE address_candidates AS
    SELECT DISTINCT
        s.source1_entity_id,
        c.candidate_entity_id
    FROM s1_address_tokens s
    JOIN address_token_frequency f
      ON s.country_norm = f.country_norm
     AND s.token = f.token
     AND f.freq <= 1000
    JOIN candidate_address_tokens c
      ON c.country_norm = s.country_norm
     AND c.token = s.token
""")

# =========================================================
# 8. Statistics
# =========================================================

stats = con.execute("""
    SELECT
        COUNT(*) AS total_pairs,
        COUNT(DISTINCT source1_entity_id) AS s1_count
    FROM address_candidates
""").fetchone()

distribution = con.execute("""
    SELECT
        AVG(candidate_count),
        MEDIAN(candidate_count),
        QUANTILE_CONT(candidate_count, 0.95),
        MAX(candidate_count)
    FROM (
        SELECT
            source1_entity_id,
            COUNT(*) AS candidate_count
        FROM address_candidates
        GROUP BY source1_entity_id
    )
""").fetchone()

# =========================================================
# 9. Recall
# =========================================================

recall = con.execute("""
    SELECT
        COUNT(*) AS total_true_pairs,
        COUNT(c.candidate_entity_id) AS found_true_pairs,
        COUNT(c.candidate_entity_id) * 100.0 / COUNT(*) AS recall_pct
    FROM true_pairs t
    LEFT JOIN address_candidates c
      ON t.source1_entity_id = c.source1_entity_id
     AND t.candidate_entity_id = c.candidate_entity_id
""").fetchone()

print("=" * 75)
print("ADDRESS BLOCKING EXPERIMENT — 10,000 S1 ENTITIES")
print("=" * 75)

print("\nAddress token blocking + country")
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
# 10. Missed examples
# =========================================================

print("\nExamples of missed true matches:")

missed = con.execute("""
    SELECT
        t.source1_entity_id,
        t.candidate_entity_id
    FROM true_pairs t
    LEFT JOIN address_candidates c
      ON t.source1_entity_id = c.source1_entity_id
     AND t.candidate_entity_id = c.candidate_entity_id
    WHERE c.candidate_entity_id IS NULL
    LIMIT 10
""").fetchall()

for row in missed:
    print(f"  {row[0]} -> {row[1]}")

con.close()