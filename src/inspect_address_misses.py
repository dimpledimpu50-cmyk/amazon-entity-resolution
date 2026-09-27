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
# 1. Same 10,000 S1 sample
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
# 4. Address token blocking
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

con.execute("""
    CREATE OR REPLACE TEMP TABLE address_token_frequency AS
    SELECT
        country_norm,
        token,
        COUNT(*) AS freq
    FROM candidate_address_tokens
    GROUP BY country_norm, token
""")

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
# 5. Find missed true matches
# =========================================================

missed = con.execute("""
    SELECT
        t.source1_entity_id,
        t.candidate_entity_id
    FROM true_pairs t
    LEFT JOIN address_candidates c
      ON t.source1_entity_id = c.source1_entity_id
     AND t.candidate_entity_id = c.candidate_entity_id
    WHERE c.candidate_entity_id IS NULL
    LIMIT 15
""").fetchall()

print("=" * 100)
print("INSPECTION OF MISSED ADDRESS-BLOCKING MATCHES")
print("=" * 100)

for s1_id, candidate_id in missed:

    print("\n" + "-" * 100)
    print(f"S1: {s1_id}")
    print(f"TRUE MATCH: {candidate_id}")

    s1 = con.execute("""
        SELECT
            country_norm,
            name_norm,
            address_norm
        FROM sample_s1
        WHERE entity_id = ?
    """, [s1_id]).fetchone()

    candidate = con.execute("""
        SELECT
            country_norm,
            name_norm,
            address_norm
        FROM candidates_source
        WHERE entity_id = ?
    """, [candidate_id]).fetchone()

    print("\nSOURCE 1")
    print(f"Country : {s1[0]}")
    print(f"Name    : {s1[1]}")
    print(f"Address : {s1[2]}")

    print("\nTRUE MATCH")
    print(f"Country : {candidate[0]}")
    print(f"Name    : {candidate[1]}")
    print(f"Address : {candidate[2]}")

con.close()