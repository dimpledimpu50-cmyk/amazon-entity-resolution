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

# ---------------------------------------------------------
# Recreate the same 10,000 sample
# ---------------------------------------------------------

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE sample_s1 AS
    SELECT *
    FROM read_parquet('{S1}')
    ORDER BY entity_id
    LIMIT 10000
""")

# ---------------------------------------------------------
# Ground truth
# ---------------------------------------------------------

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

# ---------------------------------------------------------
# Recreate V2 blocking
# ---------------------------------------------------------

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE candidates_source AS

    SELECT
        entity_id,
        country_norm,
        name_norm,
        address_norm,
        'S2' AS source
    FROM read_parquet('{S2}')

    UNION ALL

    SELECT
        entity_id,
        country_norm,
        name_norm,
        address_norm,
        'S3' AS source
    FROM read_parquet('{S3}')
""")

con.execute("""
    CREATE OR REPLACE TEMP TABLE exact_candidates AS
    SELECT DISTINCT
        s.entity_id AS source1_entity_id,
        c.entity_id AS candidate_entity_id
    FROM sample_s1 s
    JOIN candidates_source c
      ON s.country_norm = c.country_norm
     AND s.name_norm = c.name_norm
""")

con.execute("""
    CREATE OR REPLACE TEMP TABLE s1_name_tokens AS
    SELECT DISTINCT
        s.entity_id AS source1_entity_id,
        s.country_norm,
        TRIM(token) AS token
    FROM sample_s1 s,
    UNNEST(STRING_SPLIT(s.name_norm, ' ')) AS t(token)
    WHERE LENGTH(TRIM(token)) >= 4
""")

con.execute("""
    CREATE OR REPLACE TEMP TABLE candidate_name_tokens AS
    SELECT DISTINCT
        c.entity_id AS candidate_entity_id,
        c.country_norm,
        TRIM(token) AS token
    FROM candidates_source c,
    UNNEST(STRING_SPLIT(c.name_norm, ' ')) AS t(token)
    WHERE LENGTH(TRIM(token)) >= 4
""")

con.execute("""
    CREATE OR REPLACE TEMP TABLE token_frequency AS
    SELECT
        country_norm,
        token,
        COUNT(*) AS freq
    FROM candidate_name_tokens
    GROUP BY country_norm, token
""")

con.execute("""
    CREATE OR REPLACE TEMP TABLE token_candidates AS
    SELECT DISTINCT
        s.source1_entity_id,
        c.candidate_entity_id
    FROM s1_name_tokens s
    JOIN token_frequency f
      ON s.country_norm = f.country_norm
     AND s.token = f.token
     AND f.freq <= 500
    JOIN candidate_name_tokens c
      ON c.country_norm = s.country_norm
     AND c.token = s.token
""")

con.execute("""
    CREATE OR REPLACE TEMP TABLE all_candidates AS
    SELECT * FROM exact_candidates
    UNION
    SELECT * FROM token_candidates
""")

# ---------------------------------------------------------
# Find missed true pairs
# ---------------------------------------------------------

missed = con.execute("""
    SELECT
        t.source1_entity_id,
        t.candidate_entity_id
    FROM true_pairs t
    LEFT JOIN all_candidates c
      ON t.source1_entity_id = c.source1_entity_id
     AND t.candidate_entity_id = c.candidate_entity_id
    WHERE c.candidate_entity_id IS NULL
    LIMIT 10
""").fetchall()

print("=" * 100)
print("INSPECTION OF MISSED TRUE MATCHES")
print("=" * 100)

# ---------------------------------------------------------
# Retrieve records
# ---------------------------------------------------------

for s1_id, candidate_id in missed:

    print("\n" + "-" * 100)
    print(f"S1: {s1_id}")
    print(f"TRUE MATCH: {candidate_id}")

    s1 = con.execute("""
        SELECT entity_id, country_norm, name_norm, address_norm
        FROM sample_s1
        WHERE entity_id = ?
    """, [s1_id]).fetchone()

    candidate = con.execute("""
        SELECT entity_id, country_norm, name_norm, address_norm, source
        FROM candidates_source
        WHERE entity_id = ?
    """, [candidate_id]).fetchone()

    print("\nSOURCE 1")
    print(f"Country : {s1[1]}")
    print(f"Name    : {s1[2]}")
    print(f"Address : {s1[3]}")

    print("\nTRUE MATCH")
    print(f"Source  : {candidate[4]}")
    print(f"Country : {candidate[1]}")
    print(f"Name    : {candidate[2]}")
    print(f"Address : {candidate[3]}")

con.close()