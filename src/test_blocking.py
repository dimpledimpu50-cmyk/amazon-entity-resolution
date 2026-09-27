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
# 1. Take a deterministic sample of 10,000 S1 entities
# ---------------------------------------------------------

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE sample_s1 AS
    SELECT *
    FROM read_parquet('{S1}')
    ORDER BY entity_id
    LIMIT 10000
""")

# ---------------------------------------------------------
# 2. Load ground truth for the sample
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

# ---------------------------------------------------------
# 3. Create source 2 + source 3 candidate pool
# ---------------------------------------------------------

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE candidate_sources AS

    SELECT
        entity_id,
        business_name,
        business_address,
        country_norm,
        name_norm,
        name_compact,
        address_norm,
        address_compact,
        'S2' AS source
    FROM read_parquet('{S2}')

    UNION ALL

    SELECT
        entity_id,
        business_name,
        business_address,
        country_norm,
        name_norm,
        name_compact,
        address_norm,
        address_compact,
        'S3' AS source
    FROM read_parquet('{S3}')
""")

# ---------------------------------------------------------
# 4. BLOCK 1:
#    Same country + exact normalized name
# ---------------------------------------------------------

con.execute("""
    CREATE OR REPLACE TEMP TABLE candidates_exact_name AS
    SELECT DISTINCT
        s.entity_id AS source1_entity_id,
        c.entity_id AS candidate_entity_id,
        c.source
    FROM sample_s1 s
    JOIN candidate_sources c
      ON s.country_norm = c.country_norm
     AND s.name_norm = c.name_norm
""")

# ---------------------------------------------------------
# 5. Candidate statistics
# ---------------------------------------------------------

stats = con.execute("""
    SELECT
        COUNT(*) AS total_candidate_pairs,
        COUNT(DISTINCT source1_entity_id) AS s1_with_candidates,
        AVG(candidate_count) AS avg_candidates,
        MEDIAN(candidate_count) AS median_candidates,
        QUANTILE_CONT(candidate_count, 0.95) AS p95_candidates,
        MAX(candidate_count) AS max_candidates
    FROM (
        SELECT
            source1_entity_id,
            COUNT(*) AS candidate_count
        FROM candidates_exact_name
        GROUP BY source1_entity_id
    )
""").fetchone()

print("=" * 75)
print("BLOCKING EXPERIMENT — 10,000 S1 ENTITIES")
print("=" * 75)

print("\nExact normalized-name blocking + country")
print(f"Total candidate pairs : {stats[0]:,}")
print(f"S1 with candidates    : {stats[1]:,}")
print(f"Average candidates    : {stats[2]:.2f}")
print(f"Median candidates     : {stats[3]:.2f}")
print(f"95th percentile       : {stats[4]:.2f}")
print(f"Maximum candidates    : {stats[5]:,}")

# ---------------------------------------------------------
# 6. Measure blocking recall
# ---------------------------------------------------------

# Convert ground truth IDs into individual rows
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

recall = con.execute("""
    SELECT
        COUNT(*) AS total_true_pairs,
        COUNT(c.candidate_entity_id) AS found_true_pairs,
        COUNT(c.candidate_entity_id) * 100.0 / COUNT(*) AS recall_pct
    FROM true_pairs t
    LEFT JOIN candidates_exact_name c
      ON t.source1_entity_id = c.source1_entity_id
     AND t.candidate_entity_id = c.candidate_entity_id
""").fetchone()

print("\nBlocking recall")
print(f"Total true pairs     : {recall[0]:,}")
print(f"Found by blocker     : {recall[1]:,}")
print(f"Blocking recall      : {recall[2]:.2f}%")

# ---------------------------------------------------------
# 7. Show examples of missed true matches
# ---------------------------------------------------------

print("\nExamples of missed true matches:")

missed = con.execute("""
    SELECT
        t.source1_entity_id,
        t.candidate_entity_id
    FROM true_pairs t
    LEFT JOIN candidates_exact_name c
      ON t.source1_entity_id = c.source1_entity_id
     AND t.candidate_entity_id = c.candidate_entity_id
    WHERE c.candidate_entity_id IS NULL
    LIMIT 10
""").fetchall()

for row in missed:
    print(f"  {row[0]} -> {row[1]}")

con.close()