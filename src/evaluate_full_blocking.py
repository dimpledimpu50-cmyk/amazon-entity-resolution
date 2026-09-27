import duckdb
import time

DB = duckdb.connect()

DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")
DB.execute("SET preserve_insertion_order=false")

CANDIDATES = "intermediate/train_candidate_pairs.parquet"
GROUND_TRUTH = "dataset/train/train_ground_truth.tsv"

print("=" * 80)
print("FULL TRAINING BLOCKING RECALL")
print("=" * 80)

start = time.time()

# --------------------------------------------------------------------------
# Ground truth
# --------------------------------------------------------------------------

print("\nLoading ground truth...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW gt AS
SELECT
    source1_entity_id,
    unnest(
        string_split(
            matched_entity_ids,
            ','
        )
    ) AS candidate_entity_id
FROM read_csv(
    '{GROUND_TRUTH}',
    delim='\\t',
    header=true,
    columns={{
        'source1_entity_id': 'VARCHAR',
        'matched_entity_ids': 'VARCHAR'
    }}
)
WHERE matched_entity_ids IS NOT NULL
  AND matched_entity_ids <> ''
""")

gt_pairs = DB.execute("""
SELECT COUNT(*)
FROM gt
""").fetchone()[0]

print(f"Ground-truth pairs: {gt_pairs:,}")


# --------------------------------------------------------------------------
# Candidate set
# --------------------------------------------------------------------------

print("\nLoading candidate set...")

candidate_pairs = DB.execute(f"""
SELECT COUNT(*)
FROM read_parquet('{CANDIDATES}')
""").fetchone()[0]

s1_covered = DB.execute(f"""
SELECT COUNT(DISTINCT source1_entity_id)
FROM read_parquet('{CANDIDATES}')
""").fetchone()[0]

print(f"Candidate pairs:     {candidate_pairs:,}")
print(f"S1 entities covered: {s1_covered:,}")


# --------------------------------------------------------------------------
# Recall
# --------------------------------------------------------------------------

print("\nCalculating true pairs found...")

found = DB.execute(f"""
SELECT COUNT(*)
FROM gt g
JOIN read_parquet('{CANDIDATES}') c
    ON g.source1_entity_id = c.source1_entity_id
   AND g.candidate_entity_id = c.candidate_entity_id
""").fetchone()[0]

recall = found / gt_pairs * 100

print()
print("=" * 80)
print("RESULT")
print("=" * 80)

print(f"Ground-truth pairs : {gt_pairs:,}")
print(f"Found in candidates: {found:,}")
print(f"Missed true pairs  : {gt_pairs - found:,}")
print(f"Blocking recall    : {recall:.2f}%")

print()
print(f"Candidate pairs    : {candidate_pairs:,}")
print(f"S1 covered         : {s1_covered:,}")

print()
print(f"Elapsed time: {(time.time() - start) / 60:.2f} minutes")