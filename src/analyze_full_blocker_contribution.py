import duckdb
import time

DB = duckdb.connect()

DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")
DB.execute("SET preserve_insertion_order=false")

GT = "dataset/train/train_ground_truth.tsv"
BLOCKER_DIR = "intermediate/train_candidates"

print("=" * 80)
print("FULL-DATA BLOCKER CONTRIBUTION ANALYSIS")
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
    '{GT}',
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

gt_count = DB.execute("""
SELECT COUNT(*)
FROM gt
""").fetchone()[0]

print(f"Ground-truth pairs: {gt_count:,}")


# --------------------------------------------------------------------------
# Blockers
# --------------------------------------------------------------------------

blockers = [
    ("V4 name", "v4_name.parquet"),
    ("V4 compact", "v4_compact.parquet"),
    ("V4 prefix", "v4_prefix.parquet"),
    ("V4 address", "v4_address.parquet"),
    ("V4 house", "v4_house.parquet"),
    ("R7", "r7.parquet"),
    ("R10", "r10.parquet"),
    ("R11", "r11.parquet"),
    ("R13", "r13.parquet"),
]


# --------------------------------------------------------------------------
# Analyze each blocker independently
# --------------------------------------------------------------------------

results = []

print("\nAnalyzing individual blockers...\n")

for name, filename in blockers:

    path = f"{BLOCKER_DIR}/{filename}"

    print(f"Processing {name}...")

    candidate_count = DB.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{path}')
    """).fetchone()[0]

    found = DB.execute(f"""
        SELECT COUNT(*)
        FROM gt g
        JOIN read_parquet('{path}') c
          ON g.source1_entity_id = c.source1_entity_id
         AND g.candidate_entity_id = c.candidate_entity_id
    """).fetchone()[0]

    recall = found / gt_count * 100

    results.append(
        (name, candidate_count, found, recall)
    )

    print(
        f"  candidates={candidate_count:,} "
        f"true_pairs={found:,} "
        f"recall={recall:.2f}%"
    )


# --------------------------------------------------------------------------
# Print summary
# --------------------------------------------------------------------------

print()
print("=" * 100)
print("INDIVIDUAL BLOCKER RESULTS")
print("=" * 100)

print(
    f"{'Blocker':<18}"
    f"{'Candidates':>18}"
    f"{'True pairs':>18}"
    f"{'Recall':>12}"
)

print("-" * 100)

for name, candidates, found, recall in results:

    print(
        f"{name:<18}"
        f"{candidates:>18,}"
        f"{found:>18,}"
        f"{recall:>11.2f}%"
    )


# --------------------------------------------------------------------------
# Incremental cumulative contribution
# --------------------------------------------------------------------------

print()
print("=" * 100)
print("CUMULATIVE BLOCKER CONTRIBUTION")
print("=" * 100)

# Start with no candidates.
DB.execute("""
CREATE OR REPLACE TEMP TABLE cumulative_candidates (
    source1_entity_id VARCHAR,
    candidate_entity_id VARCHAR
)
""")

previous_found = 0

print(
    f"{'After blocker':<18}"
    f"{'Unique candidates':>20}"
    f"{'True pairs found':>20}"
    f"{'Recall':>12}"
    f"{'New true pairs':>16}"
)

print("-" * 100)

for name, filename in blockers:

    path = f"{BLOCKER_DIR}/{filename}"

    DB.execute(f"""
        INSERT INTO cumulative_candidates
        SELECT
            source1_entity_id,
            candidate_entity_id
        FROM read_parquet('{path}')
    """)

    # Deduplicate.
    DB.execute("""
        CREATE OR REPLACE TEMP TABLE dedup AS
        SELECT DISTINCT
            source1_entity_id,
            candidate_entity_id
        FROM cumulative_candidates
    """)

    DB.execute("""
        DROP TABLE cumulative_candidates
    """)

    DB.execute("""
        ALTER TABLE dedup
        RENAME TO cumulative_candidates
    """)

    found = DB.execute("""
        SELECT COUNT(*)
        FROM gt g
        JOIN cumulative_candidates c
          ON g.source1_entity_id = c.source1_entity_id
         AND g.candidate_entity_id = c.candidate_entity_id
    """).fetchone()[0]

    candidates = DB.execute("""
        SELECT COUNT(*)
        FROM cumulative_candidates
    """).fetchone()[0]

    recall = found / gt_count * 100
    new_pairs = found - previous_found

    print(
        f"{name:<18}"
        f"{candidates:>20,}"
        f"{found:>20,}"
        f"{recall:>11.2f}%"
        f"{new_pairs:>16,}"
    )

    previous_found = found


print()
print("=" * 100)
print("DONE")
print("=" * 100)

print(
    f"Elapsed time: {(time.time() - start) / 60:.2f} minutes"
)