import duckdb
import time
import os

start = time.time()

DB = duckdb.connect()

DB.execute("SET memory_limit='6GB'")
DB.execute("SET threads=2")
DB.execute("SET preserve_insertion_order=false")
DB.execute("SET temp_directory='intermediate\\duckdb_temp'")

S1 = "parquet/test_source1_normalized.parquet"

PART_DIR = "intermediate\\submission_partitions"

CANDIDATE_OUT = "output\\candidate_pairs.tsv"
MATCH_OUT = "output\\matching_results.tsv"

PARTITIONS = 32

os.makedirs("output", exist_ok=True)

print("=" * 80)
print("REBUILDING FINAL SUBMISSION FILES")
print("=" * 80)


# ============================================================
# Remove old output files
# ============================================================

for path in [CANDIDATE_OUT, MATCH_OUT]:
    if os.path.exists(path):
        os.remove(path)


# ============================================================
# Write headers
# ============================================================

with open(CANDIDATE_OUT, "w", encoding="utf-8", newline="") as f:
    f.write("source1_entity_id\tcandidate_entity_ids\n")

with open(MATCH_OUT, "w", encoding="utf-8", newline="") as f:
    f.write("source1_entity_id\tmatched_entity_ids\n")


# ============================================================
# Process candidates
# ============================================================

print("\nGenerating candidate_pairs.tsv...")

for i in range(PARTITIONS):

    print(f"  Candidate partition {i + 1}/{PARTITIONS}...", end=" ", flush=True)

    cand_file = os.path.join(
        PART_DIR,
        f"candidates_{i}.parquet"
    )

    temp_file = os.path.join(
        PART_DIR,
        f"candidate_output_{i}.tsv"
    )

    if os.path.exists(temp_file):
        os.remove(temp_file)

    DB.execute(f"""
    COPY (
        SELECT
            s.entity_id AS source1_entity_id,
            COALESCE(c.candidate_entity_ids, '') AS candidate_entity_ids
        FROM read_parquet('{S1}') s
        LEFT JOIN (
            SELECT
                source1_entity_id,
                string_agg(
                    candidate_entity_id,
                    ',' ORDER BY candidate_entity_id
                ) AS candidate_entity_ids
            FROM read_parquet('{cand_file}')
            GROUP BY source1_entity_id
        ) c
        ON s.entity_id = c.source1_entity_id
        WHERE mod(hash(s.entity_id)::UBIGINT, {PARTITIONS}) = {i}
        ORDER BY s.entity_id
    )
    TO '{temp_file}'
    (
        FORMAT CSV,
        DELIMITER '\\t',
        HEADER FALSE,
        QUOTE '"'
    )
    """)

    with open(temp_file, "r", encoding="utf-8", newline="") as src:
        with open(CANDIDATE_OUT, "a", encoding="utf-8", newline="") as dst:
            for line in src:
                dst.write(line)

    os.remove(temp_file)

    print("done")


# ============================================================
# Process matches
# ============================================================

print("\nGenerating matching_results.tsv...")

for i in range(PARTITIONS):

    print(f"  Match partition {i + 1}/{PARTITIONS}...", end=" ", flush=True)

    match_file = os.path.join(
        PART_DIR,
        f"matches_{i}.parquet"
    )

    temp_file = os.path.join(
        PART_DIR,
        f"match_output_{i}.tsv"
    )

    if os.path.exists(temp_file):
        os.remove(temp_file)

    DB.execute(f"""
    COPY (
        SELECT
            s.entity_id AS source1_entity_id,
            COALESCE(m.matched_entity_ids, '') AS matched_entity_ids
        FROM read_parquet('{S1}') s
        LEFT JOIN (
            SELECT
                source1_entity_id,
                string_agg(
                    candidate_entity_id,
                    ',' ORDER BY candidate_entity_id
                ) AS matched_entity_ids
            FROM read_parquet('{match_file}')
            GROUP BY source1_entity_id
        ) m
        ON s.entity_id = m.source1_entity_id
        WHERE mod(hash(s.entity_id)::UBIGINT, {PARTITIONS}) = {i}
        ORDER BY s.entity_id
    )
    TO '{temp_file}'
    (
        FORMAT CSV,
        DELIMITER '\\t',
        HEADER FALSE,
        QUOTE '"'
    )
    """)

    with open(temp_file, "r", encoding="utf-8", newline="") as src:
        with open(MATCH_OUT, "a", encoding="utf-8", newline="") as dst:
            for line in src:
                dst.write(line)

    os.remove(temp_file)

    print("done")


# ============================================================
# Final file statistics
# ============================================================

print("\n" + "=" * 80)
print("FINAL FILES")
print("=" * 80)

for path in [CANDIDATE_OUT, MATCH_OUT]:

    with open(path, "r", encoding="utf-8") as f:
        rows = sum(1 for _ in f) - 1

    size_mb = os.path.getsize(path) / (1024 * 1024)

    print(f"{path}")
    print(f"  Rows : {rows:,}")
    print(f"  Size : {size_mb:.2f} MB")

print("\nExpected rows: 1,732,544")

print("\n" + "=" * 80)
print(f"DONE - elapsed {(time.time() - start) / 60:.2f} minutes")
print("=" * 80)