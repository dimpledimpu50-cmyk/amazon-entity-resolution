from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parent.parent

PARQUET = ROOT / "parquet"
GT = ROOT / "dataset" / "train" / "train_ground_truth.tsv"

OUT_DIR = ROOT / "intermediate"
OUT_DIR.mkdir(exist_ok=True)

SAMPLE_SIZE = 10_000

con = duckdb.connect()

# ------------------------------------------------------------
# RAM protection
# ------------------------------------------------------------

con.execute("SET memory_limit='2GB'")
con.execute("SET threads=2")
con.execute("SET preserve_insertion_order=false")

print("=" * 80)
print("PREPARING FIXED BLOCKING EVALUATION SET")
print("=" * 80)

# ============================================================
# 1. FIXED S1 SAMPLE
# ============================================================

print("\n[1] Selecting fixed 10,000 S1 records...")

con.execute(f"""
CREATE OR REPLACE TABLE fixed_s1 AS

SELECT
    entity_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact

FROM read_parquet(
    '{PARQUET / "train_source1_normalized.parquet"}'
)

ORDER BY entity_id

LIMIT {SAMPLE_SIZE}
""")

count = con.execute("""
SELECT COUNT(*)
FROM fixed_s1
""").fetchone()[0]

print(f"    Fixed S1 records: {count:,}")


# ============================================================
# 2. FIXED GROUND-TRUTH PAIRS
# ============================================================

print("\n[2] Building fixed true-pair table...")

con.execute(f"""
CREATE OR REPLACE TABLE fixed_true_pairs AS

SELECT
    gt.source1_entity_id AS s1_id,
    TRIM(t.x) AS candidate_id

FROM read_csv(
    '{GT}',
    delim='\\t',
    header=true,
    columns={{
        'source1_entity_id': 'VARCHAR',
        'matched_entity_ids': 'VARCHAR'
    }}
) gt

JOIN fixed_s1 s
    ON s.entity_id = gt.source1_entity_id

CROSS JOIN UNNEST(
    STRING_SPLIT(
        COALESCE(gt.matched_entity_ids, ''),
        ','
    )
) AS t(x)

WHERE TRIM(t.x) <> ''
""")

true_pairs = con.execute("""
SELECT COUNT(*)
FROM fixed_true_pairs
""").fetchone()[0]

s1_with_matches = con.execute("""
SELECT COUNT(DISTINCT s1_id)
FROM fixed_true_pairs
""").fetchone()[0]

print(f"    S1 with true matches : {s1_with_matches:,}")
print(f"    True match pairs     : {true_pairs:,}")


# ============================================================
# 3. SAVE FIXED S1
# ============================================================

s1_path = OUT_DIR / "blocking_eval_s1.parquet"

con.execute(f"""
COPY fixed_s1
TO '{s1_path}'
(FORMAT PARQUET)
""")

print(f"\n[3] Saved:")
print(f"    {s1_path}")


# ============================================================
# 4. SAVE FIXED TRUE PAIRS
# ============================================================

pairs_path = OUT_DIR / "blocking_eval_true_pairs.parquet"

con.execute(f"""
COPY fixed_true_pairs
TO '{pairs_path}'
(FORMAT PARQUET)
""")

print(f"    {pairs_path}")


# ============================================================
# 5. SUMMARY
# ============================================================

print()
print("=" * 80)
print("FIXED EVALUATION SET READY")
print("=" * 80)

print()
print(f"S1 sample       : {count:,}")
print(f"True match pairs : {true_pairs:,}")

print()
print("Every future blocking experiment should use:")
print("    intermediate/blocking_eval_s1.parquet")
print("    intermediate/blocking_eval_true_pairs.parquet")

print()
print("RAM:")
print("    DuckDB limit : 2 GB")
print("    Threads      : 2")

print("=" * 80)

con.close()