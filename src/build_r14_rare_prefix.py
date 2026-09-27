import duckdb
import time

con = duckdb.connect()
con.execute("SET memory_limit='6GB'")
con.execute("SET threads=2")
con.execute("SET preserve_insertion_order=false")

start = time.time()

S1 = "parquet/train_source1_normalized.parquet"
S2 = "parquet/train_source2_normalized.parquet"
S3 = "parquet/train_source3_normalized.parquet"
EXISTING = "intermediate/train_candidate_pairs.parquet"
OUTPUT = "intermediate/train_candidates/r14_rare_prefix100.parquet"

print("=" * 90)
print("BUILDING R14: RARE PREFIX4 + LENGTH<=2")
print("=" * 90)

print("\nLoading Source 2 + Source 3...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE s23 AS
SELECT
    entity_id,
    country_norm,
    name_compact,
    LENGTH(name_compact) AS name_len,
    LEFT(name_compact, 4) AS prefix4
FROM read_parquet([
    '{S2}',
    '{S3}'
])
WHERE name_compact IS NOT NULL
  AND LENGTH(name_compact) >= 4
""")

print("Building prefix frequencies...")

con.execute("""
CREATE OR REPLACE TEMP TABLE prefix_freq AS
SELECT
    country_norm,
    prefix4,
    COUNT(*) AS freq
FROM s23
GROUP BY country_norm, prefix4
HAVING COUNT(*) <= 100
""")

print("Generating R14 candidates...")

con.execute(f"""
COPY (
    SELECT
        s1.entity_id AS source1_entity_id,
        s23.entity_id AS candidate_entity_id
    FROM read_parquet('{S1}') s1
    JOIN s23
        ON s1.country_norm = s23.country_norm
       AND LEFT(s1.name_compact, 4) = s23.prefix4
       AND ABS(
            LENGTH(s1.name_compact) - s23.name_len
       ) <= 2
    JOIN prefix_freq pf
        ON s23.country_norm = pf.country_norm
       AND s23.prefix4 = pf.prefix4
) TO '{OUTPUT}'
(FORMAT PARQUET, COMPRESSION ZSTD);
""")

r14_count = con.execute(f"""
SELECT COUNT(*)
FROM read_parquet('{OUTPUT}')
""").fetchone()[0]

print(f"\nR14 raw candidate rows: {r14_count:,}")

print("\nRemoving candidates already present in existing blocker...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE new_r14 AS
SELECT DISTINCT
    r.source1_entity_id,
    r.candidate_entity_id
FROM read_parquet('{OUTPUT}') r
ANTI JOIN read_parquet('{EXISTING}') e
    ON r.source1_entity_id = e.source1_entity_id
   AND r.candidate_entity_id = e.candidate_entity_id
""")

new_count = con.execute("""
SELECT COUNT(*) FROM new_r14
""").fetchone()[0]

print(f"NEW R14 candidate pairs: {new_count:,}")

print("\nSaving only NEW R14 candidates...")

con.execute(f"""
COPY (
    SELECT
        source1_entity_id,
        candidate_entity_id
    FROM new_r14
) TO '{OUTPUT}'
(FORMAT PARQUET, COMPRESSION ZSTD);
""")

print("\nChecking true-pair recovery...")

GT = "dataset/train/train_ground_truth.tsv"

con.execute(f"""
CREATE OR REPLACE TEMP TABLE gt AS
SELECT
    source1_entity_id,
    UNNEST(string_split(matched_entity_ids, ',')) AS candidate_entity_id
FROM read_csv(
    '{GT}',
    delim='\\t',
    header=true,
    columns={{
        'source1_entity_id':'VARCHAR',
        'matched_entity_ids':'VARCHAR'
    }}
)
WHERE matched_entity_ids IS NOT NULL
  AND matched_entity_ids <> ''
""")

recovered = con.execute(f"""
SELECT COUNT(*)
FROM new_r14 r
JOIN gt g
    ON r.source1_entity_id = g.source1_entity_id
   AND r.candidate_entity_id = g.candidate_entity_id
""").fetchone()[0]

print(f"\nNEW TRUE PAIRS RECOVERED: {recovered:,}")

print("\n" + "=" * 90)
print("R14 SUMMARY")
print("=" * 90)

print(f"Raw R14 candidates       : {r14_count:,}")
print(f"New candidates           : {new_count:,}")
print(f"True pairs recovered     : {recovered:,}")

if new_count > 0:
    print(
        f"New candidate precision  : "
        f"{recovered / new_count * 100:.4f}%"
    )

print(f"\nOutput: {OUTPUT}")
print(f"Elapsed: {(time.time() - start) / 60:.2f} minutes")
print("=" * 90)