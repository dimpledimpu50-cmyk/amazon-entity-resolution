import duckdb
import time

con = duckdb.connect()

con.execute("SET memory_limit='6GB'")
con.execute("SET threads=2")
con.execute("SET preserve_insertion_order=false")

start = time.time()

S1 = "parquet/train_source1_normalized.parquet"
S23 = "parquet/train_source2_normalized.parquet"
GT = "dataset/train/train_ground_truth.tsv"

print("=" * 90)
print("CORRECTED RARE PREFIX4 + LENGTH<=2 DIAGNOSTIC")
print("=" * 90)

print("\nLoading ground truth...")

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

print("Creating unique missed true pairs...")

con.execute("""
CREATE OR REPLACE TEMP TABLE existing AS
SELECT DISTINCT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet('intermediate/train_candidate_pairs.parquet')
""")

con.execute("""
CREATE OR REPLACE TEMP TABLE missed AS
SELECT DISTINCT
    g.source1_entity_id,
    g.candidate_entity_id
FROM gt g
LEFT JOIN existing e
    ON g.source1_entity_id = e.source1_entity_id
   AND g.candidate_entity_id = e.candidate_entity_id
WHERE e.candidate_entity_id IS NULL
""")

missed_count = con.execute("""
SELECT COUNT(*) FROM missed
""").fetchone()[0]

print(f"Unique missed true pairs: {missed_count:,}")

print("\nBuilding Source2 + Source3 prefix frequencies...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE s23 AS
SELECT
    entity_id,
    country_norm,
    name_compact,
    LENGTH(name_compact) AS name_len,
    LEFT(name_compact, 4) AS prefix4
FROM read_parquet('{S23}')
WHERE name_compact IS NOT NULL
  AND LENGTH(name_compact) >= 4
""")

con.execute("""
CREATE OR REPLACE TEMP TABLE prefix_freq AS
SELECT
    country_norm,
    prefix4,
    COUNT(*) AS freq
FROM s23
GROUP BY country_norm, prefix4
""")

print("\nJoining missed pairs with Source2/3 values...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE missed_features AS
SELECT
    m.source1_entity_id,
    m.candidate_entity_id,
    s1.country_norm AS country,
    LEFT(s1.name_compact, 4) AS prefix4,
    LENGTH(s1.name_compact) AS s1_len,
    s23.name_len AS s23_len,
    pf.freq
FROM missed m
JOIN read_parquet('{S1}') s1
    ON m.source1_entity_id = s1.entity_id
JOIN s23
    ON m.candidate_entity_id = s23.entity_id
JOIN prefix_freq pf
    ON s1.country_norm = pf.country_norm
   AND LEFT(s1.name_compact, 4) = pf.prefix4
WHERE ABS(LENGTH(s1.name_compact) - s23.name_len) <= 2
""")

print("\n" + "=" * 90)
print("UNIQUE MISSED TRUE-PAIR RECOVERY")
print("=" * 90)

print(
    f"{'Threshold':>12}"
    f"{'Recovered':>18}"
    f"{'Recovery %':>15}"
)

print("-" * 50)

for threshold in [1, 2, 5, 10, 25, 50, 100, 200, 500]:
    recovered = con.execute(f"""
        SELECT COUNT(DISTINCT source1_entity_id || '|' || candidate_entity_id)
        FROM missed_features
        WHERE freq <= {threshold}
    """).fetchone()[0]

    pct = recovered / missed_count * 100

    print(
        f"{threshold:>12}"
        f"{recovered:>18,}"
        f"{pct:>14.2f}%"
    )

print("\n" + "=" * 90)
print("DONE")
print("=" * 90)

print(f"Elapsed: {(time.time() - start) / 60:.2f} minutes")