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
GT = "dataset/train/train_ground_truth.tsv"
OUTPUT = "intermediate/train_candidates/r15_address_prefix5_len10_freq50.parquet"

print("=" * 90)
print("BUILDING R15: ADDRESS PREFIX5 + LENGTH<=10 + FREQUENCY<=50")
print("=" * 90)

print("\nLoading Source 2 + Source 3...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE s23 AS
SELECT
    entity_id,
    country_norm,
    address_compact,
    LENGTH(address_compact) AS address_len,
    LEFT(address_compact, 5) AS address_prefix5
FROM read_parquet([
    '{S2}',
    '{S3}'
])
WHERE address_compact IS NOT NULL
  AND LENGTH(address_compact) >= 5
""")

print("Building rare address-prefix table...")

con.execute("""
CREATE OR REPLACE TEMP TABLE address_freq AS
SELECT
    country_norm,
    address_prefix5,
    COUNT(*) AS freq
FROM s23
GROUP BY country_norm, address_prefix5
HAVING COUNT(*) <= 50
""")

print("Generating R15 candidates...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE r15 AS
SELECT DISTINCT
    s1.entity_id AS source1_entity_id,
    s23.entity_id AS candidate_entity_id
FROM read_parquet('{S1}') s1
JOIN s23
    ON s1.country_norm = s23.country_norm
   AND LEFT(s1.address_compact, 5) = s23.address_prefix5
   AND ABS(
        LENGTH(s1.address_compact) - s23.address_len
   ) <= 10
JOIN address_freq af
    ON s23.country_norm = af.country_norm
   AND s23.address_prefix5 = af.address_prefix5
""")

raw_count = con.execute("""
SELECT COUNT(*) FROM r15
""").fetchone()[0]

print(f"\nR15 raw unique candidates: {raw_count:,}")

print("\nRemoving candidates already in existing blocker...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE new_r15 AS
SELECT
    r.source1_entity_id,
    r.candidate_entity_id
FROM r15 r
ANTI JOIN read_parquet('{EXISTING}') e
    ON r.source1_entity_id = e.source1_entity_id
   AND r.candidate_entity_id = e.candidate_entity_id
""")

new_count = con.execute("""
SELECT COUNT(*) FROM new_r15
""").fetchone()[0]

print(f"NEW R15 candidates: {new_count:,}")

print("\nSaving new R15 candidates...")

con.execute(f"""
COPY (
    SELECT
        source1_entity_id,
        candidate_entity_id
    FROM new_r15
) TO '{OUTPUT}'
(FORMAT PARQUET, COMPRESSION ZSTD)
""")

print("\nChecking true-pair recovery...")

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

recovered = con.execute("""
SELECT COUNT(*)
FROM new_r15 r
JOIN gt g
    ON r.source1_entity_id = g.source1_entity_id
   AND r.candidate_entity_id = g.candidate_entity_id
""").fetchone()[0]

print(f"\nNEW TRUE PAIRS RECOVERED: {recovered:,}")

print("\n" + "=" * 90)
print("R15 SUMMARY")
print("=" * 90)

print(f"Raw candidates          : {raw_count:,}")
print(f"New candidates          : {new_count:,}")
print(f"True pairs recovered    : {recovered:,}")

if new_count > 0:
    print(
        f"New candidate precision : "
        f"{recovered / new_count * 100:.4f}%"
    )

print(f"\nOutput: {OUTPUT}")
print(f"Elapsed: {(time.time() - start) / 60:.2f} minutes")
print("=" * 90)