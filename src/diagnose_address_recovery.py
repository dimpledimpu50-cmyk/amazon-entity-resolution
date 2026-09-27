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
GT = "dataset/train/train_ground_truth.tsv"
EXISTING = "intermediate/train_candidate_pairs.parquet"

print("=" * 90)
print("ADDRESS RECOVERY DIAGNOSTIC")
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

con.execute(f"""
CREATE OR REPLACE TEMP TABLE missed AS
SELECT DISTINCT
    g.source1_entity_id,
    g.candidate_entity_id
FROM gt g
LEFT JOIN read_parquet('{EXISTING}') e
    ON g.source1_entity_id = e.source1_entity_id
   AND g.candidate_entity_id = e.candidate_entity_id
WHERE e.candidate_entity_id IS NULL
""")

missed_count = con.execute("""
SELECT COUNT(*) FROM missed
""").fetchone()[0]

print(f"Unique missed true pairs: {missed_count:,}")

print("\nLoading Source 2 + Source 3 addresses...")

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

print("Building address-prefix frequencies...")

con.execute("""
CREATE OR REPLACE TEMP TABLE address_freq AS
SELECT
    country_norm,
    address_prefix5,
    COUNT(*) AS freq
FROM s23
GROUP BY country_norm, address_prefix5
""")

print("\nJoining missed true pairs...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE missed_features AS
SELECT
    m.source1_entity_id,
    m.candidate_entity_id,
    s1.country_norm AS country,
    LEFT(s1.address_compact, 5) AS address_prefix5,
    LENGTH(s1.address_compact) AS s1_len,
    s23.address_len,
    af.freq
FROM missed m
JOIN read_parquet('{S1}') s1
    ON m.source1_entity_id = s1.entity_id
JOIN s23
    ON m.candidate_entity_id = s23.entity_id
JOIN address_freq af
    ON s1.country_norm = af.country_norm
   AND LEFT(s1.address_compact, 5) = af.address_prefix5
WHERE ABS(
    LENGTH(s1.address_compact) - s23.address_len
) <= 10
""")

print("\n" + "=" * 90)
print("MISSED TRUE-PAIR RECOVERY BY ADDRESS PREFIX FREQUENCY")
print("=" * 90)

print(
    f"{'Threshold':>12}"
    f"{'Raw candidates':>20}"
    f"{'Recovered true':>20}"
    f"{'Recovery %':>15}"
)

print("-" * 70)

for threshold in [1, 2, 5, 10, 25, 50, 100, 200, 500]:
    
    raw_candidates = con.execute(f"""
        SELECT COUNT(*)
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
        WHERE af.freq <= {threshold}
    """).fetchone()[0]

    recovered = con.execute(f"""
        SELECT COUNT(DISTINCT
            source1_entity_id || '|' || candidate_entity_id
        )
        FROM missed_features
        WHERE freq <= {threshold}
    """).fetchone()[0]

    pct = recovered / missed_count * 100

    print(
        f"{threshold:>12}"
        f"{raw_candidates:>20,}"
        f"{recovered:>20,}"
        f"{pct:>14.2f}%"
    )

print("\n" + "=" * 90)
print("DONE")
print("=" * 90)

print(f"Elapsed: {(time.time() - start) / 60:.2f} minutes")