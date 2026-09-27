import duckdb
import time

DB = duckdb.connect()

DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")
DB.execute("SET preserve_insertion_order=false")

CANDIDATES = "intermediate/train_candidate_pairs.parquet"

S1 = "parquet/train_source1_normalized.parquet"
S2 = "parquet/train_source2_normalized.parquet"
S3 = "parquet/train_source3_normalized.parquet"

GT = "dataset/train/train_ground_truth.tsv"

print("=" * 90)
print("CHEAP CANDIDATE FILTER ANALYSIS")
print("=" * 90)

start = time.time()


# ---------------------------------------------------------------------
# 1. Load candidate pairs
# ---------------------------------------------------------------------

print("\nLoading candidate pairs...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW candidates AS
SELECT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet('{CANDIDATES}')
""")

candidate_count = DB.execute("""
SELECT COUNT(*)
FROM candidates
""").fetchone()[0]

print(f"Candidate pairs: {candidate_count:,}")


# ---------------------------------------------------------------------
# 2. Load S1
# ---------------------------------------------------------------------

print("\nLoading Source 1...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW s1 AS
SELECT
    entity_id AS source1_entity_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact
FROM read_parquet('{S1}')
""")


# ---------------------------------------------------------------------
# 3. Load S2 + S3
# ---------------------------------------------------------------------

print("Loading Source 2 + Source 3...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW source23 AS

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact,
    'S2' AS source
FROM read_parquet('{S2}')

UNION ALL

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact,
    'S3' AS source
FROM read_parquet('{S3}')
""")


# ---------------------------------------------------------------------
# 4. Join candidates with source data
# ---------------------------------------------------------------------

print("\nJoining candidate pairs with entity data...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE candidate_features AS
SELECT
    c.source1_entity_id,
    c.candidate_entity_id,

    s1.country_norm AS s1_country,
    s1.name_norm AS s1_name,
    s1.name_compact AS s1_name_compact,
    s1.address_norm AS s1_address,
    s1.address_compact AS s1_address_compact,

    s.country_norm AS c_country,
    s.name_norm AS c_name,
    s.name_compact AS c_name_compact,
    s.address_norm AS c_address,
    s.address_compact AS c_address_compact,

    s.source

FROM candidates c

JOIN s1
    ON c.source1_entity_id = s1.source1_entity_id

JOIN source23 s
    ON c.candidate_entity_id = s.candidate_entity_id
""")

joined = DB.execute("""
SELECT COUNT(*)
FROM candidate_features
""").fetchone()[0]

print(f"Joined candidates: {joined:,}")


# ---------------------------------------------------------------------
# 5. Cheap deterministic signals
# ---------------------------------------------------------------------

print("\nCalculating cheap signals...")

DB.execute("""
ALTER TABLE candidate_features
ADD COLUMN exact_name BOOLEAN
""")

DB.execute("""
UPDATE candidate_features
SET exact_name =
    s1_name IS NOT NULL
    AND c_name IS NOT NULL
    AND s1_name = c_name
""")


DB.execute("""
ALTER TABLE candidate_features
ADD COLUMN exact_compact_name BOOLEAN
""")

DB.execute("""
UPDATE candidate_features
SET exact_compact_name =
    s1_name_compact IS NOT NULL
    AND c_name_compact IS NOT NULL
    AND s1_name_compact = c_name_compact
""")


DB.execute("""
ALTER TABLE candidate_features
ADD COLUMN exact_address BOOLEAN
""")

DB.execute("""
UPDATE candidate_features
SET exact_address =
    s1_address_compact IS NOT NULL
    AND c_address_compact IS NOT NULL
    AND s1_address_compact = c_address_compact
""")


DB.execute("""
ALTER TABLE candidate_features
ADD COLUMN same_country BOOLEAN
""")

DB.execute("""
UPDATE candidate_features
SET same_country =
    s1_country IS NOT NULL
    AND c_country IS NOT NULL
    AND s1_country = c_country
""")


DB.execute("""
ALTER TABLE candidate_features
ADD COLUMN same_house BOOLEAN
""")

DB.execute("""
UPDATE candidate_features
SET same_house =
    regexp_extract(s1_address, '[0-9]+')
    =
    regexp_extract(c_address, '[0-9]+')
    AND regexp_extract(s1_address, '[0-9]+') <> ''
""")


DB.execute("""
ALTER TABLE candidate_features
ADD COLUMN name_length_diff INTEGER
""")

DB.execute("""
UPDATE candidate_features
SET name_length_diff =
    abs(
        length(coalesce(s1_name_compact, ''))
        -
        length(coalesce(c_name_compact, ''))
    )
""")


DB.execute("""
ALTER TABLE candidate_features
ADD COLUMN address_length_diff INTEGER
""")

DB.execute("""
UPDATE candidate_features
SET address_length_diff =
    abs(
        length(coalesce(s1_address_compact, ''))
        -
        length(coalesce(c_address_compact, ''))
    )
""")


# ---------------------------------------------------------------------
# 6. Prefix signals
# ---------------------------------------------------------------------

DB.execute("""
ALTER TABLE candidate_features
ADD COLUMN name_prefix4 BOOLEAN
""")

DB.execute("""
UPDATE candidate_features
SET name_prefix4 =
    length(s1_name_compact) >= 4
    AND length(c_name_compact) >= 4
    AND left(s1_name_compact, 4) = left(c_name_compact, 4)
""")


DB.execute("""
ALTER TABLE candidate_features
ADD COLUMN name_prefix5 BOOLEAN
""")

DB.execute("""
UPDATE candidate_features
SET name_prefix5 =
    length(s1_name_compact) >= 5
    AND length(c_name_compact) >= 5
    AND left(s1_name_compact, 5) = left(c_name_compact, 5)
""")


# ---------------------------------------------------------------------
# 7. Name containment
# ---------------------------------------------------------------------

DB.execute("""
ALTER TABLE candidate_features
ADD COLUMN name_contains BOOLEAN
""")

DB.execute("""
UPDATE candidate_features
SET name_contains =
    (
        length(coalesce(s1_name_compact, '')) >= 5
        AND length(coalesce(c_name_compact, '')) >= 5
        AND (
            position(s1_name_compact IN c_name_compact) > 0
            OR
            position(c_name_compact IN s1_name_compact) > 0
        )
    )
""")


# ---------------------------------------------------------------------
# 8. Produce signal statistics
# ---------------------------------------------------------------------

print("\n")
print("=" * 90)
print("CHEAP SIGNAL COUNTS")
print("=" * 90)

signals = [
    ("exact_name", "exact_name"),
    ("exact_compact_name", "exact_compact_name"),
    ("exact_address", "exact_address"),
    ("same_country", "same_country"),
    ("same_house", "same_house"),
    ("name_prefix4", "name_prefix4"),
    ("name_prefix5", "name_prefix5"),
    ("name_contains", "name_contains"),
    ("name_length_diff <= 2", "name_length_diff <= 2"),
    ("name_length_diff <= 5", "name_length_diff <= 5"),
    ("address_length_diff <= 5", "address_length_diff <= 5"),
    ("address_length_diff <= 10", "address_length_diff <= 10"),
]

for label, condition in signals:

    count = DB.execute(f"""
        SELECT COUNT(*)
        FROM candidate_features
        WHERE {condition}
    """).fetchone()[0]

    percentage = count / candidate_count * 100

    print(
        f"{label:<30}"
        f"{count:>18,}"
        f"  ({percentage:>6.2f}%)"
    )


# ---------------------------------------------------------------------
# 9. Load ground truth
# ---------------------------------------------------------------------

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


# ---------------------------------------------------------------------
# 10. Evaluate simple filter combinations
# ---------------------------------------------------------------------

print("\n")
print("=" * 110)
print("FILTER COMBINATION ANALYSIS")
print("=" * 110)

filters = [

    (
        "Exact name OR exact address",
        """
        exact_name
        OR exact_address
        """
    ),

    (
        "Exact compact name OR exact address",
        """
        exact_compact_name
        OR exact_address
        """
    ),

    (
        "Exact name OR compact OR address",
        """
        exact_name
        OR exact_compact_name
        OR exact_address
        """
    ),

    (
        "Exact signals OR same house",
        """
        exact_name
        OR exact_compact_name
        OR exact_address
        OR same_house
        """
    ),

    (
        "Exact signals OR prefix4",
        """
        exact_name
        OR exact_compact_name
        OR exact_address
        OR name_prefix4
        """
    ),

    (
        "Exact signals OR prefix4 OR same house",
        """
        exact_name
        OR exact_compact_name
        OR exact_address
        OR name_prefix4
        OR same_house
        """
    ),

    (
        "Exact signals OR prefix5 OR same house",
        """
        exact_name
        OR exact_compact_name
        OR exact_address
        OR name_prefix5
        OR same_house
        """
    ),

    (
        "Exact signals OR containment OR same house",
        """
        exact_name
        OR exact_compact_name
        OR exact_address
        OR name_contains
        OR same_house
        """
    ),
]


print(
    f"{'Filter':<45}"
    f"{'Candidates':>18}"
    f"{'True pairs':>18}"
    f"{'Recall':>12}"
)

print("-" * 110)


for label, condition in filters:

    count = DB.execute(f"""
        SELECT COUNT(*)
        FROM candidate_features
        WHERE {condition}
    """).fetchone()[0]

    found = DB.execute(f"""
        SELECT COUNT(*)
        FROM gt g
        JOIN candidate_features c
          ON g.source1_entity_id = c.source1_entity_id
         AND g.candidate_entity_id = c.candidate_entity_id
        WHERE {condition}
    """).fetchone()[0]

    recall = found / gt_count * 100

    print(
        f"{label:<45}"
        f"{count:>18,}"
        f"{found:>18,}"
        f"{recall:>11.2f}%"
    )


print("\n")
print("=" * 90)
print("DONE")
print("=" * 90)

print(
    f"Elapsed: {(time.time() - start) / 60:.2f} minutes"
)