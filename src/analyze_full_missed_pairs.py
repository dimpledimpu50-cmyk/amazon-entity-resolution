import duckdb
import time

start = time.time()

DB = duckdb.connect()

DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")
DB.execute("SET preserve_insertion_order=false")

GT = "dataset/train/train_ground_truth.tsv"
CANDIDATES = "intermediate/train_candidate_pairs.parquet"

S1 = "parquet/train_source1_normalized.parquet"
S2 = "parquet/train_source2_normalized.parquet"
S3 = "parquet/train_source3_normalized.parquet"

print("=" * 90)
print("FULL-DATA MISSED TRUE-PAIR ANALYSIS")
print("=" * 90)


# ============================================================
# 1. Ground truth
# ============================================================

print("\nLoading ground truth...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW gt AS
SELECT DISTINCT
    source1_entity_id,
    candidate_entity_id
FROM (
    SELECT
        source1_entity_id,
        unnest(string_split(matched_entity_ids, ',')) AS candidate_entity_id
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
)
""")

gt_count = DB.execute("""
SELECT COUNT(*) FROM gt
""").fetchone()[0]

print(f"Ground-truth pairs: {gt_count:,}")


# ============================================================
# 2. Existing candidates
# ============================================================

print("\nLoading existing candidate pairs...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW candidates AS
SELECT DISTINCT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet('{CANDIDATES}')
""")

candidate_count = DB.execute("""
SELECT COUNT(*) FROM candidates
""").fetchone()[0]

print(f"Candidate pairs: {candidate_count:,}")


# ============================================================
# 3. Find missed true pairs
# ============================================================

print("\nFinding missed true pairs...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE missed AS
SELECT
    g.source1_entity_id,
    g.candidate_entity_id
FROM gt g
LEFT JOIN candidates c
    ON g.source1_entity_id = c.source1_entity_id
   AND g.candidate_entity_id = c.candidate_entity_id
WHERE c.candidate_entity_id IS NULL
""")

missed_count = DB.execute("""
SELECT COUNT(*) FROM missed
""").fetchone()[0]

print(f"Missed true pairs: {missed_count:,}")


# ============================================================
# 4. Load S1
# ============================================================

print("\nLoading Source 1...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW s1 AS
SELECT
    entity_id AS source1_entity_id,
    name_norm,
    name_compact,
    address_norm,
    address_compact,
    country_norm
FROM read_parquet('{S1}')
""")


# ============================================================
# 5. Load Source 2 + Source 3
# ============================================================

print("Loading Source 2 + Source 3...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW source23 AS

SELECT
    entity_id AS candidate_entity_id,
    name_norm,
    name_compact,
    address_norm,
    address_compact,
    country_norm
FROM read_parquet('{S2}')

UNION ALL

SELECT
    entity_id AS candidate_entity_id,
    name_norm,
    name_compact,
    address_norm,
    address_compact,
    country_norm
FROM read_parquet('{S3}')
""")


# ============================================================
# 6. Attach actual values to missed pairs
# ============================================================

print("\nJoining missed pairs with actual entity values...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE missed_features AS

SELECT
    m.source1_entity_id,
    m.candidate_entity_id,

    a.name_norm AS s1_name,
    b.name_norm AS s2_name,

    a.name_compact AS s1_name_compact,
    b.name_compact AS s2_name_compact,

    a.address_norm AS s1_address,
    b.address_norm AS s2_address,

    a.address_compact AS s1_address_compact,
    b.address_compact AS s2_address_compact,

    a.country_norm AS s1_country,
    b.country_norm AS s2_country,

    regexp_extract(
        a.address_norm,
        '[0-9]+'
    ) AS s1_house,

    regexp_extract(
        b.address_norm,
        '[0-9]+'
    ) AS s2_house

FROM missed m

JOIN s1 a
    ON m.source1_entity_id = a.source1_entity_id

JOIN source23 b
    ON m.candidate_entity_id = b.candidate_entity_id
""")


# ============================================================
# 7. Basic signal counts
# ============================================================

print("\n" + "=" * 90)
print("INDIVIDUAL MISSED-PAIR SIGNALS")
print("=" * 90)


signals = [

    (
        "Exact normalized name",
        "s1_name IS NOT NULL AND s2_name IS NOT NULL AND s1_name = s2_name"
    ),

    (
        "Exact compact name",
        "s1_name_compact IS NOT NULL AND s2_name_compact IS NOT NULL AND s1_name_compact = s2_name_compact"
    ),

    (
        "Name containment",
        """
        length(s1_name_compact) >= 5
        AND length(s2_name_compact) >= 5
        AND (
            position(s1_name_compact IN s2_name_compact) > 0
            OR
            position(s2_name_compact IN s1_name_compact) > 0
        )
        """
    ),

    (
        "Name prefix 3",
        """
        length(s1_name_compact) >= 3
        AND length(s2_name_compact) >= 3
        AND left(s1_name_compact, 3)
            = left(s2_name_compact, 3)
        """
    ),

    (
        "Name prefix 4",
        """
        length(s1_name_compact) >= 4
        AND length(s2_name_compact) >= 4
        AND left(s1_name_compact, 4)
            = left(s2_name_compact, 4)
        """
    ),

    (
        "Name prefix 5",
        """
        length(s1_name_compact) >= 5
        AND length(s2_name_compact) >= 5
        AND left(s1_name_compact, 5)
            = left(s2_name_compact, 5)
        """
    ),

    (
        "Name suffix 4",
        """
        length(s1_name_compact) >= 4
        AND length(s2_name_compact) >= 4
        AND right(s1_name_compact, 4)
            = right(s2_name_compact, 4)
        """
    ),

    (
        "Name length diff <= 2",
        """
        abs(
            length(coalesce(s1_name_compact, ''))
            -
            length(coalesce(s2_name_compact, ''))
        ) <= 2
        """
    ),

    (
        "Name length diff <= 5",
        """
        abs(
            length(coalesce(s1_name_compact, ''))
            -
            length(coalesce(s2_name_compact, ''))
        ) <= 5
        """
    ),

    (
        "Exact address",
        """
        s1_address_compact IS NOT NULL
        AND s2_address_compact IS NOT NULL
        AND s1_address_compact = s2_address_compact
        """
    ),

    (
        "Address prefix 5",
        """
        length(s1_address_compact) >= 5
        AND length(s2_address_compact) >= 5
        AND left(s1_address_compact, 5)
            = left(s2_address_compact, 5)
        """
    ),

    (
        "Same house number",
        """
        s1_house <> ''
        AND s2_house <> ''
        AND s1_house = s2_house
        """
    ),

    (
        "Address length diff <= 5",
        """
        abs(
            length(coalesce(s1_address_compact, ''))
            -
            length(coalesce(s2_address_compact, ''))
        ) <= 5
        """
    ),

    (
        "Address length diff <= 10",
        """
        abs(
            length(coalesce(s1_address_compact, ''))
            -
            length(coalesce(s2_address_compact, ''))
        ) <= 10
        """
    ),
]


for name, condition in signals:

    count = DB.execute(f"""
        SELECT COUNT(*)
        FROM missed_features
        WHERE {condition}
    """).fetchone()[0]

    pct = count / missed_count * 100

    print(
        f"{name:<30}"
        f"{count:>15,}"
        f"  {pct:>7.2f}%"
    )


# ============================================================
# 8. Combined signals
# ============================================================

print("\n" + "=" * 90)
print("COMBINED SIGNALS")
print("=" * 90)


combinations = [

    (
        "prefix4 + house",
        """
        length(s1_name_compact) >= 4
        AND length(s2_name_compact) >= 4
        AND left(s1_name_compact, 4)
            = left(s2_name_compact, 4)
        AND s1_house <> ''
        AND s2_house <> ''
        AND s1_house = s2_house
        """
    ),

    (
        "prefix5 + house",
        """
        length(s1_name_compact) >= 5
        AND length(s2_name_compact) >= 5
        AND left(s1_name_compact, 5)
            = left(s2_name_compact, 5)
        AND s1_house <> ''
        AND s2_house <> ''
        AND s1_house = s2_house
        """
    ),

    (
        "prefix4 + address prefix5",
        """
        length(s1_name_compact) >= 4
        AND length(s2_name_compact) >= 4
        AND left(s1_name_compact, 4)
            = left(s2_name_compact, 4)

        AND length(s1_address_compact) >= 5
        AND length(s2_address_compact) >= 5
        AND left(s1_address_compact, 5)
            = left(s2_address_compact, 5)
        """
    ),

    (
        "prefix5 + address prefix5",
        """
        length(s1_name_compact) >= 5
        AND length(s2_name_compact) >= 5
        AND left(s1_name_compact, 5)
            = left(s2_name_compact, 5)

        AND length(s1_address_compact) >= 5
        AND length(s2_address_compact) >= 5
        AND left(s1_address_compact, 5)
            = left(s2_address_compact, 5)
        """
    ),

    (
        "containment + house",
        """
        length(s1_name_compact) >= 5
        AND length(s2_name_compact) >= 5

        AND (
            position(s1_name_compact IN s2_name_compact) > 0
            OR
            position(s2_name_compact IN s1_name_compact) > 0
        )

        AND s1_house <> ''
        AND s2_house <> ''
        AND s1_house = s2_house
        """
    ),

    (
        "prefix4 + length diff <= 2",
        """
        length(s1_name_compact) >= 4
        AND length(s2_name_compact) >= 4

        AND left(s1_name_compact, 4)
            = left(s2_name_compact, 4)

        AND abs(
            length(s1_name_compact)
            -
            length(s2_name_compact)
        ) <= 2
        """
    ),

    (
        "prefix4 + length diff <= 5",
        """
        length(s1_name_compact) >= 4
        AND length(s2_name_compact) >= 4

        AND left(s1_name_compact, 4)
            = left(s2_name_compact, 4)

        AND abs(
            length(s1_name_compact)
            -
            length(s2_name_compact)
        ) <= 5
        """
    ),

    (
        "prefix5 + house + address prefix5",
        """
        length(s1_name_compact) >= 5
        AND length(s2_name_compact) >= 5
        AND left(s1_name_compact, 5)
            = left(s2_name_compact, 5)

        AND s1_house <> ''
        AND s2_house <> ''
        AND s1_house = s2_house

        AND length(s1_address_compact) >= 5
        AND length(s2_address_compact) >= 5
        AND left(s1_address_compact, 5)
            = left(s2_address_compact, 5)
        """
    ),
]


for name, condition in combinations:

    count = DB.execute(f"""
        SELECT COUNT(*)
        FROM missed_features
        WHERE {condition}
    """).fetchone()[0]

    pct = count / missed_count * 100

    print(
        f"{name:<40}"
        f"{count:>15,}"
        f"  {pct:>7.2f}%"
    )


# ============================================================
# 9. Save missed pairs for deeper analysis
# ============================================================

print("\nSaving missed-pair analysis...")

DB.execute("""
COPY missed_features
TO 'intermediate/full_missed_pairs_features.parquet'
(FORMAT PARQUET)
""")

print(
    "Saved: intermediate/full_missed_pairs_features.parquet"
)


# ============================================================
# 10. Show examples
# ============================================================

print("\n" + "=" * 90)
print("EXAMPLES: PREFIX4 + HOUSE")
print("=" * 90)

examples = DB.execute("""
SELECT
    source1_entity_id,
    candidate_entity_id,
    s1_name,
    s2_name,
    s1_address,
    s2_address
FROM missed_features
WHERE
    length(s1_name_compact) >= 4
    AND length(s2_name_compact) >= 4
    AND left(s1_name_compact, 4)
        = left(s2_name_compact, 4)
    AND s1_house <> ''
    AND s2_house <> ''
    AND s1_house = s2_house
LIMIT 20
""").fetchall()

for row in examples:
    print("\nS1:", row[0])
    print("S2:", row[1])
    print("S1 name:", row[2])
    print("S2 name:", row[3])
    print("S1 addr:", row[4])
    print("S2 addr:", row[5])


# ============================================================
# DONE
# ============================================================

print("\n" + "=" * 90)
print("DONE")
print("=" * 90)

print(
    f"Elapsed time: {(time.time() - start) / 60:.2f} minutes"
)