import duckdb

DB = duckdb.connect()

print("=" * 80)
print("R13 DIAGNOSTIC — FUZZY NAME + ADDRESS SUPPORT")
print("=" * 80)

S1_EVAL = "intermediate/blocking_eval_s1.parquet"
TRUE_PAIRS = "intermediate/blocking_eval_true_pairs.parquet"

BASELINE = "intermediate/verified_baseline_candidates.parquet"
R7 = "intermediate/recovery7_candidates.parquet"
R10 = "intermediate/recovery10_candidates.parquet"
R11 = "intermediate/recovery11_candidates.parquet"

SOURCE2 = "parquet/train_source2_normalized.parquet"
SOURCE3 = "parquet/train_source3_normalized.parquet"


# ============================================================================
# 1. Load evaluation S1
# ============================================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW s1 AS
SELECT
    entity_id AS source1_entity_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact
FROM read_parquet('{S1_EVAL}')
""")


# ============================================================================
# 2. Load true pairs
# ============================================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW true_pairs AS
SELECT
    s1_id AS source1_entity_id,
    candidate_id AS candidate_entity_id
FROM read_parquet('{TRUE_PAIRS}')
""")


# ============================================================================
# 3. Current blocking union
# ============================================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW current_blocking AS

SELECT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet('{BASELINE}')

UNION

SELECT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet('{R7}')

UNION

SELECT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet('{R10}')

UNION

SELECT
    s1_id AS source1_entity_id,
    candidate_id AS candidate_entity_id
FROM read_parquet('{R11}')
""")


# ============================================================================
# 4. Source 2 + Source 3
# ============================================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW all_sources AS

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact
FROM read_parquet('{SOURCE2}')

UNION ALL

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact
FROM read_parquet('{SOURCE3}')
""")


# ============================================================================
# 5. Remaining missed true pairs
# ============================================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW missed AS
SELECT
    t.source1_entity_id,
    t.candidate_entity_id,

    s.country_norm,

    s.name_norm AS s1_name,
    c.name_norm AS candidate_name,

    s.name_compact AS s1_compact,
    c.name_compact AS candidate_compact,

    s.address_norm AS s1_address,
    c.address_norm AS candidate_address

FROM true_pairs t

JOIN s1 s
    ON t.source1_entity_id = s.source1_entity_id

JOIN all_sources c
    ON t.candidate_entity_id = c.candidate_entity_id
   AND s.country_norm = c.country_norm

LEFT JOIN current_blocking b
    ON t.source1_entity_id = b.source1_entity_id
   AND t.candidate_entity_id = b.candidate_entity_id

WHERE b.source1_entity_id IS NULL
""")


missed_count = DB.execute("""
SELECT COUNT(*) FROM missed
""").fetchone()[0]

print()
print(f"Remaining missed pairs: {missed_count:,}")


# ============================================================================
# 6. Prepare signals
# ============================================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW signals AS

SELECT
    *,

    -- Name prefix
    left(s1_compact, 3) AS s1_prefix3,
    left(candidate_compact, 3) AS cand_prefix3,

    left(s1_compact, 4) AS s1_prefix4,
    left(candidate_compact, 4) AS cand_prefix4,

    left(s1_compact, 5) AS s1_prefix5,
    left(candidate_compact, 5) AS cand_prefix5,

    -- Address prefix
    left(s1_address, 5) AS s1_addr_prefix5,
    left(candidate_address, 5) AS cand_addr_prefix5,

    -- Canonical first numeric component
    TRY_CAST(
        regexp_extract(s1_address, '[0-9]+')
        AS BIGINT
    ) AS s1_house,

    TRY_CAST(
        regexp_extract(candidate_address, '[0-9]+')
        AS BIGINT
    ) AS cand_house,

    -- Name edit distance
    editdist3(
        s1_compact,
        candidate_compact
    ) AS name_edit_distance

FROM missed
""")


# ============================================================================
# 7. Test individual signals
# ============================================================================

print()
print("=" * 80)
print("INDIVIDUAL SIGNALS")
print("=" * 80)

conditions = [

    (
        "Prefix4 + edit <= 1",
        """
        s1_prefix4 = cand_prefix4
        AND name_edit_distance <= 1
        """
    ),

    (
        "Prefix4 + edit <= 2",
        """
        s1_prefix4 = cand_prefix4
        AND name_edit_distance <= 2
        """
    ),

    (
        "Prefix4 + edit <= 3",
        """
        s1_prefix4 = cand_prefix4
        AND name_edit_distance <= 3
        """
    ),

    (
        "Prefix4 + same house",
        """
        s1_prefix4 = cand_prefix4
        AND s1_house IS NOT NULL
        AND cand_house IS NOT NULL
        AND s1_house = cand_house
        """
    ),

    (
        "Prefix4 + address prefix5",
        """
        s1_prefix4 = cand_prefix4
        AND s1_addr_prefix5 <> ''
        AND cand_addr_prefix5 <> ''
        AND s1_addr_prefix5 = cand_addr_prefix5
        """
    ),

    (
        "Edit <=3 + same house",
        """
        name_edit_distance <= 3
        AND s1_house IS NOT NULL
        AND cand_house IS NOT NULL
        AND s1_house = cand_house
        """
    ),

    (
        "Edit <=3 + address prefix5",
        """
        name_edit_distance <= 3
        AND s1_addr_prefix5 <> ''
        AND cand_addr_prefix5 <> ''
        AND s1_addr_prefix5 = cand_addr_prefix5
        """
    ),

    (
        "Prefix4 + edit <=3 + same house",
        """
        s1_prefix4 = cand_prefix4
        AND name_edit_distance <= 3
        AND s1_house IS NOT NULL
        AND cand_house IS NOT NULL
        AND s1_house = cand_house
        """
    ),

    (
        "Prefix4 + edit <=3 + addr prefix5",
        """
        s1_prefix4 = cand_prefix4
        AND name_edit_distance <= 3
        AND s1_addr_prefix5 <> ''
        AND cand_addr_prefix5 <> ''
        AND s1_addr_prefix5 = cand_addr_prefix5
        """
    ),

    (
        "Prefix4 + (house OR addr)",
        """
        s1_prefix4 = cand_prefix4
        AND (
            (
                s1_house IS NOT NULL
                AND cand_house IS NOT NULL
                AND s1_house = cand_house
            )
            OR
            (
                s1_addr_prefix5 <> ''
                AND cand_addr_prefix5 <> ''
                AND s1_addr_prefix5 = cand_addr_prefix5
            )
        )
        """
    ),

    (
        "Prefix4 + edit<=3 + (house OR addr)",
        """
        s1_prefix4 = cand_prefix4
        AND name_edit_distance <= 3
        AND (
            (
                s1_house IS NOT NULL
                AND cand_house IS NOT NULL
                AND s1_house = cand_house
            )
            OR
            (
                s1_addr_prefix5 <> ''
                AND cand_addr_prefix5 <> ''
                AND s1_addr_prefix5 = cand_addr_prefix5
            )
        )
        """
    ),
]


for label, condition in conditions:

    count = DB.execute(f"""
        SELECT COUNT(*)
        FROM signals
        WHERE {condition}
    """).fetchone()[0]

    print(f"{label:<45}: {count:>5}")


# ============================================================================
# 8. Show strongest examples
# ============================================================================

print()
print("=" * 80)
print("STRONGEST FUZZY + ADDRESS EXAMPLES")
print("=" * 80)

rows = DB.execute("""
SELECT
    source1_entity_id,
    candidate_entity_id,
    s1_name,
    candidate_name,
    name_edit_distance,
    s1_house,
    cand_house,
    s1_address,
    candidate_address
FROM signals
WHERE
    s1_prefix4 = cand_prefix4
    AND name_edit_distance <= 3
    AND (
        (
            s1_house IS NOT NULL
            AND cand_house IS NOT NULL
            AND s1_house = cand_house
        )
        OR
        (
            s1_addr_prefix5 <> ''
            AND cand_addr_prefix5 <> ''
            AND s1_addr_prefix5 = cand_addr_prefix5
        )
    )
ORDER BY name_edit_distance
LIMIT 30
""").fetchall()


for row in rows:

    print()
    print("-" * 80)

    print(f"S1:        {row[0]}")
    print(f"Candidate: {row[1]}")

    print(f"S1 name:   {row[2]}")
    print(f"Cand name: {row[3]}")

    print(f"Edit dist: {row[4]}")

    print(f"S1 house:  {row[5]}")
    print(f"Cand house:{row[6]}")

    print(f"S1 addr:   {row[7]}")
    print(f"Cand addr: {row[8]}")


# ============================================================================
# 9. Summary
# ============================================================================

print()
print("=" * 80)
print("R13 DIAGNOSTIC COMPLETE")
print("=" * 80)

print(f"Remaining missed pairs: {missed_count:,}")
print()
print("We will use these results to decide whether")
print("a selective R13 blocker is worth generating.")