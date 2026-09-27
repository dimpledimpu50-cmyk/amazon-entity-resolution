import duckdb

DB = duckdb.connect()

print("=" * 80)
print("ANALYZE REMAINING MISSES AFTER R13")
print("=" * 80)

S1_EVAL = "intermediate/blocking_eval_s1.parquet"
TRUE_PAIRS = "intermediate/blocking_eval_true_pairs.parquet"

BASELINE = "intermediate/verified_baseline_candidates.parquet"
R7 = "intermediate/recovery7_candidates.parquet"
R10 = "intermediate/recovery10_candidates.parquet"
R11 = "intermediate/recovery11_candidates.parquet"
R13 = "intermediate/recovery13_candidates.parquet"

SOURCE2 = "parquet/train_source2_normalized.parquet"
SOURCE3 = "parquet/train_source3_normalized.parquet"


# ============================================================================
# 1. S1 evaluation
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
# 2. True pairs
# ============================================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW true_pairs AS
SELECT
    s1_id AS source1_entity_id,
    candidate_id AS candidate_entity_id
FROM read_parquet('{TRUE_PAIRS}')
""")


# ============================================================================
# 3. All current blockers
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

UNION

SELECT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet('{R13}')
""")


# ============================================================================
# 4. Source 2 + Source 3
# ============================================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW source_data AS

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

JOIN source_data c
    ON t.candidate_entity_id = c.candidate_entity_id
   AND s.country_norm = c.country_norm

LEFT JOIN current_blocking b
    ON t.source1_entity_id = b.source1_entity_id
   AND t.candidate_entity_id = b.candidate_entity_id

WHERE b.source1_entity_id IS NULL
""")


missed_count = DB.execute("""
SELECT COUNT(*)
FROM missed
""").fetchone()[0]

print()
print(f"Remaining missed pairs: {missed_count:,}")


# ============================================================================
# 6. Calculate diagnostic signals
# ============================================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW signals AS

SELECT
    *,

    left(s1_compact, 3) =
    left(candidate_compact, 3)
    AS prefix3,

    left(s1_compact, 4) =
    left(candidate_compact, 4)
    AS prefix4,

    left(s1_compact, 5) =
    left(candidate_compact, 5)
    AS prefix5,

    right(s1_compact, 4) =
    right(candidate_compact, 4)
    AS suffix4,

    abs(
        length(s1_compact) -
        length(candidate_compact)
    ) <= 2
    AS length_close,

    editdist3(
        s1_compact,
        candidate_compact
    ) <= 1
    AS edit1,

    editdist3(
        s1_compact,
        candidate_compact
    ) <= 2
    AS edit2,

    editdist3(
        s1_compact,
        candidate_compact
    ) <= 3
    AS edit3,

    left(s1_address, 5) =
    left(candidate_address, 5)
    AS address_prefix5,

    TRY_CAST(
        regexp_extract(
            s1_address,
            '[0-9]+'
        ) AS BIGINT
    ) =
    TRY_CAST(
        regexp_extract(
            candidate_address,
            '[0-9]+'
        ) AS BIGINT
    )
    AND
    TRY_CAST(
        regexp_extract(
            s1_address,
            '[0-9]+'
        ) AS BIGINT
    ) IS NOT NULL
    AS same_house

FROM missed
""")


# ============================================================================
# 7. Signal counts
# ============================================================================

print()
print("=" * 80)
print("NAME SIGNALS")
print("=" * 80)

signals = [
    ("Prefix3", "prefix3"),
    ("Prefix4", "prefix4"),
    ("Prefix5", "prefix5"),
    ("Suffix4", "suffix4"),
    ("Length diff <=2", "length_close"),
    ("Edit distance <=1", "edit1"),
    ("Edit distance <=2", "edit2"),
    ("Edit distance <=3", "edit3"),
]


for label, condition in signals:

    count = DB.execute(f"""
        SELECT COUNT(*)
        FROM signals
        WHERE {condition}
    """).fetchone()[0]

    print(
        f"{label:<25}: "
        f"{count:>5} "
        f"({count / missed_count * 100:.2f}%)"
    )


# ============================================================================
# 8. Address signals
# ============================================================================

print()
print("=" * 80)
print("ADDRESS SIGNALS")
print("=" * 80)

for label, condition in [
    ("Address prefix5", "address_prefix5"),
    ("Same house", "same_house"),
]:

    count = DB.execute(f"""
        SELECT COUNT(*)
        FROM signals
        WHERE {condition}
    """).fetchone()[0]

    print(
        f"{label:<25}: "
        f"{count:>5} "
        f"({count / missed_count * 100:.2f}%)"
    )


# ============================================================================
# 9. Combined signals
# ============================================================================

print()
print("=" * 80)
print("COMBINED SIGNALS")
print("=" * 80)

combined = [

    (
        "Prefix4 + edit<=3",
        """
        prefix4
        AND edit3
        """
    ),

    (
        "Edit<=3 + house",
        """
        edit3
        AND same_house
        """
    ),

    (
        "Edit<=3 + address",
        """
        edit3
        AND address_prefix5
        """
    ),

    (
        "Prefix4 + house",
        """
        prefix4
        AND same_house
        """
    ),

    (
        "Prefix4 + address",
        """
        prefix4
        AND address_prefix5
        """
    ),

    (
        "Prefix4 + edit<=3 + house",
        """
        prefix4
        AND edit3
        AND same_house
        """
    ),

    (
        "Prefix4 + edit<=3 + address",
        """
        prefix4
        AND edit3
        AND address_prefix5
        """
    ),

    (
        "Prefix4 + edit<=3 + (house OR address)",
        """
        prefix4
        AND edit3
        AND (
            same_house
            OR address_prefix5
        )
        """
    ),
]


for label, condition in combined:

    count = DB.execute(f"""
        SELECT COUNT(*)
        FROM signals
        WHERE {condition}
    """).fetchone()[0]

    print(
        f"{label:<45}: "
        f"{count:>5} "
        f"({count / missed_count * 100:.2f}%)"
    )


# ============================================================================
# 10. Examples of remaining difficult pairs
# ============================================================================

print()
print("=" * 80)
print("REMAINING MISS EXAMPLES")
print("=" * 80)

rows = DB.execute("""
SELECT
    source1_entity_id,
    candidate_entity_id,
    s1_name,
    candidate_name,
    s1_address,
    candidate_address
FROM signals
ORDER BY
    source1_entity_id
LIMIT 40
""").fetchall()


for row in rows:

    print()
    print("-" * 80)

    print(f"S1:        {row[0]}")
    print(f"Candidate: {row[1]}")
    print(f"S1 name:   {row[2]}")
    print(f"Cand name: {row[3]}")
    print(f"S1 addr:   {row[4]}")
    print(f"Cand addr: {row[5]}")


print()
print("=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)