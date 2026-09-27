import duckdb

DB = duckdb.connect()

print("=" * 80)
print("R12 DIAGNOSTIC — CHARACTER CONFUSION NORMALIZATION")
print("=" * 80)

# -------------------------------------------------------------------
# Paths
# -------------------------------------------------------------------

S1_EVAL = "intermediate/blocking_eval_s1.parquet"
TRUE_PAIRS = "intermediate/blocking_eval_true_pairs.parquet"

BASELINE = "intermediate/verified_baseline_candidates.parquet"
R7 = "intermediate/recovery7_candidates.parquet"
R10 = "intermediate/recovery10_candidates.parquet"
R11 = "intermediate/recovery11_candidates.parquet"

SOURCE2 = "parquet/train_source2_normalized.parquet"
SOURCE3 = "parquet/train_source3_normalized.parquet"

# -------------------------------------------------------------------
# Load evaluation S1
# -------------------------------------------------------------------

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

# -------------------------------------------------------------------
# Load true pairs
# -------------------------------------------------------------------

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW true_pairs AS
SELECT
    s1_id AS source1_entity_id,
    candidate_id AS candidate_entity_id
FROM read_parquet('{TRUE_PAIRS}')
""")

# -------------------------------------------------------------------
# Build current blocking union
# -------------------------------------------------------------------

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

# -------------------------------------------------------------------
# Build Source 2 + Source 3 candidate view
# -------------------------------------------------------------------

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW all_sources AS

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_norm,
    name_compact
FROM read_parquet('{SOURCE2}')

UNION ALL

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_norm,
    name_compact
FROM read_parquet('{SOURCE3}')
""")

# -------------------------------------------------------------------
# Find remaining missed true pairs
# -------------------------------------------------------------------

DB.execute("""
CREATE OR REPLACE TEMP VIEW missed AS
SELECT
    t.source1_entity_id,
    t.candidate_entity_id,
    s.country_norm,
    s.name_norm AS s1_name,
    c.name_norm AS candidate_name,
    s.name_compact AS s1_compact,
    c.name_compact AS candidate_compact
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

# -------------------------------------------------------------------
# Character confusion normalization
#
# Conservative OCR-style mappings:
#   0 -> o
#   1 -> i
#   5 -> s
# -------------------------------------------------------------------

DB.execute("""
CREATE OR REPLACE TEMP VIEW confusion_test AS
SELECT
    *,

    replace(
        replace(
            replace(s1_compact, '0', 'o'),
            '1', 'i'
        ),
        '5', 's'
    ) AS s1_confused,

    replace(
        replace(
            replace(candidate_compact, '0', 'o'),
            '1', 'i'
        ),
        '5', 's'
    ) AS candidate_confused

FROM missed
""")

# -------------------------------------------------------------------
# Exact confusion-normalized match
# -------------------------------------------------------------------

confused_matches = DB.execute("""
SELECT COUNT(*)
FROM confusion_test
WHERE s1_confused = candidate_confused
  AND s1_confused <> ''
""").fetchone()[0]

print()
print("=" * 80)
print("CONSERVATIVE CHARACTER-CONFUSION TEST")
print("=" * 80)

print(
    f"Exact after 0→o, 1→i, 5→s: {confused_matches:,}"
)

print(
    f"Recall among remaining: "
    f"{confused_matches / missed_count * 100:.2f}%"
    if missed_count
    else "0%"
)

# -------------------------------------------------------------------
# Test individual mappings
# -------------------------------------------------------------------

tests = [
    (
        "0 -> o",
        """
        replace(s1_compact, '0', 'o')
        =
        replace(candidate_compact, '0', 'o')
        """
    ),
    (
        "1 -> i",
        """
        replace(s1_compact, '1', 'i')
        =
        replace(candidate_compact, '1', 'i')
        """
    ),
    (
        "5 -> s",
        """
        replace(s1_compact, '5', 's')
        =
        replace(candidate_compact, '5', 's')
        """
    ),
]

print()
print("INDIVIDUAL MAPPINGS")
print("-" * 80)

for label, condition in tests:

    count = DB.execute(f"""
        SELECT COUNT(*)
        FROM missed
        WHERE {condition}
          AND s1_compact <> ''
          AND candidate_compact <> ''
    """).fetchone()[0]

    print(f"{label:<12}: {count:,}")

# -------------------------------------------------------------------
# Show examples
# -------------------------------------------------------------------

print()
print("=" * 80)
print("RECOVERED EXAMPLES")
print("=" * 80)

rows = DB.execute("""
SELECT
    source1_entity_id,
    candidate_entity_id,
    s1_name,
    candidate_name,
    s1_compact,
    candidate_compact,
    s1_confused,
    candidate_confused
FROM confusion_test
WHERE s1_confused = candidate_confused
  AND s1_confused <> ''
LIMIT 30
""").fetchall()

for row in rows:

    print()
    print(f"S1:        {row[0]}")
    print(f"Candidate: {row[1]}")
    print(f"S1 name:   {row[2]}")
    print(f"Cand name: {row[3]}")
    print(f"S1 norm:   {row[4]}")
    print(f"Cand norm: {row[5]}")
    print(f"Confused:  {row[6]}")

print()
print("=" * 80)
print("R12 DIAGNOSTIC COMPLETE")
print("=" * 80)