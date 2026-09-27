import duckdb

DB = duckdb.connect()
DB.execute("PRAGMA threads=2")
DB.execute("PRAGMA memory_limit='4GB'")

print("=" * 70)
print("CHECKING MISSED NAME KEY PATTERNS")
print("=" * 70)

# ---------------------------------------------------------
# Load missed pairs
# ---------------------------------------------------------

DB.execute("""
CREATE OR REPLACE TEMP TABLE missed AS
SELECT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet(
    'intermediate/missed_true_pairs.parquet'
)
""")

# ---------------------------------------------------------
# Load S1
# ---------------------------------------------------------

DB.execute("""
CREATE OR REPLACE TEMP TABLE s1 AS
SELECT
    entity_id,
    name_norm,
    address_norm,
    country_norm
FROM read_parquet(
    'intermediate/blocking_eval_s1.parquet'
)
""")

# ---------------------------------------------------------
# Load source
# ---------------------------------------------------------

DB.execute("""
CREATE OR REPLACE TEMP TABLE src AS

SELECT
    entity_id,
    name_norm,
    address_norm,
    country_norm
FROM read_parquet(
    'parquet/train_source2_normalized.parquet'
)

UNION ALL

SELECT
    entity_id,
    name_norm,
    address_norm,
    country_norm
FROM read_parquet(
    'parquet/train_source3_normalized.parquet'
)
""")

# ---------------------------------------------------------
# Build name keys
# ---------------------------------------------------------

DB.execute("""
CREATE OR REPLACE TEMP TABLE details AS

SELECT

    m.source1_entity_id,
    m.candidate_entity_id,

    s.country_norm,

    s.name_norm AS s1_name,
    x.name_norm AS candidate_name,

    regexp_replace(
        lower(coalesce(s.name_norm,'')),
        '[^a-z0-9]',
        '',
        'g'
    ) AS s1_compact,

    regexp_replace(
        lower(coalesce(x.name_norm,'')),
        '[^a-z0-9]',
        '',
        'g'
    ) AS candidate_compact

FROM missed m

JOIN s1 s
  ON m.source1_entity_id = s.entity_id

JOIN src x
  ON m.candidate_entity_id = x.entity_id
""")

# ---------------------------------------------------------
# Exact compact name
# ---------------------------------------------------------

print()
print("=" * 70)
print("COMPACT NAME CHECK")
print("=" * 70)

row = DB.execute("""
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (
        WHERE s1_compact <> ''
          AND s1_compact = candidate_compact
    ) AS exact_compact
FROM details
""").fetchone()

print("Missed pairs       :", f"{row[0]:,}")
print(
    "Exact compact name :",
    f"{row[1]:,}",
    f"({row[1] / row[0] * 100:.2f}%)"
)

# ---------------------------------------------------------
# First 2 tokens
# ---------------------------------------------------------

row = DB.execute("""
SELECT COUNT(*)
FROM details
WHERE
    regexp_extract(s1_name, '^([^ ]+ +[^ ]+)', 1)
    =
    regexp_extract(candidate_name, '^([^ ]+ +[^ ]+)', 1)
AND regexp_extract(s1_name, '^([^ ]+ +[^ ]+)', 1) <> ''
""").fetchone()

print(
    "Exact first 2 name tokens:",
    f"{row[0]:,}",
    f"({row[0] / row[0] * 100:.2f}%)"
    if row[0] else ""
)

# ---------------------------------------------------------
# Print compact matches
# ---------------------------------------------------------

print()
print("=" * 70)
print("MISSED PAIRS WITH EXACT COMPACT NAME")
print("=" * 70)

rows = DB.execute("""
SELECT
    source1_entity_id,
    candidate_entity_id,
    s1_name,
    candidate_name,
    s1_compact,
    candidate_compact
FROM details
WHERE
    s1_compact <> ''
    AND s1_compact = candidate_compact
LIMIT 50
""").fetchall()

if not rows:
    print("NONE")
else:
    for i, row in enumerate(rows, 1):
        print()
        print(f"[{i}]")
        print("S1 ID        :", row[0])
        print("Candidate ID :", row[1])
        print("S1 Name      :", row[2])
        print("Candidate    :", row[3])
        print("S1 Compact   :", row[4])
        print("Candidate    :", row[5])

# ---------------------------------------------------------
# Print approximate name ordering cases
# ---------------------------------------------------------

print()
print("=" * 70)
print("SAMPLE MISSED NAME PAIRS")
print("=" * 70)

rows = DB.execute("""
SELECT
    source1_entity_id,
    candidate_entity_id,
    s1_name,
    candidate_name,
    s1_compact,
    candidate_compact
FROM details
LIMIT 100
""").fetchall()

for i, row in enumerate(rows, 1):
    print()
    print(f"[{i}]")
    print("S1 ID        :", row[0])
    print("Candidate ID :", row[1])
    print("S1 Name      :", row[2])
    print("Candidate    :", row[3])
    print("S1 Compact   :", row[4])
    print("Candidate    :", row[5])

print()
print("=" * 70)
print("DONE")
print("=" * 70)