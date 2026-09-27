from pathlib import Path
import duckdb

# ============================================================
# V5-LITE — SELECTIVE BLOCKING DIAGNOSTIC
#
# Designed for 16 GB RAM machines.
#
# IMPORTANT:
# We DO NOT reproduce V4 here.
# Original trusted V4 result:
#   391,315 candidates
#   60.17% recall
#
# This script tests only NEW blocking keys.
# ============================================================

ROOT = Path(__file__).resolve().parent.parent

PARQUET = ROOT / "parquet"
GT = ROOT / "dataset" / "train" / "train_ground_truth.tsv"

SAMPLE_SIZE = 10_000

con = duckdb.connect()

# ------------------------------------------------------------
# RAM / CPU protection
# ------------------------------------------------------------

con.execute("SET memory_limit='4GB'")
con.execute("SET threads=2")
con.execute("SET preserve_insertion_order=false")

print("=" * 80)
print("V5-LITE — SELECTIVE BLOCKING DIAGNOSTIC")
print("=" * 80)

print("\nRAM limit : 4 GB")
print("Threads   : 2")
print("S1 sample : 10,000")
print()


# ============================================================
# 1. S1 SAMPLE
# ============================================================

print("[1] Loading S1 sample...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE s1 AS
SELECT
    entity_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact
FROM read_parquet(
    '{PARQUET / "train_source1_normalized.parquet"}'
)
LIMIT {SAMPLE_SIZE}
""")

print("    S1 sample ready.")


# ============================================================
# 2. GROUND TRUTH
# ============================================================

print("[2] Loading ground truth...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE gt AS

SELECT
    source1_entity_id,
    matched_entity_ids
FROM read_csv(
    '{GT}',
    delim='\\t',
    header=true,
    columns={{
        'source1_entity_id': 'VARCHAR',
        'matched_entity_ids': 'VARCHAR'
    }}
)
WHERE source1_entity_id IN (
    SELECT entity_id FROM s1
)
""")

print("    Ground truth ready.")


# ============================================================
# 3. SOURCE PARQUET PATHS
# ============================================================

S2 = str(PARQUET / "train_source2_normalized.parquet")
S3 = str(PARQUET / "train_source3_normalized.parquet")


# ============================================================
# HELPER: EVALUATE BLOCKER
# ============================================================

def evaluate(table_name, label):

    print()
    print("-" * 80)
    print(label)
    print("-" * 80)

    # --------------------------------------------------------
    # Candidate statistics
    # --------------------------------------------------------

    stats = con.execute(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT s1_id) AS s1_count,
            AVG(cnt) AS avg_cnt,
            MEDIAN(cnt) AS median_cnt,
            QUANTILE_CONT(cnt, 0.95) AS p95,
            MAX(cnt) AS maximum

        FROM (
            SELECT
                s1_id,
                COUNT(*) AS cnt
            FROM {table_name}
            GROUP BY s1_id
        )
    """).fetchone()

    total, s1_count, avg_cnt, median_cnt, p95, maximum = stats

    print(f"Candidate pairs : {total:,}")
    print(f"S1 with cand.   : {s1_count:,}")
    print(f"Average/S1      : {avg_cnt:.2f}")
    print(f"Median          : {median_cnt:.2f}")
    print(f"95th percentile : {p95:.2f}")
    print(f"Maximum         : {maximum:,}")

    # --------------------------------------------------------
    # Ground truth recall
    # --------------------------------------------------------

    result = con.execute(f"""
        SELECT
            COUNT(*) AS true_pairs,
            COUNT_IF(c.candidate_id IS NOT NULL) AS found_pairs

        FROM (

            SELECT
                g.source1_entity_id,
                TRIM(t.x) AS true_id

            FROM gt g

            CROSS JOIN UNNEST(
                STRING_SPLIT(
                    COALESCE(g.matched_entity_ids, ''),
                    ','
                )
            ) AS t(x)

            WHERE TRIM(t.x) <> ''

        ) truth

        LEFT JOIN {table_name} c

            ON c.s1_id = truth.source1_entity_id
           AND c.candidate_id = truth.true_id
    """).fetchone()

    true_pairs, found_pairs = result

    recall = (
        found_pairs / true_pairs * 100
        if true_pairs
        else 0
    )

    print()
    print(f"True pairs      : {true_pairs:,}")
    print(f"Found pairs     : {found_pairs:,}")
    print(f"Blocking recall : {recall:.2f}%")

    return recall


# ============================================================
# BLOCKER 1
#
# NAME PREFIX
#
# country + first 5 compact name characters
#
# Frequency cap prevents common prefixes exploding.
# ============================================================

print()
print("[3] Testing selective name-prefix blocker...")
print("    This scans Parquet but does NOT create a Cartesian join.")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE name_prefix_counts AS

SELECT
    country_norm,
    LEFT(name_compact, 5) AS key,
    COUNT(*) AS frequency

FROM (
    SELECT country_norm, name_compact
    FROM read_parquet('{S2}')

    UNION ALL

    SELECT country_norm, name_compact
    FROM read_parquet('{S3}')
)

WHERE LENGTH(name_compact) >= 5

GROUP BY
    country_norm,
    LEFT(name_compact, 5)

HAVING COUNT(*) <= 100
""")

con.execute("""
CREATE OR REPLACE TEMP TABLE name_prefix_candidates AS

SELECT DISTINCT
    s.entity_id AS s1_id,
    x.entity_id AS candidate_id

FROM s1 s

JOIN name_prefix_counts k

    ON k.country_norm = s.country_norm
   AND k.key = LEFT(s.name_compact, 5)

JOIN (
    SELECT
        entity_id,
        country_norm,
        name_compact
    FROM read_parquet(?)

    UNION ALL

    SELECT
        entity_id,
        country_norm,
        name_compact
    FROM read_parquet(?)
) x

    ON x.country_norm = k.country_norm
   AND LEFT(x.name_compact, 5) = k.key

WHERE LENGTH(s.name_compact) >= 5
""", [S2, S3])

name_prefix_recall = evaluate(
    "name_prefix_candidates",
    "NAME PREFIX — country + first 5 chars"
)


# ============================================================
# CLEAN UP
# ============================================================

con.execute("DROP TABLE name_prefix_counts")
con.execute("DROP TABLE name_prefix_candidates")


# ============================================================
# BLOCKER 2
#
# HOUSE NUMBER + FIRST ADDRESS WORD
#
# Frequency cap = 200
# ============================================================

print()
print("[4] Testing house-number + address-word blocker...")
print("    No address-token-pair explosion.")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE address_key_counts AS

SELECT
    country_norm,

    regexp_extract(
        address_norm,
        '^[0-9]+'
    ) AS house_number,

    regexp_extract(
        address_norm,
        '[a-zA-Z]{{4,}}'
    ) AS address_word,

    COUNT(*) AS frequency

FROM (
    SELECT country_norm, address_norm
    FROM read_parquet('{S2}')

    UNION ALL

    SELECT country_norm, address_norm
    FROM read_parquet('{S3}')
)

WHERE
    address_norm <> ''
    AND regexp_extract(address_norm, '^[0-9]+') <> ''
    AND regexp_extract(address_norm, '[a-zA-Z]{{4,}}') <> ''

GROUP BY
    country_norm,
    regexp_extract(address_norm, '^[0-9]+'),
    regexp_extract(address_norm, '[a-zA-Z]{{4,}}')

HAVING COUNT(*) <= 200
""")

con.execute("""
CREATE OR REPLACE TEMP TABLE address_key_candidates AS

SELECT DISTINCT

    s.entity_id AS s1_id,
    x.entity_id AS candidate_id

FROM s1 s

JOIN address_key_counts k

    ON k.country_norm = s.country_norm

   AND k.house_number =
       regexp_extract(
           s.address_norm,
           '^[0-9]+'
       )

   AND k.address_word =
       regexp_extract(
           s.address_norm,
           '[a-zA-Z]{4,}'
       )

JOIN (
    SELECT
        entity_id,
        country_norm,
        address_norm
    FROM read_parquet(?)

    UNION ALL

    SELECT
        entity_id,
        country_norm,
        address_norm
    FROM read_parquet(?)
) x

    ON x.country_norm = k.country_norm

   AND regexp_extract(
           x.address_norm,
           '^[0-9]+'
       ) = k.house_number

   AND regexp_extract(
           x.address_norm,
           '[a-zA-Z]{4,}'
       ) = k.address_word

WHERE
    s.address_norm <> ''
""", [S2, S3])

address_recall = evaluate(
    "address_key_candidates",
    "HOUSE NUMBER + FIRST ADDRESS WORD"
)


# ============================================================
# CLEAN UP
# ============================================================

con.execute("DROP TABLE address_key_counts")
con.execute("DROP TABLE address_key_candidates")


# ============================================================
# BLOCKER 3
#
# FIRST + LAST NAME CHARACTER SIGNATURE
#
# Example:
#   "amazon business solutions"
#   -> ama + ons
#
# This is NOT a 3-gram system.
# ============================================================

print()
print("[5] Testing name edge-signature blocker...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE name_edge_counts AS

SELECT
    country_norm,

    LEFT(name_compact, 4) ||
    RIGHT(name_compact, 4) AS key,

    COUNT(*) AS frequency

FROM (
    SELECT country_norm, name_compact
    FROM read_parquet('{S2}')

    UNION ALL

    SELECT country_norm, name_compact
    FROM read_parquet('{S3}')
)

WHERE LENGTH(name_compact) >= 8

GROUP BY
    country_norm,
    LEFT(name_compact, 4) ||
    RIGHT(name_compact, 4)

HAVING COUNT(*) <= 100
""")

con.execute("""
CREATE OR REPLACE TEMP TABLE name_edge_candidates AS

SELECT DISTINCT

    s.entity_id AS s1_id,
    x.entity_id AS candidate_id

FROM s1 s

JOIN name_edge_counts k

    ON k.country_norm = s.country_norm

   AND k.key =
       LEFT(s.name_compact, 4) ||
       RIGHT(s.name_compact, 4)

JOIN (
    SELECT
        entity_id,
        country_norm,
        name_compact
    FROM read_parquet(?)

    UNION ALL

    SELECT
        entity_id,
        country_norm,
        name_compact
    FROM read_parquet(?)
) x

    ON x.country_norm = k.country_norm

   AND (
       LEFT(x.name_compact, 4) ||
       RIGHT(x.name_compact, 4)
   ) = k.key

WHERE LENGTH(s.name_compact) >= 8
""", [S2, S3])

edge_recall = evaluate(
    "name_edge_candidates",
    "NAME EDGE SIGNATURE"
)


# ============================================================
# CLEAN UP
# ============================================================

con.execute("DROP TABLE name_edge_counts")
con.execute("DROP TABLE name_edge_candidates")


# ============================================================
# SUMMARY
# ============================================================

print()
print("=" * 80)
print("V5-LITE RESULTS")
print("=" * 80)

print()
print("Trusted V4 baseline:")
print("    Candidates : 391,315")
print("    Recall     : 60.17%")

print()
print("New blockers tested:")

print(f"    Name prefix              : {name_prefix_recall:.2f}% recall")
print(f"    House + address word     : {address_recall:.2f}% recall")
print(f"    Name edge signature      : {edge_recall:.2f}% recall")

print()
print("RAM protection:")
print("    DuckDB memory : 4 GB")
print("    Threads       : 2")
print("    Pandas        : NO")
print("    3-grams       : NO")
print("    Token pairs   : NO")
print("    Cartesian     : NO")

print()
print("=" * 80)
print("DONE")
print("=" * 80)

con.close()