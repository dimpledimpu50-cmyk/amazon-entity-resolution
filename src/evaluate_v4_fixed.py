from pathlib import Path
import duckdb

# ============================================================
# FIXED V4 BLOCKING EVALUATION
#
# Uses the SAME 10,000 S1 + SAME 34,264 true pairs
# for every future experiment.
#
# RAM-safe for 16 GB machines.
# ============================================================

ROOT = Path(__file__).resolve().parent.parent

PARQUET = ROOT / "parquet"
INTERMEDIATE = ROOT / "intermediate"

S1_FILE = INTERMEDIATE / "blocking_eval_s1.parquet"
TRUE_FILE = INTERMEDIATE / "blocking_eval_true_pairs.parquet"

S2_FILE = PARQUET / "train_source2_normalized.parquet"
S3_FILE = PARQUET / "train_source3_normalized.parquet"

con = duckdb.connect()

# ------------------------------------------------------------
# RAM protection
# ------------------------------------------------------------

con.execute("SET memory_limit='4GB'")
con.execute("SET threads=2")
con.execute("SET preserve_insertion_order=false")

print("=" * 80)
print("FIXED V4 BLOCKING EVALUATION")
print("=" * 80)

# ============================================================
# 1. LOAD FIXED S1
# ============================================================

print("\n[1] Loading fixed S1 sample...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE s1 AS
SELECT *
FROM read_parquet('{S1_FILE}')
""")

s1_count = con.execute("""
SELECT COUNT(*)
FROM s1
""").fetchone()[0]

print(f"    S1 records: {s1_count:,}")


# ============================================================
# 2. LOAD FIXED TRUE PAIRS
# ============================================================

print("\n[2] Loading fixed true pairs...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE true_pairs AS
SELECT *
FROM read_parquet('{TRUE_FILE}')
""")

true_count = con.execute("""
SELECT COUNT(*)
FROM true_pairs
""").fetchone()[0]

print(f"    True pairs: {true_count:,}")


# ============================================================
# 3. CREATE SOURCE VIEWS
#
# IMPORTANT:
# These are VIEWS, not in-memory copies.
# ============================================================

print("\n[3] Preparing source views...")

con.execute(f"""
CREATE OR REPLACE VIEW source2 AS
SELECT
    entity_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact
FROM read_parquet('{S2_FILE}')
""")

con.execute(f"""
CREATE OR REPLACE VIEW source3 AS
SELECT
    entity_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact
FROM read_parquet('{S3_FILE}')
""")

print("    Source views ready.")


# ============================================================
# HELPER FUNCTION
# ============================================================

def evaluate_candidate_table(table_name):

    print("\n" + "-" * 80)
    print("V4 CANDIDATE STATISTICS")
    print("-" * 80)

    stats = con.execute(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT s1_id) AS s1_with_candidates,
            AVG(cnt) AS avg_candidates,
            MEDIAN(cnt) AS median_candidates,
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

    total, s1_with, avg, median, p95, maximum = stats

    print(f"Candidate pairs : {total:,}")
    print(f"S1 with cand.   : {s1_with:,}")
    print(f"Average/S1      : {avg:.2f}")
    print(f"Median          : {median:.2f}")
    print(f"95th percentile : {p95:.2f}")
    print(f"Maximum         : {maximum:,}")

    # --------------------------------------------------------
    # Recall
    # --------------------------------------------------------

    found = con.execute(f"""
        SELECT COUNT(*)

        FROM true_pairs t

        INNER JOIN {table_name} c

            ON c.s1_id = t.s1_id
           AND c.candidate_id = t.candidate_id
    """).fetchone()[0]

    recall = found / true_count * 100

    print("\nBlocking recall")
    print(f"True pairs      : {true_count:,}")
    print(f"Found pairs     : {found:,}")
    print(f"Blocking recall : {recall:.2f}%")

    return total, recall


# ============================================================
# 4. V4 — SEPARATE BLOCKING KEYS
#
# IMPORTANT:
# We use UNION instead of one giant OR join.
# ============================================================

print("\n[4] Generating V4 candidates...")
print("    Using selective joins.")

# ------------------------------------------------------------
# KEY 1 — exact normalized name + country
# ------------------------------------------------------------

print("\n    Key 1/5: exact normalized name...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE v4_name AS

SELECT DISTINCT
    s.entity_id AS s1_id,
    x.entity_id AS candidate_id

FROM s1 s

JOIN source2 x
    ON s.country_norm = x.country_norm
   AND s.name_norm = x.name_norm

UNION

SELECT DISTINCT
    s.entity_id AS s1_id,
    x.entity_id AS candidate_id

FROM s1 s

JOIN source3 x
    ON s.country_norm = x.country_norm
   AND s.name_norm = x.name_norm
""")


# ------------------------------------------------------------
# KEY 2 — compact name + country
# ------------------------------------------------------------

print("    Key 2/5: compact name...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE v4_compact_name AS

SELECT DISTINCT
    s.entity_id AS s1_id,
    x.entity_id AS candidate_id

FROM s1 s

JOIN source2 x
    ON s.country_norm = x.country_norm
   AND s.name_compact = x.name_compact

UNION

SELECT DISTINCT
    s.entity_id AS s1_id,
    x.entity_id AS candidate_id

FROM s1 s

JOIN source3 x
    ON s.country_norm = x.country_norm
   AND s.name_compact = x.name_compact
""")


# ------------------------------------------------------------
# KEY 3 — first two name tokens + country
# ------------------------------------------------------------

print("    Key 3/5: first two name tokens...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE v4_two_tokens AS

SELECT DISTINCT
    s.entity_id AS s1_id,
    x.entity_id AS candidate_id

FROM s1 s

JOIN source2 x

    ON s.country_norm = x.country_norm

   AND array_to_string(
        list_slice(
            string_split(s.name_norm, ' '),
            1,
            2
       ),
       ' '
   )
   =
   array_to_string(
        list_slice(
            string_split(x.name_norm, ' '),
            1,
            2
       ),
       ' '
   )

UNION

SELECT DISTINCT
    s.entity_id AS s1_id,
    x.entity_id AS candidate_id

FROM s1 s

JOIN source3 x

    ON s.country_norm = x.country_norm

   AND array_to_string(
        list_slice(
            string_split(s.name_norm, ' '),
            1,
            2
       ),
       ' '
   )
   =
   array_to_string(
        list_slice(
            string_split(x.name_norm, ' '),
            1,
            2
       ),
       ' '
   )
""")


# ------------------------------------------------------------
# KEY 4 — exact address + country
# ------------------------------------------------------------

print("    Key 4/5: exact address...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE v4_address AS

SELECT DISTINCT
    s.entity_id AS s1_id,
    x.entity_id AS candidate_id

FROM s1 s

JOIN source2 x
    ON s.country_norm = x.country_norm
   AND s.address_norm <> ''
   AND s.address_norm = x.address_norm

UNION

SELECT DISTINCT
    s.entity_id AS s1_id,
    x.entity_id AS candidate_id

FROM s1 s

JOIN source3 x
    ON s.country_norm = x.country_norm
   AND s.address_norm <> ''
   AND s.address_norm = x.address_norm
""")


# ------------------------------------------------------------
# KEY 5 — house number + country
# ------------------------------------------------------------

print("    Key 5/5: house number...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE v4_house AS

SELECT DISTINCT
    s.entity_id AS s1_id,
    x.entity_id AS candidate_id

FROM s1 s

JOIN source2 x

    ON s.country_norm = x.country_norm

   AND regexp_extract(
        s.address_norm,
        '^[0-9]+'
   ) <> ''

   AND regexp_extract(
        s.address_norm,
        '^[0-9]+'
   )
   =
   regexp_extract(
        x.address_norm,
        '^[0-9]+'
   )

UNION

SELECT DISTINCT
    s.entity_id AS s1_id,
    x.entity_id AS candidate_id

FROM s1 s

JOIN source3 x

    ON s.country_norm = x.country_norm

   AND regexp_extract(
        s.address_norm,
        '^[0-9]+'
   ) <> ''

   AND regexp_extract(
        s.address_norm,
        '^[0-9]+'
   )
   =
   regexp_extract(
        x.address_norm,
        '^[0-9]+'
   )
""")


# ============================================================
# 5. UNION ALL V4 KEYS
# ============================================================

print("\n[5] Combining V4 keys...")

con.execute("""
CREATE OR REPLACE TEMP TABLE v4_candidates AS

SELECT s1_id, candidate_id FROM v4_name

UNION

SELECT s1_id, candidate_id FROM v4_compact_name

UNION

SELECT s1_id, candidate_id FROM v4_two_tokens

UNION

SELECT s1_id, candidate_id FROM v4_address

UNION

SELECT s1_id, candidate_id FROM v4_house
""")


# ============================================================
# 6. EVALUATE
# ============================================================

total, recall = evaluate_candidate_table(
    "v4_candidates"
)


# ============================================================
# 7. SUMMARY
# ============================================================

print("\n" + "=" * 80)
print("FIXED V4 BASELINE")
print("=" * 80)

print(f"""
Candidate pairs : {total:,}
Blocking recall : {recall:.2f}%

Reference V4 result:
Candidate pairs : 391,315
Blocking recall : 60.17%
""")

if abs(recall - 60.17) < 2:
    print("STATUS: V4 baseline is approximately reproduced.")
else:
    print("WARNING: V4 differs significantly from the original implementation.")

print("=" * 80)

con.close()