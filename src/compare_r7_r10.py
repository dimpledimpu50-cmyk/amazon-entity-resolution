import duckdb

DB = duckdb.connect()

BASE = "intermediate/verified_baseline_candidates.parquet"
R7 = "intermediate/recovery7_candidates.parquet"
R10 = "intermediate/recovery10_candidates.parquet"
TRUE = "intermediate/blocking_eval_true_pairs.parquet"

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW base AS
SELECT *
FROM read_parquet('{BASE}')
""")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW r7 AS
SELECT *
FROM read_parquet('{R7}')
""")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW r10 AS
SELECT *
FROM read_parquet('{R10}')
""")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW true_pairs AS
SELECT
    s1_id AS source1_entity_id,
    candidate_id AS candidate_entity_id
FROM read_parquet('{TRUE}')
""")


# ============================================================
# COUNTS
# ============================================================

true_total = DB.execute("""
SELECT COUNT(*)
FROM true_pairs
""").fetchone()[0]

base_count = DB.execute("""
SELECT COUNT(*)
FROM base
""").fetchone()[0]

r7_count = DB.execute("""
SELECT COUNT(*)
FROM r7
""").fetchone()[0]

r10_count = DB.execute("""
SELECT COUNT(*)
FROM r10
""").fetchone()[0]


# ============================================================
# BASE + R7
# ============================================================

base_r7 = """
SELECT source1_entity_id, candidate_entity_id
FROM base

UNION

SELECT source1_entity_id, candidate_entity_id
FROM r7
"""

base_r7_count = DB.execute(f"""
SELECT COUNT(*)
FROM ({base_r7})
""").fetchone()[0]

base_r7_found = DB.execute(f"""
SELECT COUNT(*)
FROM true_pairs t
JOIN ({base_r7}) x
    ON t.source1_entity_id = x.source1_entity_id
   AND t.candidate_entity_id = x.candidate_entity_id
""").fetchone()[0]


# ============================================================
# BASE + R10
# ============================================================

base_r10 = """
SELECT source1_entity_id, candidate_entity_id
FROM base

UNION

SELECT source1_entity_id, candidate_entity_id
FROM r10
"""

base_r10_count = DB.execute(f"""
SELECT COUNT(*)
FROM ({base_r10})
""").fetchone()[0]

base_r10_found = DB.execute(f"""
SELECT COUNT(*)
FROM true_pairs t
JOIN ({base_r10}) x
    ON t.source1_entity_id = x.source1_entity_id
   AND t.candidate_entity_id = x.candidate_entity_id
""").fetchone()[0]


# ============================================================
# BASE + R7 + R10
# ============================================================

all_three = """
SELECT source1_entity_id, candidate_entity_id
FROM base

UNION

SELECT source1_entity_id, candidate_entity_id
FROM r7

UNION

SELECT source1_entity_id, candidate_entity_id
FROM r10
"""

all_three_count = DB.execute(f"""
SELECT COUNT(*)
FROM ({all_three})
""").fetchone()[0]

all_three_found = DB.execute(f"""
SELECT COUNT(*)
FROM true_pairs t
JOIN ({all_three}) x
    ON t.source1_entity_id = x.source1_entity_id
   AND t.candidate_entity_id = x.candidate_entity_id
""").fetchone()[0]


# ============================================================
# R10 NEW BEYOND BASE + R7
# ============================================================

r10_new_after_r7 = DB.execute(f"""
SELECT COUNT(*)
FROM true_pairs t

JOIN r10 r
    ON t.source1_entity_id = r.source1_entity_id
   AND t.candidate_entity_id = r.candidate_entity_id

LEFT JOIN ({base_r7}) x
    ON x.source1_entity_id = r.source1_entity_id
   AND x.candidate_entity_id = r.candidate_entity_id

WHERE x.source1_entity_id IS NULL
""").fetchone()[0]


# ============================================================
# R7 NEW BEYOND BASE + R10
# ============================================================

r7_new_after_r10 = DB.execute(f"""
SELECT COUNT(*)
FROM true_pairs t

JOIN r7 r
    ON t.source1_entity_id = r.source1_entity_id
   AND t.candidate_entity_id = r.candidate_entity_id

LEFT JOIN ({base_r10}) x
    ON x.source1_entity_id = r.source1_entity_id
   AND x.candidate_entity_id = r.candidate_entity_id

WHERE x.source1_entity_id IS NULL
""").fetchone()[0]


# ============================================================
# OVERLAP
# ============================================================

r7_r10_overlap = DB.execute("""
SELECT COUNT(*)
FROM r7 a
JOIN r10 b
    ON a.source1_entity_id = b.source1_entity_id
   AND a.candidate_entity_id = b.candidate_entity_id
""").fetchone()[0]


# ============================================================
# RESULTS
# ============================================================

print("=" * 70)
print("R7 + R10 OVERLAP ANALYSIS")
print("=" * 70)

print()

print("True pairs                  :", f"{true_total:,}")

print()

print("BASELINE")
print("  Candidates                :", f"{base_count:,}")
print("  Found                     :", "28,560")
print("  Recall                    :", "83.35%")

print()

print("BASELINE + R7")
print("  Candidates                :", f"{base_r7_count:,}")
print("  Found                     :", f"{base_r7_found:,}")
print("  Recall                    :", f"{100 * base_r7_found / true_total:.2f}%")

print()

print("BASELINE + R10")
print("  Candidates                :", f"{base_r10_count:,}")
print("  Found                     :", f"{base_r10_found:,}")
print("  Recall                    :", f"{100 * base_r10_found / true_total:.2f}%")

print()

print("R7 / R10 OVERLAP")
print("  Shared candidates         :", f"{r7_r10_overlap:,}")

print()

print("R10 NEW AFTER BASE + R7")
print("  New true pairs            :", f"{r10_new_after_r7:,}")

print()

print("R7 NEW AFTER BASE + R10")
print("  New true pairs            :", f"{r7_new_after_r10:,}")

print()

print("BASE + R7 + R10")
print("  Candidates                :", f"{all_three_count:,}")
print("  Found                     :", f"{all_three_found:,}")
print("  Recall                    :", f"{100 * all_three_found / true_total:.2f}%")
print("  Remaining missed          :", f"{true_total - all_three_found:,}")

if r10_new_after_r7 > 0:
    print(
        "  Candidates / R10 new pair:",
        round(
            (all_three_count - base_r7_count) /
            r10_new_after_r7,
            2
        )
    )

print()
print("=" * 70)
print("DONE")
print("=" * 70)