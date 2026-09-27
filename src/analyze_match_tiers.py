import duckdb
import time

start = time.time()

DB = duckdb.connect()
DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")
DB.execute("SET preserve_insertion_order=false")

FEATURES = "intermediate/train_match_features.parquet"

print("=" * 100)
print("PRECISION-FIRST MATCH TIER ANALYSIS")
print("=" * 100)

DB.execute(f"""
CREATE OR REPLACE TEMP TABLE f AS
SELECT *
FROM read_parquet('{FEATURES}')
""")

total_true = DB.execute("""
SELECT SUM(label)
FROM f
""").fetchone()[0]

print(f"\nTotal candidate pairs : {DB.execute('SELECT COUNT(*) FROM f').fetchone()[0]:,}")
print(f"Total true pairs      : {total_true:,}")


# ------------------------------------------------------------
# Evaluate cumulative tiers
# ------------------------------------------------------------

tiers = [

    (
        "TIER 1: exact name + exact address",
        """
        exact_compact_name = 1
        AND exact_compact_address = 1
        """
    ),

    (
        "TIER 2: + exact name + same house",
        """
        exact_compact_name = 1
        AND same_house = 1
        """
    ),

    (
        "TIER 3: + prefix5 + house + address prefix",
        """
        (
            exact_compact_name = 1
            AND same_house = 1
        )
        OR
        (
            name_prefix5 = 1
            AND same_house = 1
            AND address_prefix5 = 1
        )
        """
    ),

    (
        "TIER 4: + prefix4 + house + address prefix",
        """
        (
            exact_compact_name = 1
            AND same_house = 1
        )
        OR
        (
            name_prefix5 = 1
            AND same_house = 1
            AND address_prefix5 = 1
        )
        OR
        (
            name_prefix4 = 1
            AND same_house = 1
            AND address_prefix5 = 1
        )
        """
    ),

    (
        "TIER 5: + contains + house",
        """
        (
            exact_compact_name = 1
            AND same_house = 1
        )
        OR
        (
            name_prefix5 = 1
            AND same_house = 1
            AND address_prefix5 = 1
        )
        OR
        (
            name_prefix4 = 1
            AND same_house = 1
            AND address_prefix5 = 1
        )
        OR
        (
            name_contains = 1
            AND same_house = 1
        )
        """
    )
]


print()
print(
    f"{'TIER':<45}"
    f"{'PAIRS':>15}"
    f"{'TRUE':>15}"
    f"{'PRECISION':>13}"
    f"{'RECALL':>13}"
    f"{'F0.5':>10}"
)

print("-" * 115)


for name, condition in tiers:

    row = DB.execute(f"""
        SELECT
            COUNT(*) AS pairs,
            COALESCE(SUM(label), 0) AS true_pairs
        FROM f
        WHERE {condition}
    """).fetchone()

    pairs = row[0]
    true_pairs = row[1]

    precision = (
        true_pairs / pairs * 100
        if pairs else 0
    )

    recall = (
        true_pairs / total_true * 100
        if total_true else 0
    )

    beta2 = 0.25

    f05 = (
        1.25 * precision * recall
        / (0.25 * precision + recall)
        if precision + recall > 0
        else 0
    )

    print(
        f"{name:<45}"
        f"{pairs:>15,}"
        f"{true_pairs:>15,}"
        f"{precision:>12.2f}%"
        f"{recall:>12.2f}%"
        f"{f05:>10.2f}"
    )


# ------------------------------------------------------------
# Remaining true pairs after strong tiers
# ------------------------------------------------------------

strong_condition = """
(
    exact_compact_name = 1
    AND same_house = 1
)
OR
(
    name_prefix5 = 1
    AND same_house = 1
    AND address_prefix5 = 1
)
OR
(
    name_prefix4 = 1
    AND same_house = 1
    AND address_prefix5 = 1
)
OR
(
    name_contains = 1
    AND same_house = 1
)
"""

remaining = DB.execute(f"""
SELECT
    COUNT(*) AS missed_candidate_pairs,
    SUM(label) AS missed_true_pairs
FROM f
WHERE NOT ({strong_condition})
  AND label = 1
""").fetchone()

print()
print("=" * 100)
print("REMAINING TRUE MATCHES AFTER STRONG TIERS")
print("=" * 100)

print(f"Remaining true pairs: {remaining[1]:,}")
print(
    f"Remaining recall gap: "
    f"{remaining[1] / total_true * 100:.2f}%"
)


# ------------------------------------------------------------
# Analyze remaining true pairs
# ------------------------------------------------------------

print()
print("=" * 100)
print("SIGNALS AMONG REMAINING TRUE PAIRS")
print("=" * 100)

signals = [
    ("exact name", "exact_name = 1"),
    ("same house", "same_house = 1"),
    ("prefix4", "name_prefix4 = 1"),
    ("prefix5", "name_prefix5 = 1"),
    ("suffix4", "name_suffix4 = 1"),
    ("contains", "name_contains = 1"),
    ("address prefix5", "address_prefix5 = 1"),
    ("name length <=2", "name_length_diff <= 2"),
    ("name length <=5", "name_length_diff <= 5"),
    ("address length <=5", "address_length_diff <= 5"),
    ("address length <=10", "address_length_diff <= 10"),
]

for label, condition in signals:

    count = DB.execute(f"""
        SELECT COUNT(*)
        FROM f
        WHERE label = 1
          AND NOT ({strong_condition})
          AND {condition}
    """).fetchone()[0]

    pct = (
        count / remaining[1] * 100
        if remaining[1] else 0
    )

    print(
        f"{label:<25}"
        f"{count:>15,}"
        f"{pct:>12.2f}%"
    )


print()
print("=" * 100)
print(f"DONE — elapsed {(time.time() - start) / 60:.2f} minutes")
print("=" * 100)