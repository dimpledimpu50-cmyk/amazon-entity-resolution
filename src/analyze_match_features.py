import duckdb
import time

start = time.time()

DB = duckdb.connect()

DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")
DB.execute("SET preserve_insertion_order=false")

FEATURES = "intermediate/train_match_features.parquet"

print("=" * 100)
print("MATCH FEATURE ANALYSIS")
print("=" * 100)

# ------------------------------------------------------------
# Helper
# ------------------------------------------------------------

def evaluate(condition, name):
    row = DB.execute(f"""
        SELECT
            COUNT(*) AS candidates,
            COALESCE(SUM(label), 0) AS true_pairs
        FROM read_parquet('{FEATURES}')
        WHERE {condition}
    """).fetchone()

    candidates = row[0]
    true_pairs = row[1]

    precision = (
        true_pairs / candidates * 100
        if candidates else 0
    )

    total_true = DB.execute(f"""
        SELECT SUM(label)
        FROM read_parquet('{FEATURES}')
    """).fetchone()[0]

    recall = (
        true_pairs / total_true * 100
        if total_true else 0
    )

    beta2 = 0.5 ** 2

    f05 = (
        (1 + beta2) * precision * recall
        / (beta2 * precision + recall)
        if precision + recall > 0
        else 0
    )

    print(
        f"{name:<35}"
        f"{candidates:>15,}"
        f"{true_pairs:>15,}"
        f"{precision:>12.2f}%"
        f"{recall:>12.2f}%"
        f"{f05:>12.2f}"
    )


print()
print(
    f"{'RULE':<35}"
    f"{'CANDIDATES':>15}"
    f"{'TRUE':>15}"
    f"{'PRECISION':>12}"
    f"{'RECALL':>12}"
    f"{'F0.5':>12}"
)

print("-" * 100)


# ------------------------------------------------------------
# Basic individual signals
# ------------------------------------------------------------

rules = [

    ("exact_name = 1",
     "exact_name = 1"),

    ("exact_compact_name = 1",
     "exact_compact_name = 1"),

    ("exact_address = 1",
     "exact_address = 1"),

    ("exact_compact_address = 1",
     "exact_compact_address = 1"),

    ("same_house = 1",
     "same_house = 1"),

    ("name_prefix4 = 1",
     "name_prefix4 = 1"),

    ("name_prefix5 = 1",
     "name_prefix5 = 1"),

    ("name_suffix4 = 1",
     "name_suffix4 = 1"),

    ("name_contains = 1",
     "name_contains = 1"),
]

for name, condition in rules:
    evaluate(condition, name)


# ------------------------------------------------------------
# Strong combinations
# ------------------------------------------------------------

print()
print("=" * 100)
print("COMBINED SIGNALS")
print("=" * 100)

print(
    f"{'RULE':<35}"
    f"{'CANDIDATES':>15}"
    f"{'TRUE':>15}"
    f"{'PRECISION':>12}"
    f"{'RECALL':>12}"
    f"{'F0.5':>12}"
)

print("-" * 100)

combined_rules = [

    (
        "exact name + house",
        """
        exact_compact_name = 1
        AND same_house = 1
        """
    ),

    (
        "exact name + address",
        """
        exact_compact_name = 1
        AND exact_compact_address = 1
        """
    ),

    (
        "prefix4 + house",
        """
        name_prefix4 = 1
        AND same_house = 1
        """
    ),

    (
        "prefix5 + house",
        """
        name_prefix5 = 1
        AND same_house = 1
        """
    ),

    (
        "prefix4 + address prefix",
        """
        name_prefix4 = 1
        AND address_prefix5 = 1
        """
    ),

    (
        "prefix5 + address prefix",
        """
        name_prefix5 = 1
        AND address_prefix5 = 1
        """
    ),

    (
        "contains + house",
        """
        name_contains = 1
        AND same_house = 1
        """
    ),

    (
        "prefix4 + short name diff",
        """
        name_prefix4 = 1
        AND name_length_diff <= 2
        """
    ),

    (
        "prefix5 + short name diff",
        """
        name_prefix5 = 1
        AND name_length_diff <= 2
        """
    ),

    (
        "prefix4 + house + short diff",
        """
        name_prefix4 = 1
        AND same_house = 1
        AND name_length_diff <= 3
        """
    ),

    (
        "prefix4 + address + short diff",
        """
        name_prefix4 = 1
        AND address_prefix5 = 1
        AND name_length_diff <= 3
        """
    ),
]

for name, condition in combined_rules:
    evaluate(condition, name)


# ------------------------------------------------------------
# Very high precision rules
# ------------------------------------------------------------

print()
print("=" * 100)
print("HIGH-CONFIDENCE RULES")
print("=" * 100)

print(
    f"{'RULE':<35}"
    f"{'CANDIDATES':>15}"
    f"{'TRUE':>15}"
    f"{'PRECISION':>12}"
    f"{'RECALL':>12}"
    f"{'F0.5':>12}"
)

print("-" * 100)

high_confidence = [

    (
        "exact name AND exact address",
        """
        exact_name = 1
        AND exact_address = 1
        """
    ),

    (
        "exact compact name AND exact address",
        """
        exact_compact_name = 1
        AND exact_address = 1
        """
    ),

    (
        "exact compact name AND same house",
        """
        exact_compact_name = 1
        AND same_house = 1
        """
    ),

    (
        "exact name + same house + country",
        """
        exact_name = 1
        AND same_house = 1
        AND country_equal = 1
        """
    ),

    (
        "prefix4 + house + prefix address",
        """
        name_prefix4 = 1
        AND same_house = 1
        AND address_prefix5 = 1
        """
    ),

    (
        "prefix5 + house + prefix address",
        """
        name_prefix5 = 1
        AND same_house = 1
        AND address_prefix5 = 1
        """
    ),
]

for name, condition in high_confidence:
    evaluate(condition, name)


# ------------------------------------------------------------
# Score distribution
# ------------------------------------------------------------

print()
print("=" * 100)
print("SIMPLE FEATURE SCORE DISTRIBUTION")
print("=" * 100)

DB.execute(f"""
CREATE OR REPLACE TEMP TABLE scored AS

SELECT
    *,
    (
        exact_name * 5
        + exact_compact_name * 5
        + exact_address * 5
        + exact_compact_address * 3
        + same_house * 2
        + name_prefix4 * 2
        + name_prefix5 * 2
        + name_contains * 2
        + name_suffix4 * 1
        + CASE
            WHEN name_length_diff <= 2 THEN 1
            ELSE 0
          END
        + CASE
            WHEN address_length_diff <= 10 THEN 1
            ELSE 0
          END
    ) AS simple_score

FROM read_parquet('{FEATURES}')
""")

print(
    f"{'SCORE':>8}"
    f"{'CANDIDATES':>18}"
    f"{'TRUE':>18}"
    f"{'PRECISION':>14}"
)

print("-" * 65)

rows = DB.execute("""
SELECT
    simple_score,
    COUNT(*) AS candidates,
    SUM(label) AS true_pairs,
    ROUND(
        100.0 * SUM(label) / COUNT(*),
        2
    ) AS precision
FROM scored
GROUP BY simple_score
ORDER BY simple_score
""").fetchall()

for score, candidates, true_pairs, precision in rows:
    print(
        f"{score:>8}"
        f"{candidates:>18,}"
        f"{true_pairs:>18,}"
        f"{precision:>13.2f}%"
    )


print()
print("=" * 100)
print(f"DONE — elapsed {(time.time() - start) / 60:.2f} minutes")
print("=" * 100)