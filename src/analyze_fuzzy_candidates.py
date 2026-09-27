import duckdb
import time

DB = duckdb.connect()
DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")

FEATURES = "intermediate/train_match_features.parquet"

print("=" * 100)
print("FUZZY CANDIDATE ANALYSIS")
print("=" * 100)

start = time.time()

queries = [
    (
        "prefix4 + name_length<=5",
        """
        name_prefix4 = true
        AND name_length_diff <= 5
        """
    ),
    (
        "prefix4 + name_length<=2",
        """
        name_prefix4 = true
        AND name_length_diff <= 2
        """
    ),
    (
        "prefix4 + same_house",
        """
        name_prefix4 = true
        AND same_house = true
        """
    ),
    (
        "prefix4 + address_prefix",
        """
        name_prefix4 = true
        AND address_prefix5 = true
        """
    ),
    (
        "same_house + name_length<=5",
        """
        same_house = true
        AND name_length_diff <= 5
        """
    ),
    (
        "same_house + address_prefix",
        """
        same_house = true
        AND address_prefix5 = true
        """
    ),
    (
        "prefix4 + (same_house OR address_prefix)",
        """
        name_prefix4 = true
        AND (same_house = true OR address_prefix5 = true)
        """
    ),
    (
        "prefix4 + length<=5 + (house OR address)",
        """
        name_prefix4 = true
        AND name_length_diff <= 5
        AND (same_house = true OR address_prefix5 = true)
        """
    ),
]

for name, condition in queries:
    q = f"""
    SELECT
        COUNT(*) AS pairs,
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END) AS true_pairs
    FROM read_parquet('{FEATURES}')
    WHERE {condition}
    """

    pairs, true_pairs = DB.execute(q).fetchone()

    precision = (true_pairs / pairs * 100) if pairs else 0

    print(
        f"{name:<50}"
        f"{pairs:>15,}"
        f"{true_pairs:>15,}"
        f"{precision:>12.2f}%"
    )

print()
print(f"Elapsed: {(time.time() - start) / 60:.2f} minutes")
print("=" * 100)