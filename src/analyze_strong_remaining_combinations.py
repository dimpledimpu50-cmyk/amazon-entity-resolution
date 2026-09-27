import duckdb
import time

DB = duckdb.connect()

DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")

FEATURES = "intermediate/train_match_features.parquet"

start = time.time()

print("=" * 100)
print("STRONG REMAINING COMBINATION ANALYSIS")
print("=" * 100)

query = f"""
WITH base AS (

    SELECT
        *,

        CASE
            WHEN
                (exact_name = 1 AND exact_address = 1)
                OR
                (exact_name = 1 AND same_house = 1)
                OR
                (exact_compact_name = 1 AND same_house = 1)
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
            THEN 1
            ELSE 0
        END AS already_matched

    FROM read_parquet('{FEATURES}')
),

r AS (

    SELECT
        *,

        (
            name_prefix4
            + name_prefix5
            + same_house
            + address_prefix5
            + name_contains
            + exact_compact_name
        ) AS signal_count

    FROM base

    WHERE already_matched = 0
)

SELECT
    rule,
    candidates,
    true_pairs,
    ROUND(
        100.0 * true_pairs / NULLIF(candidates, 0),
        2
    ) AS precision

FROM (

    SELECT
        'prefix4 + house + address + name_len<=2' AS rule,
        COUNT(*) AS candidates,
        COALESCE(SUM(label), 0) AS true_pairs
    FROM r
    WHERE
        name_prefix4 = 1
        AND same_house = 1
        AND address_prefix5 = 1
        AND name_length_diff <= 2

    UNION ALL

    SELECT
        'prefix5 + house + address + name_len<=2',
        COUNT(*),
        COALESCE(SUM(label), 0)
    FROM r
    WHERE
        name_prefix5 = 1
        AND same_house = 1
        AND address_prefix5 = 1
        AND name_length_diff <= 2

    UNION ALL

    SELECT
        'prefix4 + house + address + addr_len<=5',
        COUNT(*),
        COALESCE(SUM(label), 0)
    FROM r
    WHERE
        name_prefix4 = 1
        AND same_house = 1
        AND address_prefix5 = 1
        AND address_length_diff <= 5

    UNION ALL

    SELECT
        'prefix5 + house + address + addr_len<=10',
        COUNT(*),
        COALESCE(SUM(label), 0)
    FROM r
    WHERE
        name_prefix5 = 1
        AND same_house = 1
        AND address_prefix5 = 1
        AND address_length_diff <= 10

    UNION ALL

    SELECT
        'prefix4 + house + address + name_len<=2 + addr_len<=5',
        COUNT(*),
        COALESCE(SUM(label), 0)
    FROM r
    WHERE
        name_prefix4 = 1
        AND same_house = 1
        AND address_prefix5 = 1
        AND name_length_diff <= 2
        AND address_length_diff <= 5

    UNION ALL

    SELECT
        'prefix5 + house + address + name_len<=2 + addr_len<=10',
        COUNT(*),
        COALESCE(SUM(label), 0)
    FROM r
    WHERE
        name_prefix5 = 1
        AND same_house = 1
        AND address_prefix5 = 1
        AND name_length_diff <= 2
        AND address_length_diff <= 10

    UNION ALL

    SELECT
        '3+ signals among name/address/house',
        COUNT(*),
        COALESCE(SUM(label), 0)
    FROM r
    WHERE signal_count >= 3

    UNION ALL

    SELECT
        '4+ signals among name/address/house',
        COUNT(*),
        COALESCE(SUM(label), 0)
    FROM r
    WHERE signal_count >= 4

) x

ORDER BY precision DESC
"""

rows = DB.execute(query).fetchall()

print()
print(
    f"{'RULE':<60}"
    f"{'CANDIDATES':>15}"
    f"{'TRUE':>15}"
    f"{'PRECISION':>14}"
)

print("-" * 104)

for rule, candidates, true_pairs, precision in rows:

    precision = precision or 0

    print(
        f"{rule:<60}"
        f"{candidates:>15,}"
        f"{true_pairs:>15,}"
        f"{precision:>13.2f}%"
    )

print()
print("=" * 100)
print("DONE")
print("=" * 100)

print(f"Elapsed: {(time.time() - start) / 60:.2f} minutes")
print("=" * 100)