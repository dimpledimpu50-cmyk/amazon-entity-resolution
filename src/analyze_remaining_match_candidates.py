import duckdb
import time

DB = duckdb.connect()

DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")

FEATURES = "intermediate/train_match_features.parquet"

start = time.time()

print("=" * 100)
print("REMAINING MATCH CANDIDATE ANALYSIS")
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

remaining AS (

    SELECT *
    FROM base
    WHERE already_matched = 0
),

rules AS (

    SELECT
        *,

        CASE
            WHEN name_prefix4 = 1
             AND same_house = 1
            THEN 1 ELSE 0
        END AS r_prefix4_house,

        CASE
            WHEN name_prefix5 = 1
             AND same_house = 1
            THEN 1 ELSE 0
        END AS r_prefix5_house,

        CASE
            WHEN name_prefix4 = 1
             AND address_prefix5 = 1
            THEN 1 ELSE 0
        END AS r_prefix4_address,

        CASE
            WHEN name_prefix5 = 1
             AND address_prefix5 = 1
            THEN 1 ELSE 0
        END AS r_prefix5_address,

        CASE
            WHEN name_contains = 1
             AND address_prefix5 = 1
            THEN 1 ELSE 0
        END AS r_contains_address,

        CASE
            WHEN name_prefix4 = 1
             AND same_house = 1
             AND name_length_diff <= 2
            THEN 1 ELSE 0
        END AS r_prefix4_house_len2,

        CASE
            WHEN name_prefix5 = 1
             AND same_house = 1
             AND name_length_diff <= 2
            THEN 1 ELSE 0
        END AS r_prefix5_house_len2,

        CASE
            WHEN name_prefix4 = 1
             AND same_house = 1
             AND address_length_diff <= 5
            THEN 1 ELSE 0
        END AS r_prefix4_house_addrlen5,

        CASE
            WHEN name_prefix4 = 1
             AND same_house = 1
             AND address_length_diff <= 10
            THEN 1 ELSE 0
        END AS r_prefix4_house_addrlen10,

        CASE
            WHEN name_prefix5 = 1
             AND same_house = 1
             AND address_length_diff <= 10
            THEN 1 ELSE 0
        END AS r_prefix5_house_addrlen10,

        CASE
            WHEN name_prefix4 = 1
             AND same_house = 1
             AND address_prefix5 = 1
             AND name_length_diff <= 2
            THEN 1 ELSE 0
        END AS r_prefix4_house_address_len2,

        CASE
            WHEN name_prefix5 = 1
             AND same_house = 1
             AND address_prefix5 = 1
             AND name_length_diff <= 2
            THEN 1 ELSE 0
        END AS r_prefix5_house_address_len2

    FROM remaining
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
        'prefix4 + house' AS rule,
        COUNT(*) AS candidates,
        COALESCE(SUM(label), 0) AS true_pairs
    FROM rules
    WHERE r_prefix4_house = 1

    UNION ALL

    SELECT
        'prefix5 + house',
        COUNT(*),
        SUM(label)
    FROM rules
    WHERE r_prefix5_house = 1

    UNION ALL

    SELECT
        'prefix4 + address',
        COUNT(*),
        SUM(label)
    FROM rules
    WHERE r_prefix4_address = 1

    UNION ALL

    SELECT
        'prefix5 + address',
        COUNT(*),
        SUM(label)
    FROM rules
    WHERE r_prefix5_address = 1

    UNION ALL

    SELECT
        'contains + address',
        COUNT(*),
        SUM(label)
    FROM rules
    WHERE r_contains_address = 1

    UNION ALL

    SELECT
        'prefix4 + house + name_len<=2',
        COUNT(*),
        SUM(label)
    FROM rules
    WHERE r_prefix4_house_len2 = 1

    UNION ALL

    SELECT
        'prefix5 + house + name_len<=2',
        COUNT(*),
        SUM(label)
    FROM rules
    WHERE r_prefix5_house_len2 = 1

    UNION ALL

    SELECT
        'prefix4 + house + addr_len<=5',
        COUNT(*),
        SUM(label)
    FROM rules
    WHERE r_prefix4_house_addrlen5 = 1

    UNION ALL

    SELECT
        'prefix4 + house + addr_len<=10',
        COUNT(*),
        SUM(label)
    FROM rules
    WHERE r_prefix4_house_addrlen10 = 1

    UNION ALL

    SELECT
        'prefix5 + house + addr_len<=10',
        COUNT(*),
        SUM(label)
    FROM rules
    WHERE r_prefix5_house_addrlen10 = 1

    UNION ALL

    SELECT
        'prefix4 + house + address + name_len<=2',
        COUNT(*),
        SUM(label)
    FROM rules
    WHERE r_prefix4_house_address_len2 = 1

    UNION ALL

    SELECT
        'prefix5 + house + address + name_len<=2',
        COUNT(*),
        SUM(label)
    FROM rules
    WHERE r_prefix5_house_address_len2 = 1

) x

ORDER BY precision DESC, candidates ASC
"""

rows = DB.execute(query).fetchall()

print()
print(
    f"{'RULE':<48}"
    f"{'CANDIDATES':>16}"
    f"{'TRUE':>16}"
    f"{'PRECISION':>14}"
)

print("-" * 96)

for rule, candidates, true_pairs, precision in rows:

    print(
        f"{rule:<48}"
        f"{candidates:>16,}"
        f"{true_pairs:>16,}"
        f"{precision:>13.2f}%"
    )

print()
print("=" * 100)
print("DONE")
print("=" * 100)

print(f"Elapsed: {(time.time() - start) / 60:.2f} minutes")
print("=" * 100)