import duckdb

DB_MEMORY = "6GB"
THREADS = 2

FEATURE_FILE = "intermediate/train_match_features.parquet"

con = duckdb.connect()
con.execute(f"SET memory_limit='{DB_MEMORY}'")
con.execute(f"SET threads={THREADS}")
con.execute("SET preserve_insertion_order=false")

print("=" * 80)
print("ANALYZING MISSED TRUE MATCHES")
print("=" * 80)

# Current six accepted matching rules
rules = """
    (
        exact_name = 1
        AND exact_address = 1
    )
    OR
    (
        exact_name = 1
        AND same_house = 1
    )
    OR
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

print("\nLoading training features...")

# True pairs that were generated as candidates but rejected
query = f"""
SELECT
    COUNT(*) AS missed_true_pairs,

    SUM(
        CASE
            WHEN country_equal = 1
            THEN 1 ELSE 0
        END
    ) AS country_equal,

    SUM(
        CASE
            WHEN exact_name = 1
            THEN 1 ELSE 0
        END
    ) AS exact_name,

    SUM(
        CASE
            WHEN exact_compact_name = 1
            THEN 1 ELSE 0
        END
    ) AS exact_compact_name,

    SUM(
        CASE
            WHEN name_prefix5 = 1
            THEN 1 ELSE 0
        END
    ) AS prefix5,

    SUM(
        CASE
            WHEN name_prefix4 = 1
            THEN 1 ELSE 0
        END
    ) AS prefix4,

    SUM(
        CASE
            WHEN name_contains = 1
            THEN 1 ELSE 0
        END
    ) AS name_contains,

    SUM(
        CASE
            WHEN exact_address = 1
            THEN 1 ELSE 0
        END
    ) AS exact_address,

    SUM(
        CASE
            WHEN exact_compact_address = 1
            THEN 1 ELSE 0
        END
    ) AS exact_compact_address,

    SUM(
        CASE
            WHEN same_house = 1
            THEN 1 ELSE 0
        END
    ) AS same_house,

    SUM(
        CASE
            WHEN address_prefix5 = 1
            THEN 1 ELSE 0
        END
    ) AS address_prefix5,

    SUM(
        CASE
            WHEN prefix4_same_house = 1
            THEN 1 ELSE 0
        END
    ) AS prefix4_same_house,

    SUM(
        CASE
            WHEN prefix4_address_prefix = 1
            THEN 1 ELSE 0
        END
    ) AS prefix4_address_prefix,

    SUM(
        CASE
            WHEN exact_name_same_house = 1
            THEN 1 ELSE 0
        END
    ) AS exact_name_same_house,

    SUM(
        CASE
            WHEN exact_name_exact_address = 1
            THEN 1 ELSE 0
        END
    ) AS exact_name_exact_address

FROM read_parquet('{FEATURE_FILE}')
WHERE label = 1
  AND NOT ({rules})
"""

result = con.execute(query).fetchone()
columns = [x[0] for x in con.description]

print("\n" + "=" * 80)
print("MISSED TRUE MATCH ANALYSIS")
print("=" * 80)

for name, value in zip(columns, result):
    print(f"{name:30s}: {value:,}")

print("\nDone.")