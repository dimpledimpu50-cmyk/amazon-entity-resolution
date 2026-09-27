import duckdb

con = duckdb.connect()

con.execute("SET memory_limit='6GB'")
con.execute("SET threads=2")

print("Checking whether existing ML sample can be joined back to IDs...")
print()

query = """
SELECT COUNT(*)
FROM read_parquet('intermediate/ml_training_sample.parquet') m
JOIN read_parquet('intermediate/train_match_features.parquet') f
ON
    m.label = f.label
    AND m.country_equal = f.country_equal
    AND m.exact_name = f.exact_name
    AND m.exact_compact_name = f.exact_compact_name
    AND m.name_prefix4 = f.name_prefix4
    AND m.name_prefix5 = f.name_prefix5
    AND m.name_suffix4 = f.name_suffix4
    AND m.name_contains = f.name_contains
    AND m.name_length_diff = f.name_length_diff
    AND m.exact_address = f.exact_address
    AND m.exact_compact_address = f.exact_compact_address
    AND m.address_prefix5 = f.address_prefix5
    AND m.same_house = f.same_house
    AND m.address_length_diff = f.address_length_diff
    AND m.prefix4_same_house = f.prefix4_same_house
    AND m.prefix4_address_prefix = f.prefix4_address_prefix
    AND m.exact_name_same_house = f.exact_name_same_house
    AND m.exact_name_exact_address = f.exact_name_exact_address
"""

count = con.execute(query).fetchone()[0]

sample_count = con.execute("""
    SELECT COUNT(*)
    FROM read_parquet('intermediate/ml_training_sample.parquet')
""").fetchone()[0]

print(f"Existing sample rows : {sample_count:,}")
print(f"Matched rows         : {count:,}")

if count == sample_count:
    print()
    print("SUCCESS: every sample row can be recovered.")
else:
    print()
    print("WARNING: row count does not match.")