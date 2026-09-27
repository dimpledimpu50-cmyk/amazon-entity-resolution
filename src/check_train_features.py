import duckdb

con = duckdb.connect()

result = con.execute("""
    DESCRIBE
    SELECT *
    FROM read_parquet('intermediate/train_match_features.parquet')
""").fetchall()

print("TRAIN MATCH FEATURES")
print("=" * 80)

for row in result:
    print(row)

print("\nTotal columns:", len(result))