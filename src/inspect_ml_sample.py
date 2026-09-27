import duckdb

con = duckdb.connect()

result = con.execute("""
    DESCRIBE
    SELECT *
    FROM read_parquet('intermediate/ml_training_sample.parquet')
""").fetchall()

print("ML TRAINING SAMPLE COLUMNS")
print("=" * 80)

for row in result:
    print(row)

print("\nTotal columns:", len(result))