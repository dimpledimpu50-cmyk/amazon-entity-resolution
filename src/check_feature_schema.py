import duckdb

con = duckdb.connect()

result = con.execute("""
    DESCRIBE
    SELECT *
    FROM read_parquet('intermediate/train_match_features.parquet')
""").fetchdf()

print(result.to_string(index=False))