import duckdb

con = duckdb.connect()

print("Testing DuckDB string similarity functions...")

print(
    "levenshtein:",
    con.execute(
        "SELECT levenshtein('amazon', 'amazn')"
    ).fetchone()[0]
)

print(
    "jaro_winkler:",
    con.execute(
        "SELECT jaro_winkler_similarity('amazon', 'amazn')"
    ).fetchone()[0]
)

print("DONE")