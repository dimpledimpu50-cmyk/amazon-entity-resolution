import duckdb

con = duckdb.connect()

for path in [
    "dataset/test/test_source1.tsv",
    "dataset/test/test_source2.tsv",
    "dataset/test/test_source3.tsv",
]:
    print("\n" + "=" * 80)
    print(path)
    print("=" * 80)

    result = con.execute(
        f"""
        DESCRIBE
        SELECT *
        FROM read_csv_auto(
            '{path}',
            delim='\\t'
        )
        """
    ).fetchall()

    for row in result:
        print(row)