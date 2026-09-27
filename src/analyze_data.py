import duckdb
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INTERMEDIATE = ROOT / "intermediate"


def inspect_file(con, path):
    print("\n" + "=" * 70)
    print(f"FILE: {path.name}")
    print("=" * 70)

    # --------------------------------------------------
    # Basic information
    # --------------------------------------------------

    columns = con.execute(f"""
        DESCRIBE SELECT *
        FROM read_csv_auto(
            '{path}',
            delim='\\t',
            header=true
        )
    """).fetchall()

    print("\nColumns:")
    for row in columns:
        print(f"  {row[0]:25} {row[1]}")

    # --------------------------------------------------
    # Row count
    # --------------------------------------------------

    row_count = con.execute(f"""
        SELECT COUNT(*)
        FROM read_csv_auto(
            '{path}',
            delim='\\t',
            header=true
        )
    """).fetchone()[0]

    print(f"\nRows: {row_count:,}")

    # --------------------------------------------------
    # Missing values
    # --------------------------------------------------

    print("\nMissing values:")

    result = con.execute(f"""
        SELECT
            COUNT(*) AS total_rows,

            COUNT(*) FILTER (
                WHERE business_name IS NULL
                   OR TRIM(business_name) = ''
            ) AS missing_name,

            COUNT(*) FILTER (
                WHERE business_address IS NULL
                   OR TRIM(business_address) = ''
            ) AS missing_address,

            COUNT(*) FILTER (
                WHERE country IS NULL
                   OR TRIM(country) = ''
            ) AS missing_country

        FROM read_csv_auto(
            '{path}',
            delim='\\t',
            header=true
        )
    """).fetchone()

    total, missing_name, missing_address, missing_country = result

    print(f"  Name:    {missing_name:,} ({missing_name / total * 100:.2f}%)")
    print(f"  Address: {missing_address:,} ({missing_address / total * 100:.2f}%)")
    print(f"  Country: {missing_country:,} ({missing_country / total * 100:.2f}%)")

    # --------------------------------------------------
    # Country distribution
    # --------------------------------------------------

    print("\nTop countries:")

    countries = con.execute(f"""
        SELECT
            country_norm,
            COUNT(*) AS cnt
        FROM read_csv_auto(
            '{path}',
            delim='\\t',
            header=true
        )
        GROUP BY country_norm
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()

    for country, count in countries:
        print(f"  {str(country):20} {count:,}")


def main():

    con = duckdb.connect()

    files = [
        INTERMEDIATE / "train_source1_normalized.tsv",
        INTERMEDIATE / "train_source2_normalized.tsv",
        INTERMEDIATE / "train_source3_normalized.tsv",
    ]

    for path in files:

        if not path.exists():
            print(f"[WARNING] Missing: {path}")
            continue

        inspect_file(con, path)

    con.close()


if __name__ == "__main__":
    main()