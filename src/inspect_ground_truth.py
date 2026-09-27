import duckdb
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

GROUND_TRUTH = ROOT / "dataset" / "train" / "train_ground_truth.tsv"


def main():

    con = duckdb.connect()

    print("=" * 75)
    print("GROUND TRUTH INSPECTION")
    print("=" * 75)

    if not GROUND_TRUTH.exists():
        print(f"[ERROR] File not found: {GROUND_TRUTH}")
        return

    # --------------------------------------------------
    # Schema
    # --------------------------------------------------

    print("\nColumns:")

    schema = con.execute(f"""
        DESCRIBE
        SELECT *
        FROM read_csv_auto(
            '{GROUND_TRUTH}',
            delim='\\t',
            header=true
        )
    """).fetchall()

    for row in schema:
        print(f"  {row[0]:30} {row[1]}")

    # --------------------------------------------------
    # Row count
    # --------------------------------------------------

    count = con.execute(f"""
        SELECT COUNT(*)
        FROM read_csv_auto(
            '{GROUND_TRUTH}',
            delim='\\t',
            header=true
        )
    """).fetchone()[0]

    print(f"\nTotal ground-truth rows: {count:,}")

    # --------------------------------------------------
    # Sample
    # --------------------------------------------------

    print("\nFirst 20 rows:")

    rows = con.execute(f"""
        SELECT *
        FROM read_csv_auto(
            '{GROUND_TRUTH}',
            delim='\\t',
            header=true
        )
        LIMIT 20
    """).fetchall()

    for row in rows:
        print(row)

    # --------------------------------------------------
    # Distinct counts
    # --------------------------------------------------

    columns = [row[0] for row in schema]

    print("\nDistinct values:")

    for column in columns:

        result = con.execute(f"""
            SELECT COUNT(DISTINCT "{column}")
            FROM read_csv_auto(
                '{GROUND_TRUTH}',
                delim='\\t',
                header=true
            )
        """).fetchone()[0]

        print(f"  {column:30} {result:,}")

    con.close()


if __name__ == "__main__":
    main()