import duckdb
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INTERMEDIATE = ROOT / "intermediate"


def analyze_source(con, path):

    print("\n" + "=" * 75)
    print(f"FILE: {path.name}")
    print("=" * 75)

    # --------------------------------------------------
    # 1. Unique normalized names
    # --------------------------------------------------

    total, unique_names = con.execute(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT name_norm) AS unique_names
        FROM read_csv_auto(
            '{path}',
            delim='\\t',
            header=true
        )
    """).fetchone()

    print(f"\nTotal records:       {total:,}")
    print(f"Unique name_norm:    {unique_names:,}")
    print(
        f"Name uniqueness:     {unique_names / total * 100:.2f}%"
    )

    # --------------------------------------------------
    # 2. Duplicate name distribution
    # --------------------------------------------------

    print("\nName frequency distribution:")

    result = con.execute(f"""
        SELECT
            CASE
                WHEN cnt = 1 THEN '1'
                WHEN cnt = 2 THEN '2'
                WHEN cnt = 3 THEN '3'
                WHEN cnt BETWEEN 4 AND 10 THEN '4-10'
                WHEN cnt BETWEEN 11 AND 50 THEN '11-50'
                WHEN cnt BETWEEN 51 AND 100 THEN '51-100'
                WHEN cnt BETWEEN 101 AND 1000 THEN '101-1000'
                ELSE '1000+'
            END AS frequency_group,

            COUNT(*) AS number_of_names

        FROM (
            SELECT
                name_norm,
                COUNT(*) AS cnt
            FROM read_csv_auto(
                '{path}',
                delim='\\t',
                header=true
            )
            GROUP BY name_norm
        )
        GROUP BY frequency_group
        ORDER BY
            CASE frequency_group
                WHEN '1' THEN 1
                WHEN '2' THEN 2
                WHEN '3' THEN 3
                WHEN '4-10' THEN 4
                WHEN '11-50' THEN 5
                WHEN '51-100' THEN 6
                WHEN '101-1000' THEN 7
                ELSE 8
            END
    """).fetchall()

    for group, count in result:
        print(f"  {group:10} {count:,} names")

    # --------------------------------------------------
    # 3. Largest duplicate groups
    # --------------------------------------------------

    print("\nTop 20 most frequent normalized names:")

    result = con.execute(f"""
        SELECT
            name_norm,
            COUNT(*) AS cnt
        FROM read_csv_auto(
            '{path}',
            delim='\\t',
            header=true
        )
        WHERE name_norm <> ''
        GROUP BY name_norm
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()

    for name, count in result:
        print(f"  {count:8,}  {name}")

    # --------------------------------------------------
    # 4. Compact-name collisions
    # --------------------------------------------------

    print("\nCompact-name statistics:")

    total_compact, unique_compact = con.execute(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT name_compact) AS unique_compact
        FROM read_csv_auto(
            '{path}',
            delim='\\t',
            header=true
        )
        WHERE name_compact <> ''
    """).fetchone()

    print(f"  Unique name_compact: {unique_compact:,}")
    print(
        f"  Compact uniqueness: {unique_compact / total_compact * 100:.2f}%"
    )

    # --------------------------------------------------
    # 5. Very large compact groups
    # --------------------------------------------------

    print("\nTop 20 compact-name groups:")

    result = con.execute(f"""
        SELECT
            name_compact,
            COUNT(*) AS cnt
        FROM read_csv_auto(
            '{path}',
            delim='\\t',
            header=true
        )
        WHERE name_compact <> ''
        GROUP BY name_compact
        ORDER BY cnt DESC
        LIMIT 20
    """).fetchall()

    for name, count in result:
        print(f"  {count:8,}  {name}")


def main():

    con = duckdb.connect()

    files = [
        INTERMEDIATE / "train_source1_normalized.tsv",
        INTERMEDIATE / "train_source2_normalized.tsv",
        INTERMEDIATE / "train_source3_normalized.tsv",
    ]

    for path in files:

        if path.exists():
            analyze_source(con, path)
        else:
            print(f"[WARNING] Missing: {path}")

    con.close()


if __name__ == "__main__":
    main()