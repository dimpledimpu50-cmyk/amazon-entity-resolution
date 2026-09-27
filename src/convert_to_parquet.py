import duckdb
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent

INTERMEDIATE = ROOT / "intermediate"
PARQUET_DIR = ROOT / "parquet"

PARQUET_DIR.mkdir(exist_ok=True)


FILES = [
    "train_source1_normalized.tsv",
    "train_source2_normalized.tsv",
    "train_source3_normalized.tsv",
    "test_source1_normalized.tsv",
    "test_source2_normalized.tsv",
    "test_source3_normalized.tsv",
]


def convert_file(con, filename):

    input_path = INTERMEDIATE / filename

    output_name = filename.replace(".tsv", ".parquet")
    output_path = PARQUET_DIR / output_name

    print("\n" + "=" * 70)
    print(f"Converting: {filename}")
    print("=" * 70)

    if not input_path.exists():
        print(f"[WARNING] Missing: {input_path}")
        return

    if output_path.exists():
        print(f"Already exists: {output_path}")
        return

    con.execute(f"""
        COPY (
            SELECT *
            FROM read_csv(
                '{input_path}',
                delim='\\t',
                header=true,
                auto_detect=true
            )
        )
        TO '{output_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    print(f"Saved: {output_path}")


def main():

    # Keep DuckDB from trying to use all system resources.
    con = duckdb.connect()

    con.execute("SET threads TO 4")

    for filename in FILES:
        convert_file(con, filename)

    con.close()


if __name__ == "__main__":
    main()