from pathlib import Path
import polars as pl


ROOT = Path(__file__).resolve().parent.parent

TRAIN_DIR = ROOT / "dataset" / "train"
TEST_DIR = ROOT / "dataset" / "test"


def inspect_file(path):
    print("\n" + "=" * 70)
    print(f"FILE: {path.name}")
    print("=" * 70)

    # Read only a small sample first.
    df = pl.read_csv(
        path,
        separator="\t",
        n_rows=10
    )

    print("\nColumns:")
    print(df.columns)

    print("\nFirst 10 rows:")
    print(df)

    print("\nSchema:")
    print(df.schema)


def main():

    train_files = [
        TRAIN_DIR / "train_source1.tsv",
        TRAIN_DIR / "train_source2.tsv",
        TRAIN_DIR / "train_source3.tsv",
        TRAIN_DIR / "train_ground_truth.tsv",
    ]

    test_files = [
        TEST_DIR / "test_source1.tsv",
        TEST_DIR / "test_source2.tsv",
        TEST_DIR / "test_source3.tsv",
    ]

    for path in train_files + test_files:

        if path.exists():
            inspect_file(path)
        else:
            print(f"\n[WARNING] File not found: {path}")


if __name__ == "__main__":
    main()