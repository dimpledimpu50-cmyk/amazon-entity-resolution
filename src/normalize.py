from pathlib import Path
import re
import unicodedata

import polars as pl


ROOT = Path(__file__).resolve().parent.parent

TRAIN_DIR = ROOT / "dataset" / "train"
TEST_DIR = ROOT / "dataset" / "test"

INTERMEDIATE_DIR = ROOT / "intermediate"
INTERMEDIATE_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------
# Text normalization
# ---------------------------------------------------------

def normalize_text(value):
    """
    General normalization.

    Keeps Unicode characters so that names such as Hindi,
    Tamil, French accented characters, etc. are not destroyed.
    """
    if value is None:
        return ""

    value = str(value)

    # Unicode normalization
    value = unicodedata.normalize("NFKC", value)

    # Lowercase
    value = value.lower().strip()

    # Replace punctuation/symbol-like characters with spaces.
    # Keep Unicode letters/numbers.
    value = re.sub(r"[^\w\s]", " ", value, flags=re.UNICODE)

    # Collapse multiple spaces
    value = re.sub(r"\s+", " ", value).strip()

    return value


def normalize_compact(value):
    """
    More aggressive version used for similarity/blocking.

    Example:
        "Prime Money Inc."
        -> "primemoneyinc"
    """
    text = normalize_text(value)

    return re.sub(r"\s+", "", text)


def tokenize(value):
    """
    Return normalized tokens.
    """
    text = normalize_text(value)

    if not text:
        return []

    return text.split()


# ---------------------------------------------------------
# Name-specific normalization
# ---------------------------------------------------------

def normalize_name(value):
    return normalize_text(value)


def normalize_name_compact(value):
    return normalize_compact(value)


# ---------------------------------------------------------
# Address-specific normalization
# ---------------------------------------------------------

def normalize_address(value):
    return normalize_text(value)


def normalize_address_compact(value):
    return normalize_compact(value)


# ---------------------------------------------------------
# Country normalization
# ---------------------------------------------------------

def normalize_country(value):
    if value is None:
        return ""

    return str(value).strip().lower()


# ---------------------------------------------------------
# Add normalized columns
# ---------------------------------------------------------

def add_normalized_columns(df: pl.DataFrame) -> pl.DataFrame:

    return df.with_columns([
        pl.col("business_name")
        .map_elements(normalize_name, return_dtype=pl.String)
        .alias("name_norm"),

        pl.col("business_name")
        .map_elements(normalize_name_compact, return_dtype=pl.String)
        .alias("name_compact"),

        pl.col("business_address")
        .map_elements(normalize_address, return_dtype=pl.String)
        .alias("address_norm"),

        pl.col("business_address")
        .map_elements(normalize_address_compact, return_dtype=pl.String)
        .alias("address_compact"),

        pl.col("country")
        .map_elements(normalize_country, return_dtype=pl.String)
        .alias("country_norm"),
    ])


# ---------------------------------------------------------
# Process one file
# ---------------------------------------------------------

def process_file(input_path: Path, output_path: Path):

    print(f"\nProcessing: {input_path.name}")

    df = pl.read_csv(
        input_path,
        separator="\t",
        infer_schema_length=1000,
        null_values=["", "NULL", "null", "None"]
    )

    print(f"Original rows: {len(df):,}")

    df = add_normalized_columns(df)

    df.write_csv(
        output_path,
        separator="\t"
    )

    print(f"Saved: {output_path}")
    print(f"Rows: {len(df):,}")


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():

    files = [
        # Training
        (
            TRAIN_DIR / "train_source1.tsv",
            INTERMEDIATE_DIR / "train_source1_normalized.tsv"
        ),
        (
            TRAIN_DIR / "train_source2.tsv",
            INTERMEDIATE_DIR / "train_source2_normalized.tsv"
        ),
        (
            TRAIN_DIR / "train_source3.tsv",
            INTERMEDIATE_DIR / "train_source3_normalized.tsv"
        ),

        # Test
        (
            TEST_DIR / "test_source1.tsv",
            INTERMEDIATE_DIR / "test_source1_normalized.tsv"
        ),
        (
            TEST_DIR / "test_source2.tsv",
            INTERMEDIATE_DIR / "test_source2_normalized.tsv"
        ),
        (
            TEST_DIR / "test_source3.tsv",
            INTERMEDIATE_DIR / "test_source3_normalized.tsv"
        ),
    ]

    for input_path, output_path in files:

        if not input_path.exists():
            print(f"[WARNING] Missing: {input_path}")
            continue

        process_file(input_path, output_path)


if __name__ == "__main__":
    main()