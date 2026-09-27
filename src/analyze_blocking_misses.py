import duckdb
import pandas as pd
import time
from pathlib import Path


# ================================================================
# PATHS
# ================================================================

PROJECT_ROOT = Path.cwd()

GT_FILE = (
    PROJECT_ROOT
    / "dataset"
    / "train"
    / "train_ground_truth.tsv"
)

CANDIDATE_FILE = (
    PROJECT_ROOT
    / "intermediate"
    / "train_candidate_pairs.parquet"
)

S1_FILE = (
    PROJECT_ROOT
    / "parquet"
    / "train_source1_normalized.parquet"
)

S2_FILE = (
    PROJECT_ROOT
    / "parquet"
    / "train_source2_normalized.parquet"
)

S3_FILE = (
    PROJECT_ROOT
    / "parquet"
    / "train_source3_normalized.parquet"
)


# ================================================================
# CHECK FILES
# ================================================================

print("=" * 80)
print("ANALYZING BLOCKING MISSES")
print("=" * 80)

print()
print("Checking required files...")

required_files = [
    GT_FILE,
    CANDIDATE_FILE,
    S1_FILE,
    S2_FILE,
    S3_FILE,
]

for file in required_files:

    if not file.exists():

        raise FileNotFoundError(
            f"\nRequired file not found:\n{file}"
        )

    print(f"OK: {file}")


# ================================================================
# START
# ================================================================

start = time.time()


# ================================================================
# DUCKDB
# ================================================================

con = duckdb.connect()

con.execute(
    "SET memory_limit='6GB'"
)

con.execute(
    "SET threads=2"
)

con.execute(
    "SET preserve_insertion_order=false"
)


# ================================================================
# 1. LOAD GROUND TRUTH
# ================================================================

print()
print("Loading ground truth with pandas...")

gt_df = pd.read_csv(
    str(GT_FILE),
    sep="\t",
    dtype=str,
    keep_default_na=False
)

print(
    f"Ground-truth rows: "
    f"{len(gt_df):,}"
)

print(
    f"Ground-truth columns: "
    f"{list(gt_df.columns)}"
)


# ================================================================
# CHECK COLUMNS
# ================================================================

required_columns = {
    "source1_entity_id",
    "matched_entity_ids",
}

missing = (
    required_columns
    - set(gt_df.columns)
)

if missing:

    raise ValueError(
        f"Missing columns: {missing}"
    )


# ================================================================
# REGISTER GROUND TRUTH
# ================================================================

con.register(
    "gt_dataframe",
    gt_df[
        [
            "source1_entity_id",
            "matched_entity_ids",
        ]
    ]
)


con.execute("""
CREATE OR REPLACE TEMP TABLE gt AS

SELECT

    source1_entity_id,
    matched_entity_ids

FROM gt_dataframe;
""")


# ================================================================
# 2. CREATE TRUE PAIRS
# ================================================================

print()
print("Creating individual ground-truth pairs...")


con.execute("""
CREATE OR REPLACE TEMP TABLE true_pairs AS

SELECT

    g.source1_entity_id,

    trim(x) AS candidate_entity_id

FROM gt g

CROSS JOIN UNNEST(
    string_split(
        COALESCE(
            g.matched_entity_ids,
            ''
        ),
        ','
    )
) AS t(x)

WHERE
    trim(x) <> '';
""")


true_count = con.execute("""
SELECT COUNT(*)
FROM true_pairs
""").fetchone()[0]


print(
    f"Ground-truth pairs: "
    f"{true_count:,}"
)


# ================================================================
# 3. FIND MISSED PAIRS
# ================================================================

print()
print("Comparing against current candidate pairs...")


con.execute(
    f"""
    CREATE OR REPLACE TEMP TABLE missed AS

    SELECT

        t.source1_entity_id,
        t.candidate_entity_id

    FROM true_pairs t

    ANTI JOIN read_parquet(
        '{CANDIDATE_FILE.as_posix()}'
    ) c

    ON
        t.source1_entity_id
        =
        c.source1_entity_id

    AND
        t.candidate_entity_id
        =
        c.candidate_entity_id;
    """
)


missed_count = con.execute("""
SELECT COUNT(*)
FROM missed
""").fetchone()[0]


found_count = (
    true_count
    - missed_count
)


recall = (
    found_count
    / true_count
    * 100
    if true_count
    else 0
)


loss = (
    missed_count
    / true_count
    * 100
    if true_count
    else 0
)


print()
print("=" * 80)
print("CURRENT BLOCKING PERFORMANCE")
print("=" * 80)

print(
    f"True pairs       : "
    f"{true_count:,}"
)

print(
    f"Found by blocker : "
    f"{found_count:,}"
)

print(
    f"Missed           : "
    f"{missed_count:,}"
)

print(
    f"Blocking recall  : "
    f"{recall:.2f}%"
)

print(
    f"Blocking loss    : "
    f"{loss:.2f}%"
)


# ================================================================
# 4. JOIN NORMALIZED RECORDS
# ================================================================

print()
print("Joining missed pairs with normalized data...")
print("This may take a few minutes.")


con.execute(
    f"""
    CREATE OR REPLACE TEMP TABLE missed_features AS

    SELECT

        m.source1_entity_id,
        m.candidate_entity_id,

        s1.name_norm
            AS s1_name,

        c.name_norm
            AS candidate_name,

        s1.name_compact
            AS s1_compact,

        c.name_compact
            AS candidate_compact,

        s1.address_norm
            AS s1_address,

        c.address_norm
            AS candidate_address,

        s1.address_compact
            AS s1_address_compact,

        c.address_compact
            AS candidate_address_compact,

        s1.country_norm
            AS s1_country,

        c.country_norm
            AS candidate_country

    FROM missed m


    JOIN read_parquet(
        '{S1_FILE.as_posix()}'
    ) s1

    ON
        m.source1_entity_id
        =
        s1.entity_id


    JOIN (

        SELECT

            entity_id,
            name_norm,
            name_compact,
            address_norm,
            address_compact,
            country_norm

        FROM read_parquet(
            '{S2_FILE.as_posix()}'
        )


        UNION ALL


        SELECT

            entity_id,
            name_norm,
            name_compact,
            address_norm,
            address_compact,
            country_norm

        FROM read_parquet(
            '{S3_FILE.as_posix()}'
        )

    ) c

    ON
        m.candidate_entity_id
        =
        c.entity_id;
    """
)


feature_count = con.execute("""
SELECT COUNT(*)
FROM missed_features
""").fetchone()[0]


print(
    f"Missed pairs with features: "
    f"{feature_count:,}"
)


# ================================================================
# HELPER
# ================================================================

def analyze(label, query):

    count = con.execute(
        query
    ).fetchone()[0]

    percentage = (
        count
        / missed_count
        * 100
        if missed_count
        else 0
    )

    print(
        f"{label:<42}"
        f"{count:>12,}"
        f" ({percentage:6.2f}%)"
    )


# ================================================================
# 5. NAME ANALYSIS
# ================================================================

print()
print("=" * 80)
print("NAME RELATIONSHIPS OF MISSED TRUE PAIRS")
print("=" * 80)


analyze(
    "Exact normalized name",
    """
    SELECT COUNT(*)

    FROM missed_features

    WHERE
        s1_name <> ''
        AND
        s1_name = candidate_name
    """
)


analyze(
    "Exact compact name",
    """
    SELECT COUNT(*)

    FROM missed_features

    WHERE
        s1_compact <> ''
        AND
        s1_compact = candidate_compact
    """
)


analyze(
    "Same first 4 name characters",
    """
    SELECT COUNT(*)

    FROM missed_features

    WHERE
        length(s1_name) >= 4
        AND
        length(candidate_name) >= 4
        AND
        left(s1_name, 4)
        =
        left(candidate_name, 4)
    """
)


analyze(
    "Same first 5 name characters",
    """
    SELECT COUNT(*)

    FROM missed_features

    WHERE
        length(s1_name) >= 5
        AND
        length(candidate_name) >= 5
        AND
        left(s1_name, 5)
        =
        left(candidate_name, 5)
    """
)


analyze(
    "Same last 4 name characters",
    """
    SELECT COUNT(*)

    FROM missed_features

    WHERE
        length(s1_name) >= 4
        AND
        length(candidate_name) >= 4
        AND
        right(s1_name, 4)
        =
        right(candidate_name, 4)
    """
)


analyze(
    "Name contains",
    """
    SELECT COUNT(*)

    FROM missed_features

    WHERE
        length(s1_name) >= 4
        AND
        length(candidate_name) >= 4
        AND
        (
            s1_name LIKE
                '%' || candidate_name || '%'

            OR

            candidate_name LIKE
                '%' || s1_name || '%'
        )
    """
)


# ================================================================
# 6. ADDRESS ANALYSIS
# ================================================================

print()
print("=" * 80)
print("ADDRESS RELATIONSHIPS OF MISSED TRUE PAIRS")
print("=" * 80)


analyze(
    "Exact normalized address",
    """
    SELECT COUNT(*)

    FROM missed_features

    WHERE
        s1_address <> ''
        AND
        s1_address = candidate_address
    """
)


analyze(
    "Exact compact address",
    """
    SELECT COUNT(*)

    FROM missed_features

    WHERE
        s1_address_compact <> ''
        AND
        s1_address_compact =
        candidate_address_compact
    """
)


analyze(
    "Same first 5 address characters",
    """
    SELECT COUNT(*)

    FROM missed_features

    WHERE
        length(s1_address) >= 5
        AND
        length(candidate_address) >= 5
        AND
        left(s1_address, 5)
        =
        left(candidate_address, 5)
    """
)


# ================================================================
# 7. COUNTRY
# ================================================================

print()
print("=" * 80)
print("COUNTRY RELATIONSHIP")
print("=" * 80)


analyze(
    "Same country",
    """
    SELECT COUNT(*)

    FROM missed_features

    WHERE
        s1_country <> ''
        AND
        s1_country = candidate_country
    """
)


# ================================================================
# 8. HOUSE NUMBER
# ================================================================

print()
print("=" * 80)
print("HOUSE NUMBER RELATIONSHIP")
print("=" * 80)


analyze(
    "Same extracted house number",
    """
    SELECT COUNT(*)

    FROM missed_features

    WHERE

        regexp_extract(
            s1_address,
            '(^|[^0-9])[0-9]+',
            0
        )

        =

        regexp_extract(
            candidate_address,
            '(^|[^0-9])[0-9]+',
            0
        )

        AND

        regexp_extract(
            s1_address,
            '(^|[^0-9])[0-9]+',
            0
        ) <> ''
    """
)


# ================================================================
# 9. COMBINATION ANALYSIS
# ================================================================

print()
print("=" * 80)
print("COMBINATION ANALYSIS")
print("=" * 80)


combination = con.execute("""
SELECT

    COUNT(*) AS pairs,

    SUM(
        CASE
            WHEN
                s1_name <> ''
                AND
                s1_name = candidate_name
            THEN 1
            ELSE 0
        END
    ) AS exact_name,

    SUM(
        CASE
            WHEN
                length(s1_name) >= 4
                AND
                length(candidate_name) >= 4
                AND
                left(s1_name, 4)
                =
                left(candidate_name, 4)
            THEN 1
            ELSE 0
        END
    ) AS prefix4,

    SUM(
        CASE
            WHEN
                length(s1_name) >= 5
                AND
                length(candidate_name) >= 5
                AND
                left(s1_name, 5)
                =
                left(candidate_name, 5)
            THEN 1
            ELSE 0
        END
    ) AS prefix5,

    SUM(
        CASE
            WHEN
                s1_address <> ''
                AND
                s1_address = candidate_address
            THEN 1
            ELSE 0
        END
    ) AS exact_address,

    SUM(
        CASE
            WHEN
                length(s1_address) >= 5
                AND
                length(candidate_address) >= 5
                AND
                left(s1_address, 5)
                =
                left(candidate_address, 5)
            THEN 1
            ELSE 0
        END
    ) AS address_prefix5,

    SUM(
        CASE
            WHEN
                s1_country <> ''
                AND
                s1_country = candidate_country
            THEN 1
            ELSE 0
        END
    ) AS same_country

FROM missed_features;
""").fetchone()


combination_names = [
    "pairs",
    "exact_name",
    "prefix4",
    "prefix5",
    "exact_address",
    "address_prefix5",
    "same_country",
]


for name, value in zip(
    combination_names,
    combination
):

    percentage = (
        value
        / missed_count
        * 100
        if missed_count
        else 0
    )

    print(
        f"{name:<25}"
        f"{value:>12,}"
        f" ({percentage:6.2f}%)"
    )


# ================================================================
# 10. JARO-WINKLER
# ================================================================

print()
print("=" * 80)
print("JARO-WINKLER SIMILARITY")
print("=" * 80)


similarity = con.execute("""
SELECT

    COUNT(*) AS pairs,

    AVG(
        jaro_winkler_similarity(
            s1_name,
            candidate_name
        )
    ) AS name_jaro,

    AVG(
        jaro_winkler_similarity(
            s1_address,
            candidate_address
        )
    ) AS address_jaro

FROM missed_features

WHERE
    s1_name <> ''
    AND
    candidate_name <> '';
""").fetchone()


print(
    f"Pairs                : "
    f"{similarity[0]:,}"
)

print(
    f"Average name Jaro    : "
    f"{similarity[1]:.4f}"
)

print(
    f"Average address Jaro : "
    f"{similarity[2]:.4f}"
)


# ================================================================
# 11. NAME JARO BANDS
# ================================================================

print()
print("=" * 80)
print("NAME JARO-WINKLER BANDS")
print("=" * 80)


bands = con.execute("""
SELECT

    CASE

        WHEN
            jaro_winkler_similarity(
                s1_name,
                candidate_name
            ) >= 0.95

        THEN '0.95 - 1.00'


        WHEN
            jaro_winkler_similarity(
                s1_name,
                candidate_name
            ) >= 0.90

        THEN '0.90 - 0.95'


        WHEN
            jaro_winkler_similarity(
                s1_name,
                candidate_name
            ) >= 0.85

        THEN '0.85 - 0.90'


        WHEN
            jaro_winkler_similarity(
                s1_name,
                candidate_name
            ) >= 0.80

        THEN '0.80 - 0.85'


        WHEN
            jaro_winkler_similarity(
                s1_name,
                candidate_name
            ) >= 0.70

        THEN '0.70 - 0.80'


        ELSE '< 0.70'

    END AS band,

    COUNT(*) AS pairs

FROM missed_features

WHERE
    s1_name <> ''
    AND
    candidate_name <> ''

GROUP BY band

ORDER BY band DESC;
""").fetchall()


for band, count in bands:

    percentage = (
        count
        / missed_count
        * 100
        if missed_count
        else 0
    )

    print(
        f"{band:<18}"
        f"{count:>12,}"
        f" ({percentage:6.2f}%)"
    )


# ================================================================
# 12. TOP MISSED PREFIXES
# ================================================================

print()
print("=" * 80)
print("TOP MISSED NAME PREFIXES")
print("=" * 80)


prefixes = con.execute("""
SELECT

    left(s1_name, 4) AS prefix,

    COUNT(*) AS pairs

FROM missed_features

WHERE
    length(s1_name) >= 4

GROUP BY prefix

ORDER BY pairs DESC

LIMIT 20;
""").fetchall()


for prefix, count in prefixes:

    print(
        f"{prefix:<20}"
        f"{count:>12,}"
    )


# ================================================================
# 13. FINISH
# ================================================================

elapsed = (
    time.time() - start
) / 60


print()
print("=" * 80)
print("DONE")
print("=" * 80)

print(
    f"Elapsed: {elapsed:.2f} minutes"
)

print()
print("No files were modified.")