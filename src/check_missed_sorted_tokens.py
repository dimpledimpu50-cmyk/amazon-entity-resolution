import duckdb
import os

BASE = r"C:\Users\DIMPLE K B\Documents\amazon-project\amazon_entity_resolution"

MISSED = os.path.join(
    BASE, "intermediate", "missed_true_pairs.parquet"
)

S2 = os.path.join(
    BASE, "parquet", "train_source2_normalized.parquet"
)

S3 = os.path.join(
    BASE, "parquet", "train_source3_normalized.parquet"
)

con = duckdb.connect()

con.execute("""
SET memory_limit='10GB';
SET threads=4;
SET preserve_insertion_order=false;
""")

print("=" * 70)
print("MISSED-PAIR SORTED TOKEN DIAGNOSTIC")
print("=" * 70)


# ============================================================
# 1. MISSED TRUE PAIRS
# ============================================================

print("\n[1/5] Loading missed true pairs...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE missed AS

SELECT *
FROM read_parquet('{MISSED}');
""")

missed_count = con.execute("""
SELECT COUNT(*)
FROM missed;
""").fetchone()[0]

print(f"Missed true pairs: {missed_count:,}")


# ============================================================
# 2. GET S1 DETAILS
# ============================================================

print("\n[2/5] Preparing S1 names...")

# missed_true_pairs already contains the S1 details from
# analyze_missed_pairs_fast.py.
#
# We only need to inspect its schema first.

columns = con.execute("""
DESCRIBE missed;
""").fetchall()

print("\nColumns:")
for c in columns:
    print("  ", c[0])


# ============================================================
# 3. IDENTIFY NAME COLUMNS
# ============================================================

# The saved missed-pair file normally contains:
#
# source1_entity_id
# candidate_entity_id
# s1_name
# candidate_name
# ...
#
# We handle the common names explicitly.

column_names = [c[0] for c in columns]

s1_name_col = None
candidate_name_col = None

for name in [
    "s1_name",
    "source1_name",
    "name_s1",
    "name_norm_s1"
]:
    if name in column_names:
        s1_name_col = name
        break

for name in [
    "candidate_name",
    "source_name",
    "name_candidate",
    "name_norm_candidate"
]:
    if name in column_names:
        candidate_name_col = name
        break

if s1_name_col is None or candidate_name_col is None:
    print("\nCould not automatically identify name columns.")

    print("\nAvailable columns:")
    for c in column_names:
        print(" ", c)

    print(
        "\nStop here and send me the column list above."
    )

    con.close()
    raise SystemExit


print(
    f"\nS1 name column:        {s1_name_col}"
)

print(
    f"Candidate name column: {candidate_name_col}"
)


# ============================================================
# 4. BUILD SORTED TOKEN SIGNATURES
# ============================================================

print("\n[3/5] Creating sorted-token signatures...")


generic_pattern = (
    r'\b('
    r'private|limited|ltd|pvt|pvtltd|'
    r'company|co|inc|incorporated|'
    r'corporation|corp|llc|llp|pllc|lp|'
    r'services|service|center|centre|group|'
    r'associates|partners|holdings|international|'
    r'industries|industry|enterprises|enterprise|'
    r'solutions|technologies|technology|trading|'
    r'products|product|management|foundation|'
    r'clinic|medical|healthcare|health|'
    r'systems|system|consulting|consultants'
    r')\b'
)


# Normalize names into tokens, remove generic/legal words,
# sort remaining tokens alphabetically and join them.

con.execute(f"""
CREATE OR REPLACE TEMP TABLE signatures AS

SELECT

    *,

    (
        SELECT string_agg(token, ' ' ORDER BY token)

        FROM unnest(
            regexp_extract_all(
                regexp_replace(
                    lower(
                        {s1_name_col}
                    ),
                    '{generic_pattern}',
                    ' ',
                    'g'
                ),
                '[a-z0-9]+'
            )
        ) AS t(token)

        WHERE
            length(token) >= 3
    ) AS s1_sorted_tokens,

    (
        SELECT string_agg(token, ' ' ORDER BY token)

        FROM unnest(
            regexp_extract_all(
                regexp_replace(
                    lower(
                        {candidate_name_col}
                    ),
                    '{generic_pattern}',
                    ' ',
                    'g'
                ),
                '[a-z0-9]+'
            )
        ) AS t(token)

        WHERE
            length(token) >= 3
    ) AS candidate_sorted_tokens

FROM missed;
""")


# ============================================================
# 5. EVALUATE
# ============================================================

print("\n[4/5] Evaluating exact sorted-token matches...")


result = con.execute("""
SELECT

    COUNT(*) AS missed_pairs,

    COUNT(*) FILTER (
        WHERE
            s1_sorted_tokens IS NOT NULL
            AND candidate_sorted_tokens IS NOT NULL
            AND s1_sorted_tokens != ''
            AND candidate_sorted_tokens != ''
            AND s1_sorted_tokens =
                candidate_sorted_tokens
    ) AS exact_sorted_token_matches

FROM signatures;
""").fetchone()


missed_pairs = result[0]
sorted_matches = result[1]

percentage = (
    sorted_matches / missed_pairs * 100
    if missed_pairs
    else 0
)


print("\n" + "=" * 70)
print("SORTED TOKEN DIAGNOSTIC")
print("=" * 70)

print(
    f"Missed true pairs:             "
    f"{missed_pairs:,}"
)

print(
    f"Exact sorted-token matches:    "
    f"{sorted_matches:,}"
)

print(
    f"Recoverable percentage:        "
    f"{percentage:.2f}%"
)


# ============================================================
# EXAMPLES
# ============================================================

print("\n[5/5] Examples...")

examples = con.execute("""
SELECT
    s1_sorted_tokens,
    candidate_sorted_tokens

FROM signatures

WHERE
    s1_sorted_tokens IS NOT NULL
    AND candidate_sorted_tokens IS NOT NULL
    AND s1_sorted_tokens != ''
    AND candidate_sorted_tokens != ''
    AND s1_sorted_tokens =
        candidate_sorted_tokens

LIMIT 15;
""").fetchall()


if examples:

    print("\nExamples of recoverable pairs:")

    for s1, candidate in examples:

        print(
            f"\n  S1:        {s1}"
        )

        print(
            f"  Candidate: {candidate}"
        )

else:

    print(
        "\nNo exact sorted-token matches found."
    )


print("\n" + "=" * 70)

con.close()