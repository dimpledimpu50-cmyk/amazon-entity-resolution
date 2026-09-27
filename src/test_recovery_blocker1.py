from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parent.parent

PARQUET_DIR = ROOT / "parquet"
INTERMEDIATE = ROOT / "intermediate"

S2 = PARQUET_DIR / "train_source2_normalized.parquet"
S3 = PARQUET_DIR / "train_source3_normalized.parquet"

FIXED_S1 = INTERMEDIATE / "blocking_eval_s1.parquet"
FIXED_TRUE = INTERMEDIATE / "blocking_eval_true_pairs.parquet"


# ============================================================
# CONNECTION
# ============================================================

con = duckdb.connect()

con.execute("SET memory_limit='4GB'")
con.execute("SET threads=2")
con.execute("SET preserve_insertion_order=false")


print("=" * 80)
print("RECOVERY BLOCKER #1")
print("Country + House Number + Rare Address Token")
print("=" * 80)


# ============================================================
# 1. FIXED S1
# ============================================================

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE s1 AS
    SELECT
        entity_id,
        country_norm,
        address_norm
    FROM read_parquet('{FIXED_S1}')
""")


# ============================================================
# 2. FIXED TRUE PAIRS
# ============================================================

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE true_pairs AS
    SELECT
        s1_id,
        candidate_id
    FROM read_parquet('{FIXED_TRUE}')
""")


true_count = con.execute("""
    SELECT COUNT(*)
    FROM true_pairs
""").fetchone()[0]


# ============================================================
# 3. SOURCE DATA
# ============================================================

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE sources AS

    SELECT
        entity_id,
        country_norm,
        address_norm
    FROM read_parquet('{S2}')

    UNION ALL

    SELECT
        entity_id,
        country_norm,
        address_norm
    FROM read_parquet('{S3}')
""")


# ============================================================
# 4. EXTRACT HOUSE NUMBER
# ============================================================

print("\n[1] Extracting house numbers...")

con.execute("""
    CREATE OR REPLACE TEMP TABLE s1_addr AS
    SELECT
        entity_id,
        country_norm,
        address_norm,

        regexp_extract(
            address_norm,
            '[0-9]+'
        ) AS house_number

    FROM s1
""")


con.execute("""
    CREATE OR REPLACE TEMP TABLE source_addr AS
    SELECT
        entity_id,
        country_norm,
        address_norm,

        regexp_extract(
            address_norm,
            '[0-9]+'
        ) AS house_number

    FROM sources
""")


# ============================================================
# 5. ADDRESS TOKENS
# ============================================================

print("[2] Extracting address tokens...")


# We use tokens of length >= 5.
#
# Ignore:
#   empty tokens
#   pure numbers
#
# This prevents common tiny tokens such as:
#   st
#   rd
#   in
#   the
#
# from generating huge candidate sets.

con.execute("""
    CREATE OR REPLACE TEMP TABLE s1_tokens AS

    SELECT DISTINCT
        a.entity_id,
        a.country_norm,
        a.house_number,
        lower(trim(t.token)) AS token

    FROM s1_addr a

    CROSS JOIN UNNEST(
        string_split(
            regexp_replace(
                a.address_norm,
                '[^a-zA-Z0-9]+',
                ' ',
                'g'
            ),
            ' '
        )
    ) AS t(token)

    WHERE length(trim(t.token)) >= 5

      AND NOT regexp_matches(
          trim(t.token),
          '^[0-9]+$'
      )
""")


con.execute("""
    CREATE OR REPLACE TEMP TABLE source_tokens AS

    SELECT DISTINCT
        a.entity_id,
        a.country_norm,
        a.house_number,
        lower(trim(t.token)) AS token

    FROM source_addr a

    CROSS JOIN UNNEST(
        string_split(
            regexp_replace(
                a.address_norm,
                '[^a-zA-Z0-9]+',
                ' ',
                'g'
            ),
            ' '
        )
    ) AS t(token)

    WHERE length(trim(t.token)) >= 5

      AND NOT regexp_matches(
          trim(t.token),
          '^[0-9]+$'
      )
""")


# ============================================================
# 6. TOKEN FREQUENCY
# ============================================================

print("[3] Calculating token frequencies...")


con.execute("""
    CREATE OR REPLACE TEMP TABLE token_freq AS

    SELECT
        country_norm,
        house_number,
        token,
        COUNT(DISTINCT entity_id) AS freq

    FROM source_tokens

    WHERE house_number <> ''
      AND token <> ''

    GROUP BY
        country_norm,
        house_number,
        token
""")


# ============================================================
# 7. GENERATE CANDIDATES
# ============================================================

print("[4] Generating candidates...")

con.execute("""
    CREATE OR REPLACE TEMP TABLE candidates AS

    SELECT DISTINCT

        s.entity_id AS s1_id,

        c.entity_id AS candidate_id

    FROM s1_tokens s

    JOIN source_tokens c

      ON s.country_norm = c.country_norm

     AND s.house_number = c.house_number

     AND s.token = c.token

    JOIN token_freq f

      ON c.country_norm = f.country_norm

     AND c.house_number = f.house_number

     AND c.token = f.token

    WHERE s.house_number <> ''

      AND s.token <> ''

      AND f.freq <= 50
""")


# ============================================================
# 8. STATISTICS
# ============================================================

stats = con.execute("""
    SELECT
        COUNT(*) AS total_pairs,
        COUNT(DISTINCT s1_id) AS s1_count
    FROM candidates
""").fetchone()


distribution = con.execute("""
    SELECT
        AVG(cnt),
        MEDIAN(cnt),
        QUANTILE_CONT(cnt, 0.95),
        MAX(cnt)
    FROM (
        SELECT
            s1_id,
            COUNT(*) AS cnt
        FROM candidates
        GROUP BY s1_id
    )
""").fetchone()


# ============================================================
# 9. RECALL
# ============================================================

recall = con.execute("""
    SELECT
        COUNT(*) AS found
    FROM true_pairs t

    INNER JOIN candidates c

      ON t.s1_id = c.s1_id

     AND t.candidate_id = c.candidate_id
""").fetchone()[0]


recall_pct = recall * 100.0 / true_count


# ============================================================
# 10. OUTPUT
# ============================================================

print("\n" + "=" * 80)
print("RECOVERY BLOCKER #1 RESULTS")
print("=" * 80)

print("\nCandidate statistics")

print(f"Candidate pairs    : {stats[0]:,}")
print(f"S1 with candidates : {stats[1]:,}")
print(f"Average/S1         : {distribution[0]:.2f}")
print(f"Median             : {distribution[1]:.2f}")
print(f"P95                : {distribution[2]:.2f}")
print(f"Maximum            : {distribution[3]:,}")

print("\nBlocking recall")

print(f"True pairs         : {true_count:,}")
print(f"Found              : {recall:,}")
print(f"Recall             : {recall_pct:.2f}%")

print("\nComparison with V4")

print("V4 recall          : 60.17%")
print(f"Recovery recall    : {recall_pct:.2f}%")

print("\nIMPORTANT:")
print("This blocker is evaluated independently.")
print("Do NOT combine it with V4 yet.")

print("=" * 80)

con.close()