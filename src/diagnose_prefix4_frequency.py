import duckdb
import time

start = time.time()

DB = duckdb.connect()

DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")
DB.execute("SET preserve_insertion_order=false")

S1 = "parquet/train_source1_normalized.parquet"
S2 = "parquet/train_source2_normalized.parquet"
S3 = "parquet/train_source3_normalized.parquet"

print("=" * 90)
print("PREFIX4 FREQUENCY DIAGNOSTIC")
print("=" * 90)

# ------------------------------------------------------------
# Source 1 prefix groups
# ------------------------------------------------------------

print("\nBuilding S1 prefix groups...")

DB.execute(f"""
CREATE OR REPLACE TEMP TABLE s1_groups AS
SELECT
    country_norm,
    left(name_compact, 4) AS prefix4,
    length(name_compact) AS name_len,
    COUNT(*) AS cnt
FROM read_parquet('{S1}')
WHERE name_compact IS NOT NULL
  AND length(name_compact) >= 4
GROUP BY
    country_norm,
    prefix4,
    name_len
""")

# ------------------------------------------------------------
# Source 2 + Source 3 prefix groups
# ------------------------------------------------------------

print("Building Source2+Source3 prefix groups...")

DB.execute(f"""
CREATE OR REPLACE TEMP TABLE s23_groups AS
SELECT
    country_norm,
    left(name_compact, 4) AS prefix4,
    length(name_compact) AS name_len,
    COUNT(*) AS cnt
FROM (
    SELECT
        country_norm,
        name_compact
    FROM read_parquet('{S2}')
    WHERE name_compact IS NOT NULL
      AND length(name_compact) >= 4

    UNION ALL

    SELECT
        country_norm,
        name_compact
    FROM read_parquet('{S3}')
    WHERE name_compact IS NOT NULL
      AND length(name_compact) >= 4
)
GROUP BY
    country_norm,
    prefix4,
    name_len
""")

print("Groups created.")


# ------------------------------------------------------------
# Estimate candidate count
# ------------------------------------------------------------

print("\nEstimating candidate volume...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE estimates AS
SELECT
    a.country_norm,
    a.prefix4,
    a.name_len AS s1_len,
    b.name_len AS s23_len,
    a.cnt AS s1_count,
    b.cnt AS s23_count,

    a.cnt * b.cnt AS pair_count

FROM s1_groups a

JOIN s23_groups b
    ON a.country_norm = b.country_norm
   AND a.prefix4 = b.prefix4
   AND abs(a.name_len - b.name_len) <= 2
""")


total = DB.execute("""
SELECT COALESCE(SUM(pair_count), 0)
FROM estimates
""").fetchone()[0]

s1_covered = DB.execute("""
SELECT COUNT(DISTINCT
    country_norm || '|' || prefix4 || '|' || CAST(s1_len AS VARCHAR)
)
FROM estimates
""").fetchone()[0]


print()
print("=" * 90)
print("ESTIMATED CANDIDATE VOLUME")
print("=" * 90)

print(f"Estimated candidate pairs: {total:,}")
print(f"Prefix/length groups:       {s1_covered:,}")


# ------------------------------------------------------------
# Distribution of group sizes
# ------------------------------------------------------------

print("\nGroup-size distribution...")

stats = DB.execute("""
SELECT
    MIN(pair_count),
    AVG(pair_count),
    median(pair_count),
    quantile_cont(pair_count, 0.90),
    quantile_cont(pair_count, 0.95),
    quantile_cont(pair_count, 0.99),
    MAX(pair_count)
FROM estimates
""").fetchone()

print(f"Minimum group-pairs: {stats[0]:,}")
print(f"Average group-pairs: {stats[1]:,.2f}")
print(f"Median group-pairs:  {stats[2]:,.2f}")
print(f"P90:                 {stats[3]:,.2f}")
print(f"P95:                 {stats[4]:,.2f}")
print(f"P99:                 {stats[5]:,.2f}")
print(f"Maximum:             {stats[6]:,}")


# ------------------------------------------------------------
# Test stricter length windows
# ------------------------------------------------------------

print()
print("=" * 90)
print("ESTIMATES FOR DIFFERENT LENGTH WINDOWS")
print("=" * 90)

for window in [0, 1, 2, 3, 4, 5]:

    result = DB.execute(f"""
        SELECT COALESCE(SUM(
            a.cnt * b.cnt
        ), 0)
        FROM s1_groups a
        JOIN s23_groups b
            ON a.country_norm = b.country_norm
           AND a.prefix4 = b.prefix4
           AND abs(a.name_len - b.name_len) <= {window}
    """).fetchone()[0]

    print(
        f"length diff <= {window}: "
        f"{result:,} candidates"
    )


# ------------------------------------------------------------
# Prefix length comparison
# ------------------------------------------------------------

print()
print("=" * 90)
print("PREFIX-LENGTH ESTIMATES")
print("=" * 90)

for prefix_len in [3, 4, 5, 6]:

    DB.execute(f"""
    CREATE OR REPLACE TEMP TABLE p AS
    SELECT
        country_norm,
        left(name_compact, {prefix_len}) AS prefix,
        length(name_compact) AS name_len,
        COUNT(*) AS cnt
    FROM read_parquet('{S1}')
    WHERE name_compact IS NOT NULL
      AND length(name_compact) >= {prefix_len}
    GROUP BY country_norm, prefix, name_len
    """)

    DB.execute(f"""
    CREATE OR REPLACE TEMP TABLE q AS
    SELECT
        country_norm,
        left(name_compact, {prefix_len}) AS prefix,
        length(name_compact) AS name_len,
        COUNT(*) AS cnt
    FROM (
        SELECT country_norm, name_compact
        FROM read_parquet('{S2}')
        WHERE name_compact IS NOT NULL
          AND length(name_compact) >= {prefix_len}

        UNION ALL

        SELECT country_norm, name_compact
        FROM read_parquet('{S3}')
        WHERE name_compact IS NOT NULL
          AND length(name_compact) >= {prefix_len}
    )
    GROUP BY country_norm, prefix, name_len
    """)

    result = DB.execute("""
        SELECT COALESCE(SUM(a.cnt * b.cnt), 0)
        FROM p a
        JOIN q b
          ON a.country_norm = b.country_norm
         AND a.prefix = b.prefix
         AND abs(a.name_len - b.name_len) <= 2
    """).fetchone()[0]

    print(
        f"prefix {prefix_len} + length<=2: "
        f"{result:,} candidates"
    )


print()
print("=" * 90)
print("DONE")
print("=" * 90)

print(
    f"Elapsed time: {(time.time() - start) / 60:.2f} minutes"
)