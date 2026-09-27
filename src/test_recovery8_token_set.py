import duckdb

# ============================================================
# R8-v2 — RARE TOKEN-PAIR BLOCKER
#
# Goal:
#   Catch word-order changes and extra generic/legal words
#
# Example:
#   "strategic fields llc"
#   "fields strategic llc"
#
# After removing generic words:
#   strategic fields
#   fields strategic
#
# The two rarest informative tokens are sorted into the same
# deterministic pair key.
#
# This version avoids expensive list intersections.
# ============================================================

con = duckdb.connect()

print("=" * 70)
print("RECOVERY #8-v2 — RARE TOKEN-PAIR BLOCKER")
print("=" * 70)

# ============================================================
# FILES
# ============================================================

s1_file = "intermediate/blocking_eval_s1.parquet"
truth_file = "intermediate/blocking_eval_true_pairs.parquet"
baseline_file = "intermediate/verified_baseline_candidates.parquet"

# ============================================================
# LOAD FIXED SAMPLE
# ============================================================

con.execute(f"""
CREATE OR REPLACE TEMP TABLE s1_sample AS
SELECT *
FROM read_parquet('{s1_file}');
""")

print("Loading S2 + S3 source data...")

con.execute("""
CREATE OR REPLACE TEMP TABLE source_data AS
SELECT *
FROM (
    SELECT *
    FROM read_parquet('parquet/train_source2_normalized.parquet')

    UNION ALL

    SELECT *
    FROM read_parquet('parquet/train_source3_normalized.parquet')
);
""")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE true_pairs AS
SELECT
    s1_id AS source1_entity_id,
    candidate_id AS candidate_entity_id
FROM read_parquet('{truth_file}');
""")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE base_candidates AS
SELECT *
FROM read_parquet('{baseline_file}');
""")

# ============================================================
# BASELINE
# ============================================================

baseline_count = con.execute("""
SELECT COUNT(*)
FROM base_candidates;
""").fetchone()[0]

baseline_found = con.execute("""
SELECT COUNT(*)
FROM true_pairs t
JOIN base_candidates b
  ON t.source1_entity_id = b.source1_entity_id
 AND t.candidate_entity_id = b.candidate_entity_id;
""").fetchone()[0]

true_count = con.execute("""
SELECT COUNT(*)
FROM true_pairs;
""").fetchone()[0]

print()
print(f"Verified baseline candidates : {baseline_count:,}")
print(f"Verified baseline found     : {baseline_found:,}")
print(f"True pairs                  : {true_count:,}")
print()

# ============================================================
# GENERIC / LEGAL WORDS
# ============================================================

generic_words = """
private
limited
ltd
pvt
pvtltd
company
co
inc
incorporated
corporation
corp
llc
llp
pllc
lp
services
service
center
centre
group
associates
associate
partners
partner
holdings
holding
international
industries
industry
enterprises
enterprise
solutions
solution
technologies
technology
trading
products
product
management
foundation
clinic
medical
healthcare
health
systems
system
consulting
consultants
consultant
"""

generic_list = [
    x.strip()
    for x in generic_words.split()
    if x.strip()
]

generic_sql = ", ".join(
    "'" + x.replace("'", "''") + "'"
    for x in generic_list
)

# ============================================================
# CREATE NAME TOKENS — S1
# ============================================================

print("Building S1 informative tokens...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE s1_tokens AS
SELECT
    entity_id,
    country_norm,
    UNNEST(
        list_filter(
            list_distinct(
                regexp_split_to_array(
                    regexp_replace(
                        lower(coalesce(name_norm, '')),
                        '[^a-z0-9]+',
                        ' ',
                        'g'
                    ),
                    ' '
                )
            ),
            x ->
                length(x) >= 4
                AND x <> ''
                AND x NOT IN ({generic_sql})
                AND regexp_matches(x, '^[a-z0-9]+$')
        )
    ) AS token
FROM s1_sample;
""")

# ============================================================
# CREATE NAME TOKENS — SOURCE
# ============================================================

print("Building source informative tokens...")

con.execute(f"""
CREATE OR REPLACE TEMP TABLE source_tokens AS
SELECT
    entity_id,
    country_norm,
    UNNEST(
        list_filter(
            list_distinct(
                regexp_split_to_array(
                    regexp_replace(
                        lower(coalesce(name_norm, '')),
                        '[^a-z0-9]+',
                        ' ',
                        'g'
                    ),
                    ' '
                )
            ),
            x ->
                length(x) >= 4
                AND x <> ''
                AND x NOT IN ({generic_sql})
                AND regexp_matches(x, '^[a-z0-9]+$')
        )
    ) AS token
FROM source_data;
""")

# ============================================================
# TOKEN FREQUENCY IN SOURCE
# ============================================================

print("Calculating source token frequencies...")

con.execute("""
CREATE OR REPLACE TEMP TABLE source_token_freq AS
SELECT
    country_norm,
    token,
    COUNT(*) AS freq
FROM source_tokens
GROUP BY
    country_norm,
    token;
""")

# ============================================================
# KEEP RARE TOKENS
#
# Frequency <= 100
# ============================================================

print("Keeping rare informative tokens...")

con.execute("""
CREATE OR REPLACE TEMP TABLE source_rare_tokens AS
SELECT
    s.entity_id,
    s.country_norm,
    s.token,
    f.freq
FROM source_tokens s
JOIN source_token_freq f
  ON s.country_norm = f.country_norm
 AND s.token = f.token
WHERE f.freq <= 100;
""")

con.execute("""
CREATE OR REPLACE TEMP TABLE s1_rare_tokens AS
SELECT
    s.entity_id,
    s.country_norm,
    s.token,
    f.freq
FROM s1_tokens s
JOIN source_token_freq f
  ON s.country_norm = f.country_norm
 AND s.token = f.token
WHERE f.freq <= 100;
""")

# ============================================================
# RANK TOKENS BY FREQUENCY
#
# The rarest tokens get rank 1 and rank 2.
# ============================================================

print("Selecting two rarest tokens per entity...")

con.execute("""
CREATE OR REPLACE TEMP TABLE source_ranked_tokens AS
SELECT
    entity_id,
    country_norm,
    token,
    freq,
    ROW_NUMBER() OVER (
        PARTITION BY entity_id
        ORDER BY freq ASC, token ASC
    ) AS rn
FROM source_rare_tokens;
""")

con.execute("""
CREATE OR REPLACE TEMP TABLE s1_ranked_tokens AS
SELECT
    entity_id,
    country_norm,
    token,
    freq,
    ROW_NUMBER() OVER (
        PARTITION BY entity_id
        ORDER BY freq ASC, token ASC
    ) AS rn
FROM s1_rare_tokens;
""")

# ============================================================
# CREATE TOKEN-PAIR KEYS
#
# If only one rare token exists, it is not enough for R8.
#
# We require TWO rare informative tokens.
# ============================================================

print("Creating rare token-pair keys...")

con.execute("""
CREATE OR REPLACE TEMP TABLE source_token_pairs AS
SELECT
    entity_id,
    country_norm,

    CASE
        WHEN token1 < token2
        THEN token1 || '|' || token2
        ELSE token2 || '|' || token1
    END AS pair_key

FROM (
    SELECT
        a.entity_id,
        a.country_norm,
        a.token AS token1,
        b.token AS token2
    FROM source_ranked_tokens a
    JOIN source_ranked_tokens b
      ON a.entity_id = b.entity_id
     AND a.country_norm = b.country_norm
     AND a.rn = 1
     AND b.rn = 2
);
""")

con.execute("""
CREATE OR REPLACE TEMP TABLE s1_token_pairs AS
SELECT
    entity_id,
    country_norm,

    CASE
        WHEN token1 < token2
        THEN token1 || '|' || token2
        ELSE token2 || '|' || token1
    END AS pair_key

FROM (
    SELECT
        a.entity_id,
        a.country_norm,
        a.token AS token1,
        b.token AS token2
    FROM s1_ranked_tokens a
    JOIN s1_ranked_tokens b
      ON a.entity_id = b.entity_id
     AND a.country_norm = b.country_norm
     AND a.rn = 1
     AND b.rn = 2
);
""")

# ============================================================
# PAIR FREQUENCY
#
# Even a pair of rare tokens can occasionally occur many times.
# Keep only pair keys occurring <= 25 source records.
# ============================================================

print("Filtering frequent token pairs...")

con.execute("""
CREATE OR REPLACE TEMP TABLE source_pair_freq AS
SELECT
    country_norm,
    pair_key,
    COUNT(*) AS freq
FROM source_token_pairs
GROUP BY
    country_norm,
    pair_key;
""")

con.execute("""
CREATE OR REPLACE TEMP TABLE source_token_pairs_clean AS
SELECT
    p.entity_id,
    p.country_norm,
    p.pair_key
FROM source_token_pairs p
JOIN source_pair_freq f
  ON p.country_norm = f.country_norm
 AND p.pair_key = f.pair_key
WHERE f.freq <= 25;
""")

# ============================================================
# R8 CANDIDATES
#
# This is now a simple indexed-style equality join:
#
# country + pair_key
#
# No expensive token-set comparison.
# ============================================================

print("Generating R8-v2 candidates...")

con.execute("""
CREATE OR REPLACE TEMP TABLE r8_candidates AS
SELECT DISTINCT
    s.entity_id AS source1_entity_id,
    c.entity_id AS candidate_entity_id
FROM s1_token_pairs s
JOIN source_token_pairs_clean c
  ON s.country_norm = c.country_norm
 AND s.pair_key = c.pair_key;
""")

# ============================================================
# BASIC R8 STATS
# ============================================================

r8_count = con.execute("""
SELECT COUNT(*)
FROM r8_candidates;
""").fetchone()[0]

s1_count = con.execute("""
SELECT COUNT(DISTINCT source1_entity_id)
FROM r8_candidates;
""").fetchone()[0]

avg_count = con.execute("""
SELECT AVG(cnt)
FROM (
    SELECT
        source1_entity_id,
        COUNT(*) AS cnt
    FROM r8_candidates
    GROUP BY source1_entity_id
);
""").fetchone()[0]

median_count = con.execute("""
SELECT MEDIAN(cnt)
FROM (
    SELECT
        source1_entity_id,
        COUNT(*) AS cnt
    FROM r8_candidates
    GROUP BY source1_entity_id
);
""").fetchone()[0]

p95 = con.execute("""
SELECT QUANTILE_CONT(cnt, 0.95)
FROM (
    SELECT
        source1_entity_id,
        COUNT(*) AS cnt
    FROM r8_candidates
    GROUP BY source1_entity_id
);
""").fetchone()[0]

max_count = con.execute("""
SELECT MAX(cnt)
FROM (
    SELECT
        source1_entity_id,
        COUNT(*) AS cnt
    FROM r8_candidates
    GROUP BY source1_entity_id
);
""").fetchone()[0]

# ============================================================
# R8 TRUE PAIRS
# ============================================================

r8_found = con.execute("""
SELECT COUNT(*)
FROM true_pairs t
JOIN r8_candidates r
  ON t.source1_entity_id = r.source1_entity_id
 AND t.candidate_entity_id = r.candidate_entity_id;
""").fetchone()[0]

# ============================================================
# NEW TRUE PAIRS BEYOND BASELINE
# ============================================================

new_pairs = con.execute("""
SELECT COUNT(*)
FROM true_pairs t
JOIN r8_candidates r
  ON t.source1_entity_id = r.source1_entity_id
 AND t.candidate_entity_id = r.candidate_entity_id
LEFT JOIN base_candidates b
  ON t.source1_entity_id = b.source1_entity_id
 AND t.candidate_entity_id = b.candidate_entity_id
WHERE b.source1_entity_id IS NULL;
""").fetchone()[0]

# ============================================================
# ADDITIONAL CANDIDATES
# ============================================================

additional_candidates = con.execute("""
SELECT COUNT(*)
FROM r8_candidates r
LEFT JOIN base_candidates b
  ON r.source1_entity_id = b.source1_entity_id
 AND r.candidate_entity_id = b.candidate_entity_id
WHERE b.source1_entity_id IS NULL;
""").fetchone()[0]

# ============================================================
# COMBINED
# ============================================================

combined_candidates = (
    baseline_count +
    additional_candidates
)

combined_found = (
    baseline_found +
    new_pairs
)

combined_recall = (
    combined_found /
    true_count *
    100
)

if new_pairs > 0:
    candidates_per_new_pair = (
        additional_candidates /
        new_pairs
    )
else:
    candidates_per_new_pair = 0

# ============================================================
# RESULTS
# ============================================================

print()
print("=" * 70)
print("R8-v2 RESULTS")
print("=" * 70)

print()
print("R8-v2 BLOCKER")
print("-" * 70)

print(f"R8 candidates         : {r8_count:,}")
print(f"S1 with R8 candidates : {s1_count:,}")
print(f"Average / S1          : {avg_count:.2f}")
print(f"Median / S1           : {median_count:.1f}")
print(f"P95 / S1              : {p95:.1f}")
print(f"Max / S1              : {max_count:,}")

print()
print("R8-v2 RECALL")
print("-" * 70)

print(f"True pairs            : {true_count:,}")
print(f"R8 found              : {r8_found:,}")
print(
    f"R8 recall             : "
    f"{r8_found / true_count * 100:.2f}%"
)

print()
print("BASELINE")
print("-" * 70)

print(f"Baseline candidates   : {baseline_count:,}")
print(f"Baseline found        : {baseline_found:,}")
print(
    f"Baseline recall       : "
    f"{baseline_found / true_count * 100:.2f}%"
)

print()
print("INCREMENTAL GAIN")
print("-" * 70)

print(f"NEW true pairs        : {new_pairs:,}")
print(f"Additional candidates : {additional_candidates:,}")
print(
    f"Candidates / new pair: "
    f"{candidates_per_new_pair:.2f}"
)

print()
print("COMBINED")
print("-" * 70)

print(f"Combined candidates   : {combined_candidates:,}")
print(f"Combined found        : {combined_found:,}")
print(f"Combined recall       : {combined_recall:.2f}%")
print(f"Remaining missed      : {true_count - combined_found:,}")

print()
print("=" * 70)
print("DONE")
print("=" * 70)

con.close()