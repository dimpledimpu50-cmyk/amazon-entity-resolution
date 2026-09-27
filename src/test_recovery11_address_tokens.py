from pathlib import Path
import duckdb


# ============================================================
# RECOVERY #11 — ADDRESS TOKEN INTERSECTION
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

BLOCKING_EVAL = BASE_DIR / "intermediate" / "blocking_eval_s1.parquet"
TRUE_PAIRS = BASE_DIR / "intermediate" / "blocking_eval_true_pairs.parquet"

S2 = BASE_DIR / "parquet" / "train_source2_normalized.parquet"
S3 = BASE_DIR / "parquet" / "train_source3_normalized.parquet"

BASE_CANDIDATES = (
    BASE_DIR / "intermediate" / "verified_baseline_candidates.parquet"
)

R7_CANDIDATES = (
    BASE_DIR / "intermediate" / "recovery7_candidates.parquet"
)

R10_CANDIDATES = (
    BASE_DIR / "intermediate" / "recovery10_candidates.parquet"
)

OUTPUT = (
    BASE_DIR / "intermediate" / "recovery11_candidates.parquet"
)

MAX_TOKEN_FREQ = 50
MIN_SHARED = 2

DB = duckdb.connect()


print("=" * 75)
print("RECOVERY #11 — ADDRESS TOKEN INTERSECTION")
print("=" * 75)

print()
print("Settings:")
print(f"MAX_TOKEN_FREQ  = {MAX_TOKEN_FREQ}")
print(f"MIN_SHARED      = {MIN_SHARED}")


# ============================================================
# 1. LOAD S1
# ============================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW s1 AS
SELECT
    entity_id AS s1_id,
    country_norm,
    address_norm
FROM read_parquet('{BLOCKING_EVAL}')
""")

s1_count = DB.execute("""
SELECT COUNT(*)
FROM s1
""").fetchone()[0]

print()
print(f"S1 evaluation rows: {s1_count:,}")


# ============================================================
# 2. LOAD SOURCE 2 + SOURCE 3
# ============================================================

print()
print("Loading Source 2 + Source 3...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW all_sources AS

SELECT
    entity_id AS candidate_id,
    country_norm,
    address_norm
FROM read_parquet('{S2}')

UNION ALL

SELECT
    entity_id AS candidate_id,
    country_norm,
    address_norm
FROM read_parquet('{S3}')
""")


# ============================================================
# 3. SOURCE ADDRESS TOKENS
# ============================================================

print("Building source address-token index...")

DB.execute("""
CREATE OR REPLACE TEMP VIEW source_address_tokens AS

SELECT DISTINCT
    candidate_id,
    country_norm,
    token

FROM
(
    SELECT
        candidate_id,
        country_norm,

        UNNEST(
            STRING_SPLIT(
                REGEXP_REPLACE(
                    LOWER(COALESCE(address_norm, '')),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                ),
                ' '
            )
        ) AS token

    FROM all_sources
)

WHERE
    LENGTH(token) >= 5
    AND NOT REGEXP_MATCHES(token, '^[0-9]+$')
""")


# ============================================================
# 4. REMOVE GENERIC ADDRESS TOKENS
# ============================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW source_address_tokens_clean AS

SELECT
    candidate_id,
    country_norm,
    token

FROM source_address_tokens

WHERE token NOT IN
(
    'road',
    'rd',
    'street',
    'st',
    'avenue',
    'ave',
    'lane',
    'ln',
    'drive',
    'dr',
    'boulevard',
    'blvd',
    'highway',
    'hwy',
    'block',
    'floor',
    'building',
    'bldg',
    'apartment',
    'apt',
    'unit',
    'suite',
    'plot',
    'sector',
    'phase',
    'area',
    'district',
    'city',
    'town',
    'village',
    'county',
    'state',
    'india',
    'usa',
    'united',
    'states'
)
""")


# ============================================================
# 5. TOKEN FREQUENCY
# ============================================================

print("Calculating token frequencies...")

DB.execute("""
CREATE OR REPLACE TEMP VIEW source_token_frequency AS

SELECT
    country_norm,
    token,
    COUNT(DISTINCT candidate_id) AS token_freq

FROM source_address_tokens_clean

GROUP BY
    country_norm,
    token
""")


DB.execute(f"""
CREATE OR REPLACE TEMP VIEW rare_source_tokens AS

SELECT
    country_norm,
    token

FROM source_token_frequency

WHERE token_freq <= {MAX_TOKEN_FREQ}
""")


rare_rows = DB.execute("""
SELECT COUNT(*)

FROM source_address_tokens_clean t

JOIN rare_source_tokens r
    ON t.country_norm = r.country_norm
   AND t.token = r.token
""").fetchone()[0]

print(f"Rare source-token rows: {rare_rows:,}")


# ============================================================
# 6. S1 ADDRESS TOKENS
# ============================================================

print("Building S1 address-token rows...")

DB.execute("""
CREATE OR REPLACE TEMP VIEW s1_address_tokens AS

SELECT DISTINCT
    s1_id,
    country_norm,
    token

FROM
(
    SELECT
        s1_id,
        country_norm,

        UNNEST(
            STRING_SPLIT(
                REGEXP_REPLACE(
                    LOWER(COALESCE(address_norm, '')),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                ),
                ' '
            )
        ) AS token

    FROM s1
)

WHERE
    LENGTH(token) >= 5

    AND NOT REGEXP_MATCHES(
        token,
        '^[0-9]+$'
    )

    AND token NOT IN
    (
        'road',
        'rd',
        'street',
        'st',
        'avenue',
        'ave',
        'lane',
        'ln',
        'drive',
        'dr',
        'boulevard',
        'blvd',
        'highway',
        'hwy',
        'block',
        'floor',
        'building',
        'bldg',
        'apartment',
        'apt',
        'unit',
        'suite',
        'plot',
        'sector',
        'phase',
        'area',
        'district',
        'city',
        'town',
        'village',
        'county',
        'state',
        'india',
        'usa',
        'united',
        'states'
    )
""")


# ============================================================
# 7. GENERATE R11 CANDIDATES
# ============================================================

print("Generating rare-token candidates...")

DB.execute("""
CREATE OR REPLACE TEMP VIEW rare_token_pairs AS

SELECT DISTINCT
    s.s1_id,
    t.candidate_id,
    s.token

FROM s1_address_tokens s

JOIN rare_source_tokens r
    ON s.country_norm = r.country_norm
   AND s.token = r.token

JOIN source_address_tokens_clean t
    ON t.country_norm = s.country_norm
   AND t.token = s.token
""")


rare_candidate_rows = DB.execute("""
SELECT COUNT(*)
FROM rare_token_pairs
""").fetchone()[0]

print(
    f"Rare-token candidate rows: "
    f"{rare_candidate_rows:,}"
)


# ============================================================
# 8. REQUIRE >= 2 SHARED TOKENS
# ============================================================

print("Applying >=2 shared-token condition...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW r11 AS

SELECT
    s1_id,
    candidate_id,
    COUNT(DISTINCT token) AS shared_tokens

FROM rare_token_pairs

GROUP BY
    s1_id,
    candidate_id

HAVING COUNT(DISTINCT token) >= {MIN_SHARED}
""")


# ============================================================
# 9. CANDIDATE STATISTICS
# ============================================================

print()
print("-" * 75)
print("R11 CANDIDATE STATISTICS")
print("-" * 75)

candidate_count = DB.execute("""
SELECT COUNT(*)
FROM r11
""").fetchone()[0]

s1_covered = DB.execute("""
SELECT COUNT(DISTINCT s1_id)
FROM r11
""").fetchone()[0]

avg_candidates = DB.execute("""
SELECT AVG(cnt)

FROM
(
    SELECT
        s1_id,
        COUNT(*) AS cnt

    FROM r11

    GROUP BY s1_id
)
""").fetchone()[0]

median_candidates = DB.execute("""
SELECT MEDIAN(cnt)

FROM
(
    SELECT
        s1_id,
        COUNT(*) AS cnt

    FROM r11

    GROUP BY s1_id
)
""").fetchone()[0]

p95_candidates = DB.execute("""
SELECT QUANTILE_CONT(cnt, 0.95)

FROM
(
    SELECT
        s1_id,
        COUNT(*) AS cnt

    FROM r11

    GROUP BY s1_id
)
""").fetchone()[0]

max_candidates = DB.execute("""
SELECT MAX(cnt)

FROM
(
    SELECT
        s1_id,
        COUNT(*) AS cnt

    FROM r11

    GROUP BY s1_id
)
""").fetchone()[0]

print(f"Candidates:          {candidate_count:,}")
print(f"S1 with candidates:  {s1_covered:,}")
print(f"Average candidates:  {avg_candidates:.2f}")
print(f"Median candidates:   {median_candidates:.2f}")
print(f"P95 candidates:      {p95_candidates:.2f}")
print(f"Max candidates:      {max_candidates:,}")


# ============================================================
# 10. TRUE PAIRS
# ============================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW true_pairs AS

SELECT
    s1_id AS source1_entity_id,
    candidate_id AS candidate_entity_id

FROM read_parquet('{TRUE_PAIRS}')
""")


true_pair_count = DB.execute("""
SELECT COUNT(*)
FROM true_pairs
""").fetchone()[0]

print()
print(f"True evaluation pairs: {true_pair_count:,}")


# ============================================================
# 11. R11 STANDALONE RECALL
# ============================================================

r11_found = DB.execute("""
SELECT COUNT(*)

FROM r11 r

JOIN true_pairs t

    ON r.s1_id = t.source1_entity_id
   AND r.candidate_id = t.candidate_entity_id
""").fetchone()[0]

r11_recall = (
    r11_found / true_pair_count
    if true_pair_count
    else 0
)

print()
print("-" * 75)
print("R11 STANDALONE RECALL")
print("-" * 75)

print(f"R11 true pairs found: {r11_found:,}")
print(f"R11 recall:           {r11_recall * 100:.2f}%")


# ============================================================
# 12. CURRENT BASELINE
# ============================================================

print()
print("Loading current baseline...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW base AS

SELECT DISTINCT
    source1_entity_id AS s1_id,
    candidate_entity_id AS candidate_id

FROM read_parquet('{BASE_CANDIDATES}')

UNION

SELECT DISTINCT
    source1_entity_id AS s1_id,
    candidate_entity_id AS candidate_id

FROM read_parquet('{R7_CANDIDATES}')

UNION

SELECT DISTINCT
    source1_entity_id AS s1_id,
    candidate_entity_id AS candidate_id

FROM read_parquet('{R10_CANDIDATES}')
""")


base_count = DB.execute("""
SELECT COUNT(*)
FROM base
""").fetchone()[0]

base_found = DB.execute("""
SELECT COUNT(*)

FROM base b

JOIN true_pairs t

    ON b.s1_id = t.source1_entity_id
   AND b.candidate_id = t.candidate_entity_id
""").fetchone()[0]

base_recall = (
    base_found / true_pair_count
    if true_pair_count
    else 0
)

print()
print("-" * 75)
print("CURRENT BASELINE")
print("-" * 75)

print(f"Current baseline candidates: {base_count:,}")
print(f"Current baseline true pairs: {base_found:,}")
print(f"Current baseline recall:     {base_recall * 100:.2f}%")


# ============================================================
# 13. R11 NEW CANDIDATES
# ============================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW r11_new AS

SELECT
    r.s1_id,
    r.candidate_id

FROM r11 r

WHERE NOT EXISTS
(
    SELECT 1

    FROM base b

    WHERE
        b.s1_id = r.s1_id
        AND b.candidate_id = r.candidate_id
)
""")


new_candidates = DB.execute("""
SELECT COUNT(*)
FROM r11_new
""").fetchone()[0]


new_true_pairs = DB.execute("""
SELECT COUNT(*)

FROM r11_new r

JOIN true_pairs t

    ON r.s1_id = t.source1_entity_id
   AND r.candidate_id = t.candidate_entity_id
""").fetchone()[0]


cand_per_new_pair = (
    new_candidates / new_true_pairs
    if new_true_pairs
    else float("inf")
)


# ============================================================
# 14. COMBINED BASELINE + R11
# ============================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW combined AS

SELECT
    s1_id,
    candidate_id

FROM base

UNION

SELECT
    s1_id,
    candidate_id

FROM r11
""")


combined_candidates = DB.execute("""
SELECT COUNT(*)
FROM combined
""").fetchone()[0]


combined_found = DB.execute("""
SELECT COUNT(*)

FROM combined c

JOIN true_pairs t

    ON c.s1_id = t.source1_entity_id
   AND c.candidate_id = t.candidate_entity_id
""").fetchone()[0]


combined_recall = (
    combined_found / true_pair_count
    if true_pair_count
    else 0
)


remaining_missed = true_pair_count - combined_found


print()
print("-" * 75)
print("R11 INCREMENTAL VALUE")
print("-" * 75)

print(f"New candidates:       {new_candidates:,}")
print(f"New true pairs:       {new_true_pairs:,}")
print(f"Candidates/new pair:  {cand_per_new_pair:.2f}")
print(f"Combined candidates:  {combined_candidates:,}")
print(f"Combined true pairs:  {combined_found:,}")
print(f"Combined recall:      {combined_recall * 100:.2f}%")
print(f"Remaining missed:     {remaining_missed:,}")


# ============================================================
# 15. SAVE R11 CANDIDATES
#
# IMPORTANT:
# SAVE BEFORE OPTIONAL EXAMPLES
# ============================================================

print()
print("-" * 75)
print("SAVING R11 CANDIDATES")
print("-" * 75)

DB.execute(f"""
COPY r11
TO '{OUTPUT}'
(FORMAT PARQUET)
""")

print(f"Saved: {OUTPUT}")


# ============================================================
# 16. SHARED TOKEN DISTRIBUTION
# ============================================================

print()
print("-" * 75)
print("SHARED TOKEN DISTRIBUTION")
print("-" * 75)

distribution = DB.execute("""
SELECT
    shared_tokens,
    COUNT(*) AS candidate_pairs

FROM r11

GROUP BY shared_tokens

ORDER BY shared_tokens
""").fetchall()

for shared_tokens, count in distribution:
    print(
        f"{shared_tokens:>2} shared tokens : "
        f"{count:>8,} candidates"
    )


# ============================================================
# 17. RECOVERED TRUE-PAIR EXAMPLES
# ============================================================

print()
print("-" * 75)
print("RECOVERED TRUE-PAIR EXAMPLES")
print("-" * 75)


# We do NOT read BLOCKING_EVAL here because all necessary
# information is already available in the temporary views.


DB.execute("""
CREATE OR REPLACE TEMP VIEW recovered_examples AS

SELECT
    r.s1_id,
    r.candidate_id,
    r11.shared_tokens

FROM r11_new r

JOIN r11

    ON r.s1_id = r11.s1_id
   AND r.candidate_id = r11.candidate_id

JOIN true_pairs t

    ON r.s1_id = t.source1_entity_id
   AND r.candidate_id = t.candidate_entity_id

ORDER BY
    r11.shared_tokens DESC

LIMIT 20
""")


examples = DB.execute("""
SELECT
    r.s1_id,
    r.candidate_id,
    r.shared_tokens,

    s.address_norm AS s1_address,
    c.address_norm AS candidate_address

FROM recovered_examples r

LEFT JOIN s1 s
    ON r.s1_id = s.s1_id

LEFT JOIN all_sources c
    ON r.candidate_id = c.candidate_id

ORDER BY
    r.shared_tokens DESC

LIMIT 20
""").fetchall()


for row in examples:

    (
        s1_id,
        candidate_id,
        shared,
        s1_address,
        candidate_address
    ) = row

    print()
    print(f"S1:        {s1_id}")
    print(f"Candidate: {candidate_id}")
    print(f"Shared:    {shared} address tokens")
    print(f"S1 addr:   {s1_address}")
    print(f"Cand addr: {candidate_address}")


# ============================================================
# 18. FINAL
# ============================================================

print()
print("=" * 75)
print("R11 COMPLETE")
print("=" * 75)

print()
print(f"Output: {OUTPUT}")
print(f"Candidates: {candidate_count:,}")
print(f"New candidates: {new_candidates:,}")
print(f"New true pairs: {new_true_pairs:,}")
print(f"Combined recall: {combined_recall * 100:.2f}%")
print(f"Remaining missed: {remaining_missed:,}")

DB.close()