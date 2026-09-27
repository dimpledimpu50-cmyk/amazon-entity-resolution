import duckdb
import os
import time

DB = duckdb.connect()

# Give DuckDB reasonable memory limits.
# It will spill large operations to disk instead of trying to keep everything
# in Python RAM.
DB.execute("SET memory_limit='8GB'")
DB.execute("SET threads=4")
DB.execute("SET preserve_insertion_order=false")

OUT_DIR = "intermediate/train_candidates"
os.makedirs(OUT_DIR, exist_ok=True)

S1 = "parquet/train_source1_normalized.parquet"
S2 = "parquet/train_source2_normalized.parquet"
S3 = "parquet/train_source3_normalized.parquet"

print("=" * 80)
print("BUILD FULL TRAINING CANDIDATE SET")
print("=" * 80)

start = time.time()


# ============================================================================
# 1. Source 1
# ============================================================================

print("\nLoading Source 1...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW s1 AS
SELECT
    entity_id AS source1_entity_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact,

    -- V4 legal suffix normalization
    regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            lower(name_norm),
                            '\\bprivate limited\\b',
                            'pvt ltd',
                            'g'
                        ),
                        '\\bprivate ltd\\b',
                        'pvt ltd',
                        'g'
                    ),
                    '\\bpvt limited\\b',
                    'pvt ltd',
                    'g'
                ),
                '\\bprivate\\b',
                'pvt',
                'g'
            ),
            '\\blimited\\b',
            'ltd',
            'g'
        ),
        '\\bincorporated\\b',
        'inc',
        'g'
    ) AS name_std

FROM read_parquet('{S1}')
""")


# ============================================================================
# 2. Source 2 + Source 3
# ============================================================================

print("Loading Source 2 + Source 3...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW source_data AS

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact,

    regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            lower(name_norm),
                            '\\bprivate limited\\b',
                            'pvt ltd',
                            'g'
                        ),
                        '\\bprivate ltd\\b',
                        'pvt ltd',
                        'g'
                    ),
                    '\\bpvt limited\\b',
                    'pvt ltd',
                    'g'
                ),
                '\\bprivate\\b',
                'pvt',
                'g'
            ),
            '\\blimited\\b',
            'ltd',
            'g'
        ),
        '\\bincorporated\\b',
        'inc',
        'g'
    ) AS name_std

FROM read_parquet('{S2}')

UNION ALL

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    name_norm,
    name_compact,
    address_norm,
    address_compact,

    regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            lower(name_norm),
                            '\\bprivate limited\\b',
                            'pvt ltd',
                            'g'
                        ),
                        '\\bprivate ltd\\b',
                        'pvt ltd',
                        'g'
                    ),
                    '\\bpvt limited\\b',
                    'pvt ltd',
                    'g'
                ),
                '\\bprivate\\b',
                'pvt',
                'g'
            ),
            '\\blimited\\b',
            'ltd',
            'g'
        ),
        '\\bincorporated\\b',
        'inc',
        'g'
    ) AS name_std

FROM read_parquet('{S3}')
""")


# ============================================================================
# 3. Prepare blocker keys
# ============================================================================

print("Preparing blocker keys...")

DB.execute("""
CREATE OR REPLACE TEMP VIEW s1_keys AS
SELECT
    *,
    regexp_replace(name_std, '[^a-z0-9]', '', 'g') AS name_std_compact,

    regexp_extract(
        address_norm,
        '[0-9]+'
    ) AS house_number,

    left(
        regexp_replace(name_std, '[^a-z0-9]', '', 'g'),
        4
    ) AS prefix4,

    left(address_compact, 5) AS address_prefix5
FROM s1
""")


DB.execute("""
CREATE OR REPLACE TEMP VIEW source_keys AS
SELECT
    *,
    regexp_replace(name_std, '[^a-z0-9]', '', 'g') AS name_std_compact,

    regexp_extract(
        address_norm,
        '[0-9]+'
    ) AS house_number,

    left(
        regexp_replace(name_std, '[^a-z0-9]', '', 'g'),
        4
    ) AS prefix4,

    left(address_compact, 5) AS address_prefix5
FROM source_data
""")


# ============================================================================
# 4. Candidate blocker 1 — country + exact normalized name
# ============================================================================

print("\nBlocker 1: exact normalized name...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE cand_v4_name AS
SELECT DISTINCT
    s.source1_entity_id,
    c.candidate_entity_id
FROM s1_keys s
JOIN source_keys c
    ON s.country_norm = c.country_norm
   AND s.name_std = c.name_std
WHERE s.name_std <> ''
  AND c.name_std <> ''
""")


print(
    "V4 name candidates:",
    DB.execute("SELECT COUNT(*) FROM cand_v4_name").fetchone()[0]
)


# ============================================================================
# 5. Candidate blocker 2 — compact normalized name
# ============================================================================

print("Blocker 2: compact normalized name...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE cand_v4_compact AS
SELECT DISTINCT
    s.source1_entity_id,
    c.candidate_entity_id
FROM s1_keys s
JOIN source_keys c
    ON s.country_norm = c.country_norm
   AND s.name_std_compact = c.name_std_compact
WHERE s.name_std_compact <> ''
  AND c.name_std_compact <> ''
""")


# ============================================================================
# 6. Candidate blocker 3 — rare first-two-name structure
# ============================================================================

print("Blocker 3: rare name prefix...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE source_prefix_freq AS
SELECT
    country_norm,
    prefix4,
    COUNT(*) AS freq
FROM source_keys
WHERE prefix4 <> ''
GROUP BY country_norm, prefix4
HAVING COUNT(*) <= 100
""")


DB.execute("""
CREATE OR REPLACE TEMP TABLE cand_prefix AS
SELECT DISTINCT
    s.source1_entity_id,
    c.candidate_entity_id
FROM s1_keys s
JOIN source_keys c
    ON s.country_norm = c.country_norm
   AND s.prefix4 = c.prefix4
JOIN source_prefix_freq f
    ON c.country_norm = f.country_norm
   AND c.prefix4 = f.prefix4
WHERE s.prefix4 <> ''
""")


# ============================================================================
# 7. Candidate blocker 4 — exact address
# ============================================================================

print("Blocker 4: exact address...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE cand_address AS
SELECT DISTINCT
    s.source1_entity_id,
    c.candidate_entity_id
FROM s1_keys s
JOIN source_keys c
    ON s.country_norm = c.country_norm
   AND s.address_compact = c.address_compact
WHERE s.address_compact <> ''
  AND c.address_compact <> ''
""")


# ============================================================================
# 8. Candidate blocker 5 — house number + address token
# ============================================================================

print("Blocker 5: house number + address token...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE cand_house AS

WITH source_tokens AS (
    SELECT
        candidate_entity_id,
        country_norm,
        house_number,

        unnest(
            string_split(
                regexp_replace(
                    address_norm,
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                ),
                ' '
            )
        ) AS token
    FROM source_keys
),

s1_tokens AS (
    SELECT
        source1_entity_id,
        country_norm,
        house_number,

        unnest(
            string_split(
                regexp_replace(
                    address_norm,
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                ),
                ' '
            )
        ) AS token
    FROM s1_keys
),

freq AS (
    SELECT
        country_norm,
        house_number,
        token,
        COUNT(*) AS freq
    FROM source_tokens
    WHERE length(token) >= 5
      AND token !~ '^[0-9]+$'
      AND house_number <> ''
      AND token <> ''
    GROUP BY country_norm, house_number, token
    HAVING COUNT(*) <= 50
)

SELECT DISTINCT
    s.source1_entity_id,
    c.candidate_entity_id
FROM s1_tokens s
JOIN freq f
    ON s.country_norm = f.country_norm
   AND s.house_number = f.house_number
   AND s.token = f.token
JOIN source_tokens c
    ON c.country_norm = f.country_norm
   AND c.house_number = f.house_number
   AND c.token = f.token
""")


# ============================================================================
# 9. R7 — name signature
# ============================================================================

print("R7: name signature...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE cand_r7 AS

WITH source_sig AS (
    SELECT
        candidate_entity_id,
        country_norm,
        name_compact,
        left(name_compact, 4)
        || right(name_compact, 4)
        || CAST(length(name_compact) AS VARCHAR) AS sig
    FROM source_keys
    WHERE length(name_compact) >= 5
),

s1_sig AS (
    SELECT
        source1_entity_id,
        country_norm,
        name_compact,
        left(name_compact, 4)
        || right(name_compact, 4)
        || CAST(length(name_compact) AS VARCHAR) AS sig
    FROM s1_keys
    WHERE length(name_compact) >= 5
),

freq AS (
    SELECT
        country_norm,
        sig,
        COUNT(*) AS freq
    FROM source_sig
    GROUP BY country_norm, sig
    HAVING COUNT(*) <= 25
)

SELECT DISTINCT
    s.source1_entity_id,
    c.candidate_entity_id
FROM s1_sig s
JOIN freq f
    ON s.country_norm = f.country_norm
   AND s.sig = f.sig
JOIN source_sig c
    ON c.country_norm = s.country_norm
   AND c.sig = s.sig
""")


# ============================================================================
# 10. R10 — sorted informative name tokens
# ============================================================================

print("R10: sorted name tokens...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE cand_r10 AS

WITH source_tokens AS (

    SELECT
        candidate_entity_id,
        country_norm,

        array_to_string(
            list_sort(
                list(
                    DISTINCT token
                )
            ),
            ' '
        ) AS sorted_tokens

    FROM (

        SELECT
            candidate_entity_id,
            country_norm,

            unnest(
                string_split(
                    regexp_replace(
                        lower(name_norm),
                        '[^a-z0-9]+',
                        ' ',
                        'g'
                    ),
                    ' '
                )
            ) AS token

        FROM source_keys
    )

    WHERE length(token) >= 3
      AND length(token) < 50
      AND token NOT IN (
          'private',
          'limited',
          'ltd',
          'pvt',
          'pvtltd',
          'company',
          'co',
          'inc',
          'incorporated',
          'corporation',
          'corp',
          'llc',
          'llp',
          'services',
          'service',
          'center',
          'centre',
          'group',
          'associates',
          'partners',
          'holdings',
          'international',
          'industries',
          'industry',
          'enterprises',
          'enterprise',
          'solutions',
          'technologies',
          'technology',
          'trading',
          'products',
          'product',
          'management',
          'foundation',
          'clinic',
          'medical',
          'healthcare',
          'health',
          'systems',
          'system',
          'consulting',
          'consultants'
      )

    GROUP BY candidate_entity_id, country_norm
),

s1_tokens AS (

    SELECT
        source1_entity_id,
        country_norm,

        array_to_string(
            list_sort(
                list(
                    DISTINCT token
                )
            ),
            ' '
        ) AS sorted_tokens

    FROM (

        SELECT
            source1_entity_id,
            country_norm,

            unnest(
                string_split(
                    regexp_replace(
                        lower(name_norm),
                        '[^a-z0-9]+',
                        ' ',
                        'g'
                    ),
                    ' '
                )
            ) AS token

        FROM s1_keys
    )

    WHERE length(token) >= 3
      AND length(token) < 50
      AND token NOT IN (
          'private',
          'limited',
          'ltd',
          'pvt',
          'pvtltd',
          'company',
          'co',
          'inc',
          'incorporated',
          'corporation',
          'corp',
          'llc',
          'llp',
          'services',
          'service',
          'center',
          'centre',
          'group',
          'associates',
          'partners',
          'holdings',
          'international',
          'industries',
          'industry',
          'enterprises',
          'enterprise',
          'solutions',
          'technologies',
          'technology',
          'trading',
          'products',
          'product',
          'management',
          'foundation',
          'clinic',
          'medical',
          'healthcare',
          'health',
          'systems',
          'system',
          'consulting',
          'consultants'
      )

    GROUP BY source1_entity_id, country_norm
),

freq AS (
    SELECT
        country_norm,
        sorted_tokens,
        COUNT(*) AS freq
    FROM source_tokens
    WHERE sorted_tokens <> ''
    GROUP BY country_norm, sorted_tokens
    HAVING COUNT(*) <= 50
)

SELECT DISTINCT
    s.source1_entity_id,
    c.candidate_entity_id
FROM s1_tokens s
JOIN freq f
    ON s.country_norm = f.country_norm
   AND s.sorted_tokens = f.sorted_tokens
JOIN source_tokens c
    ON c.country_norm = s.country_norm
   AND c.sorted_tokens = s.sorted_tokens
""")


# ============================================================================
# 11. R11 — address token overlap
# ============================================================================

print("R11: rare address token overlap...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE cand_r11 AS

WITH source_tokens AS (

    SELECT
        candidate_entity_id,
        country_norm,
        house_number,
        token

    FROM (

        SELECT
            candidate_entity_id,
            country_norm,

            regexp_extract(
                address_norm,
                '[0-9]+'
            ) AS house_number,

            unnest(
                string_split(
                    regexp_replace(
                        lower(address_norm),
                        '[^a-z0-9]+',
                        ' ',
                        'g'
                    ),
                    ' '
                )
            ) AS token

        FROM source_keys
    )

    WHERE length(token) >= 5
      AND token !~ '^[0-9]+$'
      AND token NOT IN (
          'private',
          'limited',
          'company',
          'street',
          'road',
          'floor',
          'district',
          'india',
          'delhi',
          'mumbai',
          'bengaluru',
          'bangalore'
      )
),

freq AS (
    SELECT
        country_norm,
        token,
        COUNT(*) AS freq
    FROM source_tokens
    GROUP BY country_norm, token
    HAVING COUNT(*) <= 50
),

s1_tokens AS (

    SELECT
        source1_entity_id,
        country_norm,
        token

    FROM (

        SELECT
            source1_entity_id,
            country_norm,

            unnest(
                string_split(
                    regexp_replace(
                        lower(address_norm),
                        '[^a-z0-9]+',
                        ' ',
                        'g'
                    ),
                    ' '
                )
            ) AS token

        FROM s1_keys
    )

    WHERE length(token) >= 5
      AND token !~ '^[0-9]+$'
),

shared AS (

    SELECT
        s.source1_entity_id,
        c.candidate_entity_id,
        COUNT(DISTINCT s.token) AS shared_tokens

    FROM s1_tokens s

    JOIN freq f
        ON s.country_norm = f.country_norm
       AND s.token = f.token

    JOIN source_tokens c
        ON c.country_norm = s.country_norm
       AND c.token = s.token

    GROUP BY
        s.source1_entity_id,
        c.candidate_entity_id

    HAVING COUNT(DISTINCT s.token) >= 2
)

SELECT
    source1_entity_id,
    candidate_entity_id
FROM shared
""")


# ============================================================================
# 12. R13 — prefix4 + house/address + edit distance
# ============================================================================

print("R13: selective fuzzy blocker...")

DB.execute("""
CREATE OR REPLACE TEMP TABLE cand_r13 AS

WITH structural AS (

    SELECT DISTINCT
        s.source1_entity_id,
        c.candidate_entity_id,
        s.name_compact AS s_name,
        c.name_compact AS c_name

    FROM s1_keys s

    JOIN source_keys c

        ON s.country_norm = c.country_norm

       AND left(s.name_compact, 4)
           = left(c.name_compact, 4)

       AND left(s.name_compact, 4) <> ''

       AND (
            (
                TRY_CAST(s.house_number AS BIGINT)
                IS NOT NULL

                AND

                TRY_CAST(c.house_number AS BIGINT)
                IS NOT NULL

                AND

                TRY_CAST(s.house_number AS BIGINT)
                =
                TRY_CAST(c.house_number AS BIGINT)
            )

            OR

            (
                s.address_prefix5 <> ''
                AND c.address_prefix5 <> ''
                AND s.address_prefix5 = c.address_prefix5
            )
       )
)

SELECT
    source1_entity_id,
    candidate_entity_id

FROM structural

WHERE editdist3(
    s_name,
    c_name
) <= 3
""")


# ============================================================================
# 13. Write each blocker separately
#
# Keeping them separate prevents one huge UNION DISTINCT from consuming
# excessive memory.
# ============================================================================

print("\nWriting blocker files...")

blockers = {
    "v4_name": "cand_v4_name",
    "v4_compact": "cand_v4_compact",
    "v4_prefix": "cand_prefix",
    "v4_address": "cand_address",
    "v4_house": "cand_house",
    "r7": "cand_r7",
    "r10": "cand_r10",
    "r11": "cand_r11",
    "r13": "cand_r13",
}

for name, view in blockers.items():

    path = f"{OUT_DIR}/{name}.parquet"

    print(f"Writing {name}...")

    DB.execute(f"""
        COPY {view}
        TO '{path}'
        (FORMAT PARQUET)
    """)

    count = DB.execute(
        f"SELECT COUNT(*) FROM {view}"
    ).fetchone()[0]

    print(f"  {count:,} candidates")


# ============================================================================
# 14. Build union using DuckDB directly on Parquet
# ============================================================================

print("\nBuilding unique training candidate union...")

UNION_OUT = "intermediate/train_candidate_pairs.parquet"

DB.execute(f"""
COPY (

    SELECT
        source1_entity_id,
        candidate_entity_id
    FROM read_parquet('{OUT_DIR}/*.parquet')
    GROUP BY
        source1_entity_id,
        candidate_entity_id

)
TO '{UNION_OUT}'
(FORMAT PARQUET)
""")


final_count = DB.execute(f"""
SELECT COUNT(*)
FROM read_parquet('{UNION_OUT}')
""").fetchone()[0]


s1_count = DB.execute(f"""
SELECT COUNT(DISTINCT source1_entity_id)
FROM read_parquet('{UNION_OUT}')
""").fetchone()[0]


print()
print("=" * 80)
print("TRAINING CANDIDATE SET COMPLETE")
print("=" * 80)

print(f"Unique candidate pairs : {final_count:,}")
print(f"S1 entities covered    : {s1_count:,}")
print(f"Output                 : {UNION_OUT}")

print()
print(f"Elapsed time: {(time.time() - start) / 60:.2f} minutes")
print()
print("IMPORTANT:")
print("The blocker files are retained separately.")
print("Do NOT delete them yet.")