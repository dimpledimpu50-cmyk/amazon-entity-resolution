from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parent.parent
PARQUET_DIR = ROOT / "parquet"

S1 = PARQUET_DIR / "train_source1_normalized.parquet"
S2 = PARQUET_DIR / "train_source2_normalized.parquet"
S3 = PARQUET_DIR / "train_source3_normalized.parquet"
GT = ROOT / "dataset" / "train" / "train_ground_truth.tsv"

con = duckdb.connect()
con.execute("SET threads TO 4")


# ------------------------------------------------------------
# Same 10,000 S1 sample
# ------------------------------------------------------------

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE sample_s1 AS
    SELECT *
    FROM read_parquet('{S1}')
    ORDER BY entity_id
    LIMIT 10000
""")


# ------------------------------------------------------------
# Ground truth
# ------------------------------------------------------------

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE ground_truth AS
    SELECT source1_entity_id, matched_entity_ids
    FROM read_csv(
        '{GT}',
        delim='\\t',
        header=true,
        auto_detect=true
    )
    WHERE source1_entity_id IN (
        SELECT entity_id FROM sample_s1
    )
""")


con.execute("""
    CREATE OR REPLACE TEMP TABLE true_pairs AS
    SELECT
        g.source1_entity_id,
        TRIM(x) AS candidate_entity_id
    FROM ground_truth g,
    UNNEST(
        CASE
            WHEN g.matched_entity_ids IS NULL
              OR TRIM(g.matched_entity_ids) = ''
            THEN []
            ELSE STRING_SPLIT(g.matched_entity_ids, ',')
        END
    ) AS t(x)
""")


# ------------------------------------------------------------
# Source 2 + Source 3
# ------------------------------------------------------------

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE sources AS

    SELECT
        entity_id,
        'S2' AS source,
        country_norm,
        name_norm,
        address_norm
    FROM read_parquet('{S2}')

    UNION ALL

    SELECT
        entity_id,
        'S3' AS source,
        country_norm,
        name_norm,
        address_norm
    FROM read_parquet('{S3}')
""")


# ------------------------------------------------------------
# Recreate V3 blocking keys
# ------------------------------------------------------------

con.execute("""
    CREATE OR REPLACE TEMP TABLE s1_keys AS
    SELECT
        entity_id,
        country_norm,

        name_norm AS name_key,

        regexp_replace(
            name_norm,
            '\\s+',
            '',
            'g'
        ) AS name_compact_key,

        LEFT(
            regexp_replace(
                name_norm,
                '[^a-z0-9]',
                '',
                'g'
            ),
            4
        ) AS name_prefix4,

        SPLIT_PART(
            name_norm,
            ' ',
            1
        ) AS name_first_token,

        address_norm AS address_key,

        regexp_replace(
            address_norm,
            '\\s+',
            '',
            'g'
        ) AS address_compact_key,

        SPLIT_PART(
            address_norm,
            ' ',
            1
        ) AS address_first_token

    FROM sample_s1
""")


con.execute("""
    CREATE OR REPLACE TEMP TABLE source_keys AS
    SELECT
        entity_id,
        country_norm,

        name_norm AS name_key,

        regexp_replace(
            name_norm,
            '\\s+',
            '',
            'g'
        ) AS name_compact_key,

        LEFT(
            regexp_replace(
                name_norm,
                '[^a-z0-9]',
                '',
                'g'
            ),
            4
        ) AS name_prefix4,

        SPLIT_PART(
            name_norm,
            ' ',
            1
        ) AS name_first_token,

        address_norm AS address_key,

        regexp_replace(
            address_norm,
            '\\s+',
            '',
            'g'
        ) AS address_compact_key,

        SPLIT_PART(
            address_norm,
            ' ',
            1
        ) AS address_first_token

    FROM sources
""")


# ------------------------------------------------------------
# Frequencies
# ------------------------------------------------------------

con.execute("""
    CREATE OR REPLACE TEMP TABLE name_prefix_freq AS
    SELECT
        country_norm,
        name_prefix4,
        COUNT(*) AS freq
    FROM source_keys
    WHERE name_prefix4 <> ''
    GROUP BY country_norm, name_prefix4
""")


con.execute("""
    CREATE OR REPLACE TEMP TABLE name_first_freq AS
    SELECT
        country_norm,
        name_first_token,
        COUNT(*) AS freq
    FROM source_keys
    WHERE name_first_token <> ''
    GROUP BY country_norm, name_first_token
""")


con.execute("""
    CREATE OR REPLACE TEMP TABLE address_first_freq AS
    SELECT
        country_norm,
        address_first_token,
        COUNT(*) AS freq
    FROM source_keys
    WHERE address_first_token <> ''
    GROUP BY country_norm, address_first_token
""")


# ------------------------------------------------------------
# V3 candidates
# ------------------------------------------------------------

con.execute("""
    CREATE OR REPLACE TEMP TABLE candidates AS

    SELECT
        s.entity_id AS source1_entity_id,
        c.entity_id AS candidate_entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.name_key = c.name_key
     AND s.name_key <> ''

    UNION

    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.name_compact_key = c.name_compact_key
     AND s.name_compact_key <> ''

    UNION

    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.name_prefix4 = c.name_prefix4
     AND s.name_prefix4 <> ''
    JOIN name_prefix_freq f
      ON c.country_norm = f.country_norm
     AND c.name_prefix4 = f.name_prefix4
    WHERE f.freq <= 100

    UNION

    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.name_first_token = c.name_first_token
     AND s.name_first_token <> ''
    JOIN name_first_freq f
      ON c.country_norm = f.country_norm
     AND c.name_first_token = f.name_first_token
    WHERE f.freq <= 100

    UNION

    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.address_key = c.address_key
     AND s.address_key <> ''

    UNION

    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.address_compact_key = c.address_compact_key
     AND s.address_compact_key <> ''

    UNION

    SELECT
        s.entity_id,
        c.entity_id
    FROM s1_keys s
    JOIN source_keys c
      ON s.country_norm = c.country_norm
     AND s.address_first_token = c.address_first_token
     AND s.address_first_token <> ''
    JOIN address_first_freq f
      ON c.country_norm = f.country_norm
     AND c.address_first_token = f.address_first_token
    WHERE f.freq <= 100
""")


# ------------------------------------------------------------
# Find missed true pairs
# ------------------------------------------------------------

missed = con.execute("""
    SELECT
        t.source1_entity_id,
        t.candidate_entity_id
    FROM true_pairs t
    LEFT JOIN candidates c
      ON t.source1_entity_id = c.source1_entity_id
     AND t.candidate_entity_id = c.candidate_entity_id
    WHERE c.candidate_entity_id IS NULL
    LIMIT 20
""").fetchall()


# ------------------------------------------------------------
# Print actual records
# ------------------------------------------------------------

print("=" * 90)
print("INSPECTING V3 MISSED TRUE MATCHES")
print("=" * 90)

for s1_id, candidate_id in missed:

    print("\n" + "-" * 90)
    print(f"S1:       {s1_id}")
    print(f"Candidate:{candidate_id}")

    s1 = con.execute("""
        SELECT
            entity_id,
            country_norm,
            name_norm,
            address_norm
        FROM sample_s1
        WHERE entity_id = ?
    """, [s1_id]).fetchone()

    candidate = con.execute("""
        SELECT
            entity_id,
            source,
            country_norm,
            name_norm,
            address_norm
        FROM sources
        WHERE entity_id = ?
    """, [candidate_id]).fetchone()

    print("\nSOURCE 1")
    print(f"  Country : {s1[1]}")
    print(f"  Name    : {s1[2]}")
    print(f"  Address : {s1[3]}")

    print("\nMATCHED SOURCE")
    print(f"  Source  : {candidate[1]}")
    print(f"  Country : {candidate[2]}")
    print(f"  Name    : {candidate[3]}")
    print(f"  Address : {candidate[4]}")


con.close()