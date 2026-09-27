import duckdb

DB = duckdb.connect()

REMAINING = "intermediate/remaining_missed_pairs_r7_r10.parquet"
SOURCE2 = "parquet/train_source2_normalized.parquet"
SOURCE3 = "parquet/train_source3_normalized.parquet"


# ============================================================
# Generic / non-informative address words
# ============================================================

GENERIC = {
    "road", "rd",
    "street", "st",
    "avenue", "ave",
    "lane", "ln",
    "drive", "dr",
    "boulevard", "blvd",
    "park",
    "place", "pl",
    "floor", "fl",
    "building", "bldg",
    "unit",
    "suite", "ste",
    "office",
    "no",
    "number",
    "plot",
    "block",
    "ground",
    "near",
    "opp",
    "opposite",
    "city",
    "state",
    "county",
    "district",
    "north",
    "south",
    "east",
    "west",
    "central",
    "india",
    "delhi",
    "mumbai",
    "pune",
    "bangalore",
    "bengaluru",
    "maharashtra",
    "karnataka",
    "california",
    "texas",
    "new",
    "york",
    "private",
    "limited",
    "ltd",
    "pvt",
    "llc",
    "inc",
    "company",
}

GENERIC_SQL = ",".join(
    f"'{x}'" for x in sorted(GENERIC)
)


print("=" * 75)
print("REMAINING MISSED PAIRS — ADDRESS TOKEN ANALYSIS")
print("=" * 75)


# ============================================================
# 1. Load remaining missed true pairs
# ============================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW remaining AS
SELECT *
FROM read_parquet('{REMAINING}')
""")

remaining_count = DB.execute("""
SELECT COUNT(*)
FROM remaining
""").fetchone()[0]

print(f"\nRemaining missed pairs: {remaining_count:,}")


# ============================================================
# 2. Load Source 2 + Source 3
# ============================================================

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW all_sources AS

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    address_norm
FROM read_parquet('{SOURCE2}')

UNION ALL

SELECT
    entity_id AS candidate_entity_id,
    country_norm,
    address_norm
FROM read_parquet('{SOURCE3}')
""")


# ============================================================
# 3. Tokenize source addresses
# ============================================================

print("\nBuilding source address-token index...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW source_address_tokens AS
SELECT DISTINCT
    candidate_entity_id,
    country_norm,
    token
FROM (
    SELECT
        candidate_entity_id,
        country_norm,
        unnest(
            string_split(
                regexp_replace(
                    lower(coalesce(address_norm, '')),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                ),
                ' '
            )
        ) AS token
    FROM all_sources
)
WHERE length(token) >= 4
  AND NOT regexp_matches(token, '^[0-9]+$')
  AND token NOT IN ({GENERIC_SQL})
""")


# ============================================================
# 4. Source token frequencies
# ============================================================

print("Calculating source token frequencies...")

DB.execute("""
CREATE OR REPLACE TEMP VIEW source_token_frequency AS
SELECT
    country_norm,
    token,
    COUNT(*) AS freq
FROM source_address_tokens
GROUP BY
    country_norm,
    token
""")


# ============================================================
# 5. Tokenize S1 addresses from remaining missed pairs
# ============================================================

print("Tokenizing remaining S1 addresses...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW remaining_s1_tokens AS
SELECT DISTINCT
    r.source1_entity_id,
    r.candidate_entity_id,
    r.country,
    t.token
FROM (
    SELECT
        source1_entity_id,
        candidate_entity_id,
        country,
        regexp_replace(
            lower(coalesce(s1_address, '')),
            '[^a-z0-9]+',
            ' ',
            'g'
        ) AS clean_address
    FROM remaining
) r
CROSS JOIN UNNEST(
    string_split(r.clean_address, ' ')
) AS t(token)
WHERE length(t.token) >= 4
  AND NOT regexp_matches(t.token, '^[0-9]+$')
  AND t.token NOT IN ({GENERIC_SQL})
""")


# ============================================================
# 6. Rare-token coverage
# ============================================================

print("\nRare address-token coverage:")

for threshold in [1, 5, 10, 25, 50, 100, 200]:

    count = DB.execute(f"""
        SELECT COUNT(DISTINCT
            r.source1_entity_id || '|' || r.candidate_entity_id
        )
        FROM remaining_s1_tokens r

        JOIN source_token_frequency f
          ON r.country = f.country_norm
         AND r.token = f.token

        WHERE f.freq <= {threshold}
    """).fetchone()[0]

    print(
        f"Rare address token <= {threshold:3}: "
        f"{count:,} "
        f"({count / remaining_count * 100:.2f}%)"
    )


# ============================================================
# 7. Prepare address token lists for true remaining pairs
# ============================================================

print("\nBuilding address token lists...")

DB.execute(f"""
CREATE OR REPLACE TEMP VIEW pair_token_lists AS
SELECT
    source1_entity_id,
    candidate_entity_id,
    country,

    list_filter(
        string_split(
            regexp_replace(
                lower(coalesce(s1_address, '')),
                '[^a-z0-9]+',
                ' ',
                'g'
            ),
            ' '
        ),
        x ->
            length(x) >= 4
            AND NOT regexp_matches(x, '^[0-9]+$')
            AND x NOT IN ({GENERIC_SQL})
    ) AS s1_tokens,

    list_filter(
        string_split(
            regexp_replace(
                lower(coalesce(candidate_address, '')),
                '[^a-z0-9]+',
                ' ',
                'g'
            ),
            ' '
        ),
        x ->
            length(x) >= 4
            AND NOT regexp_matches(x, '^[0-9]+$')
            AND x NOT IN ({GENERIC_SQL})
    ) AS candidate_tokens

FROM remaining
""")


# ============================================================
# 8. Count shared useful address tokens
# ============================================================

DB.execute("""
CREATE OR REPLACE TEMP VIEW address_overlap AS
SELECT
    source1_entity_id,
    candidate_entity_id,
    country,

    list_count(
        list_intersect(
            s1_tokens,
            candidate_tokens
        )
    ) AS shared_count

FROM pair_token_lists
""")


# ============================================================
# 9. Shared-token statistics
# ============================================================

print("\nShared useful address tokens:")

for n in [1, 2, 3, 4]:

    count = DB.execute(f"""
        SELECT COUNT(*)
        FROM address_overlap
        WHERE shared_count >= {n}
    """).fetchone()[0]

    print(
        f"Pairs sharing >= {n} useful address tokens: "
        f"{count:,} "
        f"({count / remaining_count * 100:.2f}%)"
    )


# ============================================================
# 10. Rare shared address-token statistics
# ============================================================

print("\nShared address token + source rarity:")

for threshold in [5, 10, 25, 50, 100]:

    count = DB.execute(f"""
        SELECT COUNT(DISTINCT
            o.source1_entity_id || '|' || o.candidate_entity_id
        )

        FROM address_overlap o

        JOIN remaining_s1_tokens r
          ON o.source1_entity_id = r.source1_entity_id
         AND o.candidate_entity_id = r.candidate_entity_id
         AND o.country = r.country

        JOIN source_token_frequency f
          ON r.country = f.country_norm
         AND r.token = f.token

        WHERE o.shared_count >= 2
          AND f.freq <= {threshold}
    """).fetchone()[0]

    print(
        f">=2 shared tokens + one token freq <= {threshold:3}: "
        f"{count:,} "
        f"({count / remaining_count * 100:.2f}%)"
    )


# ============================================================
# 11. Stronger condition:
#     >=2 shared useful tokens AND
#     at least one rare token
# ============================================================

print("\nStronger address conditions:")

for threshold in [10, 25, 50, 100]:

    count = DB.execute(f"""
        SELECT COUNT(DISTINCT
            o.source1_entity_id || '|' || o.candidate_entity_id
        )

        FROM address_overlap o

        JOIN remaining_s1_tokens r
          ON o.source1_entity_id = r.source1_entity_id
         AND o.candidate_entity_id = r.candidate_entity_id
         AND o.country = r.country

        JOIN source_token_frequency f
          ON r.country = f.country_norm
         AND r.token = f.token

        WHERE o.shared_count >= 2
          AND f.freq <= {threshold}
    """).fetchone()[0]

    print(
        f">=2 shared + rare token <= {threshold:3}: "
        f"{count:,} "
        f"({count / remaining_count * 100:.2f}%)"
    )


# ============================================================
# 12. Examples
# ============================================================

print("\nExamples with >=2 useful shared address tokens:\n")

examples = DB.execute("""
SELECT
    r.source1_entity_id,
    r.candidate_entity_id,
    r.s1_name,
    r.candidate_name,
    r.s1_address,
    r.candidate_address,
    o.shared_count

FROM remaining r

JOIN address_overlap o
  ON r.source1_entity_id = o.source1_entity_id
 AND r.candidate_entity_id = o.candidate_entity_id

WHERE o.shared_count >= 2

ORDER BY
    o.shared_count DESC

LIMIT 30
""").fetchall()


for row in examples:

    (
        s1_id,
        candidate_id,
        s1_name,
        candidate_name,
        s1_address,
        candidate_address,
        shared_count,
    ) = row

    print("-" * 75)

    print(f"S1:        {s1_id}")
    print(f"Candidate: {candidate_id}")
    print(f"Shared:    {shared_count}")

    print(f"Name S1:   {s1_name}")
    print(f"Name C:    {candidate_name}")

    print(f"Addr S1:   {s1_address}")
    print(f"Addr C:    {candidate_address}")


print("\n" + "=" * 75)
print("ANALYSIS COMPLETE")
print("=" * 75)