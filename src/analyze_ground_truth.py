import duckdb
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent

GROUND_TRUTH = (
    ROOT
    / "dataset"
    / "train"
    / "train_ground_truth.tsv"
)


def main():

    con = duckdb.connect()

    print("=" * 75)
    print("GROUND TRUTH MATCH DISTRIBUTION")
    print("=" * 75)

    # --------------------------------------------------
    # Create a temporary normalized ground-truth table
    # --------------------------------------------------

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ground_truth AS

        SELECT
            source1_entity_id,
            matched_entity_ids

        FROM read_csv(
            '{GROUND_TRUTH}',
            delim='\\t',
            header=true
        )
    """)

    # --------------------------------------------------
    # Total S1 entities
    # --------------------------------------------------

    total_s1 = con.execute("""
        SELECT COUNT(*)
        FROM ground_truth
    """).fetchone()[0]

    print(f"\nTotal Source 1 entities: {total_s1:,}")

    # --------------------------------------------------
    # Unmatched S1
    # --------------------------------------------------

    unmatched = con.execute("""
        SELECT COUNT(*)
        FROM ground_truth
        WHERE matched_entity_ids IS NULL
           OR TRIM(matched_entity_ids) = ''
    """).fetchone()[0]

    print(f"Unmatched S1 entities:   {unmatched:,}")

    matched_s1 = total_s1 - unmatched

    print(f"Matched S1 entities:     {matched_s1:,}")

    print(
        f"Unmatched percentage:    "
        f"{unmatched / total_s1 * 100:.2f}%"
    )

    # --------------------------------------------------
    # Count matches per S1
    # --------------------------------------------------

    print("\nMatches per Source 1 entity:")

    distribution = con.execute("""
        SELECT
            CASE
                WHEN matched_entity_ids IS NULL
                     OR TRIM(matched_entity_ids) = ''
                    THEN '0'

                WHEN array_length(
                    string_split(matched_entity_ids, ',')
                ) = 1
                    THEN '1'

                WHEN array_length(
                    string_split(matched_entity_ids, ',')
                ) = 2
                    THEN '2'

                WHEN array_length(
                    string_split(matched_entity_ids, ',')
                ) = 3
                    THEN '3'

                WHEN array_length(
                    string_split(matched_entity_ids, ',')
                ) = 4
                    THEN '4'

                WHEN array_length(
                    string_split(matched_entity_ids, ',')
                ) = 5
                    THEN '5'

                ELSE '6+'
            END AS match_group,

            COUNT(*) AS s1_count

        FROM ground_truth

        GROUP BY match_group

        ORDER BY
            CASE match_group
                WHEN '0' THEN 0
                WHEN '1' THEN 1
                WHEN '2' THEN 2
                WHEN '3' THEN 3
                WHEN '4' THEN 4
                WHEN '5' THEN 5
                ELSE 6
            END
    """).fetchall()

    for group, count in distribution:
        print(f"  {group:8} {count:,}")

    # --------------------------------------------------
    # Total number of actual pairs
    # --------------------------------------------------

    total_pairs = con.execute("""
        SELECT
            SUM(
                CASE
                    WHEN matched_entity_ids IS NULL
                      OR TRIM(matched_entity_ids) = ''
                    THEN 0

                    ELSE array_length(
                        string_split(matched_entity_ids, ',')
                    )
                END
            )
        FROM ground_truth
    """).fetchone()[0]

    print(f"\nTotal ground-truth match pairs: {total_pairs:,}")

    # --------------------------------------------------
    # Average matches per matched S1
    # --------------------------------------------------

    average_matches = con.execute("""
        SELECT
            AVG(
                array_length(
                    string_split(matched_entity_ids, ',')
                )
            )
        FROM ground_truth
        WHERE matched_entity_ids IS NOT NULL
          AND TRIM(matched_entity_ids) <> ''
    """).fetchone()[0]

    print(
        f"Average matches per matched S1: "
        f"{average_matches:.2f}"
    )

    # --------------------------------------------------
    # S2 vs S3 matches
    # --------------------------------------------------

    source_counts = con.execute("""
        SELECT
            SUM(
                CASE
                    WHEN matched_entity_ids IS NULL
                      OR TRIM(matched_entity_ids) = ''
                    THEN 0

                    ELSE length(
                        matched_entity_ids
                    )
                    -
                    length(
                        replace(
                            matched_entity_ids,
                            'S2-',
                            ''
                        )
                    )
                END
            ) / 3 AS approximate_s2_matches,

            SUM(
                CASE
                    WHEN matched_entity_ids IS NULL
                      OR TRIM(matched_entity_ids) = ''
                    THEN 0

                    ELSE length(
                        matched_entity_ids
                    )
                    -
                    length(
                        replace(
                            matched_entity_ids,
                            'S3-',
                            ''
                        )
                    )
                END
            ) / 3 AS approximate_s3_matches

        FROM ground_truth
    """).fetchone()

    print("\nApproximate source distribution:")
    print(f"  S2 matches: {source_counts[0]:,.0f}")
    print(f"  S3 matches: {source_counts[1]:,.0f}")

    # --------------------------------------------------
    # Maximum number of matches
    # --------------------------------------------------

    maximum = con.execute("""
        SELECT MAX(
            CASE
                WHEN matched_entity_ids IS NULL
                  OR TRIM(matched_entity_ids) = ''
                THEN 0

                ELSE array_length(
                    string_split(matched_entity_ids, ',')
                )
            END
        )
        FROM ground_truth
    """).fetchone()[0]

    print(
        f"\nMaximum matches for one S1: {maximum}"
    )

    con.close()


if __name__ == "__main__":
    main()