import os

EXPECTED_S1 = 1_732_544

CANDIDATE_FILE = "output/candidate_pairs.tsv"
MATCH_FILE = "output/matching_results.tsv"


def validate_file(path, expected_header, list_column_name):
    print(f"\nChecking: {path}")

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        header = f.readline().rstrip("\r\n")

        print(f"Header: {header}")

        if header != expected_header:
            print("✗ WRONG HEADER")
            return False

        rows = 0
        nonempty = 0
        bad_rows = 0
        first_bad = None
        ids = set()

        for line in f:
            line = line.rstrip("\r\n")
            rows += 1

            parts = line.split("\t", 1)

            if len(parts) != 2:
                bad_rows += 1
                if first_bad is None:
                    first_bad = line
                continue

            entity_id, values = parts

            if not entity_id.startswith("S1-"):
                bad_rows += 1
                if first_bad is None:
                    first_bad = line

            if entity_id in ids:
                bad_rows += 1
                if first_bad is None:
                    first_bad = line
            else:
                ids.add(entity_id)

            if values != "":
                nonempty += 1

        print(f"Rows              : {rows:,}")
        print(f"Unique S1 IDs     : {len(ids):,}")
        print(f"Non-empty lists   : {nonempty:,}")
        print(f"Bad rows          : {bad_rows:,}")
        print(f"File size         : {os.path.getsize(path) / (1024 * 1024):.2f} MB")

        if first_bad:
            print(f"First bad row     : {first_bad[:300]}")

        return (
            rows == EXPECTED_S1
            and len(ids) == EXPECTED_S1
            and bad_rows == 0
        )


print("=" * 70)
print("FINAL SUBMISSION VALIDATION")
print("=" * 70)

candidate_ok = validate_file(
    CANDIDATE_FILE,
    "source1_entity_id\tcandidate_entity_ids",
    "candidate_entity_ids"
)

match_ok = validate_file(
    MATCH_FILE,
    "source1_entity_id\tmatched_entity_ids",
    "matched_entity_ids"
)

print("\n" + "=" * 70)

if candidate_ok and match_ok:
    print("✓ BOTH SUBMISSION FILES PASSED BASIC VALIDATION")
else:
    print("✗ VALIDATION FAILED")

print("=" * 70)