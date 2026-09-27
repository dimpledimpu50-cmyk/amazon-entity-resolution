from pathlib import Path

FILES = [
    "output/candidate_pairs.tsv",
    "output/matching_results.tsv",
]

for filename in FILES:
    src = Path(filename)
    tmp = Path(str(src) + ".tmp")

    print(f"Processing: {src}")

    fixed = 0

    with src.open("r", encoding="utf-8", newline="") as fin, \
         tmp.open("w", encoding="utf-8", newline="") as fout:

        for i, line in enumerate(fin):
            body = line.rstrip("\r\n")

            if i > 0 and body.endswith('\t""'):
                body = body[:-2]
                fixed += 1

            fout.write(body + "\n")

    tmp.replace(src)

    print(f"Fixed empty rows: {fixed:,}")

print("DONE")