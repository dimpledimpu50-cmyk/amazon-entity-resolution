import duckdb
import lightgbm as lgb
import pandas as pd
import time
import gc

FEATURES = "intermediate/train_r15_similarity_features.parquet"
GT = "dataset/train/train_ground_truth.tsv"
MODEL = "models/lightgbm_similarity.txt"

THRESHOLD = 0.76
BATCH_SIZE = 250_000

FEATURE_COLUMNS = [
    "country_equal",
    "exact_name",
    "exact_compact_name",
    "name_prefix4",
    "name_prefix5",
    "name_suffix4",
    "name_contains",
    "name_length_diff",
    "exact_address",
    "exact_compact_address",
    "address_prefix5",
    "same_house",
    "address_length_diff",
    "prefix4_same_house",
    "prefix4_address_prefix",
    "exact_name_same_house",
    "exact_name_exact_address",
    "name_jaro",
    "name_levenshtein",
    "name_length_ratio",
    "address_jaro",
    "address_levenshtein",
    "address_length_ratio",
]

start = time.time()

print("=" * 90)
print("MEMORY-SAFE R15 LIGHTGBM EVALUATION")
print("=" * 90)

print("\nLoading model...")

model = lgb.Booster(model_file=MODEL)

print(f"Model: {MODEL}")
print(f"Threshold: {THRESHOLD}")
print(f"Batch size: {BATCH_SIZE:,}")

con = duckdb.connect()
con.execute("SET memory_limit='4GB'")
con.execute("SET threads=2")
con.execute("SET preserve_insertion_order=false")

print("\nCounting feature rows...")

total_rows = con.execute(f"""
SELECT COUNT(*)
FROM read_parquet('{FEATURES}')
""").fetchone()[0]

print(f"Total rows: {total_rows:,}")

print("\nLoading ground truth...")

gt = con.execute(f"""
SELECT DISTINCT
    source1_entity_id,
    UNNEST(string_split(matched_entity_ids, ',')) AS candidate_entity_id
FROM read_csv(
    '{GT}',
    delim='\\t',
    header=true,
    columns={{
        'source1_entity_id':'VARCHAR',
        'matched_entity_ids':'VARCHAR'
    }}
)
WHERE matched_entity_ids IS NOT NULL
  AND matched_entity_ids <> ''
""").df()

gt["_key"] = (
    gt["source1_entity_id"].astype(str)
    + "|"
    + gt["candidate_entity_id"].astype(str)
)

gt_keys = set(gt["_key"])

del gt
gc.collect()

print(f"Ground-truth pairs: {len(gt_keys):,}")

print("\nStarting batched prediction...")

tp = 0
fp = 0
predicted_total = 0

offset = 0
batch_number = 0

while offset < total_rows:

    batch_number += 1

    query = f"""
    SELECT
        source1_entity_id,
        candidate_entity_id,
        {", ".join(FEATURE_COLUMNS)}
    FROM read_parquet('{FEATURES}')
    LIMIT {BATCH_SIZE}
    OFFSET {offset}
    """

    batch = con.execute(query).df()

    if len(batch) == 0:
        break

    ids = batch[
        ["source1_entity_id", "candidate_entity_id"]
    ].copy()

    X = batch[FEATURE_COLUMNS]

    scores = model.predict(X)

    positive_mask = scores >= THRESHOLD

    predicted = ids.loc[positive_mask].copy()

    predicted_count = len(predicted)

    if predicted_count > 0:

        predicted["_key"] = (
            predicted["source1_entity_id"].astype(str)
            + "|"
            + predicted["candidate_entity_id"].astype(str)
        )

        predicted_keys = set(predicted["_key"])

        batch_tp = len(predicted_keys & gt_keys)
        batch_fp = len(predicted_keys - gt_keys)

        tp += batch_tp
        fp += batch_fp
        predicted_total += predicted_count

    offset += len(batch)

    print(
        f"Batch {batch_number:>4} | "
        f"Processed {offset:>12,}/{total_rows:,} | "
        f"Predicted {predicted_total:>10,}"
    )

    del batch
    del ids
    del X
    del scores
    del predicted

    gc.collect()

fn = len(gt_keys) - tp

precision = (
    tp / (tp + fp)
    if (tp + fp) > 0
    else 0
)

recall = (
    tp / (tp + fn)
    if (tp + fn) > 0
    else 0
)

beta2 = 0.25

f05 = (
    (1 + beta2) * precision * recall
    /
    (beta2 * precision + recall)
    if (precision + recall) > 0
    else 0
)

print("\n" + "=" * 90)
print("R15 LIGHTGBM RESULTS")
print("=" * 90)

print(f"Threshold : {THRESHOLD}")
print(f"TP        : {tp:,}")
print(f"FP        : {fp:,}")
print(f"FN        : {fn:,}")
print(f"Precision : {precision:.4f}")
print(f"Recall    : {recall:.4f}")
print(f"F0.5      : {f05:.4f}")

print("\n" + "=" * 90)
print("COMPARISON")
print("=" * 90)

old_f05 = 0.9585

print(f"Previous F0.5 : {old_f05:.4f}")
print(f"R15 F0.5      : {f05:.4f}")
print(f"Difference    : {f05 - old_f05:+.4f}")

if f05 > old_f05:
    print("\nR15 IMPROVED F0.5")
elif f05 < old_f05:
    print("\nR15 REDUCED F0.5")
else:
    print("\nR15 DID NOT CHANGE F0.5")

print(f"\nElapsed: {(time.time() - start) / 60:.2f} minutes")
print("=" * 90)