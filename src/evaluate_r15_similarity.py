import duckdb
import lightgbm as lgb
import pandas as pd
import time

FEATURES = "intermediate/train_r15_similarity_features.parquet"
GT = "dataset/train/train_ground_truth.tsv"
MODEL = "models/lightgbm_similarity.txt"

THRESHOLD = 0.76

start = time.time()

print("=" * 90)
print("EVALUATING R15 WITH EXISTING LIGHTGBM SIMILARITY MODEL")
print("=" * 90)

print("\nLoading model...")

model = lgb.Booster(model_file=MODEL)

print(f"Model loaded: {MODEL}")
print(f"Threshold: {THRESHOLD}")

print("\nLoading feature data...")

con = duckdb.connect()
con.execute("SET memory_limit='6GB'")
con.execute("SET threads=2")
con.execute("SET preserve_insertion_order=false")

feature_count = con.execute(f"""
SELECT COUNT(*)
FROM read_parquet('{FEATURES}')
""").fetchone()[0]

print(f"Feature rows: {feature_count:,}")

print("\nGenerating predictions...")

feature_columns = [
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

# Load only feature columns into pandas.
df = con.execute(f"""
SELECT
    {", ".join(feature_columns)}
FROM read_parquet('{FEATURES}')
""").df()

print(f"Loaded rows: {len(df):,}")
print(f"Features: {len(feature_columns)}")

print("\nRunning LightGBM prediction...")

scores = model.predict(df)

del df

print("Predictions generated.")

print("\nLoading ground truth...")

gt = con.execute(f"""
SELECT
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

print(f"Ground-truth pairs: {len(gt):,}")

print("\nLoading candidate IDs...")

ids = con.execute(f"""
SELECT
    source1_entity_id,
    candidate_entity_id
FROM read_parquet('{FEATURES}')
""").df()

print(f"Candidate ID rows: {len(ids):,}")

print("\nApplying threshold...")

predicted = ids.loc[scores >= THRESHOLD].copy()

print(f"Predicted positive pairs: {len(predicted):,}")

print("\nEvaluating against ground truth...")

predicted["_key"] = (
    predicted["source1_entity_id"].astype(str)
    + "|"
    + predicted["candidate_entity_id"].astype(str)
)

gt["_key"] = (
    gt["source1_entity_id"].astype(str)
    + "|"
    + gt["candidate_entity_id"].astype(str)
)

gt_keys = set(gt["_key"])

predicted_keys = set(predicted["_key"])

tp = len(predicted_keys & gt_keys)
fp = len(predicted_keys - gt_keys)
fn = len(gt_keys - predicted_keys)

precision = tp / (tp + fp) if tp + fp else 0
recall = tp / (tp + fn) if tp + fn else 0

beta2 = 0.25

f05 = (
    (1 + beta2) * precision * recall
    /
    (beta2 * precision + recall)
    if precision + recall > 0
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
else:
    print("\nR15 DID NOT IMPROVE F0.5")

print(f"\nElapsed: {(time.time() - start) / 60:.2f} minutes")
print("=" * 90)