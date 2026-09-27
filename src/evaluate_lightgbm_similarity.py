import lightgbm as lgb
import pandas as pd
import numpy as np

INPUT_FILE = "intermediate/ml_training_similarity.parquet"
MODEL_FILE = "models/lightgbm_similarity.txt"

FEATURES = [
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

print("=" * 80)
print("EVALUATING LIGHTGBM WITH SIMILARITY FEATURES")
print("=" * 80)

print("\nLoading data...")

df = pd.read_parquet(INPUT_FILE)

print(f"Rows loaded: {len(df):,}")

X = df[FEATURES]
y = df["label"]

# IMPORTANT: exactly the same deterministic split used during training
rng = np.random.default_rng(42)

indices = np.arange(len(df))
rng.shuffle(indices)

split = int(len(indices) * 0.80)

valid_idx = indices[split:]

X_valid = X.iloc[valid_idx]
y_valid = y.iloc[valid_idx]

print(f"Validation rows: {len(valid_idx):,}")

print("\nLoading model...")

model = lgb.Booster(model_file=MODEL_FILE)

print("Generating probabilities...")

scores = model.predict(X_valid)

print("Prediction complete.")

print("\n" + "=" * 80)
print("THRESHOLD EVALUATION")
print("=" * 80)

best_f05 = 0
best_threshold = 0
best_precision = 0
best_recall = 0

results = []

for threshold in np.arange(0.50, 0.951, 0.01):

    predicted = scores >= threshold

    tp = np.sum((predicted == 1) & (y_valid.values == 1))
    fp = np.sum((predicted == 1) & (y_valid.values == 0))
    fn = np.sum((predicted == 0) & (y_valid.values == 1))

    precision = tp / (tp + fp) if (tp + fp) else 0
    recall = tp / (tp + fn) if (tp + fn) else 0

    beta = 0.5

    f05 = (
        (1 + beta ** 2)
        * precision
        * recall
        /
        (
            beta ** 2 * precision + recall
        )
        if (precision + recall) > 0
        else 0
    )

    results.append(
        (
            threshold,
            precision,
            recall,
            f05,
            tp,
            fp,
            fn
        )
    )

    if f05 > best_f05:
        best_f05 = f05
        best_threshold = threshold
        best_precision = precision
        best_recall = recall

print("\nTop threshold results:")

for row in sorted(results, key=lambda x: x[3], reverse=True)[:15]:

    threshold, precision, recall, f05, tp, fp, fn = row

    print(
        f"Threshold {threshold:.2f} | "
        f"Precision {precision:.4f} | "
        f"Recall {recall:.4f} | "
        f"F0.5 {f05:.4f} | "
        f"TP {tp:,} | "
        f"FP {fp:,} | "
        f"FN {fn:,}"
    )

print("\n" + "=" * 80)
print("BEST RESULT")
print("=" * 80)

print(f"Threshold : {best_threshold:.2f}")
print(f"Precision : {best_precision:.4f}")
print(f"Recall    : {best_recall:.4f}")
print(f"F0.5      : {best_f05:.4f}")

print("\nPrevious LightGBM:")
print("F0.5      : 0.9039")

print("\nTarget:")
print("F0.5      : >= 0.9600")