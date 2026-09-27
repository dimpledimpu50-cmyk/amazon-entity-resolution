import duckdb
import lightgbm as lgb
import numpy as np

SAMPLE_FILE = "intermediate/ml_training_sample.parquet"
MODEL_FILE = "models/lightgbm_matcher.txt"

DB_MEMORY = "6GB"
THREADS = 2

features = [
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
]

print("=" * 80)
print("EVALUATING LIGHTGBM MATCHER")
print("=" * 80)

con = duckdb.connect()
con.execute(f"SET memory_limit='{DB_MEMORY}'")
con.execute(f"SET threads={THREADS}")
con.execute("SET preserve_insertion_order=false")

print("\nLoading validation data...")

df = con.execute(f"""
    SELECT
        label,
        {", ".join(features)}
    FROM read_parquet('{SAMPLE_FILE}')
""").fetchdf()

print(f"Rows loaded: {len(df):,}")

y = df["label"].astype(np.int8).to_numpy()
X = df[features]

# Same deterministic split as training
rng = np.random.default_rng(42)

indices = np.arange(len(y))
rng.shuffle(indices)

split = int(len(indices) * 0.80)

valid_idx = indices[split:]

X_valid = X.iloc[valid_idx]
y_valid = y[valid_idx]

print(f"Validation rows: {len(y_valid):,}")

print("\nLoading model...")

model = lgb.Booster(model_file=MODEL_FILE)

print("Predicting...")

scores = model.predict(X_valid)

print("Prediction complete.")

def f05(precision, recall):
    if precision == 0 and recall == 0:
        return 0.0

    return (
        1.25 * precision * recall
        / (0.25 * precision + recall)
    )

print("\n" + "=" * 80)
print("THRESHOLD ANALYSIS")
print("=" * 80)

best = None

thresholds = np.arange(0.10, 1.001, 0.01)

for threshold in thresholds:

    pred = scores >= threshold

    tp = int(np.sum(pred & (y_valid == 1)))
    fp = int(np.sum(pred & (y_valid == 0)))
    fn = int(np.sum(~pred & (y_valid == 1)))

    precision = tp / (tp + fp) if (tp + fp) else 0
    recall = tp / (tp + fn) if (tp + fn) else 0

    score = f05(precision, recall)

    if best is None or score > best["f05"]:
        best = {
            "threshold": threshold,
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "precision": precision,
            "recall": recall,
            "f05": score
        }

    if threshold in [0.50, 0.60, 0.70, 0.80, 0.85, 0.90, 0.95]:
        print(
            f"Threshold {threshold:.2f} | "
            f"Precision {precision:.4f} | "
            f"Recall {recall:.4f} | "
            f"F0.5 {score:.4f} | "
            f"TP {tp:,} | FP {fp:,}"
        )

print("\n" + "=" * 80)
print("BEST THRESHOLD")
print("=" * 80)

print(f"Threshold : {best['threshold']:.2f}")
print(f"TP        : {best['tp']:,}")
print(f"FP        : {best['fp']:,}")
print(f"FN        : {best['fn']:,}")
print(f"Precision : {best['precision']:.4f}")
print(f"Recall    : {best['recall']:.4f}")
print(f"F0.5      : {best['f05']:.4f}")

print("=" * 80)