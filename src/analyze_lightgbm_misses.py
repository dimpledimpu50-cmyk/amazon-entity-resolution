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
print("ANALYZING LIGHTGBM FALSE NEGATIVES")
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

# EXACT SAME SPLIT USED DURING TRAINING/EVALUATION
rng = np.random.default_rng(42)

indices = np.arange(len(y))
rng.shuffle(indices)

split = int(len(indices) * 0.80)
valid_idx = indices[split:]

X_valid = X.iloc[valid_idx].reset_index(drop=True)
y_valid = y[valid_idx]

print(f"Validation rows: {len(y_valid):,}")

print("\nLoading LightGBM model...")

model = lgb.Booster(model_file=MODEL_FILE)

print("Predicting scores...")

scores = model.predict(X_valid)

# Best threshold found previously
THRESHOLD = 0.73

pred = scores >= THRESHOLD

# False negatives = actual match but predicted non-match
fn = (y_valid == 1) & (pred == 0)

print("\n" + "=" * 80)
print("FALSE NEGATIVE SUMMARY")
print("=" * 80)

print(f"False negatives: {int(fn.sum()):,}")

print("\nFeature presence among false negatives:")
print("-" * 80)

for feature in features:
    count = int(X_valid.loc[fn, feature].sum())
    percentage = count / int(fn.sum()) * 100

    print(
        f"{feature:28s} "
        f"{count:10,} "
        f"({percentage:6.2f}%)"
    )

print("\n" + "=" * 80)
print("FALSE NEGATIVE SCORE DISTRIBUTION")
print("=" * 80)

fn_scores = scores[fn]

print(f"Minimum score : {fn_scores.min():.6f}")
print(f"Maximum score : {fn_scores.max():.6f}")
print(f"Mean score    : {fn_scores.mean():.6f}")
print(f"Median score  : {np.median(fn_scores):.6f}")

print("\nScore ranges:")

ranges = [
    (0.00, 0.10),
    (0.10, 0.20),
    (0.20, 0.30),
    (0.30, 0.40),
    (0.40, 0.50),
    (0.50, 0.60),
    (0.60, 0.70),
    (0.70, 0.73),
]

for low, high in ranges:
    count = int(np.sum((fn_scores >= low) & (fn_scores < high)))

    print(
        f"{low:.2f} - {high:.2f}: "
        f"{count:,}"
    )

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)