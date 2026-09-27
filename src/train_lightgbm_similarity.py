import lightgbm as lgb
import pandas as pd
import numpy as np
import os
import time

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

    # New continuous features
    "name_jaro",
    "name_levenshtein",
    "name_length_ratio",
    "address_jaro",
    "address_levenshtein",
    "address_length_ratio",
]

start = time.time()

print("=" * 80)
print("TRAINING LIGHTGBM WITH SIMILARITY FEATURES")
print("=" * 80)

print("\nLoading training data...")

df = pd.read_parquet(INPUT_FILE)

print(f"Rows loaded: {len(df):,}")
print(f"Features: {len(FEATURES):,}")

X = df[FEATURES]
y = df["label"]

print(f"Positive: {int(y.sum()):,}")
print(f"Negative: {int((y == 0).sum()):,}")

# ------------------------------------------------------------
# Deterministic 80/20 split
# ------------------------------------------------------------

rng = np.random.default_rng(42)

indices = np.arange(len(df))
rng.shuffle(indices)

split = int(len(indices) * 0.80)

train_idx = indices[:split]
valid_idx = indices[split:]

X_train = X.iloc[train_idx]
y_train = y.iloc[train_idx]

X_valid = X.iloc[valid_idx]
y_valid = y.iloc[valid_idx]

print(f"\nTraining rows  : {len(train_idx):,}")
print(f"Validation rows: {len(valid_idx):,}")

# ------------------------------------------------------------
# LightGBM
# ------------------------------------------------------------

print("\nTraining LightGBM...")

model = lgb.LGBMClassifier(
    n_estimators=800,
    learning_rate=0.04,
    num_leaves=31,
    max_depth=-1,
    min_child_samples=100,
    subsample=0.8,
    colsample_bytree=0.9,
    reg_alpha=0.1,
    reg_lambda=1.0,
    random_state=42,
    n_jobs=2,
    verbosity=-1
)

model.fit(
    X_train,
    y_train,
    eval_set=[(X_valid, y_valid)],
    eval_metric="binary_logloss",
    callbacks=[
        lgb.early_stopping(80),
        lgb.log_evaluation(100)
    ]
)

os.makedirs("models", exist_ok=True)

model.booster_.save_model(MODEL_FILE)

elapsed = (time.time() - start) / 60

print("\n" + "=" * 80)
print("TRAINING COMPLETE")
print("=" * 80)

print(f"Model: {MODEL_FILE}")
print(f"Best iteration: {model.best_iteration_}")
print(f"Elapsed: {elapsed:.2f} minutes")