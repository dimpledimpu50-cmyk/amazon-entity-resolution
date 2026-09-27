import duckdb
import lightgbm as lgb
import numpy as np
import os

DB_MEMORY = "6GB"
THREADS = 2

SAMPLE_FILE = "intermediate/ml_training_sample.parquet"
MODEL_FILE = "models/lightgbm_matcher.txt"

os.makedirs("models", exist_ok=True)

print("=" * 80)
print("TRAINING LIGHTGBM MATCHER")
print("=" * 80)

# ------------------------------------------------------------------
# Feature columns
# ------------------------------------------------------------------

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

# ------------------------------------------------------------------
# Load training sample
# ------------------------------------------------------------------

print("\nLoading training sample...")

con = duckdb.connect()
con.execute(f"SET memory_limit='{DB_MEMORY}'")
con.execute(f"SET threads={THREADS}")
con.execute("SET preserve_insertion_order=false")

df = con.execute(f"""
    SELECT
        label,
        {", ".join(features)}
    FROM read_parquet('{SAMPLE_FILE}')
""").fetchdf()

print(f"Rows loaded: {len(df):,}")

# ------------------------------------------------------------------
# Prepare X / y
# ------------------------------------------------------------------

y = df["label"].astype(np.int8).to_numpy()
X = df[features].copy()

del df

print(f"Features: {len(features)}")
print(f"Positive: {int(y.sum()):,}")
print(f"Negative: {int((y == 0).sum()):,}")

# ------------------------------------------------------------------
# Train/validation split
# ------------------------------------------------------------------

# Deterministic split
rng = np.random.default_rng(42)

indices = np.arange(len(y))
rng.shuffle(indices)

split = int(len(indices) * 0.80)

train_idx = indices[:split]
valid_idx = indices[split:]

X_train = X.iloc[train_idx]
y_train = y[train_idx]

X_valid = X.iloc[valid_idx]
y_valid = y[valid_idx]

print("\nTraining rows :", len(train_idx))
print("Validation rows:", len(valid_idx))

# ------------------------------------------------------------------
# LightGBM
# ------------------------------------------------------------------

model = lgb.LGBMClassifier(
    objective="binary",

    n_estimators=600,
    learning_rate=0.05,

    num_leaves=31,
    max_depth=-1,

    min_child_samples=100,

    subsample=0.8,
    colsample_bytree=0.9,

    reg_alpha=0.1,
    reg_lambda=1.0,

    n_jobs=2,

    random_state=42
)

print("\nStarting training...")

model.fit(
    X_train,
    y_train,

    eval_set=[
        (X_valid, y_valid)
    ],

    callbacks=[
        lgb.early_stopping(
            stopping_rounds=50,
            verbose=True
        )
    ]
)

print("\nTraining finished.")

# ------------------------------------------------------------------
# Save model
# ------------------------------------------------------------------

model.booster_.save_model(MODEL_FILE)

print("\n" + "=" * 80)
print("MODEL SAVED")
print("=" * 80)

print(f"Model: {MODEL_FILE}")
print(f"Best iteration: {model.best_iteration_}")