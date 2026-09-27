import duckdb
import lightgbm as lgb
import pandas as pd

FEATURES = "intermediate/train_r15_similarity_features.parquet"
GT = "intermediate/train_match_features.parquet"
MODEL = "models/lightgbm_similarity.txt"
THRESHOLD = 0.76
SAMPLE_ROWS = 1000000

print("R15 SAMPLE EVALUATION")
print(f"Sample rows: {SAMPLE_ROWS:,}")

con = duckdb.connect()
con.execute("SET memory_limit='4GB'")
con.execute("SET threads=2")
con.execute("SET preserve_insertion_order=false")

print("Loading ground truth...")

gt = set()

for row in con.execute(
    f"SELECT source1_entity_id, candidate_entity_id "
    f"FROM read_parquet('{GT}') WHERE label=1"
).fetchall():
    gt.add(str(row[0]) + "|" + str(row[1]))

print(f"Ground-truth pairs: {len(gt):,}")

model = lgb.Booster(model_file=MODEL)
features = model.feature_name()

print("Loading 1M R15 rows...")

query = f"""
SELECT *
FROM read_parquet('{FEATURES}')
LIMIT {SAMPLE_ROWS}
"""

df = con.execute(query).df()

print(f"Loaded: {len(df):,} rows")

X = df[features]
prob = model.predict(X)
pred = prob >= THRESHOLD

keys = (
    df["source1_entity_id"].astype(str)
    + "|"
    + df["candidate_entity_id"].astype(str)
)

actual = keys.isin(gt).to_numpy()

TP = int((pred & actual).sum())
FP = int((pred & ~actual).sum())
FN = int((~pred & actual).sum())

precision = TP / (TP + FP) if TP + FP else 0
recall = TP / (TP + FN) if TP + FN else 0

f05 = (
    1.25 * precision * recall / (0.25 * precision + recall)
    if precision + recall else 0
)

print()
print("========== R15 SAMPLE RESULT ==========")
print(f"TP:        {TP:,}")
print(f"FP:        {FP:,}")
print(f"FN:        {FN:,}")
print(f"Precision: {precision:.4f}")
print(f"Recall:    {recall:.4f}")
print(f"F0.5:      {f05:.4f}")
print()
print("Base F0.5:  0.9585")
print(f"Difference: {f05 - 0.9585:+.4f}")
print("========================================")
