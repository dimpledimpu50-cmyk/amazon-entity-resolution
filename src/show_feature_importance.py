import lightgbm as lgb

MODEL_FILE = "models/lightgbm_matcher.txt"

model = lgb.Booster(model_file=MODEL_FILE)

names = model.feature_name()
gains = model.feature_importance(importance_type="gain")

results = sorted(
    zip(names, gains),
    key=lambda x: x[1],
    reverse=True
)

print("=" * 70)
print("LIGHTGBM FEATURE IMPORTANCE")
print("=" * 70)

for name, gain in results:
    print(f"{name:30s} {gain:,.2f}")

print("=" * 70)