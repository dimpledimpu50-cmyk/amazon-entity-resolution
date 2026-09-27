import duckdb

DB_MEMORY = "6GB"
THREADS = 2

FEATURE_FILE = "intermediate/train_match_features.parquet"

con = duckdb.connect()
con.execute(f"SET memory_limit='{DB_MEMORY}'")
con.execute(f"SET threads={THREADS}")
con.execute("SET preserve_insertion_order=false")

print("=" * 80)
print("ANALYZING INCREMENTAL MATCHING RULES")
print("=" * 80)

current = """
    (exact_name = 1 AND exact_address = 1)
    OR (exact_name = 1 AND same_house = 1)
    OR (exact_compact_name = 1 AND same_house = 1)
    OR (name_prefix5 = 1 AND same_house = 1 AND address_prefix5 = 1)
    OR (name_prefix4 = 1 AND same_house = 1 AND address_prefix5 = 1)
    OR (name_contains = 1 AND same_house = 1)
"""

rules = {
    "PREFIX5_HOUSE": """
        name_prefix5 = 1
        AND same_house = 1
    """,

    "PREFIX4_HOUSE": """
        name_prefix4 = 1
        AND same_house = 1
    """,

    "PREFIX5_ADDRESS": """
        name_prefix5 = 1
        AND address_prefix5 = 1
    """,

    "PREFIX4_ADDRESS": """
        name_prefix4 = 1
        AND address_prefix5 = 1
    """,

    "CONTAINS_HOUSE": """
        name_contains = 1
        AND same_house = 1
    """,

    "PREFIX5_HOUSE_OR_ADDRESS": """
        name_prefix5 = 1
        AND (
            same_house = 1
            OR address_prefix5 = 1
        )
    """,

    "PREFIX4_HOUSE_OR_ADDRESS": """
        name_prefix4 = 1
        AND (
            same_house = 1
            OR address_prefix5 = 1
        )
    """,

    "PREFIX5_HOUSE_ADDRESS_PREFIX4": """
        name_prefix5 = 1
        AND same_house = 1
        AND prefix4_address_prefix = 1
    """,

    "PREFIX4_HOUSE_ADDRESS_PREFIX4": """
        name_prefix4 = 1
        AND same_house = 1
        AND prefix4_address_prefix = 1
    """
}

print("\nCalculating baseline...")

baseline_query = f"""
SELECT
    COUNT(*) AS predicted,
    SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END) AS tp,
    SUM(CASE WHEN label = 0 THEN 1 ELSE 0 END) AS fp
FROM read_parquet('{FEATURE_FILE}')
WHERE {current}
"""

baseline_pred, baseline_tp, baseline_fp = con.execute(baseline_query).fetchone()

total_true = con.execute(f"""
SELECT SUM(label)
FROM read_parquet('{FEATURE_FILE}')
""").fetchone()[0]

def metrics(predicted, tp):
    precision = tp / predicted if predicted else 0
    recall = tp / total_true if total_true else 0

    beta2 = 0.25

    f05 = (
        (1 + beta2) * precision * recall
        / (beta2 * precision + recall)
        if precision + recall > 0
        else 0
    )

    return precision, recall, f05

p, r, f = metrics(baseline_pred, baseline_tp)

print("\nBASELINE CURRENT 6 RULES")
print("-" * 80)
print(f"Predicted : {baseline_pred:,}")
print(f"TP        : {baseline_tp:,}")
print(f"FP        : {baseline_fp:,}")
print(f"Precision : {p * 100:.4f}%")
print(f"Recall    : {r * 100:.4f}%")
print(f"F0.5      : {f:.6f}")

for rule_name, rule in rules.items():

    query = f"""
    SELECT
        COUNT(*) AS new_predictions,
        SUM(CASE WHEN label = 1 THEN 1 ELSE 0 END) AS new_tp,
        SUM(CASE WHEN label = 0 THEN 1 ELSE 0 END) AS new_fp
    FROM read_parquet('{FEATURE_FILE}')
    WHERE ({rule})
      AND NOT ({current})
    """

    new_pred, new_tp, new_fp = con.execute(query).fetchone()

    combined_pred = baseline_pred + new_pred
    combined_tp = baseline_tp + new_tp
    combined_fp = baseline_fp + new_fp

    precision, recall, f05 = metrics(combined_pred, combined_tp)

    print("\n" + "=" * 80)
    print(rule_name)
    print("=" * 80)

    print(f"New predictions : {new_pred:,}")
    print(f"New TP          : {new_tp:,}")
    print(f"New FP          : {new_fp:,}")

    print(f"Combined pred   : {combined_pred:,}")
    print(f"Combined TP     : {combined_tp:,}")
    print(f"Combined FP     : {combined_fp:,}")

    print(f"Precision       : {precision * 100:.4f}%")
    print(f"Recall          : {recall * 100:.4f}%")
    print(f"F0.5            : {f05:.6f}")

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)