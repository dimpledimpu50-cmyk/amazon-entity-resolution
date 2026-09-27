# Amazon Business Entity Resolution

## Project Handoff & Improvement Guide

---

# 1. Project Objective

The task is **business entity resolution**.

We need to match businesses from:

* Source 1 → reference/source to match
* Source 2
* Source 3

For every Source 1 test entity, the final system must identify the corresponding Source 2 and/or Source 3 entities.

The challenge has two required output files:

```text
candidate_pairs.tsv
matching_results.tsv
```

---

# 2. Important Challenge Rules

The system must work using the provided challenge datasets.

Do NOT use:

* Google Search
* Google Maps
* external business databases
* commercial entity-resolution APIs
* external company databases
* geocoding services

The matching must be based on the challenge data.

---

# 3. Dataset Structure

Each source contains approximately:

```text
entity_id
business_name
business_address
country
```

Training datasets:

```text
train_source1.tsv
train_source2.tsv
train_source3.tsv
```

Test datasets:

```text
test_source1.tsv
test_source2.tsv
test_source3.tsv
```

There is also training ground truth containing the actual Source 1 → Source 2/3 matches.

---

# 4. Dataset Sizes

Normalized training data:

```text
train_source1: 2,206,821 rows
train_source2: 5,034,616 rows
train_source3: 5,285,603 rows
```

Normalized test data:

```text
test_source1: 1,732,544 rows
test_source2: 4,887,273 rows
test_source3: 5,082,316 rows
```

The test Source 1 contains:

```text
1,732,544 entities
```

---

# 5. Ground Truth

The training ground truth can contain multiple matches for a Source 1 entity.

Example:

```text
S1-965667
    S2-681193310
    S2-743505751
    S3-775321672
    S3-11291185
    S3-860443364
```

Therefore:

**The final matcher must support one-to-many matches.**

Do NOT assume there is always exactly one correct entity.

---

# 6. Evaluation Metric

The challenge uses:

```text
F0.5
```

F0.5 gives more importance to precision than recall.

Formula:

```text
F0.5 = 1.25 × Precision × Recall
       --------------------------------
       0.25 × Precision + Recall
```

Therefore, adding huge numbers of low-quality candidates/matches can hurt the score.

The goal is:

```text
HIGH PRECISION
+
HIGH RECALL
+
CONTROLLED CANDIDATE VOLUME
```

---

# 7. Current Project Structure

Important source code is inside:

```text
src/
```

Models:

```text
models/
```

Final submissions:

```text
output/
```

Large generated data:

```text
parquet/
intermediate/
```

The original challenge data is inside:

```text
dataset/
```

---

# 8. Current Pipeline

The implemented pipeline is:

```text
Raw datasets
      ↓
Normalization
      ↓
Blocking
      ↓
Candidate pairs
      ↓
Feature engineering
      ↓
LightGBM matcher
      ↓
Threshold optimization
      ↓
Test prediction
      ↓
Submission generation
      ↓
Validation
```

---

# 9. Normalization

The project creates normalized versions of:

```text
business_name
business_address
country
```

Important normalized fields include:

```text
name_norm
name_compact
address_norm
address_compact
country_norm
```

The normalization code is in:

```text
src/normalize.py
```

Normalized Parquet files are stored in:

```text
parquet/
```

---

# 10. Blocking

Blocking is currently the biggest weakness.

The reason is simple:

If a true pair is NOT generated during blocking, the ML matcher can never recover it.

Current full-training blocker:

```text
Candidate pairs:       82,890,394
Ground-truth pairs:     7,638,365
True pairs found:       6,460,364
Blocking recall:            84.58%
```

Therefore approximately 15% of true pairs are missed before matching.

This is the main area that should be improved.

---

# 11. Current Blocking Rules

The base blocker consists of several rules:

```text
V4 name
V4 compact name
V4 prefix
V4 address
V4 house
R7
R10
R11
R13
```

Corresponding files are in:

```text
intermediate/train_candidates/
```

Examples:

```text
v4_name.parquet
v4_compact.parquet
v4_prefix.parquet
v4_address.parquet
v4_house.parquet
r7.parquet
r10.parquet
r11.parquet
r13.parquet
```

The union is:

```text
train_candidate_pairs.parquet
```

---

# 12. Base Blocker Performance

Current base blocker:

```text
82,890,394 candidate pairs
```

Training ground truth:

```text
7,638,365 true pairs
```

Recovered:

```text
6,460,364 true pairs
```

Blocking recall:

```text
84.58%
```

This is not good enough for a high leaderboard score.

---

# 13. Test Blocking

The same blocker was applied to the test data.

Results:

```text
Test Source 1 entities:       1,732,544
Candidate pairs:             60,196,718
S1 with candidates:           1,728,313
S1 without candidates:            4,231
```

Therefore:

```text
4,231 test Source 1 entities
```

have no candidates at all.

Those entities can never be matched by the current matcher.

---

# 14. Existing Blocking Experiments

Several additional blocking strategies were tested.

---

## R14 — Rare Name Prefix

Rule:

```text
rare prefix4
+
name length difference <= 2
+
prefix frequency <= 100
```

Results:

```text
Raw candidates:       2,477,833
New candidates:          13,007
New true pairs:              90
Precision:               0.6919%
```

Conclusion:

R14 was not useful.

Do not prioritize this approach.

---

# 15. R15 — Address Recovery

Rule:

```text
address prefix5
+
address length difference <= 10
+
prefix frequency <= 50
```

Results:

```text
Raw candidates:        16,084,661
New candidates:         12,679,586
New true pairs:            46,284
Precision:                   0.3650%
```

Estimated blocking recall improvement:

```text
84.58%
→ approximately 85.19%
```

R15 recovered some true matches, but candidate precision was extremely low.

It was not incorporated into the final baseline.

---

# 16. Important R15 Evaluation Mistake

A quick R15 sample evaluator produced:

```text
Precision: 0.1155
Recall:    0.9773
F0.5:      0.1402
```

This result is **NOT a valid measurement of R15**.

Why?

The evaluator used:

```text
train_match_features.parquet
```

as ground truth.

That file only contains true pairs that were already inside the original candidate set.

Therefore, newly recovered true pairs from R15 were incorrectly treated as false positives.

So:

```text
DO NOT conclude that R15 is bad from this result.
```

If you want to evaluate R15 properly, compare it against the **full training ground truth**, not only the base candidate set.

---

# 17. Blocking Miss Analysis

We analyzed the true pairs missed by the base blocker.

Missed true pairs:

```text
1,178,001
```

Important signals among missed pairs:

```text
Name containment                  21.55%
Name prefix 3                     54.23%
Name prefix 4                     51.82%
Name prefix 5                     50.00%
Name suffix 4                     21.78%
Name length diff <=2              37.19%
Name length diff <=5              60.62%

Address prefix 5                  15.06%
Same house number                 18.65%
Address length diff <=5           42.18%
Address length diff <=10          61.25%
```

Combined signals:

```text
prefix4 + length diff <=2         23.12%
prefix4 + length diff <=5         36.14%
prefix5 + length diff <=5         significant
prefix4 + house                    5.88%
prefix5 + house                    5.62%
prefix4 + address prefix5          4.91%
prefix5 + address prefix5          4.69%
containment + house                2.68%
```

This shows that many missed matches have strong partial similarity but are not captured by the current blocking rules.

---

# 18. Important Warning About Prefix Blocking

A naive rule such as:

```text
same prefix4
+
name length difference <=2
```

is NOT practical.

Diagnostic showed an estimated:

```text
~3.42 billion candidate pairs
```

for one such unrestricted strategy.

Therefore:

**Never implement unrestricted prefix4 + length blocking on the full dataset.**

The rule needs strong frequency controls / additional conditions.

---

# 19. Rare Prefix Experiment

For missed true pairs:

```text
prefix4 + length difference <=2
```

we tested prefix-frequency restrictions.

Recovered missed true pairs:

```text
frequency <= 1       1,939
frequency <= 2       3,904
frequency <= 5       7,617
frequency <= 10     10,648
frequency <= 25     14,295
frequency <= 50     17,308
frequency <= 100    28,243
frequency <= 200    42,338
frequency <= 500    63,780
```

However, R14 with frequency <=100 produced only:

```text
90 new true pairs
```

after removing candidates already present in the base blocker.

This illustrates an important point:

**Raw recovery numbers are not the same as useful new candidates.**

Always anti-join against the existing candidate set before evaluating a new blocker.

---

# 20. Address Recovery Diagnostic

Missed true pairs were also analyzed using:

```text
address prefix5
+
address length difference <=10
```

Recovered missed true pairs:

```text
frequency <= 1       26,927
frequency <= 2       55,575
frequency <= 5      127,249
frequency <= 10     207,489
frequency <= 25     337,789
frequency <= 50     429,234
frequency <= 100    493,886
frequency <= 200    531,682
frequency <= 500    563,607
```

This suggests address-based recovery is potentially more promising than rare-name-prefix recovery.

But candidate volume becomes very large, so precision/volume must be controlled.

---

# 21. Current Matcher

The original rule-based matcher contained:

```text
1. exact_name + exact_address

2. exact_name + same_house

3. exact_compact_name + same_house

4. name_prefix5 + same_house + address_prefix5

5. name_prefix4 + same_house + address_prefix5

6. name_contains + same_house
```

The rule-based matcher produced:

```text
Predicted pairs:       3,768,876
True pairs:            3,643,968
False positives:        124,908
Precision:                  96.69%
Recall:                      56.40%
F0.5:                         0.8460
```

Therefore the rule-based matcher was replaced with ML.

---

# 22. ML Feature Set

Base matcher features include:

```text
country_equal
exact_name
exact_compact_name
name_prefix4
name_prefix5
name_suffix4
name_contains
name_length_diff

exact_address
exact_compact_address
address_prefix5
same_house
address_length_diff

prefix4_same_house
prefix4_address_prefix
exact_name_same_house
exact_name_exact_address
```

Similarity features were then added:

```text
name_jaro
name_levenshtein
name_length_ratio

address_jaro
address_levenshtein
address_length_ratio
```

---

# 23. LightGBM Similarity Model

The current best local model is:

```text
models/lightgbm_similarity.txt
```

Training data:

```text
25,834,043 rows
```

Class distribution:

```text
Positive: 6,460,364
Negative: 19,373,679
```

Features:

```text
23
```

Training/validation:

```text
80/20
```

Model configuration:

```text
800 estimators
learning_rate = 0.04
num_leaves = 31
min_child_samples = 100
subsample = 0.8
colsample_bytree = 0.9
reg_alpha = 0.1
reg_lambda = 1
n_jobs = 2
```

Best iteration:

```text
800
```

Validation binary log loss:

```text
0.0749216
```

---

# 24. Local Validation Result

Best threshold:

```text
0.76
```

Results:

```text
Precision: 0.9778
Recall:    0.8885
F0.5:      0.9585
```

Counts:

```text
TP: 1,147,741
FP:    26,071
FN:   144,061
```

This was a strong improvement over the original rule-based matcher.

However:

**This is NOT the leaderboard score.**

It is only validation performance on the training-derived candidate set.

---

# 25. Original LightGBM Model

The simpler LightGBM model:

```text
models/lightgbm_matcher.txt
```

achieved approximately:

```text
Precision: 0.9711
Recall:    0.7077
F0.5:      0.9038
```

The similarity model improved local F0.5 substantially:

```text
0.9038
→
0.9585
```

Therefore the similarity model should currently be treated as the primary matcher baseline.

---

# 26. Feature Importance

Important features in the simpler model included:

```text
prefix4_same_house
address_prefix5
address_length_diff
name_length_diff
name_contains
same_house
exact_compact_name
prefix4_address_prefix
name_suffix4
exact_compact_address
name_prefix4
name_prefix5
```

This suggests that:

* address structure
* house number
* name structure
* partial string similarity

are important.

---

# 27. Important Observation About Prefix4_same_house

An experiment removed:

```text
prefix4_same_house
```

The result remained almost identical:

```text
F0.5 ≈ 0.9039
```

Therefore don't assume high feature importance automatically means the feature is essential.

Feature interactions matter.

---

# 28. Test Matcher Results

Using the current six-rule matcher on the test candidate set:

```text
Test Source 1 entities:      1,732,544
Predicted match pairs:       3,020,081
Matched S1 entities:         1,278,519
Unmatched S1 entities:         454,025
```

Rule distribution:

```text
TIER_3_PREFIX5_HOUSE_ADDRESS
Pairs: 1,277,770
S1:      775,573

TIER_2_EXACT_NAME_HOUSE
Pairs:   929,906
S1:      662,922

TIER_5_CONTAINS_HOUSE
Pairs:   610,676
S1:      464,472

TIER_1_EXACT_NAME_ADDRESS
Pairs:    84,699
S1:       77,837

TIER_2_COMPACT_NAME_HOUSE
Pairs:    68,910
S1:       62,763

TIER_4_PREFIX4_HOUSE_ADDRESS
Pairs:    48,120
S1:       43,376
```

Note that the test matcher above was based on the original rule-based matcher, not necessarily the latest similarity LightGBM model.

---

# 29. Actual Leaderboard Result

The submitted/current baseline received:

```text
F0.5 = 0.654
```

This is the important real-world baseline.

Do NOT confuse it with:

```text
Local validation F0.5 = 0.9585
```

The gap is:

```text
Local validation: 0.9585
Leaderboard:      0.654
```

This means the current system has significant test generalization / end-to-end problems.

---

# 30. Main Improvement Goal

The goal is to improve:

```text
0.654
```

on the actual leaderboard.

Do not optimize only for:

```text
0.9585
```

because that number is only the local validation result.

The actual objective is:

```text
better end-to-end test performance
```

---

# 31. Main Areas to Investigate

## Priority 1 — Blocking

Current training blocking recall:

```text
84.58%
```

This is the biggest known structural limitation.

Investigate:

* address-based blocking
* multilingual normalization
* reordered business names
* abbreviations
* token-based blocking
* rare tokens
* house-number + address signals
* country-aware blocking
* multiple independent blocking rules
* controlled fuzzy blocking
* token-prefix blocking
* character n-gram blocking
* transliteration where justified by the supplied data
* name/address hybrid blocks

Every new blocker should report:

```text
Raw candidates
New candidates
New true pairs
Candidate precision
Blocking recall
Candidate volume increase
```

---

# 32. Priority 2 — Test Distribution

Analyze:

```text
train_source1
train_source2
train_source3
```

versus:

```text
test_source1
test_source2
test_source3
```

Look for differences in:

* name length
* address length
* country distribution
* empty values
* language
* punctuation
* abbreviations
* transliteration
* duplicate frequency
* business-name structure
* address structure

A model that performs well on training validation but poorly on test may be exploiting patterns that don't generalize.

---

# 33. Priority 3 — Threshold

Do not assume:

```text
threshold = 0.76
```

is optimal for test data.

It was optimized using training validation.

Test score may require a different threshold.

However, because hidden test labels are unavailable, threshold changes should be evaluated carefully using multiple validation splits rather than repeatedly guessing.

---

# 34. Priority 4 — False Positive Control

F0.5 strongly rewards precision.

Therefore:

```text
More matches ≠ automatically better
```

A blocker that adds:

```text
10 million candidates
```

but only:

```text
20,000 true pairs
```

may hurt the downstream matcher substantially.

Always calculate candidate precision.

---

# 35. Priority 5 — One-to-Many Matching

Do not force one Source 1 entity to have only one match.

Ground truth can contain:

```text
S1 → S2
S1 → S2 + S3
S1 → multiple S2/S3
```

The final output must preserve legitimate one-to-many matches.

---

# 36. Final Output Requirements

For every Source 1 test entity, there must be exactly one row.

Candidate file:

```text
source1_entity_id    candidate_entity_ids
```

Matching file:

```text
source1_entity_id    matched_entity_ids
```

If there are no candidates:

```text
S1-123
```

If there are no matches:

```text
S1-123
```

Do not write:

```text
""
```

for empty values.

---

# 37. Current Validated Submission

Current baseline files:

```text
output/candidate_pairs.tsv
output/matching_results.tsv
```

Validation:

```text
Rows: 1,732,544
Unique S1 IDs: 1,732,544
Bad rows: 0
```

Candidate file:

```text
Non-empty lists: 1,728,313
File size: ~761 MB
```

Matching file:

```text
Non-empty lists: 1,278,519
File size: ~59 MB
```

Validation status:

```text
BOTH SUBMISSION FILES PASSED BASIC VALIDATION
```

These are the **baseline files**.

Do not overwrite them during experiments.

---

# 38. Memory Constraints

Development machine:

```text
RAM: 16 GB
OS: Windows
```

Be careful with:

```text
pandas.read_parquet()
```

on extremely large datasets.

A previous attempt to load approximately:

```text
95.57 million rows
```

into pandas caused severe system slowdown / interruption.

Avoid loading the entire candidate set into pandas.

Prefer:

```text
DuckDB
Parquet
chunked processing
streaming
SQL joins
limited samples
```

Use:

```text
SET memory_limit='4GB'
SET threads=2
SET preserve_insertion_order=false
```

when appropriate.

---

# 39. Large Files

Important large files include:

```text
intermediate/train_match_features.parquet
~1.05 GB
```

```text
intermediate/train_r15_similarity_features.parquet
~large / 95M rows
```

```text
output/candidate_pairs.tsv
~761 MB
```

Do not repeatedly load these entirely into pandas.

---

# 40. Safe Experimentation Strategy

Always create versioned files.

For example:

```text
r16_...
r17_...
r18_...
```

Do NOT overwrite:

```text
train_candidate_pairs.parquet
output/candidate_pairs.tsv
output/matching_results.tsv
```

until the experiment has been proven useful.

Recommended workflow:

```text
New blocker
     ↓
Measure candidates
     ↓
Anti-join against base
     ↓
Measure new true pairs
     ↓
Calculate candidate precision
     ↓
Estimate blocking recall
     ↓
Decide whether worth testing
     ↓
Build ML features
     ↓
Evaluate matcher
     ↓
Only then generate test submission
```

---

# 41. What NOT To Do

Do not:

```text
❌ Run unrestricted prefix4 + length joins
❌ Load 95M+ rows into pandas
❌ Assume local validation = leaderboard score
❌ Add blockers without measuring candidate precision
❌ Replace the baseline files during experiments
❌ Use external business databases
❌ Force one-to-one matching
❌ Optimize only recall
❌ Optimize only precision
```

---

# 42. Recommended Improvement Direction

The most promising direction is a **multi-blocker system**.

Instead of one giant fuzzy join:

```text
Source 1
   ↓
many cheap independent blocks
   ↓
union
   ↓
deduplicate
   ↓
ML matcher
```

Potential blocks to investigate:

```text
1. Exact normalized name

2. Exact compact name

3. Exact address

4. Exact compact address

5. Same house number + name token

6. Rare name token

7. Rare address token

8. Name prefix + controlled length

9. Address prefix + controlled length

10. Name token + address token

11. Name suffix + house number

12. Country + rare name token

13. Country + address token

14. Character n-gram based block

15. Token-based block
```

Each should be independently evaluated.

---

# 43. Special Attention: Multilingual Data

The training data contains multilingual records.

Therefore investigate:

* Unicode normalization
* punctuation normalization
* whitespace normalization
* transliteration where appropriate
* token normalization
* common abbreviations
* reordered words
* legal suffixes such as:

  * private limited
  * pvt ltd
  * ltd
  * llp
  * limited
  * services
  * enterprises
  * trading
  * company

But be careful:

Removing too much information during normalization can create false matches.

---

# 44. Current Baseline Summary

```text
                         BASELINE
------------------------------------------------
Training blocker recall       84.58%

Training ML validation
Precision                     0.9778
Recall                        0.8885
F0.5                          0.9585

Actual leaderboard
F0.5                          0.654

Test S1                       1,732,544
Test candidate pairs          60,196,718
Test S1 without candidates        4,231

Validated submission           YES
------------------------------------------------
```

---

# 45. Definition of Success

An improvement should be considered meaningful only if it improves the **actual leaderboard score**, or if it demonstrates a strong improvement on robust validation experiments that are likely to generalize.

Do not celebrate an improvement such as:

```text
validation F0.5:
0.9585 → 0.9700
```

if it causes:

```text
leaderboard:
0.654 → 0.60
```

The leaderboard is the final reality.

---

# 46. First Task For Anyone Improving This Project

Before writing another blocker, answer these questions:

### A.

Why does the model have:

```text
0.9585 validation
```

but:

```text
0.654 leaderboard?
```

### B.

Are the training and test distributions significantly different?

### C.

Which types of test businesses are currently producing:

```text
no candidates
```

?

### D.

Can blocking be made more recall-heavy without producing billions of candidates?

### E.

Can the matcher be made robust to unseen name/address variations?

### F.

Can one-to-many matching be handled more accurately?

---

# 47. Important Baseline Files

Keep these:

```text
src/
models/
output/
README.md
requirements.txt
PROJECT_STATUS.md
```

The original dataset and generated Parquet files do not need to be shared if the recipient can obtain the challenge data themselves.

---

# 48. Final Instruction

The current system is a **working baseline, not the final optimized solution**.

The main challenge is not simply training a stronger LightGBM model.

The critical problem is:

```text
GET THE RIGHT TRUE PAIRS INTO THE CANDIDATE SET
+
MAKE THE MATCHER GENERALIZE TO THE TEST DATA
+
CONTROL FALSE POSITIVES
```

Start with diagnostics and controlled experiments.

Never destroy the validated baseline.
