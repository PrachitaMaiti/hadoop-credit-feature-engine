# Distributed Credit Default Prediction Engine

A big-data financial feature engineering and credit default prediction pipeline built to aggregate and evaluate consumer credit risk across 100,000 multi-statement customer records.

The system uses a hybrid architecture:
1. Distributed Feature Engineering (Java MapReduce on Apache Hadoop): Solves time-series statement ordering during the distributed shuffle phase via the Secondary Sort design pattern, aggregating chronological statement logs in O(1) memory per account.
2. Supervised Default Classification (Python / Scikit-learn): Ingests the aggregated feature mart, merges ground-truth default labels, trains an ensemble Random Forest classifier, and evaluates feature importances.

---

## System Architecture

Raw Columnar Parquet Statement Archives
                   |
                   v  [prep_amex_sample.py - PyArrow / Pandas ETL Engine]
        credit_statements.csv (100,000 Statement Logs)
                   |
+------------------v----------------------------------------------+
|             Hadoop Distributed Financial Feature Engine         |
|                                                                 |
|  StatementMapper   --> Emits (CustomerDateKey, StatementMetrics)|
|  CustomerPartitioner --> Enforces account locality via key hash |
|  CustomerGroupingComparator --> Demarcates customer boundaries  |
|  FeatureReducer    --> Single-pass chronological state machine  |
+------------------+----------------------------------------------+
                   |
                   v  [output/part-r-00000]
        Customer Risk Feature Mart (8,294 Unique Accounts)
                   |
                   v  [train_classifier.py - Scikit-learn Engine]
        Supervised Random Forest Classifier & Probability Scoring
                   |
                   v
 customer_credit_risk_predictions.csv (Scored Portfolio Predictions)

---

## Distributed Design: The Secondary Sort Pattern

Financial statements arrive partitioned across distributed splits without intrinsic ordering. Computing chronological metrics (such as delinquency streaks and spend velocity) typically forces in-memory buffering, risking memory overflow at scale.

This project offloads time-series ordering entirely to Hadoop's shuffle phase:
* Composite Key (CustomerDateKey): Combines customerId and statementDate, sorting first by customer hash ascending, then by statement date chronologically.
* Custom Partitioner (CustomerPartitioner): Hashes exclusively on customerId so that all statement cycles for a given customer route to the same physical Reducer node.
* Grouping Comparator (CustomerGroupingComparator): Evaluates strictly the customerId field during the reduce-grouping phase, ensuring all chronological statements for a customer stream through a single reduce() invocation.

### Engineered Account Features
* avg_spend: Mean historical transaction volume across active statement cycles.
* spend_acceleration: Ratio of final billing cycle spend relative to historical mean spend.
* avg_debt_ratio: Rolling ratio of carried balance relative to payment volume (balance / payment).
* max_delinquent_streak: Maximum consecutive statement cycles flagged with delinquent payment status (delinquency_flag == 1).

---

## Model Performance & Evaluation

The feature mart was merged against official ground-truth default labels on the active 8,294-customer partition (6,096 Non-Default, 2,198 Default) and evaluated on a held-out stratified 20% test split:

* ROC-AUC Score: 0.8882
* Classification Accuracy: 82.40%
* Default Class Recall: 68.64%
* Default Class Precision: 66.23%
* Macro Average F1-Score: 0.7768

### Feature Importance Breakdown
Tree-based split attribution identified financial leverage as the primary determinant of default risk:

1. avg_debt_ratio (63.90%): Strongest predictive signal; carried balance relative to repayment capacity drives the majority of split decisions.
2. avg_spend (25.92%): Baseline spending volume serves as a core commercial capacity factor.
3. max_delinquent_streak (5.54%): Sustained non-payment cycles confirm credit distress escalation.
4. spend_acceleration (4.64%): Detects abrupt spend spikes immediately preceding insolvency.

---

## File Structure

hadoop-credit-feature-engine/
|
|-- prep_amex_sample.py              # Ingestion ETL streaming 100k records from Parquet
|-- train_classifier.py              # Scikit-learn Random Forest training and evaluation
|-- README.md                        # Technical project documentation
|
|-- src/                             # Distributed Hadoop MapReduce source code
|   |-- CustomerDateKey.java         # Composite key for 2D natural ordering
|   |-- CustomerPartitioner.java     # Hash routing strictly on customer_ID
|   |-- CustomerGroupingComparator.java # Reducer boundary demarcation
|   `-- CreditFeatureEngine.java     # Job driver, StatementMapper, and FeatureReducer
|
`-- output/
    `-- part-r-00000                 # Hadoop feature mart (8,294 customer records)

---

## Reproduction Guide

### Prerequisites
* Java JDK 11
* Apache Hadoop 3.x
* Python 3.9+ (pyarrow, pandas, scikit-learn)

### 1. Ingest Data Sample
python3 prep_amex_sample.py

### 2. Compile Java Engine & Run Hadoop Job
javac --release 11 -classpath $(hadoop classpath) -d build/ src/*.java
jar -cvf credit-engine.jar -C build/ .
rm -rf output
hadoop jar credit-engine.jar CreditFeatureEngine input/credit_statements.csv output

### 3. Fit Classifier & Export Predictions
python3 train_classifier.py
