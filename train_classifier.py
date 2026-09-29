import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score, confusion_matrix

print("=" * 60)
print("STAGE 2: TRAINING CLASSIFIER ON HADOOP FEATURE MART")
print("=" * 60)

# 1. Read the Hadoop feature output (part-r-00000)
# Structure: customer_ID \t statement_count,avg_spend,spend_acceleration,avg_debt_ratio,max_delinquent_streak
rows = []
with open('output/part-r-00000', 'r') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        cust_id, feats = line.split('\t')
        rows.append([cust_id] + [float(x) for x in feats.split(',')])

feature_df = pd.DataFrame(rows, columns=[
    'customer_ID', 
    'statement_count', 
    'avg_spend', 
    'spend_acceleration', 
    'avg_debt_ratio', 
    'max_delinquent_streak'
])

print(f"[+] Loaded {len(feature_df)} aggregated customer profiles from Hadoop.")

# 2. Ingest Official Ground-Truth Competition Labels
labels_df = pd.read_csv('train_labels.csv')
print(f"[+] Loaded {len(labels_df)} total labels from train_labels.csv.")

# 3. Inner Join Hadoop Feature Mart with Official Labels
dataset = feature_df.merge(labels_df, on='customer_ID', how='inner')
print(f"[+] Successfully merged ground-truth labels for {len(dataset)} customers in this slice.")

class_counts = dataset['target'].value_counts()
print(f"[+] Target Distribution: {class_counts.get(0, 0)} Non-Default (0), {class_counts.get(1, 0)} Default (1)")

# 4. Feature Selection
feature_cols = ['avg_spend', 'spend_acceleration', 'avg_debt_ratio', 'max_delinquent_streak']
X = dataset[feature_cols]
y = dataset['target']

# 5. Stratified 80/20 Train/Test Split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.20, random_state=42, stratify=y
)

# 6. Feature Scaling
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# 7. Model Training: Random Forest
model = RandomForestClassifier(n_estimators=100, max_depth=6, random_state=42)
model.fit(X_train_scaled, y_train)

# 8. Evaluation on Held-Out Test Set
y_pred = model.predict(X_test_scaled)
y_prob = model.predict_proba(X_test_scaled)[:, 1]

print("\n" + "=" * 60)
print("TEST SET EVALUATION METRICS")
print("=" * 60)
print(classification_report(y_test, y_pred, digits=4))
print(f"Test ROC-AUC Score: {roc_auc_score(y_test, y_prob):.4f}")

# 9. Feature Importance Analysis
importance = pd.Series(model.feature_importances_, index=feature_cols).sort_values(ascending=False)
print("\n--- Feature Importance Ranking ---")
for feat, imp in importance.items():
    print(f"  {feat:<25}: {imp:.4f}")

# 10. Export Predictions
dataset['predicted_risk_score'] = model.predict_proba(scaler.transform(X))[:, 1]
dataset.to_csv("customer_credit_risk_predictions.csv", index=False)
print("\n[+] Saved final scored portfolio to 'customer_credit_risk_predictions.csv'.")
