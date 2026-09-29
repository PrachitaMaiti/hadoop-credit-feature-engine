import pyarrow.parquet as pq
import pandas as pd

print("--> Streaming a 100,000-statement batch from train.parquet...")

# Columns representing core financial signals:
# S_2: Statement date
# B_1: Balance metric
# S_3: Spend metric
# P_2: Payment metric
# D_39: Delinquency indicator
columns = ['customer_ID', 'S_2', 'B_1', 'S_3', 'P_2', 'D_39']

# Stream in a single batch to keep memory footprint light
parquet_file = pq.ParquetFile('train.parquet')
batch = next(parquet_file.iter_batches(batch_size=100000, columns=columns))
df = batch.to_pandas()

# Impute missing values (inactive accounts often have nulls in spend/payment)
df['S_3'] = df['S_3'].fillna(0.0)
df['P_2'] = df['P_2'].fillna(0.0)
df['D_39'] = df['D_39'].fillna(0).astype(int)

# Binarize delinquency flag (1 if past due, 0 if clean)
df['delinquency_flag'] = (df['D_39'] > 0).astype(int)

# Project to our Java Mapper's expected format:
# customer_ID,statement_date,balance,spend,payment,delinquency_flag
clean_df = pd.DataFrame({
    'customer_ID': df['customer_ID'],
    'statement_date': df['S_2'],
    'balance': df['B_1'].round(4),
    'spend': df['S_3'].round(4),
    'payment': df['P_2'].round(4),
    'delinquency_flag': df['delinquency_flag']
})

clean_df.to_csv('input/credit_statements.csv', index=False)
print(f"--> Done! 'input/credit_statements.csv' is ready with {len(clean_df)} records.")
