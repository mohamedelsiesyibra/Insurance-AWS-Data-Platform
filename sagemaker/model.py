import os
import pickle
import boto3
import pandas as pd
import awswrangler as wr
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, FunctionTransformer
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

# Configuration as provided
categorical_features = ["sex", "smokingclass", "maritalstatus", "prodcode", "issuestate"]
numeric_features = ["coverageunit", "policyterm"]
target = "yearlypremium"
date_of_birth_col = "customerdob"

input_data_path = "dataset"
model_output_path = "models"

# Define a function to extract year_of_birth from customerdob
def extract_year_of_birth(X):
    X = X.copy()
    X[date_of_birth_col] = pd.to_datetime(X[date_of_birth_col], errors='coerce')
    X["year_of_birth"] = X[date_of_birth_col].dt.year
    return X.drop(columns=[date_of_birth_col])

year_transformer = FunctionTransformer(extract_year_of_birth, validate=False)

# We'll add "year_of_birth" to numeric features after transformation
all_numeric_features = numeric_features + ["year_of_birth"]

# Read Data from S3
print(f"Reading training data from: {input_data_path}")
df = wr.s3.read_parquet(input_data_path)
print(f"Data shape: {df.shape}")

required_cols = categorical_features + numeric_features + [date_of_birth_col, target]
missing_cols = [c for c in required_cols if c not in df.columns]
if missing_cols:
    raise ValueError(f"Missing required columns: {missing_cols}")

df = df.dropna(subset=required_cols)

X = df[categorical_features + numeric_features + [date_of_birth_col]]
y = df[target]

# Preprocessor
preprocessor = ColumnTransformer(
    transformers=[
        ("cat", OneHotEncoder(drop='first', handle_unknown='ignore'), categorical_features),
        ("num", "passthrough", all_numeric_features)
    ]
)

# Pipeline
pipeline = Pipeline(steps=[
    ("year_of_birth", year_transformer),
    ("preprocess", preprocessor),
    ("model", LinearRegression())
])

# Split Data and Train
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
pipeline.fit(X_train, y_train)

# Evaluate
y_pred = pipeline.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)
print(f"Evaluation Results - MSE: {mse}, R2: {r2}")

# Save KPIs and Model Locally
local_dir = "./model_artifacts"
os.makedirs(local_dir, exist_ok=True)

kpi_file_path = os.path.join(local_dir, "model_kpis.txt")
with open(kpi_file_path, "w") as f:
    f.write(f"MSE: {mse}\nR2: {r2}\n")

model_file_path = os.path.join(local_dir, "model.pkl")
with open(model_file_path, "wb") as f:
    pickle.dump(pipeline, f)

print("Model and KPIs saved locally.")


# Upload Artifacts to S3
s3 = boto3.resource("s3")

def upload_to_s3(local_path, s3_uri):
    assert s3_uri.startswith("s3://")
    s3_path = s3_uri[5:]
    parts = s3_path.split("/", 1)
    bucket_name = parts[0]
    key_prefix = "" if len(parts) == 1 else parts[1]
    if key_prefix and not key_prefix.endswith('/'):
        key_prefix += '/'
    filename = os.path.basename(local_path)
    s3_key = f"{key_prefix}{filename}"
    s3.meta.client.upload_file(local_path, bucket_name, s3_key)
    print(f"Uploaded {filename} to {s3_uri}{filename}")

# Upload both files
upload_to_s3(kpi_file_path, model_output_path)
upload_to_s3(model_file_path, model_output_path)

print("Model training and artifact upload complete.")
