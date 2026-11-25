# Databricks notebook source
# DBTITLE 1,Load your table from the Databricks catalog
df = spark.table("workspace.default.nfl_survivor_csv_export").toPandas()
df.head()

# COMMAND ----------

# DBTITLE 1,Clean data types
import pandas as pd
import numpy as np

numeric_cols = [
    "Super_Bowl", "Conference", "Division",
    "Wins", "Over_Wins", "Under_Wins",
    "Playoffs_Yes", "Playoffs_No", "Home",
    "Super_Bowl_Opp", "Conference_Opp", "Division_Opp",
    "Wins_Opp", "Over_Wins_Opp", "Under_Wins_Opp",
    "Playoffs_Yes_Opp", "Playoffs_No_Opp"
]

# Remove commas and convert to numeric
df[numeric_cols] = df[numeric_cols].replace({',':''}, regex=True)
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

# COMMAND ----------

# DBTITLE 1,Impute missing numeric values
for col in numeric_cols:
    # .fillna(0) covers a column that came back completely empty from the web
    df[col] = df[col].fillna(df[col].median()).fillna(0)

# COMMAND ----------

# DBTITLE 1,Split rows with and without Target
df_present = df[df["Target"].notna()]
df_missing = df[df["Target"].isna()]

# COMMAND ----------

# DBTITLE 1,Encode categorical variables
df_present_encoded = pd.get_dummies(df_present.drop(columns=["Target"]))
df_missing_encoded = pd.get_dummies(df_missing.drop(columns=["Target"]))

# Align columns
df_missing_encoded = df_missing_encoded.reindex(columns=df_present_encoded.columns, fill_value=0)


# COMMAND ----------

# DBTITLE 1,Train Ridge Regression
from sklearn.linear_model import Ridge

X = df_present_encoded
y = df_present["Target"]

model = Ridge(alpha=1.0)
model.fit(X, y)


# COMMAND ----------

# DBTITLE 1,Predict missing Target values
df_missing["Predicted_Target"] = model.predict(df_missing_encoded)
df_missing.head()

# COMMAND ----------

# DBTITLE 1,Feature Importance for Ridge Regression
import pandas as pd
import numpy as np

# Extract coefficients from the trained Ridge model
coeffs = model.coef_

# Match coefficients to feature names
feature_names = df_present_encoded.columns

# Build importance DataFrame
importance_df = pd.DataFrame({
    "Feature": feature_names,
    "Coefficient": coeffs,
    "Importance": np.abs(coeffs)
})

# Sort by importance (largest magnitude first)
importance_df = importance_df.sort_values("Importance", ascending=False)

importance_df


# COMMAND ----------

# Clean column names: replace spaces and special chars with underscores
df_missing.columns = (
    df_missing.columns
    .str.replace(' ', '_', regex=False)
    .str.replace('[,;{}()\n\t=]', '_', regex=True)
)

# COMMAND ----------

# Convert Pandas → Spark
df_missing_spark = spark.createDataFrame(df_missing)

# Write to a new table
df_missing_spark.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.default.nfl_survivor_predictions")