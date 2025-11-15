import os

from pyspark.sql import SparkSession
from utils.paths import phase1_path, phase2_path

input_file_path = phase1_path("1A_attributes_reorder", "1A_attributes_reordered.csv")
output_dir_path = phase2_path("1A_categorical_outliers_distinct_values")

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.format("csv") \
    .option("header", "true") \
    .load(input_file_path)

year_cols = [c for c in df.columns if c.startswith("YR")]

non_year_cols = [c for c in df.columns if c not in year_cols]

for col in non_year_cols:
    distinct_df = df.select(col).distinct()
    output_path = os.path.join(output_dir_path, f"distinct_values_{col}.csv")

    distinct_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    print(f"Saved distinct values for column '{col}' to {output_path}")
