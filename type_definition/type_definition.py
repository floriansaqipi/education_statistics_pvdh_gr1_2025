import os

from pyspark.sql import SparkSession


file_path = os.path.join("..", "data", "Edstats_Updated.csv")

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()


df = spark.read.format("csv") \
    .option("header", "true") \
    .load(file_path)


year_cols = [c for c in df.columns if c.startswith("YR")]

non_year_cols = [c for c in df.columns if c not in year_cols]

for col in non_year_cols:
    distinct_df = df.select(col).distinct().na.drop()
    output_path = f"distinct_values_{col}.csv"

    distinct_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    print(f"Saved distinct values for column '{col}' to {output_path}")

