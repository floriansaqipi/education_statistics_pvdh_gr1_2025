import os

from pyspark.sql import SparkSession


input_file_path = os.path.join("..", "data", "Edstats_Updated.csv")
output_dir_path = os.path.join("..", "data", 'output', 'distinct_column_values')


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
    distinct_df = df.select(col).distinct().na.drop()
    output_path = os.path.join(output_dir_path, f"distinct_values_{col}.csv")

    distinct_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    print(f"Saved distinct values for column '{col}' to {output_path}")

