import os

from pyspark.sql import SparkSession


input_file_path = os.path.join("..", "data", "Gr1_Education_Statistics_Preview.csv")
output_dir_path = os.path.join("..", "data", 'integration', '1A_attributes_reorder')


spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()


df = spark.read.format("csv") \
    .option("header", "true") \
    .load(input_file_path)

non_year_cols = [c for c in df.columns if not c.startswith("YR")]
year_cols = sorted([c for c in df.columns if c.startswith("YR")])


ordered_cols = non_year_cols + year_cols


df = df.select(*ordered_cols)

output_path = os.path.join(output_dir_path, f"1A_attributes_reordered.csv")
df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)
