import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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

kosovo_df = df.filter(df['economy'] == 'XKX')

non_na_condition = F.lit(False)
for col in year_cols:
    non_na_condition = non_na_condition | ((F.col(col).isNotNull()) & (F.col(col) != 'NA'))

df = kosovo_df.filter(non_na_condition)




