import os

from pyspark.sql import SparkSession, functions as F
from functools import reduce
from utils.schema import schema
from utils.paths import phase1_path, phase2_path

input_file_path = phase2_path("1B_rule_based_outliers_flagging", "1B_outliers_flagged.csv")
output_dir_path = phase2_path("1C_empty_sparse_outliers_removal")

spark = SparkSession.builder.appName("Empty Sparse Outliers Removal").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.option("header", True).schema(schema).csv(input_file_path)

if "is_outlier" in df.columns:
    df = df.withColumn("is_outlier", F.col("is_outlier").cast("boolean"))
else:
    df = df.withColumn("is_outlier", F.lit(False))

df = df.replace("NA", None)

year_cols = [f"YR{y}" for y in range(1960, 2030) if f"YR{y}" in df.columns]
if year_cols:
    condition = reduce(lambda a, b: a | b, [F.col(c).isNotNull() for c in year_cols])
    df = df.filter(condition)

year_cols = [c for c in df.columns if c.startswith("YR")]
if year_cols:
    non_null_counts = df.select([F.count(F.col(c)).alias(c) for c in year_cols]).collect()[0].asDict()
    all_null_year_cols = [c for c, v in non_null_counts.items() if v == 0]
    if all_null_year_cols:
        df = df.drop(*all_null_year_cols)

cols = df.columns
year_cols = [c for c in cols if c.startswith("YR")]
non_year = [c for c in cols if c not in year_cols and c != "is_outlier"]
ordered = non_year + year_cols + (["is_outlier"] if "is_outlier" in cols else [])
df = df.select(*ordered)

output_path = os.path.join(output_dir_path, "1C_empty_sparse_outliers_cleaned.csv")
(df.coalesce(1)
   .write.option("header", True)
   .mode("overwrite")
   .csv(output_path))

df.show(20, truncate=False)
spark.stop()