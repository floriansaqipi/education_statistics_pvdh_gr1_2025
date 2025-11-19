import os

from pyspark.sql import SparkSession, functions as F

from utils.paths import phase2_path

input_file_path = phase2_path("1C_empty_sparse_outliers_removal", "1C_empty_sparse_outliers_cleaned.csv")
output_dir_path = phase2_path("1D_outliers_cleaned")

spark = (
    SparkSession.builder
    .appName("CSV to Dataset")
    .master("local[*]")
    .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.read
    .option("header", True)
    .csv(input_file_path)
)

print(df.count())

if "is_outlier" in df.columns:
    df = df.withColumn("is_outlier", F.col("is_outlier").cast("boolean"))
else:
    df = df.withColumn("is_outlier", F.lit(False))

df = df.filter(F.col("is_outlier") == False)

df = df.dropDuplicates()

print(df.count())

output_file = os.path.join(output_dir_path, "1D_outliers_cleaned.csv")
(df.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(output_file))

print("1D rows:", df.count())
print("1D cols:", len(df.columns))

spark.stop()
