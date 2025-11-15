import os

from pyspark.sql import SparkSession, functions as F

from utils.paths import phase2_path

input_file_path = phase2_path("1B_rule_based_outliers_flagging", "1B_outliers_flagged.csv")
output_dir_path = phase2_path("1C_outliers_cleaned")

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

if "is_outlier" in df.columns:
    df = df.withColumn("is_outlier", F.col("is_outlier").cast("boolean"))
else:
    df = df.withColumn("is_outlier", F.lit(False))

df = df.filter(F.col("is_outlier") == False)

df = df.dropDuplicates()

output_file = os.path.join(output_dir_path, "1C_outliers_cleaned.csv")
(df.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(output_file))

print("1C rows:", df.count())
print("1C cols:", len(df.columns))

spark.stop()
