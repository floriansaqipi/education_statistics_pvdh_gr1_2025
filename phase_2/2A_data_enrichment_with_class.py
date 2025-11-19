import os

from pyspark.sql import SparkSession, functions as F

from utils.paths import phase2_path, data_path

input_file_path = phase2_path("1D_outliers_cleaned", "1D_outliers_cleaned.csv")
class_file_path = data_path("CLASS_2025_10_07.csv")
output_dir_path = phase2_path("2A_data_enrichment_with_class")

spark = (
    SparkSession.builder
    .appName("CSV to Dataset")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

m = (
    spark.read
    .option("header", True)
    .csv(input_file_path)
)

c = (
    spark.read
    .option("header", True)
    .csv(class_file_path)
)

c = (
    c.select(F.col("Economy").alias("country_name_join"), "Code", "Region", "Income group", "Lending category")
    .withColumn("code", F.upper(F.trim(F.col("Code"))))
    .dropDuplicates(["code"])
)

m2 = m.withColumn("economy_join", F.upper(F.trim(F.col("economy"))))
joined = m2.join(c, F.col("economy_join") == F.col("code"), "left")
joined = joined.withColumn("Country name", F.col("country_name_join"))

keep_main = [col for col in m.columns if col != "is_outlier" and not col.startswith("YR")]
keep_class = ["Region", "Income group", "Lending category"]
year_cols = [c for c in joined.columns if c.startswith("YR")]

ordered_cols = (
        [F.col(f"`{c}`") for c in keep_main] +
        [F.col(c) for c in keep_class] +
        [F.col("is_outlier")] +
        [F.col(c) for c in year_cols]
)

out = joined.select(*ordered_cols)
if "Code" in out.columns:
    out = out.drop("Code")
if "code" in out.columns:
    out = out.drop("code")
if "economy_join" in out.columns:
    out = out.drop("economy_join")
if "country_name_join" in out.columns:
    out = out.drop("country_name_join")

out.show(truncate=False)

output_file = os.path.join(output_dir_path, "2A_enriched_with_class.csv")

(
    out.coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(output_file)
)

spark.stop()
