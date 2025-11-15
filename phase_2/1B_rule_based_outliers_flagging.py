import os

from pyspark.sql import SparkSession, functions as F

from utils.schema import schema
from utils.paths import phase1_path, phase2_path

input_file_path = phase1_path("1A_attributes_reorder", "1A_attributes_reordered.csv")
output_dir_path = phase2_path("1B_rule_based_outliers_flagging")

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv(input_file_path)
)

df = df.withColumn(
    "URBANIZATION",
    F.when(F.col("URBANIZATION") == "RURURUR", "RUR").otherwise(F.col("URBANIZATION"))
)

df = df.withColumn("is_outlier", F.lit(False))

df = df.withColumn(
    "economy",
    F.when(F.length(F.col("economy")) == 3, F.col("economy")).otherwise(F.lit("NA"))
).withColumn(
    "is_outlier",
    F.when(F.length(F.col("economy")) != 3, F.lit(True)).otherwise(F.col("is_outlier"))
)

df = df.withColumn(
    "SEX",
    F.when(F.col("SEX").isin(["M", "F", "_T", "NA"]), F.col("SEX")).otherwise(F.lit("NA"))
).withColumn(
    "is_outlier",
    F.when(~F.col("SEX").isin(["M", "F", "_T", "NA"]), F.lit(True)).otherwise(F.col("is_outlier"))
)

df = df.withColumn(
    "URBANIZATION",
    F.when(F.col("URBANIZATION").isin(["URB", "RUR", "_T", "NA"]), F.col("URBANIZATION")).otherwise(F.lit("NA"))
).withColumn(
    "is_outlier",
    F.when(~F.col("URBANIZATION").isin(["URB", "RUR", "_T", "NA"]), F.lit(True)).otherwise(F.col("is_outlier"))
)

output_file = os.path.join(output_dir_path, "1B_outliers_flagged.csv")
df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_file)

df.show(truncate=False)
