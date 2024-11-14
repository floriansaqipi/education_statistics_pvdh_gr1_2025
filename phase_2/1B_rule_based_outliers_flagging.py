import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.schema import schema

ROOT = Path(__file__).resolve().parents[1]

input_file_path = ROOT / "data" / "phase_1" / "output" / "1A_attributes_reorder" / "1A_attributes_reordered.csv"
output_dir_path = ROOT / "data" / "phase_2" / "output" / "1B_rule_based_outliers_flagging"

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.option(
    "header", True).schema(schema).csv(input_file_path.as_posix())

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

output_path = os.path.join(output_dir_path, f"1B_outliers_flagged.csv")
df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)

df.show(truncate=False)




