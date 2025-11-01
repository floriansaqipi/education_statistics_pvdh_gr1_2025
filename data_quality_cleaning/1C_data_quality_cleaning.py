import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.schema import schema

ROOT = Path(__file__).resolve().parents[1]

input_file_path = ROOT / "data" / "output" / "1A_attributes_reorder" / "1A_attributes_reordered.csv"
output_dir_path = ROOT / "data" / "output" / "1C_data_quality_cleaning"

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

df = df.withColumn("is_valid", F.lit(True))

df = df.withColumn(
    "economy",
    F.when(F.length(F.col("economy")) == 3, F.col("economy")).otherwise(F.lit("NA"))
).withColumn(
    "is_valid",
    F.when(F.length(F.col("economy")) != 3, F.lit(False)).otherwise(F.col("is_valid"))
)

df = df.withColumn(
    "SEX",
    F.when(F.col("SEX").isin(["M", "F", "_T", "NA"]), F.col("SEX")).otherwise(F.lit("NA"))
).withColumn(
    "is_valid",
    F.when(~F.col("SEX").isin(["M", "F", "_T", "NA"]), F.lit(False)).otherwise(F.col("is_valid"))
)

df = df.withColumn(
    "URBANIZATION",
    F.when(F.col("URBANIZATION").isin(["URB", "RUR", "_T", "NA"]), F.col("URBANIZATION")).otherwise(F.lit("NA"))
).withColumn(
    "is_valid",
    F.when(~F.col("URBANIZATION").isin(["URB", "RUR", "_T", "NA"]), F.lit(False)).otherwise(F.col("is_valid"))
)

df.show(truncate=False)




