import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.schema import schema

input_file_path = os.path.join("..", "data", "output", "1A_attributes_reorder", "1A_attributes_reorder.csv")
output_dir_path = os.path.join("..", "data", 'output', '1C_data_quality_cleaning')


spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv(
    input_file_path,
    schema=schema,
    header=True
)


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

output_path = os.path.join(output_dir_path, f"1C_quality_cleaned.csv")
df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)


df.show(truncate=False)




