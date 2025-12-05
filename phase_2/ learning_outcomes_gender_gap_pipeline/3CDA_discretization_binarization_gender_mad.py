import os

from pyspark.sql import SparkSession, functions as F
from utils.schema import gender_gap_schema
from utils.paths import phase2_path

input_file_path = phase2_path("3CD_aggregation_gender_learning_mad","3CD_country_gender_diff_abs_mad.csv")
output_dir_path = phase2_path("3CDA_discretization_binarization_gender_mad")

spark = SparkSession.builder \
    .appName("3CDA discretization and binarization MAD") \
    .master("local[*]") \
    .getOrCreate()

df = (
    spark.read
    .option("header", True)
    .schema(gender_gap_schema)
    .csv(input_file_path)
)

df = df.withColumn("diff_abs_F_M_mad", F.col("diff_abs_F_M_mad").cast("double"))

df = df.withColumn(
    "gap_category_mad",
    F.when(F.col("diff_abs_F_M_mad") <= 0.02, "Very small")
     .when(F.col("diff_abs_F_M_mad") <= 0.06, "Small")
     .when(F.col("diff_abs_F_M_mad") <= 0.12, "Moderate")
     .otherwise("Very large")
)

df = df.withColumn(
    "gap_binary_high_mad",
    F.when(F.col("diff_abs_F_M_mad") >= 0.06, 1).otherwise(0)
)

os.makedirs(output_dir_path, exist_ok=True)

output_file = os.path.join(output_dir_path, "3CDA_country_gender_diff_abs_mad_binned.csv")

df.coalesce(1).write \
    .option("header", True) \
    .mode("overwrite") \
    .csv(output_file)

spark.stop()
