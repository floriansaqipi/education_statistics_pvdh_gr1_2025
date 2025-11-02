from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
in_dir = ROOT / "data" / "output" / "4CC_aggregation_gender_learning_zscore" / "4CC_country_gender_diff_abs_z"
out_dir = ROOT / "data" / "output" / "4CC_aggregation_gender_learning_zscore"

spark = (
    SparkSession.builder
    .appName("4CCB Discretization and Binarization - Gender Gap")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.option("header", True).csv(in_dir.as_posix())

df = df.withColumn("diff_abs_F_M_z", F.col("diff_abs_F_M_z").cast("double"))

df = (
    df.withColumn(
        "gap_category",
        F.when(F.col("diff_abs_F_M_z") <= 0.05, "Very small")
         .when(F.col("diff_abs_F_M_z") <= 0.15, "Moderate")
         .when(F.col("diff_abs_F_M_z") <= 0.30, "Large")
         .otherwise("Very large")
    )
)

df = df.withColumn(
    "gap_binary_high",
    F.when(F.col("diff_abs_F_M_z") >= 0.1, F.lit(1)).otherwise(F.lit(0))
)

(df.write
   .mode("overwrite")
   .option("header", True)
   .csv((out_dir / "4CCB_country_gender_diff_abs_z_binned").as_posix()))

spark.stop()

