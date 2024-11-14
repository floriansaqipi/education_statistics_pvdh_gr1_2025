import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
in_file = ROOT / "data" / "phase_2" / "output" / "1B_rule_based_outliers_flagging" / "1B_outliers_flagged.csv"
out_dir = ROOT / "data" / "phase_2" / "output" / "1C_outliers_cleaned"

spark = (SparkSession.builder
    .appName("CSV to Dataset")
    .master("local[*]")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.option("header", True).csv(in_file.as_posix())

if "is_outlier" in df.columns:
    df = df.withColumn("is_outlier", F.col("is_outlier").cast("boolean"))
else:
    df = df.withColumn("is_outlier", F.lit(False))

df = df.filter(F.col("is_outlier") == False)

df = df.dropDuplicates()

out_path = (out_dir / "1C_outliers_cleaned.csv").as_posix()
(df.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv(out_path))

print("1C rows:", df.count())
print("1C cols:", len(df.columns))

spark.stop()
