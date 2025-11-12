import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
in_file = ROOT / "data" / "phase_1" / "output" / "2A_data_missing_values_handling" / "2A_missing_values_cleaned.csv"
out_dir = ROOT / "data" / "phase_1" / "output" / "2B_data_cleaning"

spark = (SparkSession.builder
    .appName("2B Data Cleaning")
    .master("local[*]")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.option("header", True).csv(in_file.as_posix())

if "is_valid" in df.columns:
    df = df.withColumn("is_valid", F.col("is_valid").cast("boolean"))
else:
    df = df.withColumn("is_valid", F.lit(True))

df = df.filter(F.col("is_valid") == True)

df = df.dropDuplicates()

out_path = (out_dir / "2B_cleaned.csv").as_posix()
(df.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv(out_path))

print("2B rows:", df.count())
print("2B cols:", len(df.columns))

spark.stop()
