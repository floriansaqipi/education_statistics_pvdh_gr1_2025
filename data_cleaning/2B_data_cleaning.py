import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession, functions as F, Window

ROOT = Path(__file__).resolve().parents[1]
in_file = ROOT / "data" / "output" / "2A_data_missing_values_handling" / "2A_missing_values_cleaned.csv"
out_dir = ROOT / "data" / "output" / "2B_data_cleaning"

spark = (SparkSession.builder
    .appName("2B Data Cleaning")
    .master("local[1]")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.option("header", True).csv(in_file.as_posix())

df = df.filter(F.col("is_valid") == True)
df = df.dropDuplicates()

year_cols = [c for c in df.columns if c.startswith("YR")]
key_cols = [c for c in df.columns if c not in year_cols]

if year_cols:
    non_nulls = sum([F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in year_cols])
    df = df.withColumn("__nn_years", non_nulls)
    w = Window.partitionBy(*key_cols).orderBy(F.col("__nn_years").desc())
    df = (df
          .withColumn("__rk", F.row_number().over(w))
          .filter(F.col("__rk") == 1)
          .drop("__rk", "__nn_years"))

out_path = (out_dir / "2B_cleaned.csv").as_posix()
(df.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv(out_path))

print("2B rows:", df.count())
print("2B cols:", len(df.columns))

spark.stop()
