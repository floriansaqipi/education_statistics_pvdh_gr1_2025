import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql import functions as F
from pyspark.sql import Window as W

ROOT = Path(__file__).resolve().parents[1]

input_file_path = ROOT / "data" / "output" / "4BA_transformation" / "4BA_transformation_unpivot.csv"
output_dir_path = ROOT / "data" / "output" / "4BB_aggregation"

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.option(
    "header", True).csv(input_file_path.as_posix())

wKey = W.partitionBy("economy","INDICATOR").orderBy(F.col("Year").desc())
df = (
  df
  .where(F.col("Value").isNotNull())
  .withColumn("rk", F.row_number().over(wKey))
  .where(F.col("rk")==1)
  .drop("rk")
)

(df.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv((output_dir_path / "4BB_aggregation_latest.csv").as_posix()))