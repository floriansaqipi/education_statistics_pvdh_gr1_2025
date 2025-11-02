import os
import sys

from utils.schema import attributes_schema

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import QuantileDiscretizer

ROOT = Path(__file__).resolve().parents[1]

input_file_path = ROOT / "data" / "output" / "4BE_attribute_creation_subset" / "4BE_attribute_creation_subset.csv"

output_dir_path = ROOT / "data" / "output" / "4BF_discretization_binarization"

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.option(
    "header", True).schema(attributes_schema).csv(input_file_path.as_posix())

for src, dst in [("EII_z", "EII_band"), ("OUTCOME_z", "OUTCOME_band"), ("Efficiency", "Efficiency_band")]:
    qd = QuantileDiscretizer(numBuckets=3, inputCol=src, outputCol=dst, handleInvalid="skip")
    df = qd.fit(df).transform(df)


df = df.withColumn("EII_high",       (F.col("EII_band") == 2).cast("int"))
df = df.withColumn("OUTCOME_high",   (F.col("OUTCOME_band") == 2).cast("int"))
df = df.withColumn("Efficiency_high",(F.col("Efficiency_band") == 2).cast("int"))
df = df.orderBy("Efficiency")

(df.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv((output_dir_path / "4BF_discretization_binarization.csv").as_posix()))


