import os
import sys

from utils.schema import long_schema, normalized_schema

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]

input_file_path_finance = ROOT / "data" / "phase_1" / "output" / "4BD_transformation_normalization" / "4BDA_transformation_normalized_finance.csv"
input_file_path_learning = ROOT / "data" / "phase_1" / "output" / "4BD_transformation_normalization" / "4BDB_transformation_normalized_learning.csv"

output_dir_path = ROOT / "data" / "phase_1" / "output" / "4BE_attribute_creation_subset"

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

finance_z = spark.read.option(
    "header", True).schema(normalized_schema).csv(input_file_path_finance.as_posix())

learning_z = spark.read.option(
    "header", True).schema(normalized_schema).csv(input_file_path_learning.as_posix())


education_investment_indicator = (finance_z.groupBy("economy","Country name","Region","Income group","Lending category")
             .agg(F.avg("z").alias("EII_z"),
                  F.count("*").alias("k_fin")))

outcome = (learning_z.groupBy("economy")
             .agg(F.avg("z").alias("OUTCOME_z"),
                  F.count("*").alias("k_out")))


indices = education_investment_indicator.join(outcome, "economy", "inner")
min_eii = indices.agg(F.min("EII_z").alias("m")).collect()[0]["m"]
indices = indices.withColumn("EII_pos", F.col("EII_z") - F.lit(min_eii) + F.lit(1.0))
indices = indices.withColumn("Efficiency", F.col("OUTCOME_z")/F.col("EII_pos"))

indices.show(truncate=False)

(indices.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv((output_dir_path / "4BE_attribute_creation_subset.csv").as_posix()))
