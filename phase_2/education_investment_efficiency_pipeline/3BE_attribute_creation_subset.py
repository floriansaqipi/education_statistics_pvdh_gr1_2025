import os
import sys

from utils.paths import phase2_path
from utils.schema import long_schema, normalized_schema, normalized_outlier_schema

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

input_file_path_finance = phase2_path("3BD_transformation_normalization", "3BDA_transformation_normalized_finance.csv")
input_file_path_learning = phase2_path("3BD_transformation_normalization", "3BDB_transformation_normalized_learning.csv")

output_dir_path = phase2_path("3BE_attribute_creation_subset")

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

finance_z = spark.read.option(
    "header", True).schema(normalized_outlier_schema).csv(input_file_path_finance)

learning_z = spark.read.option(
    "header", True).schema(normalized_outlier_schema).csv(input_file_path_learning)

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

learning_output_file = os.path.join(output_dir_path, "3BE_attribute_creation_subset.csv")

print(indices.count())

(indices.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv(learning_output_file))
