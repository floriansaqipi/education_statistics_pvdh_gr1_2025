import os
import sys

from utils.paths import phase2_path
from utils.schema import discrete_schema

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

input_file_path = phase2_path("3BF_discretization_binarization", "3BF_discretization_binarization.csv")
output_dir_path = phase2_path("4B_final_results")

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.option(
    "header", True).schema(discrete_schema).csv(input_file_path)

by_country = (df
              .groupBy("Country name")
              .agg(
    F.count("*").alias("n"),
    F.avg("EII_z").alias("avg_EII_z"),
    F.avg("OUTCOME_z").alias("avg_OUTCOME_z"),
    F.avg("Efficiency").alias("avg_Efficiency"),
    F.avg("EII_high").alias("share_EII_high"),
    F.avg("OUTCOME_high").alias("share_OUTCOME_high"),
    F.avg("Efficiency_high").alias("share_Efficiency_high")

)
              .filter(F.col("Country name").isNotNull())
              .orderBy("Country name"))

by_region = (df
             .groupBy("Region")
             .agg(
    F.count("*").alias("n"),
    F.avg("EII_z").alias("avg_EII_z"),
    F.avg("OUTCOME_z").alias("avg_OUTCOME_z"),
    F.avg("Efficiency").alias("avg_Efficiency"),
    F.avg("EII_high").alias("share_EII_high"),
    F.avg("OUTCOME_high").alias("share_OUTCOME_high"),
    F.avg("Efficiency_high").alias("share_Efficiency_high")
)
             .filter(F.col("Region").isNotNull())
             .orderBy("Region"))

by_lending = (df
              .groupBy("Lending category")
              .agg(
    F.count("*").alias("n"),
    F.avg("EII_z").alias("avg_EII_z"),
    F.avg("OUTCOME_z").alias("avg_OUTCOME_z"),
    F.avg("Efficiency").alias("avg_Efficiency"),
    F.avg("EII_high").alias("share_EII_high"),
    F.avg("OUTCOME_high").alias("share_OUTCOME_high"),
    F.avg("Efficiency_high").alias("share_Efficiency_high")
)
              .filter(F.col("Lending category").isNotNull())
              .orderBy("Lending category"))

by_income = (df
             .groupBy("Income group")
             .agg(
    F.count("*").alias("n"),
    F.avg("EII_z").alias("avg_EII_z"),
    F.avg("OUTCOME_z").alias("avg_OUTCOME_z"),
    F.avg("Efficiency").alias("avg_Efficiency"),
    F.avg("EII_high").alias("share_EII_high"),
    F.avg("OUTCOME_high").alias("share_OUTCOME_high"),
    F.avg("Efficiency_high").alias("share_Efficiency_high")
)
             .filter(F.col("Income group").isNotNull())
             .orderBy("Income group"))


def write_single_csv(dfin, file_path):
    (dfin.coalesce(1)
     .write.mode("overwrite")
     .option("header", True)
     .csv(file_path))

by_country_output_file_path = os.path.join(output_dir_path, "4B_education_investment_outcome_efficiency_by_country.csv")
by_region_output_file_path = os.path.join(output_dir_path, "4B_education_investment_outcome_efficiency_by_region.csv")
by_lending_output_file_path = os.path.join(output_dir_path, "4B_education_investment_outcome_efficiency_by_lending.csv")
by_income_output_file_path = os.path.join(output_dir_path, "4B_education_investment_outcome_efficiency_by_income.csv")

write_single_csv(by_country, by_country_output_file_path)
write_single_csv(by_region, by_region_output_file_path)
write_single_csv(by_lending, by_lending_output_file_path)
write_single_csv(by_income, by_income_output_file_path)
