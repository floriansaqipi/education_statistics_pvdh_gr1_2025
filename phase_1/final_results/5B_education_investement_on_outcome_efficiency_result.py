import os
import sys

from utils.schema import discrete_schema

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]

input_file_path = ROOT / "data" / "phase_1" /  "output" / "4BF_discretization_binarization" / "4BF_discretization_binarization.csv"

output_dir_path = ROOT / "data" / "phase_1" / "output" / "5B_final_results"

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.option(
    "header", True).schema(discrete_schema).csv(input_file_path.as_posix())

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


def write_single_csv(dfin, name):
    (dfin.coalesce(1)
     .write.mode("overwrite")
     .option("header", True)
     .csv((output_dir_path / f"{name}.csv").as_posix()))


write_single_csv(by_country, "5B_education_investment_outcome_efficiency_by_country")
write_single_csv(by_region, "5B_education_investment_outcome_efficiency_by_region")
write_single_csv(by_lending, "5B_education_investment_outcome_efficiency_by_lending")
write_single_csv(by_income, "5B_education_investment_outcome_efficiency_by_income")
