import os
import sys

from utils.schema import long_schema

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql import functions as F
from pyspark.sql import Window as W

ROOT = Path(__file__).resolve().parents[1]

input_file_path = ROOT / "data" / "phase_1" / "output" / "4BB_aggregation" / "4BB_aggregation_latest.csv"
output_dir_path = ROOT / "data" / "phase_1" / "output" / "4BC_dimension_reduction"

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.option(
    "header", True).schema(long_schema).csv(input_file_path.as_posix())


finance = df.filter(F.lower(F.col("Indicator name")).rlike("expenditure|spending|% of gdp|per student|government"))
learning = df.filter(F.lower(F.col("Indicator name")).rlike("harmonized test|learning-?adjusted|lays"))

(finance.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv((output_dir_path / "4BCA_dimension_reduction_filtered_finance.csv").as_posix()))


(learning.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv((output_dir_path / "4BCB_dimension_reduction_filtered_learning.csv").as_posix()))