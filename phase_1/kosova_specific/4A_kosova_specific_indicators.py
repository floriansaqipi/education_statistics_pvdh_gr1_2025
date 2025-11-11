import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql import functions as F
from utils.schema import schema, integrated_schema


ROOT = Path(__file__).resolve().parents[1]

input_file_path = ROOT / "data" / "output" / "3A_integration" / "3A_integrated_with_class.csv"
output_dir_path = ROOT / "data" / "output" / "4A_kosova_specific"

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.option(
    "header", True).schema(integrated_schema).csv(input_file_path.as_posix())

kosovo_inds = (
    df
    .filter( (F.col("economy") == "XKX") | (F.col("Country name") == "Kosovo") )
    .select("INDICATOR", "Indicator name", "name")
    .distinct()
    .orderBy("INDICATOR")
)

(kosovo_inds.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv((output_dir_path / "4A_kosova_specific.csv").as_posix()))