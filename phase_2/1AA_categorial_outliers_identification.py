import os

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from utils.paths import phase1_path, phase2_path
from utils.schema import schema

input_file_path = phase1_path("1A_attributes_reorder", "1A_attributes_reordered.csv")
output_dir_path = phase2_path("1A_categorical_outliers_distinct_values")

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(input_file_path)

df = df.filter(
    (F.col("SEX") == "students can’t really change how intelligent they are\"\"\"")
    | (F.col("UNIT_MEASURE") == "SE.PRM.TINM.8")
    | (F.col("economy") == "(De Facto) Percent of teachers that agree or strongly agrees with \"\"To be honest")
    | (F.col("URBANIZATION") == "RURURUR")
    | (F.col("INDICATOR") == "System.IO.MemoryStream")
)

df.show(truncate=False, n=df.count())

print(df.count())
