import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from utils.schema import schema

input_file_path = os.path.join("../..", "data", "output", "1A_attributes_reorder", "1A_attributes_reordered.csv")


spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv(
    input_file_path,
    schema=schema,
    header=True
)

df.count()
