import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.schema import schema

input_file_path = os.path.join("..", "data", "output","1A_attributes_reorder","1A_attributes_reorder.csv")


spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv(
    input_file_path,
    schema=schema,
    header=True,
    nullValue="NA"
)

search_str = 'teachers'

# Get distinct values for 'economy'
filtered_df = df.filter(F.col("economy").like(f"%{search_str}%"))
filtered_df.show(n=filtered_df.count(), truncate=False)


