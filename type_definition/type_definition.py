import os

from pyspark.sql import SparkSession


file_path = os.path.join("..", "data", "Edstats_Updated.csv")

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()


df = spark.read.format("csv") \
    .option("header", "true") \
    .load(file_path)


schema = df.schema

df.printSchema()


df.show(5)
