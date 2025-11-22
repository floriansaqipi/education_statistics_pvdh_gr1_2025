import os


from utils.paths import phase2_path
from utils.schema import long_outlier_schema


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

input_file_path = phase2_path("3BA_transformation", "3BA_transformation_unpivot.csv")
output_dir_path = phase2_path("3BB_aggregation")

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[1]") \
    .getOrCreate()

df = spark.read.option(
    "header", True).schema(long_outlier_schema).csv(input_file_path)

wKey = W.partitionBy("economy", "INDICATOR").orderBy(F.col("Year").desc())
df = (
    df
    .where(F.col("Value").isNotNull())
    .withColumn("rk", F.row_number().over(wKey))
    .where(F.col("rk") == 1)
    .drop("rk")
)

output_file = os.path.join(output_dir_path, "3BB_aggregation_latest.csv")

print(df.count())

(df.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(output_file))
