import os

from utils.paths import phase2_path
from utils.schema import attributes_schema

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import QuantileDiscretizer


input_file_path = phase2_path("3BE_attribute_creation_subset", "3BE_attribute_creation_subset.csv")
output_dir_path = phase2_path("3BF_discretization_binarization")

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.option(
    "header", True).schema(attributes_schema).csv(input_file_path)

for src, dst in [("EII_z", "EII_band"), ("OUTCOME_z", "OUTCOME_band"), ("Efficiency", "Efficiency_band")]:
    qd = QuantileDiscretizer(numBuckets=3, inputCol=src, outputCol=dst, handleInvalid="skip")
    df = qd.fit(df).transform(df)


df = df.withColumn("EII_high",       (F.col("EII_band") == 2).cast("int"))
df = df.withColumn("OUTCOME_high",   (F.col("OUTCOME_band") == 2).cast("int"))
df = df.withColumn("Efficiency_high",(F.col("Efficiency_band") == 2).cast("int"))
df = df.orderBy(F.col("Efficiency").desc())

output_file = os.path.join(output_dir_path, "3BF_discretization_binarization.csv")

print(df.count())

(df.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv(output_file))


