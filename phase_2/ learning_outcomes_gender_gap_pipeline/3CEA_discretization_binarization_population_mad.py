import os

from pyspark.sql import SparkSession, functions as F
from utils.schema import population_learning_mad_schema
from utils.paths import phase2_path

input_file_path = phase2_path("3CE_aggregation_population_learning_mad","3CE_country_population_mean_mad_1970_2023.csv")
output_dir_path = phase2_path("3CEA_discretization_binarization_population_mad")

spark = (
    SparkSession.builder
    .appName("3CEA discretization and binarization population MAD")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.read
    .option("header", True)
    .schema(population_learning_mad_schema)
    .csv(input_file_path)
)

df = df.withColumn(
    "avg_all_indicators_population_mad",
    F.col("avg_all_indicators_population_mad").cast("double")
)

z = F.col("avg_all_indicators_population_mad")

df = df.withColumn(
    "performance_category_mad",
    F.when(z <= -1.0, "Very low")
     .when(z <= -0.3, "Low")
     .when(z <=  0.3, "Moderate")
     .when(z <=  1.0, "High")
     .otherwise("Very high")
)

df = df.withColumn(
    "performance_binary_mad",
    F.when(z >= 0, 1).otherwise(0)
)

os.makedirs(output_dir_path, exist_ok=True)

output_file = os.path.join(output_dir_path,"3CEA_country_population_mean_mad_binned.csv")

(
    df.coalesce(1)
      .write
      .option("header", True)
      .mode("overwrite")
      .csv(output_file)
)

spark.stop()
