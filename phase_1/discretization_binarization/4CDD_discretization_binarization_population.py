from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]

base_dir = ROOT / "data" / "phase_1" / "output" / "4CD_aggregation_country_learning_population_zscore"
in_dir   = base_dir / "4CD_country_population_mean_z_1970_2023"   # <- input folder (si në 4CD)
out_dir  = base_dir / "4CDD_discretization_binarization_population"
tmp_dir  = ROOT / "tmp_spark"

out_dir.mkdir(parents=True, exist_ok=True)
tmp_dir.mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("4CDD_discretization_binarization_population")
    .master("local[*]")
    .config("spark.local.dir", tmp_dir.as_posix())
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.option("header", True).csv(in_dir.as_posix(), inferSchema=True)

z = F.col("avg_all_indicators_population_z")

df2 = (
    df.withColumn(
        "performance_category",
        F.when(z <= -1.0, "Very low")
         .when(z <= -0.3, "Low")
         .when(z <=  0.3, "Moderate")
         .when(z <=  1.0, "High")
         .otherwise("Very high")
    )
    .withColumn("performance_binary", F.when(z >= 0, 1).otherwise(0))
   
)

(df2.coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(out_dir.as_posix()))

spark.stop()
print(f"Saved to: {out_dir.as_posix()}")
