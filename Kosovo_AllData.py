from pyspark.sql import SparkSession, functions as F
from pathlib import Path
import shutil, glob

spark = SparkSession.builder.appName("Kosovo_Data").getOrCreate()

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/integration_output/1A_attributes_reorder/education_reordered.csv")
)

xkx_cnt = df.filter(F.col("economy") == "XKX").count()
print("Rows with economy == XKX:", xkx_cnt)

(df.filter(F.col("economy") == "XKX")
   .groupBy("Country name")
   .count()
   .orderBy(F.desc("count"))
   .show(50, truncate=False))

(df.filter(F.lower(F.col("Country name")).like("%kosov%"))
   .select("economy","Country name","Indicator name")
   .show(50, truncate=False))

df_std = df.withColumn(
    "Country_name_std",
    F.when(F.col("economy") == "XKX", F.lit("Kosovo"))
     .when(F.lower(F.col("Country name")).like("%kosov%"), F.lit("Kosovo"))
     .otherwise(F.col("Country name"))
)

(df_std.filter(F.col("Country_name_std") == "Kosovo")
       .select("economy","Country_name_std","Indicator name")
       .show(20, truncate=False))


tmp_dir = "data/_tmp_kosovo_data"
out_file = Path("data/kosovo_data.csv")

(df_std.filter(F.col("Country_name_std") == "Kosovo")
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(tmp_dir))

part = glob.glob(f"{tmp_dir}/part-*.csv")[0]
if out_file.exists():
    out_file.unlink()
shutil.move(part, out_file)
shutil.rmtree(tmp_dir, ignore_errors=True)

print("Të dhënat për Kosovën u ruajtën në:", out_file)
