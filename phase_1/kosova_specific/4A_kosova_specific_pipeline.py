import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from utils.schema import integrated_schema, long_schema

ROOT = Path(__file__).resolve().parents[1]

input_file_path = ROOT / "data" / "output" / "4BA_transformation" / "4BA_transformation_unpivot.csv"
output_dir_path = ROOT / "data" / "output" / "5A_kosova_specific"

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[2]") \
    .getOrCreate()

df = spark.read.option(
    "header", True).schema(long_schema).csv(input_file_path.as_posix())

df = df.filter((F.col("economy") == "XKX") | (F.col("Country name") == "Kosovo"))


lit_regex = "(literacy|reading|math|science|proficiency|test)"
df_lit = df.filter(F.lower(F.col("Indicator name")).rlike(lit_regex)) \
    .where(F.col("Value").isNotNull())

w_latest = W.partitionBy("INDICATOR").orderBy(F.col("Year").desc())
df_latest = (df_lit
  .withColumn("rk", F.row_number().over(w_latest))
  .where(F.col("rk")==1)
  .drop("rk"))

df_urb = (df_lit
            .filter(F.col("URBANIZATION").isin("URB", "RUR"))
            .select("INDICATOR", "Indicator name", "Year", "URBANIZATION", "Value"))

folded = (df_lit
  .withColumn("IND_BASE", F.regexp_replace(F.col("INDICATOR"), r'\.(U|R)$', ""))
  .withColumn(
      "URB_LBL",
      F.when(F.col("INDICATOR").rlike(r'\.U$'), "U")
       .when(F.col("INDICATOR").rlike(r'\.R$'), "R")
       .when(F.lower(F.col("Indicator name")).rlike(r',\s*urban\s*$'), "U")
       .when(F.lower(F.col("Indicator name")).rlike(r',\s*rural\s*$'), "R")
       .when(F.col("URBANIZATION").isin("U","URB"), "U")
       .when(F.col("URBANIZATION").isin("R","RUR"), "R")
       .when(F.lower(F.col("URBANIZATION"))=="urban", "U")
       .when(F.lower(F.col("URBANIZATION"))=="rural", "R")
  )

  .withColumn("NAME_BASE", F.regexp_replace(F.col("Indicator name"), r',\s*(Urban|Rural)\s*$', ""))
  .filter(F.col("URB_LBL").isin("U","R"))
)


df_pivoted = (folded
  .groupBy("IND_BASE","NAME_BASE","Year")
  .pivot("URB_LBL", ["U","R"])
  .agg(F.first("Value"))
  .withColumn("gap_U_minus_R", F.col("U") - F.col("R"))
  .withColumn("abs_gap_urb", F.abs(F.col("gap_U_minus_R")))
  .orderBy("IND_BASE","Year")
)

pretty = (df_pivoted
  .withColumn("U_pct", F.col("U")*100)
  .withColumn("R_pct", F.col("R")*100)
  .withColumn("gap_pp", (F.col("gap_U_minus_R")*100))
  .orderBy(F.col("abs_gap_urb").desc()))


(pretty.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv((output_dir_path / "5AA_kosova_urban_gap.csv").as_posix()))

w_desc = W.partitionBy("INDICATOR").orderBy(F.col("Year").desc())
xkx_recent = (df_lit
    .withColumn("rn", F.row_number().over(w_desc))
    .where(F.col("rn") <= 10)
    .drop("rn")
    .withColumn("YearD", F.col("Year").cast("double")))

trend = (xkx_recent.groupBy("INDICATOR","Indicator name")
    .agg(F.covar_samp("YearD","Value").alias("cov"),
         F.variance("YearD").alias("varx"))
    .withColumn("slope_per_year", F.when((F.col("varx").isNull()) | (F.col("varx")==0), F.lit(None))
                                   .otherwise(F.col("cov")/F.col("varx")))
    .orderBy(F.col("slope_per_year").desc_nulls_last()))

trend = trend.filter(F.col("cov").isNotNull())

(trend.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv((output_dir_path / "5AB_kosova_indicators_slope.csv").as_posix()))
