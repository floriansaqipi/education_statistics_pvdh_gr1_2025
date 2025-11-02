import os, sys, re
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import Window as W

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, ROOT.as_posix())

in_file = ROOT / "data" / "output" / "4CA_check_learning_roots" / "learning_indicators_only.csv"
out_dir = ROOT / "data" / "output" / "4CC_aggregation_gender_learning_zscore"
tmp_dir = ROOT / "tmp_spark"
out_dir.mkdir(parents=True, exist_ok=True)
tmp_dir.mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("4CC aggregation gender learning (z-score by INDICATOR)")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.local.dir", tmp_dir.as_posix())
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "96")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.option("header", True).csv(in_file.as_posix())

year_cols = [
    c for c in df.columns
    if c.startswith("YR") and c[2:].isdigit() and 1970 <= int(c[2:]) <= 2023
]
non_year_cols = [c for c in df.columns if c not in year_cols]

stack_pairs = [f"'{c[2:]}', `{c}`" for c in year_cols]
stack_expr = f"stack({len(year_cols)}, {', '.join(stack_pairs)}) as (Year, Value)"

df_long = (
    df.select(*non_year_cols, *year_cols)
      .select(*non_year_cols, F.expr(stack_expr))
      .withColumn("Year", F.col("Year").cast("int"))
      .withColumn("Value", F.col("Value").cast("double"))
)

df_long = df_long.withColumn(
    "SEX_NORM",
    F.when(F.upper(F.trim(F.col("SEX"))).isin("M","MALE"), "M")
     .when(F.upper(F.trim(F.col("SEX"))).isin("F","FEMALE"), "F")
     .otherwise(F.lit(None))
).filter(F.col("SEX_NORM").isNotNull())

def standardize(df_in):
    wI = W.partitionBy("INDICATOR")
    return (
        df_in
        .withColumn(
            "val_std",
            F.when(F.col("UNIT_TYPE") == "NUMBER", F.log1p(F.col("Value")))
             .when((F.col("UNIT_TYPE") == "SHARE") & (F.col("Value") > 1.0), F.col("Value")/100.0)
             .otherwise(F.col("Value"))
        )
        .withColumn("mu", F.avg("val_std").over(wI))
        .withColumn("sd", F.stddev("val_std").over(wI))
        .withColumn("sd_safe", F.when(F.col("sd").isNull() | (F.col("sd")==0), F.lit(1.0)).otherwise(F.col("sd")))
        .withColumn("z", (F.col("val_std") - F.col("mu")) / F.col("sd_safe"))
        .drop("sd_safe")
    )

df_z = standardize(df_long).filter(F.col("z").isNotNull())

by_root = (
    df_z.groupBy("Country name", "INDICATOR_ROOT", "SEX_NORM")
        .agg(F.avg("z").alias("root_mean_z"))
)

paired = (
    by_root.groupBy("Country name", "INDICATOR_ROOT")
           .pivot("SEX_NORM", ["M","F"])
           .agg(F.first("root_mean_z"))
           .filter(F.col("M").isNotNull() & F.col("F").isNotNull())
)

out_country = (
    paired.groupBy("Country name")
          .agg(
              F.avg("M").alias("avg_all_roots_z_M"),
              F.avg("F").alias("avg_all_roots_z_F"),
              F.count(F.lit(1)).alias("n_roots_used")
          )
          .withColumn("diff_abs_F_M_z", F.abs(F.col("avg_all_roots_z_F") - F.col("avg_all_roots_z_M")))
          .orderBy("Country name")
)

(out_country
 .write.mode("overwrite").option("header", True)
 .csv((out_dir / "4CC_country_gender_diff_abs_z").as_posix()))

spark.stop()
