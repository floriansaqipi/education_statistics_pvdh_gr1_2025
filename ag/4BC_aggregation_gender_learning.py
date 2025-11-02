import os, sys, re
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parent

in_file = ROOT / "data" / "output" / "4AZ_check_learning_roots" / "learning_indicators_only.csv"
out_dir = ROOT / "data" / "output" / "4BC_aggregation_gender_learning"
tmp_dir = ROOT / "tmp_spark"
out_dir.mkdir(parents=True, exist_ok=True)
tmp_dir.mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("4BC aggregation gender learning (absolute diff M-F by ROOT)")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.local.dir", tmp_dir.as_posix())
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.option("header", True).csv(in_file.as_posix())

all_year_cols = [c for c in df.columns if re.fullmatch(r"YR\d{4}", c)]
year_cols = [c for c in all_year_cols if 1970 <= int(c[2:]) <= 2023]
non_year_cols = [c for c in df.columns if not c.startswith("YR")]

stack_parts = [f"'{c[2:]}', `{c}`" for c in year_cols]
stack_expr = f"stack({len(year_cols)}, {', '.join(stack_parts)}) as (Year, Value)"

long_df = (df.select(*non_year_cols, *year_cols)
             .select(*non_year_cols, F.expr(stack_expr))
             .withColumn("Value", F.col("Value").cast("double"))
             .filter(F.col("Value").isNotNull())
             .filter(F.col("SEX").isin("F","M"))
             .filter((F.col("Value") >= 0) & (F.col("Value") <= 1000)))

by_root = (long_df
    .groupBy("Country name", "INDICATOR_ROOT", "SEX")
    .agg(F.avg("Value").alias("root_mean")))

paired = (by_root
    .groupBy("Country name", "INDICATOR_ROOT")
    .pivot("SEX", ["M","F"])
    .agg(F.first("root_mean"))
    .filter(F.col("M").isNotNull() & F.col("F").isNotNull()))

out = (paired
    .groupBy("Country name")
    .agg(
        F.avg("M").alias("avg_all_indicators_M"),
        F.avg("F").alias("avg_all_indicators_F"),
        F.count(F.lit(1)).alias("n_indicators")
    )
    .withColumn(
        "diff_abs_F_M",
        F.abs(F.col("avg_all_indicators_F") - F.col("avg_all_indicators_M"))
    )
    .orderBy("Country name"))

(out.coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv((out_dir / "4BC_country_gender_diff_abs_1970_2023.csv").as_posix()))

print(f"U krijua: {out_dir / '4BC_country_gender_diff_abs_1970_2023.csv'}")
spark.stop()
