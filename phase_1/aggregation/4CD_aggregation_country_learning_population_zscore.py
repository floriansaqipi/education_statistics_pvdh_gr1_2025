import re
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import Window as W

ROOT = Path(__file__).resolve().parents[1]

in_file = ROOT / "data" / "phase_1" / "output" / "4CA_check_learning_roots" / "learning_indicators_only.csv"
out_dir = ROOT / "data" / "phase_1" / "output" / "4CD_aggregation_country_learning_population_zscore"
tmp_dir = ROOT / "tmp_spark"

out_dir.mkdir(parents=True, exist_ok=True)
tmp_dir.mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("4CD Population Mean - ZScore")
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.local.dir", tmp_dir.as_posix())
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.option("header", True).csv(in_file.as_posix())

year_cols = [c for c in df.columns if re.fullmatch(r"YR\d{4}", c) and 1970 <= int(c[2:]) <= 2023]
non_year_cols = [c for c in df.columns if not c.startswith("YR")]

stack_parts = [f"'{c[2:]}', `{c}`" for c in year_cols]
stack_expr = f"stack({len(year_cols)}, {', '.join(stack_parts)}) as (Year, Value)"

long_df = (
    df.select(*non_year_cols, *year_cols)
      .select(*non_year_cols, F.expr(stack_expr))
      .withColumn("Value", F.col("Value").cast("double"))
      .filter(F.col("Value").isNotNull())
      .filter(F.col("SEX").isin("F", "M", "T"))
)

wI = W.partitionBy("INDICATOR")

standardized = (
    long_df
    .withColumn(
        "val_std",
        F.when(F.col("UNIT_TYPE") == "NUMBER", F.log1p(F.col("Value")))
         .when((F.col("UNIT_TYPE") == "SHARE") & (F.col("Value") > 1), F.col("Value") / 100.0)
         .otherwise(F.col("Value"))
    )
    .withColumn("mu", F.avg("val_std").over(wI))
    .withColumn("sd", F.stddev("val_std").over(wI))
    .withColumn("sd_safe", F.when((F.col("sd").isNull()) | (F.col("sd") == 0), F.lit(1.0)).otherwise(F.col("sd")))
    .withColumn("z", (F.col("val_std") - F.col("mu")) / F.col("sd_safe"))
    .drop("sd_safe")
)

by_root_sex = (
    standardized.groupBy("Country name", "INDICATOR_ROOT", "SEX")
                .agg(F.avg("z").alias("root_mean_z"))
)

pivoted = (
    by_root_sex.groupBy("Country name", "INDICATOR_ROOT")
               .pivot("SEX", ["M", "F", "T"])
               .agg(F.first("root_mean_z"))
)

pop_mean_root = (
    pivoted
    .withColumn("mf_avg_z", (F.col("M") + F.col("F")) / 2.0)
    .withColumn(
        "pop_mean_root_z",
        F.when(F.col("T").isNotNull() & F.col("mf_avg_z").isNotNull(), (F.col("T") + F.col("mf_avg_z")) / 2.0)
         .when(F.col("T").isNotNull(), F.col("T"))
         .otherwise(F.col("mf_avg_z"))
    )
    .filter(F.col("pop_mean_root_z").isNotNull())
)

out_country = (
    pop_mean_root.groupBy("Country name")
                 .agg(
                     F.avg("pop_mean_root_z").alias("avg_all_indicators_population_z"),
                     F.count(F.lit(1)).alias("n_roots_used")
                 )
                 .orderBy("Country name")
)

(pop_mean_root.coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv((out_dir / "4CD_country_root_population_mean_z").as_posix()))

(out_country.coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv((out_dir / "4CD_country_population_mean_z_1970_2023").as_posix()))

spark.stop()
