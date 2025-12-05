import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql import Window as W
from utils.paths import phase2_path
from utils.schema import learning_indicators_schema


input_file_path = phase2_path("3CA_check_learning_roots", "learning_indicators_only")
output_dir_path = phase2_path("3CB_aggregation_gender_learning_zscore")

spark = (
    SparkSession.builder
    .appName("3CB aggregation gender learning")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.read
    .option("header", True)
    .schema(learning_indicators_schema)
    .csv(input_file_path)
)

min_year = 1970
max_year = 2023

year_cols = []
for c in df.columns:
    if c.startswith("YR"):
        year_str = c[2:]
        if year_str.isdigit():
            year_int = int(year_str)
            if min_year <= year_int <= max_year:
                year_cols.append(c)

non_year_cols = [c for c in df.columns if c not in year_cols]

stack_parts = []
for c in year_cols:
    year_str = c[2:]
    stack_parts.append(f"'{year_str}', `{c}`")

stack_expr = f"stack({len(year_cols)}, {', '.join(stack_parts)}) as (Year, Value)"

df_long = (
    df.select(*non_year_cols, *year_cols)
    .select(*non_year_cols, F.expr(stack_expr))
    .withColumn("Year", F.col("Year").cast("int"))
    .withColumn("Value", F.col("Value").cast("double"))
    .filter(F.col("Value").isNotNull())
    .filter(F.col("Country name").isNotNull())
    .filter(F.length(F.col("Country name")) > 0)
)

df_long = df_long.withColumn(
    "SEX_NORM",
    F.when(F.upper(F.trim(F.col("SEX"))).isin("M", "MALE"), "M")
    .when(F.upper(F.trim(F.col("SEX"))).isin("F", "FEMALE"), "F")
    .otherwise(F.lit(None))
).filter(F.col("SEX_NORM").isNotNull())

df_long = df_long.filter(F.col("Value") >= 0)


def standardize(df_in):
    wI = W.partitionBy("INDICATOR")
    return (
        df_in
        .withColumn(
            "val_std",
            F.when(F.col("UNIT_TYPE") == "NUMBER", F.log1p(F.col("Value")))
            .when((F.col("UNIT_TYPE") == "SHARE") & (F.col("Value") > 1.0), F.col("Value") / 100.0)
            .otherwise(F.col("Value"))
        )
        .withColumn("mu", F.avg("val_std").over(wI))
        .withColumn("sd", F.stddev("val_std").over(wI))
        .withColumn(
            "sd_safe",
            F.when(F.col("sd").isNull() | (F.col("sd") == 0), F.lit(1.0)).otherwise(F.col("sd"))
        )
        .withColumn("z", (F.col("val_std") - F.col("mu")) / F.col("sd_safe"))
        .drop("sd_safe")
    )


df_z = standardize(df_long).filter(F.col("z").isNotNull())
df_z = df_z.filter(F.abs(F.col("z")) <= 3)

by_root = (
    df_z.groupBy("Country name", "INDICATOR_ROOT", "SEX_NORM")
    .agg(F.avg("z").alias("root_mean_z"))
)

paired = (
    by_root.groupBy("Country name", "INDICATOR_ROOT")
    .pivot("SEX_NORM", ["M", "F"])
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
    .withColumn(
        "diff_abs_F_M_z",
        F.abs(F.col("avg_all_roots_z_F") - F.col("avg_all_roots_z_M"))
    )
    .orderBy("Country name")
)

output_file = os.path.join(output_dir_path, "3CB_country_gender_diff_abs_z.csv")

(
    out_country.coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(output_file)
)

spark.stop()
