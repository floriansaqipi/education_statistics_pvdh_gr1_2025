import os

from pyspark.sql import SparkSession, functions as F
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from utils.paths import phase2_path
from utils.schema import learning_indicators_schema

input_file_path = phase2_path("3CA_check_learning_roots", "learning_indicators_only")
output_dir_path = phase2_path("skewness_analysis", "")

spark = (
    SparkSession.builder
    .appName("Skewness before vs after IQR")
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
        y = c[2:]
        if y.isdigit() and min_year <= int(y) <= max_year:
            year_cols.append(c)

non_year_cols = [c for c in df.columns if c not in year_cols]

stack_parts = [f"'{c[2:]}', `{c}`" for c in year_cols]
stack_expr = f"stack({len(year_cols)}, {', '.join(stack_parts)}) as (Year, Value)"

df_long = (
    df.select(*non_year_cols, *year_cols)
      .select(*non_year_cols, F.expr(stack_expr))
      .withColumn("Year", F.col("Year").cast("int"))
      .withColumn("Value", F.col("Value").cast("double"))
      .filter(F.col("Value").isNotNull())
      .filter(F.col("Value") >= 0)
)

df_std = (
    df_long.withColumn(
        "val_std",
        F.when(F.col("UNIT_TYPE") == "NUMBER", F.log1p(F.col("Value")))
         .when((F.col("UNIT_TYPE") == "SHARE") & (F.col("Value") > 1.0), F.col("Value") / 100.0)
         .otherwise(F.col("Value"))
    )
)

before_stats = (
    df_std.groupBy("INDICATOR")
          .agg(
              F.avg("val_std").alias("mean_before"),
              F.expr("percentile_approx(val_std, 0.5)").alias("median_before"),
              F.stddev("val_std").alias("sd_before")
          )
          .withColumn(
              "sd_before_safe",
              F.when(F.col("sd_before").isNull() | (F.col("sd_before") == 0), 1.0)
               .otherwise(F.col("sd_before"))
          )
          .withColumn(
              "skew_before",
              3 * (F.col("mean_before") - F.col("median_before")) / F.col("sd_before_safe")
          )
          .select("INDICATOR", "skew_before")
)

q1_q3 = (
    df_std.groupBy("INDICATOR")
          .agg(
              F.expr("percentile_approx(val_std, 0.25)").alias("q1"),
              F.expr("percentile_approx(val_std, 0.75)").alias("q3")
          )
)

df_iqr = (
    df_std.join(q1_q3, on="INDICATOR")
          .withColumn("iqr", F.col("q3") - F.col("q1"))
          .withColumn("lower", F.col("q1") - 1.5 * F.col("iqr"))
          .withColumn("upper", F.col("q3") + 1.5 * F.col("iqr"))
          .filter((F.col("val_std") >= F.col("lower")) & (F.col("val_std") <= F.col("upper")))
)

count_before = df_std.count()
count_after = df_iqr.count()

removed_outliers = count_before - count_after
percent_removed = removed_outliers / count_before * 100

print("=== OUTLIERS REMOVED BY IQR ===")
print(f"Total observations before: {count_before}")
print(f"Total observations after : {count_after}")
print(f"Outliers removed        : {removed_outliers}")
print(f"Percentage removed      : {percent_removed:.3f}%")

after_stats = (
    df_iqr.groupBy("INDICATOR")
          .agg(
              F.avg("val_std").alias("mean_after"),
              F.expr("percentile_approx(val_std, 0.5)").alias("median_after"),
              F.stddev("val_std").alias("sd_after")
          )
          .withColumn(
              "sd_after_safe",
              F.when(F.col("sd_after").isNull() | (F.col("sd_after") == 0), 1.0)
               .otherwise(F.col("sd_after"))
          )
          .withColumn(
              "skew_after",
              3 * (F.col("mean_after") - F.col("median_after")) / F.col("sd_after_safe")
          )
          .select("INDICATOR", "skew_after")
)

comparison = (
    before_stats.alias("b")
    .join(after_stats.alias("a"), on="INDICATOR", how="inner")
    .select(
        "INDICATOR",
        F.col("b.skew_before").alias("skew_before"),
        F.col("a.skew_after").alias("skew_after")
    )
)

comparison = (
    comparison
    .withColumn("abs_skew_before", F.abs(F.col("skew_before")))
    .withColumn("abs_skew_after", F.abs(F.col("skew_after")))
)

summary = (
    comparison
    .agg(
        F.avg("abs_skew_before").alias("mean_abs_skew_before"),
        F.avg("abs_skew_after").alias("mean_abs_skew_after")
    )
    .collect()[0]
)

mean_abs_before = summary["mean_abs_skew_before"]
mean_abs_after = summary["mean_abs_skew_after"]
avg_change = mean_abs_after - mean_abs_before

improved = comparison.filter(F.col("abs_skew_after") < F.col("abs_skew_before")).count()
total = comparison.count()

print("=== SKEWNESS BEFORE vs AFTER (IQR) ===")
print(f"Mean |skew| BEFORE : {mean_abs_before}")
print(f"Mean |skew| AFTER  : {mean_abs_after}")
print(f"Average change     : {avg_change}")
print(f"Indicators improved: {improved} / {total}")

comparison_pdf = comparison.toPandas()

plt.figure(figsize=(10, 5))
plt.hist(comparison_pdf["abs_skew_before"], bins=40, alpha=0.5, label="before IQR")
plt.hist(comparison_pdf["abs_skew_after"], bins=40, alpha=0.5, label="after IQR")
plt.xlabel("|skewness| per indicator")
plt.ylabel("Number of indicators")
plt.title("Skewness before vs after IQR-based outlier handling")
plt.legend()

plot_file = os.path.join(output_dir_path, "3C_iqr_skewness_before_after.png")
plt.savefig(plot_file, bbox_inches="tight")
plt.close()

print(f"Saved skewness comparison plot to: {plot_file}")

output_file = os.path.join(output_dir_path, "3C_iqr_skewness_before_after.csv")

(
    comparison
    .select(
        "INDICATOR",
        "skew_before",
        "skew_after",
        "abs_skew_before",
        "abs_skew_after"
    )
    .coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(output_file)
)
