import os

from pyspark.sql import SparkSession, functions as F

from utils.paths import phase2_path
from utils.schema import learning_indicators_schema

input_file_path = phase2_path("3CA_check_learning_roots", "learning_indicators_only")
output_dir_path = phase2_path("3CAA_skewness_diagnostics", "")

spark = (
    SparkSession.builder
    .appName("3CAA skewness diagnostics by INDICATOR")
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
)

stats = (
    df_long.groupBy("INDICATOR")
           .agg(
               F.avg("Value").alias("mean_val"),
               F.expr("percentile_approx(Value, 0.5)").alias("median_val"),
               F.stddev("Value").alias("sd_val")
           )
           .withColumn(
               "sd_safe",
               F.when(F.col("sd_val").isNull() | (F.col("sd_val") == 0), F.lit(1.0))
                .otherwise(F.col("sd_val"))
           )
           .withColumn(
               "skew_pearson",
               3 * (F.col("mean_val") - F.col("median_val")) / F.col("sd_safe")
           )
           .drop("sd_safe")
)

stats = (
    stats
    .withColumn("abs_skew", F.abs(F.col("skew_pearson")))
    .withColumn(
        "skew_category",
        F.when(F.col("abs_skew") < 0.5, "nearly_symmetric")
         .when(F.col("abs_skew") < 1.0, "moderately_skewed")
         .when(F.col("abs_skew") < 2.0, "highly_skewed")
         .otherwise("very_highly_skewed")
    )
    .withColumn(
        "preferred_method",
        F.when(F.col("abs_skew") < 0.5, "zscore")
         .when(F.col("abs_skew") < 1.0, "iqr")
         .when(F.col("abs_skew") < 2.0, "mad")
         .otherwise("mad")
    )
)

summary_row = (
    stats.agg(
        F.avg("skew_pearson").alias("mean_skew"),
        F.avg("abs_skew").alias("mean_abs_skew")
    )
    .collect()[0]
)

mean_skew = summary_row["mean_skew"]
mean_abs_skew = summary_row["mean_abs_skew"]

if mean_abs_skew < 0.5:
    global_cat = "nearly_symmetric"
    global_method = "zscore"
elif mean_abs_skew < 1.0:
    global_cat = "moderately_skewed"
    global_method = "iqr"
elif mean_abs_skew < 2.0:
    global_cat = "highly_skewed"
    global_method = "mad"
else:
    global_cat = "very_highly_skewed"
    global_method = "mad"

print("Global skewness diagnostics:")
print(f"  Mean skewness:      {mean_skew}")
print(f"  Mean |skewness|:    {mean_abs_skew}")
print(f"  → Global category (by |skewness|): {global_cat}")
print(f"  → Globally reasonable method:      {global_method}")

category_counts_rows = (
    stats.groupBy("skew_category", "preferred_method")
         .agg(F.count("*").alias("n_indicators"))
         .collect()
)

counts = {}
for row in category_counts_rows:
    key = (row["skew_category"], row["preferred_method"])
    counts[key] = row["n_indicators"]

nearly_sym = counts.get(("nearly_symmetric", "zscore"), 0)
moderate = counts.get(("moderately_skewed", "iqr"), 0)
high_plus_very = (
    counts.get(("highly_skewed", "mad"), 0)
    + counts.get(("very_highly_skewed", "mad"), 0)
)

print("Skewness categories and preferred methods (indicator-level):")
print(f"  nearly_symmetric           | method = zscore | indicators = {nearly_sym}")
print(f"  moderately_skewed          | method = iqr    | indicators = {moderate}")
print(f"  highly+very_highly_skewed  | method = mad    | indicators = {high_plus_very}")

max_count = max(nearly_sym, moderate, high_plus_very)
if max_count == nearly_sym:
    indicator_level_method = "zscore"
elif max_count == moderate:
    indicator_level_method = "iqr"
else:
    indicator_level_method = "mad"

print(f"  → Indicator-level preferred method (by counts): {indicator_level_method}")
output_file = os.path.join(output_dir_path, "3CAA_indicator_skewness_with_methods.csv")

(
    stats.coalesce(1)
         .write.mode("overwrite")
         .option("header", True)
         .csv(output_file)
)

spark.stop()
