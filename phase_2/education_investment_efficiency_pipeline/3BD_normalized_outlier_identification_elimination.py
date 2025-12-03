import os
from logging import Filter

from pyspark.sql import SparkSession
from pyspark.sql import Window as W
from pyspark.sql import functions as F

from utils.paths import phase2_path
from utils.schema import long_outlier_schema

input_file_path_finance = phase2_path("3BC_dimension_reduction", "3BCA_dimension_reduction_filtered_finance.csv")
input_file_path_learning = phase2_path("3BC_dimension_reduction", "3BCB_dimension_reduction_filtered_learning.csv")

output_dir_path = phase2_path("3BD_transformation_normalization")

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

finance = spark.read.option(
    "header", True).schema(long_outlier_schema).csv(input_file_path_finance)

learning = spark.read.option(
    "header", True).schema(long_outlier_schema).csv(input_file_path_learning)

wI = W.partitionBy("INDICATOR")


def standardize(df_in):
    df_z = (
        df_in
        .withColumn(
            "val_std",
            F.when(F.col("UNIT_TYPE") == "NUMBER", F.log1p(F.col("Value")))
             .when(
                 (F.col("UNIT_TYPE") == "SHARE") & (F.col("Value") > 1),
                 F.col("Value") / 100.0
             )
             .otherwise(F.col("Value"))
        )
        .withColumn("mu", F.avg("val_std").over(wI))
        .withColumn("sd", F.stddev("val_std").over(wI))
        .withColumn(
            "sd_safe",
            F.when((F.col("sd").isNull()) | (F.col("sd") == 0), F.lit(1.0)).otherwise(F.col("sd"))
        )
        .withColumn("z", (F.col("val_std") - F.col("mu")) / F.col("sd_safe"))
        .withColumn("skewness", F.skewness("val_std").over(wI))
        .drop("sd_safe")
    )

    df_z = df_z.withColumn(
        "is_z_outlier",
        (F.abs(F.col("z")) > 3).cast("boolean")
    )

    iqr_stats = (
        df_z
        .groupBy("INDICATOR")
        .agg(
            F.expr("percentile_approx(val_std, 0.25)").alias("q1"),
            F.expr("percentile_approx(val_std, 0.75)").alias("q3"),
        )
        .withColumn("iqr", F.col("q3") - F.col("q1"))
        .withColumn("lower_fence", F.col("q1") - F.lit(1.5) * F.col("iqr"))
        .withColumn("upper_fence", F.col("q3") + F.lit(1.5) * F.col("iqr"))
    )

    df_z = (
        df_z
        .join(
            iqr_stats.select("INDICATOR", "lower_fence", "upper_fence"),
            on="INDICATOR",
            how="left"
        )
        .withColumn(
            "is_iqr_outlier",
            (
                (F.col("val_std") < F.col("lower_fence")) |
                (F.col("val_std") > F.col("upper_fence"))
            ).cast("boolean")
        )
        .drop("lower_fence", "upper_fence")
    )

    return df_z


fin_z = standardize(finance)
out_z = standardize(learning)

fin_z = fin_z.filter(
    ~(F.col("is_z_outlier") | F.col("is_iqr_outlier"))
)

out_z = out_z.filter(
    ~(F.col("is_z_outlier") | F.col("is_iqr_outlier"))
)

fin_z = fin_z.drop("is_z_outlier", "is_iqr_outlier")
out_z = out_z.drop("is_z_outlier", "is_iqr_outlier")

finance_output_file = os.path.join(output_dir_path, "3BDA_transformation_normalized_finance.csv")
learning_output_file = os.path.join(output_dir_path, "3BDB_transformation_normalized_learning.csv")

(fin_z.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv(finance_output_file))


(out_z.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv(learning_output_file))