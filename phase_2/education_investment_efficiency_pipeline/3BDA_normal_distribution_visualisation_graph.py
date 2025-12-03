import os
import sys

from utils.paths import phase2_path
from utils.schema import long_schema, long_outlier_schema

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql import functions as F
from pyspark.sql import Window as W


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
    return (df_in
            .withColumn("val_std",
                        F.when(F.col("UNIT_TYPE") == "NUMBER", F.col("Value"))
                        .when((F.col("UNIT_TYPE") == "SHARE") & (F.col("Value") > 1), F.col("Value") / 100.0)
                        .otherwise(F.col("Value"))
                        )
            .withColumn("mu", F.avg("val_std").over(wI))
            .withColumn("sd", F.stddev("val_std").over(wI))
            .withColumn("sd_safe",
                        F.when((F.col("sd").isNull()) | (F.col("sd") == 0), F.lit(1.0)).otherwise(F.col("sd")))
            .withColumn("z", (F.col("val_std") - F.col("mu")) / F.col("sd_safe"))
            .drop("sd_safe")
            )


fin_z = standardize(finance)
out_z = standardize(learning)



print(fin_z.count())
print(out_z.count())

finance_output_file = os.path.join(output_dir_path, "3BDA_transformation_normalized_finance.csv")
learning_output_file = os.path.join(output_dir_path, "3BDB_transformation_normalized_learning.csv")

import matplotlib.pyplot as plt


fin_pd = fin_z.toPandas()
out_pd = out_z.toPandas()

def plot_z_hist(df, title):
    # take the z column, drop missing
    z = df["z"].dropna()

    plt.figure()
    # more bins => more bars => smoother look
    plt.hist(z, bins=250, density=True)  # density=True makes it a bit more "bell-shaped"

    # add vertical lines at -3, -2, -1, 0, 1, 2, 3 standard deviations
    for k in range(-10, 10):
        plt.axvline(k, linestyle="--")  # dashed vertical line at each k

    plt.title(title)
    plt.xlabel("z-score")
    plt.ylabel("Density")
    plt.show()


# Finance z distribution
plot_z_hist(fin_pd, "Finance indicators – z distribution")

# Learning z distribution
plot_z_hist(out_pd, "Learning indicators – z distribution")



