import os
import sys

from utils.schema import long_schema

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql import functions as F
from pyspark.sql import Window as W

ROOT = Path(__file__).resolve().parents[1]

input_file_path_finance = ROOT / "data" / "output" / "4BC_dimension_reduction" / "4BCA_dimension_reduction_filtered_finance.csv"
input_file_path_learning = ROOT / "data" / "output" / "4BC_dimension_reduction" / "4BCB_dimension_reduction_filtered_learning.csv"

output_dir_path = ROOT / "data" / "output" / "4BD_transformation_normalization"

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

finance = spark.read.option(
    "header", True).schema(long_schema).csv(input_file_path_finance.as_posix())

learning = spark.read.option(
    "header", True).schema(long_schema).csv(input_file_path_learning.as_posix())

wI = W.partitionBy("INDICATOR")


def standardize(df_in):
    return (df_in
            .withColumn("val_std",
                        F.when(F.col("UNIT_TYPE") == "NUMBER", F.log1p(F.col("Value")))
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

(fin_z.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv((output_dir_path / "4BDA_transformation_normalized_finance.csv").as_posix()))


(out_z.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv((output_dir_path / "4BDB_transformation_normalized_learning.csv").as_posix()))

