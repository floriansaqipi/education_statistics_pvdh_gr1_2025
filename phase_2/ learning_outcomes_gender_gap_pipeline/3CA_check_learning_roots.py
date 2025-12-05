import os
from functools import reduce
import operator

from pyspark.sql import SparkSession, functions as F
from utils.paths import phase2_path
from utils.schema import integrated_outlier_schema


input_file_path = phase2_path("2A_data_enrichment_with_class", "2A_enriched_with_class.csv")
output_dir_path = phase2_path("3CA_check_learning_roots")

spark = (
    SparkSession.builder
    .appName("CSV to Dataset")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

WL = [
    r"^SE\.CLO\.15Y\.",
    r"^SE\.CLO\.(3|4|5|6|8|9)\.",
    r"LLC|LLECE|\.SAC",
    r"\.PSC|PASEC|\.SPM|SEA\-PLM",
    r"^SE\.LPV\.",
    r"^HD\.HCI\.HLOS$",
    r"^HD\.HCI\.LAYS$",
]

BL = [
    r"^UIS\.YADULT\.PROFI",
    r"^UIS\.ICTSKILL",
    r"^SE\.PRM\.LERN\.",
    r"^SE\.PRM\.CONT$",
    r"^UIS\.(GER|GAR|XSPENDP|PTRHC|OFST|O[AMR]|QUTP|NART|NERT|ROFST|SLE|SAP)\b",
]

df = (
    spark.read
    .option("header", True)
    .schema(integrated_outlier_schema)
    .csv(input_file_path)
)

if "INDICATOR_ROOT" not in df.columns and "INDICATOR" in df.columns:
    df = df.withColumn("INDICATOR_ROOT", F.col("INDICATOR"))

df = df.filter(F.col("INDICATOR_ROOT").isNotNull())


def any_rlike(col, patterns):
    if not patterns:
        return F.lit(False)
    exprs = [F.col(col).rlike(p) for p in patterns]
    return reduce(operator.or_, exprs)


keep_expr = any_rlike("INDICATOR_ROOT", WL) & (~any_rlike("INDICATOR_ROOT", BL))
learn_df = df.filter(keep_expr)

report = (
    learn_df
    .groupBy("INDICATOR_ROOT")
    .agg(F.countDistinct("INDICATOR_ROOT_NAME").alias("n_indicator_names"))
    .orderBy(F.col("n_indicator_names").desc())
)

learning_out_path = os.path.join(output_dir_path, "learning_indicators_only")
(
    learn_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(learning_out_path)
)

spark.stop()
