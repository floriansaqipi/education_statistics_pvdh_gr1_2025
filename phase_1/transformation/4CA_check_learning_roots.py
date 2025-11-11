import os, sys
from pathlib import Path
from functools import reduce
import operator

from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, ROOT.as_posix())

in_file  = ROOT / "data" / "output" / "3A_integration" / "3A_integrated_with_class.csv"
out_root = ROOT / "data" / "output" / "4CA_check_learning_roots"
tmp_dir  = ROOT / "tmp_spark"
out_root.mkdir(parents=True, exist_ok=True)
tmp_dir.mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("4CA check learning roots")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.local.dir", tmp_dir.as_posix())
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

df = spark.read.option("header", True).csv(in_file.as_posix())

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

report = (learn_df
          .groupBy("INDICATOR_ROOT")
          .agg(F.countDistinct("INDICATOR_ROOT_NAME").alias("n_indicator_names"))
          .orderBy(F.col("n_indicator_names").desc()))

(learn_df.coalesce(1)
    .write.mode("overwrite").option("header", True)
    .csv((out_root / "learning_indicators_only.csv").as_posix()))

(report.coalesce(1)
    .write.mode("overwrite").option("header", True)
    .csv((out_root / "learning_roots_report.csv").as_posix()))


spark.stop()
