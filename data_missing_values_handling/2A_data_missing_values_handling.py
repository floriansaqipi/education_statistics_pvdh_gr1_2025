import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
main_file = ROOT / "data" / "output" / "2B_data_cleaning" / "2B_cleaned.csv"
class_file = ROOT / "data" / "CLASS_2025_10_07.csv"
out_dir = ROOT / "data" / "output" / "3A_integration"

spark = SparkSession.builder.appName("3A integrate with CLASS").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

m = spark.read.option("header", True).csv(main_file.as_posix())
c = spark.read.option("header", True).csv(class_file.as_posix())

c = (
    c.select("Code", "Region", "Income group", "Lending category")
    .withColumn("code", F.upper(F.trim(F.col("Code"))))
    .dropDuplicates(["code"])
)

m2 = m.withColumn("economy_norm", F.upper(F.trim(F.col("economy"))))
joined = m2.join(c, F.col("economy_norm") == F.col("code"), "left")

keep_main = [c for c in m.columns]
keep_class = ["code", "Region", "Income group", "Lending category"]

out = joined.select(*[F.col(f"`{c}`") for c in keep_main], *[F.col(c) for c in keep_class])
if "Code" in out.columns:
    out = out.drop("Code")

cols = out.columns
if "economy" in cols and "code" in cols:
    cols_ordered = []
    for c in cols:
        cols_ordered.append(c)
        if c == "economy":
            cols_ordered.append("code")
    seen = set()
    cols_ordered = [x for x in cols_ordered if not (x in seen or seen.add(x))]
    missing = [c for c in cols if c not in cols_ordered]
    cols_ordered += missing
    out = out.select(*[F.col(f"`{c}`") for c in cols_ordered])

(out.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv((out_dir / "3A_integrated_with_class.csv").as_posix()))

spark.stop()
