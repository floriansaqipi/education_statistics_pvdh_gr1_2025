import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.schema import schema

ROOT = Path(__file__).resolve().parents[1]

input_file_path = ROOT / "data" / "output" / "1C_data_quality_cleaning" / "1C_quality_cleaned.csv"
output_dir_path = ROOT / "data" / "output" / "2A_data_missing_values_handling"

spark = SparkSession.builder.appName("Data Missing Values Handling").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.option("header", True).schema(schema).csv(input_file_path.as_posix())

if "is_valid" in df.columns:
    df = df.withColumn("is_valid", F.col("is_valid").cast("boolean"))
else:
    df = df.withColumn("is_valid", F.lit(True))

df = df.replace("NA", None)

all_year_cols = [f"YR{y}" for y in range(1960, 2030)]
year_cols = [c for c in all_year_cols if c in df.columns]

if year_cols:
    counts_row = df.select([F.count(F.col(c)).alias(c) for c in year_cols]).collect()[0].asDict()
    non_empty_year_cols = [c for c, cnt in counts_row.items() if cnt > 0]
    keep_cols = [c for c in df.columns if (c not in year_cols) or (c in non_empty_year_cols)]
    df = df.select(*keep_cols)
else:
    non_empty_year_cols = []

if non_empty_year_cols:
    any_year_not_null = reduce(lambda a, b: a | b, [F.col(c).isNotNull() for c in non_empty_year_cols])
    df = df.filter(any_year_not_null)


year_cols2 = [c for c in df.columns if c.startswith("YR")]
if year_cols2:
    cnt2 = df.select([F.count(F.col(c)).alias(c) for c in year_cols2]).collect()[0].asDict()
    keep_year2 = [c for c, v in cnt2.items() if v > 0]
    keep_all = [c for c in df.columns if not c.startswith("YR")] + keep_year2
    df = df.select(*keep_all)

cols = df.columns
year_cols = [c for c in cols if c.startswith("YR")]
non_year = [c for c in cols if c not in year_cols and c != "is_valid"]
ordered = non_year + year_cols + (["is_valid"] if "is_valid" in cols else [])
df = df.select(*ordered)

output_path = os.path.join(output_dir_path, "2A_missing_values_cleaned.csv")
(df.coalesce(1)
   .write.option("header", True)
   .mode("overwrite")
   .csv(output_path))

df.show(20, truncate=False)
spark.stop()
