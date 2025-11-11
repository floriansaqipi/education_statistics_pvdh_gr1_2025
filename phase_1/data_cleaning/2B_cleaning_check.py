import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
in_2A = ROOT / "data" / "output" / "2A_data_missing_values_handling" / "2A_missing_values_cleaned.csv"
in_2B = ROOT / "data" / "output" / "2B_data_cleaning" / "2B_cleaned.csv"

spark = (SparkSession.builder.appName("2B Cleaning Check").master("local[*]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

a = spark.read.option("header", True).csv(in_2A.as_posix())
b = spark.read.option("header", True).csv(in_2B.as_posix())

def stats(df):
    year_cols = [c for c in df.columns if c.startswith("YR")]
    invalid = df.filter(F.coalesce(F.col("is_valid").cast("boolean"), F.lit(True)) == False).count() if "is_valid" in df.columns else 0
    dup_exact = df.count() - df.dropDuplicates().count()
    key_cols = [c for c in df.columns if c not in year_cols]
    dup_by_key = 0
    if key_cols:
        dup_by_key = df.groupBy(key_cols).count().filter(F.col("count") > 1).count()
    return {
        "rows": df.count(),
        "cols": len(df.columns),
        "dup_exact": dup_exact,
        "dup_by_key": dup_by_key,
        "invalid": invalid
    }

before_stats = stats(a)
after_stats = stats(b)

print("=== 2B Data Cleaning Check ===")
print("Before (2A):", before_stats)
print("After  (2B):", after_stats)
print("Δ (after - before):", {
    "rows_delta": after_stats["rows"] - before_stats["rows"],
    "cols_delta": after_stats["cols"] - before_stats["cols"],
    "dup_exact_delta": after_stats["dup_exact"] - before_stats["dup_exact"],
    "dup_by_key_delta": after_stats["dup_by_key"] - before_stats["dup_by_key"],
    "invalid_delta": after_stats["invalid"] - before_stats["invalid"]
})

spark.stop()
