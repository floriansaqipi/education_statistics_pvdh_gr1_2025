import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils.schema import schema

PY = sys.executable
os.environ["PYSPARK_PYTHON"] = PY
os.environ["PYSPARK_DRIVER_PYTHON"] = PY

ROOT = Path(__file__).resolve().parents[1]
before_dir = ROOT / "data" / "output" / "1C_data_quality_cleaning" / "1C_quality_cleaned.csv"
after_dir  = ROOT / "data" / "output" / "2A_data_missing_values_handling" / "2A_missing_values_cleaned.csv"

spark = (
    SparkSession.builder
    .appName("2A Missing Values Check")
    .master("local[1]")
    .config("spark.pyspark.python", PY)
    .config("spark.pyspark.driver.python", PY)
    .config("spark.python.worker.reuse", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

def year_columns(df):
    all_years = [f"YR{y}" for y in range(1960, 2030)]
    return [c for c in all_years if c in df.columns]

def metrics(df):
    yrs = year_columns(df)
    rows = df.count()
    cols = len(df.columns)
    na_cells = 0
    if df.columns:
        sums = df.select([F.sum(F.when(F.col(c).cast("string") == "NA", 1).otherwise(0)).alias(c)
                          for c in df.columns]).collect()[0].asDict()
        na_cells = int(sum(sums.values()))
    empty_year_cols = []
    if yrs:
        cnts = df.select([F.count(F.col(c)).alias(c) for c in yrs]).collect()[0].asDict()
        empty_year_cols = [c for c, v in cnts.items() if v == 0]
    rows_all_years_null = 0
    if yrs:
        any_not_null = reduce(lambda a, b: a | b, [F.col(c).isNotNull() for c in yrs])
        rows_all_years_null = df.filter(~any_not_null).count()
    return {
        "rows": rows,
        "cols": cols,
        "year_cols": len(yrs),
        "na_cells": na_cells,
        "empty_year_cols_count": len(empty_year_cols),
        "empty_year_cols_sample": ",".join(empty_year_cols[:10]),
        "rows_all_years_null": rows_all_years_null,
    }

df_before = spark.read.option("header", True).schema(schema).csv(before_dir.as_posix())
df_after  = spark.read.option("header", True).csv(after_dir.as_posix())


m_before = metrics(df_before)
m_after  = metrics(df_after)

delta = {
    "rows_delta": m_after["rows"] - m_before["rows"],
    "cols_delta": m_after["cols"] - m_before["cols"],
    "year_cols_delta": m_after["year_cols"] - m_before["year_cols"],
    "na_cells_delta": m_after["na_cells"] - m_before["na_cells"],
    "empty_year_cols_delta": m_after["empty_year_cols_count"] - m_before["empty_year_cols_count"],
    "rows_all_years_null_delta": m_after["rows_all_years_null"] - m_before["rows_all_years_null"],
}

print("\n=== 2A Missing Values Check (dry) ===")
print("Before  (1C):", m_before)
print("After   (2A):", m_after)
print("Δ (after - before):", delta)

spark.stop()
