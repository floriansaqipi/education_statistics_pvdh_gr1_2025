import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
main_file = ROOT / "data" / "output" / "2B_data_cleaning" / "2B_cleaned.csv"
joined_dir = ROOT / "data" / "output" / "3A_integration" / "3A_integrated_with_class"

spark = SparkSession.builder.appName("3A Integration Check").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

m = spark.read.option("header", True).csv(main_file.as_posix())
j = spark.read.option("header", True).csv(joined_dir.as_posix())

year_m = [c for c in m.columns if c.startswith("YR")]
year_j = [c for c in j.columns if c.startswith("YR")]
reintroduced = sorted(list(set(year_j) - set(year_m)))
missing_years = sorted(list(set(year_m) - set(year_j)))

missing_match = j.filter(F.col("economy").isNotNull() & (F.col("code").isNull())).count() if "code" in j.columns else j.count()
match_rate = 0.0 if j.count() == 0 else round(100.0 * (j.count() - missing_match) / j.count(), 2)

dupes_before = m.groupBy(*[c for c in m.columns if not c.startswith("YR")]).count().filter("count > 1").count()
dupes_after = j.groupBy(*[c for c in m.columns if not c.startswith("YR") if c in j.columns]).count().filter("count > 1").count()

print("=== 3A Integration Check ===")
print("Main rows/cols:", m.count(), len(m.columns))
print("Joined rows/cols:", j.count(), len(j.columns))
print("Reintroduced year columns:", reintroduced)
print("Missing year columns:", missing_years)
print("Has YR1960 in joined:", "YR1960" in year_j)
print("Rows without CLASS match:", missing_match)
print("Match rate %:", match_rate)
print("Dupes before:", dupes_before, "Dupes after:", dupes_after)
print("New CLASS columns:", [c for c in j.columns if c not in m.columns])

ok = (
    len(reintroduced) == 0 and
    len(missing_years) == 0 and
    ("code" in j.columns) and
    match_rate >= 90.0 and
    dupes_after <= dupes_before
)
print("Final status:", "OK" if ok else "CHECK")

spark.stop()
