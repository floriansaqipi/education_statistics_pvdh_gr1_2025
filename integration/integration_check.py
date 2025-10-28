from pyspark.sql import SparkSession, functions as F
from pathlib import Path

spark = SparkSession.builder.appName("Integration_Check").getOrCreate()

project_root = Path(__file__).resolve().parents[1]
EDU_PATH  = project_root / "data" / "integration_output" / "1A_attributes_reorder" / "education_reordered.csv"
ECON_PATH = project_root / "data" / "CLASS_2025_10_07.csv"
OUT_PATH  = project_root / "data" / "integration_output" / "education_integrated.csv"  # <<< KETU

read_csv = lambda p: (spark.read.option("header", True).option("inferSchema", True).csv(str(p)))

edu  = read_csv(EDU_PATH)
econ = read_csv(ECON_PATH)
out  = read_csv(OUT_PATH)



print("Kolonat EDU:", len(edu.columns))
print("Kolonat ECON:", len(econ.columns))
print("Kolonat OUT:", len(out.columns))

print("\n=== 2) Numrat e rreshtave ===")
n_edu  = edu.count()
n_out  = out.count()
print(f"Rreshta EDU: {n_edu}")
print(f"Rreshta OUT: {n_out}")
print("Kontroll ruajtje rreshtash:", "OK" if n_edu == n_out else "Mospërputhje")

print("\n=== 3) Kolonat pas integrimit ===")
expected_cols = {"Region","Income_group","Lending_category"}
has_cols = expected_cols.issubset(set(out.columns))
missing = expected_cols - set(out.columns)
print("Prezencë:", "OK" if has_cols else f"Jo mungojnë: {sorted(missing)}")

print("\n=== 4) Normalizim kyçesh ===")
edu_key  = edu.withColumn("economy_norm", F.upper(F.trim(F.col("economy"))))
econ_key = (econ
            .withColumn("Code_norm", F.upper(F.trim(F.col("Code"))))
            .select("Code_norm").dropna().dropDuplicates())
matchable = (edu_key.join(econ_key, edu_key["economy_norm"] == econ_key["Code_norm"], "left_semi")).count()
print(f"Matchable në bazë kyçi: {matchable} ({matchable/max(n_edu,1)*100:.2f}%)")

print("\n=== 5) Match real në OUT ===")
matched = out.filter(F.col("Region").isNotNull()).count()
print(f"OUT me Region != NULL: {matched} ({matched/max(n_out,1)*100:.2f}%)")
gap = matchable - matched
print("Krahasim matchable vs matched:", "OK" if abs(gap) <= max(1, int(0.01*matchable)) else f"⚠️ Diferencë {gap}")

print("\n=== 6) Mungesat te kolonat e reja ===")
for c in ["Region","Income_group","Lending_category"]:
    missing_cnt = out.filter(F.col(c).isNull()).count()
    print(f"{c}: {missing_cnt} ({missing_cnt/max(n_out,1)*100:.2f}%)")

print("\n=== 7) Duplikata në ECON.Code ===")
dup = (econ
       .withColumn("Code_norm", F.upper(F.trim(F.col("Code"))))
       .groupBy("Code_norm").count().filter("count > 1"))
dup_cnt = dup.count()
print("Duplikata:", "Po" if dup_cnt>0 else "Jo")
if dup_cnt>0:
    dup.orderBy(F.desc("count")).show(20, truncate=False)

print("\n=== 8) Ekonomitë pa match (OUT.Region IS NULL) ===")
(out.filter(F.col("Region").isNull())
    .select("economy").dropna().dropDuplicates()
    .orderBy("economy").show(20, truncate=False))

print("\n=== 9) Shembuj ===")
out.select("economy","Country name","Region","Income_group","Lending_Category").show(5, truncate=False)
out.filter(F.col("economy")=="XKX") \
   .select("economy","Country name","Region","Income_group","Lending_Category") \
   .dropDuplicates().show(truncate=False)

print("\n=== 10) Vlerësim final ===")
all_good = (n_edu == n_out) and has_cols and (matched > 0)
print("Integrimi:", "OK" if all_good else "Ka çështje")
