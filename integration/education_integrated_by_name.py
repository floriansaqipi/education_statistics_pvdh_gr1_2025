from pyspark.sql import SparkSession, functions as F
from pathlib import Path
import glob, shutil, os

spark = SparkSession.builder.appName("Join_By_CountryName_to_Economy").getOrCreate()

project_root = Path(__file__).resolve().parents[1]
BIG_PATH   = project_root / "data" / "integration_output" / "1A_attributes_reorder" / "education_reordered.csv"
SMALL_PATH = project_root / "data" / "CLASS_2025_10_07.csv"
OUT_SINGLE = project_root / "data" / "integration" / "education_integrated_by_name.csv"
TMP_DIR    = project_root / "data" / "integration" / "_tmp_by_name"

big = (spark.read.option("header", True).option("inferSchema", True).csv(str(BIG_PATH)))
small = (spark.read.option("header", True).option("inferSchema", True).csv(str(SMALL_PATH))
           .withColumnRenamed("Income group","Income_group")
           .withColumnRenamed("Lending category","Lending_Category"))

def norm(col):
    c = F.upper(F.trim(col))
    c = F.regexp_replace(c, r"[.,'’()\-]", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.regexp_replace(c, r"\bTHE\b", "")
    c = F.regexp_replace(c, r"\bGAMBIA,? THE\b", "GAMBIA")
    c = F.regexp_replace(c, r"\bBAHAMAS,? THE\b", "BAHAMAS")
    c = F.regexp_replace(c, r"\bCÔTE D[ ’]?IVOIRE\b", "COTE D IVOIRE")
    c = F.regexp_replace(c, r"\bCOTE D[ ]?IVOIRE\b", "COTE D IVOIRE")
    c = F.regexp_replace(c, r"\bKOSOVA\b", "KOSOVO")
    return F.regexp_replace(c, r"\s+", " ")

big_n   = big.withColumn("name_key",  norm(F.col("Country name")))
small_n = small.withColumn("name_key", norm(F.col("Economy")))

joined = (big_n
          .join(small_n.select("name_key","Region","Income_group","Lending_Category"),
                on="name_key", how="left")
          .drop("name_key"))

total   = big.count()
matched = joined.filter(F.col("Region").isNotNull()).count()
print(f"Matched by NAME: {matched}/{total} ({matched/max(total,1)*100:.2f}%)")

(joined.coalesce(1)
       .write.mode("overwrite")
       .option("header", True)
       .csv(str(TMP_DIR)))

part = glob.glob(str(TMP_DIR / "part-*.csv"))[0]
if OUT_SINGLE.exists(): os.remove(OUT_SINGLE)
shutil.move(part, OUT_SINGLE)
shutil.rmtree(TMP_DIR, ignore_errors=True)

print("U ruajt:", OUT_SINGLE)
