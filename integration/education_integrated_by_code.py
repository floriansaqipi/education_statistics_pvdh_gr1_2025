from pyspark.sql import SparkSession, functions as F
from pathlib import Path
import shutil, glob

spark = SparkSession.builder.appName("Education_Integration").getOrCreate()

edu_path = "data/integration_output/1A_attributes_reorder/education_reordered.csv"
edu = (spark.read.option("header", True).option("inferSchema", True).csv(edu_path))
edu = edu.withColumn("economy", F.upper(F.trim(F.col("economy"))))

econ_path = "data/CLASS_2025_10_07.csv"
econ = (spark.read.option("header", True).option("inferSchema", True).csv(econ_path))
econ = (econ.withColumn("Code", F.upper(F.trim(F.col("Code"))))
            .withColumnRenamed("Income group", "Income_group")
            .withColumnRenamed("Lending category", "Lending_category"))

joined = (edu.join(
            econ.select("Code","Region","Income_group","Lending_category"),
            edu["economy"] == econ["Code"], "left")
          .drop("Code"))

tmp_dir = "data/integration/_tmp_education_integrated"
out_file = Path("data/integration_output/education_integrated.csv")

(joined.coalesce(1)
       .write.mode("overwrite")
       .option("header", True)
       .csv(tmp_dir))

part = glob.glob(f"{tmp_dir}/part-*.csv")[0]
if out_file.exists():
    out_file.unlink()
shutil.move(part, out_file)
shutil.rmtree(tmp_dir, ignore_errors=True)

print("Integrimi u kry me sukses! U ruajt:", out_file)
