from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("Compare_Integration_Matches").getOrCreate()

path_code = "data/integration_output/education_integrated.csv"
path_name = "data/integration/education_integrated_by_name.csv"

print("\n=== 1) Leximi i të dhënave ===")
df_code = spark.read.option("header", True).csv(path_code)
df_name = spark.read.option("header", True).csv(path_name)

total_code = df_code.count()
total_name = df_name.count()

match_code = df_code.filter(F.col("Region").isNotNull()).count()
match_name = df_name.filter(F.col("Region").isNotNull()).count()

match_economies_code = df_code.filter(F.col("Region").isNotNull()).select("economy").distinct().count()
match_economies_name = df_name.filter(F.col("Region").isNotNull()).select("economy").distinct().count()

print("\n=== 2) Rezultatet kryesore ===")
print(f"Rreshta gjithsej (Code): {total_code}")
print(f"Rreshta gjithsej (Name): {total_name}")
print()
print(f"Rreshta të përputhur (Region ≠ NULL) me CODE: {match_code} ({match_code/total_code*100:.2f}%)")
print(f"Rreshta të përputhur (Region ≠ NULL) me NAME: {match_name} ({match_name/total_name*100:.2f}%)")
print()
print(f"Vende të përputhura unike me CODE: {match_economies_code}")
print(f"Vende të përputhura unike me NAME: {match_economies_name}")

no_match_code = df_code.filter(F.col("Region").isNull()).select("economy").distinct()
no_match_name = df_name.filter(F.col("Region").isNull()).select("economy").distinct()

diff_name_better = no_match_code.subtract(no_match_name)
diff_code_better = no_match_name.subtract(no_match_code)

print("\n=== 3) Krahasimi i vendeve që ndryshojnë ===")
print("Vende që janë match vetëm në versionin me NAME:")
diff_name_better.show(20, truncate=False)
print("Vende që janë match vetëm në versionin me CODE:")
diff_code_better.show(20, truncate=False)

diff_r = match_name - match_code
diff_c = match_economies_name - match_economies_code
print("\n=== 4) Përfundim ===")
if diff_r > 0:
    print(f"Versioni me NAME ka {diff_r} rreshta më shumë të përputhur dhe {diff_c} ekonomi shtesë.")
elif diff_r < 0:
    print(f"Versioni me CODE ka {-diff_r} rreshta më shumë të përputhur dhe {-diff_c} ekonomi shtesë.")
else:
    print("Të dy versionet kanë të njëjtin numër përputhjesh.")
