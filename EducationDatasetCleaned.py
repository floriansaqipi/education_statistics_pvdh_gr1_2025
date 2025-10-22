from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
import os
os.environ["HADOOP_HOME"] = r"C:\hadoop\hadoop-3.3.6"
os.environ["hadoop.home.dir"] = r"C:\hadoop\hadoop-3.3.6"
os.environ["PATH"] += r";C:\hadoop\hadoop-3.3.6\bin"


spark = (
    SparkSession.builder
    .appName("EducationStats-Cleaning-and-Unpivot")
    .master("local[*]")
    .getOrCreate()
)

INPUT_PATH = "data/Gr1_Education_Statistics_Preview.csv"

# Lexim më robust për CSV me tekste të gjata/quote
df = (
    spark.read.format("csv")
    .option("header", True)
    .option("multiLine", True)   # rëndësishme
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")
    .load(INPUT_PATH)
)

year_cols = [c for c in df.columns if c.startswith("YR")]
id_cols   = [c for c in df.columns if not c.startswith("YR")]

cleaned = df
for c in year_cols:
    # 1) "NA" ose bosh -> null
    col_clean = F.when((F.col(c) == "NA") | (F.trim(F.col(c)) == ""), None).otherwise(F.col(c))
    # 2) Lejo vetëm numra (me ose pa presje dhjetore). Tjerat -> null (KËTU s’ka cast akoma)
    col_numeric_or_null = F.when(F.col(c).rlike(r'^\s*-?\d+(\.\d+)?\s*$'), col_clean).otherwise(None)
    # 3) Tani cast është i sigurt
    cleaned = cleaned.withColumn(c, col_numeric_or_null.cast(DoubleType()))

# Hiq kolonat e viteve 100% bosh (opsionale)
non_empty_years = []
for c in year_cols:
    if cleaned.select(c).na.drop().limit(1).count() > 0:
        non_empty_years.append(c)
year_cols = non_empty_years or year_cols  # nëse s’gjen asnjë, lë listën origjinale

# Unpivot (wide -> long)
stack_expr = "stack({n}, {pairs}) as (Year, Value)".format(
    n=len(year_cols),
    pairs=", ".join([f"'{c}', `{c}`" for c in year_cols])
)

long_df = (
    cleaned
    .select(*id_cols, F.expr(stack_expr))
    .withColumn("Year", F.regexp_replace("Year", "^YR", "").cast(IntegerType()))
    .filter(F.col("Value").isNotNull())
)

# Ruajtje
(long_df.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv("data/Gr1_Education_Statistics_Cleaned_LONG.csv"))

# (opsionale) Non-NA count nga versioni i pastruar
(non_na := (long_df.groupBy("Year").agg(F.count(F.lit(1)).alias("NonNA_Count")).orderBy("Year")))
(non_na.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv("data/Gr1_Education_Statistics_NonNA_By_Year_fromClean.csv"))

print(f"ID cols: {len(id_cols)}, Year cols used: {len(year_cols)}")
long_df.show(10, truncate=False)

spark.stop()
