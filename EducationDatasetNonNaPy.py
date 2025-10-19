from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = SparkSession.builder.appName("EdStats Non-NA Count").master("local").getOrCreate()

df = spark.read.format("csv").option("header", True).load("data/Edstats_Updated.csv")

results = []
schema = StructType([
    StructField("Year", StringType(), True),
    StructField("NonNA_Count", LongType(), True)
])

for year in range(1960, 2026):
    col_name = f"YR{year}"
    if col_name in df.columns:
        count = df.filter(df[col_name] != "NA").count()
        results.append(Row(Year=col_name, NonNA_Count=count))

counts_df = spark.createDataFrame(results, schema)
counts_df.show()

counts_df.coalesce(1).write.option("header", True).mode("overwrite").csv("data/Yearly_NonNA_Counts.csv")

spark.stop()
