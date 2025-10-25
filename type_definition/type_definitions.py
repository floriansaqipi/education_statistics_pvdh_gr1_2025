import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


input_file_path = os.path.join("..", "data", "Edstats_Updated.csv")


spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()


non_year_fields = [
    "INDICATOR","name","SEX","URBANIZATION","AGE","COMP_BREAKDOWN_1",
    "INDICATOR_ROOT","UNIT_MEASURE","UNIT_TYPE","INDICATOR_ROOT_NAME",
    "economy","Country name","Indicator name"
]

schema = StructType(
    [StructField(c, StringType(), True) for c in non_year_fields] +
    [StructField(f"YR{year}", DoubleType(), True) for year in range(1960, 2030)]
)

df = spark.read.csv(
    input_file_path,
    schema=schema,
    header=True,
    nullValue="NA"
)
