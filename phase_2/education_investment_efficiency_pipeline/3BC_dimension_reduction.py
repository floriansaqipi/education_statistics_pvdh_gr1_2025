import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from utils.paths import phase2_path
from utils.schema import long_outlier_schema

input_file_path = phase2_path("3BB_aggregation", "3BB_aggregation_latest.csv")
output_dir_path = phase2_path("3BC_dimension_reduction")

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.option(
    "header", True).schema(long_outlier_schema).csv(input_file_path)

finance_pattern = r"expenditure|funding|gdp|ppp\$|us\$|compensation"

learning_pattern = (
    r"learning deprivation|harmonized test|learning-adjusted|lays|"
    r"pisa|timss|pirls|sacmeq|pasec|sea-plm|"
    r"proficiency|test score|assessment|literacy|numeracy|"
    r"completion rate|school life expectancy|"
    r"gross enrolment|net enrolment|enrolment|"
    r"attendance rate|survival rate|repetition rate|out-of-school|"
    r"ld =|lp =|lpsev =|ldgap|ldsev|lpgap"
)

finance = df.filter(F.lower(F.col("Indicator name")).rlike(finance_pattern))
learning = df.filter(F.lower(F.col("Indicator name")).rlike(learning_pattern))

print(finance.count())
print(learning.count())

finance_output_file = os.path.join(output_dir_path, "3BCA_dimension_reduction_filtered_finance.csv")
learning_output_file = os.path.join(output_dir_path, "3BCB_dimension_reduction_filtered_learning.csv")

(finance.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(finance_output_file))

(learning.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(learning_output_file))
