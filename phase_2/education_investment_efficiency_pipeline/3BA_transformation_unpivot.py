import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import expr

from utils.paths import phase2_path
from utils.schema import integrated_outlier_schema

input_file_path = phase2_path("2A_data_enrichment_with_class", "2A_enriched_with_class.csv")
output_dir_path = phase2_path("3BA_transformation")

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.option(
    "header", True).schema(integrated_outlier_schema).csv(input_file_path)

year_columns = [c for c in df.columns if c.startswith('YR')]

stack_args = []
for year_col in year_columns:
    year_val = year_col.replace('YR', '')
    stack_args.append(f"'{year_val}', `{year_col}`")

stack_expression = f"stack({len(year_columns)}, {', '.join(stack_args)}) as (Year, Value)"

non_year_columns = [c for c in df.columns if not c.startswith('YR')]
unpivot_df = df.select(*non_year_columns, expr(stack_expression))
unpivot_df = unpivot_df.filter(F.col("Value").isNotNull())

print(unpivot_df.count())

output_file = os.path.join(output_dir_path, "3BA_transformation_unpivot.csv")

(unpivot_df.coalesce(1)
 .write.mode("overwrite")
 .option("header", True)
 .csv(output_file))
