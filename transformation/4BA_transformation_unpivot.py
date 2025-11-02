import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, ROOT)

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql import functions as F
from utils.schema import schema, integrated_schema


ROOT = Path(__file__).resolve().parents[1]

input_file_path = ROOT / "data" / "output" / "3A_integration" / "3A_integrated_with_class.csv"
output_dir_path = ROOT / "data" / "output" / "4BA_transformation"

spark = SparkSession.builder \
    .appName("CSV to Dataset") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.option(
    "header", True).schema(integrated_schema).csv(input_file_path.as_posix())

year_columns = [c for c in df.columns if c.startswith('YR')]

stack_args = []
for year_col in year_columns:
    year_val = year_col.replace('YR', '')
    stack_args.append(f"'{year_val}', `{year_col}`")

stack_expression = f"stack({len(year_columns)}, {', '.join(stack_args)}) as (Year, Value)"

non_year_columns = [c for c in df.columns if not c.startswith('YR')]
unpivot_df = df.select(*non_year_columns, expr(stack_expression))
unpivot_df = unpivot_df.filter(F.col("Value").isNotNull())

(unpivot_df.coalesce(1)
   .write.mode("overwrite")
   .option("header", True)
   .csv((output_dir_path / "4BA_transformation_unpivot.csv").as_posix()))