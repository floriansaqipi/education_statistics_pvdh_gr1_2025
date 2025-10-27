from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Non-year columns
non_year_fields = [
    "INDICATOR", "name", "SEX", "URBANIZATION", "AGE", "COMP_BREAKDOWN_1",
    "INDICATOR_ROOT", "UNIT_MEASURE", "UNIT_TYPE", "INDICATOR_ROOT_NAME",
    "economy", "Country name", "Indicator name"
]

# Full schema
schema = StructType(
    [StructField(c, StringType(), True) for c in non_year_fields] +
    [StructField(f"YR{year}", DoubleType(), True) for year in range(1960, 2030)]
)