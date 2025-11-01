from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

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

integrated_fields = [
    "Region", "Income group", "Lending category"
]

integrated_schema = StructType(
    [StructField(c, StringType(), True) for c in non_year_fields] +
    [StructField(c, StringType(), True) for c in integrated_fields] +
    [StructField("is_valid", BooleanType())] +
    [StructField(f"YR{year}", DoubleType(), True) for year in range(1970, 2025)]
)
