from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType

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

integrated_outlier_schema = StructType(
    [StructField(c, StringType(), True) for c in non_year_fields] +
    [StructField(c, StringType(), True) for c in integrated_fields] +
    [StructField("is_outlier", BooleanType())] +
    [StructField(f"YR{year}", DoubleType(), True) for year in range(1970, 2025)]
)

long_schema = StructType(
    [StructField(c, StringType(), True) for c in non_year_fields] +
    [StructField(c, StringType(), True) for c in integrated_fields] +
    [StructField("is_valid", BooleanType())] +
    [StructField("Year", IntegerType())] +
    [StructField("Value", DoubleType())]
)

long_outlier_schema = StructType(
    [StructField(c, StringType(), True) for c in non_year_fields] +
    [StructField(c, StringType(), True) for c in integrated_fields] +
    [StructField("is_outlier", BooleanType())] +
    [StructField("Year", IntegerType())] +
    [StructField("Value", DoubleType())]
)

normalization_fields = [
    "val_std", "mu", "sd", "z"
]

normalized_schema = StructType(
    [StructField(c, StringType(), True) for c in non_year_fields] +
    [StructField(c, StringType(), True) for c in integrated_fields] +
    [StructField("is_valid", BooleanType())] +
    [StructField("Year", IntegerType())] +
    [StructField("Value", DoubleType())] +
    [StructField(c, DoubleType(), True) for c in normalization_fields]
)

attributes_schema = StructType(
    [StructField("economy", StringType(), True)] +
    [StructField("Country name", StringType(), True)] +
    [StructField(c, StringType(), True) for c in integrated_fields] +
    [StructField("EII_z", DoubleType())] +
    [StructField("k_fin", IntegerType())] +
    [StructField("OUTCOME_z", DoubleType())] +
    [StructField("k_out", IntegerType())] +
    [StructField("EII_pos", DoubleType())] +
    [StructField("Efficiency", DoubleType())]
)

discrete_schema = StructType(
    [StructField("economy", StringType(), True)] +
    [StructField("Country name", StringType(), True)] +
    [StructField(c, StringType(), True) for c in integrated_fields] +
    [StructField("EII_z", DoubleType())] +
    [StructField("k_fin", IntegerType())] +
    [StructField("OUTCOME_z", DoubleType())] +
    [StructField("k_out", IntegerType())] +
    [StructField("EII_pos", DoubleType())] +
    [StructField("Efficiency", DoubleType())] +
    [StructField("EII_band", IntegerType())] +
    [StructField("OUTCOME_band", IntegerType())] +
    [StructField("Efficiency_band", IntegerType())] +
    [StructField("EII_high", IntegerType())] +
    [StructField("OUTCOME_high", IntegerType())] +
    [StructField("Efficiency_high", IntegerType())]
)
