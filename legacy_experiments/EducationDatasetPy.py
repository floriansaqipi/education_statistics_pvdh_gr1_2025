from pyspark.sql import SparkSession

def main():
    
    spark = SparkSession.builder \
        .appName("CSV To Dataset") \
        .master("local") \
        .getOrCreate()

    df = spark.read \
        .format("csv") \
        .option("header", True) \
        .load("data/ONTIME_REPORTING_02.csv")

    row_count = df.count()

    fraction = 10000.0 / row_count if row_count > 0 else 0

    sampled = df.sample(False, fraction, seed=42)

    final_sample = sampled.limit(10000)

    final_sample.coalesce(1).write \
        .option("header", True) \
        .mode("overwrite") \
        .csv("data/sample_output_1.csv")

    spark.stop()

if __name__ == "__main__":
    main()
