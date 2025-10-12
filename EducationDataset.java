package org.chapter01;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class EducationDataset {
  public static void main(String[] args) {
    EducationDataset app = new EducationDataset();
    app.start();
  }

  private void start(){
    SparkSession spark = SparkSession.builder()
        .appName("CSV To Dataset")
        .master("local")
        .getOrCreate();

    Dataset<Row> df = spark.read()
        .format("csv")
        .option("header", true)
        .load("data/ONTIME_REPORTING_02.csv");


    long rowCount = df.count();


    double fraction = 10000.0 / rowCount;


    Dataset<Row> sampled = df.sample(false, fraction, 42);


    Dataset<Row> finalSample = sampled.limit(10000);

    finalSample.coalesce(1).write()
        .option("header", true)
        .mode("overwrite")
        .csv("data/sample_output_1.csv");
  }
}
