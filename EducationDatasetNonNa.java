package org.chapter01;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class EducationDataset1 {
  public static void main(String[] args) {
    EducationDataset1 app = new EducationDataset1();
    app.start();
  }

  private void start(){
    SparkSession spark = SparkSession.builder()
        .appName("EdStats Non-NA Count")
        .master("local")
        .getOrCreate();

    Dataset<Row> df = spark.read()
        .format("csv")
        .option("header", true)
        .load("data/Edstats_Updated.csv");

    // List to store results
    List<Row> results = new ArrayList<>();
    StructType schema = new StructType()
        .add("Year", "string")
        .add("NonNA_Count", "long");

    for (int year = 1960; year <= 2025; year++) {
      String colName = "YR" + year;
      if (Arrays.asList(df.columns()).contains(colName)) {
        long count = df.filter(col(colName).notEqual("NA")).count();
        results.add(RowFactory.create(colName, count));
      }
    }

    // Create DataFrame from results
    Dataset<Row> countsDF = spark.createDataFrame(results, schema);

    // Show the results
    countsDF.show();

    // Save to CSV
    countsDF.coalesce(1)  // single CSV file
        .write()
        .option("header", true)
        .mode("overwrite")
        .csv("data/Yearly_NonNA_Counts.csv");

    spark.stop();
  }
}
