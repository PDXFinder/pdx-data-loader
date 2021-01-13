package org.pdxfinder.pdxdataloader;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PatientDataLoader {
  public void load() {
    System.out.println("Loading patient data");

    // The entry point to programming Spark with the Dataset and DataFrame API.
    SparkSession sparkSession = SparkSession.builder()
        .master("local[*]") // only for demo and testing purposes
        .appName("PDX data loader")
        .config("spark.some.config.option", "some-value") // Just an example of setting configuration
        .getOrCreate();

    // Read the tsv file
    Dataset<Row> df = sparkSession.read()
        .option("header","true")
        .option("delimiter", "\t")
        .csv("Curie-BC_metadata-patient.tsv");

    // Ignore the 4 first lines of the content because they are not data
    // (Description, Example , Format, and Essential?)
    Dataset<Row> indexed = df.withColumn("index", monotonically_increasing_id());
    Dataset<Row> filtered = indexed.filter(col("index").$greater(3)).drop("index");

    // Create a dataset only with the columns we want to process
    Dataset<Row> sqlResult = filtered.select(
        col("patient_id").alias("id"),
        col("sex"),
        col("history"),
        col("ethnicity"),
        col("ethnicity_assessment_method"),
        col("initial_diagnosis"),
        col("age_at_initial_diagnosis")
    );

    // Save the dataset into table "patient"
    String url = "jdbc:postgresql://localhost/pdx";
    Properties props = new Properties();
    props.setProperty("user","pdx_admin");
    props.setProperty("password","pdx_admin");
    sqlResult.write().mode("append").jdbc(url,"patient",props);
    sqlResult.show();
  }
}
