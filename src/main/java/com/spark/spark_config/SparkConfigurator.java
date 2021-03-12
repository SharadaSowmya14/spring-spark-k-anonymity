package com.spark.spark_config;

import com.spark.file_utils.FileUtility;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.nio.file.Paths;
import java.util.List;

public class SparkConfigurator {
  final static String ANONYMIZED_DIR_PATH = "src/main/resources/output-data/anonymized/";
  final static String MERGED_FILEPATH = "src/main/resources/output-data/merged/";
  SparkSession sparkSession;

  public void setSparkSession(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  public SparkConfigurator() {
    // TODO: read from properties file.
    System.setProperty("hadoop.home.dir", "c:/hadoop/hadoop-3.2.1");
  }

  public void startSparkSession() {
    SparkSession initSparkSession = SparkSession.builder().master("local[4]")
      .config("spark.sql.warehouse.dir", "file:///D:/Hadoop")
      .config("spark.shuffle.service.index.cache.size", 100)
      .config("spark.network.timeout", "600s")
      .config("spark.executor.heartbeatInterval", "10000s")
      .config("spark.ui.port", "9090").appName("CarSetExample").getOrCreate();

    setSparkSession(initSparkSession);
  }

  public Dataset<Row> loadDataSource(StructType dataSchema, Integer numberOfPartitions) {
    return sparkSession.read().format("com.databricks.spark.csv").option("header", "true")
      .schema(dataSchema).load("src/main/resources/input-data/sample.csv").repartition(numberOfPartitions);
  }

  public void writePartitionsToCSV(Dataset<Row> dataSet) {
    dataSet.write().format("com.databricks.spark.csv").option("header", "true")
      .save("src/main/resources/output-data/partitions");
  }

  public void mergePartitions() {
    Dataset<Row> unifiedDataset = null;
    List<String> partitionFileNames = FileUtility.getFileNames(ANONYMIZED_DIR_PATH, ".csv");

    for (String fileName : partitionFileNames) {
      Dataset<Row> tempDataset = sparkSession.read()
        .option("inferSchema", "true")
        .option("header", "true")
        .format("csv")
        .option("delimiter", ",")
        .load(Paths.get(ANONYMIZED_DIR_PATH + fileName).toString());
      if (unifiedDataset != null) {
        unifiedDataset = unifiedDataset.unionAll(tempDataset);
      } else {
        unifiedDataset = tempDataset;
      }

    }

    try {
      unifiedDataset
        .coalesce(1)
        .write()
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .save(MERGED_FILEPATH);
    } catch (Exception e) {
      System.out.println("ERROR MERGING PARTITIONS" + e.getMessage());
    }
  }

  public void closeSparkSession() {
    this.sparkSession.close();
  }
}
