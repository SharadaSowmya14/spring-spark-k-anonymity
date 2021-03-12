package com.spark.car_basic;

import com.spark.arx.ArxApi;
import com.spark.file_utils.FileUtility;
import com.spark.spark_config.SparkConfigurator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class DataAnonymizer {
  /*@Autowired
  SparkConfigurator sparkConfigurator;*/

  static Logger logger = LogManager.getRootLogger();
  
  public void anonymize() {
    try {
      FileUtility.deleteFilesBeforeRun();
    } catch(Exception e) {
      logger.error("Error deleting files" + e.getMessage());
    }

    StructType carSchema = new StructType(
      new StructField[] { new StructField("car_id", DataTypes.StringType, true, Metadata.empty()),
        new StructField("car_model", DataTypes.StringType, true, Metadata.empty()),
        new StructField("charging_method", DataTypes.StringType, true, Metadata.empty()),
        new StructField("charging_status", DataTypes.StringType, true, Metadata.empty()),
        new StructField("smart_charging_status", DataTypes.StringType, true, Metadata.empty()),
        new StructField("mileage", DataTypes.IntegerType, true, Metadata.empty()),
        new StructField("fuel_percentage", DataTypes.DoubleType, true, Metadata.empty()),
        new StructField("isc_timestamp", DataTypes.StringType, true, Metadata.empty()),
        new StructField("gps_lat", DataTypes.DoubleType, true, Metadata.empty()),
        new StructField("gps_long", DataTypes.DoubleType, true, Metadata.empty()),
        new StructField("temperature_external", DataTypes.IntegerType, true, Metadata.empty()),
      });

    SparkConfigurator sparkConfigurator = new SparkConfigurator();
    sparkConfigurator.startSparkSession();
    long startTime1 = System.currentTimeMillis();
    // TODO: Read 20 from properties file.
    Dataset<Row> carSet = sparkConfigurator.loadDataSource(carSchema, 20);
    
    // Write partitions to csv files
    sparkConfigurator.writePartitionsToCSV(carSet);
    sparkConfigurator.closeSparkSession();

    long endTime1 = System.currentTimeMillis();
    long totalTime1 = endTime1 - startTime1;
    logger.info("PDS = " + totalTime1);

    List<String> inputFileNameList = FileUtility.getFileNames("src/main/resources/output-data/partitions", ".csv");

    // anonymize the partition files
    anonymizePartitions(inputFileNameList, sparkConfigurator);
  }

  public void createRunnables(
    String inputFilePath,
    String outputFilePath,
    ExecutorService executorService,
    ArxApi arxApi
  ) {
    Runnable runnable = () -> {
      try {
        logger.info("Running anonymisation for partition " + inputFilePath);
        long startTime = System.currentTimeMillis();
        arxApi.anonymization(inputFilePath, outputFilePath);
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        logger.info("Completed anonymisation for partition " + inputFilePath + "in time: " + totalTime);
      } catch (Exception e) { logger.error(e.getMessage()); }
    };
    executorService.submit(runnable);
  }

  public void anonymizePartitions(List<String> inputFileNameList, SparkConfigurator sparkConfigurator) {
    ArxApi arxApi = new ArxApi();
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    int partitionNo = 1;
    for (String fileName : inputFileNameList) {
      String inputFilePath = "src/main/resources/output-data/partitions/" + fileName;
      String outputFilePath = "src/main/resources/output-data/anonymized/output-anonymized-" + partitionNo + ".csv";

      createRunnables(inputFilePath, outputFilePath, executorService, arxApi);

      partitionNo += 1;
    }
    executorService.shutdown();
    try {
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      logger.info("Anonymization completed. Merging partitions");
      logger.info("Merging partitions");
      long startTime3 = System.currentTimeMillis();
      sparkConfigurator.startSparkSession();
      sparkConfigurator.mergePartitions();
      sparkConfigurator.closeSparkSession();
      long endTime3 = System.currentTimeMillis();
      long totalTime3 = endTime3 - startTime3;
      logger.info("MDS = " + totalTime3);
    } catch (Exception e) {
      logger.info("Exception in running executor service " + e.getMessage());
    }
  }
}
