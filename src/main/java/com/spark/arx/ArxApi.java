package com.spark.arx;

import com.opencsv.CSVWriter;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.deidentifier.arx.*;
import org.deidentifier.arx.criteria.KAnonymity;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ArxApi {
  static Logger logger = LogManager.getRootLogger();
  
  public void anonymization(String inputFilePath, String outputFilePath) throws IOException {
    Data dataSource = Data.create(inputFilePath, Charset.defaultCharset(), ',');
    DataHierarchyBuilder dataBuilder = new DataHierarchyBuilder(dataSource);
    dataBuilder.setDataSourceType(dataSource);

    // Setup hierarchy builders.
    dataBuilder.initCarIdBuilder();
    dataBuilder.initChargingStatusBuilder();
    dataBuilder.initFuelPercentageBuilder();
    dataBuilder.initLatitudeBuilder();
    dataBuilder.initLongitudeBuilder();
    dataBuilder.initTimeStampBuilder();

    // Define the different attribute types
    dataBuilder.setDataSourceAttributeTypes();

    // Set Hierarchies
    dataBuilder.setDataSourceHierarchy();

    ARXConfiguration config = ARXConfiguration.create();
    config.addPrivacyModel(new KAnonymity(10));
    config.setSuppressionLimit(1.0d);
    
    ARXAnonymizer anonymize = new ARXAnonymizer();
    ARXResult result = anonymize.anonymize(dataSource, config);
    
    Iterator<String[]> transformed = result.getOutput(false).iterator();

    // Write into CSV file
    try (Writer writer = Files.newBufferedWriter(Paths.get(outputFilePath));

        CSVWriter csvWriter = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
            CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);) {
      while (transformed.hasNext()) {
        csvWriter.writeNext(transformed.next());
      }
    }
  }

  public static void printResult(final ARXResult result, final Data data) {
    // Print time
    final DecimalFormat df1 = new DecimalFormat("#####0.00");
    final String sTotal = df1.format(result.getTime() / 1000d) + "s";
    logger.info(" - Time needed: " + sTotal);

    // Extract
    final ARXLattice.ARXNode optimum = result.getGlobalOptimum();
    final List<String> qis = new ArrayList<String>(data.getDefinition().getQuasiIdentifyingAttributes());

    if (optimum == null) {
      logger.info(" - No solution found!");
      return;
    }

    // Initialize
    final StringBuffer[] identifiers = new StringBuffer[qis.size()];
    final StringBuffer[] generalizations = new StringBuffer[qis.size()];
    int lengthI = 0;
    int lengthG = 0;
    for (int i = 0; i < qis.size(); i++) {
      identifiers[i] = new StringBuffer();
      generalizations[i] = new StringBuffer();
      identifiers[i].append(qis.get(i));
      generalizations[i].append(optimum.getGeneralization(qis.get(i)));
      if (data.getDefinition().isHierarchyAvailable(qis.get(i)))
        generalizations[i].append("/").append(data.getDefinition().getHierarchy(qis.get(i))[0].length - 1);
      lengthI = Math.max(lengthI, identifiers[i].length());
      lengthG = Math.max(lengthG, generalizations[i].length());
    }

    // Padding
    for (int i = 0; i < qis.size(); i++) {
      while (identifiers[i].length() < lengthI) {
        identifiers[i].append(" ");
      }
      while (generalizations[i].length() < lengthG) {
        generalizations[i].insert(0, " ");
      }
    }

    // Print
    logger.info(" - Information loss: " + result.getGlobalOptimum().getLowestScore() + " / "
        + result.getGlobalOptimum().getHighestScore());
    logger.info(" - Optimal generalization");
    for (int i = 0; i < qis.size(); i++) {
      logger.info("   * " + identifiers[i] + ": " + generalizations[i]);
    }
    logger.info(" - Statistics");
    System.out
        .println(result.getOutput(result.getGlobalOptimum(), false).getStatistics().getEquivalenceClassStatistics());
  }
}
