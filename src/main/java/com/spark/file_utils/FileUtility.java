package com.spark.file_utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class FileUtility {
  // TODO: read from properties file.
  final static String ANONYMIZED_DIR_PATH = "src/main/resources/output-data/anonymized/";
  final static String PARTITIONS_DIR_PATH = "src/main/resources/output-data/partitions/";
  final static String MERGED_DIR_PATH = "src/main/resources/output-data/merged/";
  
  public static List<String> getFileNames(String path, String filePattern) {
    List<String> fileNameList = new ArrayList<>();
    File[] files = new File(path).listFiles();
    assert files != null;
    for (File file : files) {
      String fileName = file.getName();
      if (fileName.endsWith(filePattern)) {
        fileNameList.add(fileName);
      }
    }
    return fileNameList;
  }
  
  public static void deleteFilesBeforeRun() throws IOException {
    // delete anonymized and un-anoymized partitions
    File anonymizedDirectory = new File(ANONYMIZED_DIR_PATH);
    File partitionsDirectory = new File(PARTITIONS_DIR_PATH);
    File margedDirectory = new File(MERGED_DIR_PATH);
    FileUtils.cleanDirectory(anonymizedDirectory);
    FileUtils.deleteDirectory(partitionsDirectory);
    FileUtils.deleteDirectory(margedDirectory);
  }
}
