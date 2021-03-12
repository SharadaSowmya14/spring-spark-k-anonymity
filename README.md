# Spring-Spark-K-Anonymity
This repository contains a Spring-boot application with services exposed to generate and anonymize data sets in a distributed manner.

## Description
Implements K-Anonymization in a distributed manner using:
- Apache Spark for partitioning and merging of data set.
- Executor Service for asynchronous anonymization of partitions.
- ARX library for applying K-Anonymization on partitioned data set.

## Prerequisites
1. Install Java as described in [How do I install Java?](https://java.com/en/download/help/download_options.html) 
2. Install Maven as described in [Installing Apache Maven](https://maven.apache.org/install.html) 
3. Install Apache Spark in [Spark Overview](https://spark.apache.org/docs/latest/) 

## Build and Run
- Build the project by installing all dependencies required using the below command:
```
mvn clean install
```
- Run the spring-boot application using the below command:
```
mvn spring-boot:run
```
The application will run on port ```8080``` by default.

## Data Set Generation
1. For generating data set, run the below Standalone Java application:
```
javac DataSetGenerator.java
java DataSetGenerator.java
```
2. To increase the number of records in the data set, the below-mentioned strategies can be used:
    - By icreasing the number of cars.
    - By capturing latitude and longitude for shorter distance intervals.

## Anonymization
1. Below endpoints are exposed as part of the Spring application.

| Method | Request | Description |
| --- | --- | --- |
| POST | /api/health | Reports if the application has started. |
| POST | /api/anonymize | Anonymizes the created data set. |

2. The number of partitions can be altered as shown below.
```
sparkConfigurator.loadDataSource(carSchema, 20);
```
3. The K-value can be specified as shown below.
```
config.addPrivacyModel(new KAnonymity(10));
```
