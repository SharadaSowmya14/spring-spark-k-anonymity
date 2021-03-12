# Spring-Spark-K-Anonymity
This repository contains Spring-boot application with services exposed to generate and anonymize car data sets in a disrtibuted manner.

## Description
Implements K-Anonymization in disrtibuted manner using:
- Apache Spark for partitioning and merging of data set.
- Executor Service for asynchronous anonymization of partitions.
- ARX library for applying K-Anonymization on partitioned data set.

## Prerequisites
1. Install Java
2. Install Maven
3. Install Apache Spark

## Build and Run
- Build the project by installing all dependencies required using the below command:
```
mvn clean install
```
- Run the spring-boot application using the below command:
```
mvn spring-boot:run
```


