package com.spark.data_generator;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DataSetGenerator {
    /**
     * generate custom car data record.
     *
     * @return CustomCar object.
     */
    public static CustomCar generateCustomCar() {
        List<CustomCar> customCars = Arrays.asList(
                new CustomCar("1 Series", "AC_TYPE1PLUG", "RENEWABLE_UNOPTIMIZED"),
                new CustomCar("2 Series", "AC_TYPE2PLUG", "RENEWABLE_OPTIMIZED"),
                new CustomCar("3 Series", "AC_TYPE1PLUG", "RENEWABLE_UNOPTIMIZED"),
                new CustomCar("4 Series", "AC_TYPE2PLUG", "RENEWABLE_OPTIMIZED"),
                new CustomCar("5 Series", "AC_TYPE1PLUG", "RENEWABLE_UNOPTIMIZED"),
                new CustomCar("6 Series", "AC_TYPE2PLUG", "RENEWABLE_OPTIMIZED"),
                new CustomCar("7 Series", "AC_TYPE1PLUG", "RENEWABLE_UNOPTIMIZED"),
                new CustomCar("X1 Series", "AC_TYPE2PLUG", "RENEWABLE_OPTIMIZED"),
                new CustomCar("X2 Series", "AC_TYPE2PLUG", "RENEWABLE_OPTIMIZED"),
                new CustomCar("X3 Series", "AC_TYPE2PLUG", "RENEWABLE_OPTIMIZED"),
                new CustomCar("X4 Series", "AC_TYPE2PLUG", "RENEWABLE_OPTIMIZED")
        );
        Random rand = new Random();
        return customCars.get(rand.nextInt(customCars.size()));
    }

    /**
     * generate random temperature in the specified range.
     *
     * @return random temperature in the specified range.
     */
    public static Integer randomTemperateGenerator() {
        Integer minTemp = -10;
        Integer maxTemp = 50;
        return ThreadLocalRandom.current().nextInt(minTemp, maxTemp);
    }

    /**
     * generate random timestamp in the specified range.
     *
     * @return random timestamp in the specified range.
     */
    public static Timestamp randomTimestampGenerator() {
        long offset = Timestamp.valueOf("2012-01-01 00:00:00").getTime();
        long end = Timestamp.valueOf("2020-01-01 00:00:00").getTime();
        long diff = end - offset + 1;
        return new Timestamp(offset + (long) (Math.random() * diff));
    }

    /**
     * Create the csv file header.
     *
     * @param writer.
     */
    public static void createCSVHeader(PrintWriter writer) {
        StringBuilder sb = new StringBuilder();
        sb.append("car_id;");
        sb.append("car_model;");
        sb.append("charging_method;");
        sb.append("charging_status;");
        sb.append("smart_charging_status;");
        sb.append("mileage;");
        sb.append("fuel_percentage;");
        sb.append("isc_timestamp;");
        sb.append("gps_lat;");
        sb.append("gps_long;");
        sb.append("temperature_external;");
        sb.append("\n");
        writer.write(sb.toString());
    }

    /**
     * Create the car data record in the generated csv.
     *
     * @param carDataRecord
     * @param writer
     */
    public static void addCarDataRecord(CarDataRecord carDataRecord, PrintWriter writer) {
        StringBuilder sb = new StringBuilder();
        sb.append(carDataRecord.getCar_id() + ",");
        sb.append(carDataRecord.getCar_model() + ",");
        sb.append(carDataRecord.getCharging_method() + ",");
        sb.append(carDataRecord.getCharging_status() + ",");
        sb.append(carDataRecord.getSmart_charging_status() + ",");
        sb.append(carDataRecord.getMileage() + ",");
        sb.append(carDataRecord.getFuel_percentage() + ",");
        sb.append(carDataRecord.getTimestamp().toString() + ",");
        sb.append(carDataRecord.getGps_lat() + ",");
        sb.append(carDataRecord.getGps_long() + ",");
        sb.append(carDataRecord.getTemperature());
        sb.append("\n");
        writer.write(sb.toString());
    }

    /**
     * generate card data record for specified custom coordinate.
     *
     * @param customCoordinate - lat and long of the custom coordinate.
     * @param bearing          - in degree from where the car direction should proceed.
     * @param writer
     */
    public static void generateCarDataRecordForCoordinate(CustomCoordinate customCoordinate, double bearing, PrintWriter writer) {
        for (int i = 1; i <= 1; i++) {
            Double distance = 10.00;
            Integer mileage = 60000;
            CustomCar customCar = generateCustomCar();
            UUID carUuid = UUID.randomUUID();
            Double carRemainingFuelPercentage = 100.00;
            String carChargingStatus = "CHARGING_INACTIVE";
            Integer index = 1;
            Timestamp timestamp = randomTimestampGenerator();
            CustomCoordinate e = customCoordinate;

            while (carChargingStatus != "CHARGING_ACTIVE") {
                CarDataRecord carDataRecord = new CarDataRecord();
                carDataRecord.setCar_id(carUuid);
                carDataRecord.setCar_model(customCar.carModel);
                carDataRecord.setCharging_method(customCar.chargingMethod);
                carDataRecord.setCharging_status(carChargingStatus);
                carDataRecord.setSmart_charging_status(customCar.smartChargingStatus);
                carDataRecord.setFuel_percentage(carRemainingFuelPercentage);
                carDataRecord.setMileage(mileage);
                carDataRecord.setGps_lat((double) Math.round(e.getLat() * 1000000) / 1000000);
                carDataRecord.setGps_long((double) Math.round(e.getLng() * 1000000) / 1000000);
                carDataRecord.setTemperature(randomTemperateGenerator());
                carDataRecord.setTimestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp));

                distance += 1;
                index += 1;
                mileage += 10;
                timestamp.setTime(timestamp.getTime() + TimeUnit.MINUTES.toMillis(10));
                e.setLat(e.getLat() + 0.00001);
                e.setLng(e.getLng() + 0.00001);

                if (carRemainingFuelPercentage <= 5) {
                    carDataRecord.setCharging_status("CHARGING_ACTIVE");
                    addCarDataRecord(carDataRecord, writer);
                    for (Double j = carRemainingFuelPercentage; j < 100; j = j + 1) {
                        CarDataRecord carDataRecord1 = new CarDataRecord();
                        carDataRecord1.setCar_id(carUuid);
                        carDataRecord1.setCar_model(customCar.carModel);
                        carDataRecord1.setCharging_method(customCar.chargingMethod);
                        carDataRecord1.setCharging_status("CHARGING_ACTIVE");
                        carDataRecord1.setSmart_charging_status(customCar.smartChargingStatus);
                        carDataRecord1.setFuel_percentage(carRemainingFuelPercentage);
                        carDataRecord1.setMileage(mileage);
                        carDataRecord1.setGps_lat((double) Math.round(e.getLat() * 1000000) / 1000000);
                        carDataRecord1.setGps_long((double) Math.round(e.getLng() * 1000000) / 1000000);
                        carDataRecord1.setTemperature(randomTemperateGenerator());
                        carDataRecord1.setTimestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp));
                        addCarDataRecord(carDataRecord1, writer);
                    }
                    break;
                } else {
                    carRemainingFuelPercentage -= 0.0001;
                    addCarDataRecord(carDataRecord, writer);
                }
            }
        }
    }

    public static void main(String[] args) {
        PrintWriter writer = null;

        try {
            writer = new PrintWriter(new File("src/main/resources/input-data/sample.csv"));
            createCSVHeader(writer);
        } catch (FileNotFoundException e1) {
            System.out.println("error creating file");
            e1.printStackTrace();
        }
        CustomCoordinate c1 = new CustomCoordinate(3.211614, 3.479464);
        CustomCoordinate c2 = new CustomCoordinate(4.211614, 3.479464);/*
        CustomCoordinate c3 = new CustomCoordinate(5.211614, 3.479464);
        CustomCoordinate c4 = new CustomCoordinate(6.211614, 3.479464);
        CustomCoordinate c5 = new CustomCoordinate(7.211614, 3.479464);
        CustomCoordinate c6 = new CustomCoordinate(8.211614, 3.479464);
        CustomCoordinate c7 = new CustomCoordinate(9.211614, 3.479464);
        CustomCoordinate c8 = new CustomCoordinate(10.211614, 3.479464);
        CustomCoordinate c9 = new CustomCoordinate(11.211614, 3.479464);
        CustomCoordinate c10 = new CustomCoordinate(12.211614, 3.479464);
        CustomCoordinate c11 = new CustomCoordinate(13.211614, 3.479464);*/

        generateCarDataRecordForCoordinate(c1, 1, writer);
        generateCarDataRecordForCoordinate(c2, 2, writer);/*
        generateCarDataRecordForCoordinate(c3, 3, writer);
        generateCarDataRecordForCoordinate(c4, 4, writer);
        generateCarDataRecordForCoordinate(c5, 5, writer);
        generateCarDataRecordForCoordinate(c6, 6, writer);
        generateCarDataRecordForCoordinate(c7, 7, writer);
        generateCarDataRecordForCoordinate(c8, 8, writer);
        generateCarDataRecordForCoordinate(c9, 9, writer);
        generateCarDataRecordForCoordinate(c10, 10, writer);
        generateCarDataRecordForCoordinate(c11, 11, writer);*/

        writer.close();
        System.out.println("Done");
    }

}
