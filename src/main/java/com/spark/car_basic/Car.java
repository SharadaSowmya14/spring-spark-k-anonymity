package com.spark.car_basic;

import java.io.Serializable;
import java.util.UUID;
import java.util.stream.Stream;

public class Car implements Serializable {
	UUID car_id;
	String car_model;
	String charging_method;
	String charging_status;
	String smart_charging_status;
	Integer mileage;
	Double fuel_percentage;
    String isc_timestamp;
	Double gps_lat;
	Double gps_long;
	Integer temperature_external;

  public Car(UUID car_id, String car_model, String charging_method, String charging_status, String smart_charging_status, Integer mileage, Double fuel_percentage, String isc_timestamp, Double gps_lat, Double gps_long, Integer temperature_external) {
      super();
      this.car_id = car_id;
      this.car_model = car_model;
      this.charging_method = charging_method;
      this.charging_status = charging_status;
      this.smart_charging_status = smart_charging_status;
      this.mileage = mileage;
      this.fuel_percentage = fuel_percentage;
      this.isc_timestamp = isc_timestamp;
      this.gps_lat = gps_lat;
      this.gps_long = gps_long;
      this.temperature_external = temperature_external;
  }
  
  public static Car parseCarData(String str) {
      String[] fields = str.split(",");
      if (fields.length != 11) {
          System.out.println("The elements are ::" );
          Stream.of(fields).forEach(System.out::println);
          throw new IllegalArgumentException("Each line must contain 11 fields while the current line has ::" + fields.length);
      }
      UUID car_id = UUID.fromString(fields[0]);
      String car_model = fields[1].trim();
      String charging_method = fields[2].trim();
      String charging_status = fields[3].trim();
      String smart_charging_status = fields[4].trim();
      Integer mileage = Integer.parseInt(fields[5]);
      Double fuel_percentage = Double.parseDouble(fields[6]);
      String isc_timestamp = fields[7].trim();
      Double gps_lat = Double.parseDouble(fields[8]);
      Double gps_long = Double.parseDouble(fields[9]);
      Integer temperature_external = Integer.parseInt(fields[10]);
      return new Car(car_id,car_model, charging_method, charging_status, smart_charging_status, mileage, fuel_percentage, isc_timestamp, gps_lat, gps_long, temperature_external);
  }
}
