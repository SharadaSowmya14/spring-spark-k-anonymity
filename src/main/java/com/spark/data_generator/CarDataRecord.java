package com.spark.data_generator;

import java.util.UUID;

public class CarDataRecord {
	UUID car_id;
	String car_model;
	String charging_method;
	String charging_status;
	String smart_charging_status;
	Integer mileage;
	Double fuel_percentage;
	Double gps_lat;
	Double gps_long;
	Integer temperature;
	String timestamp;
	
	public UUID getCar_id() {
		return car_id;
	}

	public String getCar_model() {
		return car_model;
	}

	public String getCharging_method() {
		return charging_method;
	}

	public String getCharging_status() {
		return charging_status;
	}

	public String getSmart_charging_status() {
		return smart_charging_status;
	}

	public Integer getMileage() {
		return mileage;
	}

	public Double getFuel_percentage() {
		return Math.floor(fuel_percentage * 100) / 100;
	}

	public Double getGps_lat() {
		return gps_lat;
	}

	public Double getGps_long() {
		return gps_long;
	}
	
	public Integer getTemperature() {
		return temperature;
	}
	
	public String getTimestamp() {
		return timestamp;
	}
	
	public void setCar_id(UUID car_id) {
		this.car_id = car_id;
	}
	
	public void setCar_model(String car_model) {
		this.car_model = car_model;
	}
	
	public void setCharging_method(String charging_method) {
		this.charging_method = charging_method;
	}
	
	public void setCharging_status(String charging_status) {
		this.charging_status = charging_status;
	}
	
	public void setSmart_charging_status(String smart_charging_status) {
		this.smart_charging_status = smart_charging_status;
	}
	
	public void setMileage(Integer mileage) {
		this.mileage = mileage;
	}
	
	public void setFuel_percentage(Double fuel_percentage) {
		this.fuel_percentage = fuel_percentage;
	}
	
	public void setGps_lat(Double gps_lat) {
		this.gps_lat = gps_lat;
	}
	
	public void setGps_long(Double gps_long) {
		this.gps_long = gps_long;
	}
	
	public void setTemperature(Integer temperature) {
		this.temperature = temperature;
	}
	
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
}
