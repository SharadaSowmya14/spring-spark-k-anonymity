package com.spark.data_generator;

public class CustomCar {
	String carModel;
	String chargingMethod;
	String smartChargingStatus;
	
	public String getCarModel() {
		return carModel;
	}
	
	public String getChargingMethod() {
		return chargingMethod;
	}

	public String getSmartChargingStatus() {
		return smartChargingStatus;
	}
	
	public CustomCar(String carModel, String chargingMethod, String smartChargingStatus) {
		this.carModel = carModel;
		this.chargingMethod = chargingMethod;
		this.smartChargingStatus = smartChargingStatus;
	}
}
