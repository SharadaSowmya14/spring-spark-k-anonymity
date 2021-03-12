package com.spark.data_generator;

public class CustomCoordinate {
	double lat;
	double lng;
	
	public void setLat(double lat) {
		this.lat = lat;
	}

	public void setLng(double lng) {
		this.lng = lng;
	}
	
	public double getLat() {
		return lat;
	}
	
	public double getLng() {
		return lng;
	}
	
	public CustomCoordinate(Double lng, Double lat) {
		this.lng = lng;
		this.lat = lat;
	}
}
