package com.spark.arx;

import org.deidentifier.arx.AttributeType;
import org.deidentifier.arx.Data;
import org.deidentifier.arx.DataType;
import org.deidentifier.arx.aggregates.HierarchyBuilder;
import org.deidentifier.arx.aggregates.HierarchyBuilderDate;
import org.deidentifier.arx.aggregates.HierarchyBuilderIntervalBased;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased;

import java.io.IOException;
import java.util.Date;

public class DataHierarchyBuilder {
  Data dataSource = null;
  HierarchyBuilderRedactionBased<?> carIdBuilder = null;
  AttributeType.Hierarchy.DefaultHierarchy chargingStatusBuilder = null;
  HierarchyBuilderIntervalBased<Double> fuelPercentageBuilder = null;
  HierarchyBuilderIntervalBased<Double> latitudeBuilder = null;
  HierarchyBuilderIntervalBased<Double> longitudeBuilder = null;
  HierarchyBuilder<Date> timeBuilder = null;

  public Data getDataSource() {
    return dataSource;
  }

  DataHierarchyBuilder(Data dataSource) {
    this.dataSource = dataSource;
  }

  public void setDataSourceType(Data dataSource) {
    dataSource.getDefinition().setDataType("car_id", DataType.STRING);
    dataSource.getDefinition().setDataType("car_model", DataType.STRING);
    dataSource.getDefinition().setDataType("charging_method", DataType.STRING);
    dataSource.getDefinition().setDataType("charging_status", DataType.STRING);
    dataSource.getDefinition().setDataType("smart_charging_status", DataType.STRING);
    dataSource.getDefinition().setDataType("mileage", DataType.INTEGER);
    dataSource.getDefinition().setDataType("fuel_percentage", DataType.DECIMAL);
    dataSource.getDefinition().setDataType("isc_timestamp", DataType.STRING);
    dataSource.getDefinition().setDataType("gps_lat", DataType.DECIMAL);
    dataSource.getDefinition().setDataType("gps_long", DataType.DECIMAL);
    dataSource.getDefinition().setDataType("temperature_external", DataType.INTEGER);
  }

  public void initCarIdBuilder() {
    this.carIdBuilder = HierarchyBuilderRedactionBased.create(HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT,
        HierarchyBuilderRedactionBased.Order.RIGHT_TO_LEFT, ' ', '*');
    this.carIdBuilder.setAlphabetSize(40, 36);
  }

  public void initChargingStatusBuilder() {
    this.chargingStatusBuilder = AttributeType.Hierarchy.create();
    this.chargingStatusBuilder.add("CHARGING_INACTIVE", "*");
    this.chargingStatusBuilder.add("CHARGING_ACTIVE", "*");
  }

  public void initFuelPercentageBuilder() {
    this.fuelPercentageBuilder = HierarchyBuilderIntervalBased.create(DataType.DECIMAL);
    this.fuelPercentageBuilder.addInterval(0d, 20.0d, "very low");
    this.fuelPercentageBuilder.addInterval(20.0d, 35.0d, "low");
    this.fuelPercentageBuilder.addInterval(35.0d, 65.0d, "normal");
    this.fuelPercentageBuilder.addInterval(65.0d, 90.4d, "full");
    this.fuelPercentageBuilder.addInterval(90.4d, 100.1d, "very full");
  }

  public void initTimeStampBuilder() throws IOException {
    this.timeBuilder = HierarchyBuilderDate.create(DataType.createDate("yyyy-MM-dd HH:mm:SS"),
        HierarchyBuilderDate.Granularity.HOUR_DAY_MONTH_YEAR, HierarchyBuilderDate.Granularity.DAY_MONTH_YEAR,
        HierarchyBuilderDate.Granularity.MONTH_YEAR, HierarchyBuilderDate.Granularity.YEAR);
  }

  public void initLatitudeBuilder() {
    this.latitudeBuilder = HierarchyBuilderIntervalBased.create(DataType.DECIMAL);
    this.latitudeBuilder.addInterval(1.00d, 10.00d, "[1.00-10.00]");
    this.latitudeBuilder.addInterval(10.00d, 20.00d, "[10.00-20.00]");
    this.latitudeBuilder.addInterval(20.00d, 30.00d, "[20.00-30.00]");
    this.latitudeBuilder.addInterval(30.00d, 40.00d, "[30.00-0.00]");
    this.latitudeBuilder.addInterval(40.00d, 50.00d, "[40.00-50.00]");
    this.latitudeBuilder.addInterval(50.00d, 60.00d, "[50.00-60.00]");
    this.latitudeBuilder.addInterval(60.00d, 70.00d, "[60.00-70.00]");
    this.latitudeBuilder.addInterval(70.00d, 80.00d, "[70.00-80.00]");
    this.latitudeBuilder.addInterval(80.00d, 90.00d, "[80.00-90.00]");
    this.latitudeBuilder.addInterval(90.00d, 100.00d, "[90.00-100.00]");
    this.latitudeBuilder.addInterval(100.00d, 110.00d, "[100.00-110.00]");
    this.latitudeBuilder.addInterval(110.00d, 120.00d, "[110.00-120.00]");
    this.latitudeBuilder.addInterval(120.00d, 130.00d, "[120.00-130.00]");
    this.latitudeBuilder.addInterval(130.00d, 140.00d, "[130.00-140.00]");
    this.latitudeBuilder.addInterval(140.00d, 150.00d, "[140.00-150.00]");
    this.latitudeBuilder.addInterval(150.00d, 160.00d, "[150.00-160.00]");
    this.latitudeBuilder.addInterval(160.00d, 170.00d, "[160.00-170.00]");
    this.latitudeBuilder.addInterval(170.00d, 180.00d, "[170.00-180.00]");
    this.latitudeBuilder.addInterval(180.00d, 190.00d, "[180.00-190.00]");
    this.latitudeBuilder.addInterval(190.00d, 200.00d, "[190.00-200.00]");
  }

  public void initLongitudeBuilder() {
    this.longitudeBuilder = HierarchyBuilderIntervalBased.create(DataType.DECIMAL);
    this.longitudeBuilder.addInterval(1.00d, 10.00d, "[1.00-10.00]");
    this.longitudeBuilder.addInterval(10.00d, 20.00d, "[10.00-20.00]");
    this.longitudeBuilder.addInterval(20.00d, 30.00d, "[20.00-30.00]");
    this.longitudeBuilder.addInterval(30.00d, 40.00d, "[30.00-40.00]");
    this.longitudeBuilder.addInterval(40.00d, 50.00d, "[40.00-50.00]");
    this.longitudeBuilder.addInterval(50.00d, 60.00d, "[50.00-60.00]");
    this.longitudeBuilder.addInterval(60.00d, 70.00d, "[60.00-70.00]");
    this.longitudeBuilder.addInterval(70.00d, 80.00d, "[70.00-80.00]");
    this.longitudeBuilder.addInterval(80.00d, 90.00d, "[80.00-90.00]");
    this.longitudeBuilder.addInterval(90.00d, 100.00d, "[90.00-100.00]");
    this.longitudeBuilder.addInterval(100.00d, 110.00d, "[100.00-110.00]");
    this.longitudeBuilder.addInterval(110.00d, 120.00d, "[110.00-120.00]");
    this.longitudeBuilder.addInterval(120.00d, 130.00d, "[120.00-130.00]");
    this.longitudeBuilder.addInterval(130.00d, 140.00d, "[130.00-140.00]");
    this.longitudeBuilder.addInterval(140.00d, 150.00d, "[140.00-150.00]");
    this.longitudeBuilder.addInterval(150.00d, 160.00d, "[150.00-160.00]");
    this.longitudeBuilder.addInterval(160.00d, 170.00d, "[160.00-170.00]");
    this.longitudeBuilder.addInterval(170.00d, 180.00d, "[170.00-180.00]");
    this.longitudeBuilder.addInterval(180.00d, 190.00d, "[180.00-190.00]");
    this.longitudeBuilder.addInterval(190.00d, 200.00d, "[190.00-200.00]");
  }

  public void setDataSourceAttributeTypes() {
    this.dataSource.getDefinition().setAttributeType("car_id", AttributeType.IDENTIFYING_ATTRIBUTE);
    this.dataSource.getDefinition().setAttributeType("car_model", AttributeType.INSENSITIVE_ATTRIBUTE);
    this.dataSource.getDefinition().setAttributeType("charging_method", AttributeType.INSENSITIVE_ATTRIBUTE);
    this.dataSource.getDefinition().setAttributeType("charging_status", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
    this.dataSource.getDefinition().setAttributeType("smart_charging_status", AttributeType.INSENSITIVE_ATTRIBUTE);
    this.dataSource.getDefinition().setAttributeType("mileage", AttributeType.INSENSITIVE_ATTRIBUTE);
    this.dataSource.getDefinition().setAttributeType("fuel_percentage", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
    this.dataSource.getDefinition().setAttributeType("isc_timestamp", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
    this.dataSource.getDefinition().setAttributeType("gps_lat", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
    this.dataSource.getDefinition().setAttributeType("gps_long", AttributeType.QUASI_IDENTIFYING_ATTRIBUTE);
    this.dataSource.getDefinition().setAttributeType("temperature_external", AttributeType.INSENSITIVE_ATTRIBUTE);
  }

  public void setDataSourceHierarchy() {
    this.dataSource.getDefinition().setHierarchy("charging_status", this.chargingStatusBuilder);
    this.dataSource.getDefinition().setHierarchy("fuel_percentage", this.fuelPercentageBuilder);
    this.dataSource.getDefinition().setHierarchy("gps_lat", this.latitudeBuilder);
    this.dataSource.getDefinition().setHierarchy("gps_long", this.longitudeBuilder);
    this.dataSource.getDefinition().setHierarchy("isc_timestamp", this.timeBuilder);
  }
}
