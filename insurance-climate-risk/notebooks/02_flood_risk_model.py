# Databricks notebook source
# MAGIC %md
# MAGIC # Flood Risk Assessment Model
# MAGIC 
# MAGIC This notebook develops a comprehensive flood risk model using Databricks' geospatial functions.
# MAGIC The model incorporates elevation data, precipitation patterns, drainage analysis, and historical flood events.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

# DBR 16+ imports with native geospatial support
import pyspark.sql.functions as F
from pyspark.sql.types import *
import h3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import math

# Geospatial functions - native in DBR 16+
from pyspark.sql.functions import col, when, lit, expr
import pyspark.sql.functions as sf

# Note: Spark session automatically available in DBR 16+ with Spark Connect
# All H3 and geospatial functions are built-in and optimized

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Parameters

# COMMAND ----------

class FloodConfig:
    # H3 resolution for spatial analysis (resolution 8 = ~0.7km hexagons for detailed flood analysis)
    H3_RESOLUTION = 8
    
    # Flood risk thresholds based on return periods
    FLOOD_RETURN_PERIODS = {
        'low': 100,      # 1 in 100 year flood
        'moderate': 50,   # 1 in 50 year flood
        'high': 25,      # 1 in 25 year flood
        'very_high': 10  # 1 in 10 year flood
    }
    
    # Precipitation intensity thresholds (mm/hour)
    PRECIP_INTENSITY_THRESHOLDS = {
        'light': 2.5,
        'moderate': 10.0,
        'heavy': 50.0,
        'extreme': 100.0
    }
    
    # Elevation analysis parameters
    SLOPE_THRESHOLD = 15.0  # degrees
    ELEVATION_BUFFER = 10.0  # meters for flood plain analysis
    
    # Risk scoring weights
    WEIGHTS = {
        'elevation_factor': 0.30,
        'slope_factor': 0.25,
        'precipitation_intensity': 0.20,
        'drainage_capacity': 0.15,
        'historical_frequency': 0.10
    }

config = FloodConfig()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Elevation and Topographic Data Processing

# COMMAND ----------

def create_elevation_data():
    """Create sample elevation and topographic data"""
    
    # Define explicit schema for DBR 16+ compatibility
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
    
    schema = StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("h3_cell", StringType(), True),
        StructField("elevation_m", DoubleType(), True),
        StructField("slope_degrees", DoubleType(), True),
        StructField("in_floodplain", BooleanType(), True),
        StructField("distance_to_water", DoubleType(), True)
    ])
    
    # Generate sample elevation data for a watershed region
    lat_range = np.arange(37.0, 38.0, 0.01)  # San Francisco Bay Area example
    lon_range = np.arange(-122.5, -121.5, 0.01)
    
    elevation_data = []
    for lat in lat_range[::2]:  # Sample every 2nd point for performance
        for lon in lon_range[::2]:
            h3_cell = h3.latlng_to_cell(lat, lon, config.H3_RESOLUTION)
            
            # Simulate realistic elevation with coastal to inland gradient
            distance_from_coast = abs(lon + 122.0) * 111000  # rough meters from coast
            base_elevation = distance_from_coast * 0.02  # gentle slope inland
            
            # Add random variation and some valleys/hills
            noise = np.random.normal(0, 20)
            elevation = max(0.0, float(base_elevation + noise))
            
            # Calculate slope (simplified - in reality would use neighboring cells)
            slope = min(45.0, float(abs(noise) * 0.5 + np.random.uniform(0, 5)))
            
            # Determine if in floodplain (low elevation near water bodies)
            in_floodplain = elevation < 50 and np.random.random() < 0.3
            
            elevation_data.append((
                float(lat),
                float(lon),
                str(h3_cell),
                elevation,
                slope,
                bool(in_floodplain),
                float(np.random.uniform(100, 5000) if not in_floodplain else np.random.uniform(10, 500))
            ))
    
    return spark.createDataFrame(elevation_data, schema)

def calculate_topographic_factors(elevation_df):
    """Calculate topographic factors that influence flood risk"""
    
    topo_df = elevation_df.withColumn("elevation_risk",
        # Higher risk for lower elevation areas
        F.when(F.col("elevation_m") <= 10, 1.0)
        .when(F.col("elevation_m") <= 50, 0.8)
        .when(F.col("elevation_m") <= 100, 0.6)
        .when(F.col("elevation_m") <= 200, 0.4)
        .otherwise(0.2)
    ).withColumn("slope_risk",
        # Higher risk for flatter areas (poor drainage)
        F.when(F.col("slope_degrees") <= 2, 1.0)
        .when(F.col("slope_degrees") <= 5, 0.8)
        .when(F.col("slope_degrees") <= 10, 0.6)
        .when(F.col("slope_degrees") <= 15, 0.4)
        .otherwise(0.2)
    ).withColumn("floodplain_risk",
        F.when(F.col("in_floodplain"), 1.0).otherwise(0.3)
    ).withColumn("water_proximity_risk",
        F.when(F.col("distance_to_water") <= 100, 1.0)
        .when(F.col("distance_to_water") <= 500, 0.8)
        .when(F.col("distance_to_water") <= 1000, 0.6)
        .when(F.col("distance_to_water") <= 2000, 0.4)
        .otherwise(0.2)
    )
    
    return topo_df

# Create elevation data
print("Generating elevation and topographic data...")
elevation_df = create_elevation_data()
elevation_df = calculate_topographic_factors(elevation_df)
elevation_df = elevation_df.persist()

print(f"Created elevation data for {elevation_df.count()} locations")
elevation_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Precipitation and Weather Data Analysis

# COMMAND ----------

def create_precipitation_data():
    """Create sample precipitation data with various intensities"""
    
    # Define explicit schema for DBR 16+ compatibility
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    
    schema = StructType([
        StructField("h3_cell", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("date", StringType(), True),
        StructField("daily_precipitation_mm", DoubleType(), True),
        StructField("max_hourly_intensity_mm", DoubleType(), True),
        StructField("precipitation_intensity", DoubleType(), True)
    ])
    
    # Get H3 cells from elevation data
    h3_cells = elevation_df.select("h3_cell", "latitude", "longitude").distinct().collect()
    
    precip_data = []
    for row in h3_cells:
        h3_cell = row['h3_cell']
        lat = float(row['latitude'])
        lon = float(row['longitude'])
        
        # Generate 365 days of precipitation data
        for i in range(365):
            date = datetime.now() - timedelta(days=i)
            
            # Seasonal precipitation pattern (winter wet season in CA)
            seasonal_factor = 1.5 if date.month in [11, 12, 1, 2, 3] else 0.5
            
            # Generate daily precipitation with storm events
            base_precip = seasonal_factor * 3.0
            
            # Occasional storm events (5% chance of heavy rain)
            if np.random.random() < 0.05:
                daily_precip = float(np.random.gamma(2, 15) * seasonal_factor)
                max_hourly = daily_precip * 0.6  # Peak intensity
            else:
                daily_precip = max(0.0, float(np.random.gamma(1, base_precip)))
                max_hourly = daily_precip * 0.3
            
            # Calculate 24-hour precipitation intensity
            precip_intensity = max_hourly
            
            precip_data.append((
                str(h3_cell),
                lat,
                lon,
                date.strftime('%Y-%m-%d'),
                daily_precip,
                float(max_hourly),
                float(precip_intensity)
            ))
    
    # Limit for demo and create DataFrame with schema
    limited_data = precip_data[:50000]
    return spark.createDataFrame(limited_data, schema)

def classify_precipitation_events(precip_df):
    """Classify precipitation events by intensity and flood potential"""
    
    classified_df = precip_df.withColumn("intensity_class",
        F.when(F.col("precipitation_intensity") >= config.PRECIP_INTENSITY_THRESHOLDS['extreme'], "extreme")
        .when(F.col("precipitation_intensity") >= config.PRECIP_INTENSITY_THRESHOLDS['heavy'], "heavy")
        .when(F.col("precipitation_intensity") >= config.PRECIP_INTENSITY_THRESHOLDS['moderate'], "moderate")
        .when(F.col("precipitation_intensity") >= config.PRECIP_INTENSITY_THRESHOLDS['light'], "light")
        .otherwise("trace")
    ).withColumn("flood_potential",
        F.when(F.col("intensity_class") == "extreme", 1.0)
        .when(F.col("intensity_class") == "heavy", 0.8)
        .when(F.col("intensity_class") == "moderate", 0.4)
        .when(F.col("intensity_class") == "light", 0.1)
        .otherwise(0.0)
    )
    
    return classified_df

# Create precipitation data
print("Generating precipitation data...")
precipitation_df = create_precipitation_data()
precipitation_df = classify_precipitation_events(precipitation_df)
precipitation_df = precipitation_df.persist()

print(f"Created precipitation data for {precipitation_df.count()} observations")

# Show precipitation intensity distribution
print("Precipitation Intensity Distribution:")
precipitation_df.groupBy("intensity_class").count().orderBy("intensity_class").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Historical Flood Event Analysis

# COMMAND ----------

def create_historical_flood_data():
    """Create sample historical flood event data"""
    
    # Define explicit schema for DBR 16+ compatibility
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    
    schema = StructType([
        StructField("h3_cell", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("event_date", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("flood_depth_m", DoubleType(), True),
        StructField("duration_hours", DoubleType(), True),
        StructField("damage_estimate", DoubleType(), True)
    ])
    
    # Generate historical flood events
    flood_events = []
    h3_cells = elevation_df.select("h3_cell", "latitude", "longitude").collect()
    
    for i in range(100):  # 100 historical flood events
        # Select random location
        location = h3_cells[np.random.randint(0, len(h3_cells))]
        
        # Generate event characteristics
        event_date = datetime.now() - timedelta(days=np.random.randint(1, 3650))  # Last 10 years
        severity = np.random.choice(['minor', 'moderate', 'major', 'severe'], 
                                   p=[0.4, 0.3, 0.2, 0.1])
        
        # Flood depth based on severity
        depth_mapping = {'minor': (0.1, 0.5), 'moderate': (0.5, 1.5), 
                        'major': (1.5, 3.0), 'severe': (3.0, 8.0)}
        min_depth, max_depth = depth_mapping[severity]
        flood_depth = float(np.random.uniform(min_depth, max_depth))
        
        # Duration (hours)
        duration = float(np.random.gamma(2, 8))  # Gamma distribution for duration
        
        flood_events.append((
            str(location['h3_cell']),
            float(location['latitude']),
            float(location['longitude']),
            event_date.strftime('%Y-%m-%d'),
            str(severity),
            flood_depth,
            duration,
            float(flood_depth * 10000 * np.random.uniform(0.5, 2.0))  # Rough damage estimate
        ))
    
    return spark.createDataFrame(flood_events, schema)

def calculate_historical_flood_frequency(flood_df):
    """Calculate historical flood frequency by location"""
    
    # Calculate flood frequency and severity by H3 cell
    freq_df = flood_df.groupBy("h3_cell") \
        .agg(
            F.count("*").alias("flood_event_count"),
            F.avg("flood_depth_m").alias("avg_flood_depth"),
            F.max("flood_depth_m").alias("max_flood_depth"),
            F.avg("duration_hours").alias("avg_duration"),
            F.sum("damage_estimate").alias("total_damage"),
            F.first("latitude").alias("latitude"),
            F.first("longitude").alias("longitude")
        ) \
        .withColumn("annual_flood_frequency", 
            F.col("flood_event_count") / 10.0  # 10 years of data
        ) \
        .withColumn("historical_risk_score",
            F.least(F.lit(1.0), 
                F.col("annual_flood_frequency") * 0.5 + 
                F.col("avg_flood_depth") * 0.2
            )
        )
    
    return freq_df

# Create historical flood data
print("Generating historical flood event data...")
historical_floods_df = create_historical_flood_data()
flood_frequency_df = calculate_historical_flood_frequency(historical_floods_df)

print(f"Created {historical_floods_df.count()} historical flood events")
print("Historical flood frequency summary:")
flood_frequency_df.select("annual_flood_frequency", "avg_flood_depth", "historical_risk_score").describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drainage and Watershed Analysis

# COMMAND ----------

def calculate_drainage_factors():
    """Calculate drainage capacity and watershed characteristics"""
    
    # Simplified drainage analysis based on topography
    drainage_df = elevation_df.withColumn("drainage_capacity",
        # Better drainage for steeper slopes and higher elevation
        F.when(F.col("slope_degrees") >= 15, 0.9)
        .when(F.col("slope_degrees") >= 10, 0.7)
        .when(F.col("slope_degrees") >= 5, 0.5)
        .when(F.col("slope_degrees") >= 2, 0.3)
        .otherwise(0.1)
    ).withColumn("watershed_risk",
        # Higher risk in floodplains and low-lying areas
        F.when(F.col("in_floodplain") & (F.col("elevation_m") <= 20), 1.0)
        .when(F.col("in_floodplain"), 0.8)
        .when(F.col("elevation_m") <= 50, 0.6)
        .otherwise(0.3)
    )
    
    # Calculate flow accumulation (simplified)
    flow_df = drainage_df.withColumn("flow_accumulation",
        F.when(F.col("elevation_m") <= 10, "high")
        .when(F.col("elevation_m") <= 50, "moderate")
        .otherwise("low")
    ).withColumn("drainage_risk",
        F.when(F.col("flow_accumulation") == "high", 1.0)
        .when(F.col("flow_accumulation") == "moderate", 0.6)
        .otherwise(0.2)
    )
    
    return flow_df

# Calculate drainage factors
drainage_analysis_df = calculate_drainage_factors()

print("Drainage capacity distribution:")
drainage_analysis_df.groupBy("flow_accumulation").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Composite Flood Risk Model

# COMMAND ----------

def calculate_flood_risk_score():
    """Calculate composite flood risk score combining all factors"""
    
    # Join all data sources
    base_df = drainage_analysis_df.alias("topo") \
        .join(precipitation_df.alias("precip"), ["h3_cell"], "left") \
        .join(flood_frequency_df.alias("hist"), ["h3_cell"], "left")
    
    # Fill nulls for areas with no historical floods
    risk_df = base_df.fillna({
        'flood_event_count': 0,
        'annual_flood_frequency': 0,
        'historical_risk_score': 0,
        'daily_precipitation_mm': 0,
        'flood_potential': 0
    })
    
    # Calculate weighted composite flood risk score
    risk_df = risk_df.withColumn("flood_risk_score",
        (F.col("elevation_risk") * config.WEIGHTS['elevation_factor'] +
         F.col("slope_risk") * config.WEIGHTS['slope_factor'] +
         F.col("flood_potential") * config.WEIGHTS['precipitation_intensity'] +
         F.col("drainage_risk") * config.WEIGHTS['drainage_capacity'] +
         F.col("historical_risk_score") * config.WEIGHTS['historical_frequency'])
    )
    
    # Classify flood risk levels
    risk_df = risk_df.withColumn("flood_risk_level",
        F.when(F.col("flood_risk_score") >= 0.8, "extreme")
        .when(F.col("flood_risk_score") >= 0.6, "high")
        .when(F.col("flood_risk_score") >= 0.4, "moderate")
        .when(F.col("flood_risk_score") >= 0.2, "low")
        .otherwise("minimal")
    )
    
    # Calculate return period estimation
    risk_df = risk_df.withColumn("estimated_return_period",
        F.when(F.col("flood_risk_score") >= 0.8, 10)
        .when(F.col("flood_risk_score") >= 0.6, 25)
        .when(F.col("flood_risk_score") >= 0.4, 50)
        .when(F.col("flood_risk_score") >= 0.2, 100)
        .otherwise(500)
    )
    
    return risk_df

# Calculate comprehensive flood risk
print("Calculating composite flood risk scores...")
flood_risk_model_df = calculate_flood_risk_score()
flood_risk_model_df = flood_risk_model_df.persist()

# Show risk distribution
print("Flood Risk Level Distribution:")
flood_risk_model_df.groupBy("flood_risk_level").count().orderBy("flood_risk_level").show()

# Show high-risk areas
print("\nHigh-Risk Flood Areas:")
flood_risk_model_df.filter(F.col("flood_risk_level").isin(["high", "extreme"])) \
    .select("h3_cell", "latitude", "longitude", "flood_risk_score", "flood_risk_level", "estimated_return_period") \
    .orderBy(F.desc("flood_risk_score")) \
    .show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insurance Risk Assessment

# COMMAND ----------

def calculate_flood_insurance_risk():
    """Convert flood risk scores to insurance risk ratings and premiums"""
    
    insurance_df = flood_risk_model_df.withColumn("insurance_flood_class",
        F.when(F.col("flood_risk_score") >= 0.8, "Zone_AE_extreme")
        .when(F.col("flood_risk_score") >= 0.6, "Zone_A_high")
        .when(F.col("flood_risk_score") >= 0.4, "Zone_X_moderate")
        .when(F.col("flood_risk_score") >= 0.2, "Zone_X_low")
        .otherwise("Zone_C_minimal")
    ) \
    .withColumn("base_premium_multiplier",
        F.when(F.col("flood_risk_score") >= 0.8, 5.0)
        .when(F.col("flood_risk_score") >= 0.6, 3.0)
        .when(F.col("flood_risk_score") >= 0.4, 2.0)
        .when(F.col("flood_risk_score") >= 0.2, 1.5)
        .otherwise(1.0)
    ) \
    .withColumn("coverage_recommendation",
        F.when(F.col("flood_risk_score") >= 0.8, "Mandatory flood insurance with high deductible")
        .when(F.col("flood_risk_score") >= 0.6, "Strongly recommended with mitigation requirements")
        .when(F.col("flood_risk_score") >= 0.4, "Recommended coverage")
        .when(F.col("flood_risk_score") >= 0.2, "Optional coverage available")
        .otherwise("Standard coverage at base rates")
    ) \
    .withColumn("required_elevation",
        F.when(F.col("flood_risk_score") >= 0.6, 
            F.col("elevation_m") + 2.0  # Require 2m above current elevation
        ).otherwise(F.col("elevation_m"))
    )
    
    return insurance_df

# Calculate insurance risk assessment
insurance_flood_df = calculate_flood_insurance_risk()

print("Insurance Flood Zone Distribution:")
insurance_flood_df.groupBy("insurance_flood_class", "coverage_recommendation").count().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Risk Aggregation

# COMMAND ----------

def aggregate_flood_risk_by_region():
    """Aggregate flood risk by administrative or geographic regions"""
    
    # Create regional summary
    regional_summary = insurance_flood_df.groupBy("flood_risk_level") \
        .agg(
            F.count("*").alias("location_count"),
            F.avg("flood_risk_score").alias("avg_risk_score"),
            F.max("flood_risk_score").alias("max_risk_score"),
            F.avg("elevation_m").alias("avg_elevation"),
            F.avg("base_premium_multiplier").alias("avg_premium_multiplier"),
            F.sum(F.when(F.col("in_floodplain"), 1).otherwise(0)).alias("floodplain_locations")
        )
    
    # Calculate risk concentration by elevation bands
    elevation_risk = insurance_flood_df.withColumn("elevation_band",
        F.when(F.col("elevation_m") <= 10, "0-10m")
        .when(F.col("elevation_m") <= 25, "10-25m")
        .when(F.col("elevation_m") <= 50, "25-50m")
        .when(F.col("elevation_m") <= 100, "50-100m")
        .otherwise("100m+")
    ).groupBy("elevation_band") \
    .agg(
        F.count("*").alias("location_count"),
        F.avg("flood_risk_score").alias("avg_risk_score"),
        F.countDistinct(F.when(F.col("flood_risk_level").isin(["high", "extreme"]), F.col("h3_cell"))).alias("high_risk_count")
    )
    
    return regional_summary, elevation_risk

regional_flood_summary, elevation_flood_risk = aggregate_flood_risk_by_region()

print("Regional Flood Risk Summary:")
regional_flood_summary.show()

print("\nFlood Risk by Elevation Band:")
elevation_flood_risk.orderBy("elevation_band").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Validation and Performance

# COMMAND ----------

def validate_flood_model():
    """Validate flood model performance and calculate metrics"""
    
    # Calculate model statistics
    model_stats = insurance_flood_df.agg(
        F.min("flood_risk_score").alias("min_risk"),
        F.max("flood_risk_score").alias("max_risk"),
        F.avg("flood_risk_score").alias("avg_risk"),
        F.stddev("flood_risk_score").alias("stddev_risk"),
        F.corr("elevation_m", "flood_risk_score").alias("elevation_correlation"),
        F.corr("slope_degrees", "flood_risk_score").alias("slope_correlation")
    ).collect()[0]
    
    print("Flood Risk Model Statistics:")
    print(f"Risk Score Range: {model_stats['min_risk']:.3f} - {model_stats['max_risk']:.3f}")
    print(f"Average Risk Score: {model_stats['avg_risk']:.3f}")
    print(f"Standard Deviation: {model_stats['stddev_risk']:.3f}")
    print(f"Elevation Correlation: {model_stats['elevation_correlation']:.3f}")
    print(f"Slope Correlation: {model_stats['slope_correlation']:.3f}")
    
    # Risk factor contribution analysis
    factor_importance = insurance_flood_df.select(
        F.corr("elevation_risk", "flood_risk_score").alias("elevation_importance"),
        F.corr("slope_risk", "flood_risk_score").alias("slope_importance"),
        F.corr("flood_potential", "flood_risk_score").alias("precipitation_importance"),
        F.corr("drainage_risk", "flood_risk_score").alias("drainage_importance")
    ).collect()[0]
    
    print("\nRisk Factor Importance (Correlation with Final Score):")
    print(f"Elevation Factor: {factor_importance['elevation_importance']:.3f}")
    print(f"Slope Factor: {factor_importance['slope_importance']:.3f}")
    print(f"Precipitation Factor: {factor_importance['precipitation_importance']:.3f}")
    print(f"Drainage Factor: {factor_importance['drainage_importance']:.3f}")

validate_flood_model()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results and Create Views

# COMMAND ----------

# Save detailed flood risk model results using Delta tables (DBR 16+ optimized)
insurance_flood_df.write \
    .mode("overwrite") \
    .partitionBy("flood_risk_level") \
    .format("delta") \
    .saveAsTable("climate_risk.flood_risk_detailed")

# Save regional summaries
regional_flood_summary.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("climate_risk.flood_risk_regional_summary")

elevation_flood_risk.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("climate_risk.flood_risk_elevation_analysis")

# Create summary view for quick access
flood_summary_df = insurance_flood_df.select(
    "h3_cell",
    "latitude",
    "longitude", 
    "elevation_m",
    "flood_risk_score",
    "flood_risk_level",
    "insurance_flood_class",
    "base_premium_multiplier",
    "estimated_return_period",
    "coverage_recommendation"
)

flood_summary_df.createOrReplaceTempView("flood_risk_summary")

# Create high-risk locations view
high_risk_flood_df = insurance_flood_df.filter(F.col("flood_risk_level").isin(["high", "extreme"]))
high_risk_flood_df.createOrReplaceTempView("high_risk_flood_zones")

print("✅ Flood risk model completed successfully!")
print(f"- Analyzed {insurance_flood_df.count()} spatial locations")
print(f"- Identified {high_risk_flood_df.count()} high-risk flood zones")
print(f"- Generated insurance risk classifications and premium multipliers")
print(f"- Created spatial risk aggregations and validation metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export for Visualization

# COMMAND ----------

# Create GeoJSON-ready export for mapping
mapping_export = flood_summary_df.limit(1000).toPandas()  # Limit for demo

# Save as CSV for external tools
mapping_export.to_csv("/dbfs/mnt/risk-models/flood_risk_mapping_export.csv", index=False)

print("Flood risk mapping data exported for visualization tools")
print(f"Exported {len(mapping_export)} locations for mapping")
