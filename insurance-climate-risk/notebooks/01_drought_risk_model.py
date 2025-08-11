# Databricks notebook source
# MAGIC %md
# MAGIC # Drought Risk Assessment Model
# MAGIC 
# MAGIC This notebook develops a comprehensive drought risk model using Databricks' geospatial functions.
# MAGIC The model incorporates multiple climate variables and uses H3 hexagonal indexing for spatial analysis.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import h3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

# Geospatial functions
from pyspark.sql.functions import col, when, lit, expr
import pyspark.sql.functions as sf

# Initialize Spark session with geospatial extensions
spark = SparkSession.builder \
    .appName("DroughtRiskModel") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .getOrCreate()

# Enable H3 functions
spark.sql("CREATE OR REPLACE FUNCTION h3_latlng_to_cell AS 'com.uber.h3.spark.sql.H3_LatLngToCell'")
spark.sql("CREATE OR REPLACE FUNCTION h3_cell_to_boundary AS 'com.uber.h3.spark.sql.H3_CellToBoundary'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Parameters

# COMMAND ----------

# Model configuration
class DroughtConfig:
    # H3 resolution for spatial analysis (resolution 7 = ~5km hexagons)
    H3_RESOLUTION = 7
    
    # Drought severity thresholds
    DROUGHT_THRESHOLDS = {
        'mild': 0.3,
        'moderate': 0.2,
        'severe': 0.1,
        'extreme': 0.05
    }
    
    # Time windows for analysis
    SHORT_TERM_DAYS = 90
    LONG_TERM_DAYS = 365
    
    # Risk scoring weights
    WEIGHTS = {
        'precipitation_deficit': 0.35,
        'temperature_anomaly': 0.25,
        'soil_moisture': 0.20,
        'vegetation_index': 0.15,
        'historical_frequency': 0.05
    }

config = DroughtConfig()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading and Preprocessing

# COMMAND ----------

def load_climate_data():
    """Load and preprocess climate data from various sources"""
    
    # Load precipitation data (example with synthetic data structure)
    precipitation_df = spark.sql("""
        SELECT 
            latitude,
            longitude,
            date,
            precipitation_mm,
            h3_latlng_to_cell(latitude, longitude, {resolution}) as h3_cell
        FROM climate.precipitation_daily
        WHERE date >= current_date() - interval 2 years
    """.format(resolution=config.H3_RESOLUTION))
    
    # Load temperature data
    temperature_df = spark.sql("""
        SELECT 
            latitude,
            longitude,
            date,
            temperature_celsius,
            h3_latlng_to_cell(latitude, longitude, {resolution}) as h3_cell
        FROM climate.temperature_daily
        WHERE date >= current_date() - interval 2 years
    """.format(resolution=config.H3_RESOLUTION))
    
    # Load soil moisture data
    soil_moisture_df = spark.sql("""
        SELECT 
            latitude,
            longitude,
            date,
            soil_moisture_percent,
            h3_latlng_to_cell(latitude, longitude, {resolution}) as h3_cell
        FROM climate.soil_moisture_daily
        WHERE date >= current_date() - interval 2 years
    """.format(resolution=config.H3_RESOLUTION))
    
    return precipitation_df, temperature_df, soil_moisture_df

# For demo purposes, create synthetic data
def create_sample_data():
    """Create sample climate data for demonstration"""
    
    # Generate sample coordinates for a region (e.g., California)
    lat_range = np.arange(32.0, 42.0, 0.1)
    lon_range = np.arange(-124.0, -114.0, 0.1)
    
    sample_data = []
    for lat in lat_range[::5]:  # Sample every 5th point
        for lon in lon_range[::5]:
            h3_cell = h3.latlng_to_cell(lat, lon, config.H3_RESOLUTION)
            
            # Generate 365 days of sample data
            for i in range(365):
                date = datetime.now() - timedelta(days=i)
                
                # Synthetic climate data with seasonal patterns
                base_precip = 2.0 + np.sin(i * 2 * np.pi / 365) * 1.5
                precipitation = max(0, np.random.normal(base_precip, 1.0))
                
                base_temp = 20 + np.sin(i * 2 * np.pi / 365) * 10
                temperature = np.random.normal(base_temp, 3.0)
                
                soil_moisture = max(0, min(100, np.random.normal(40, 15)))
                
                sample_data.append({
                    'latitude': lat,
                    'longitude': lon,
                    'date': date.strftime('%Y-%m-%d'),
                    'h3_cell': h3_cell,
                    'precipitation_mm': precipitation,
                    'temperature_celsius': temperature,
                    'soil_moisture_percent': soil_moisture
                })
    
    return spark.createDataFrame(sample_data)

# Load or create sample data
climate_df = create_sample_data()
climate_df.cache()

print(f"Loaded {climate_df.count()} climate records")
climate_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drought Indicators Calculation

# COMMAND ----------

def calculate_precipitation_percentiles(df):
    """Calculate precipitation percentiles for drought assessment"""
    
    # Calculate historical percentiles by H3 cell and day of year
    percentiles_df = df.withColumn("day_of_year", F.dayofyear(F.col("date"))) \
        .groupBy("h3_cell", "day_of_year") \
        .agg(
            F.expr("percentile_approx(precipitation_mm, 0.05)").alias("p5_precip"),
            F.expr("percentile_approx(precipitation_mm, 0.10)").alias("p10_precip"),
            F.expr("percentile_approx(precipitation_mm, 0.20)").alias("p20_precip"),
            F.expr("percentile_approx(precipitation_mm, 0.30)").alias("p30_precip"),
            F.avg("precipitation_mm").alias("avg_precip")
        )
    
    # Join back with original data to calculate drought severity
    drought_df = df.withColumn("day_of_year", F.dayofyear(F.col("date"))) \
        .join(percentiles_df, ["h3_cell", "day_of_year"]) \
        .withColumn("drought_severity",
            F.when(F.col("precipitation_mm") <= F.col("p5_precip"), "extreme")
            .when(F.col("precipitation_mm") <= F.col("p10_precip"), "severe")
            .when(F.col("precipitation_mm") <= F.col("p20_precip"), "moderate")
            .when(F.col("precipitation_mm") <= F.col("p30_precip"), "mild")
            .otherwise("normal")
        ) \
        .withColumn("precip_deficit_ratio",
            (F.col("avg_precip") - F.col("precipitation_mm")) / F.col("avg_precip")
        )
    
    return drought_df

def calculate_temperature_anomalies(df):
    """Calculate temperature anomalies"""
    
    # Calculate historical temperature averages
    temp_normals = df.withColumn("day_of_year", F.dayofyear(F.col("date"))) \
        .groupBy("h3_cell", "day_of_year") \
        .agg(
            F.avg("temperature_celsius").alias("avg_temp"),
            F.stddev("temperature_celsius").alias("stddev_temp")
        )
    
    # Calculate temperature anomalies (z-scores)
    temp_anomaly_df = df.withColumn("day_of_year", F.dayofyear(F.col("date"))) \
        .join(temp_normals, ["h3_cell", "day_of_year"]) \
        .withColumn("temp_anomaly",
            (F.col("temperature_celsius") - F.col("avg_temp")) / F.col("stddev_temp")
        )
    
    return temp_anomaly_df

def calculate_soil_moisture_index(df):
    """Calculate soil moisture drought index"""
    
    # Calculate soil moisture percentiles
    sm_percentiles = df.groupBy("h3_cell") \
        .agg(
            F.expr("percentile_approx(soil_moisture_percent, 0.10)").alias("sm_p10"),
            F.expr("percentile_approx(soil_moisture_percent, 0.30)").alias("sm_p30"),
            F.avg("soil_moisture_percent").alias("avg_soil_moisture")
        )
    
    sm_index_df = df.join(sm_percentiles, ["h3_cell"]) \
        .withColumn("soil_moisture_drought",
            F.when(F.col("soil_moisture_percent") <= F.col("sm_p10"), "severe")
            .when(F.col("soil_moisture_percent") <= F.col("sm_p30"), "moderate")
            .otherwise("normal")
        ) \
        .withColumn("sm_deficit_ratio",
            (F.col("avg_soil_moisture") - F.col("soil_moisture_percent")) / F.col("avg_soil_moisture")
        )
    
    return sm_index_df

# Calculate drought indicators
print("Calculating precipitation percentiles...")
drought_indicators = calculate_precipitation_percentiles(climate_df)

print("Calculating temperature anomalies...")
drought_indicators = calculate_temperature_anomalies(drought_indicators)

print("Calculating soil moisture index...")
drought_indicators = calculate_soil_moisture_index(drought_indicators)

drought_indicators.cache()
print(f"Calculated drought indicators for {drought_indicators.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Composite Drought Risk Score

# COMMAND ----------

def calculate_drought_risk_score(df):
    """Calculate composite drought risk score using multiple indicators"""
    
    # Normalize indicators to 0-1 scale (higher = more drought risk)
    risk_df = df.withColumn("precip_risk", 
                F.greatest(F.lit(0), F.least(F.lit(1), F.col("precip_deficit_ratio")))
            ) \
            .withColumn("temp_risk",
                F.greatest(F.lit(0), F.least(F.lit(1), F.col("temp_anomaly") / 3.0))
            ) \
            .withColumn("soil_risk",
                F.greatest(F.lit(0), F.least(F.lit(1), F.col("sm_deficit_ratio")))
            )
    
    # Calculate weighted composite score
    risk_df = risk_df.withColumn("drought_risk_score",
        (F.col("precip_risk") * config.WEIGHTS['precipitation_deficit'] +
         F.col("temp_risk") * config.WEIGHTS['temperature_anomaly'] +
         F.col("soil_risk") * config.WEIGHTS['soil_moisture'])
    )
    
    # Classify risk levels
    risk_df = risk_df.withColumn("risk_level",
        F.when(F.col("drought_risk_score") >= 0.8, "very_high")
        .when(F.col("drought_risk_score") >= 0.6, "high")
        .when(F.col("drought_risk_score") >= 0.4, "moderate")
        .when(F.col("drought_risk_score") >= 0.2, "low")
        .otherwise("very_low")
    )
    
    return risk_df

# Calculate drought risk scores
drought_risk_df = calculate_drought_risk_score(drought_indicators)

# Show risk distribution
print("Drought Risk Level Distribution:")
drought_risk_df.groupBy("risk_level").count().orderBy("risk_level").show()

# Show sample of high-risk areas
print("\nSample of High-Risk Areas:")
drought_risk_df.filter(F.col("risk_level").isin(["high", "very_high"])) \
    .select("h3_cell", "latitude", "longitude", "date", "drought_risk_score", "risk_level") \
    .orderBy(F.desc("drought_risk_score")) \
    .show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spatial Aggregation and Analysis

# COMMAND ----------

def aggregate_risk_by_region(df):
    """Aggregate drought risk by H3 cells for spatial analysis"""
    
    # Calculate recent risk (last 30 days)
    recent_date = F.date_sub(F.current_date(), 30)
    
    regional_risk = df.filter(F.col("date") >= recent_date) \
        .groupBy("h3_cell") \
        .agg(
            F.avg("drought_risk_score").alias("avg_risk_score"),
            F.max("drought_risk_score").alias("max_risk_score"),
            F.count("*").alias("observation_count"),
            F.first("latitude").alias("latitude"),
            F.first("longitude").alias("longitude"),
            F.avg("precipitation_mm").alias("avg_precipitation"),
            F.avg("temperature_celsius").alias("avg_temperature"),
            F.avg("soil_moisture_percent").alias("avg_soil_moisture")
        ) \
        .withColumn("risk_category",
            F.when(F.col("avg_risk_score") >= 0.8, "very_high")
            .when(F.col("avg_risk_score") >= 0.6, "high")
            .when(F.col("avg_risk_score") >= 0.4, "moderate")
            .when(F.col("avg_risk_score") >= 0.2, "low")
            .otherwise("very_low")
        )
    
    return regional_risk

# Calculate regional risk aggregations
regional_drought_risk = aggregate_risk_by_region(drought_risk_df)
regional_drought_risk.cache()

print("Regional Drought Risk Summary:")
regional_drought_risk.groupBy("risk_category").agg(
    F.count("*").alias("hexagon_count"),
    F.avg("avg_risk_score").alias("mean_risk_score")
).orderBy("risk_category").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insurance Risk Rating

# COMMAND ----------

def calculate_insurance_risk_rating(df):
    """Convert drought risk scores to insurance risk ratings"""
    
    # Define insurance risk classes and multipliers
    insurance_df = df.withColumn("insurance_risk_class",
        F.when(F.col("avg_risk_score") >= 0.8, "A1_extreme")
        .when(F.col("avg_risk_score") >= 0.6, "A2_high")
        .when(F.col("avg_risk_score") >= 0.4, "B1_moderate_high")
        .when(F.col("avg_risk_score") >= 0.3, "B2_moderate")
        .when(F.col("avg_risk_score") >= 0.2, "C1_low_moderate")
        .otherwise("C2_low")
    ) \
    .withColumn("risk_multiplier",
        F.when(F.col("avg_risk_score") >= 0.8, 2.5)
        .when(F.col("avg_risk_score") >= 0.6, 2.0)
        .when(F.col("avg_risk_score") >= 0.4, 1.5)
        .when(F.col("avg_risk_score") >= 0.3, 1.2)
        .when(F.col("avg_risk_score") >= 0.2, 1.0)
        .otherwise(0.8)
    ) \
    .withColumn("recommended_action",
        F.when(F.col("avg_risk_score") >= 0.8, "Decline coverage or require mitigation")
        .when(F.col("avg_risk_score") >= 0.6, "High premium with conditions")
        .when(F.col("avg_risk_score") >= 0.4, "Standard premium with monitoring")
        .when(F.col("avg_risk_score") >= 0.2, "Standard premium")
        .otherwise("Preferred rates available")
    )
    
    return insurance_df

# Calculate insurance risk ratings
insurance_risk_df = calculate_insurance_risk_rating(regional_drought_risk)

print("Insurance Risk Class Distribution:")
insurance_risk_df.groupBy("insurance_risk_class", "recommended_action").count().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

# Save detailed drought risk data
drought_risk_df.write \
    .mode("overwrite") \
    .partitionBy("date") \
    .parquet("/mnt/risk-models/drought_risk_detailed")

# Save regional aggregated risk
insurance_risk_df.write \
    .mode("overwrite") \
    .parquet("/mnt/risk-models/drought_risk_regional")

# Create summary table for quick access
summary_df = insurance_risk_df.select(
    "h3_cell",
    "latitude", 
    "longitude",
    "avg_risk_score",
    "insurance_risk_class",
    "risk_multiplier",
    "recommended_action"
)

summary_df.createOrReplaceTempView("drought_risk_summary")

print("✅ Drought risk model completed successfully!")
print(f"- Processed {drought_risk_df.count()} climate observations")
print(f"- Generated risk scores for {regional_drought_risk.count()} spatial regions")
print(f"- Created insurance risk ratings for underwriting decisions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Validation and Performance Metrics

# COMMAND ----------

def validate_model_performance():
    """Calculate model performance metrics and validation statistics"""
    
    # Calculate correlation between different risk indicators
    correlations = drought_risk_df.select(
        F.corr("precip_risk", "drought_risk_score").alias("precip_correlation"),
        F.corr("temp_risk", "drought_risk_score").alias("temp_correlation"),
        F.corr("soil_risk", "drought_risk_score").alias("soil_correlation")
    ).collect()[0]
    
    print("Risk Indicator Correlations with Composite Score:")
    print(f"Precipitation Risk: {correlations['precip_correlation']:.3f}")
    print(f"Temperature Risk: {correlations['temp_correlation']:.3f}")
    print(f"Soil Moisture Risk: {correlations['soil_correlation']:.3f}")
    
    # Risk score statistics
    stats = drought_risk_df.agg(
        F.min("drought_risk_score").alias("min_score"),
        F.max("drought_risk_score").alias("max_score"),
        F.avg("drought_risk_score").alias("avg_score"),
        F.stddev("drought_risk_score").alias("stddev_score")
    ).collect()[0]
    
    print(f"\nDrought Risk Score Statistics:")
    print(f"Min: {stats['min_score']:.3f}")
    print(f"Max: {stats['max_score']:.3f}")
    print(f"Mean: {stats['avg_score']:.3f}")
    print(f"Std Dev: {stats['stddev_score']:.3f}")

validate_model_performance()
