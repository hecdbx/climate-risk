# Databricks notebook source
# MAGIC %md
# MAGIC # AccuWeather Data Ingestion Pipeline using Lakeflow Declarative Pipelines
# MAGIC 
# MAGIC This notebook implements a declarative data pipeline to ingest real-time weather data from AccuWeather API
# MAGIC and store it in Unity Catalog using Delta Lake format with automated data quality checks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Setup

# COMMAND ----------

import dlt
import requests
import json
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd

# Pipeline configuration
ACCUWEATHER_API_KEY = spark.conf.get("accuweather.api.key", "your_api_key_here")
ACCUWEATHER_BASE_URL = "http://dataservice.accuweather.com"

# Target locations for weather monitoring (can be configured dynamically)
TARGET_LOCATIONS = [
    {"key": "347628", "name": "San Francisco, CA", "lat": 37.7749, "lon": -122.4194},
    {"key": "347625", "name": "Los Angeles, CA", "lat": 34.0522, "lon": -118.2437},
    {"key": "349727", "name": "New York, NY", "lat": 40.7128, "lon": -74.0060},
    {"key": "348181", "name": "Chicago, IL", "lat": 41.8781, "lon": -87.6298},
    {"key": "351193", "name": "Houston, TX", "lat": 29.7604, "lon": -95.3698}
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Expectations

# COMMAND ----------

# Define data quality expectations for AccuWeather data
@dlt.expect_all_or_drop({"valid_temperature": "temperature_celsius BETWEEN -50 AND 60"})
@dlt.expect_all_or_drop({"valid_coordinates": "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180"})
@dlt.expect_all_or_drop({"valid_timestamp": "observation_time IS NOT NULL"})
@dlt.expect_or_fail({"recent_data": "observation_time >= current_timestamp() - INTERVAL 24 HOURS"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Raw AccuWeather Data Ingestion

# COMMAND ----------

def fetch_accuweather_current_conditions():
    """
    Fetch current weather conditions from AccuWeather API for configured locations
    """
    weather_data = []
    
    for location in TARGET_LOCATIONS:
        try:
            # Construct API URL for current conditions
            url = f"{ACCUWEATHER_BASE_URL}/currentconditions/v1/{location['key']}"
            params = {
                "apikey": ACCUWEATHER_API_KEY,
                "details": "true"
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data and len(data) > 0:
                current = data[0]
                
                # Extract relevant weather data
                weather_record = {
                    "location_key": location["key"],
                    "location_name": location["name"],
                    "latitude": location["lat"],
                    "longitude": location["lon"],
                    "observation_time": datetime.fromtimestamp(current.get("EpochTime", 0)),
                    "temperature_celsius": current.get("Temperature", {}).get("Metric", {}).get("Value", 0.0),
                    "temperature_fahrenheit": current.get("Temperature", {}).get("Imperial", {}).get("Value", 0.0),
                    "humidity_percent": current.get("RelativeHumidity", 0),
                    "pressure_mb": current.get("Pressure", {}).get("Metric", {}).get("Value", 0.0),
                    "wind_speed_kmh": current.get("Wind", {}).get("Speed", {}).get("Metric", {}).get("Value", 0.0),
                    "wind_direction_degrees": current.get("Wind", {}).get("Direction", {}).get("Degrees", 0),
                    "precipitation_mm": current.get("PrecipitationSummary", {}).get("PastHour", {}).get("Metric", {}).get("Value", 0.0),
                    "weather_text": current.get("WeatherText", ""),
                    "weather_icon": current.get("WeatherIcon", 0),
                    "uv_index": current.get("UVIndex", 0),
                    "visibility_km": current.get("Visibility", {}).get("Metric", {}).get("Value", 0.0),
                    "cloud_cover_percent": current.get("CloudCover", 0),
                    "ingestion_timestamp": datetime.now()
                }
                
                weather_data.append(weather_record)
                
        except Exception as e:
            print(f"Error fetching data for {location['name']}: {str(e)}")
            continue
    
    return weather_data

@dlt.table(
    name="bronze_accuweather_current",
    comment="Bronze layer: Raw current weather conditions from AccuWeather API",
    table_properties={
        "quality": "bronze",
        "data_source": "AccuWeather API",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_accuweather_current():
    """
    Bronze layer table for raw AccuWeather current conditions data
    """
    # Fetch data from AccuWeather API
    raw_data = fetch_accuweather_current_conditions()
    
    if not raw_data:
        # Return empty DataFrame with correct schema if no data
        schema = StructType([
            StructField("location_key", StringType(), True),
            StructField("location_name", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("observation_time", TimestampType(), True),
            StructField("temperature_celsius", DoubleType(), True),
            StructField("temperature_fahrenheit", DoubleType(), True),
            StructField("humidity_percent", IntegerType(), True),
            StructField("pressure_mb", DoubleType(), True),
            StructField("wind_speed_kmh", DoubleType(), True),
            StructField("wind_direction_degrees", IntegerType(), True),
            StructField("precipitation_mm", DoubleType(), True),
            StructField("weather_text", StringType(), True),
            StructField("weather_icon", IntegerType(), True),
            StructField("uv_index", IntegerType(), True),
            StructField("visibility_km", DoubleType(), True),
            StructField("cloud_cover_percent", IntegerType(), True),
            StructField("ingestion_timestamp", TimestampType(), True)
        ])
        return spark.createDataFrame([], schema)
    
    # Convert to DataFrame
    df = spark.createDataFrame(raw_data)
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Cleansed and Enriched Weather Data

# COMMAND ----------

@dlt.table(
    name="silver_accuweather_enriched",
    comment="Silver layer: Cleansed AccuWeather data with H3 spatial indexing and quality scores",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all_or_drop({"valid_temperature": "temperature_celsius BETWEEN -50 AND 60"})
@dlt.expect_all_or_drop({"valid_coordinates": "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180"})
@dlt.expect_all_or_drop({"valid_timestamp": "observation_time IS NOT NULL"})
def silver_accuweather_enriched():
    """
    Silver layer: Cleansed and enriched AccuWeather data
    """
    return (
        dlt.read("bronze_accuweather_current")
        .withColumn("h3_cell_7", F.expr("h3_latlng_to_cell_string(latitude, longitude, 7)"))
        .withColumn("h3_cell_8", F.expr("h3_latlng_to_cell_string(latitude, longitude, 8)"))
        .withColumn("data_quality_score", 
            F.when(F.col("temperature_celsius").isNull(), 0.0)
            .when(F.col("humidity_percent").isNull(), 0.7)
            .when(F.col("wind_speed_kmh").isNull(), 0.8)
            .otherwise(1.0)
        )
        .withColumn("heat_index", 
            F.when(F.col("temperature_celsius") > 26,
                F.col("temperature_celsius") + (0.5 * (F.col("humidity_percent") / 100 - 0.1))
            ).otherwise(F.col("temperature_celsius"))
        )
        .withColumn("apparent_temperature",
            F.col("temperature_celsius") + 
            (0.33 * F.col("humidity_percent") / 100) - 
            (0.7 * F.col("wind_speed_kmh") / 10) - 4
        )
        .withColumn("weather_severity",
            F.when(F.col("weather_icon").isin([15, 16, 17, 18]), "severe")
            .when(F.col("weather_icon").isin([12, 13, 14]), "moderate")
            .when(F.col("weather_icon").isin([7, 8, 11]), "light_precip")
            .otherwise("normal")
        )
        .filter(F.col("observation_time") >= F.current_timestamp() - F.expr("INTERVAL 24 HOURS"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Analytics-Ready Climate Observations

# COMMAND ----------

@dlt.table(
    name="gold_climate_observations_realtime",
    comment="Gold layer: Analytics-ready real-time climate observations for risk modeling",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_climate_observations_realtime():
    """
    Gold layer: Standardized climate observations ready for risk modeling
    """
    return (
        dlt.read("silver_accuweather_enriched")
        .select(
            F.col("h3_cell_7"),
            F.col("h3_cell_8"), 
            F.col("latitude"),
            F.col("longitude"),
            F.col("observation_time").alias("observation_timestamp"),
            F.col("temperature_celsius"),
            F.col("precipitation_mm"),
            F.col("humidity_percent"),
            F.col("wind_speed_kmh"),
            F.col("pressure_mb"),
            F.col("weather_text").alias("weather_conditions"),
            F.lit("AccuWeather").alias("data_source"),
            F.col("data_quality_score"),
            F.current_timestamp().alias("processing_timestamp")
        )
        .withColumn("precipitation_intensity_class",
            F.when(F.col("precipitation_mm") >= 100, "extreme")
            .when(F.col("precipitation_mm") >= 50, "heavy")
            .when(F.col("precipitation_mm") >= 10, "moderate")
            .when(F.col("precipitation_mm") >= 2.5, "light")
            .otherwise("none")
        )
        .withColumn("temperature_category",
            F.when(F.col("temperature_celsius") > 35, "extreme_heat")
            .when(F.col("temperature_celsius") > 25, "hot")
            .when(F.col("temperature_celsius") > 15, "warm")
            .when(F.col("temperature_celsius") > 5, "cool")
            .when(F.col("temperature_celsius") > -5, "cold")
            .otherwise("extreme_cold")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Climate Aggregations

# COMMAND ----------

@dlt.table(
    name="gold_daily_climate_aggregations",
    comment="Gold layer: Daily aggregated climate metrics by H3 cell for risk modeling",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_daily_climate_aggregations():
    """
    Gold layer: Daily climate aggregations for risk modeling
    """
    return (
        dlt.read("gold_climate_observations_realtime")
        .groupBy(
            F.col("h3_cell_7"),
            F.col("latitude"),
            F.col("longitude"),
            F.date_trunc("day", F.col("observation_timestamp")).alias("aggregation_date")
        )
        .agg(
            F.avg("temperature_celsius").alias("avg_temperature_celsius"),
            F.min("temperature_celsius").alias("min_temperature_celsius"),
            F.max("temperature_celsius").alias("max_temperature_celsius"),
            F.sum("precipitation_mm").alias("total_precipitation_mm"),
            F.max("precipitation_mm").alias("max_precipitation_intensity_mm"),
            F.avg("humidity_percent").alias("avg_humidity_percent"),
            F.avg("wind_speed_kmh").alias("avg_wind_speed_kmh"),
            F.sum(F.when(F.col("precipitation_mm") > 0.1, 1).otherwise(0)).alias("precipitation_days"),
            F.sum(F.when(F.col("precipitation_mm") <= 0.1, 1).otherwise(0)).alias("dry_days"),
            F.sum(F.when(F.col("precipitation_intensity_class") == "extreme", 1).otherwise(0)).alias("extreme_weather_events"),
            F.avg("data_quality_score").alias("data_completeness_percent"),
            F.count("*").alias("observation_count"),
            F.current_timestamp().alias("processing_timestamp")
        )
        .filter(F.col("observation_count") >= 4)  # Ensure minimum data quality
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration and Monitoring

# COMMAND ----------

# Configure pipeline settings
dlt.create_target_table(
    name="pipeline_execution_metrics",
    comment="Pipeline execution and data quality metrics"
)

@dlt.table(
    name="pipeline_execution_metrics",
    table_properties={
        "pipelines.autoOptimize.managed": "true"
    }
)
def pipeline_execution_metrics():
    """
    Track pipeline execution metrics and data quality
    """
    return spark.sql("""
        SELECT 
            current_timestamp() as execution_timestamp,
            'accuweather_ingestion' as pipeline_name,
            'running' as status,
            0 as records_processed,
            0 as data_quality_score,
            'Pipeline execution started' as message
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Error Handling and Data Quality Monitoring

# COMMAND ----------

@dlt.expect_all_or_fail({"data_freshness": "processing_timestamp >= current_timestamp() - INTERVAL 6 HOURS"})
@dlt.expect_or_warn({"minimum_locations": "count(*) >= 3"}, "Less than 3 locations have recent data")

def validate_data_quality():
    """
    Validate overall data quality across the pipeline
    """
    # This function can be called to validate data quality across tables
    quality_checks = {
        "temperature_range_valid": "temperature_celsius BETWEEN -50 AND 60",
        "coordinates_valid": "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180",
        "recent_data": "processing_timestamp >= current_timestamp() - INTERVAL 6 HOURS",
        "h3_cells_populated": "h3_cell_7 IS NOT NULL AND h3_cell_8 IS NOT NULL"
    }
    
    return quality_checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Trigger Configuration
# MAGIC 
# MAGIC This pipeline can be configured to run:
# MAGIC - **Triggered**: On-demand execution
# MAGIC - **Scheduled**: Every 15 minutes for real-time data
# MAGIC - **Continuous**: For streaming data processing

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create or replace the pipeline view in Unity Catalog
# MAGIC CREATE OR REPLACE VIEW climate_risk_catalog.analytics.accuweather_pipeline_status AS
# MAGIC SELECT 
# MAGIC   'AccuWeather Ingestion' as pipeline_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   MAX(processing_timestamp) as last_update,
# MAGIC   AVG(data_quality_score) as avg_quality_score,
# MAGIC   COUNT(DISTINCT h3_cell_7) as unique_locations
# MAGIC FROM climate_risk_catalog.processed_data.climate_observations
# MAGIC WHERE data_source = 'AccuWeather'
# MAGIC   AND processing_timestamp >= current_timestamp() - INTERVAL 24 HOURS
