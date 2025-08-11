# Databricks notebook source
# MAGIC %md
# MAGIC # Historical Climate Data Processing Pipeline using Lakeflow Declarative Pipelines
# MAGIC 
# MAGIC This notebook processes historical climate data from multiple sources (NOAA, ERA5, etc.)
# MAGIC and creates standardized datasets for drought and flood risk modeling.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Raw Historical Data Ingestion

# COMMAND ----------

@dlt.table(
    name="bronze_noaa_prism_data",
    comment="Bronze layer: Raw NOAA PRISM climate data",
    table_properties={
        "quality": "bronze",
        "data_source": "NOAA_PRISM",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_noaa_prism_data():
    """
    Bronze layer for NOAA PRISM historical climate data
    For demo purposes, this creates synthetic historical data
    In production, this would read from actual NOAA data sources
    """
    
    # Define schema for NOAA PRISM data
    schema = StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("observation_date", DateType(), True),
        StructField("temperature_celsius", DoubleType(), True),
        StructField("precipitation_mm", DoubleType(), True),
        StructField("humidity_percent", IntegerType(), True),
        StructField("wind_speed_kmh", DoubleType(), True),
        StructField("pressure_mb", DoubleType(), True),
        StructField("soil_moisture_percent", DoubleType(), True),
        StructField("snow_depth_cm", DoubleType(), True),
        StructField("data_source", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    
    # In production, replace this with actual data loading logic
    # For demo, create sample historical data
    sample_data = []
    
    # Sample coordinates for California region
    lat_range = [37.7749, 34.0522, 32.7157]  # SF, LA, San Diego
    lon_range = [-122.4194, -118.2437, -117.1611]
    
    for i, (lat, lon) in enumerate(zip(lat_range, lon_range)):
        # Generate 2 years of historical data
        for days_back in range(0, 730):
            date = datetime.now().date() - timedelta(days=days_back)
            
            # Simulate seasonal patterns
            day_of_year = date.timetuple().tm_yday
            seasonal_temp = 15 + 10 * np.sin(2 * np.pi * day_of_year / 365)
            seasonal_precip = 2.0 + 1.5 * np.sin(2 * np.pi * (day_of_year + 90) / 365)
            
            # Add random variation
            temp = float(seasonal_temp + np.random.normal(0, 3))
            precip = max(0.0, float(seasonal_precip + np.random.gamma(1, 2)))
            
            sample_data.append((
                float(lat),
                float(lon),
                date,
                temp,
                precip,
                int(np.random.uniform(30, 90)),
                float(np.random.uniform(5, 25)),
                float(np.random.uniform(1010, 1030)),
                float(np.random.uniform(20, 60)),
                max(0.0, float(np.random.normal(0, 5))) if date.month in [12, 1, 2] else 0.0,
                "NOAA_PRISM",
                datetime.now()
            ))
    
    return spark.createDataFrame(sample_data, schema)

@dlt.table(
    name="bronze_era5_reanalysis",
    comment="Bronze layer: Raw ERA5 reanalysis climate data",
    table_properties={
        "quality": "bronze",
        "data_source": "ERA5",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_era5_reanalysis():
    """
    Bronze layer for ERA5 reanalysis data
    """
    
    schema = StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("observation_date", DateType(), True),
        StructField("temperature_celsius", DoubleType(), True),
        StructField("precipitation_mm", DoubleType(), True),
        StructField("humidity_percent", IntegerType(), True),
        StructField("wind_speed_kmh", DoubleType(), True),
        StructField("pressure_mb", DoubleType(), True),
        StructField("soil_moisture_percent", DoubleType(), True),
        StructField("evaporation_mm", DoubleType(), True),
        StructField("data_source", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])
    
    # Sample ERA5 data (in production, load from actual ERA5 sources)
    sample_data = []
    
    # Broader geographic coverage for ERA5
    lat_range = np.arange(32.0, 42.0, 2.0)
    lon_range = np.arange(-124.0, -114.0, 2.0)
    
    for lat in lat_range:
        for lon in lon_range:
            for days_back in range(0, 365):  # 1 year of data
                date = datetime.now().date() - timedelta(days=days_back)
                
                # ERA5 has different characteristics than PRISM
                day_of_year = date.timetuple().tm_yday
                seasonal_temp = 16 + 8 * np.sin(2 * np.pi * day_of_year / 365)
                seasonal_precip = 2.5 + 2.0 * np.sin(2 * np.pi * (day_of_year + 90) / 365)
                
                temp = float(seasonal_temp + np.random.normal(0, 2.5))
                precip = max(0.0, float(seasonal_precip + np.random.gamma(1, 1.5)))
                
                sample_data.append((
                    float(lat),
                    float(lon),
                    date,
                    temp,
                    precip,
                    int(np.random.uniform(25, 85)),
                    float(np.random.uniform(3, 30)),
                    float(np.random.uniform(1008, 1032)),
                    float(np.random.uniform(15, 55)),
                    float(np.random.uniform(0, 5)),
                    "ERA5",
                    datetime.now()
                ))
    
    return spark.createDataFrame(sample_data, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Unified Historical Climate Data

# COMMAND ----------

@dlt.table(
    name="silver_unified_historical_climate",
    comment="Silver layer: Unified and standardized historical climate data from all sources",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all_or_drop({"valid_temperature": "temperature_celsius BETWEEN -50 AND 60"})
@dlt.expect_all_or_drop({"valid_coordinates": "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180"})
@dlt.expect_all_or_drop({"valid_date": "observation_date IS NOT NULL AND observation_date >= '2020-01-01'"})
def silver_unified_historical_climate():
    """
    Unified historical climate data from multiple sources with spatial indexing
    """
    
    # Union all historical data sources
    noaa_data = (
        dlt.read("bronze_noaa_prism_data")
        .select(
            F.col("latitude"),
            F.col("longitude"),
            F.col("observation_date"),
            F.col("temperature_celsius"),
            F.col("precipitation_mm"),
            F.col("humidity_percent"),
            F.col("wind_speed_kmh"),
            F.col("pressure_mb"),
            F.col("soil_moisture_percent"),
            F.lit(None).cast(DoubleType()).alias("evaporation_mm"),
            F.col("data_source"),
            F.col("ingestion_timestamp")
        )
    )
    
    era5_data = (
        dlt.read("bronze_era5_reanalysis")
        .select(
            F.col("latitude"),
            F.col("longitude"),
            F.col("observation_date"),
            F.col("temperature_celsius"),
            F.col("precipitation_mm"),
            F.col("humidity_percent"),
            F.col("wind_speed_kmh"),
            F.col("pressure_mb"),
            F.col("soil_moisture_percent"),
            F.col("evaporation_mm"),
            F.col("data_source"),
            F.col("ingestion_timestamp")
        )
    )
    
    # Union and enrich with spatial indexing
    return (
        noaa_data.union(era5_data)
        .withColumn("h3_cell_7", F.expr("h3_latlng_to_cell_string(latitude, longitude, 7)"))
        .withColumn("h3_cell_8", F.expr("h3_latlng_to_cell_string(latitude, longitude, 8)"))
        .withColumn("data_quality_score",
            F.when(F.col("temperature_celsius").isNull(), 0.0)
            .when(F.col("precipitation_mm").isNull(), 0.3)
            .when(F.col("humidity_percent").isNull(), 0.7)
            .otherwise(1.0)
        )
        .withColumn("processing_timestamp", F.current_timestamp())
        .filter(F.col("observation_date") >= F.date_sub(F.current_date(), 1095))  # Last 3 years
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Climate Analytics and Risk Indicators

# COMMAND ----------

@dlt.table(
    name="gold_climate_normals",
    comment="Gold layer: Climate normals and statistics by location for risk modeling",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_climate_normals():
    """
    Calculate climate normals (30-year averages) and statistics by location
    """
    return (
        dlt.read("silver_unified_historical_climate")
        .withColumn("month", F.month("observation_date"))
        .withColumn("day_of_year", F.dayofyear("observation_date"))
        .groupBy("h3_cell_7", "latitude", "longitude", "month")
        .agg(
            F.avg("temperature_celsius").alias("avg_temperature_celsius"),
            F.stddev("temperature_celsius").alias("std_temperature_celsius"),
            F.percentile_approx("temperature_celsius", 0.1).alias("p10_temperature"),
            F.percentile_approx("temperature_celsius", 0.9).alias("p90_temperature"),
            F.avg("precipitation_mm").alias("avg_precipitation_mm"),
            F.stddev("precipitation_mm").alias("std_precipitation_mm"),
            F.percentile_approx("precipitation_mm", 0.1).alias("p10_precipitation"),
            F.percentile_approx("precipitation_mm", 0.9).alias("p90_precipitation"),
            F.avg("humidity_percent").alias("avg_humidity_percent"),
            F.avg("wind_speed_kmh").alias("avg_wind_speed_kmh"),
            F.avg("pressure_mb").alias("avg_pressure_mb"),
            F.avg("soil_moisture_percent").alias("avg_soil_moisture_percent"),
            F.count("*").alias("observation_count"),
            F.current_timestamp().alias("processing_timestamp")
        )
        .filter(F.col("observation_count") >= 30)  # Ensure statistical significance
    )

@dlt.table(
    name="gold_drought_indicators",
    comment="Gold layer: Drought indicators and indices for risk assessment",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_drought_indicators():
    """
    Calculate drought indicators including SPI, temperature anomalies, and soil moisture indices
    """
    
    # Calculate rolling averages and percentiles for drought assessment
    window_30 = F.window("observation_date", "30 days")
    window_90 = F.window("observation_date", "90 days")
    
    return (
        dlt.read("silver_unified_historical_climate")
        .withColumn("year", F.year("observation_date"))
        .withColumn("month", F.month("observation_date"))
        .withColumn("day_of_year", F.dayofyear("observation_date"))
        .groupBy("h3_cell_7", "latitude", "longitude", "observation_date")
        .agg(
            F.avg("temperature_celsius").alias("daily_avg_temperature"),
            F.sum("precipitation_mm").alias("daily_total_precipitation"),
            F.avg("soil_moisture_percent").alias("daily_avg_soil_moisture"),
            F.avg("humidity_percent").alias("daily_avg_humidity")
        )
        .withColumn("temp_30day_avg", 
            F.avg("daily_avg_temperature").over(
                F.window("observation_date", "30 days").partitionBy("h3_cell_7")
            )
        )
        .withColumn("precip_30day_total", 
            F.sum("daily_total_precipitation").over(
                F.window("observation_date", "30 days").partitionBy("h3_cell_7")
            )
        )
        .withColumn("precip_90day_total", 
            F.sum("daily_total_precipitation").over(
                F.window("observation_date", "90 days").partitionBy("h3_cell_7")
            )
        )
        .withColumn("soil_moisture_30day_avg", 
            F.avg("daily_avg_soil_moisture").over(
                F.window("observation_date", "30 days").partitionBy("h3_cell_7")
            )
        )
        .withColumn("processing_timestamp", F.current_timestamp())
    )

@dlt.table(
    name="gold_flood_risk_indicators",
    comment="Gold layer: Flood risk indicators from climate and precipitation data",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_flood_risk_indicators():
    """
    Calculate flood risk indicators including precipitation intensity and accumulation patterns
    """
    return (
        dlt.read("silver_unified_historical_climate")
        .withColumn("precipitation_intensity_class",
            F.when(F.col("precipitation_mm") >= 100, "extreme")
            .when(F.col("precipitation_mm") >= 50, "heavy")
            .when(F.col("precipitation_mm") >= 10, "moderate")
            .when(F.col("precipitation_mm") >= 2.5, "light")
            .otherwise("none")
        )
        .withColumn("extreme_precip_event", 
            F.when(F.col("precipitation_mm") >= 50, 1).otherwise(0)
        )
        .groupBy("h3_cell_7", "latitude", "longitude", "observation_date")
        .agg(
            F.max("precipitation_mm").alias("max_precipitation_mm"),
            F.sum("precipitation_mm").alias("total_precipitation_mm"),
            F.sum("extreme_precip_event").alias("extreme_events_count"),
            F.avg("humidity_percent").alias("avg_humidity"),
            F.max("wind_speed_kmh").alias("max_wind_speed")
        )
        .withColumn("precip_7day_total", 
            F.sum("total_precipitation_mm").over(
                F.window("observation_date", "7 days").partitionBy("h3_cell_7")
            )
        )
        .withColumn("precip_intensity_7day_max", 
            F.max("max_precipitation_mm").over(
                F.window("observation_date", "7 days").partitionBy("h3_cell_7")
            )
        )
        .withColumn("flood_potential_score",
            F.when(F.col("precip_7day_total") > 150, 1.0)
            .when(F.col("precip_7day_total") > 100, 0.8)
            .when(F.col("precip_7day_total") > 50, 0.6)
            .when(F.col("precip_7day_total") > 25, 0.4)
            .otherwise(0.2)
        )
        .withColumn("processing_timestamp", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality and Monitoring

# COMMAND ----------

@dlt.table(
    name="gold_data_quality_metrics",
    comment="Data quality metrics and pipeline monitoring",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_data_quality_metrics():
    """
    Calculate data quality metrics across the historical data pipeline
    """
    
    # Get quality metrics from silver layer
    climate_quality = (
        dlt.read("silver_unified_historical_climate")
        .groupBy("data_source", F.date_trunc("day", "observation_date").alias("date"))
        .agg(
            F.count("*").alias("total_records"),
            F.avg("data_quality_score").alias("avg_quality_score"),
            F.sum(F.when(F.col("temperature_celsius").isNull(), 1).otherwise(0)).alias("missing_temperature"),
            F.sum(F.when(F.col("precipitation_mm").isNull(), 1).otherwise(0)).alias("missing_precipitation"),
            F.countDistinct("h3_cell_7").alias("unique_locations")
        )
        .withColumn("completeness_score",
            1.0 - (F.col("missing_temperature") + F.col("missing_precipitation")) / (F.col("total_records") * 2)
        )
        .withColumn("processing_timestamp", F.current_timestamp())
    )
    
    return climate_quality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Expectations and Monitoring

# COMMAND ----------

@dlt.expect_all_or_warn({"sufficient_data_coverage": "unique_locations >= 10"})
@dlt.expect_all_or_warn({"data_quality_threshold": "avg_quality_score >= 0.8"})
@dlt.expect_or_fail({"recent_data": "date >= current_date() - interval 7 days"})

def validate_pipeline_quality():
    """
    Validate overall pipeline data quality
    """
    return {
        "temperature_valid": "temperature_celsius BETWEEN -50 AND 60",
        "precipitation_valid": "precipitation_mm >= 0 AND precipitation_mm <= 500",
        "coordinates_valid": "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180",
        "h3_populated": "h3_cell_7 IS NOT NULL",
        "recent_processing": "processing_timestamp >= current_timestamp() - INTERVAL 24 HOURS"
    }

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create views in Unity Catalog for analytics
# MAGIC CREATE OR REPLACE VIEW climate_risk_catalog.analytics.historical_climate_summary AS
# MAGIC SELECT 
# MAGIC   data_source,
# MAGIC   COUNT(*) as total_records,
# MAGIC   COUNT(DISTINCT h3_cell_7) as unique_locations,
# MAGIC   MIN(observation_date) as earliest_date,
# MAGIC   MAX(observation_date) as latest_date,
# MAGIC   AVG(data_quality_score) as avg_quality_score
# MAGIC FROM climate_risk_catalog.processed_data.climate_observations
# MAGIC WHERE observation_date >= current_date() - INTERVAL 365 DAYS
# MAGIC GROUP BY data_source;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create monitoring view for pipeline health
# MAGIC CREATE OR REPLACE VIEW climate_risk_catalog.analytics.pipeline_health_status AS
# MAGIC SELECT 
# MAGIC   'Historical Climate Processing' as pipeline_name,
# MAGIC   COUNT(*) as records_processed_today,
# MAGIC   AVG(data_quality_score) as avg_quality_today,
# MAGIC   COUNT(DISTINCT h3_cell_7) as locations_updated_today,
# MAGIC   MAX(processing_timestamp) as last_processing_time,
# MAGIC   CASE 
# MAGIC     WHEN MAX(processing_timestamp) >= current_timestamp() - INTERVAL 6 HOURS THEN 'HEALTHY'
# MAGIC     WHEN MAX(processing_timestamp) >= current_timestamp() - INTERVAL 24 HOURS THEN 'WARNING'
# MAGIC     ELSE 'CRITICAL'
# MAGIC   END as health_status
# MAGIC FROM climate_risk_catalog.processed_data.climate_observations
# MAGIC WHERE DATE(processing_timestamp) = current_date()
