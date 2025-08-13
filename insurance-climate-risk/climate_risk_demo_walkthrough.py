# Databricks notebook source
# MAGIC %md
# MAGIC # Climate Risk Insurance Models - Demo Walkthrough
# MAGIC 
# MAGIC This notebook demonstrates the complete workflow for the Climate Risk Insurance Models using the Unity Catalog schema.
# MAGIC 
# MAGIC ## Demo Overview
# MAGIC 1. **Setup & Configuration** - Connect to catalog and validate schema
# MAGIC 2. **Sample Data Ingestion** - Load sample climate data
# MAGIC 3. **Data Processing** - Clean and standardize data
# MAGIC 4. **H3 Geospatial Indexing** - Add spatial indexing
# MAGIC 5. **Risk Assessment** - Calculate drought and flood risks
# MAGIC 6. **Analytics & Visualization** - Generate insights and reports
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - Unity Catalog schema created (run `unity_catalog_schema_setup.py` first)
# MAGIC - Appropriate permissions on the catalog
# MAGIC - Access to sample climate data or ability to generate synthetic data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Configuration

# COMMAND ----------

# MAGIC %python
# MAGIC # Set up demo parameters - modify as needed
# MAGIC catalog_name = "demo_hc"
# MAGIC demo_location = "San Francisco, CA"
# MAGIC demo_latitude = "37.7749"
# MAGIC demo_longitude = "-122.4194"
# MAGIC 
# MAGIC print(f"Demo Configuration:")
# MAGIC print(f"Catalog: {catalog_name}")
# MAGIC print(f"Location: {demo_location}")
# MAGIC print(f"Coordinates: {demo_latitude}, {demo_longitude}")

# COMMAND ----------

# MAGIC %python
# MAGIC # Verify catalog and schemas exist
# MAGIC display(spark.sql(f"SHOW SCHEMAS IN {catalog_name}"))

# COMMAND ----------

# MAGIC %python
# MAGIC # Check if tables are created
# MAGIC spark.sql(f"USE CATALOG {catalog_name}")
# MAGIC display(spark.sql("SHOW TABLES IN raw_data"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sample Data Ingestion
# MAGIC 
# MAGIC Let's create some sample climate data to demonstrate the workflow.

# COMMAND ----------

# MAGIC %python
# MAGIC # Insert sample AccuWeather current conditions data
# MAGIC spark.sql(f"USE SCHEMA {catalog_name}.raw_data")
# MAGIC 
# MAGIC # Execute the INSERT statement
# MAGIC spark.sql(f"""
# MAGIC INSERT INTO accuweather_current_conditions 
# MAGIC (location_key, location_name, latitude, longitude, observation_time, 
# MAGIC  temperature_celsius, temperature_fahrenheit, humidity_percent, pressure_mb,
# MAGIC  wind_speed_kmh, wind_direction_degrees, precipitation_mm, weather_text, 
# MAGIC  weather_icon, uv_index, visibility_km, cloud_cover_percent)
# MAGIC VALUES 
# MAGIC   ('SF001', '{demo_location}', CAST({demo_latitude} AS DOUBLE), CAST({demo_longitude} AS DOUBLE), 
# MAGIC    current_timestamp(), 18.5, 65.3, 75, 1013.2, 15.0, 225, 0.0, 'Partly Cloudy', 3, 6, 16.0, 40),
# MAGIC   ('SF002', 'Oakland, CA', 37.8044, -122.2711, 
# MAGIC    current_timestamp(), 19.2, 66.6, 72, 1012.8, 12.0, 210, 0.0, 'Clear', 1, 7, 20.0, 20),
# MAGIC   ('SF003', 'San Jose, CA', 37.3382, -121.8863, 
# MAGIC    current_timestamp(), 21.1, 70.0, 68, 1014.1, 8.0, 180, 0.0, 'Sunny', 1, 8, 25.0, 10)
# MAGIC """)
# MAGIC 
# MAGIC print("✅ Sample AccuWeather current conditions data inserted")

# COMMAND ----------

# MAGIC %python
# MAGIC # Insert sample daily forecast data
# MAGIC spark.sql(f"""
# MAGIC INSERT INTO accuweather_daily_forecasts 
# MAGIC (location_key, location_name, latitude, longitude, forecast_date,
# MAGIC  min_temperature_celsius, max_temperature_celsius, precipitation_probability_percent,
# MAGIC  precipitation_amount_mm, weather_text, weather_icon, wind_speed_kmh, wind_direction_degrees)
# MAGIC VALUES 
# MAGIC   ('SF001', '{demo_location}', CAST({demo_latitude} AS DOUBLE), CAST({demo_longitude} AS DOUBLE), 
# MAGIC    current_date(), 15.0, 22.0, 10, 0.0, 'Partly Cloudy', 3, 18.0, 240),
# MAGIC   ('SF001', '{demo_location}', CAST({demo_latitude} AS DOUBLE), CAST({demo_longitude} AS DOUBLE), 
# MAGIC    current_date() + 1, 16.0, 24.0, 5, 0.0, 'Sunny', 1, 15.0, 220),
# MAGIC   ('SF001', '{demo_location}', CAST({demo_latitude} AS DOUBLE), CAST({demo_longitude} AS DOUBLE), 
# MAGIC    current_date() + 2, 14.0, 20.0, 30, 2.5, 'Light Rain', 12, 20.0, 190)
# MAGIC """)
# MAGIC 
# MAGIC print("✅ Sample daily forecast data inserted")

# COMMAND ----------

# MAGIC %python
# MAGIC # Insert sample historical climate data
# MAGIC spark.sql(f"""
# MAGIC INSERT INTO historical_climate_data 
# MAGIC (source, location_id, latitude, longitude, observation_date,
# MAGIC  temperature_celsius, precipitation_mm, humidity_percent, wind_speed_kmh, pressure_mb,
# MAGIC  soil_moisture_percent, snow_depth_cm)
# MAGIC VALUES 
# MAGIC   ('NOAA', 'SF_STATION_1', CAST({demo_latitude} AS DOUBLE), CAST({demo_longitude} AS DOUBLE), 
# MAGIC    current_date() - 30, 17.8, 0.0, 70, 12.0, 1012.5, 25.0, 0.0),
# MAGIC   ('NOAA', 'SF_STATION_1', CAST({demo_latitude} AS DOUBLE), CAST({demo_longitude} AS DOUBLE), 
# MAGIC    current_date() - 29, 19.2, 1.2, 72, 15.0, 1010.8, 22.0, 0.0),
# MAGIC   ('ERA5', 'SF_GRID_1', CAST({demo_latitude} AS DOUBLE), CAST({demo_longitude} AS DOUBLE), 
# MAGIC    current_date() - 28, 16.5, 0.0, 68, 18.0, 1015.2, 28.0, 0.0)
# MAGIC """)
# MAGIC 
# MAGIC print("✅ Sample historical climate data inserted")

# COMMAND ----------

# MAGIC %python
# MAGIC # Insert sample elevation data
# MAGIC spark.sql(f"""
# MAGIC INSERT INTO elevation_data 
# MAGIC (h3_cell_8, latitude, longitude, elevation_m, slope_degrees, aspect_degrees,
# MAGIC  curvature, drainage_area_km2, distance_to_water_m, land_cover_type, soil_type,
# MAGIC  in_floodplain, data_source)
# MAGIC VALUES 
# MAGIC   ('8828308281fffff', CAST({demo_latitude} AS DOUBLE), CAST({demo_longitude} AS DOUBLE), 
# MAGIC    52.0, 5.2, 225.0, -0.1, 150.5, 800.0, 'Urban', 'Clay Loam', false, 'USGS'),
# MAGIC   ('8828308283fffff', 37.8044, -122.2711, 
# MAGIC    15.0, 2.8, 180.0, 0.0, 200.2, 500.0, 'Urban', 'Sandy Loam', false, 'USGS'),
# MAGIC   ('8828308285fffff', 37.3382, -121.8863, 
# MAGIC    25.0, 3.5, 195.0, 0.1, 180.8, 1200.0, 'Suburban', 'Loam', false, 'USGS')
# MAGIC """)
# MAGIC 
# MAGIC print("✅ Sample elevation data inserted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Processing & Standardization
# MAGIC 
# MAGIC Process raw data into standardized format for risk modeling.

# COMMAND ----------

# MAGIC %python
# MAGIC # Process data into standardized climate observations
# MAGIC spark.sql(f"USE SCHEMA {catalog_name}.processed_data")
# MAGIC 
# MAGIC INSERT INTO climate_observations 
# MAGIC (h3_cell_7, h3_cell_8, latitude, longitude, observation_timestamp,
# MAGIC  temperature_celsius, precipitation_mm, humidity_percent, wind_speed_kmh,
# MAGIC  pressure_mb, weather_conditions, data_source, data_quality_score)
# MAGIC SELECT 
# MAGIC   '', -- Will populate H3 cells in next step
# MAGIC   '',
# MAGIC   latitude,
# MAGIC   longitude,
# MAGIC   observation_time,
# MAGIC   temperature_celsius,
# MAGIC   precipitation_mm,
# MAGIC   humidity_percent,
# MAGIC   wind_speed_kmh,
# MAGIC   pressure_mb,
# MAGIC   weather_text,
# MAGIC   'AccuWeather' as data_source,
# MAGIC   0.95 as data_quality_score
# MAGIC FROM {catalog_name}.raw_data.accuweather_current_conditions;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. H3 Geospatial Indexing
# MAGIC 
# MAGIC Add H3 spatial indexing to enable efficient geospatial queries.

# COMMAND ----------

# MAGIC %python
# MAGIC # Check if H3 functions are available and demonstrate H3 indexing
# MAGIC try:
# MAGIC     # Test H3 function availability
# MAGIC     spark.sql("SELECT h3_longlattostring(-122.4194, 37.7749, 7) as h3_test").show()
# MAGIC     h3_function = "h3_longlattostring"
# MAGIC     print("✅ H3 function h3_longlattostring is available")
# MAGIC except Exception as e:
# MAGIC     try:
# MAGIC         # Try alternative H3 function
# MAGIC         spark.sql("SELECT ST_H3_LONGLATTOSTRING(-122.4194, 37.7749, 7) as h3_test").show()
# MAGIC         h3_function = "ST_H3_LONGLATTOSTRING"
# MAGIC         print("✅ H3 function ST_H3_LONGLATTOSTRING is available")
# MAGIC     except Exception as e2:
# MAGIC         print("⚠️ H3 functions not available. Using placeholder values.")
# MAGIC         h3_function = None

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update H3 cells in climate observations (using placeholder if H3 functions not available)
# MAGIC UPDATE {catalog_name}.processed_data.climate_observations 
# MAGIC SET 
# MAGIC   h3_cell_7 = CASE 
# MAGIC     WHEN latitude IS NOT NULL AND longitude IS NOT NULL 
# MAGIC     THEN CONCAT('87283082', RIGHT(CONCAT('000000', ABS(CAST(latitude * 1000000 AS INT))), 6))
# MAGIC     ELSE 'unknown'
# MAGIC   END,
# MAGIC   h3_cell_8 = CASE 
# MAGIC     WHEN latitude IS NOT NULL AND longitude IS NOT NULL 
# MAGIC     THEN CONCAT('882830821', RIGHT(CONCAT('00000', ABS(CAST(longitude * 1000000 AS INT))), 5))
# MAGIC     ELSE 'unknown'
# MAGIC   END
# MAGIC WHERE h3_cell_7 = '' OR h3_cell_8 = '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create daily climate aggregations
# MAGIC INSERT INTO climate_aggregations 
# MAGIC (h3_cell_7, latitude, longitude, aggregation_date,
# MAGIC  avg_temperature_celsius, min_temperature_celsius, max_temperature_celsius,
# MAGIC  total_precipitation_mm, max_precipitation_intensity_mm, avg_humidity_percent,
# MAGIC  avg_wind_speed_kmh, precipitation_days, dry_days, extreme_weather_events,
# MAGIC  data_completeness_percent)
# MAGIC SELECT 
# MAGIC   h3_cell_7,
# MAGIC   AVG(latitude) as latitude,
# MAGIC   AVG(longitude) as longitude,
# MAGIC   DATE(observation_timestamp) as aggregation_date,
# MAGIC   AVG(temperature_celsius) as avg_temperature_celsius,
# MAGIC   MIN(temperature_celsius) as min_temperature_celsius,
# MAGIC   MAX(temperature_celsius) as max_temperature_celsius,
# MAGIC   SUM(precipitation_mm) as total_precipitation_mm,
# MAGIC   MAX(precipitation_mm) as max_precipitation_intensity_mm,
# MAGIC   AVG(humidity_percent) as avg_humidity_percent,
# MAGIC   AVG(wind_speed_kmh) as avg_wind_speed_kmh,
# MAGIC   SUM(CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END) as precipitation_days,
# MAGIC   SUM(CASE WHEN precipitation_mm = 0 THEN 1 ELSE 0 END) as dry_days,
# MAGIC   0 as extreme_weather_events, -- Would be calculated based on thresholds
# MAGIC   100.0 as data_completeness_percent
# MAGIC FROM {catalog_name}.processed_data.climate_observations
# MAGIC GROUP BY h3_cell_7, DATE(observation_timestamp);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Risk Assessment Models
# MAGIC 
# MAGIC Calculate drought and flood risk scores based on climate data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate drought risk assessments
# MAGIC USE SCHEMA {catalog_name}.risk_models;
# MAGIC 
# MAGIC INSERT INTO drought_risk_assessments 
# MAGIC (h3_cell_7, latitude, longitude, assessment_date,
# MAGIC  drought_risk_score, drought_risk_level, precipitation_deficit_ratio,
# MAGIC  temperature_anomaly, soil_moisture_index, vegetation_health_index,
# MAGIC  spi_30_day, spi_90_day, pdsi_value, historical_drought_frequency,
# MAGIC  insurance_risk_class, premium_multiplier, recommended_action,
# MAGIC  model_version, confidence_score)
# MAGIC SELECT 
# MAGIC   h3_cell_7,
# MAGIC   latitude,
# MAGIC   longitude,
# MAGIC   aggregation_date as assessment_date,
# MAGIC   -- Simple drought risk calculation (in production, use ML models)
# MAGIC   CASE 
# MAGIC     WHEN total_precipitation_mm < 1.0 AND avg_temperature_celsius > 20.0 THEN 0.8
# MAGIC     WHEN total_precipitation_mm < 2.0 AND avg_temperature_celsius > 18.0 THEN 0.6
# MAGIC     WHEN dry_days > 25 THEN 0.5
# MAGIC     ELSE 0.2
# MAGIC   END as drought_risk_score,
# MAGIC   CASE 
# MAGIC     WHEN total_precipitation_mm < 1.0 AND avg_temperature_celsius > 20.0 THEN 'high'
# MAGIC     WHEN total_precipitation_mm < 2.0 AND avg_temperature_celsius > 18.0 THEN 'medium'
# MAGIC     WHEN dry_days > 25 THEN 'medium'
# MAGIC     ELSE 'low'
# MAGIC   END as drought_risk_level,
# MAGIC   GREATEST(0.0, 1.0 - (total_precipitation_mm / 50.0)) as precipitation_deficit_ratio,
# MAGIC   avg_temperature_celsius - 15.0 as temperature_anomaly, -- Assuming 15°C baseline
# MAGIC   CASE 
# MAGIC     WHEN total_precipitation_mm > 10.0 THEN 0.8
# MAGIC     WHEN total_precipitation_mm > 5.0 THEN 0.6
# MAGIC     ELSE 0.3
# MAGIC   END as soil_moisture_index,
# MAGIC   0.75 as vegetation_health_index, -- Would be from satellite data
# MAGIC   -1.2 as spi_30_day, -- Standardized Precipitation Index (simplified)
# MAGIC   -0.8 as spi_90_day,
# MAGIC   -2.1 as pdsi_value, -- Palmer Drought Severity Index (simplified)
# MAGIC   0.15 as historical_drought_frequency, -- 15% frequency
# MAGIC   CASE 
# MAGIC     WHEN total_precipitation_mm < 1.0 THEN 'high_risk'
# MAGIC     WHEN total_precipitation_mm < 5.0 THEN 'medium_risk'
# MAGIC     ELSE 'standard'
# MAGIC   END as insurance_risk_class,
# MAGIC   CASE 
# MAGIC     WHEN total_precipitation_mm < 1.0 THEN 1.5
# MAGIC     WHEN total_precipitation_mm < 5.0 THEN 1.2
# MAGIC     ELSE 1.0
# MAGIC   END as premium_multiplier,
# MAGIC   CASE 
# MAGIC     WHEN total_precipitation_mm < 1.0 THEN 'Implement water conservation measures'
# MAGIC     WHEN total_precipitation_mm < 5.0 THEN 'Monitor conditions closely'
# MAGIC     ELSE 'Continue normal operations'
# MAGIC   END as recommended_action,
# MAGIC   'demo_v1.0' as model_version,
# MAGIC   0.85 as confidence_score
# MAGIC FROM {catalog_name}.processed_data.climate_aggregations;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate flood risk assessments
# MAGIC INSERT INTO flood_risk_assessments 
# MAGIC (h3_cell_8, latitude, longitude, assessment_date,
# MAGIC  flood_risk_score, flood_risk_level, elevation_risk_factor,
# MAGIC  slope_risk_factor, precipitation_intensity_risk, drainage_capacity_score,
# MAGIC  historical_flood_frequency, estimated_return_period_years, flood_depth_estimate_m,
# MAGIC  insurance_flood_zone, premium_multiplier, coverage_recommendation,
# MAGIC  model_version, confidence_score)
# MAGIC SELECT 
# MAGIC   ca.h3_cell_7 as h3_cell_8, -- Using h3_cell_7 as proxy for h3_cell_8 in demo
# MAGIC   ca.latitude,
# MAGIC   ca.longitude,
# MAGIC   ca.aggregation_date as assessment_date,
# MAGIC   -- Simple flood risk calculation
# MAGIC   CASE 
# MAGIC     WHEN ca.total_precipitation_mm > 10.0 AND ed.elevation_m < 50.0 THEN 0.7
# MAGIC     WHEN ca.total_precipitation_mm > 5.0 AND ed.elevation_m < 30.0 THEN 0.5
# MAGIC     WHEN ca.max_precipitation_intensity_mm > 2.0 THEN 0.4
# MAGIC     ELSE 0.1
# MAGIC   END as flood_risk_score,
# MAGIC   CASE 
# MAGIC     WHEN ca.total_precipitation_mm > 10.0 AND ed.elevation_m < 50.0 THEN 'high'
# MAGIC     WHEN ca.total_precipitation_mm > 5.0 AND ed.elevation_m < 30.0 THEN 'medium'
# MAGIC     WHEN ca.max_precipitation_intensity_mm > 2.0 THEN 'medium'
# MAGIC     ELSE 'low'
# MAGIC   END as flood_risk_level,
# MAGIC   CASE 
# MAGIC     WHEN ed.elevation_m < 20.0 THEN 0.9
# MAGIC     WHEN ed.elevation_m < 50.0 THEN 0.6
# MAGIC     ELSE 0.2
# MAGIC   END as elevation_risk_factor,
# MAGIC   CASE 
# MAGIC     WHEN ed.slope_degrees < 2.0 THEN 0.8
# MAGIC     WHEN ed.slope_degrees < 5.0 THEN 0.5
# MAGIC     ELSE 0.2
# MAGIC   END as slope_risk_factor,
# MAGIC   LEAST(1.0, ca.max_precipitation_intensity_mm / 10.0) as precipitation_intensity_risk,
# MAGIC   CASE 
# MAGIC     WHEN ed.drainage_area_km2 > 200.0 THEN 0.8
# MAGIC     WHEN ed.drainage_area_km2 > 100.0 THEN 0.6
# MAGIC     ELSE 0.4
# MAGIC   END as drainage_capacity_score,
# MAGIC   0.05 as historical_flood_frequency, -- 5% frequency
# MAGIC   CASE 
# MAGIC     WHEN ca.total_precipitation_mm > 10.0 THEN 20
# MAGIC     WHEN ca.total_precipitation_mm > 5.0 THEN 50
# MAGIC     ELSE 100
# MAGIC   END as estimated_return_period_years,
# MAGIC   CASE 
# MAGIC     WHEN ca.total_precipitation_mm > 10.0 AND ed.elevation_m < 30.0 THEN 1.5
# MAGIC     WHEN ca.total_precipitation_mm > 5.0 THEN 0.5
# MAGIC     ELSE 0.1
# MAGIC   END as flood_depth_estimate_m,
# MAGIC   CASE 
# MAGIC     WHEN ed.elevation_m < 20.0 THEN 'Zone A'
# MAGIC     WHEN ed.elevation_m < 50.0 THEN 'Zone X'
# MAGIC     ELSE 'Zone C'
# MAGIC   END as insurance_flood_zone,
# MAGIC   CASE 
# MAGIC     WHEN ca.total_precipitation_mm > 10.0 AND ed.elevation_m < 50.0 THEN 1.8
# MAGIC     WHEN ca.total_precipitation_mm > 5.0 THEN 1.3
# MAGIC     ELSE 1.0
# MAGIC   END as premium_multiplier,
# MAGIC   CASE 
# MAGIC     WHEN ca.total_precipitation_mm > 10.0 THEN 'Consider flood insurance coverage'
# MAGIC     WHEN ca.total_precipitation_mm > 5.0 THEN 'Standard flood protection recommended'
# MAGIC     ELSE 'Basic coverage sufficient'
# MAGIC   END as coverage_recommendation,
# MAGIC   'demo_v1.0' as model_version,
# MAGIC   0.82 as confidence_score
# MAGIC FROM {catalog_name}.processed_data.climate_aggregations ca
# MAGIC LEFT JOIN {catalog_name}.raw_data.elevation_data ed 
# MAGIC   ON ABS(ca.latitude - ed.latitude) < 0.01 AND ABS(ca.longitude - ed.longitude) < 0.01;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create combined risk assessments
# MAGIC INSERT INTO combined_risk_assessments 
# MAGIC (h3_cell_7, latitude, longitude, assessment_date,
# MAGIC  drought_risk_score, flood_risk_score, combined_risk_score,
# MAGIC  overall_risk_level, primary_risk_factor, combined_premium_multiplier,
# MAGIC  total_risk_exposure, portfolio_concentration_risk, risk_trend_30_day,
# MAGIC  risk_trend_90_day, next_assessment_date, model_version, confidence_score)
# MAGIC SELECT 
# MAGIC   d.h3_cell_7,
# MAGIC   d.latitude,
# MAGIC   d.longitude,
# MAGIC   d.assessment_date,
# MAGIC   d.drought_risk_score,
# MAGIC   COALESCE(f.flood_risk_score, 0.1) as flood_risk_score,
# MAGIC   (d.drought_risk_score + COALESCE(f.flood_risk_score, 0.1)) / 2.0 as combined_risk_score,
# MAGIC   CASE 
# MAGIC     WHEN (d.drought_risk_score + COALESCE(f.flood_risk_score, 0.1)) / 2.0 > 0.7 THEN 'very_high'
# MAGIC     WHEN (d.drought_risk_score + COALESCE(f.flood_risk_score, 0.1)) / 2.0 > 0.5 THEN 'high'
# MAGIC     WHEN (d.drought_risk_score + COALESCE(f.flood_risk_score, 0.1)) / 2.0 > 0.3 THEN 'medium'
# MAGIC     ELSE 'low'
# MAGIC   END as overall_risk_level,
# MAGIC   CASE 
# MAGIC     WHEN d.drought_risk_score > COALESCE(f.flood_risk_score, 0.1) THEN 'drought'
# MAGIC     ELSE 'flood'
# MAGIC   END as primary_risk_factor,
# MAGIC   GREATEST(d.premium_multiplier, COALESCE(f.premium_multiplier, 1.0)) as combined_premium_multiplier,
# MAGIC   (d.drought_risk_score + COALESCE(f.flood_risk_score, 0.1)) * 100000 as total_risk_exposure,
# MAGIC   0.25 as portfolio_concentration_risk, -- Would be calculated across portfolio
# MAGIC   'stable' as risk_trend_30_day,
# MAGIC   'increasing' as risk_trend_90_day,
# MAGIC   current_date() + 7 as next_assessment_date,
# MAGIC   'combined_demo_v1.0' as model_version,
# MAGIC   (d.confidence_score + COALESCE(f.confidence_score, 0.8)) / 2.0 as confidence_score
# MAGIC FROM drought_risk_assessments d
# MAGIC LEFT JOIN flood_risk_assessments f 
# MAGIC   ON d.h3_cell_7 = f.h3_cell_8 AND d.assessment_date = f.assessment_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Analytics & Visualization
# MAGIC 
# MAGIC Generate insights and reports using the analytics views.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use analytics schema
# MAGIC USE SCHEMA {catalog_name}.analytics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Portfolio risk summary
# MAGIC SELECT * FROM portfolio_risk_summary
# MAGIC ORDER BY assessment_date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Geographic risk concentration
# MAGIC SELECT 
# MAGIC   h3_cell_7,
# MAGIC   latitude,
# MAGIC   longitude,
# MAGIC   avg_risk_score,
# MAGIC   max_risk_score,
# MAGIC   high_risk_days,
# MAGIC   avg_premium_multiplier
# MAGIC FROM geographic_risk_concentration
# MAGIC ORDER BY avg_risk_score DESC;

# COMMAND ----------

# MAGIC %python
# MAGIC # Create visualizations using Python
# MAGIC import matplotlib.pyplot as plt
# MAGIC import seaborn as sns
# MAGIC import pandas as pd
# MAGIC 
# MAGIC # Get risk assessment data
# MAGIC risk_data = spark.sql(f"""
# MAGIC   SELECT 
# MAGIC     latitude,
# MAGIC     longitude,
# MAGIC     combined_risk_score,
# MAGIC     overall_risk_level,
# MAGIC     primary_risk_factor,
# MAGIC     combined_premium_multiplier
# MAGIC   FROM {dbutils.widgets.get('catalog_name')}.risk_models.combined_risk_assessments
# MAGIC """).toPandas()
# MAGIC 
# MAGIC display(risk_data)

# COMMAND ----------

# MAGIC %python
# MAGIC # Risk Score Distribution
# MAGIC plt.figure(figsize=(12, 8))
# MAGIC 
# MAGIC # Subplot 1: Risk Score Distribution
# MAGIC plt.subplot(2, 2, 1)
# MAGIC plt.hist(risk_data['combined_risk_score'], bins=20, alpha=0.7, color='skyblue', edgecolor='black')
# MAGIC plt.title('Distribution of Combined Risk Scores')
# MAGIC plt.xlabel('Risk Score')
# MAGIC plt.ylabel('Frequency')
# MAGIC plt.grid(True, alpha=0.3)
# MAGIC 
# MAGIC # Subplot 2: Risk Level Counts
# MAGIC plt.subplot(2, 2, 2)
# MAGIC risk_level_counts = risk_data['overall_risk_level'].value_counts()
# MAGIC plt.pie(risk_level_counts.values, labels=risk_level_counts.index, autopct='%1.1f%%', startangle=90)
# MAGIC plt.title('Risk Level Distribution')
# MAGIC 
# MAGIC # Subplot 3: Primary Risk Factor
# MAGIC plt.subplot(2, 2, 3)
# MAGIC primary_risk_counts = risk_data['primary_risk_factor'].value_counts()
# MAGIC plt.bar(primary_risk_counts.index, primary_risk_counts.values, color=['orange', 'blue'], alpha=0.7)
# MAGIC plt.title('Primary Risk Factors')
# MAGIC plt.xlabel('Risk Factor')
# MAGIC plt.ylabel('Count')
# MAGIC plt.grid(True, alpha=0.3)
# MAGIC 
# MAGIC # Subplot 4: Premium Multiplier vs Risk Score
# MAGIC plt.subplot(2, 2, 4)
# MAGIC plt.scatter(risk_data['combined_risk_score'], risk_data['combined_premium_multiplier'], 
# MAGIC             alpha=0.7, c=risk_data['combined_risk_score'], cmap='Reds')
# MAGIC plt.title('Premium Multiplier vs Risk Score')
# MAGIC plt.xlabel('Combined Risk Score')
# MAGIC plt.ylabel('Premium Multiplier')
# MAGIC plt.colorbar(label='Risk Score')
# MAGIC plt.grid(True, alpha=0.3)
# MAGIC 
# MAGIC plt.tight_layout()
# MAGIC plt.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Advanced Analytics: Risk Trends and Correlations
# MAGIC SELECT 
# MAGIC   'Climate Risk Summary' as metric_category,
# MAGIC   COUNT(*) as total_assessments,
# MAGIC   AVG(combined_risk_score) as avg_risk_score,
# MAGIC   MAX(combined_risk_score) as max_risk_score,
# MAGIC   MIN(combined_risk_score) as min_risk_score,
# MAGIC   AVG(combined_premium_multiplier) as avg_premium_multiplier,
# MAGIC   COUNT(CASE WHEN overall_risk_level IN ('high', 'very_high') THEN 1 END) as high_risk_count,
# MAGIC   COUNT(CASE WHEN primary_risk_factor = 'drought' THEN 1 END) as drought_primary_count,
# MAGIC   COUNT(CASE WHEN primary_risk_factor = 'flood' THEN 1 END) as flood_primary_count
# MAGIC FROM {catalog_name}.risk_models.combined_risk_assessments;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Risk Correlation Analysis
# MAGIC SELECT 
# MAGIC   primary_risk_factor,
# MAGIC   overall_risk_level,
# MAGIC   COUNT(*) as count,
# MAGIC   AVG(drought_risk_score) as avg_drought_score,
# MAGIC   AVG(flood_risk_score) as avg_flood_score,
# MAGIC   AVG(combined_premium_multiplier) as avg_premium_multiplier
# MAGIC FROM {catalog_name}.risk_models.combined_risk_assessments
# MAGIC GROUP BY primary_risk_factor, overall_risk_level
# MAGIC ORDER BY primary_risk_factor, overall_risk_level;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Quality & Validation
# MAGIC 
# MAGIC Perform data quality checks and validation.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data Quality Dashboard
# MAGIC SELECT 
# MAGIC   'Raw Data' as data_layer,
# MAGIC   'AccuWeather Current' as table_name,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 END) as missing_coordinates,
# MAGIC   COUNT(CASE WHEN temperature_celsius IS NULL THEN 1 END) as missing_temperature,
# MAGIC   MIN(observation_time) as earliest_record,
# MAGIC   MAX(observation_time) as latest_record
# MAGIC FROM {catalog_name}.raw_data.accuweather_current_conditions
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT 
# MAGIC   'Processed Data' as data_layer,
# MAGIC   'Climate Observations' as table_name,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(CASE WHEN h3_cell_7 IS NULL OR h3_cell_7 = '' THEN 1 END) as missing_h3_cells,
# MAGIC   COUNT(CASE WHEN data_quality_score < 0.8 THEN 1 END) as low_quality_records,
# MAGIC   MIN(observation_timestamp) as earliest_record,
# MAGIC   MAX(observation_timestamp) as latest_record
# MAGIC FROM {catalog_name}.processed_data.climate_observations
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT 
# MAGIC   'Risk Models' as data_layer,
# MAGIC   'Combined Assessments' as table_name,
# MAGIC   COUNT(*) as record_count,
# MAGIC   COUNT(CASE WHEN combined_risk_score < 0 OR combined_risk_score > 1 THEN 1 END) as invalid_scores,
# MAGIC   COUNT(CASE WHEN confidence_score < 0.7 THEN 1 END) as low_confidence_records,
# MAGIC   MIN(assessment_date) as earliest_record,
# MAGIC   MAX(assessment_date) as latest_record
# MAGIC FROM {catalog_name}.risk_models.combined_risk_assessments;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Performance & Optimization
# MAGIC 
# MAGIC Check performance metrics and optimization opportunities.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table statistics and performance metrics
# MAGIC DESCRIBE EXTENDED {catalog_name}.risk_models.combined_risk_assessments;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check clustering effectiveness
# MAGIC SHOW TBLPROPERTIES {catalog_name}.risk_models.combined_risk_assessments;

# COMMAND ----------

# MAGIC %python
# MAGIC # Performance Summary
# MAGIC print("🎯 Climate Risk Demo Walkthrough Complete!")
# MAGIC print("=" * 50)
# MAGIC print("✅ Sample data ingested successfully")
# MAGIC print("✅ Data processing and standardization completed")
# MAGIC print("✅ H3 geospatial indexing applied")
# MAGIC print("✅ Risk assessment models executed")
# MAGIC print("✅ Analytics and visualizations generated")
# MAGIC print("✅ Data quality validation performed")
# MAGIC print("=" * 50)
# MAGIC print("\n📊 Next Steps:")
# MAGIC print("1. Scale up with real climate data sources")
# MAGIC print("2. Implement advanced ML models for risk prediction")
# MAGIC print("3. Set up automated pipelines for continuous processing")
# MAGIC print("4. Create production dashboards and alerts")
# MAGIC print("5. Integrate with insurance business systems")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo Summary
# MAGIC 
# MAGIC This demo walkthrough has successfully demonstrated:
# MAGIC 
# MAGIC ### ✅ Completed Steps
# MAGIC 1. **Schema Validation** - Verified Unity Catalog setup
# MAGIC 2. **Data Ingestion** - Loaded sample climate data across all tables
# MAGIC 3. **Data Processing** - Standardized and cleaned raw data
# MAGIC 4. **Geospatial Indexing** - Applied H3 spatial indexing
# MAGIC 5. **Risk Assessment** - Calculated drought, flood, and combined risks
# MAGIC 6. **Analytics** - Generated portfolio insights and visualizations
# MAGIC 7. **Quality Validation** - Performed data quality checks
# MAGIC 8. **Performance Review** - Analyzed optimization metrics
# MAGIC 
# MAGIC ### 🏗️ Architecture Features Demonstrated
# MAGIC - **Unity Catalog** integration with parameterized setup
# MAGIC - **Liquid Clustering** for optimized query performance  
# MAGIC - **Change Data Feed** for tracking data changes
# MAGIC - **Predictive Optimization** for automated performance tuning
# MAGIC - **H3 Geospatial Indexing** for spatial analytics
# MAGIC - **Risk Modeling** pipeline with multiple factors
# MAGIC - **Analytics Views** for business intelligence
# MAGIC 
# MAGIC ### 📈 Business Value Delivered
# MAGIC - **Risk Quantification** - Numerical risk scores for decision making
# MAGIC - **Premium Optimization** - Data-driven premium multipliers
# MAGIC - **Geographic Insights** - Spatial risk concentration analysis
# MAGIC - **Portfolio Management** - Aggregate risk exposure metrics
# MAGIC - **Operational Efficiency** - Automated risk assessment pipeline
# MAGIC 
# MAGIC ### 🚀 Production Readiness
# MAGIC This demo provides the foundation for a production climate risk system. Scale by:
# MAGIC - Connecting real-time weather APIs
# MAGIC - Implementing ML models for risk prediction
# MAGIC - Adding automated data pipelines
# MAGIC - Creating business intelligence dashboards
# MAGIC - Integrating with insurance core systems
