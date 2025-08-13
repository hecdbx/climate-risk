# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Schema Configuration for Climate Risk Insurance Models
# MAGIC 
# MAGIC This notebook sets up the complete Unity Catalog schema structure for climate data ingestion and processing.
# MAGIC 
# MAGIC ## Features
# MAGIC - Parameterized catalog name for flexibility
# MAGIC - Databricks Liquid Clustering for optimal performance
# MAGIC - Predictive optimization enabled
# MAGIC - Comprehensive schema for climate risk insurance models
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - Unity Catalog enabled workspace
# MAGIC - Permissions to create catalogs and schemas
# MAGIC - Access to required user groups for permissions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Parameters
# MAGIC 
# MAGIC Set the catalog name parameter using SQL SET command. Modify the value as needed for your environment.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Parameter for catalog name - modify this value as needed
# MAGIC SET catalog_name = 'demo_hc';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog and Schema Creation
# MAGIC 
# MAGIC Creating the main catalog and data domain schemas.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog for climate risk data
# MAGIC CREATE CATALOG IF NOT EXISTS ${catalog_name}
# MAGIC COMMENT 'Unified catalog for climate risk insurance models and data';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the catalog
# MAGIC USE CATALOG ${catalog_name};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schemas for different data domains
# MAGIC CREATE SCHEMA IF NOT EXISTS raw_data
# MAGIC COMMENT 'Raw ingested data from external sources (AccuWeather, NOAA, etc.)';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS processed_data
# MAGIC COMMENT 'Cleaned and transformed climate data';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS risk_models
# MAGIC COMMENT 'Risk assessment models and computed risk scores';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS analytics
# MAGIC COMMENT 'Analytical views and aggregated data for reporting';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw Data Tables
# MAGIC 
# MAGIC Setting up tables for ingesting raw climate data from external sources.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set up raw data tables for AccuWeather ingestion
# MAGIC USE SCHEMA raw_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- AccuWeather current conditions table
# MAGIC CREATE OR REPLACE TABLE accuweather_current_conditions (
# MAGIC   location_key STRING NOT NULL,
# MAGIC   location_name STRING,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   observation_time TIMESTAMP,
# MAGIC   temperature_celsius DOUBLE,
# MAGIC   temperature_fahrenheit DOUBLE,
# MAGIC   humidity_percent INT,
# MAGIC   pressure_mb DOUBLE,
# MAGIC   wind_speed_kmh DOUBLE,
# MAGIC   wind_direction_degrees INT,
# MAGIC   precipitation_mm DOUBLE,
# MAGIC   weather_text STRING,
# MAGIC   weather_icon INT,
# MAGIC   uv_index INT,
# MAGIC   visibility_km DOUBLE,
# MAGIC   cloud_cover_percent INT,
# MAGIC   ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   h3_cell_7 STRING,
# MAGIC   h3_cell_8 STRING
# MAGIC ) 
# MAGIC USING DELTA
# MAGIC CLUSTER BY (DATE(observation_time), location_key)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enablePredictiveOptimization' = 'true'
# MAGIC )
# MAGIC COMMENT 'Real-time current weather conditions from AccuWeather API';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- AccuWeather daily forecasts table
# MAGIC CREATE OR REPLACE TABLE accuweather_daily_forecasts (
# MAGIC   location_key STRING NOT NULL,
# MAGIC   location_name STRING,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   forecast_date DATE,
# MAGIC   min_temperature_celsius DOUBLE,
# MAGIC   max_temperature_celsius DOUBLE,
# MAGIC   precipitation_probability_percent INT,
# MAGIC   precipitation_amount_mm DOUBLE,
# MAGIC   weather_text STRING,
# MAGIC   weather_icon INT,
# MAGIC   wind_speed_kmh DOUBLE,
# MAGIC   wind_direction_degrees INT,
# MAGIC   ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   h3_cell_7 STRING,
# MAGIC   h3_cell_8 STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (forecast_date, location_key)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enablePredictiveOptimization' = 'true'
# MAGIC )
# MAGIC COMMENT 'Daily weather forecasts from AccuWeather API';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Historical climate data from other sources
# MAGIC CREATE OR REPLACE TABLE historical_climate_data (
# MAGIC   source STRING NOT NULL,
# MAGIC   location_id STRING,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   observation_date DATE,
# MAGIC   temperature_celsius DOUBLE,
# MAGIC   precipitation_mm DOUBLE,
# MAGIC   humidity_percent INT,
# MAGIC   wind_speed_kmh DOUBLE,
# MAGIC   pressure_mb DOUBLE,
# MAGIC   soil_moisture_percent DOUBLE,
# MAGIC   snow_depth_cm DOUBLE,
# MAGIC   ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   h3_cell_7 STRING,
# MAGIC   h3_cell_8 STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (source, observation_date, location_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enablePredictiveOptimization' = 'true'
# MAGIC )
# MAGIC COMMENT 'Historical climate data from various sources (NOAA, ERA5, etc.)';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Elevation and topographic data
# MAGIC CREATE OR REPLACE TABLE elevation_data (
# MAGIC   h3_cell_8 STRING NOT NULL,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   elevation_m DOUBLE,
# MAGIC   slope_degrees DOUBLE,
# MAGIC   aspect_degrees DOUBLE,
# MAGIC   curvature DOUBLE,
# MAGIC   drainage_area_km2 DOUBLE,
# MAGIC   distance_to_water_m DOUBLE,
# MAGIC   land_cover_type STRING,
# MAGIC   soil_type STRING,
# MAGIC   in_floodplain BOOLEAN,
# MAGIC   data_source STRING,
# MAGIC   last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enablePredictiveOptimization' = 'true'
# MAGIC )
# MAGIC COMMENT 'Elevation and topographic data for flood risk modeling';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processed Data Tables
# MAGIC 
# MAGIC Creating tables for cleaned and standardized climate data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set up processed data schema
# MAGIC USE SCHEMA processed_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Unified climate observations (cleaned and standardized)
# MAGIC CREATE OR REPLACE TABLE climate_observations (
# MAGIC   h3_cell_7 STRING NOT NULL,
# MAGIC   h3_cell_8 STRING NOT NULL,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   observation_timestamp TIMESTAMP,
# MAGIC   temperature_celsius DOUBLE,
# MAGIC   precipitation_mm DOUBLE,
# MAGIC   humidity_percent INT,
# MAGIC   wind_speed_kmh DOUBLE,
# MAGIC   pressure_mb DOUBLE,
# MAGIC   weather_conditions STRING,
# MAGIC   data_source STRING,
# MAGIC   data_quality_score DOUBLE,
# MAGIC   processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (DATE(observation_timestamp), data_source, h3_cell_7)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enablePredictiveOptimization' = 'true'
# MAGIC )
# MAGIC COMMENT 'Standardized climate observations from all sources';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Climate aggregations for risk modeling
# MAGIC CREATE OR REPLACE TABLE climate_aggregations (
# MAGIC   h3_cell_7 STRING NOT NULL,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   aggregation_date DATE,
# MAGIC   avg_temperature_celsius DOUBLE,
# MAGIC   min_temperature_celsius DOUBLE,
# MAGIC   max_temperature_celsius DOUBLE,
# MAGIC   total_precipitation_mm DOUBLE,
# MAGIC   max_precipitation_intensity_mm DOUBLE,
# MAGIC   avg_humidity_percent DOUBLE,
# MAGIC   avg_wind_speed_kmh DOUBLE,
# MAGIC   precipitation_days INT,
# MAGIC   dry_days INT,
# MAGIC   extreme_weather_events INT,
# MAGIC   data_completeness_percent DOUBLE,
# MAGIC   processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (aggregation_date, h3_cell_7)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enablePredictiveOptimization' = 'true'
# MAGIC )
# MAGIC COMMENT 'Daily aggregated climate data by H3 cell for risk modeling';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Risk Assessment Tables
# MAGIC 
# MAGIC Creating tables for storing risk assessment results and models.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set up risk models schema
# MAGIC USE SCHEMA risk_models;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drought risk assessments
# MAGIC CREATE OR REPLACE TABLE drought_risk_assessments (
# MAGIC   h3_cell_7 STRING NOT NULL,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   assessment_date DATE,
# MAGIC   drought_risk_score DOUBLE,
# MAGIC   drought_risk_level STRING,
# MAGIC   precipitation_deficit_ratio DOUBLE,
# MAGIC   temperature_anomaly DOUBLE,
# MAGIC   soil_moisture_index DOUBLE,
# MAGIC   vegetation_health_index DOUBLE,
# MAGIC   spi_30_day DOUBLE,
# MAGIC   spi_90_day DOUBLE,
# MAGIC   pdsi_value DOUBLE,
# MAGIC   historical_drought_frequency DOUBLE,
# MAGIC   insurance_risk_class STRING,
# MAGIC   premium_multiplier DOUBLE,
# MAGIC   recommended_action STRING,
# MAGIC   model_version STRING,
# MAGIC   confidence_score DOUBLE,
# MAGIC   processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (assessment_date, h3_cell_7, drought_risk_level)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enablePredictiveOptimization' = 'true'
# MAGIC )
# MAGIC COMMENT 'Drought risk assessments by location and date';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Flood risk assessments
# MAGIC CREATE OR REPLACE TABLE flood_risk_assessments (
# MAGIC   h3_cell_8 STRING NOT NULL,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   assessment_date DATE,
# MAGIC   flood_risk_score DOUBLE,
# MAGIC   flood_risk_level STRING,
# MAGIC   elevation_risk_factor DOUBLE,
# MAGIC   slope_risk_factor DOUBLE,
# MAGIC   precipitation_intensity_risk DOUBLE,
# MAGIC   drainage_capacity_score DOUBLE,
# MAGIC   historical_flood_frequency DOUBLE,
# MAGIC   estimated_return_period_years INT,
# MAGIC   flood_depth_estimate_m DOUBLE,
# MAGIC   insurance_flood_zone STRING,
# MAGIC   premium_multiplier DOUBLE,
# MAGIC   coverage_recommendation STRING,
# MAGIC   model_version STRING,
# MAGIC   confidence_score DOUBLE,
# MAGIC   processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (assessment_date, h3_cell_8, flood_risk_level)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enablePredictiveOptimization' = 'true'
# MAGIC )
# MAGIC COMMENT 'Flood risk assessments by location and date';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Combined climate risk assessments
# MAGIC CREATE OR REPLACE TABLE combined_risk_assessments (
# MAGIC   h3_cell_7 STRING NOT NULL,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   assessment_date DATE,
# MAGIC   drought_risk_score DOUBLE,
# MAGIC   flood_risk_score DOUBLE,
# MAGIC   combined_risk_score DOUBLE,
# MAGIC   overall_risk_level STRING,
# MAGIC   primary_risk_factor STRING,
# MAGIC   combined_premium_multiplier DOUBLE,
# MAGIC   total_risk_exposure DOUBLE,
# MAGIC   portfolio_concentration_risk DOUBLE,
# MAGIC   risk_trend_30_day STRING,
# MAGIC   risk_trend_90_day STRING,
# MAGIC   next_assessment_date DATE,
# MAGIC   model_version STRING,
# MAGIC   confidence_score DOUBLE,
# MAGIC   processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (assessment_date, overall_risk_level, h3_cell_7)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enablePredictiveOptimization' = 'true'
# MAGIC )
# MAGIC COMMENT 'Combined climate risk assessments for insurance applications';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analytics Views
# MAGIC 
# MAGIC Creating analytical views for reporting and business intelligence.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set up analytics schema
# MAGIC USE SCHEMA analytics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Portfolio risk summary view
# MAGIC CREATE OR REPLACE VIEW portfolio_risk_summary AS
# MAGIC SELECT 
# MAGIC   assessment_date,
# MAGIC   COUNT(*) as total_locations,
# MAGIC   AVG(combined_risk_score) as avg_risk_score,
# MAGIC   MAX(combined_risk_score) as max_risk_score,
# MAGIC   STDDEV(combined_risk_score) as risk_score_std,
# MAGIC   AVG(combined_premium_multiplier) as avg_premium_multiplier,
# MAGIC   SUM(CASE WHEN overall_risk_level IN ('high', 'very_high') THEN 1 ELSE 0 END) as high_risk_locations,
# MAGIC   SUM(CASE WHEN primary_risk_factor = 'drought' THEN 1 ELSE 0 END) as drought_primary_locations,
# MAGIC   SUM(CASE WHEN primary_risk_factor = 'flood' THEN 1 ELSE 0 END) as flood_primary_locations
# MAGIC FROM risk_models.combined_risk_assessments
# MAGIC GROUP BY assessment_date
# MAGIC COMMENT 'Portfolio-level risk summary for executive reporting';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Geographic risk concentration view
# MAGIC CREATE OR REPLACE VIEW geographic_risk_concentration AS
# MAGIC SELECT 
# MAGIC   h3_cell_7,
# MAGIC   latitude,
# MAGIC   longitude,
# MAGIC   COUNT(*) as assessment_count,
# MAGIC   AVG(combined_risk_score) as avg_risk_score,
# MAGIC   MAX(combined_risk_score) as max_risk_score,
# MAGIC   COUNT(CASE WHEN overall_risk_level IN ('high', 'very_high') THEN 1 END) as high_risk_days,
# MAGIC   AVG(combined_premium_multiplier) as avg_premium_multiplier
# MAGIC FROM risk_models.combined_risk_assessments
# MAGIC WHERE assessment_date >= CURRENT_DATE() - INTERVAL 90 DAYS
# MAGIC GROUP BY h3_cell_7, latitude, longitude
# MAGIC COMMENT 'Geographic concentration of risk for spatial analysis';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permissions Setup
# MAGIC 
# MAGIC Setting up proper permissions for different user groups. 
# MAGIC 
# MAGIC **Note:** Adjust group names according to your organization's structure.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant permissions (adjust as needed for your organization)
# MAGIC GRANT USAGE ON CATALOG ${catalog_name} TO `domain-users`;
# MAGIC GRANT USAGE ON SCHEMA ${catalog_name}.raw_data TO `domain-users`;
# MAGIC GRANT USAGE ON SCHEMA ${catalog_name}.processed_data TO `domain-users`;
# MAGIC GRANT USAGE ON SCHEMA ${catalog_name}.risk_models TO `domain-users`;
# MAGIC GRANT USAGE ON SCHEMA ${catalog_name}.analytics TO `domain-users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant read access to analytics teams
# MAGIC GRANT SELECT ON SCHEMA ${catalog_name}.analytics TO `analytics-team`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant write access to data engineering teams
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA ${catalog_name}.raw_data TO `data-engineering-team`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA ${catalog_name}.processed_data TO `data-engineering-team`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA ${catalog_name}.risk_models TO `data-engineering-team`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## H3 Geospatial Index Population
# MAGIC 
# MAGIC The tables include H3 cell columns for geospatial indexing. Use the following queries to populate these columns after data ingestion:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable H3 Functions
# MAGIC 
# MAGIC First, ensure H3 functions are available in your Databricks workspace. If not available, you can use alternative geospatial indexing.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Update H3 cells in AccuWeather current conditions table
# MAGIC -- Use this pattern after inserting data into your tables
# MAGIC /*
# MAGIC UPDATE ${catalog_name}.raw_data.accuweather_current_conditions 
# MAGIC SET 
# MAGIC   h3_cell_7 = h3_longlattostring(longitude, latitude, 7),
# MAGIC   h3_cell_8 = h3_longlattostring(longitude, latitude, 8)
# MAGIC WHERE h3_cell_7 IS NULL OR h3_cell_8 IS NULL;
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternative: Use ST_H3_LONGLATTOSTRING (if available)
# MAGIC 
# MAGIC If the above function is not available, try the ST_ prefixed version:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Alternative H3 function approach
# MAGIC /*
# MAGIC UPDATE ${catalog_name}.raw_data.accuweather_current_conditions 
# MAGIC SET 
# MAGIC   h3_cell_7 = ST_H3_LONGLATTOSTRING(longitude, latitude, 7),
# MAGIC   h3_cell_8 = ST_H3_LONGLATTOSTRING(longitude, latitude, 8)
# MAGIC WHERE h3_cell_7 IS NULL OR h3_cell_8 IS NULL;
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** Apply similar UPDATE statements to all tables with H3 columns:
# MAGIC - `accuweather_daily_forecasts`
# MAGIC - `historical_climate_data`
# MAGIC - `climate_observations`
# MAGIC - `climate_aggregations`
# MAGIC - Risk assessment tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation and Summary
# MAGIC 
# MAGIC Let's validate the setup and provide a summary of created objects.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show created schemas
# MAGIC SHOW SCHEMAS IN ${catalog_name};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show tables in raw_data schema
# MAGIC SHOW TABLES IN ${catalog_name}.raw_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show tables in processed_data schema
# MAGIC SHOW TABLES IN ${catalog_name}.processed_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show tables in risk_models schema
# MAGIC SHOW TABLES IN ${catalog_name}.risk_models;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show views in analytics schema
# MAGIC SHOW TABLES IN ${catalog_name}.analytics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete! 🎉
# MAGIC 
# MAGIC Your Unity Catalog schema for the Climate Risk Insurance Models has been successfully set up with:
# MAGIC 
# MAGIC ### Catalogs & Schemas
# MAGIC - **Catalog:** `${catalog_name}`
# MAGIC - **Schemas:** `raw_data`, `processed_data`, `risk_models`, `analytics`
# MAGIC 
# MAGIC ### Raw Data Tables (4)
# MAGIC - `accuweather_current_conditions`
# MAGIC - `accuweather_daily_forecasts`
# MAGIC - `historical_climate_data`
# MAGIC - `elevation_data`
# MAGIC 
# MAGIC ### Processed Data Tables (2)
# MAGIC - `climate_observations`
# MAGIC - `climate_aggregations`
# MAGIC 
# MAGIC ### Risk Assessment Tables (3)
# MAGIC - `drought_risk_assessments`
# MAGIC - `flood_risk_assessments`
# MAGIC - `combined_risk_assessments`
# MAGIC 
# MAGIC ### Analytics Views (2)
# MAGIC - `portfolio_risk_summary`
# MAGIC - `geographic_risk_concentration`
# MAGIC 
# MAGIC ### Features Enabled
# MAGIC ✅ **Databricks Liquid Clustering** - Automatic data layout optimization  
# MAGIC ✅ **Predictive Optimization** - ML-driven performance optimization  
# MAGIC ✅ **Change Data Feed** - Track data changes over time  
# MAGIC ✅ **Parameterized Setup** - Easy customization for different environments  
# MAGIC ✅ **H3 Geospatial Indexing** - Prepared for H3 spatial indexing (populate after data ingestion)
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 1. Start ingesting data using the pipeline notebooks
# MAGIC 2. **Populate H3 columns** using the UPDATE statements provided in the H3 section above
# MAGIC 3. Run the risk assessment models
# MAGIC 4. Create dashboards using the analytics views
# MAGIC 5. Monitor performance and adjust clustering as needed
