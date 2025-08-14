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
# MAGIC Set the catalog name parameter using Python variables. Modify the value as needed for your environment.

# COMMAND ----------

# Set parameter values - modify as needed for your environment
catalog_name = "demo_hc"
print(f"Using catalog: {catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog and Schema Creation
# MAGIC
# MAGIC Creating the main catalog and data domain schemas.

# COMMAND ----------

# Create catalog for climate risk data
spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {catalog_name}
COMMENT 'Unified catalog for climate risk insurance models and data'
""")

# COMMAND ----------

# Use the catalog
spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create unified climate risk schema for all data and models
# MAGIC CREATE SCHEMA IF NOT EXISTS climate_risk
# MAGIC COMMENT 'Unified schema for all climate risk data: raw, processed, models, and analytics';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Volume Creation
# MAGIC
# MAGIC Creating a single volume with organized directories instead of deprecated /mnt paths.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create single volume for all climate data storage needs
# MAGIC CREATE VOLUME IF NOT EXISTS climate_risk.data_volume
# MAGIC COMMENT 'Unified volume for all climate risk data: raw, processed, models, and pipeline artifacts';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume Directory Structure Setup
# MAGIC
# MAGIC Creating organized directories within the volume for different data types.

# COMMAND ----------

import os

# Define volume path and directory structure
volume_path = f"/Volumes/{catalog_name}/climate_risk/data_volume"

# Directory structure for organized data storage
directories = [
    "raw_data",           # Raw climate data files from external sources
    "processed_data",     # Processed and cleaned data files  
    "model_artifacts",    # ML model artifacts and checkpoints
    "pipeline_checkpoints", # Delta Live Tables pipeline checkpoints
    "staging",           # Temporary staging area for data processing
    "analytics"          # Analytics outputs and reports
]

print(f"📁 Setting up directory structure in volume: {volume_path}")
print("=" * 60)

# Create directories if they don't exist
for directory in directories:
    dir_path = os.path.join(volume_path, directory)
    try:
        # Use dbutils to create directories in Unity Catalog volume
        dbutils.fs.mkdirs(dir_path)
        print(f"✅ Created directory: {directory}/")
    except Exception as e:
        # Directory might already exist, which is fine
        if "already exists" in str(e).lower() or "fileexists" in str(e).lower():
            print(f"✅ Directory exists: {directory}/")
        else:
            print(f"⚠️  Warning creating {directory}/: {str(e)}")

print("\n📂 Volume structure:")
try:
    # List the volume contents to verify structure
    contents = dbutils.fs.ls(volume_path)
    for item in contents:
        print(f"   📁 {item.name}")
except Exception as e:
    print(f"   ⚠️  Could not list volume contents: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Climate Risk Data Tables
# MAGIC
# MAGIC Setting up tables in a unified schema with staging, processed, and analytics layers:
# MAGIC - **Staging Tables**: Raw data ingestion with minimal transformation
# MAGIC - **Processed Tables**: Cleaned and standardized data for analysis
# MAGIC - **Analytics Tables**: Risk models and business intelligence views

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set up climate risk schema for all data
# MAGIC USE SCHEMA climate_risk;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER SCHEMA climate_risk ENABLE PREDICTIVE OPTIMIZATION;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Staging: Raw AccuWeather current conditions
# MAGIC CREATE OR REPLACE TABLE staging_accuweather_current_conditions (
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
# MAGIC COMMENT 'Staging: Real-time current weather conditions from AccuWeather API';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Staging: Raw AccuWeather daily forecasts
# MAGIC CREATE OR REPLACE TABLE staging_accuweather_daily_forecasts (
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
# MAGIC COMMENT 'Staging: Daily weather forecasts from AccuWeather API';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Staging: Historical climate data from other sources
# MAGIC CREATE OR REPLACE TABLE staging_historical_climate_data (
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
# MAGIC   'delta.feature.allowColumnDefaults' = 'enabled'
# MAGIC )
# MAGIC COMMENT 'Staging: Historical climate data from various sources (NOAA, ERA5, etc.)';

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
# MAGIC   'delta.feature.allowColumnDefaults' = 'enabled'
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
# MAGIC ALTER SCHEMA processed_data ENABLE PREDICTIVE OPTIMIZATION;

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
# MAGIC CLUSTER BY (observation_timestamp, data_source, h3_cell_7)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.feature.allowColumnDefaults' = 'enabled'
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
# MAGIC   'delta.feature.allowColumnDefaults' = 'enabled'
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
# MAGIC ALTER SCHEMA risk_models ENABLE PREDICTIVE OPTIMIZATION;

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
# MAGIC   'delta.feature.allowColumnDefaults' = 'enabled'
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
# MAGIC   'delta.feature.allowColumnDefaults' = 'enabled'
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
# MAGIC   'delta.feature.allowColumnDefaults' = 'enabled'
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
# MAGIC ALTER SCHEMA analytics ENABLE PREDICTIVE OPTIMIZATION;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Portfolio risk summary view
# MAGIC CREATE OR REPLACE VIEW portfolio_risk_summary 
# MAGIC COMMENT 'Portfolio-level risk summary for executive reporting' 
# MAGIC AS
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
# MAGIC GROUP BY assessment_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Geographic risk concentration view
# MAGIC CREATE OR REPLACE VIEW geographic_risk_concentration 
# MAGIC COMMENT 'Geographic concentration of risk for spatial analysis'
# MAGIC AS
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
# MAGIC GROUP BY h3_cell_7, latitude, longitude;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permissions Setup
# MAGIC
# MAGIC Setting up proper permissions for different user groups. 
# MAGIC
# MAGIC **Note:** Adjust group names according to your organization's structure.

# COMMAND ----------

# Grant permissions (adjust as needed for your organization)
grant_statements = [
    f"GRANT USAGE ON CATALOG {catalog_name} TO `domain-users`",
    f"GRANT USAGE ON SCHEMA {catalog_name}.raw_data TO `domain-users`",
    f"GRANT USAGE ON SCHEMA {catalog_name}.processed_data TO `domain-users`",
    f"GRANT USAGE ON SCHEMA {catalog_name}.risk_models TO `domain-users`",
    f"GRANT USAGE ON SCHEMA {catalog_name}.analytics TO `domain-users`"
]

for stmt in grant_statements:
    try:
        spark.sql(stmt)
        print(f"✅ {stmt}")
    except Exception as e:
        print(f"⚠️ {stmt} - {str(e)}")

# COMMAND ----------

# Grant read access to analytics teams
try:
    spark.sql(f"GRANT SELECT ON SCHEMA {catalog_name}.analytics TO `analytics-team`")
    print(f"✅ Granted SELECT on {catalog_name}.analytics to analytics-team")
except Exception as e:
    print(f"⚠️ Grant SELECT failed: {str(e)}")

# COMMAND ----------

# Grant write access to data engineering teams
engineering_grants = [
    f"GRANT ALL PRIVILEGES ON SCHEMA {catalog_name}.raw_data TO `data-engineering-team`",
    f"GRANT ALL PRIVILEGES ON SCHEMA {catalog_name}.processed_data TO `data-engineering-team`",
    f"GRANT ALL PRIVILEGES ON SCHEMA {catalog_name}.risk_models TO `data-engineering-team`"
]

for stmt in engineering_grants:
    try:
        spark.sql(stmt)
        print(f"✅ {stmt}")
    except Exception as e:
        print(f"⚠️ {stmt} - {str(e)}")

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
# MAGIC UPDATE {catalog_name}.raw_data.accuweather_current_conditions 
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
# MAGIC UPDATE {catalog_name}.raw_data.accuweather_current_conditions 
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

# Show created schemas
display(spark.sql(f"SHOW SCHEMAS IN {catalog_name}"))

# COMMAND ----------

# Show all tables in unified climate_risk schema
display(spark.sql(f"SHOW TABLES IN {catalog_name}.climate_risk"))

# COMMAND ----------

# Show all volumes in climate_risk schema
display(spark.sql(f"SHOW VOLUMES IN {catalog_name}.climate_risk"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete! 🎉
# MAGIC
# MAGIC Your Unity Catalog schema for the Climate Risk Insurance Models has been successfully set up with:
# MAGIC
# MAGIC ### Catalogs & Schemas
# MAGIC - **Catalog:** Parameterized (configurable)
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Cleanup Functions
# MAGIC
# MAGIC Functions to clean up the schema and volumes when needed. Use with caution!

# COMMAND ----------

def cleanup_climate_risk_schema(catalog_name, confirm_cleanup=False):
    """
    Clean up the climate_risk schema and all its contents.
    
    Args:
        catalog_name (str): Name of the catalog containing the schema
        confirm_cleanup (bool): Must be True to actually execute cleanup
    
    WARNING: This will delete ALL data in the climate_risk schema!
    """
    
    if not confirm_cleanup:
        print("⚠️  WARNING: This will DELETE ALL DATA in the climate_risk schema!")
        print("🗑️  Including:")
        print("   - All staging tables with data")
        print("   - All processed tables with data") 
        print("   - All analytics tables with data")
        print("   - All volumes and stored files")
        print("")
        print("💡 To confirm cleanup, call: cleanup_climate_risk_schema(catalog_name, confirm_cleanup=True)")
        return
    
    print("🗑️  Starting cleanup of climate_risk schema...")
    
    try:
        # Drop the unified volume (this will delete all files and directories)
        print("📁 Dropping unified data volume and all files...")
        try:
            spark.sql(f"DROP VOLUME IF EXISTS {catalog_name}.climate_risk.data_volume")
            print(f"   ✅ Dropped volume: data_volume")
            print(f"   📂 All directories removed: raw_data, processed_data, model_artifacts, pipeline_checkpoints, staging, analytics")
        except Exception as e:
            print(f"   ⚠️  Could not drop volume data_volume: {str(e)}")
        
        # Drop the entire schema (this will drop all tables)
        print("🗄️  Dropping climate_risk schema and all tables...")
        spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.climate_risk CASCADE")
        print("   ✅ Dropped climate_risk schema")
        
        print("🎯 Cleanup completed successfully!")
        print(f"   📋 Catalog '{catalog_name}' remains intact")
        print(f"   🗑️  Schema 'climate_risk' and all contents removed")
        
    except Exception as e:
        print(f"❌ Error during cleanup: {str(e)}")
        print("💡 You may need to manually clean up remaining resources")

# COMMAND ----------

def list_climate_risk_resources(catalog_name):
    """List all resources in the climate_risk schema"""
    
    print(f"📋 Resources in {catalog_name}.climate_risk:")
    print("=" * 50)
    
    try:
        # Check if schema exists
        schemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}").collect()
        schema_exists = any(row.databaseName == 'climate_risk' for row in schemas)
        
        if not schema_exists:
            print("   📭 Schema 'climate_risk' does not exist")
            return
        
        # List tables
        print("🗄️  Tables:")
        tables = spark.sql(f"SHOW TABLES IN {catalog_name}.climate_risk").collect()
        if tables:
            for table in tables:
                print(f"   📊 {table.tableName}")
        else:
            print("   📭 No tables found")
        
        # List volumes and directory structure
        print("\n📁 Volumes:")
        try:
            volumes = spark.sql(f"SHOW VOLUMES IN {catalog_name}.climate_risk").collect()
            if volumes:
                for volume in volumes:
                    print(f"   📦 {volume.volume_name}")
                    # Show directory structure within the volume
                    if volume.volume_name == "data_volume":
                        try:
                            volume_path = f"/Volumes/{catalog_name}/climate_risk/data_volume"
                            contents = dbutils.fs.ls(volume_path)
                            print("      📂 Directories:")
                            for item in contents:
                                print(f"         📁 {item.name}")
                        except Exception as dir_e:
                            print(f"      ⚠️  Could not list volume contents: {str(dir_e)}")
            else:
                print("   📭 No volumes found")
        except Exception as e:
            print(f"   ⚠️  Could not list volumes: {str(e)}")
            
    except Exception as e:
        print(f"❌ Error listing resources: {str(e)}")

# COMMAND ----------

# Display current resources
list_climate_risk_resources(catalog_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup Instructions
# MAGIC
# MAGIC ### To clean up the climate_risk schema when needed:
# MAGIC
# MAGIC ```python
# MAGIC # First, review what will be deleted
# MAGIC list_climate_risk_resources(catalog_name)
# MAGIC
# MAGIC # Then confirm cleanup (WARNING: This deletes all data!)
# MAGIC cleanup_climate_risk_schema(catalog_name, confirm_cleanup=True)
# MAGIC ```
# MAGIC
# MAGIC ### What gets cleaned up:
# MAGIC - ✅ All tables in climate_risk schema
# MAGIC - ✅ All volumes and stored files  
# MAGIC - ✅ All data and metadata
# MAGIC - ❌ Catalog remains intact (only schema is dropped)
# MAGIC
# MAGIC ### After cleanup, you can re-run this notebook to recreate everything fresh!
