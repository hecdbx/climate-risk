-- Unity Catalog Schema Configuration for Climate Risk Insurance Models
-- This script sets up the complete schema structure for climate data ingestion and processing

-- Parameter for catalog name - modify this value as needed
SET VAR catalog_name = 'demo_hc';

-- Create catalog for climate risk data
CREATE CATALOG IF NOT EXISTS ${catalog_name}
COMMENT 'Unified catalog for climate risk insurance models and data';

-- Use the catalog
USE CATALOG ${catalog_name};

-- Create schemas for different data domains
CREATE SCHEMA IF NOT EXISTS raw_data
COMMENT 'Raw ingested data from external sources (AccuWeather, NOAA, etc.)';

CREATE SCHEMA IF NOT EXISTS processed_data
COMMENT 'Cleaned and transformed climate data';

CREATE SCHEMA IF NOT EXISTS risk_models
COMMENT 'Risk assessment models and computed risk scores';

CREATE SCHEMA IF NOT EXISTS analytics
COMMENT 'Analytical views and aggregated data for reporting';

-- Set up raw data tables for AccuWeather ingestion
USE SCHEMA raw_data;

-- AccuWeather current conditions table
CREATE OR REPLACE TABLE accuweather_current_conditions (
  location_key STRING NOT NULL,
  location_name STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  observation_time TIMESTAMP,
  temperature_celsius DOUBLE,
  temperature_fahrenheit DOUBLE,
  humidity_percent INT,
  pressure_mb DOUBLE,
  wind_speed_kmh DOUBLE,
  wind_direction_degrees INT,
  precipitation_mm DOUBLE,
  weather_text STRING,
  weather_icon INT,
  uv_index INT,
  visibility_km DOUBLE,
  cloud_cover_percent INT,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  h3_cell_7 STRING GENERATED ALWAYS AS (h3_latlng_to_cell_string(latitude, longitude, 7)),
  h3_cell_8 STRING GENERATED ALWAYS AS (h3_latlng_to_cell_string(latitude, longitude, 8))
) 
USING DELTA
CLUSTER BY (DATE(observation_time), location_key)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enablePredictiveOptimization' = 'true'
)
COMMENT 'Real-time current weather conditions from AccuWeather API';

-- AccuWeather daily forecasts table
CREATE OR REPLACE TABLE accuweather_daily_forecasts (
  location_key STRING NOT NULL,
  location_name STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  forecast_date DATE,
  min_temperature_celsius DOUBLE,
  max_temperature_celsius DOUBLE,
  precipitation_probability_percent INT,
  precipitation_amount_mm DOUBLE,
  weather_text STRING,
  weather_icon INT,
  wind_speed_kmh DOUBLE,
  wind_direction_degrees INT,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  h3_cell_7 STRING GENERATED ALWAYS AS (h3_latlng_to_cell_string(latitude, longitude, 7)),
  h3_cell_8 STRING GENERATED ALWAYS AS (h3_latlng_to_cell_string(latitude, longitude, 8))
)
USING DELTA
CLUSTER BY (forecast_date, location_key)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enablePredictiveOptimization' = 'true'
)
COMMENT 'Daily weather forecasts from AccuWeather API';

-- Historical climate data from other sources
CREATE OR REPLACE TABLE historical_climate_data (
  source STRING NOT NULL,
  location_id STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  observation_date DATE,
  temperature_celsius DOUBLE,
  precipitation_mm DOUBLE,
  humidity_percent INT,
  wind_speed_kmh DOUBLE,
  pressure_mb DOUBLE,
  soil_moisture_percent DOUBLE,
  snow_depth_cm DOUBLE,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  h3_cell_7 STRING GENERATED ALWAYS AS (h3_latlng_to_cell_string(latitude, longitude, 7)),
  h3_cell_8 STRING GENERATED ALWAYS AS (h3_latlng_to_cell_string(latitude, longitude, 8))
)
USING DELTA
CLUSTER BY (source, observation_date, location_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enablePredictiveOptimization' = 'true'
)
COMMENT 'Historical climate data from various sources (NOAA, ERA5, etc.)';

-- Elevation and topographic data
CREATE OR REPLACE TABLE elevation_data (
  h3_cell_8 STRING NOT NULL,
  latitude DOUBLE,
  longitude DOUBLE,
  elevation_m DOUBLE,
  slope_degrees DOUBLE,
  aspect_degrees DOUBLE,
  curvature DOUBLE,
  drainage_area_km2 DOUBLE,
  distance_to_water_m DOUBLE,
  land_cover_type STRING,
  soil_type STRING,
  in_floodplain BOOLEAN,
  data_source STRING,
  last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enablePredictiveOptimization' = 'true'
)
COMMENT 'Elevation and topographic data for flood risk modeling';

-- Set up processed data schema
USE SCHEMA processed_data;

-- Unified climate observations (cleaned and standardized)
CREATE OR REPLACE TABLE climate_observations (
  h3_cell_7 STRING NOT NULL,
  h3_cell_8 STRING NOT NULL,
  latitude DOUBLE,
  longitude DOUBLE,
  observation_timestamp TIMESTAMP,
  temperature_celsius DOUBLE,
  precipitation_mm DOUBLE,
  humidity_percent INT,
  wind_speed_kmh DOUBLE,
  pressure_mb DOUBLE,
  weather_conditions STRING,
  data_source STRING,
  data_quality_score DOUBLE,
  processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (DATE(observation_timestamp), data_source, h3_cell_7)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enablePredictiveOptimization' = 'true'
)
COMMENT 'Standardized climate observations from all sources';

-- Climate aggregations for risk modeling
CREATE OR REPLACE TABLE climate_aggregations (
  h3_cell_7 STRING NOT NULL,
  latitude DOUBLE,
  longitude DOUBLE,
  aggregation_date DATE,
  avg_temperature_celsius DOUBLE,
  min_temperature_celsius DOUBLE,
  max_temperature_celsius DOUBLE,
  total_precipitation_mm DOUBLE,
  max_precipitation_intensity_mm DOUBLE,
  avg_humidity_percent DOUBLE,
  avg_wind_speed_kmh DOUBLE,
  precipitation_days INT,
  dry_days INT,
  extreme_weather_events INT,
  data_completeness_percent DOUBLE,
  processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (aggregation_date, h3_cell_7)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enablePredictiveOptimization' = 'true'
)
COMMENT 'Daily aggregated climate data by H3 cell for risk modeling';

-- Set up risk models schema
USE SCHEMA risk_models;

-- Drought risk assessments
CREATE OR REPLACE TABLE drought_risk_assessments (
  h3_cell_7 STRING NOT NULL,
  latitude DOUBLE,
  longitude DOUBLE,
  assessment_date DATE,
  drought_risk_score DOUBLE,
  drought_risk_level STRING,
  precipitation_deficit_ratio DOUBLE,
  temperature_anomaly DOUBLE,
  soil_moisture_index DOUBLE,
  vegetation_health_index DOUBLE,
  spi_30_day DOUBLE,
  spi_90_day DOUBLE,
  pdsi_value DOUBLE,
  historical_drought_frequency DOUBLE,
  insurance_risk_class STRING,
  premium_multiplier DOUBLE,
  recommended_action STRING,
  model_version STRING,
  confidence_score DOUBLE,
  processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (assessment_date, h3_cell_7, drought_risk_level)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enablePredictiveOptimization' = 'true'
)
COMMENT 'Drought risk assessments by location and date';

-- Flood risk assessments
CREATE OR REPLACE TABLE flood_risk_assessments (
  h3_cell_8 STRING NOT NULL,
  latitude DOUBLE,
  longitude DOUBLE,
  assessment_date DATE,
  flood_risk_score DOUBLE,
  flood_risk_level STRING,
  elevation_risk_factor DOUBLE,
  slope_risk_factor DOUBLE,
  precipitation_intensity_risk DOUBLE,
  drainage_capacity_score DOUBLE,
  historical_flood_frequency DOUBLE,
  estimated_return_period_years INT,
  flood_depth_estimate_m DOUBLE,
  insurance_flood_zone STRING,
  premium_multiplier DOUBLE,
  coverage_recommendation STRING,
  model_version STRING,
  confidence_score DOUBLE,
  processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (assessment_date, h3_cell_8, flood_risk_level)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enablePredictiveOptimization' = 'true'
)
COMMENT 'Flood risk assessments by location and date';

-- Combined climate risk assessments
CREATE OR REPLACE TABLE combined_risk_assessments (
  h3_cell_7 STRING NOT NULL,
  latitude DOUBLE,
  longitude DOUBLE,
  assessment_date DATE,
  drought_risk_score DOUBLE,
  flood_risk_score DOUBLE,
  combined_risk_score DOUBLE,
  overall_risk_level STRING,
  primary_risk_factor STRING,
  combined_premium_multiplier DOUBLE,
  total_risk_exposure DOUBLE,
  portfolio_concentration_risk DOUBLE,
  risk_trend_30_day STRING,
  risk_trend_90_day STRING,
  next_assessment_date DATE,
  model_version STRING,
  confidence_score DOUBLE,
  processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (assessment_date, overall_risk_level, h3_cell_7)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enablePredictiveOptimization' = 'true'
)
COMMENT 'Combined climate risk assessments for insurance applications';

-- Set up analytics schema
USE SCHEMA analytics;

-- Portfolio risk summary view
CREATE OR REPLACE VIEW portfolio_risk_summary AS
SELECT 
  assessment_date,
  COUNT(*) as total_locations,
  AVG(combined_risk_score) as avg_risk_score,
  MAX(combined_risk_score) as max_risk_score,
  STDDEV(combined_risk_score) as risk_score_std,
  AVG(combined_premium_multiplier) as avg_premium_multiplier,
  SUM(CASE WHEN overall_risk_level IN ('high', 'very_high') THEN 1 ELSE 0 END) as high_risk_locations,
  SUM(CASE WHEN primary_risk_factor = 'drought' THEN 1 ELSE 0 END) as drought_primary_locations,
  SUM(CASE WHEN primary_risk_factor = 'flood' THEN 1 ELSE 0 END) as flood_primary_locations
FROM risk_models.combined_risk_assessments
GROUP BY assessment_date
COMMENT 'Portfolio-level risk summary for executive reporting';

-- Geographic risk concentration view
CREATE OR REPLACE VIEW geographic_risk_concentration AS
SELECT 
  h3_cell_7,
  latitude,
  longitude,
  COUNT(*) as assessment_count,
  AVG(combined_risk_score) as avg_risk_score,
  MAX(combined_risk_score) as max_risk_score,
  COUNT(CASE WHEN overall_risk_level IN ('high', 'very_high') THEN 1 END) as high_risk_days,
  AVG(combined_premium_multiplier) as avg_premium_multiplier
FROM risk_models.combined_risk_assessments
WHERE assessment_date >= CURRENT_DATE() - INTERVAL 90 DAYS
GROUP BY h3_cell_7, latitude, longitude
COMMENT 'Geographic concentration of risk for spatial analysis';

-- Grant permissions (adjust as needed for your organization)
GRANT USAGE ON CATALOG ${catalog_name} TO `domain-users`;
GRANT USAGE ON SCHEMA ${catalog_name}.raw_data TO `domain-users`;
GRANT USAGE ON SCHEMA ${catalog_name}.processed_data TO `domain-users`;
GRANT USAGE ON SCHEMA ${catalog_name}.risk_models TO `domain-users`;
GRANT USAGE ON SCHEMA ${catalog_name}.analytics TO `domain-users`;

-- Grant read access to analytics teams
GRANT SELECT ON SCHEMA ${catalog_name}.analytics TO `analytics-team`;

-- Grant write access to data engineering teams
GRANT ALL PRIVILEGES ON SCHEMA ${catalog_name}.raw_data TO `data-engineering-team`;
GRANT ALL PRIVILEGES ON SCHEMA ${catalog_name}.processed_data TO `data-engineering-team`;
GRANT ALL PRIVILEGES ON SCHEMA ${catalog_name}.risk_models TO `data-engineering-team`;
