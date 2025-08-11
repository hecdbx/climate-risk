# Step-by-Step Guide: Deploying Climate Risk Models on Databricks with Lakeflow Declarative Pipelines

*A comprehensive tutorial for building and deploying production-ready climate risk assessment pipelines using Databricks Runtime 17, Unity Catalog, and AccuWeather API integration.*

---

## 🌟 Introduction

Climate risk assessment is becoming increasingly critical for insurance companies as extreme weather events become more frequent and severe. In this blog post, I'll walk you through deploying a complete climate risk modeling solution on Databricks using the latest features including **Lakeflow Declarative Pipelines**, **Unity Catalog**, and **DBR 17**.

By the end of this tutorial, you'll have a production-ready system that:
- ✅ Ingests real-time weather data from AccuWeather API
- ✅ Processes historical climate data from multiple sources
- ✅ Generates drought and flood risk assessments
- ✅ Provides automated monitoring and alerting
- ✅ Follows enterprise governance best practices

## 🏗️ Architecture Overview

Our solution uses a modern **Bronze-Silver-Gold** data architecture:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Bronze Layer  │    │   Silver Layer   │    │   Gold Layer    │
│                 │    │                  │    │                 │
│ • Raw AccuWeather│───▶│ • Unified Climate│───▶│ • Risk Models   │
│ • Raw NOAA Data │    │ • Quality Checks │    │ • Analytics     │
│ • Raw ERA5 Data │    │ • H3 Indexing    │    │ • Dashboards    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 📋 Prerequisites

Before we begin, ensure you have:

- **Databricks Workspace** (AWS, Azure, or GCP)
- **Unity Catalog** enabled in your workspace
- **Cluster with DBR 17.0+** (we'll create this)
- **AccuWeather API Key** ([sign up here](https://developer.accuweather.com/))
- **Git repository access** (we'll use GitHub)
- **Appropriate permissions** for catalog and schema creation

## 🚀 Step 1: Clone the Repository

First, let's get our project code into the Databricks workspace.

### 1.1 Import from Git (Recommended)

1. Navigate to your Databricks workspace
2. Go to **Workspace** → **Repos**
3. Click **Add Repo**
4. Enter the repository URL: `https://github.com/hecdbx/climate-risk.git`
5. Choose **main** branch
6. Click **Create Repo**

### 1.2 Alternative: Upload Files

If you prefer to upload manually:
1. Download the repository as ZIP
2. Extract and upload each file to your workspace
3. Maintain the directory structure as shown in the repo

## 🛠️ Step 2: Create and Configure Databricks Cluster

### 2.1 Create a New Cluster

1. Go to **Compute** in your Databricks workspace
2. Click **Create Cluster**
3. Configure with these settings:

```yaml
Cluster Name: climate-risk-dbr17
Databricks Runtime: 17.0+ (includes Apache Spark 3.5.x, Scala 2.12)
Worker Type: Standard_DS4_v2 (or equivalent)
Driver Type: Standard_DS4_v2 (or equivalent)
Workers: 2-4 (adjust based on your data volume)
Auto Termination: 60 minutes
```

### 2.2 Advanced Configuration

In the **Advanced Options** section:

**Spark Config:**
```
spark.sql.adaptive.enabled true
spark.databricks.delta.optimizeWrite.enabled true
spark.databricks.photon.enabled true
spark.databricks.unity.enabled true
spark.sql.execution.arrow.pyspark.enabled true
```

**Environment Variables:**
```
PIPELINE_ENV=production
CATALOG_NAME=climate_risk_catalog
```

### 2.3 Install Required Libraries

Add these libraries to your cluster:

**PyPI Packages:**
```
requests>=2.31.0
pandas>=2.1.4
numpy>=1.25.2
geopandas>=0.14.1
plotly>=5.17.0
scikit-learn>=1.3.2
```

**Note:** PySpark is pre-installed in DBR 17, so don't add it separately.

## 🏛️ Step 3: Set Up Unity Catalog

Unity Catalog provides governance, security, and discovery for all your data assets.

### 3.1 Create the Catalog and Schemas

Navigate to the **SQL Editor** in Databricks and run:

```sql
-- Create main catalog
CREATE CATALOG IF NOT EXISTS climate_risk_catalog
COMMENT 'Unified catalog for climate risk insurance models and data';

USE CATALOG climate_risk_catalog;

-- Create schemas for data organization
CREATE SCHEMA IF NOT EXISTS raw_data
COMMENT 'Raw ingested data from external sources';

CREATE SCHEMA IF NOT EXISTS processed_data
COMMENT 'Cleaned and transformed climate data';

CREATE SCHEMA IF NOT EXISTS risk_models
COMMENT 'Risk assessment models and computed scores';

CREATE SCHEMA IF NOT EXISTS analytics
COMMENT 'Analytical views and aggregated data';
```

### 3.2 Set Up Permissions

Configure access control based on your organization's needs:

```sql
-- Grant permissions to data teams
GRANT USAGE ON CATALOG climate_risk_catalog TO `domain-users`;
GRANT USAGE ON SCHEMA climate_risk_catalog.raw_data TO `domain-users`;
GRANT USAGE ON SCHEMA climate_risk_catalog.processed_data TO `domain-users`;

-- Analytics team read access
GRANT SELECT ON SCHEMA climate_risk_catalog.analytics TO `analytics-team`;

-- Data engineering write access
GRANT ALL PRIVILEGES ON SCHEMA climate_risk_catalog.raw_data TO `data-engineering-team`;
```

### 3.3 Create Base Tables

Run the complete Unity Catalog schema setup:

```sql
%run /Repos/climate-risk/insurance-climate-risk/config/unity_catalog_schema.sql
```

This creates all necessary tables with proper schemas, partitioning, and optimization settings.

## 🔐 Step 4: Configure Secrets and API Keys

### 4.1 Create Secret Scope

Use Databricks CLI or the UI to create a secret scope:

**Using Databricks CLI:**
```bash
databricks secrets create-scope --scope climate-risk
```

**Using UI:**
1. Go to **User Settings** → **Developer** → **Access Tokens**
2. Navigate to `https://<databricks-instance>/secrets/createScope`
3. Create scope named `climate-risk`

### 4.2 Add AccuWeather API Key

Store your AccuWeather API key securely:

```bash
databricks secrets put --scope climate-risk --key accuweather-api-key
```

Or through the UI at: `https://<databricks-instance>/secrets/put`

## 📊 Step 5: Deploy Lakeflow Declarative Pipelines

Now we'll deploy our data pipelines using Lakeflow Declarative Pipelines.

### 5.1 Create AccuWeather Ingestion Pipeline

1. Go to **Workflows** → **Delta Live Tables**
2. Click **Create Pipeline**
3. Configure the pipeline:

```json
{
  "name": "accuweather_ingestion_pipeline",
  "libraries": [
    {
      "notebook": {
        "path": "/Repos/climate-risk/insurance-climate-risk/lakeflow/01_accuweather_ingestion_pipeline"
      }
    }
  ],
  "target": "climate_risk_catalog.processed_data",
  "configuration": {
    "accuweather.api.key": "{{secrets/climate-risk/accuweather-api-key}}"
  },
  "continuous": false,
  "development": true
}
```

4. Click **Create**

### 5.2 Create Historical Data Processing Pipeline

Repeat the process for historical data:

```json
{
  "name": "historical_climate_processing",
  "libraries": [
    {
      "notebook": {
        "path": "/Repos/climate-risk/insurance-climate-risk/lakeflow/02_historical_data_processing_pipeline"
      }
    }
  ],
  "target": "climate_risk_catalog.processed_data",
  "continuous": false,
  "development": true
}
```

### 5.3 Test Pipeline Execution

1. Select your AccuWeather pipeline
2. Click **Start**
3. Monitor the execution in the **Pipeline Details** view
4. Check data quality metrics and expectations

Expected output:
- ✅ Bronze tables with raw AccuWeather data
- ✅ Silver tables with cleaned and enriched data
- ✅ Gold tables with analytics-ready climate observations

## 🔄 Step 6: Set Up Workflow Orchestration

### 6.1 Create Lakeflow Jobs Workflow

1. Go to **Workflows** → **Jobs**
2. Click **Create Job**
3. Import the workflow configuration:

```python
%run /Repos/climate-risk/insurance-climate-risk/lakeflow/climate_risk_workflow
```

### 6.2 Configure Job Tasks

The workflow includes these key tasks:
1. **Unity Catalog Setup** - Initialize schemas and tables
2. **AccuWeather Ingestion** - Real-time weather data
3. **Historical Processing** - Multi-source climate data
4. **Risk Modeling** - Drought and flood assessments
5. **Analytics Materialization** - Generate insights
6. **Quality Validation** - Data quality checks
7. **Monitoring** - Performance and drift detection

### 6.3 Set Schedule

Configure the job to run daily:

```json
{
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC"
  }
}
```

## 🧪 Step 7: Execute Risk Models

### 7.1 Run Drought Risk Model

Open and execute the drought risk notebook:

```python
%run /Repos/climate-risk/insurance-climate-risk/notebooks/01_drought_risk_model
```

This notebook will:
- ✅ Load processed climate data from Unity Catalog
- ✅ Calculate drought indicators (SPI, PDSI, soil moisture)
- ✅ Generate risk scores by location and date
- ✅ Store results in `climate_risk_catalog.risk_models.drought_risk_assessments`

### 7.2 Run Flood Risk Model

Execute the flood risk assessment:

```python
%run /Repos/climate-risk/insurance-climate-risk/notebooks/02_flood_risk_model
```

This generates:
- ✅ Elevation-based flood risk factors
- ✅ Precipitation intensity analysis
- ✅ Combined flood risk scores
- ✅ Insurance premium recommendations

### 7.3 Generate Combined Risk Assessment

Run the combined assessment notebook:

```python
%run /Repos/climate-risk/insurance-climate-risk/notebooks/combined_risk_assessment
```

This creates portfolio-level risk summaries and geographic concentration analysis.

## 📊 Step 8: Create Visualizations and Dashboards

### 8.1 Run Visualization Notebook

Execute the visualization notebook:

```python
%run /Repos/climate-risk/insurance-climate-risk/notebooks/03_risk_visualization
```

This creates:
- 🗺️ Interactive risk maps with H3 hexagonal cells
- 📈 Time-series charts of risk trends
- 📊 Portfolio risk distribution analysis
- 🎯 Geographic concentration visualizations

### 8.2 Create Databricks SQL Dashboard

1. Go to **SQL** → **Dashboards**
2. Click **Create Dashboard**
3. Add these visualizations:

**Key Metrics:**
```sql
SELECT 
  COUNT(DISTINCT h3_cell_7) as locations_monitored,
  AVG(combined_risk_score) as avg_risk_score,
  COUNT(*) as total_assessments,
  SUM(CASE WHEN overall_risk_level = 'high' THEN 1 ELSE 0 END) as high_risk_locations
FROM climate_risk_catalog.risk_models.combined_risk_assessments
WHERE assessment_date = CURRENT_DATE()
```

**Risk Trends:**
```sql
SELECT 
  assessment_date,
  AVG(drought_risk_score) as avg_drought_risk,
  AVG(flood_risk_score) as avg_flood_risk,
  AVG(combined_risk_score) as avg_combined_risk
FROM climate_risk_catalog.risk_models.combined_risk_assessments
WHERE assessment_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY assessment_date
ORDER BY assessment_date
```

## 🔍 Step 9: Set Up Monitoring and Alerting

### 9.1 Create Data Quality Monitors

Set up automated data quality monitoring:

```python
# Create data quality alerts
dbutils.jobs.create({
  "name": "climate_risk_data_quality_monitor",
  "tasks": [{
    "task_key": "data_freshness_check",
    "notebook_task": {
      "notebook_path": "/Repos/climate-risk/insurance-climate-risk/monitoring/data_quality_monitor"
    }
  }],
  "schedule": {
    "quartz_cron_expression": "0 */30 * * * ?"  # Every 30 minutes
  }
})
```

### 9.2 Configure Alerts

Set up email and Slack notifications:

```json
{
  "email_notifications": {
    "on_failure": ["data-team@company.com"],
    "on_success": ["data-team@company.com"]
  },
  "webhook_notifications": {
    "on_failure": [{"id": "slack-webhook-url"}]
  }
}
```

### 9.3 Pipeline Health Dashboard

Create a monitoring view:

```sql
CREATE OR REPLACE VIEW climate_risk_catalog.analytics.pipeline_health AS
SELECT 
  'Climate Risk Pipeline' as pipeline_name,
  MAX(processing_timestamp) as last_run,
  COUNT(*) as records_today,
  AVG(data_quality_score) as avg_quality,
  CASE 
    WHEN MAX(processing_timestamp) >= CURRENT_TIMESTAMP() - INTERVAL 6 HOURS THEN 'HEALTHY'
    ELSE 'CRITICAL'
  END as status
FROM climate_risk_catalog.processed_data.climate_observations
WHERE DATE(processing_timestamp) = CURRENT_DATE()
```

## 🔐 Step 10: Production Deployment Best Practices

### 10.1 Environment Configuration

Set up separate environments:

**Development:**
```json
{
  "catalog": "climate_risk_dev_catalog",
  "continuous": false,
  "development": true
}
```

**Production:**
```json
{
  "catalog": "climate_risk_catalog", 
  "continuous": true,
  "development": false,
  "photon": true
}
```

### 10.2 Security Hardening

1. **Enable audit logging:**
```sql
ALTER CATALOG climate_risk_catalog SET TBLPROPERTIES ('audit.enabled' = 'true');
```

2. **Row-level security (if needed):**
```sql
CREATE ROW ACCESS POLICY risk_data_policy ON climate_risk_catalog.risk_models.combined_risk_assessments
GRANT TO ('data-analysts') 
FILTER USING (assessment_date >= CURRENT_DATE() - INTERVAL 90 DAYS);
```

3. **Column masking for sensitive data:**
```sql
CREATE MASK premium_multiplier_mask ON climate_risk_catalog.risk_models.combined_risk_assessments
RETURN CASE 
  WHEN IS_MEMBER('senior-analysts') THEN combined_premium_multiplier
  ELSE NULL 
END;
```

### 10.3 Cost Optimization

Configure auto-scaling and spot instances:

```json
{
  "autoscale": {
    "min_workers": 1,
    "max_workers": 8
  },
  "aws_attributes": {
    "first_on_demand": 1,
    "availability": "SPOT_WITH_FALLBACK",
    "spot_bid_price_percent": 50
  }
}
```

## 📈 Step 11: Performance Optimization

### 11.1 Enable Delta Optimizations

```sql
-- Enable auto-optimize for better performance
ALTER TABLE climate_risk_catalog.processed_data.climate_observations 
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

### 11.2 Optimize Table Partitioning

```sql
-- Optimize historical data table
OPTIMIZE climate_risk_catalog.processed_data.climate_observations
ZORDER BY (h3_cell_7, observation_timestamp);
```

### 11.3 Configure Photon Engine

Ensure Photon is enabled in your cluster configuration for 5x performance improvement on analytical workloads.

## 🧪 Step 12: Testing and Validation

### 12.1 Data Quality Tests

Run comprehensive data quality checks:

```python
%run /Repos/climate-risk/insurance-climate-risk/tests/data_quality_tests
```

### 12.2 Model Validation

Validate risk model outputs:

```python
# Test drought risk model
drought_results = spark.sql("""
  SELECT COUNT(*) as total_assessments,
         AVG(drought_risk_score) as avg_score,
         MIN(assessment_date) as earliest_date,
         MAX(assessment_date) as latest_date
  FROM climate_risk_catalog.risk_models.drought_risk_assessments
""")

drought_results.display()
```

### 12.3 End-to-End Pipeline Test

Execute a full pipeline run and validate all outputs:

```python
# Trigger complete pipeline
dbutils.jobs.run_now(job_id="your_climate_risk_job_id")

# Monitor execution
import time
run_id = dbutils.jobs.get_run_output(run_id)
while run_id['life_cycle_state'] in ['PENDING', 'RUNNING']:
    time.sleep(30)
    print(f"Pipeline status: {run_id['life_cycle_state']}")
```

## 📊 Step 13: Business Intelligence Integration

### 13.1 Connect to BI Tools

Your Unity Catalog tables are now accessible via:

- **Tableau:** Connect using Databricks JDBC/ODBC driver
- **Power BI:** Use Databricks connector
- **Looker:** Configure Databricks connection
- **SQL Editor:** Built-in Databricks SQL for ad-hoc analysis

### 13.2 Create Executive Dashboard

Build a high-level executive dashboard:

```sql
-- Executive Risk Summary
SELECT 
  'Portfolio Overview' as metric_type,
  COUNT(DISTINCT h3_cell_7) as locations_monitored,
  AVG(combined_risk_score) as portfolio_avg_risk,
  COUNT(CASE WHEN overall_risk_level = 'high' THEN 1 END) as high_risk_locations,
  SUM(total_risk_exposure) as total_portfolio_exposure
FROM climate_risk_catalog.risk_models.combined_risk_assessments
WHERE assessment_date = CURRENT_DATE()
```

## 🎯 Step 14: Production Rollout

### 14.1 Gradual Rollout Strategy

1. **Week 1:** Deploy to development environment
2. **Week 2:** Run parallel processing with existing systems
3. **Week 3:** Limited production deployment (10% of portfolio)
4. **Week 4:** Full production deployment

### 14.2 Success Metrics

Monitor these KPIs:
- **Data Freshness:** < 6 hours lag
- **Data Quality Score:** > 95%
- **Pipeline Reliability:** > 99.5% uptime
- **Processing Time:** < 2 hours for full refresh
- **Cost per Assessment:** Target < $0.01 per location

### 14.3 Documentation and Training

1. **Create user documentation** for business users
2. **Train data analysts** on the new dashboards  
3. **Document troubleshooting procedures** for operations team
4. **Set up knowledge transfer** sessions

## 🎉 Conclusion

Congratulations! You've successfully deployed a production-ready climate risk assessment platform on Databricks. Your solution now includes:

✅ **Real-time weather data ingestion** from AccuWeather API  
✅ **Automated data processing** with Lakeflow Declarative Pipelines  
✅ **Enterprise governance** with Unity Catalog  
✅ **Advanced risk modeling** for drought and flood assessment  
✅ **Interactive visualizations** and executive dashboards  
✅ **Comprehensive monitoring** and alerting  
✅ **Production-grade security** and cost optimization  

## 🔗 Additional Resources

- **[Databricks Lakeflow Documentation](https://docs.databricks.com/en/data-engineering/lakeflow/index.html)**
- **[Unity Catalog Best Practices](https://docs.databricks.com/en/data-governance/unity-catalog/best-practices.html)**  
- **[AccuWeather API Documentation](https://developer.accuweather.com/apis)**
- **[Delta Live Tables Guide](https://docs.databricks.com/en/data-engineering/delta-live-tables/index.html)**
- **[Project Repository](https://github.com/hecdbx/climate-risk)**

## 💬 Questions?

Feel free to reach out with questions about deploying this solution in your environment. The modular architecture makes it easy to adapt for different use cases and data sources.

---

*This blog post demonstrates enterprise-grade deployment practices using the latest Databricks features. The complete code and configuration files are available in the [GitHub repository](https://github.com/hecdbx/climate-risk).*
