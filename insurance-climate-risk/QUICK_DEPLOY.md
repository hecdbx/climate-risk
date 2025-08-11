# ⚡ Quick Deploy Guide: Climate Risk Models on Databricks

*Get your climate risk assessment platform running in under 30 minutes!*

## 🚀 Prerequisites Checklist

- [ ] Databricks workspace (any cloud provider)
- [ ] Unity Catalog enabled
- [ ] AccuWeather API key → [Get one here](https://developer.accuweather.com/)
- [ ] Admin permissions for catalog creation

## 📥 Step 1: Import Project (2 minutes)

### Option A: Git Clone (Recommended)
```bash
# In Databricks Repos
Repo URL: https://github.com/hecdbx/climate-risk.git
Branch: main
```

### Option B: Direct Upload
1. Download ZIP from [GitHub](https://github.com/hecdbx/climate-risk)
2. Upload to Databricks workspace
3. Maintain folder structure

## 🛠️ Step 2: Create Cluster (3 minutes)

**Quick Cluster Config:**
```yaml
Name: climate-risk-cluster
Runtime: 17.0+ (latest)
Workers: 2 x Standard_DS4_v2
Auto-terminate: 60 min
```

**Essential Spark Config:**
```
spark.databricks.photon.enabled true
spark.databricks.unity.enabled true
spark.databricks.delta.optimizeWrite.enabled true
```

**Required Libraries:**
```
requests>=2.31.0
pandas>=2.1.4
numpy>=1.25.2
```

## 🔐 Step 3: Configure Secrets (2 minutes)

```bash
# Create secret scope
databricks secrets create-scope --scope climate-risk

# Add AccuWeather API key
databricks secrets put --scope climate-risk --key accuweather-api-key
# Paste your API key when prompted
```

## 🏛️ Step 4: Initialize Unity Catalog (5 minutes)

**Run this SQL script:**
```sql
%run /Repos/climate-risk/insurance-climate-risk/config/unity_catalog_schema.sql
```

**Verify setup:**
```sql
SHOW CATALOGS LIKE 'climate_risk_catalog';
SHOW SCHEMAS IN climate_risk_catalog;
```

## 📊 Step 5: Deploy Pipelines (10 minutes)

### 5.1 Create AccuWeather Pipeline
1. Go to **Workflows** → **Delta Live Tables**
2. Click **Create Pipeline**
3. **Quick Config:**
   ```json
   Name: accuweather_ingestion
   Notebook: /Repos/climate-risk/.../01_accuweather_ingestion_pipeline
   Target: climate_risk_catalog.processed_data
   Mode: Triggered
   ```
4. **Configuration:**
   ```
   accuweather.api.key = {{secrets/climate-risk/accuweather-api-key}}
   ```

### 5.2 Create Historical Pipeline
Repeat for historical data:
```json
Name: historical_processing
Notebook: /Repos/climate-risk/.../02_historical_data_processing_pipeline
Target: climate_risk_catalog.processed_data
```

### 5.3 Test Pipelines
- Start AccuWeather pipeline → Should complete in ~3 minutes
- Start Historical pipeline → Should complete in ~5 minutes

## 🧪 Step 6: Run Risk Models (5 minutes)

Execute notebooks in order:

```python
# 1. Drought Risk Model
%run /Repos/climate-risk/insurance-climate-risk/notebooks/01_drought_risk_model

# 2. Flood Risk Model  
%run /Repos/climate-risk/insurance-climate-risk/notebooks/02_flood_risk_model

# 3. Combined Assessment
%run /Repos/climate-risk/insurance-climate-risk/notebooks/combined_risk_assessment
```

## 📈 Step 7: Verify Deployment (3 minutes)

**Check data ingestion:**
```sql
SELECT COUNT(*) as records, MAX(processing_timestamp) as latest_update
FROM climate_risk_catalog.processed_data.climate_observations;
```

**Verify risk models:**
```sql
SELECT 
  COUNT(*) as total_assessments,
  COUNT(DISTINCT h3_cell_7) as unique_locations,
  AVG(combined_risk_score) as avg_risk_score
FROM climate_risk_catalog.risk_models.combined_risk_assessments;
```

**Expected Results:**
- ✅ Climate observations: 100+ records
- ✅ Risk assessments: 50+ locations  
- ✅ Processing timestamps: Within last hour

## 🎯 Step 8: Create Quick Dashboard (Optional - 5 minutes)

**In Databricks SQL:**
```sql
-- Portfolio Risk Summary
SELECT 
  'Today' as period,
  COUNT(DISTINCT h3_cell_7) as locations,
  ROUND(AVG(combined_risk_score), 2) as avg_risk,
  COUNT(CASE WHEN overall_risk_level = 'high' THEN 1 END) as high_risk_locations
FROM climate_risk_catalog.risk_models.combined_risk_assessments
WHERE assessment_date = CURRENT_DATE()

UNION ALL

SELECT 
  'Last 30 Days' as period,
  COUNT(DISTINCT h3_cell_7) as locations,
  ROUND(AVG(combined_risk_score), 2) as avg_risk,
  COUNT(CASE WHEN overall_risk_level = 'high' THEN 1 END) as high_risk_locations
FROM climate_risk_catalog.risk_models.combined_risk_assessments
WHERE assessment_date >= CURRENT_DATE() - INTERVAL 30 DAYS;
```

## ✅ Success Validation

**Your deployment is successful if:**

1. **Data Pipeline Status:** ✅ Green
   ```sql
   SELECT 'HEALTHY' as status 
   WHERE EXISTS (
     SELECT 1 FROM climate_risk_catalog.processed_data.climate_observations 
     WHERE processing_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 6 HOURS
   );
   ```

2. **Risk Models Working:** ✅ Recent assessments
   ```sql
   SELECT COUNT(*) as recent_assessments
   FROM climate_risk_catalog.risk_models.combined_risk_assessments
   WHERE assessment_date >= CURRENT_DATE() - INTERVAL 1 DAY;
   ```

3. **Data Quality:** ✅ >90% quality score
   ```sql
   SELECT ROUND(AVG(data_quality_score)*100, 1) as quality_percentage
   FROM climate_risk_catalog.processed_data.climate_observations;
   ```

## 🔧 Quick Troubleshooting

**Problem:** Pipeline fails with "Cannot access AccuWeather API"
```bash
# Check secret
databricks secrets get --scope climate-risk --key accuweather-api-key
# Verify API key at https://developer.accuweather.com/user/me
```

**Problem:** "Table not found" errors
```sql
-- Verify catalog setup
SHOW CATALOGS;
USE CATALOG climate_risk_catalog;
SHOW TABLES IN raw_data;
```

**Problem:** No data in tables
```python
# Check pipeline logs
%python
dbutils.jobs.get_run_output(run_id="your_pipeline_run_id")
```

## 🚀 Next Steps

**Production Readiness:**
1. **Set up monitoring:** Configure alerts for data quality
2. **Schedule pipelines:** Set daily runs at 2 AM UTC
3. **Add more locations:** Expand AccuWeather location list
4. **Create dashboards:** Build executive reporting views
5. **Implement CI/CD:** Set up automated deployment

**Scale Up:**
- Add more data sources (NOAA, ERA5, satellite data)
- Implement real-time streaming
- Add ML model training automation
- Create API endpoints for risk scoring

## 📚 Resources

- **[Full Deployment Guide](DEPLOYMENT_BLOG.md)** - Comprehensive step-by-step tutorial
- **[GitHub Repository](https://github.com/hecdbx/climate-risk)** - Complete source code
- **[Databricks Documentation](https://docs.databricks.com/)** - Platform reference
- **[AccuWeather API Docs](https://developer.accuweather.com/apis)** - Weather data integration

---

**🎉 Congratulations!** You now have a production-ready climate risk assessment platform running on Databricks with real-time weather data, automated risk modeling, and enterprise governance.

**Total Deployment Time: ~30 minutes** ⏱️
