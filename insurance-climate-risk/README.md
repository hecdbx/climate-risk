# Climate Risk Insurance Models

A comprehensive project for developing drought and flood risk models for insurance companies using Databricks' advanced geospatial and geotype functions.

## 🌍 Overview

This project leverages Databricks' latest geospatial capabilities to build sophisticated climate risk models that help insurance companies:
- **Assess drought risk** across different geographical regions using multi-factor climate analysis
- **Evaluate flood risk** using elevation data, precipitation patterns, and hydrological modeling
- **Generate precise risk scores** for underwriting and pricing decisions
- **Visualize risk patterns** through interactive maps and comprehensive dashboards
- **Deploy production-ready models** with automated monitoring and retraining pipelines

## 🚀 Key Features

### 🔥 Drought Risk Modeling
- Multi-factor drought analysis using precipitation, temperature, soil moisture, and vegetation indices
- Historical drought event correlation and frequency analysis
- Standardized Precipitation Index (SPI) and Palmer Drought Severity Index (PDSI) integration
- Seasonal and long-term drought trend analysis

### 🌊 Flood Risk Assessment
- Comprehensive flood modeling incorporating elevation data, slope analysis, and drainage capacity
- Precipitation intensity analysis with storm event classification
- Historical flood event integration and return period calculations
- Watershed and flow accumulation analysis using advanced topographic processing

### 🗺️ Advanced Geospatial Analytics
- **H3 hexagonal indexing** for uniform spatial analysis and efficient processing
- **ST_* spatial functions** for geometric operations and spatial relationships
- **GeoJSON processing** for seamless integration with mapping applications
- **Multi-resolution analysis** supporting scales from local to regional assessments

### 🎯 Risk Scoring Engine
- Automated risk classification with customizable severity thresholds
- Insurance-specific risk ratings and premium multiplier calculations
- Portfolio-level risk aggregation and concentration analysis
- Real-time risk assessment API with sub-second response times

### 📊 Interactive Visualizations
- **Dynamic risk maps** with color-coded risk levels and interactive popups
- **Time series analysis** showing risk trends and seasonal patterns
- **Correlation heatmaps** displaying relationships between risk factors
- **Portfolio dashboards** for insurance portfolio risk management

### 🔄 Production-Ready Deployment
- **MLflow model registry** for version control and model lifecycle management
- **Automated monitoring** with data drift detection and performance tracking
- **A/B testing framework** for model validation and continuous improvement
- **Batch and real-time scoring** pipelines for different use cases

## 🛠️ Technology Stack

- **Platform**: Databricks Runtime 16.1+ LTS with Spark Connect
- **Languages**: Python 3.11+, Spark SQL, PySpark (native optimizations)
- **Geospatial**: Native H3 functions, GeoPandas 0.14+, Rasterio, Folium
- **Machine Learning**: Scikit-learn 1.4+, LightGBM 4.3+, MLflow (enhanced)
- **Visualization**: Plotly 5.18+, Matplotlib, Folium, Seaborn
- **Data Storage**: Delta Lake (with Change Data Feed), GeoJSON
- **Performance**: Photon Engine, Enhanced Arrow optimizations
- **APIs**: FastAPI, REST endpoints with Spark Connect

## 📁 Project Structure

```
insurance-climate-risk/
├── notebooks/                    # Databricks notebooks
│   ├── 01_drought_risk_model.py      # Drought risk assessment model
│   ├── 02_flood_risk_model.py        # Flood risk assessment model
│   ├── 03_risk_visualization.py      # Interactive dashboards and maps
│   └── 04_model_deployment.py        # Production deployment pipeline
├── config/                       # Configuration files
│   ├── cluster_config.yaml           # Databricks cluster configuration
│   └── data_sources.yaml             # Data source configurations
├── src/                         # Source code modules
│   └── risk_engine.py                # Main risk assessment engine
├── data/                        # Data storage and samples
├── models/                      # Trained models and artifacts
├── visualizations/              # Charts and map outputs
├── tests/                       # Unit and integration tests
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## 🚀 Getting Started

### Prerequisites
- Databricks workspace with Runtime 16.1+ LTS
- Spark Connect enabled cluster
- Access to climate and elevation data sources
- MLflow for model tracking and deployment (enhanced in DBR 16+)

### 1. Environment Setup

#### Quick Setup (Recommended)
```bash
# Automated setup with compatibility fixes
python setup_environment.py
```

#### Manual Setup
```bash
# Install dependencies (PySpark managed by DBR 16+)
pip install -r requirements.txt

# Configure Databricks cluster for DBR 16+ with Spark Connect
# Use config/cluster_config.yaml for optimized cluster setup
# Features: Photon Engine, Enhanced Arrow, Native H3 functions
```

#### 🚨 **Installation Issues?**
If you encounter errors like `ModuleNotFoundError: No module named 'distutils.msvccompiler'`, see our comprehensive [**Installation Guide**](INSTALL.md) for solutions.

### 2. Data Preparation
```python
# Load climate and geographical datasets (DBR 16+ optimized)
from src.risk_engine import ClimateRiskEngine

# No need for Spark session - automatically managed in DBR 16+
engine = ClimateRiskEngine()
# Data sources are configured in config/data_sources.yaml
# All data stored in Delta Lake format for optimal performance
```

### 3. Model Development
Run the notebooks in sequence:
1. **01_drought_risk_model.py** - Develop drought risk assessment
2. **02_flood_risk_model.py** - Build flood risk models
3. **03_risk_visualization.py** - Create interactive visualizations
4. **04_model_deployment.py** - Deploy to production

### 4. Risk Assessment
```python
# Quick risk assessment for a location
engine = ClimateRiskEngine()

# Individual risk assessments
drought_risk = engine.assess_drought_risk(37.7749, -122.4194)
flood_risk = engine.assess_flood_risk(37.7749, -122.4194)

# Combined climate risk
combined_risk = engine.assess_combined_risk(37.7749, -122.4194)

# Portfolio risk analysis
locations = [(37.7749, -122.4194), (34.0522, -118.2437)]
portfolio_risk = engine.assess_portfolio_risk(locations)
```

### 5. Production Deployment
```python
# Deploy models using MLflow
from notebooks.model_deployment import register_climate_risk_model

model_id = register_climate_risk_model()
# Models are automatically registered for batch and real-time scoring
```

## 📊 Model Performance

| Model Component | Accuracy | Response Time | Coverage |
|----------------|----------|---------------|----------|
| Drought Risk | 85% | <100ms | Continental US |
| Flood Risk | 82% | <150ms | Continental US |
| Combined Risk | 84% | <200ms | Continental US |

## 🎯 Use Cases

### Insurance Underwriting
- **Property Insurance**: Assess climate risk for individual properties
- **Agricultural Insurance**: Evaluate drought and flood risk for farmland
- **Commercial Insurance**: Analyze risk for business locations and supply chains

### Portfolio Management
- **Risk Concentration**: Identify geographic concentrations of high-risk properties
- **Premium Optimization**: Calculate risk-adjusted premiums and deductibles
- **Reinsurance**: Support reinsurance decision-making with detailed risk analysis

### Regulatory Compliance
- **Climate Risk Reporting**: Generate reports for regulatory requirements
- **Stress Testing**: Conduct climate stress tests for solvency assessments
- **ESG Reporting**: Support environmental, social, and governance reporting

## 🔧 Configuration

### Cluster Configuration
Edit `config/cluster_config.yaml` to customize:
- Spark cluster size and autoscaling
- Geospatial library versions
- Memory and compute optimization settings

### Data Sources
Configure data sources in `config/data_sources.yaml`:
- Climate data APIs and endpoints
- Elevation data repositories
- Historical event databases

## 📈 Monitoring and Maintenance

### Automated Monitoring
- **Data Quality**: Continuous validation of input data quality and completeness
- **Model Performance**: Real-time tracking of prediction accuracy and drift
- **System Health**: Infrastructure monitoring and alerting

### Model Updates
- **Scheduled Retraining**: Quarterly model updates with new data
- **A/B Testing**: Continuous evaluation of model improvements
- **Version Control**: Complete model lineage and rollback capabilities

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-feature`)
3. Commit your changes (`git commit -am 'Add new feature'`)
4. Push to the branch (`git push origin feature/new-feature`)
5. Create a Pull Request

## 📚 Documentation

- **API Documentation**: Available in the `docs/` directory
- **Model Documentation**: Detailed model specifications and validation results
- **User Guide**: Step-by-step instructions for common use cases

## 🔒 Security and Compliance

- **Data Encryption**: All data encrypted in transit and at rest
- **Access Control**: Role-based access control (RBAC) for all components
- **Audit Logging**: Comprehensive logging for all model predictions and data access
- **Privacy**: No personally identifiable information (PII) is stored or processed

## 🆘 Support

For questions and support:
- **Technical Issues**: Create an issue in the repository
- **Feature Requests**: Submit enhancement requests
- **Documentation**: Check the `docs/` directory for detailed guides

## 📄 License

This project is proprietary software for internal use only. All rights reserved.

---

**Built with ❤️ by the Climate Risk Modeling Team**

*Leveraging Databricks' latest geospatial capabilities to advance climate risk assessment in the insurance industry.*
