# Installation Guide - Climate Risk Insurance Models

## 🚨 Quick Fix for `distutils.msvccompiler` Error

If you encounter the error:
```
ModuleNotFoundError: No module named 'distutils.msvccompiler'
```

This is a **Python 3.12+ compatibility issue**. Here are the solutions:

### 🛠️ **Solution 1: Automated Setup (Recommended)**

Run the automated setup script:
```bash
cd insurance-climate-risk
python setup_environment.py
```

### 🛠️ **Solution 2: Manual Fix**

1. **Install compatibility packages first:**
```bash
pip install setuptools>=68.0.0 wheel>=0.41.0 packaging>=23.0
```

2. **Install core packages:**
```bash
pip install numpy==1.25.2 pandas==2.1.4 shapely==2.0.2
```

3. **Install requirements:**
```bash
pip install -r requirements.txt
```

### 🛠️ **Solution 3: Alternative Package Versions**

If you continue having issues, try these stable versions:
```bash
pip install geopandas==0.13.2 rasterio==1.3.8 fiona==1.8.22
```

## 📋 **Environment-Specific Instructions**

### 🚀 **Databricks Runtime 16+**
- Most packages are pre-installed and optimized
- Only install additional packages as needed:
```bash
%pip install h3==3.7.6 folium==0.15.1 plotly==5.17.0
```

### 💻 **Local Development**
- Use Python 3.9-3.11 for best compatibility
- Consider using conda for geospatial packages:
```bash
conda install -c conda-forge geopandas rasterio fiona h3-py
pip install -r requirements.txt
```

### 🐳 **Docker Environment**
```dockerfile
FROM python:3.11-slim
RUN apt-get update && apt-get install -y \
    gdal-bin libgdal-dev \
    proj-bin libproj-dev \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install -r requirements.txt
```

## ✅ **Verification**

Test your installation:
```python
# Test core functionality
from src.risk_engine import ClimateRiskEngine
engine = ClimateRiskEngine()

# Test geospatial functions
import h3
import geopandas as gpd
import folium

print("✅ Installation successful!")
```

## 🔧 **Troubleshooting**

### Common Issues:

1. **GDAL/PROJ errors**: Install system dependencies
   ```bash
   # Ubuntu/Debian
   sudo apt-get install gdal-bin libgdal-dev
   
   # macOS
   brew install gdal proj
   
   # Windows
   # Use conda-forge packages
   ```

2. **Wheel build failures**: Update pip and setuptools
   ```bash
   pip install --upgrade pip setuptools wheel
   ```

3. **Permission errors**: Use virtual environment
   ```bash
   python -m venv climate_risk_env
   source climate_risk_env/bin/activate  # Linux/macOS
   # climate_risk_env\Scripts\activate  # Windows
   ```

## 🎯 **Minimal Installation**

For basic functionality only:
```bash
pip install pandas numpy h3 folium plotly
```

This provides core risk assessment without full geospatial capabilities.

## 📞 **Support**

If issues persist:
1. Check the [GitHub Issues](https://github.com/hecdbx/climate-risk/issues)
2. Verify your Python version: `python --version`
3. Check installed packages: `pip list | grep -E "(pandas|numpy|h3|geopandas)"`

## 🎉 **Quick Start After Installation**

```python
from src.risk_engine import ClimateRiskEngine

# Initialize the engine (no Spark session needed in DBR 16+)
engine = ClimateRiskEngine()

# Assess risk for San Francisco
lat, lon = 37.7749, -122.4194
risk_assessment = engine.assess_combined_risk(lat, lon)

print(f"Combined Risk Score: {risk_assessment['combined_risk_score']:.3f}")
print(f"Risk Level: {risk_assessment['overall_risk_level']}")
```

---

**📚 For complete documentation, see [README.md](README.md)**
