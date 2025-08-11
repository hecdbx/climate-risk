#!/usr/bin/env python3
"""
Environment Setup Script for Climate Risk Insurance Models
Handles compatibility issues with Python 3.12+ and DBR 16+
"""

import sys
import subprocess
import importlib.util
import os
from pathlib import Path

def check_python_version():
    """Check Python version compatibility"""
    version = sys.version_info
    print(f"Python version: {version.major}.{version.minor}.{version.micro}")
    
    if version.major < 3 or (version.major == 3 and version.minor < 9):
        print("⚠️  Warning: Python 3.9+ recommended for best compatibility")
        return False
    elif version.major == 3 and version.minor >= 12:
        print("📋 Python 3.12+ detected - applying compatibility fixes...")
        return "needs_fixes"
    else:
        print("✅ Python version compatible")
        return True

def install_compatibility_packages():
    """Install packages to fix distutils compatibility issues"""
    compatibility_packages = [
        "setuptools>=68.0.0",
        "wheel>=0.41.0",
        "packaging>=23.0"
    ]
    
    print("📦 Installing compatibility packages...")
    for package in compatibility_packages:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"✅ Installed {package}")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install {package}: {e}")
            return False
    return True

def check_databricks_environment():
    """Check if running in Databricks environment"""
    is_databricks = (
        os.environ.get('DATABRICKS_RUNTIME_VERSION') is not None or
        importlib.util.find_spec('databricks') is not None or
        '/databricks/' in sys.executable
    )
    
    if is_databricks:
        print("🚀 Databricks environment detected")
        runtime_version = os.environ.get('DATABRICKS_RUNTIME_VERSION', 'Unknown')
        print(f"   Runtime version: {runtime_version}")
        
        # Check if it's DBR 16+
        try:
            major_version = int(runtime_version.split('.')[0])
            if major_version >= 16:
                print("✅ DBR 16+ detected - optimizations available")
                return "dbr16+"
            else:
                print("⚠️  DBR version < 16 - consider upgrading for best performance")
                return "dbr_legacy"
        except (ValueError, IndexError):
            print("❓ Could not determine DBR version")
            return "dbr_unknown"
    else:
        print("💻 Local/non-Databricks environment detected")
        return "local"

def install_geospatial_packages():
    """Install geospatial packages with error handling"""
    
    # Core packages that should install first
    core_packages = [
        "numpy==1.25.2",
        "pandas==2.1.4",
        "shapely==2.0.2"
    ]
    
    # Geospatial packages that depend on core packages
    geospatial_packages = [
        "fiona==1.9.5",
        "rasterio==1.3.9", 
        "geopandas==0.14.1",
        "h3==3.7.6",
        "pyproj==3.6.1"
    ]
    
    print("📍 Installing core packages...")
    for package in core_packages:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"✅ Installed {package}")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install {package}: {e}")
    
    print("🗺️  Installing geospatial packages...")
    for package in geospatial_packages:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"✅ Installed {package}")
        except subprocess.CalledProcessError as e:
            print(f"⚠️  Warning: Could not install {package}: {e}")
            print(f"   This may be due to system dependencies - continuing...")

def install_requirements():
    """Install requirements with error handling"""
    requirements_file = Path(__file__).parent / "requirements.txt"
    
    if not requirements_file.exists():
        print("❌ requirements.txt not found")
        return False
    
    print("📋 Installing requirements...")
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", 
            "-r", str(requirements_file),
            "--no-deps"  # Install without dependencies first
        ])
        print("✅ Requirements installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"⚠️  Some packages may have failed to install: {e}")
        print("   This is normal for geospatial packages - trying alternative installation...")
        return install_geospatial_packages()

def verify_installation():
    """Verify key packages are installed correctly"""
    critical_packages = [
        'pandas', 'numpy', 'h3', 'folium', 'plotly'
    ]
    
    optional_packages = [
        'geopandas', 'rasterio', 'fiona', 'shapely'
    ]
    
    print("🔍 Verifying installation...")
    
    all_critical_installed = True
    for package in critical_packages:
        try:
            __import__(package)
            print(f"✅ {package} - OK")
        except ImportError:
            print(f"❌ {package} - MISSING (CRITICAL)")
            all_critical_installed = False
    
    optional_count = 0
    for package in optional_packages:
        try:
            __import__(package)
            print(f"✅ {package} - OK")
            optional_count += 1
        except ImportError:
            print(f"⚠️  {package} - MISSING (optional)")
    
    print(f"\n📊 Installation Summary:")
    print(f"   Critical packages: {'✅ ALL OK' if all_critical_installed else '❌ SOME MISSING'}")
    print(f"   Optional packages: {optional_count}/{len(optional_packages)} installed")
    
    return all_critical_installed

def main():
    """Main setup function"""
    print("🚀 Climate Risk Insurance Models - Environment Setup")
    print("=" * 60)
    
    # Check Python version
    python_status = check_python_version()
    
    # Apply compatibility fixes if needed
    if python_status == "needs_fixes":
        if not install_compatibility_packages():
            print("❌ Failed to install compatibility packages")
            return False
    
    # Check environment
    env_type = check_databricks_environment()
    
    # Install packages
    if env_type.startswith("dbr"):
        print("📦 Installing packages for Databricks environment...")
        # In Databricks, some packages are pre-installed
        success = install_requirements()
    else:
        print("📦 Installing packages for local environment...")
        success = install_requirements()
    
    # Verify installation
    if success:
        verify_installation()
    
    print("\n🎉 Setup complete!")
    print("\n💡 Usage:")
    print("   from src.risk_engine import ClimateRiskEngine")
    print("   engine = ClimateRiskEngine()")
    print("   risk = engine.assess_combined_risk(37.7749, -122.4194)")
    
    return True

if __name__ == "__main__":
    main()
