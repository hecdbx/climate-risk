#!/usr/bin/env python3
"""
Type Inference Fix for Climate Risk Models
Addresses DBR 16+ type inference issues in Spark DataFrames
"""

def apply_spark_config_fixes():
    """Apply Spark configuration settings to improve type inference"""
    
    print("🔧 Applying Spark configuration fixes for type inference...")
    
    # Configuration settings that help with type inference in DBR 16+
    config_settings = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.pythonUDF.arrow.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.repl.eagerEval.enabled": "true",
        "spark.sql.repl.eagerEval.maxNumRows": "20"
    }
    
    try:
        # Get the current Spark session
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        
        if spark is None:
            print("⚠️  No active Spark session found - configurations will apply to new sessions")
            return config_settings
        
        # Apply configurations
        for key, value in config_settings.items():
            spark.conf.set(key, value)
            print(f"✅ Set {key} = {value}")
            
        print("🎉 Spark configuration fixes applied successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error applying Spark configurations: {e}")
        return False

def create_sample_schema_templates():
    """Create template schemas for common data structures"""
    
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType, DateType
    
    schemas = {
        'climate_data': StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("date", StringType(), True),
            StructField("h3_cell", StringType(), True),
            StructField("precipitation_mm", DoubleType(), True),
            StructField("temperature_celsius", DoubleType(), True),
            StructField("soil_moisture_percent", DoubleType(), True)
        ]),
        
        'elevation_data': StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("h3_cell", StringType(), True),
            StructField("elevation_m", DoubleType(), True),
            StructField("slope_degrees", DoubleType(), True),
            StructField("in_floodplain", BooleanType(), True),
            StructField("distance_to_water", DoubleType(), True)
        ]),
        
        'flood_events': StructType([
            StructField("h3_cell", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("event_date", StringType(), True),
            StructField("severity", StringType(), True),
            StructField("flood_depth_m", DoubleType(), True),
            StructField("duration_hours", DoubleType(), True),
            StructField("damage_estimate", DoubleType(), True)
        ]),
        
        'risk_assessment': StructType([
            StructField("h3_cell", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("drought_risk_score", DoubleType(), True),
            StructField("flood_risk_score", DoubleType(), True),
            StructField("combined_risk_score", DoubleType(), True),
            StructField("risk_level", StringType(), True),
            StructField("premium_multiplier", DoubleType(), True)
        ])
    }
    
    return schemas

def validate_data_types(df, expected_schema=None):
    """Validate DataFrame schema and suggest fixes"""
    
    print(f"📊 Validating DataFrame with {df.count()} rows...")
    print("Current schema:")
    df.printSchema()
    
    # Check for common type inference issues
    schema_issues = []
    for field in df.schema.fields:
        if str(field.dataType) == 'StringType' and field.name in ['latitude', 'longitude', 'elevation_m', 'precipitation_mm']:
            schema_issues.append(f"⚠️  {field.name} is StringType but should be DoubleType")
        elif 'null' in str(field.dataType).lower():
            schema_issues.append(f"⚠️  {field.name} has null type - data may be inconsistent")
    
    if schema_issues:
        print("\n🚨 Schema Issues Found:")
        for issue in schema_issues:
            print(f"   {issue}")
        print("\n💡 Suggestion: Use explicit schema when creating DataFrames")
        return False
    else:
        print("✅ Schema validation passed!")
        return True

def fix_dataframe_creation_example():
    """Show example of proper DataFrame creation with explicit schema"""
    
    example_code = '''
# ❌ PROBLEMATIC: Letting Spark infer types
data = [
    {'latitude': 37.7749, 'longitude': -122.4194, 'risk_score': 0.65},
    {'latitude': 34.0522, 'longitude': -118.2437, 'risk_score': 0.45}
]
df = spark.createDataFrame(data)  # May cause type inference errors

# ✅ SOLUTION: Use explicit schema
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("risk_score", DoubleType(), True)
])

# Convert data to tuples with explicit types
data_tuples = [
    (float(37.7749), float(-122.4194), float(0.65)),
    (float(34.0522), float(-118.2437), float(0.45))
]

df = spark.createDataFrame(data_tuples, schema)  # ✅ No type inference issues
'''
    
    print("📋 Example of fixing DataFrame creation:")
    print(example_code)

def run_diagnostic():
    """Run comprehensive diagnostic for type inference issues"""
    
    print("🔍 Running Climate Risk Models - Type Inference Diagnostic")
    print("=" * 60)
    
    # Check Spark session
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        
        if spark is None:
            print("⚠️  No active Spark session found")
            print("💡 In Databricks notebooks, the session should be automatically available")
            return False
        else:
            print("✅ Active Spark session found")
            print(f"   Spark version: {spark.version}")
            
    except ImportError:
        print("❌ PySpark not available")
        return False
    
    # Apply configuration fixes
    config_success = apply_spark_config_fixes()
    
    # Show schema templates
    schemas = create_sample_schema_templates()
    print(f"\n📐 Available schema templates: {list(schemas.keys())}")
    
    # Show example fix
    fix_dataframe_creation_example()
    
    print("\n🎯 Quick Fixes for Type Inference Errors:")
    print("1. Always use explicit schemas when creating DataFrames")
    print("2. Convert Python types to explicit types (float(), str(), bool())")
    print("3. Use tuples instead of dictionaries for data")
    print("4. Apply Spark configurations for better Arrow integration")
    
    return True

if __name__ == "__main__":
    run_diagnostic()
