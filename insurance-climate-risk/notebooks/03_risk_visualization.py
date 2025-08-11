# Databricks notebook source
# MAGIC %md
# MAGIC # Climate Risk Visualization Dashboard
# MAGIC 
# MAGIC This notebook creates interactive visualizations and dashboards for climate risk assessment.
# MAGIC It combines drought and flood risk models to create comprehensive risk maps and analytics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

# DBR 16+ optimized imports
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from folium import plugins
import h3
import json
from datetime import datetime, timedelta

# For geospatial visualization
import geopandas as gpd
from shapely.geometry import Polygon
import branca.colormap as cm

# Note: Spark session automatically available in DBR 16+

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Risk Model Results

# COMMAND ----------

def load_risk_data():
    """Load drought and flood risk model results"""
    
    # Load drought risk data from Delta tables (DBR 16+ optimized)
    try:
        drought_df = spark.sql("SELECT * FROM climate_risk.drought_risk_regional LIMIT 5000")
        print(f"Loaded {drought_df.count()} drought risk records")
    except:
        print("Drought risk data not available, using sample data")
        drought_df = spark.createDataFrame([
            {'h3_cell': '87283472bffffff', 'latitude': 37.7749, 'longitude': -122.4194, 
             'avg_risk_score': 0.65, 'risk_category': 'high', 'risk_multiplier': 1.8},
            {'h3_cell': '87283472c7fffff', 'latitude': 37.7849, 'longitude': -122.4094, 
             'avg_risk_score': 0.45, 'risk_category': 'moderate', 'risk_multiplier': 1.2}
        ])
    
    # Load flood risk data from Delta tables
    try:
        flood_df = spark.sql("SELECT * FROM climate_risk.flood_risk_summary LIMIT 5000")
        print(f"Loaded {flood_df.count()} flood risk records")
    except:
        print("Flood risk data not available, using sample data")
        flood_df = spark.createDataFrame([
            {'h3_cell': '87283472bffffff', 'latitude': 37.7749, 'longitude': -122.4194, 
             'flood_risk_score': 0.55, 'flood_risk_level': 'moderate', 'base_premium_multiplier': 1.5},
            {'h3_cell': '87283472c7fffff', 'latitude': 37.7849, 'longitude': -122.4094, 
             'flood_risk_score': 0.75, 'flood_risk_level': 'high', 'base_premium_multiplier': 2.0}
        ])
    
    return drought_df, flood_df

drought_data, flood_data = load_risk_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combined Risk Analysis

# COMMAND ----------

def combine_risk_data(drought_df, flood_df):
    """Combine drought and flood risk data for comprehensive analysis"""
    
    # Join drought and flood data
    combined_df = drought_df.alias("drought").join(
        flood_df.alias("flood"), 
        ["h3_cell", "latitude", "longitude"], 
        "full_outer"
    ).select(
        F.col("h3_cell"),
        F.col("latitude"), 
        F.col("longitude"),
        F.coalesce(F.col("drought.avg_risk_score"), F.lit(0.0)).alias("drought_risk"),
        F.coalesce(F.col("drought.risk_category"), F.lit("unknown")).alias("drought_level"),
        F.coalesce(F.col("drought.risk_multiplier"), F.lit(1.0)).alias("drought_multiplier"),
        F.coalesce(F.col("flood.flood_risk_score"), F.lit(0.0)).alias("flood_risk"),
        F.coalesce(F.col("flood.flood_risk_level"), F.lit("unknown")).alias("flood_level"),
        F.coalesce(F.col("flood.base_premium_multiplier"), F.lit(1.0)).alias("flood_multiplier")
    )
    
    # Calculate combined risk metrics
    combined_df = combined_df.withColumn("combined_risk_score",
        (F.col("drought_risk") * 0.5 + F.col("flood_risk") * 0.5)
    ).withColumn("max_risk_score",
        F.greatest(F.col("drought_risk"), F.col("flood_risk"))
    ).withColumn("primary_risk_type",
        F.when(F.col("drought_risk") > F.col("flood_risk"), "drought")
        .when(F.col("flood_risk") > F.col("drought_risk"), "flood")
        .otherwise("balanced")
    ).withColumn("combined_risk_level",
        F.when(F.col("combined_risk_score") >= 0.8, "very_high")
        .when(F.col("combined_risk_score") >= 0.6, "high")
        .when(F.col("combined_risk_score") >= 0.4, "moderate")
        .when(F.col("combined_risk_score") >= 0.2, "low")
        .otherwise("very_low")
    ).withColumn("combined_multiplier",
        F.greatest(F.col("drought_multiplier"), F.col("flood_multiplier"))
    )
    
    return combined_df

combined_risk_df = combine_risk_data(drought_data, flood_data)
combined_risk_pd = combined_risk_df.toPandas()

print(f"Combined risk analysis for {len(combined_risk_pd)} locations")
print("\nRisk Level Distribution:")
print(combined_risk_pd['combined_risk_level'].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interactive Risk Maps

# COMMAND ----------

def create_folium_risk_map(df, risk_column='combined_risk_score', map_title='Climate Risk Map'):
    """Create interactive Folium map showing risk levels"""
    
    # Calculate map center
    center_lat = df['latitude'].mean()
    center_lon = df['longitude'].mean()
    
    # Create base map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=10,
        tiles='OpenStreetMap'
    )
    
    # Add title
    title_html = f'''
                 <h3 align="center" style="font-size:20px"><b>{map_title}</b></h3>
                 '''
    m.get_root().html.add_child(folium.Element(title_html))
    
    # Define color mapping for risk levels
    def get_color(risk_score):
        if risk_score >= 0.8:
            return 'red'
        elif risk_score >= 0.6:
            return 'orange'
        elif risk_score >= 0.4:
            return 'yellow'
        elif risk_score >= 0.2:
            return 'lightgreen'
        else:
            return 'green'
    
    # Add risk points
    for _, row in df.iterrows():
        risk_score = row[risk_column]
        
        popup_text = f"""
        <b>Location:</b> {row['latitude']:.4f}, {row['longitude']:.4f}<br>
        <b>H3 Cell:</b> {row['h3_cell']}<br>
        <b>Combined Risk:</b> {row['combined_risk_score']:.3f}<br>
        <b>Risk Level:</b> {row['combined_risk_level']}<br>
        <b>Drought Risk:</b> {row['drought_risk']:.3f}<br>
        <b>Flood Risk:</b> {row['flood_risk']:.3f}<br>
        <b>Primary Risk:</b> {row['primary_risk_type']}<br>
        <b>Premium Multiplier:</b> {row['combined_multiplier']:.2f}x
        """
        
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=8,
            popup=folium.Popup(popup_text, max_width=300),
            color='black',
            weight=1,
            fillColor=get_color(risk_score),
            fillOpacity=0.7
        ).add_to(m)
    
    # Add legend
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 150px; height: 120px; 
                background-color: white; border:2px solid grey; z-index:9999; 
                font-size:14px; ">
    <p style="margin: 10px;"><b>Risk Levels</b></p>
    <p style="margin: 10px;"><i class="fa fa-circle" style="color:red"></i> Very High (≥0.8)</p>
    <p style="margin: 10px;"><i class="fa fa-circle" style="color:orange"></i> High (≥0.6)</p>
    <p style="margin: 10px;"><i class="fa fa-circle" style="color:yellow"></i> Moderate (≥0.4)</p>
    <p style="margin: 10px;"><i class="fa fa-circle" style="color:lightgreen"></i> Low (≥0.2)</p>
    <p style="margin: 10px;"><i class="fa fa-circle" style="color:green"></i> Very Low (<0.2)</p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))
    
    return m

# Create risk map
risk_map = create_folium_risk_map(combined_risk_pd, 'combined_risk_score', 'Combined Climate Risk Map')

# Save map
map_path = "/dbfs/mnt/risk-models/climate_risk_map.html"
risk_map.save(map_path)
print(f"Risk map saved to {map_path}")

# Display map in notebook
risk_map

# COMMAND ----------

# MAGIC %md
# MAGIC ## Risk Distribution Charts

# COMMAND ----------

def create_risk_distribution_charts(df):
    """Create various charts showing risk distribution"""
    
    # Create subplot figure
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Risk Level Distribution', 'Risk Score Distribution', 
                       'Drought vs Flood Risk', 'Premium Multiplier Distribution'),
        specs=[[{"type": "pie"}, {"type": "histogram"}],
               [{"type": "scatter"}, {"type": "box"}]]
    )
    
    # 1. Risk Level Distribution (Pie Chart)
    risk_counts = df['combined_risk_level'].value_counts()
    fig.add_trace(
        go.Pie(labels=risk_counts.index, values=risk_counts.values, name="Risk Levels"),
        row=1, col=1
    )
    
    # 2. Risk Score Distribution (Histogram)
    fig.add_trace(
        go.Histogram(x=df['combined_risk_score'], name="Risk Score", nbinsx=20),
        row=1, col=2
    )
    
    # 3. Drought vs Flood Risk (Scatter)
    fig.add_trace(
        go.Scatter(
            x=df['drought_risk'], 
            y=df['flood_risk'],
            mode='markers',
            text=df['combined_risk_level'],
            name="Drought vs Flood",
            marker=dict(
                size=8,
                color=df['combined_risk_score'],
                colorscale='Viridis',
                showscale=True
            )
        ),
        row=2, col=1
    )
    
    # 4. Premium Multiplier Distribution (Box Plot)
    fig.add_trace(
        go.Box(y=df['combined_multiplier'], name="Premium Multiplier"),
        row=2, col=2
    )
    
    # Update layout
    fig.update_layout(
        title_text="Climate Risk Analysis Dashboard",
        height=800,
        showlegend=False
    )
    
    # Update axis labels
    fig.update_xaxes(title_text="Drought Risk", row=2, col=1)
    fig.update_yaxes(title_text="Flood Risk", row=2, col=1)
    fig.update_xaxes(title_text="Risk Score", row=1, col=2)
    fig.update_yaxes(title_text="Count", row=1, col=2)
    fig.update_yaxes(title_text="Multiplier", row=2, col=2)
    
    return fig

# Create and display charts
distribution_fig = create_risk_distribution_charts(combined_risk_pd)
distribution_fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Risk Correlation Analysis

# COMMAND ----------

def create_correlation_heatmap(df):
    """Create correlation heatmap for risk factors"""
    
    # Select numeric columns for correlation
    numeric_cols = ['drought_risk', 'flood_risk', 'combined_risk_score', 
                   'drought_multiplier', 'flood_multiplier', 'combined_multiplier']
    
    corr_matrix = df[numeric_cols].corr()
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=corr_matrix.values,
        x=corr_matrix.columns,
        y=corr_matrix.columns,
        colorscale='RdBu',
        zmid=0,
        text=np.around(corr_matrix.values, decimals=2),
        texttemplate="%{text}",
        textfont={"size": 10},
        hoverongaps=False
    ))
    
    fig.update_layout(
        title="Risk Factor Correlation Matrix",
        xaxis_title="Risk Factors",
        yaxis_title="Risk Factors",
        width=600,
        height=500
    )
    
    return fig

correlation_fig = create_correlation_heatmap(combined_risk_pd)
correlation_fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geographic Risk Clustering

# COMMAND ----------

def analyze_risk_clusters(df):
    """Analyze geographic clustering of high-risk areas"""
    
    # Define high-risk threshold
    high_risk_threshold = 0.6
    high_risk_df = df[df['combined_risk_score'] >= high_risk_threshold].copy()
    
    if len(high_risk_df) == 0:
        print("No high-risk areas found")
        return None
    
    # Calculate risk concentration by region (simplified grid)
    lat_bins = pd.cut(df['latitude'], bins=10)
    lon_bins = pd.cut(df['longitude'], bins=10)
    
    df['lat_bin'] = lat_bins
    df['lon_bin'] = lon_bins
    
    risk_by_region = df.groupby(['lat_bin', 'lon_bin']).agg({
        'combined_risk_score': ['mean', 'max', 'count'],
        'latitude': 'mean',
        'longitude': 'mean'
    }).reset_index()
    
    # Flatten column names
    risk_by_region.columns = ['lat_bin', 'lon_bin', 'avg_risk', 'max_risk', 'location_count', 'center_lat', 'center_lon']
    
    # Create geographic risk heatmap
    fig = go.Figure(data=go.Scatter(
        x=risk_by_region['center_lon'],
        y=risk_by_region['center_lat'],
        mode='markers',
        marker=dict(
            size=risk_by_region['location_count'] * 3,
            color=risk_by_region['avg_risk'],
            colorscale='Reds',
            showscale=True,
            colorbar=dict(title="Average Risk Score"),
            sizemode='area',
            sizeref=2.*max(risk_by_region['location_count'])/(20.**2),
            sizemin=4
        ),
        text=[f"Locations: {count}<br>Avg Risk: {risk:.3f}" 
              for count, risk in zip(risk_by_region['location_count'], risk_by_region['avg_risk'])],
        hovertemplate='<b>Region Center</b><br>' +
                     'Lat: %{y:.4f}<br>' +
                     'Lon: %{x:.4f}<br>' +
                     '%{text}<br>' +
                     '<extra></extra>'
    ))
    
    fig.update_layout(
        title="Geographic Risk Clustering Analysis",
        xaxis_title="Longitude",
        yaxis_title="Latitude",
        width=800,
        height=600
    )
    
    return fig, high_risk_df

clustering_fig, high_risk_locations = analyze_risk_clusters(combined_risk_pd)
if clustering_fig:
    clustering_fig.show()
    print(f"Identified {len(high_risk_locations)} high-risk locations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Risk Trend Analysis

# COMMAND ----------

def create_risk_trend_simulation(df):
    """Simulate risk trends over time for demonstration"""
    
    # Simulate monthly risk data for the past year
    months = pd.date_range(start='2023-01-01', end='2023-12-31', freq='M')
    
    # Select sample locations for trend analysis
    sample_locations = df.sample(min(10, len(df))).copy()
    
    trend_data = []
    for _, location in sample_locations.iterrows():
        base_drought = location['drought_risk']
        base_flood = location['flood_risk']
        
        for month in months:
            # Add seasonal variation
            seasonal_factor = 0.2 * np.sin(month.month * 2 * np.pi / 12)
            
            # Add random variation
            noise = np.random.normal(0, 0.05)
            
            drought_risk = np.clip(base_drought + seasonal_factor + noise, 0, 1)
            flood_risk = np.clip(base_flood - seasonal_factor + noise, 0, 1)
            combined_risk = (drought_risk + flood_risk) / 2
            
            trend_data.append({
                'location_id': location['h3_cell'],
                'date': month,
                'drought_risk': drought_risk,
                'flood_risk': flood_risk,
                'combined_risk': combined_risk,
                'latitude': location['latitude'],
                'longitude': location['longitude']
            })
    
    trend_df = pd.DataFrame(trend_data)
    
    # Create trend visualization
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Average Risk Trends', 'Individual Location Trends'),
        shared_xaxes=True
    )
    
    # Average trends
    avg_trends = trend_df.groupby('date').agg({
        'drought_risk': 'mean',
        'flood_risk': 'mean',
        'combined_risk': 'mean'
    }).reset_index()
    
    fig.add_trace(
        go.Scatter(x=avg_trends['date'], y=avg_trends['drought_risk'], 
                  name='Avg Drought Risk', line=dict(color='red')),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=avg_trends['date'], y=avg_trends['flood_risk'], 
                  name='Avg Flood Risk', line=dict(color='blue')),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=avg_trends['date'], y=avg_trends['combined_risk'], 
                  name='Avg Combined Risk', line=dict(color='purple')),
        row=1, col=1
    )
    
    # Individual location trends (sample)
    for location_id in trend_df['location_id'].unique()[:3]:  # Show top 3
        location_data = trend_df[trend_df['location_id'] == location_id]
        fig.add_trace(
            go.Scatter(x=location_data['date'], y=location_data['combined_risk'],
                      name=f'Location {location_id[:8]}...', 
                      line=dict(dash='dot')),
            row=2, col=1
        )
    
    fig.update_layout(
        title="Climate Risk Trends Analysis",
        height=700,
        xaxis_title="Date",
        yaxis_title="Risk Score"
    )
    
    return fig, trend_df

trend_fig, trend_data = create_risk_trend_simulation(combined_risk_pd)
trend_fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insurance Portfolio Analysis

# COMMAND ----------

def create_portfolio_analysis(df):
    """Create insurance portfolio risk analysis"""
    
    # Portfolio risk metrics
    portfolio_stats = {
        'total_locations': len(df),
        'avg_risk_score': df['combined_risk_score'].mean(),
        'max_risk_score': df['combined_risk_score'].max(),
        'risk_std': df['combined_risk_score'].std(),
        'high_risk_count': len(df[df['combined_risk_level'].isin(['high', 'very_high'])]),
        'avg_premium_multiplier': df['combined_multiplier'].mean(),
        'total_premium_adjustment': (df['combined_multiplier'] - 1).sum()
    }
    
    # Risk concentration analysis
    risk_concentration = df['combined_risk_level'].value_counts(normalize=True) * 100
    
    # Premium impact analysis
    premium_impact = df.groupby('combined_risk_level').agg({
        'combined_multiplier': ['mean', 'min', 'max'],
        'h3_cell': 'count'
    }).round(3)
    
    # Create dashboard
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Portfolio Risk Distribution', 'Premium Multiplier by Risk Level',
                       'Risk Score vs Premium Impact', 'Geographic Risk Spread'),
        specs=[[{"type": "bar"}, {"type": "box"}],
               [{"type": "scatter"}, {"type": "bar"}]]
    )
    
    # 1. Portfolio Risk Distribution
    fig.add_trace(
        go.Bar(x=risk_concentration.index, y=risk_concentration.values,
               name="Risk Distribution", marker_color='skyblue'),
        row=1, col=1
    )
    
    # 2. Premium Multiplier by Risk Level
    for risk_level in df['combined_risk_level'].unique():
        risk_data = df[df['combined_risk_level'] == risk_level]
        fig.add_trace(
            go.Box(y=risk_data['combined_multiplier'], name=risk_level),
            row=1, col=2
        )
    
    # 3. Risk Score vs Premium Impact
    fig.add_trace(
        go.Scatter(
            x=df['combined_risk_score'],
            y=df['combined_multiplier'],
            mode='markers',
            text=df['combined_risk_level'],
            marker=dict(
                color=df['combined_risk_score'],
                colorscale='Reds',
                size=8
            ),
            name="Risk vs Premium"
        ),
        row=2, col=1
    )
    
    # 4. Geographic Risk Spread
    lat_spread = df.groupby(pd.cut(df['latitude'], bins=5))['combined_risk_score'].mean()
    fig.add_trace(
        go.Bar(x=[str(x) for x in lat_spread.index], y=lat_spread.values,
               name="Avg Risk by Latitude", marker_color='orange'),
        row=2, col=2
    )
    
    # Update layout
    fig.update_layout(
        title="Insurance Portfolio Risk Analysis",
        height=800,
        showlegend=False
    )
    
    # Add axis labels
    fig.update_xaxes(title_text="Risk Level", row=1, col=1)
    fig.update_yaxes(title_text="Percentage", row=1, col=1)
    fig.update_yaxes(title_text="Premium Multiplier", row=1, col=2)
    fig.update_xaxes(title_text="Risk Score", row=2, col=1)
    fig.update_yaxes(title_text="Premium Multiplier", row=2, col=1)
    fig.update_xaxes(title_text="Latitude Range", row=2, col=2)
    fig.update_yaxes(title_text="Avg Risk Score", row=2, col=2)
    
    return fig, portfolio_stats

portfolio_fig, portfolio_metrics = create_portfolio_analysis(combined_risk_pd)
portfolio_fig.show()

print("\nPortfolio Risk Metrics:")
for metric, value in portfolio_metrics.items():
    print(f"{metric}: {value:.3f}" if isinstance(value, float) else f"{metric}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Risk Report Generation

# COMMAND ----------

def generate_risk_summary_report(df, portfolio_stats):
    """Generate comprehensive risk summary report"""
    
    report_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    report = f"""
CLIMATE RISK ASSESSMENT SUMMARY REPORT
=====================================
Generated: {report_date}

PORTFOLIO OVERVIEW
------------------
Total Locations Analyzed: {portfolio_stats['total_locations']:,}
Average Risk Score: {portfolio_stats['avg_risk_score']:.3f}
Maximum Risk Score: {portfolio_stats['max_risk_score']:.3f}
Risk Score Standard Deviation: {portfolio_stats['risk_std']:.3f}

HIGH-RISK LOCATIONS
-------------------
High-Risk Location Count: {portfolio_stats['high_risk_count']}
Percentage of High-Risk: {(portfolio_stats['high_risk_count']/portfolio_stats['total_locations']*100):.1f}%

PREMIUM IMPACT ANALYSIS
-----------------------
Average Premium Multiplier: {portfolio_stats['avg_premium_multiplier']:.2f}x
Total Premium Adjustment: {portfolio_stats['total_premium_adjustment']:.1f}
Estimated Additional Premium: ${portfolio_stats['total_premium_adjustment']*1000:.0f} (per $1,000 base premium)

RISK DISTRIBUTION BY LEVEL
---------------------------
"""
    
    risk_dist = df['combined_risk_level'].value_counts()
    for level, count in risk_dist.items():
        percentage = (count / len(df)) * 100
        report += f"{level.upper()}: {count} locations ({percentage:.1f}%)\n"
    
    report += f"""

PRIMARY RISK FACTORS
--------------------
"""
    primary_risk = df['primary_risk_type'].value_counts()
    for risk_type, count in primary_risk.items():
        percentage = (count / len(df)) * 100
        report += f"{risk_type.upper()}: {count} locations ({percentage:.1f}%)\n"
    
    report += f"""

GEOGRAPHIC CONCENTRATION
------------------------
Unique H3 Cells: {df['h3_cell'].nunique()}
Average Locations per Cell: {len(df)/df['h3_cell'].nunique():.1f}

TOP 5 HIGHEST RISK LOCATIONS
-----------------------------
"""
    
    top_risk = df.nlargest(5, 'combined_risk_score')[['latitude', 'longitude', 'combined_risk_score', 'combined_risk_level', 'combined_multiplier']]
    for idx, row in top_risk.iterrows():
        report += f"Location: {row['latitude']:.4f}, {row['longitude']:.4f} | Risk: {row['combined_risk_score']:.3f} | Level: {row['combined_risk_level']} | Multiplier: {row['combined_multiplier']:.2f}x\n"
    
    report += f"""

RECOMMENDATIONS
---------------
1. Implement enhanced monitoring for {portfolio_stats['high_risk_count']} high-risk locations
2. Consider risk mitigation requirements for locations with multipliers > 2.0x
3. Review coverage terms for areas with combined risk scores > 0.7
4. Establish regular risk reassessment schedule (quarterly for high-risk areas)
5. Consider geographic diversification to reduce portfolio concentration

RISK MODEL VALIDATION
----------------------
The models incorporate multiple climate variables and geospatial analysis:
- Drought model: precipitation, temperature, soil moisture, vegetation indices
- Flood model: elevation, slope, drainage, precipitation intensity, historical events
- Combined scoring uses weighted factors and H3 hexagonal spatial indexing

Next Assessment: Recommended within 90 days
"""
    
    return report

# Generate and display report
risk_report = generate_risk_summary_report(combined_risk_pd, portfolio_metrics)
print(risk_report)

# Save report
report_path = "/dbfs/mnt/risk-models/climate_risk_assessment_report.txt"
with open(report_path, 'w') as f:
    f.write(risk_report)

print(f"\nRisk assessment report saved to: {report_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Visualization Data

# COMMAND ----------

# Export data for external visualization tools
export_data = combined_risk_pd[['h3_cell', 'latitude', 'longitude', 'combined_risk_score', 
                               'combined_risk_level', 'drought_risk', 'flood_risk', 
                               'primary_risk_type', 'combined_multiplier']].copy()

# Save to various formats
export_data.to_csv("/dbfs/mnt/risk-models/climate_risk_visualization_data.csv", index=False)
export_data.to_parquet("/dbfs/mnt/risk-models/climate_risk_visualization_data.parquet", index=False)

# Create GeoJSON for mapping applications
geojson_features = []
for _, row in export_data.iterrows():
    feature = {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [row['longitude'], row['latitude']]
        },
        "properties": {
            "h3_cell": row['h3_cell'],
            "combined_risk_score": row['combined_risk_score'],
            "combined_risk_level": row['combined_risk_level'],
            "drought_risk": row['drought_risk'],
            "flood_risk": row['flood_risk'],
            "primary_risk_type": row['primary_risk_type'],
            "combined_multiplier": row['combined_multiplier']
        }
    }
    geojson_features.append(feature)

geojson_data = {
    "type": "FeatureCollection",
    "features": geojson_features
}

# Save GeoJSON
with open("/dbfs/mnt/risk-models/climate_risk_data.geojson", 'w') as f:
    json.dump(geojson_data, f)

print("✅ Visualization dashboard completed!")
print(f"- Generated interactive maps and charts for {len(combined_risk_pd)} locations")
print(f"- Created comprehensive risk analysis dashboard")
print(f"- Exported data in multiple formats (CSV, Parquet, GeoJSON)")
print(f"- Generated detailed risk assessment report")

# Display final summary
print(f"\nFinal Portfolio Summary:")
print(f"High-Risk Locations: {portfolio_metrics['high_risk_count']} ({portfolio_metrics['high_risk_count']/portfolio_metrics['total_locations']*100:.1f}%)")
print(f"Average Risk Score: {portfolio_metrics['avg_risk_score']:.3f}")
print(f"Average Premium Multiplier: {portfolio_metrics['avg_premium_multiplier']:.2f}x")
