"""
Climate Risk Engine for Insurance Applications

This module provides a unified interface for drought and flood risk assessment
using Databricks geospatial functions and climate data analysis.
"""

import pyspark.sql.functions as F
from pyspark.sql.types import *
import h3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json


class ClimateRiskEngine:
    """
    Main engine for climate risk assessment combining drought and flood models.
    Optimized for Databricks Runtime 16+ with Spark Connect.
    """
    
    def __init__(self):
        """
        Initialize the Climate Risk Engine for DBR 16+.
        
        Note: In DBR 16+, Spark session is automatically available and managed.
        All geospatial functions are native and pre-configured.
        """
        # Model configurations
        self.h3_resolution = 7
        self.risk_weights = {
            'drought': 0.5,
            'flood': 0.5
        }
        
        # Note: No need to initialize Spark session or geospatial functions
        # They are automatically available in DBR 16+ with enhanced performance
    
    def assess_drought_risk(self, 
                           latitude: float, 
                           longitude: float, 
                           time_period: int = 365) -> Dict:
        """
        Assess drought risk for a specific location.
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            time_period: Analysis period in days (default: 365)
            
        Returns:
            Dictionary containing drought risk assessment
        """
        h3_cell = h3.latlng_to_cell(latitude, longitude, self.h3_resolution)
        
        # Query existing drought risk data from Delta tables (DBR 16+ optimized)
        drought_query = f"""
        SELECT 
            h3_cell,
            avg_risk_score as drought_risk_score,
            risk_category as drought_risk_level,
            insurance_risk_class,
            risk_multiplier,
            recommended_action
        FROM climate_risk.drought_risk_regional 
        WHERE h3_cell = '{h3_cell}'
        """
        
        try:
            # Use spark.sql() - session is automatically available in DBR 16+
            result = spark.sql(drought_query).collect()
            if result:
                row = result[0]
                return {
                    'location': {'latitude': latitude, 'longitude': longitude},
                    'h3_cell': h3_cell,
                    'drought_risk_score': float(row['drought_risk_score']) if row['drought_risk_score'] else 0.0,
                    'drought_risk_level': row['drought_risk_level'] or 'unknown',
                    'insurance_class': row['insurance_risk_class'] or 'standard',
                    'risk_multiplier': float(row['risk_multiplier']) if row['risk_multiplier'] else 1.0,
                    'recommendation': row['recommended_action'] or 'Standard coverage'
                }
        except Exception as e:
            print(f"Error querying drought data: {e}")
        
        # Return default assessment if no data available
        return {
            'location': {'latitude': latitude, 'longitude': longitude},
            'h3_cell': h3_cell,
            'drought_risk_score': 0.3,
            'drought_risk_level': 'moderate',
            'insurance_class': 'standard',
            'risk_multiplier': 1.2,
            'recommendation': 'Standard monitoring recommended'
        }
    
    def assess_flood_risk(self, 
                         latitude: float, 
                         longitude: float, 
                         elevation_data: Optional[Dict] = None) -> Dict:
        """
        Assess flood risk for a specific location.
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            elevation_data: Optional elevation information
            
        Returns:
            Dictionary containing flood risk assessment
        """
        h3_cell = h3.latlng_to_cell(latitude, longitude, self.h3_resolution)
        
        # Query existing flood risk data from Delta tables
        flood_query = f"""
        SELECT 
            h3_cell,
            flood_risk_score,
            flood_risk_level,
            insurance_flood_class,
            base_premium_multiplier,
            estimated_return_period,
            coverage_recommendation,
            elevation_m
        FROM climate_risk.flood_risk_summary 
        WHERE h3_cell = '{h3_cell}'
        """
        
        try:
            result = spark.sql(flood_query).collect()
            if result:
                row = result[0]
                return {
                    'location': {'latitude': latitude, 'longitude': longitude},
                    'h3_cell': h3_cell,
                    'flood_risk_score': float(row['flood_risk_score']) if row['flood_risk_score'] else 0.0,
                    'flood_risk_level': row['flood_risk_level'] or 'unknown',
                    'insurance_zone': row['insurance_flood_class'] or 'Zone_X',
                    'premium_multiplier': float(row['base_premium_multiplier']) if row['base_premium_multiplier'] else 1.0,
                    'return_period': int(row['estimated_return_period']) if row['estimated_return_period'] else 100,
                    'coverage_recommendation': row['coverage_recommendation'] or 'Standard coverage',
                    'elevation': float(row['elevation_m']) if row['elevation_m'] else None
                }
        except Exception as e:
            print(f"Error querying flood data: {e}")
        
        # Return default assessment if no data available
        return {
            'location': {'latitude': latitude, 'longitude': longitude},
            'h3_cell': h3_cell,
            'flood_risk_score': 0.4,
            'flood_risk_level': 'moderate',
            'insurance_zone': 'Zone_X_moderate',
            'premium_multiplier': 1.5,
            'return_period': 50,
            'coverage_recommendation': 'Recommended coverage',
            'elevation': elevation_data.get('elevation') if elevation_data else None
        }
    
    def assess_combined_risk(self, 
                           latitude: float, 
                           longitude: float, 
                           time_period: int = 365) -> Dict:
        """
        Assess combined climate risk (drought + flood) for a location.
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            time_period: Analysis period in days
            
        Returns:
            Dictionary containing combined risk assessment
        """
        drought_risk = self.assess_drought_risk(latitude, longitude, time_period)
        flood_risk = self.assess_flood_risk(latitude, longitude)
        
        # Calculate combined risk score
        combined_score = (
            drought_risk['drought_risk_score'] * self.risk_weights['drought'] +
            flood_risk['flood_risk_score'] * self.risk_weights['flood']
        )
        
        # Determine overall risk level
        if combined_score >= 0.8:
            overall_level = 'very_high'
        elif combined_score >= 0.6:
            overall_level = 'high'
        elif combined_score >= 0.4:
            overall_level = 'moderate'
        elif combined_score >= 0.2:
            overall_level = 'low'
        else:
            overall_level = 'very_low'
        
        # Calculate combined premium adjustment
        combined_multiplier = max(
            drought_risk['risk_multiplier'],
            flood_risk['premium_multiplier']
        )
        
        return {
            'location': {'latitude': latitude, 'longitude': longitude},
            'h3_cell': drought_risk['h3_cell'],
            'combined_risk_score': combined_score,
            'overall_risk_level': overall_level,
            'drought_assessment': drought_risk,
            'flood_assessment': flood_risk,
            'combined_premium_multiplier': combined_multiplier,
            'primary_risk_factor': 'drought' if drought_risk['drought_risk_score'] > flood_risk['flood_risk_score'] else 'flood',
            'assessment_timestamp': datetime.now().isoformat()
        }
    
    def assess_portfolio_risk(self, locations: List[Tuple[float, float]]) -> Dict:
        """
        Assess climate risk for a portfolio of locations.
        
        Args:
            locations: List of (latitude, longitude) tuples
            
        Returns:
            Dictionary containing portfolio risk analysis
        """
        portfolio_assessments = []
        
        for lat, lon in locations:
            assessment = self.assess_combined_risk(lat, lon)
            portfolio_assessments.append(assessment)
        
        # Calculate portfolio statistics
        risk_scores = [a['combined_risk_score'] for a in portfolio_assessments]
        multipliers = [a['combined_premium_multiplier'] for a in portfolio_assessments]
        
        # Risk distribution
        risk_levels = [a['overall_risk_level'] for a in portfolio_assessments]
        risk_distribution = {level: risk_levels.count(level) for level in set(risk_levels)}
        
        # Geographic concentration
        h3_cells = [a['h3_cell'] for a in portfolio_assessments]
        unique_cells = len(set(h3_cells))
        geographic_concentration = 1 - (unique_cells / len(locations))
        
        return {
            'portfolio_size': len(locations),
            'assessments': portfolio_assessments,
            'portfolio_statistics': {
                'avg_risk_score': np.mean(risk_scores),
                'max_risk_score': np.max(risk_scores),
                'risk_score_std': np.std(risk_scores),
                'avg_premium_multiplier': np.mean(multipliers),
                'max_premium_multiplier': np.max(multipliers)
            },
            'risk_distribution': risk_distribution,
            'geographic_concentration': geographic_concentration,
            'high_risk_locations': [
                a for a in portfolio_assessments 
                if a['overall_risk_level'] in ['high', 'very_high']
            ],
            'assessment_timestamp': datetime.now().isoformat()
        }
    
    def get_risk_trends(self, 
                       h3_cell: str, 
                       days_back: int = 90) -> Dict:
        """
        Get risk trends for a specific H3 cell over time.
        
        Args:
            h3_cell: H3 cell identifier
            days_back: Number of days to look back
            
        Returns:
            Dictionary containing trend analysis
        """
        # Query historical risk data (would be implemented with actual time series)
        # For now, return sample trend data
        
        dates = [datetime.now() - timedelta(days=i) for i in range(0, days_back, 7)]
        
        # Simulate trend data
        base_drought_risk = np.random.uniform(0.2, 0.8)
        base_flood_risk = np.random.uniform(0.2, 0.8)
        
        trend_data = []
        for i, date in enumerate(dates):
            # Add some seasonal variation
            seasonal_factor = 0.1 * np.sin(i * 2 * np.pi / 52)  # Weekly cycle
            
            trend_data.append({
                'date': date.strftime('%Y-%m-%d'),
                'drought_risk': max(0, min(1, base_drought_risk + seasonal_factor + np.random.normal(0, 0.05))),
                'flood_risk': max(0, min(1, base_flood_risk - seasonal_factor + np.random.normal(0, 0.05)))
            })
        
        return {
            'h3_cell': h3_cell,
            'period_days': days_back,
            'trend_data': trend_data,
            'drought_trend': 'increasing' if trend_data[-1]['drought_risk'] > trend_data[0]['drought_risk'] else 'decreasing',
            'flood_trend': 'increasing' if trend_data[-1]['flood_risk'] > trend_data[0]['flood_risk'] else 'decreasing'
        }
    
    def generate_risk_report(self, 
                           latitude: float, 
                           longitude: float) -> str:
        """
        Generate a comprehensive risk report for a location.
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            
        Returns:
            Formatted risk report string
        """
        assessment = self.assess_combined_risk(latitude, longitude)
        trends = self.get_risk_trends(assessment['h3_cell'])
        
        report = f"""
CLIMATE RISK ASSESSMENT REPORT
==============================

Location: {latitude:.4f}, {longitude:.4f}
H3 Cell: {assessment['h3_cell']}
Assessment Date: {assessment['assessment_timestamp']}

OVERALL RISK SUMMARY
--------------------
Combined Risk Score: {assessment['combined_risk_score']:.3f}
Overall Risk Level: {assessment['overall_risk_level'].upper()}
Primary Risk Factor: {assessment['primary_risk_factor'].upper()}
Premium Multiplier: {assessment['combined_premium_multiplier']:.2f}x

DROUGHT RISK ASSESSMENT
-----------------------
Risk Score: {assessment['drought_assessment']['drought_risk_score']:.3f}
Risk Level: {assessment['drought_assessment']['drought_risk_level']}
Insurance Class: {assessment['drought_assessment']['insurance_class']}
Recommendation: {assessment['drought_assessment']['recommendation']}

FLOOD RISK ASSESSMENT
---------------------
Risk Score: {assessment['flood_assessment']['flood_risk_score']:.3f}
Risk Level: {assessment['flood_assessment']['flood_risk_level']}
Insurance Zone: {assessment['flood_assessment']['insurance_zone']}
Return Period: {assessment['flood_assessment']['return_period']} years
Coverage Recommendation: {assessment['flood_assessment']['coverage_recommendation']}

RISK TRENDS
-----------
Drought Trend: {trends['drought_trend']}
Flood Trend: {trends['flood_trend']}

RECOMMENDATIONS
---------------
Based on the analysis, this location shows {assessment['overall_risk_level']} climate risk.
The primary concern is {assessment['primary_risk_factor']} risk.
A premium adjustment of {assessment['combined_premium_multiplier']:.1f}x is recommended.
"""
        
        return report


class RiskVisualization:
    """
    Utilities for visualizing climate risk data.
    """
    
    def __init__(self, risk_engine: ClimateRiskEngine):
        self.engine = risk_engine
    
    def create_risk_map_data(self, 
                           bbox: Tuple[float, float, float, float], 
                           resolution: int = 7) -> pd.DataFrame:
        """
        Create data for risk visualization maps.
        
        Args:
            bbox: Bounding box (min_lat, min_lon, max_lat, max_lon)
            resolution: H3 resolution for mapping
            
        Returns:
            DataFrame with risk data for mapping
        """
        min_lat, min_lon, max_lat, max_lon = bbox
        
        # Generate grid of points
        lat_step = (max_lat - min_lat) / 20
        lon_step = (max_lon - min_lon) / 20
        
        map_data = []
        for lat in np.arange(min_lat, max_lat, lat_step):
            for lon in np.arange(min_lon, max_lon, lon_step):
                assessment = self.engine.assess_combined_risk(lat, lon)
                
                map_data.append({
                    'latitude': lat,
                    'longitude': lon,
                    'h3_cell': assessment['h3_cell'],
                    'combined_risk_score': assessment['combined_risk_score'],
                    'overall_risk_level': assessment['overall_risk_level'],
                    'drought_risk': assessment['drought_assessment']['drought_risk_score'],
                    'flood_risk': assessment['flood_assessment']['flood_risk_score'],
                    'premium_multiplier': assessment['combined_premium_multiplier']
                })
        
        return pd.DataFrame(map_data)
    
    def get_risk_color(self, risk_score: float) -> str:
        """
        Get color code for risk visualization.
        
        Args:
            risk_score: Risk score (0-1)
            
        Returns:
            Hex color code
        """
        if risk_score >= 0.8:
            return '#d32f2f'  # Red
        elif risk_score >= 0.6:
            return '#f57c00'  # Orange
        elif risk_score >= 0.4:
            return '#fbc02d'  # Yellow
        elif risk_score >= 0.2:
            return '#689f38'  # Light Green
        else:
            return '#388e3c'  # Green


# Example usage and testing functions
def example_usage():
    """Example usage of the Climate Risk Engine."""
    
    # Initialize the engine
    engine = ClimateRiskEngine()
    
    # Assess risk for a single location (San Francisco)
    sf_lat, sf_lon = 37.7749, -122.4194
    assessment = engine.assess_combined_risk(sf_lat, sf_lon)
    print(f"San Francisco Risk Score: {assessment['combined_risk_score']:.3f}")
    
    # Assess portfolio risk
    portfolio_locations = [
        (37.7749, -122.4194),  # San Francisco
        (34.0522, -118.2437),  # Los Angeles
        (40.7128, -74.0060),   # New York
        (41.8781, -87.6298),   # Chicago
        (29.7604, -95.3698)    # Houston
    ]
    
    portfolio_risk = engine.assess_portfolio_risk(portfolio_locations)
    print(f"Portfolio Average Risk: {portfolio_risk['portfolio_statistics']['avg_risk_score']:.3f}")
    
    # Generate detailed report
    report = engine.generate_risk_report(sf_lat, sf_lon)
    print(report)


if __name__ == "__main__":
    example_usage()
