# utils/safety_stock/calculations.py
"""
Safety Stock Calculation Methods
3 methods only: FIXED, DAYS_OF_SUPPLY, LEAD_TIME_BASED
Clean and simplified implementation
"""

import math
import pandas as pd
import numpy as np
from typing import Dict, Optional
from datetime import datetime, timedelta
from sqlalchemy import text
from ..db import get_db_engine
import logging

logger = logging.getLogger(__name__)

# Z-Score mapping for service levels
Z_SCORE_MAP = {
    90.0: 1.28,
    91.0: 1.34,
    92.0: 1.41,
    93.0: 1.48,
    94.0: 1.56,
    95.0: 1.65,
    96.0: 1.75,
    97.0: 1.88,
    98.0: 2.05,
    99.0: 2.33,
    99.5: 2.58,
    99.9: 3.09
}


def calculate_safety_stock(method: str, **params) -> Dict:
    """
    Main calculation router for safety stock
    
    Args:
        method: Calculation method (FIXED, DAYS_OF_SUPPLY, LEAD_TIME_BASED)
        **params: Method-specific parameters
    
    Returns:
        Dict with calculation results including safety_stock_qty and formula
    """
    method_map = {
        'FIXED': calculate_fixed,
        'DAYS_OF_SUPPLY': calculate_days_of_supply,
        'LEAD_TIME_BASED': calculate_lead_time_based
    }
    
    if method not in method_map:
        return {
            'method': method,
            'safety_stock_qty': 0,
            'error': f'Unknown calculation method: {method}. Use FIXED, DAYS_OF_SUPPLY, or LEAD_TIME_BASED'
        }
    
    try:
        result = method_map[method](**params)
        result['calculated_at'] = datetime.now().isoformat()
        return result
    except Exception as e:
        logger.error(f"Error in {method} calculation: {e}")
        return {
            'method': method,
            'safety_stock_qty': 0,
            'error': str(e)
        }


def calculate_fixed(safety_stock_qty: float, **kwargs) -> Dict:
    """
    FIXED method - Manual input, no calculation
    
    Args:
        safety_stock_qty: Manually specified quantity
    
    Returns:
        Calculation result dictionary
    """
    return {
        'method': 'FIXED',
        'safety_stock_qty': float(safety_stock_qty),
        'formula_used': 'Manual Input',
        'calculation_notes': 'Safety stock quantity was manually specified'
    }


def calculate_days_of_supply(
    safety_days: int,
    avg_daily_demand: float = 0,
    product_id: int = None,
    entity_id: int = None,
    customer_id: Optional[int] = None,
    **kwargs
) -> Dict:
    """
    DAYS_OF_SUPPLY method
    Formula: safety_stock = safety_days × average_daily_demand
    
    Args:
        safety_days: Number of days to cover
        avg_daily_demand: Average daily demand (optional, will calculate if not provided)
        product_id: Product ID for demand calculation
        entity_id: Entity ID for demand calculation
        customer_id: Customer ID for demand calculation
    
    Returns:
        Calculation result dictionary
    """
    # If demand not provided, calculate from history
    if avg_daily_demand == 0 and product_id and entity_id:
        demand_stats = get_historical_demand(
            product_id, 
            entity_id,
            customer_id,
            days_back=kwargs.get('historical_days', 90)
        )
        avg_daily_demand = demand_stats['avg_daily_demand']
    
    # Calculate safety stock
    safety_stock_qty = safety_days * avg_daily_demand
    
    return {
        'method': 'DAYS_OF_SUPPLY',
        'safety_stock_qty': round(safety_stock_qty, 2),
        'formula_used': f'SS = {safety_days} days × {avg_daily_demand:.2f} units/day',
        'calculation_notes': f'Maintains {safety_days} days of average demand as buffer',
        'parameters': {
            'safety_days': safety_days,
            'avg_daily_demand': round(avg_daily_demand, 2)
        }
    }


def calculate_lead_time_based(
    lead_time_days: int,
    service_level_percent: float,
    demand_std_deviation: float = None,
    avg_daily_demand: float = None,
    product_id: int = None,
    entity_id: int = None,
    customer_id: Optional[int] = None,
    **kwargs
) -> Dict:
    """
    LEAD_TIME_BASED method - Statistical safety stock
    Formula: SS = Z-score × √lead_time × demand_std_deviation
    
    Args:
        lead_time_days: Lead time in days
        service_level_percent: Target service level (90-99.9)
        demand_std_deviation: Standard deviation of demand
        avg_daily_demand: Average daily demand (for reference)
        product_id: Product ID for demand calculation
        entity_id: Entity ID for demand calculation
        customer_id: Customer ID for demand calculation
    
    Returns:
        Calculation result dictionary
    """
    # Get demand statistics if not provided
    if (demand_std_deviation is None or avg_daily_demand is None) and product_id and entity_id:
        demand_stats = get_historical_demand(
            product_id,
            entity_id,
            customer_id,
            days_back=kwargs.get('historical_days', 90)
        )
        demand_std_deviation = demand_std_deviation or demand_stats['std_deviation']
        avg_daily_demand = avg_daily_demand or demand_stats['avg_daily_demand']
    
    # Default values if still missing
    demand_std_deviation = demand_std_deviation or 0
    avg_daily_demand = avg_daily_demand or 0
    
    # Get Z-score for service level
    z_score = get_z_score(service_level_percent)
    
    # Calculate safety stock: Z × √LT × σ_demand
    safety_stock_qty = z_score * math.sqrt(lead_time_days) * demand_std_deviation
    
    formula = f'SS = {z_score:.2f} × √{lead_time_days} × {demand_std_deviation:.2f}'
    
    return {
        'method': 'LEAD_TIME_BASED',
        'safety_stock_qty': round(safety_stock_qty, 2),
        'formula_used': formula,
        'calculation_notes': f'Statistical SS for {service_level_percent}% service level over {lead_time_days} days lead time',
        'parameters': {
            'lead_time_days': lead_time_days,
            'service_level_percent': service_level_percent,
            'z_score': z_score,
            'demand_std_deviation': round(demand_std_deviation, 2),
            'avg_daily_demand': round(avg_daily_demand, 2) if avg_daily_demand else 0
        }
    }


def get_z_score(service_level_percent: float) -> float:
    """
    Get Z-score for a given service level percentage
    
    Args:
        service_level_percent: Target service level (e.g., 95.0)
    
    Returns:
        Z-score value
    """
    if service_level_percent in Z_SCORE_MAP:
        return Z_SCORE_MAP[service_level_percent]
    
    # Find the closest value
    closest_level = min(Z_SCORE_MAP.keys(), 
                       key=lambda x: abs(x - service_level_percent))
    logger.warning(f"Service level {service_level_percent}% not in map, using {closest_level}%")
    return Z_SCORE_MAP[closest_level]


def get_historical_demand(
    product_id: int, 
    entity_id: int, 
    customer_id: Optional[int] = None,
    days_back: int = 90,
    exclude_outliers: bool = True
) -> Dict:
    """
    Analyze historical demand for a product
    
    Args:
        product_id: Product ID
        entity_id: Entity ID (seller company)
        customer_id: Optional customer ID
        days_back: Number of days to analyze
        exclude_outliers: Whether to exclude statistical outliers
    
    Returns:
        Dictionary with demand statistics
    """
    default_stats = {
        'avg_daily_demand': 0,
        'std_deviation': 0,
        'max_demand': 0,
        'min_demand': 0,
        'coefficient_variation': 0,
        'data_points': 0
    }
    
    try:
        engine = get_db_engine()
        
        # Query historical demand
        query = text("""
        SELECT 
            DATE(sod.created_date) as date,
            SUM(sodrd.stock_out_request_quantity) as daily_demand
        FROM stock_out_delivery_request_details sodrd
        JOIN stock_out_delivery sod ON sodrd.delivery_id = sod.id
        WHERE sodrd.product_id = :product_id
        AND sod.seller_company_id = :entity_id
        AND sod.created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL :days_back DAY)
        AND sodrd.delete_flag = 0
        AND sod.delete_flag = 0
        """ + ("""
        AND sod.buyer_company_id = :customer_id
        """ if customer_id else "") + """
        GROUP BY DATE(sod.created_date)
        ORDER BY date
        """)
        
        params = {
            'product_id': product_id,
            'entity_id': entity_id,
            'days_back': days_back
        }
        if customer_id:
            params['customer_id'] = customer_id
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)
        
        if df.empty:
            logger.warning(f"No historical demand found for product {product_id}, entity {entity_id}")
            return default_stats
        
        # Create complete date range with zero-fill
        date_range = pd.date_range(
            start=datetime.now() - timedelta(days=days_back),
            end=datetime.now(),
            freq='D'
        )
        
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        df = df.reindex(date_range, fill_value=0)
        df.reset_index(inplace=True)
        df.columns = ['date', 'daily_demand']
        
        # Remove outliers using IQR method
        if exclude_outliers and len(df) > 10:
            Q1 = df['daily_demand'].quantile(0.25)
            Q3 = df['daily_demand'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = max(0, Q1 - 1.5 * IQR)
            upper_bound = Q3 + 1.5 * IQR
            
            before_count = len(df)
            df = df[(df['daily_demand'] >= lower_bound) & 
                   (df['daily_demand'] <= upper_bound)]
            after_count = len(df)
            
            if before_count > after_count:
                logger.info(f"Removed {before_count - after_count} outliers from demand data")
        
        # Calculate statistics
        avg_demand = df['daily_demand'].mean()
        std_dev = df['daily_demand'].std()
        cv = (std_dev / avg_demand * 100) if avg_demand > 0 else 0
        
        return {
            'avg_daily_demand': round(avg_demand, 2),
            'std_deviation': round(std_dev, 2),
            'max_demand': df['daily_demand'].max(),
            'min_demand': df['daily_demand'].min(),
            'coefficient_variation': round(cv, 2),
            'data_points': len(df)
        }
        
    except Exception as e:
        logger.error(f"Error analyzing historical demand: {e}")
        return default_stats


def recommend_method(
    demand_variability: float,
    lead_time_days: int,
    data_availability: int,
    criticality: str = 'MEDIUM'
) -> str:
    """
    Recommend best calculation method based on characteristics
    
    Args:
        demand_variability: Coefficient of variation (%)
        lead_time_days: Average lead time
        data_availability: Number of historical data points
        criticality: Product criticality (HIGH/MEDIUM/LOW)
    
    Returns:
        Recommended method name
    """
    # Insufficient data - use manual
    if data_availability < 30:
        logger.info(f"Recommending FIXED due to limited data ({data_availability} points)")
        return 'FIXED'
    
    # High criticality - use statistical if enough data
    if criticality == 'HIGH':
        if data_availability >= 90:
            return 'LEAD_TIME_BASED'
        else:
            return 'DAYS_OF_SUPPLY'
    
    # Based on demand variability
    if demand_variability < 20:
        # Low variability - simple method is sufficient
        return 'DAYS_OF_SUPPLY'
    elif demand_variability < 50:
        # Moderate variability
        if lead_time_days > 14:
            return 'LEAD_TIME_BASED'
        else:
            return 'DAYS_OF_SUPPLY'
    else:
        # High variability - use statistical if enough data
        if data_availability >= 60:
            return 'LEAD_TIME_BASED'
        else:
            return 'DAYS_OF_SUPPLY'


def get_calculation_summary(method: str, params: Dict) -> str:
    """
    Generate human-readable summary of calculation
    
    Args:
        method: Calculation method
        params: Parameters used
    
    Returns:
        Summary string
    """
    if method == 'FIXED':
        return "Manual input - no calculation performed"
    
    elif method == 'DAYS_OF_SUPPLY':
        days = params.get('safety_days', 0)
        demand = params.get('avg_daily_demand', 0)
        return f"Buffer for {days} days at {demand:.2f} units/day average demand"
    
    elif method == 'LEAD_TIME_BASED':
        lt = params.get('lead_time_days', 0)
        sl = params.get('service_level_percent', 0)
        std = params.get('demand_std_deviation', 0)
        return f"Statistical calculation for {sl}% service level with {lt}-day lead time (σ={std:.2f})"
    
    else:
        return "Unknown calculation method"