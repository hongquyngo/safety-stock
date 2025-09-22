# utils/safety_stock/calculations.py
"""
Safety Stock Calculation Methods
Implements various calculation methods for determining optimal safety stock levels
"""

import math
import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple, List
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

def get_z_score(service_level_percent: float) -> float:
    """
    Get Z-score for a given service level percentage
    
    Args:
        service_level_percent: Target service level (e.g., 95.0 for 95%)
    
    Returns:
        Z-score value
    """
    # If exact match exists, return it
    if service_level_percent in Z_SCORE_MAP:
        return Z_SCORE_MAP[service_level_percent]
    
    # Otherwise, find the closest value
    closest_level = min(Z_SCORE_MAP.keys(), 
                       key=lambda x: abs(x - service_level_percent))
    return Z_SCORE_MAP[closest_level]


# ==================== Historical Demand Analysis ====================

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
        entity_id: Entity ID (seller company / legal entity)
        customer_id: Optional customer ID (buyer company)
        days_back: Number of days to analyze
        exclude_outliers: Whether to exclude statistical outliers
    
    Returns:
        Dictionary with demand statistics
    """
    try:
        engine = get_db_engine()
        
        # Query demand from delivery requests (actual customer demand)
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
            logger.warning(f"No historical demand data found for product {product_id}")
            return {
                'avg_daily_demand': 0,
                'std_deviation': 0,
                'max_demand': 0,
                'min_demand': 0,
                'coefficient_variation': 0,
                'data_points': 0
            }
        
        # Convert to time series with all dates
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
        
        # Remove outliers if requested
        if exclude_outliers and len(df) > 10:
            Q1 = df['daily_demand'].quantile(0.25)
            Q3 = df['daily_demand'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            df = df[(df['daily_demand'] >= lower_bound) & 
                   (df['daily_demand'] <= upper_bound)]
        
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
        return {
            'avg_daily_demand': 0,
            'std_deviation': 0,
            'max_demand': 0,
            'min_demand': 0,
            'coefficient_variation': 0,
            'data_points': 0
        }


# ==================== Calculation Methods ====================

def calculate_fixed(
    safety_stock_qty: float,
    **kwargs
) -> Dict:
    """
    FIXED method - Manual input, no calculation
    
    Args:
        safety_stock_qty: Manually set safety stock quantity
        **kwargs: Additional parameters (ignored)
    
    Returns:
        Calculation result dictionary
    """
    return {
        'method': 'FIXED',
        'safety_stock_qty': safety_stock_qty,
        'formula_used': 'Manual Input',
        'calculation_notes': 'Safety stock quantity manually specified',
        'parameters': {
            'manual_qty': safety_stock_qty
        }
    }


def calculate_days_of_supply(
    safety_days: int,
    avg_daily_demand: float,
    product_id: int = None,
    entity_id: int = None,
    **kwargs
) -> Dict:
    """
    DAYS_OF_SUPPLY method - safety_days × average_daily_demand
    
    Args:
        safety_days: Number of days of supply to maintain
        avg_daily_demand: Average daily demand
        product_id: Optional product ID for historical analysis
        entity_id: Optional entity ID for historical analysis
        **kwargs: Additional parameters
    
    Returns:
        Calculation result dictionary
    """
    # If avg_daily_demand not provided, calculate from history
    if avg_daily_demand == 0 and product_id and entity_id:
        demand_stats = get_historical_demand(
            product_id, 
            entity_id,
            kwargs.get('customer_id'),
            kwargs.get('historical_days', 90)
        )
        avg_daily_demand = demand_stats['avg_daily_demand']
    
    safety_stock_qty = safety_days * avg_daily_demand
    
    return {
        'method': 'DAYS_OF_SUPPLY',
        'safety_stock_qty': round(safety_stock_qty, 2),
        'formula_used': f'Safety Stock = {safety_days} days × {avg_daily_demand:.2f} units/day',
        'calculation_notes': f'Maintains {safety_days} days of average demand as safety buffer',
        'parameters': {
            'safety_days': safety_days,
            'avg_daily_demand': avg_daily_demand
        }
    }


def calculate_demand_percentage(
    demand_percentage: float,
    avg_period_demand: float,
    period_days: int = 30,
    product_id: int = None,
    entity_id: int = None,
    **kwargs
) -> Dict:
    """
    DEMAND_PERCENTAGE method - percentage × average_period_demand
    
    Args:
        demand_percentage: Percentage of period demand (e.g., 25 for 25%)
        avg_period_demand: Average demand for the period
        period_days: Number of days in the period (default 30)
        product_id: Optional product ID for historical analysis
        entity_id: Optional entity ID for historical analysis
        **kwargs: Additional parameters
    
    Returns:
        Calculation result dictionary
    """
    # If avg_period_demand not provided, calculate from history
    if avg_period_demand == 0 and product_id and entity_id:
        demand_stats = get_historical_demand(
            product_id,
            entity_id,
            kwargs.get('customer_id'),
            kwargs.get('historical_days', 90)
        )
        avg_period_demand = demand_stats['avg_daily_demand'] * period_days
    
    safety_stock_qty = (demand_percentage / 100) * avg_period_demand
    
    return {
        'method': 'DEMAND_PERCENTAGE',
        'safety_stock_qty': round(safety_stock_qty, 2),
        'formula_used': f'Safety Stock = {demand_percentage}% × {avg_period_demand:.2f} units',
        'calculation_notes': f'Maintains {demand_percentage}% of {period_days}-day demand as buffer',
        'parameters': {
            'demand_percentage': demand_percentage,
            'avg_period_demand': avg_period_demand,
            'period_days': period_days
        }
    }


def calculate_lead_time_based(
    lead_time_days: int,
    service_level_percent: float,
    demand_std_deviation: float = None,
    lead_time_variability: int = 0,
    avg_daily_demand: float = None,
    product_id: int = None,
    entity_id: int = None,
    **kwargs
) -> Dict:
    """
    LEAD_TIME_BASED method - Z-score × √lead_time × demand_std_deviation
    Considers both demand and lead time variability
    
    Args:
        lead_time_days: Average lead time in days
        service_level_percent: Target service level (e.g., 95 for 95%)
        demand_std_deviation: Standard deviation of daily demand
        lead_time_variability: Standard deviation of lead time in days
        avg_daily_demand: Average daily demand
        product_id: Optional product ID for historical analysis
        entity_id: Optional entity ID for historical analysis
        **kwargs: Additional parameters
    
    Returns:
        Calculation result dictionary
    """
    # Get demand statistics if not provided
    if (demand_std_deviation is None or avg_daily_demand is None) and product_id and entity_id:
        demand_stats = get_historical_demand(
            product_id,
            entity_id,
            kwargs.get('customer_id'),
            kwargs.get('historical_days', 90)
        )
        if demand_std_deviation is None:
            demand_std_deviation = demand_stats['std_deviation']
        if avg_daily_demand is None:
            avg_daily_demand = demand_stats['avg_daily_demand']
    
    # Get Z-score for service level
    z_score = get_z_score(service_level_percent)
    
    # Calculate safety stock with lead time variability
    if lead_time_variability > 0 and avg_daily_demand:
        # Account for both demand and lead time variability
        # SS = Z × √(LT × σ_D² + D² × σ_LT²)
        variance_demand = lead_time_days * (demand_std_deviation ** 2)
        variance_lead = (avg_daily_demand ** 2) * (lead_time_variability ** 2)
        combined_std = math.sqrt(variance_demand + variance_lead)
        safety_stock_qty = z_score * combined_std
        
        formula = f'SS = {z_score:.2f} × √({lead_time_days} × {demand_std_deviation:.2f}² + {avg_daily_demand:.2f}² × {lead_time_variability}²)'
    else:
        # Simple formula without lead time variability
        # SS = Z × √LT × σ_D
        safety_stock_qty = z_score * math.sqrt(lead_time_days) * demand_std_deviation
        formula = f'SS = {z_score:.2f} × √{lead_time_days} × {demand_std_deviation:.2f}'
    
    return {
        'method': 'LEAD_TIME_BASED',
        'safety_stock_qty': round(safety_stock_qty, 2),
        'formula_used': formula,
        'calculation_notes': f'Statistical safety stock for {service_level_percent}% service level with {lead_time_days} days lead time',
        'parameters': {
            'lead_time_days': lead_time_days,
            'service_level_percent': service_level_percent,
            'z_score': z_score,
            'demand_std_deviation': demand_std_deviation,
            'lead_time_variability': lead_time_variability,
            'avg_daily_demand': avg_daily_demand
        }
    }


def calculate_min_max(
    min_stock_qty: float,
    max_stock_qty: float,
    target_fill_rate: float = 0.5,
    **kwargs
) -> Dict:
    """
    MIN_MAX method - Maintain inventory between min and max levels
    Safety stock = min level
    
    Args:
        min_stock_qty: Minimum stock level (safety stock)
        max_stock_qty: Maximum stock level
        target_fill_rate: Position between min and max (0-1, default 0.5)
        **kwargs: Additional parameters
    
    Returns:
        Calculation result dictionary
    """
    safety_stock_qty = min_stock_qty
    reorder_point = min_stock_qty
    reorder_qty = max_stock_qty - min_stock_qty
    
    return {
        'method': 'MIN_MAX',
        'safety_stock_qty': round(safety_stock_qty, 2),
        'min_stock_qty': min_stock_qty,
        'max_stock_qty': max_stock_qty,
        'reorder_point': reorder_point,
        'reorder_qty': round(reorder_qty, 2),
        'formula_used': f'Safety Stock = Min Level ({min_stock_qty})',
        'calculation_notes': f'Min-Max system: Reorder at {min_stock_qty}, order up to {max_stock_qty}',
        'parameters': {
            'min_stock_qty': min_stock_qty,
            'max_stock_qty': max_stock_qty,
            'reorder_qty': reorder_qty
        }
    }


def calculate_statistical(
    product_id: int,
    entity_id: int,
    service_level_percent: float = 95.0,
    lead_time_days: int = 7,
    review_period_days: int = 7,
    historical_days: int = 180,
    seasonality_adjusted: bool = False,
    **kwargs
) -> Dict:
    """
    STATISTICAL method - Advanced statistical model with seasonality and trends
    
    Args:
        product_id: Product ID
        entity_id: Entity ID
        service_level_percent: Target service level
        lead_time_days: Lead time in days
        review_period_days: Review cycle in days
        historical_days: Days of history to analyze
        seasonality_adjusted: Apply seasonal adjustments
        **kwargs: Additional parameters
    
    Returns:
        Calculation result dictionary
    """
    try:
        # Get historical demand
        demand_stats = get_historical_demand(
            product_id,
            entity_id,
            kwargs.get('customer_id'),
            historical_days,
            exclude_outliers=True
        )
        
        if demand_stats['data_points'] < 30:
            # Fall back to simpler method if insufficient data
            return calculate_lead_time_based(
                lead_time_days=lead_time_days,
                service_level_percent=service_level_percent,
                demand_std_deviation=demand_stats['std_deviation'],
                avg_daily_demand=demand_stats['avg_daily_demand']
            )
        
        # Calculate protection period
        protection_period = lead_time_days + review_period_days
        
        # Get Z-score
        z_score = get_z_score(service_level_percent)
        
        # Calculate demand during protection period
        avg_demand_protection = demand_stats['avg_daily_demand'] * protection_period
        
        # Calculate standard deviation during protection period
        std_dev_protection = demand_stats['std_deviation'] * math.sqrt(protection_period)
        
        # Apply seasonality factor if requested
        seasonality_factor = 1.0
        if seasonality_adjusted:
            # Simple seasonality: increase by 20% during peak months
            current_month = datetime.now().month
            peak_months = kwargs.get('peak_months', [11, 12, 1])  # Nov, Dec, Jan
            if current_month in peak_months:
                seasonality_factor = 1.2
        
        # Calculate safety stock
        safety_stock_qty = z_score * std_dev_protection * seasonality_factor
        
        # Calculate reorder point
        reorder_point = avg_demand_protection + safety_stock_qty
        
        # Economic Order Quantity (EOQ) - simplified
        holding_cost = kwargs.get('holding_cost_percent', 20) / 100  # Annual holding cost
        ordering_cost = kwargs.get('ordering_cost', 50)  # Cost per order
        annual_demand = demand_stats['avg_daily_demand'] * 365
        
        if annual_demand > 0 and holding_cost > 0:
            eoq = math.sqrt((2 * annual_demand * ordering_cost) / holding_cost)
        else:
            eoq = demand_stats['avg_daily_demand'] * 30  # Default to 30 days
        
        return {
            'method': 'STATISTICAL',
            'safety_stock_qty': round(safety_stock_qty, 2),
            'reorder_point': round(reorder_point, 2),
            'reorder_qty': round(eoq, 2),
            'formula_used': f'SS = {z_score:.2f} × {std_dev_protection:.2f} × {seasonality_factor:.1f}',
            'calculation_notes': f'Advanced model with {protection_period}-day protection period, {service_level_percent}% service level' +
                               (f', seasonality factor {seasonality_factor}' if seasonality_adjusted else ''),
            'parameters': {
                'service_level_percent': service_level_percent,
                'z_score': z_score,
                'lead_time_days': lead_time_days,
                'review_period_days': review_period_days,
                'protection_period': protection_period,
                'avg_daily_demand': demand_stats['avg_daily_demand'],
                'std_deviation': demand_stats['std_deviation'],
                'cv': demand_stats['coefficient_variation'],
                'seasonality_factor': seasonality_factor,
                'eoq': round(eoq, 2)
            }
        }
        
    except Exception as e:
        logger.error(f"Error in statistical calculation: {e}")
        # Fall back to simpler method
        return calculate_lead_time_based(
            lead_time_days=lead_time_days,
            service_level_percent=service_level_percent,
            **kwargs
        )


# ==================== Main Calculation Router ====================

def calculate_safety_stock(method: str, **params) -> Dict:
    """
    Route to appropriate calculation method
    
    Args:
        method: Calculation method name
        **params: Method-specific parameters
    
    Returns:
        Calculation result dictionary
    """
    method_map = {
        'FIXED': calculate_fixed,
        'DAYS_OF_SUPPLY': calculate_days_of_supply,
        'DEMAND_PERCENTAGE': calculate_demand_percentage,
        'LEAD_TIME_BASED': calculate_lead_time_based,
        'MIN_MAX': calculate_min_max,
        'STATISTICAL': calculate_statistical
    }
    
    if method not in method_map:
        logger.error(f"Unknown calculation method: {method}")
        return {
            'method': method,
            'safety_stock_qty': 0,
            'error': f'Unknown calculation method: {method}'
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


def recommend_method(
    demand_variability: float,
    lead_time_days: int,
    data_availability: int,
    criticality: str = 'MEDIUM'
) -> str:
    """
    Recommend best calculation method based on product characteristics
    
    Args:
        demand_variability: Coefficient of variation (%)
        lead_time_days: Average lead time
        data_availability: Number of historical data points
        criticality: Product criticality (HIGH/MEDIUM/LOW)
    
    Returns:
        Recommended method name
    """
    # Insufficient data - use simple methods
    if data_availability < 30:
        return 'DAYS_OF_SUPPLY'
    
    # High criticality - use advanced methods
    if criticality == 'HIGH':
        if data_availability >= 180:
            return 'STATISTICAL'
        else:
            return 'LEAD_TIME_BASED'
    
    # Based on demand variability
    if demand_variability < 20:
        # Low variability - simple methods work
        return 'DAYS_OF_SUPPLY'
    elif demand_variability < 50:
        # Moderate variability
        if lead_time_days > 14:
            return 'LEAD_TIME_BASED'
        else:
            return 'DEMAND_PERCENTAGE'
    else:
        # High variability - need statistical methods
        if data_availability >= 90:
            return 'STATISTICAL'
        else:
            return 'LEAD_TIME_BASED'


def batch_calculate(
    product_list: List[Dict],
    default_method: str = 'AUTO',
    default_params: Dict = None
) -> List[Dict]:
    """
    Calculate safety stock for multiple products
    
    Args:
        product_list: List of products with their parameters
        default_method: Default calculation method or 'AUTO' for automatic selection
        default_params: Default parameters to apply
    
    Returns:
        List of calculation results
    """
    results = []
    default_params = default_params or {}
    
    for product in product_list:
        try:
            # Merge default params with product-specific params
            params = {**default_params, **product}
            
            # Determine method
            if default_method == 'AUTO':
                # Get demand statistics for method recommendation
                demand_stats = get_historical_demand(
                    product.get('product_id'),
                    product.get('entity_id'),
                    product.get('customer_id'),
                    params.get('historical_days', 90)
                )
                
                method = recommend_method(
                    demand_variability=demand_stats['coefficient_variation'],
                    lead_time_days=params.get('lead_time_days', 7),
                    data_availability=demand_stats['data_points'],
                    criticality=params.get('criticality', 'MEDIUM')
                )
            else:
                method = product.get('calculation_method', default_method)
            
            # Calculate safety stock
            result = calculate_safety_stock(method, **params)
            result['product_id'] = product.get('product_id')
            result['entity_id'] = product.get('entity_id')
            results.append(result)
            
        except Exception as e:
            logger.error(f"Error calculating for product {product.get('product_id')}: {e}")
            results.append({
                'product_id': product.get('product_id'),
                'entity_id': product.get('entity_id'),
                'error': str(e)
            })
    
    return results