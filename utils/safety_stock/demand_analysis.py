# utils/safety_stock/demand_analysis.py
"""
Demand Analysis Module for Safety Stock Management
Fetches and analyzes historical demand from delivery_full_view
Provides reference data for safety stock calculations
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
from sqlalchemy import text
from ..db import get_db_engine
import logging

logger = logging.getLogger(__name__)


def fetch_demand_stats(
    product_id: int,
    entity_id: int, 
    customer_id: Optional[int] = None,
    days_back: int = 90,
    exclude_pending: bool = True
) -> Dict:
    """
    Fetch demand statistics from delivery_full_view
    
    Args:
        product_id: Product ID
        entity_id: Legal entity ID  
        customer_id: Optional customer ID (None = all customers)
        days_back: Number of days to analyze
        exclude_pending: Exclude deliveries with PENDING status
        
    Returns:
        Dictionary with demand statistics for reference
    """
    try:
        engine = get_db_engine()
        
        # Build query conditions
        conditions = [
            "product_id = :product_id",
            "legal_entity_code = (SELECT company_code FROM companies WHERE id = :entity_id)"
        ]
        
        if customer_id:
            conditions.append("customer_code = (SELECT company_code FROM companies WHERE id = :customer_id)")
        
        if exclude_pending:
            conditions.append("shipment_status != 'PENDING'")
        
        # Date range condition
        conditions.append("sto_etd_date >= DATE_SUB(CURRENT_DATE(), INTERVAL :days_back DAY)")
        conditions.append("sto_etd_date IS NOT NULL")
        
        where_clause = " AND ".join(conditions)
        
        # Query for daily demand aggregation
        query = text(f"""
        WITH daily_demand AS (
            SELECT 
                DATE(sto_etd_date) as demand_date,
                SUM(stock_out_request_quantity) as daily_quantity
            FROM delivery_full_view
            WHERE {where_clause}
                AND stock_out_request_quantity > 0
            GROUP BY DATE(sto_etd_date)
        ),
        demand_stats AS (
            SELECT 
                AVG(daily_quantity) as avg_daily_demand,
                STDDEV(daily_quantity) as demand_std_dev,
                MAX(daily_quantity) as max_daily_demand,
                MIN(daily_quantity) as min_daily_demand,
                COUNT(*) as data_points
            FROM daily_demand
        )
        SELECT 
            COALESCE(avg_daily_demand, 0) as avg_daily_demand,
            COALESCE(demand_std_dev, 0) as demand_std_dev,
            COALESCE(max_daily_demand, 0) as max_daily_demand,
            COALESCE(min_daily_demand, 0) as min_daily_demand,
            COALESCE(data_points, 0) as data_points,
            -- Calculate coefficient of variation
            CASE 
                WHEN avg_daily_demand > 0 
                THEN (demand_std_dev / avg_daily_demand * 100)
                ELSE 0
            END as cv_percent
        FROM demand_stats
        """)
        
        params = {
            'product_id': product_id,
            'entity_id': entity_id,
            'days_back': days_back
        }
        if customer_id:
            params['customer_id'] = customer_id
        
        with engine.connect() as conn:
            result = conn.execute(query, params).fetchone()
        
        if result:
            stats = dict(result._mapping)
            
            # Round values for display
            stats['avg_daily_demand'] = round(float(stats['avg_daily_demand']), 2)
            stats['demand_std_dev'] = round(float(stats['demand_std_dev']), 2)
            stats['cv_percent'] = round(float(stats['cv_percent']), 1)
            stats['data_points'] = int(stats['data_points'])
            
            # Add metadata
            stats['fetch_date'] = datetime.now().strftime('%Y-%m-%d %H:%M')
            stats['days_analyzed'] = days_back
            stats['customer_specific'] = customer_id is not None
            
            # Add method suggestion based on CV%
            stats['suggested_method'] = suggest_calculation_method(stats['cv_percent'], stats['data_points'])
            
            return stats
        else:
            return get_empty_stats()
            
    except Exception as e:
        logger.error(f"Error fetching demand stats: {e}")
        return get_empty_stats()


def get_empty_stats() -> Dict:
    """Return empty statistics structure"""
    return {
        'avg_daily_demand': 0.0,
        'demand_std_dev': 0.0,
        'max_daily_demand': 0.0,
        'min_daily_demand': 0.0,
        'data_points': 0,
        'cv_percent': 0.0,
        'fetch_date': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'days_analyzed': 0,
        'customer_specific': False,
        'suggested_method': 'FIXED'
    }


def suggest_calculation_method(cv_percent: float, data_points: int) -> str:
    """
    Suggest best calculation method based on demand variability
    
    Args:
        cv_percent: Coefficient of variation (%)
        data_points: Number of data points available
        
    Returns:
        Suggested method: 'FIXED', 'DAYS_OF_SUPPLY', or 'LEAD_TIME_BASED'
    """
    # Insufficient data
    if data_points < 10:
        return 'FIXED'
    
    # Low variability - simple method works well
    if cv_percent < 20:
        return 'DAYS_OF_SUPPLY'
    
    # Moderate to high variability - use statistical method
    elif cv_percent >= 20 and data_points >= 30:
        return 'LEAD_TIME_BASED'
    
    # Some data but not enough for statistical
    else:
        return 'DAYS_OF_SUPPLY'


def get_lead_time_estimate(
    product_id: int,
    entity_id: int,
    customer_id: Optional[int] = None
) -> Dict:
    """
    Estimate lead time from historical delivery data
    Calculates from OC date to delivered date
    (Placeholder for future costbook integration)
    
    Args:
        product_id: Product ID
        entity_id: Entity ID
        customer_id: Optional customer ID
        
    Returns:
        Dictionary with lead time estimates
    """
    try:
        engine = get_db_engine()
        
        # Query actual delivery times from OC date to delivery date
        query = text("""
        SELECT 
            AVG(DATEDIFF(delivered_date, oc_date)) as avg_lead_time_days,
            MIN(DATEDIFF(delivered_date, oc_date)) as min_lead_time_days,
            MAX(DATEDIFF(delivered_date, oc_date)) as max_lead_time_days,
            COUNT(*) as sample_size
        FROM delivery_full_view
        WHERE product_id = :product_id
            AND legal_entity_code = (SELECT company_code FROM companies WHERE id = :entity_id)
            AND shipment_status = 'DELIVERED'
            AND delivered_date IS NOT NULL
            AND oc_date IS NOT NULL
            AND DATEDIFF(delivered_date, oc_date) > 0
            AND DATEDIFF(delivered_date, oc_date) < 365
        """)
        
        params = {'product_id': product_id, 'entity_id': entity_id}
        
        with engine.connect() as conn:
            result = conn.execute(query, params).fetchone()
        
        if result and result.avg_lead_time_days:
            return {
                'avg_lead_time_days': round(float(result.avg_lead_time_days), 0),
                'min_lead_time_days': int(result.min_lead_time_days) if result.min_lead_time_days else 0,
                'max_lead_time_days': int(result.max_lead_time_days) if result.max_lead_time_days else 0,
                'sample_size': int(result.sample_size) if result.sample_size else 0,
                'is_estimate': True,
                'calculation_basis': 'OC to Delivery'
            }
    except Exception as e:
        logger.error(f"Error estimating lead time: {e}")
    
    # Default fallback
    return {
        'avg_lead_time_days': 7,
        'min_lead_time_days': 0,
        'max_lead_time_days': 0,
        'sample_size': 0,
        'is_estimate': False,
        'note': 'Default value - no historical data available'
    }


def format_demand_summary(stats: Dict) -> str:
    """
    Format demand statistics for display
    
    Args:
        stats: Dictionary with demand statistics
        
    Returns:
        Formatted string for UI display
    """
    if stats['data_points'] == 0:
        return "No historical data found for the selected period"
    
    summary = f"""
📊 **Demand Analysis Summary**
- Period: Last {stats['days_analyzed']} days
- Data Points: {stats['data_points']} delivery dates
- Avg Daily Demand: {stats['avg_daily_demand']:.2f} units/day
- Std Deviation: {stats['demand_std_dev']:.2f} units
- Variability (CV%): {stats['cv_percent']:.1f}%
- Suggested Method: {stats['suggested_method']}
    """.strip()
    
    # Add variability interpretation
    if stats['cv_percent'] < 20:
        summary += "\n- Pattern: Low variability (stable demand)"
    elif stats['cv_percent'] < 50:
        summary += "\n- Pattern: Moderate variability"
    else:
        summary += "\n- Pattern: High variability (unpredictable)"
    
    return summary

