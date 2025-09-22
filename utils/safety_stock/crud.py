# utils/safety_stock/crud.py
"""
CRUD operations for Safety Stock Management
"""

import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from sqlalchemy import text
from ..db import get_db_engine

logger = logging.getLogger(__name__)

# ==================== READ Operations ====================

def get_safety_stock_levels(
    entity_id: Optional[int] = None,
    customer_id: Optional[int] = None,
    product_search: Optional[str] = None,
    status: str = 'active',
    include_inactive: bool = False
) -> pd.DataFrame:
    """
    Fetch safety stock levels with filters
    
    Args:
        entity_id: Filter by entity
        customer_id: Filter by customer
        product_search: Search in product PT code or name
        status: Filter by status (active/all/expired/future)
        include_inactive: Include inactive records
    
    Returns:
        DataFrame with safety stock data
    """
    try:
        engine = get_db_engine()
        
        # Build WHERE conditions
        conditions = ["s.delete_flag = 0"]
        params = {}
        
        if not include_inactive:
            conditions.append("s.is_active = 1")
        
        if entity_id:
            conditions.append("s.entity_id = :entity_id")
            params['entity_id'] = entity_id
        
        if customer_id:
            conditions.append("s.customer_id = :customer_id")
            params['customer_id'] = customer_id
        
        if product_search:
            conditions.append("(p.pt_code LIKE :search OR p.name LIKE :search)")
            params['search'] = f"%{product_search}%"
        
        # Status filter
        if status == 'active':
            conditions.append("CURRENT_DATE() >= s.effective_from")
            conditions.append("(s.effective_to IS NULL OR CURRENT_DATE() <= s.effective_to)")
        elif status == 'expired':
            conditions.append("CURRENT_DATE() > s.effective_to")
        elif status == 'future':
            conditions.append("CURRENT_DATE() < s.effective_from")
        # 'all' status doesn't add date conditions
        
        where_clause = " AND ".join(conditions)
        
        query = text(f"""
        SELECT 
            s.id,
            s.product_id,
            p.pt_code,
            p.name as product_name,
            p.package_size,
            p.uom as standard_uom,
            b.brand_name,
            
            s.entity_id,
            e.english_name as entity_name,
            e.company_code as entity_code,
            
            s.customer_id,
            c.english_name as customer_name,
            c.company_code as customer_code,
            
            s.safety_stock_qty,
            s.min_stock_qty,
            s.max_stock_qty,
            s.reorder_point,
            s.reorder_qty,
            
            ssp.calculation_method,
            ssp.lead_time_days,
            ssp.safety_days,
            ssp.service_level_percent,
            ssp.avg_daily_demand,
            ssp.last_calculated_date,
            
            s.effective_from,
            s.effective_to,
            s.is_active,
            s.priority_level,
            s.business_notes,
            
            CASE 
                WHEN s.customer_id IS NOT NULL THEN 'Customer Specific'
                ELSE 'General Rule'
            END as rule_type,
            
            CASE 
                WHEN CURRENT_DATE() >= s.effective_from 
                    AND (s.effective_to IS NULL OR CURRENT_DATE() <= s.effective_to)
                    AND s.is_active = 1
                THEN 'Active'
                WHEN CURRENT_DATE() < s.effective_from 
                THEN 'Future'
                WHEN CURRENT_DATE() > s.effective_to
                THEN 'Expired'
                ELSE 'Inactive'
            END as status,
            
            s.created_by,
            s.created_date,
            s.updated_by,
            s.updated_date
            
        FROM safety_stock_levels s
        LEFT JOIN products p ON s.product_id = p.id
        LEFT JOIN brands b ON p.brand_id = b.id
        LEFT JOIN companies e ON s.entity_id = e.id
        LEFT JOIN companies c ON s.customer_id = c.id
        LEFT JOIN safety_stock_parameters ssp ON s.id = ssp.safety_stock_level_id
        WHERE {where_clause}
        ORDER BY s.priority_level, p.pt_code
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)
        
        logger.info(f"Fetched {len(df)} safety stock records")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching safety stock levels: {e}")
        return pd.DataFrame()


def get_safety_stock_by_id(safety_stock_id: int) -> Optional[Dict]:
    """
    Get single safety stock record by ID
    
    Args:
        safety_stock_id: Safety stock level ID
    
    Returns:
        Dictionary with safety stock data or None
    """
    try:
        engine = get_db_engine()
        
        query = text("""
        SELECT 
            s.*,
            ssp.calculation_method,
            ssp.lead_time_days,
            ssp.lead_time_variability_days,
            ssp.safety_days,
            ssp.review_period_days,
            ssp.demand_variability_factor,
            ssp.demand_std_deviation,
            ssp.avg_daily_demand,
            ssp.service_level_percent,
            ssp.z_score,
            ssp.historical_days,
            ssp.exclude_outliers,
            ssp.seasonality_adjusted,
            ssp.last_calculated_date,
            ssp.calculation_notes,
            ssp.formula_used
        FROM safety_stock_levels s
        LEFT JOIN safety_stock_parameters ssp ON s.id = ssp.safety_stock_level_id
        WHERE s.id = :id AND s.delete_flag = 0
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {'id': safety_stock_id}).fetchone()
        
        if result:
            return dict(result._mapping)
        return None
        
    except Exception as e:
        logger.error(f"Error fetching safety stock by ID: {e}")
        return None


# ==================== CREATE Operations ====================

def create_safety_stock(data: Dict, created_by: str) -> Tuple[bool, str]:
    """
    Create new safety stock record
    
    Args:
        data: Safety stock data dictionary
        created_by: Username creating the record
    
    Returns:
        Tuple of (success: bool, message/id: str)
    """
    try:
        engine = get_db_engine()
        
        # Insert into safety_stock_levels
        insert_query = text("""
        INSERT INTO safety_stock_levels (
            product_id, entity_id, customer_id,
            safety_stock_qty, min_stock_qty, max_stock_qty,
            reorder_point, reorder_qty,
            effective_from, effective_to, is_active,
            priority_level, business_notes,
            created_by, updated_by
        ) VALUES (
            :product_id, :entity_id, :customer_id,
            :safety_stock_qty, :min_stock_qty, :max_stock_qty,
            :reorder_point, :reorder_qty,
            :effective_from, :effective_to, :is_active,
            :priority_level, :business_notes,
            :created_by, :updated_by
        )
        """)
        
        with engine.begin() as conn:
            result = conn.execute(insert_query, {
                'product_id': data['product_id'],
                'entity_id': data['entity_id'],
                'customer_id': data.get('customer_id'),
                'safety_stock_qty': data['safety_stock_qty'],
                'min_stock_qty': data.get('min_stock_qty'),
                'max_stock_qty': data.get('max_stock_qty'),
                'reorder_point': data.get('reorder_point'),
                'reorder_qty': data.get('reorder_qty'),
                'effective_from': data['effective_from'],
                'effective_to': data.get('effective_to'),
                'is_active': data.get('is_active', 1),
                'priority_level': data.get('priority_level', 100),
                'business_notes': data.get('business_notes'),
                'created_by': created_by,
                'updated_by': created_by
            })
            
            safety_stock_id = result.lastrowid
            
            # If calculation parameters provided, insert them
            if 'calculation_method' in data:
                params_query = text("""
                INSERT INTO safety_stock_parameters (
                    safety_stock_level_id, calculation_method,
                    lead_time_days, lead_time_variability_days,
                    safety_days, review_period_days,
                    demand_variability_factor, demand_std_deviation,
                    avg_daily_demand, service_level_percent, z_score,
                    historical_days, exclude_outliers, seasonality_adjusted,
                    calculation_notes, formula_used, last_calculated_date
                ) VALUES (
                    :safety_stock_level_id, :calculation_method,
                    :lead_time_days, :lead_time_variability_days,
                    :safety_days, :review_period_days,
                    :demand_variability_factor, :demand_std_deviation,
                    :avg_daily_demand, :service_level_percent, :z_score,
                    :historical_days, :exclude_outliers, :seasonality_adjusted,
                    :calculation_notes, :formula_used, NOW()
                )
                """)
                
                conn.execute(params_query, {
                    'safety_stock_level_id': safety_stock_id,
                    'calculation_method': data.get('calculation_method', 'FIXED'),
                    'lead_time_days': data.get('lead_time_days'),
                    'lead_time_variability_days': data.get('lead_time_variability_days'),
                    'safety_days': data.get('safety_days'),
                    'review_period_days': data.get('review_period_days'),
                    'demand_variability_factor': data.get('demand_variability_factor'),
                    'demand_std_deviation': data.get('demand_std_deviation'),
                    'avg_daily_demand': data.get('avg_daily_demand'),
                    'service_level_percent': data.get('service_level_percent'),
                    'z_score': data.get('z_score'),
                    'historical_days': data.get('historical_days', 90),
                    'exclude_outliers': data.get('exclude_outliers', 1),
                    'seasonality_adjusted': data.get('seasonality_adjusted', 0),
                    'calculation_notes': data.get('calculation_notes'),
                    'formula_used': data.get('formula_used')
                })
        
        logger.info(f"Created safety stock record ID: {safety_stock_id}")
        return True, str(safety_stock_id)
        
    except Exception as e:
        logger.error(f"Error creating safety stock: {e}")
        return False, str(e)


# ==================== UPDATE Operations ====================

def update_safety_stock(safety_stock_id: int, data: Dict, updated_by: str) -> Tuple[bool, str]:
    """
    Update existing safety stock record
    
    Args:
        safety_stock_id: ID of record to update
        data: Updated data dictionary
        updated_by: Username updating the record
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        engine = get_db_engine()
        
        # Build UPDATE statement dynamically
        update_fields = []
        params = {'id': safety_stock_id, 'updated_by': updated_by}
        
        # Fields that can be updated
        updatable_fields = [
            'safety_stock_qty', 'min_stock_qty', 'max_stock_qty',
            'reorder_point', 'reorder_qty', 'effective_from', 
            'effective_to', 'is_active', 'priority_level', 'business_notes'
        ]
        
        for field in updatable_fields:
            if field in data:
                update_fields.append(f"{field} = :{field}")
                params[field] = data[field]
        
        if not update_fields:
            return False, "No fields to update"
        
        update_fields.append("updated_by = :updated_by")
        update_fields.append("updated_date = NOW()")
        
        update_query = text(f"""
        UPDATE safety_stock_levels 
        SET {', '.join(update_fields)}
        WHERE id = :id AND delete_flag = 0
        """)
        
        with engine.begin() as conn:
            result = conn.execute(update_query, params)
            
            if result.rowcount == 0:
                return False, "Record not found or already deleted"
            
            # Update parameters if provided
            if any(key in data for key in ['calculation_method', 'lead_time_days', 'safety_days', 
                                           'service_level_percent', 'avg_daily_demand']):
                
                # Check if parameters record exists
                check_query = text("""
                SELECT id FROM safety_stock_parameters 
                WHERE safety_stock_level_id = :ssl_id
                """)
                
                param_exists = conn.execute(check_query, {'ssl_id': safety_stock_id}).fetchone()
                
                if param_exists:
                    # Update existing parameters
                    param_fields = []
                    param_values = {'ssl_id': safety_stock_id}
                    
                    param_updatable = [
                        'calculation_method', 'lead_time_days', 'lead_time_variability_days',
                        'safety_days', 'review_period_days', 'demand_variability_factor',
                        'demand_std_deviation', 'avg_daily_demand', 'service_level_percent',
                        'z_score', 'historical_days', 'exclude_outliers', 'seasonality_adjusted',
                        'calculation_notes', 'formula_used'
                    ]
                    
                    for field in param_updatable:
                        if field in data:
                            param_fields.append(f"{field} = :{field}")
                            param_values[field] = data[field]
                    
                    if param_fields:
                        param_fields.append("last_calculated_date = NOW()")
                        
                        param_update_query = text(f"""
                        UPDATE safety_stock_parameters 
                        SET {', '.join(param_fields)}
                        WHERE safety_stock_level_id = :ssl_id
                        """)
                        
                        conn.execute(param_update_query, param_values)
        
        logger.info(f"Updated safety stock record ID: {safety_stock_id}")
        return True, "Safety stock updated successfully"
        
    except Exception as e:
        logger.error(f"Error updating safety stock: {e}")
        return False, str(e)


# ==================== DELETE Operations ====================

def delete_safety_stock(safety_stock_id: int, deleted_by: str) -> Tuple[bool, str]:
    """
    Soft delete safety stock record
    
    Args:
        safety_stock_id: ID of record to delete
        deleted_by: Username deleting the record
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        engine = get_db_engine()
        
        query = text("""
        UPDATE safety_stock_levels 
        SET delete_flag = 1, 
            updated_by = :deleted_by,
            updated_date = NOW()
        WHERE id = :id AND delete_flag = 0
        """)
        
        with engine.begin() as conn:
            result = conn.execute(query, {
                'id': safety_stock_id,
                'deleted_by': deleted_by
            })
            
            if result.rowcount == 0:
                return False, "Record not found or already deleted"
        
        logger.info(f"Deleted safety stock record ID: {safety_stock_id}")
        return True, "Safety stock deleted successfully"
        
    except Exception as e:
        logger.error(f"Error deleting safety stock: {e}")
        return False, str(e)


# ==================== BULK Operations ====================

def bulk_create_safety_stock(data_list: List[Dict], created_by: str) -> Tuple[bool, str, Dict]:
    """
    Bulk create safety stock records
    
    Args:
        data_list: List of safety stock data dictionaries
        created_by: Username creating the records
    
    Returns:
        Tuple of (success: bool, message: str, results: dict)
    """
    results = {'created': 0, 'failed': 0, 'errors': []}
    
    try:
        engine = get_db_engine()
        
        with engine.begin() as conn:
            for idx, data in enumerate(data_list):
                try:
                    # Prepare data with defaults
                    insert_data = {
                        'product_id': data['product_id'],
                        'entity_id': data['entity_id'],
                        'customer_id': data.get('customer_id'),
                        'safety_stock_qty': data['safety_stock_qty'],
                        'min_stock_qty': data.get('min_stock_qty'),
                        'max_stock_qty': data.get('max_stock_qty'),
                        'reorder_point': data.get('reorder_point'),
                        'reorder_qty': data.get('reorder_qty'),
                        'effective_from': data.get('effective_from', datetime.now().date()),
                        'effective_to': data.get('effective_to'),
                        'is_active': data.get('is_active', 1),
                        'priority_level': data.get('priority_level', 100),
                        'business_notes': data.get('business_notes'),
                        'created_by': created_by,
                        'updated_by': created_by
                    }
                    
                    insert_query = text("""
                    INSERT INTO safety_stock_levels (
                        product_id, entity_id, customer_id,
                        safety_stock_qty, min_stock_qty, max_stock_qty,
                        reorder_point, reorder_qty,
                        effective_from, effective_to, is_active,
                        priority_level, business_notes,
                        created_by, updated_by
                    ) VALUES (
                        :product_id, :entity_id, :customer_id,
                        :safety_stock_qty, :min_stock_qty, :max_stock_qty,
                        :reorder_point, :reorder_qty,
                        :effective_from, :effective_to, :is_active,
                        :priority_level, :business_notes,
                        :created_by, :updated_by
                    )
                    """)
                    
                    conn.execute(insert_query, insert_data)
                    results['created'] += 1
                    
                except Exception as e:
                    results['failed'] += 1
                    results['errors'].append(f"Row {idx + 1}: {str(e)}")
        
        if results['created'] > 0:
            logger.info(f"Bulk created {results['created']} safety stock records")
            return True, f"Successfully created {results['created']} records", results
        else:
            return False, "No records were created", results
            
    except Exception as e:
        logger.error(f"Error in bulk create: {e}")
        return False, str(e), results


# ==================== Review Operations ====================

def create_safety_stock_review(
    safety_stock_id: int,
    review_data: Dict,
    reviewed_by: str
) -> Tuple[bool, str]:
    """
    Create a safety stock review record
    
    Args:
        safety_stock_id: ID of safety stock level being reviewed
        review_data: Review data dictionary
        reviewed_by: Username conducting the review
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        engine = get_db_engine()
        
        insert_query = text("""
        INSERT INTO safety_stock_reviews (
            safety_stock_level_id, review_date, review_type,
            old_safety_stock_qty, new_safety_stock_qty,
            avg_daily_demand, demand_std_deviation, demand_trend,
            forecast_accuracy_percent,
            stockout_incidents, stockout_quantity, stockout_days,
            service_level_achieved,
            excess_stock_days, avg_excess_quantity, holding_cost_usd,
            inventory_turns, days_of_supply,
            action_taken, action_reason, review_notes,
            system_recommendation, recommendation_accepted, override_reason,
            next_review_date, review_frequency,
            reviewed_by, approved_by
        ) VALUES (
            :safety_stock_level_id, :review_date, :review_type,
            :old_safety_stock_qty, :new_safety_stock_qty,
            :avg_daily_demand, :demand_std_deviation, :demand_trend,
            :forecast_accuracy_percent,
            :stockout_incidents, :stockout_quantity, :stockout_days,
            :service_level_achieved,
            :excess_stock_days, :avg_excess_quantity, :holding_cost_usd,
            :inventory_turns, :days_of_supply,
            :action_taken, :action_reason, :review_notes,
            :system_recommendation, :recommendation_accepted, :override_reason,
            :next_review_date, :review_frequency,
            :reviewed_by, :approved_by
        )
        """)
        
        with engine.begin() as conn:
            conn.execute(insert_query, {
                'safety_stock_level_id': safety_stock_id,
                'review_date': review_data.get('review_date', datetime.now().date()),
                'review_type': review_data.get('review_type', 'PERIODIC'),
                'old_safety_stock_qty': review_data.get('old_safety_stock_qty'),
                'new_safety_stock_qty': review_data.get('new_safety_stock_qty'),
                'avg_daily_demand': review_data.get('avg_daily_demand'),
                'demand_std_deviation': review_data.get('demand_std_deviation'),
                'demand_trend': review_data.get('demand_trend'),
                'forecast_accuracy_percent': review_data.get('forecast_accuracy_percent'),
                'stockout_incidents': review_data.get('stockout_incidents'),
                'stockout_quantity': review_data.get('stockout_quantity'),
                'stockout_days': review_data.get('stockout_days'),
                'service_level_achieved': review_data.get('service_level_achieved'),
                'excess_stock_days': review_data.get('excess_stock_days'),
                'avg_excess_quantity': review_data.get('avg_excess_quantity'),
                'holding_cost_usd': review_data.get('holding_cost_usd'),
                'inventory_turns': review_data.get('inventory_turns'),
                'days_of_supply': review_data.get('days_of_supply'),
                'action_taken': review_data.get('action_taken'),
                'action_reason': review_data.get('action_reason'),
                'review_notes': review_data.get('review_notes'),
                'system_recommendation': review_data.get('system_recommendation'),
                'recommendation_accepted': review_data.get('recommendation_accepted'),
                'override_reason': review_data.get('override_reason'),
                'next_review_date': review_data.get('next_review_date'),
                'review_frequency': review_data.get('review_frequency'),
                'reviewed_by': reviewed_by,
                'approved_by': review_data.get('approved_by')
            })
        
        logger.info(f"Created review for safety stock ID: {safety_stock_id}")
        return True, "Review created successfully"
        
    except Exception as e:
        logger.error(f"Error creating review: {e}")
        return False, str(e)


def get_review_history(safety_stock_id: int) -> pd.DataFrame:
    """
    Get review history for a safety stock record
    
    Args:
        safety_stock_id: Safety stock level ID
    
    Returns:
        DataFrame with review history
    """
    try:
        engine = get_db_engine()
        
        query = text("""
        SELECT 
            review_date,
            review_type,
            old_safety_stock_qty,
            new_safety_stock_qty,
            change_percentage,
            action_taken,
            action_reason,
            service_level_achieved,
            stockout_incidents,
            reviewed_by,
            approved_by
        FROM safety_stock_reviews
        WHERE safety_stock_level_id = :s_id
        ORDER BY review_date DESC
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={'s_id': safety_stock_id})
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching review history: {e}")
        return pd.DataFrame()