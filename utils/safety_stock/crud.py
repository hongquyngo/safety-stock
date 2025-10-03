# utils/safety_stock/crud.py
"""
CRUD operations for Safety Stock Management
Version 2.2 - Updated to remove reorder_qty field
"""

import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from sqlalchemy import text
from ..db import get_db_engine
from .permissions import filter_data_for_customer, get_user_role, log_action

logger = logging.getLogger(__name__)


# ==================== READ Operations ====================

def get_safety_stock_levels(
    entity_id: Optional[int] = None,
    customer_id: Optional[int] = None,
    product_id: Optional[int] = None,
    product_search: Optional[str] = None,
    status: str = 'active',
    include_inactive: bool = False
) -> pd.DataFrame:
    """
    Fetch safety stock levels with filters and permission filtering
    
    Args:
        entity_id: Filter by entity
        customer_id: Filter by customer (None for all, 'general' for NULL only)
        product_id: Filter by specific product ID
        product_search: Search in product PT code or name
        status: Filter by status (active/all/expired/future)
        include_inactive: Include inactive records
    
    Returns:
        DataFrame with safety stock data (filtered by permissions)
    """
    try:
        engine = get_db_engine()
        
        # Build WHERE conditions
        conditions = ["s.delete_flag = 0"]
        params = {}
        
        if not include_inactive and status != 'all':
            conditions.append("s.is_active = 1")
        
        if entity_id:
            conditions.append("s.entity_id = :entity_id")
            params['entity_id'] = entity_id
        
        # Handle customer filter
        if customer_id == 'general':
            conditions.append("s.customer_id IS NULL")
        elif customer_id:
            conditions.append("s.customer_id = :customer_id")
            params['customer_id'] = customer_id
        
        # Product filter - either by ID or search
        if product_id:
            conditions.append("s.product_id = :product_id")
            params['product_id'] = product_id
        elif product_search:
            conditions.append("(p.pt_code LIKE :search OR p.name LIKE :search)")
            params['search'] = f"%{product_search}%"
        
        # Status filter
        if status == 'active':
            conditions.append("CURRENT_DATE() >= s.effective_from")
            conditions.append("(s.effective_to IS NULL OR CURRENT_DATE() <= s.effective_to)")
        elif status == 'expired':
            conditions.append("s.effective_to IS NOT NULL AND CURRENT_DATE() > s.effective_to")
        elif status == 'future':
            conditions.append("CURRENT_DATE() < s.effective_from")
        
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
            s.reorder_point,
            
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
                WHEN s.effective_to IS NOT NULL AND CURRENT_DATE() > s.effective_to
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
        
        # Apply permission-based filtering for customer role
        df = filter_data_for_customer(df)
        
        logger.info(f"Fetched {len(df)} safety stock records (user: {get_user_role()})")
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
            p.pt_code,
            p.name as product_name,
            e.english_name as entity_name,
            c.english_name as customer_name,
            ssp.calculation_method,
            ssp.lead_time_days,
            ssp.safety_days,
            ssp.demand_std_deviation,
            ssp.avg_daily_demand,
            ssp.service_level_percent,
            ssp.last_calculated_date,
            ssp.formula_used
        FROM safety_stock_levels s
        LEFT JOIN products p ON s.product_id = p.id
        LEFT JOIN companies e ON s.entity_id = e.id
        LEFT JOIN companies c ON s.customer_id = c.id
        LEFT JOIN safety_stock_parameters ssp ON s.id = ssp.safety_stock_level_id
        WHERE s.id = :id AND s.delete_flag = 0
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {'id': safety_stock_id}).fetchone()
        
        if result:
            data = dict(result._mapping)
            
            # Check if customer role can access this data
            role = get_user_role()
            if role == 'customer':
                import streamlit as st
                customer_id = st.session_state.get('customer_id')
                if data.get('customer_id') != customer_id:
                    logger.warning(f"Customer {customer_id} tried to access data for customer {data.get('customer_id')}")
                    return None
            
            return data
        
        return None
        
    except Exception as e:
        logger.error(f"Error fetching safety stock by ID: {e}")
        return None


# ==================== CREATE Operations ====================

def create_safety_stock(data: Dict, created_by: str) -> Tuple[bool, str]:
    """
    Create new safety stock record with parameters
    
    Args:
        data: Safety stock data dictionary
        created_by: Username creating the record
    
    Returns:
        Tuple of (success: bool, message/id: str)
    """
    try:
        engine = get_db_engine()
        
        with engine.begin() as conn:
            # Insert main record - removed reorder_qty
            insert_query = text("""
            INSERT INTO safety_stock_levels (
                product_id, entity_id, customer_id,
                safety_stock_qty, reorder_point,
                effective_from, effective_to, is_active,
                priority_level, business_notes,
                created_by, updated_by
            ) VALUES (
                :product_id, :entity_id, :customer_id,
                :safety_stock_qty, :reorder_point,
                :effective_from, :effective_to, :is_active,
                :priority_level, :business_notes,
                :created_by, :updated_by
            )
            """)
            
            result = conn.execute(insert_query, {
                'product_id': data['product_id'],
                'entity_id': data['entity_id'],
                'customer_id': data.get('customer_id'),
                'safety_stock_qty': data['safety_stock_qty'],
                'reorder_point': data.get('reorder_point'),
                'effective_from': data['effective_from'],
                'effective_to': data.get('effective_to'),
                'is_active': data.get('is_active', 1),
                'priority_level': data.get('priority_level', 100),
                'business_notes': data.get('business_notes'),
                'created_by': created_by,
                'updated_by': created_by
            })
            
            safety_stock_id = result.lastrowid
            
            # Insert calculation parameters if provided
            if data.get('calculation_method'):
                _insert_parameters(conn, safety_stock_id, data)
        
        # Log the action
        log_action('CREATE', f"Created safety stock ID {safety_stock_id} for product {data['product_id']}")
        logger.info(f"Created safety stock record ID: {safety_stock_id} by {created_by}")
        
        return True, str(safety_stock_id)
        
    except Exception as e:
        logger.error(f"Error creating safety stock by {created_by}: {e}")
        return False, str(e)


def _insert_parameters(conn, safety_stock_id: int, data: Dict):
    """Helper to insert calculation parameters"""
    params_query = text("""
    INSERT INTO safety_stock_parameters (
        safety_stock_level_id, calculation_method,
        lead_time_days, safety_days,
        demand_std_deviation, avg_daily_demand,
        service_level_percent, formula_used,
        last_calculated_date
    ) VALUES (
        :safety_stock_level_id, :calculation_method,
        :lead_time_days, :safety_days,
        :demand_std_deviation, :avg_daily_demand,
        :service_level_percent, :formula_used,
        NOW()
    )
    """)
    
    conn.execute(params_query, {
        'safety_stock_level_id': safety_stock_id,
        'calculation_method': data.get('calculation_method', 'FIXED'),
        'lead_time_days': data.get('lead_time_days'),
        'safety_days': data.get('safety_days'),
        'demand_std_deviation': data.get('demand_std_deviation'),
        'avg_daily_demand': data.get('avg_daily_demand'),
        'service_level_percent': data.get('service_level_percent'),
        'formula_used': data.get('formula_used')
    })


# ==================== UPDATE Operations ====================

def update_safety_stock(
    safety_stock_id: int, 
    data: Dict, 
    updated_by: str
) -> Tuple[bool, str]:
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
        
        # Updatable fields - removed reorder_qty
        updatable_fields = [
            'safety_stock_qty', 'reorder_point',
            'effective_from', 'effective_to', 'is_active', 
            'priority_level', 'business_notes'
        ]
        
        for field in updatable_fields:
            if field in data:
                update_fields.append(f"{field} = :{field}")
                params[field] = data[field]
        
        if not update_fields:
            return False, "No fields to update"
        
        update_fields.extend(["updated_by = :updated_by", "updated_date = NOW()"])
        
        with engine.begin() as conn:
            # Update main record
            update_query = text(f"""
            UPDATE safety_stock_levels 
            SET {', '.join(update_fields)}
            WHERE id = :id AND delete_flag = 0
            """)
            
            result = conn.execute(update_query, params)
            
            if result.rowcount == 0:
                return False, "Record not found or already deleted"
            
            # Update parameters if calculation method fields present
            _update_parameters_if_needed(conn, safety_stock_id, data)
        
        # Log the action
        log_action('UPDATE', f"Updated safety stock ID {safety_stock_id}")
        logger.info(f"Updated safety stock record ID: {safety_stock_id} by {updated_by}")
        
        return True, "Safety stock updated successfully"
        
    except Exception as e:
        logger.error(f"Error updating safety stock by {updated_by}: {e}")
        return False, str(e)


def _update_parameters_if_needed(conn, safety_stock_id: int, data: Dict):
    """Helper to update calculation parameters if needed"""
    param_fields = [
        'calculation_method', 'lead_time_days', 'safety_days',
        'service_level_percent', 'avg_daily_demand', 'demand_std_deviation'
    ]
    
    if not any(field in data for field in param_fields):
        return
    
    # Check if parameters exist
    check_query = text("SELECT id FROM safety_stock_parameters WHERE safety_stock_level_id = :id")
    exists = conn.execute(check_query, {'id': safety_stock_id}).fetchone()
    
    if exists:
        # Update existing
        update_fields = []
        params = {'id': safety_stock_id}
        
        for field in param_fields:
            if field in data:
                update_fields.append(f"{field} = :{field}")
                params[field] = data[field]
        
        if update_fields:
            update_fields.append("last_calculated_date = NOW()")
            update_query = text(f"""
            UPDATE safety_stock_parameters 
            SET {', '.join(update_fields)}
            WHERE safety_stock_level_id = :id
            """)
            conn.execute(update_query, params)
    else:
        # Insert new parameters
        _insert_parameters(conn, safety_stock_id, data)


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
            result = conn.execute(query, {'id': safety_stock_id, 'deleted_by': deleted_by})
            
            if result.rowcount == 0:
                return False, "Record not found or already deleted"
        
        # Log the action
        log_action('DELETE', f"Deleted safety stock ID {safety_stock_id}")
        logger.info(f"Deleted safety stock record ID: {safety_stock_id} by {deleted_by}")
        
        return True, "Safety stock deleted successfully"
        
    except Exception as e:
        logger.error(f"Error deleting safety stock by {deleted_by}: {e}")
        return False, str(e)


# ==================== BULK Operations ====================

def bulk_create_safety_stock(
    data_list: List[Dict], 
    created_by: str
) -> Tuple[bool, str, Dict]:
    """
    Bulk create safety stock records
    
    Args:
        data_list: List of safety stock data dictionaries
        created_by: Username creating the records
    
    Returns:
        Tuple of (success: bool, message: str, results: dict)
    """
    results = {'created': 0, 'failed': 0, 'errors': []}
    
    if not data_list:
        return False, "No data to import", results
    
    try:
        engine = get_db_engine()
        
        with engine.begin() as conn:
            for idx, data in enumerate(data_list, 1):
                try:
                    # Prepare data with defaults - removed reorder_qty
                    insert_data = {
                        'product_id': data['product_id'],
                        'entity_id': data['entity_id'],
                        'customer_id': data.get('customer_id'),
                        'safety_stock_qty': data['safety_stock_qty'],
                        'reorder_point': data.get('reorder_point'),
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
                        safety_stock_qty, reorder_point,
                        effective_from, effective_to, is_active,
                        priority_level, business_notes,
                        created_by, updated_by
                    ) VALUES (
                        :product_id, :entity_id, :customer_id,
                        :safety_stock_qty, :reorder_point,
                        :effective_from, :effective_to, :is_active,
                        :priority_level, :business_notes,
                        :created_by, :updated_by
                    )
                    """)
                    
                    result = conn.execute(insert_query, insert_data)
                    
                    # Add calculation parameters if provided
                    if data.get('calculation_method'):
                        safety_stock_id = result.lastrowid
                        _insert_parameters(conn, safety_stock_id, data)
                    
                    results['created'] += 1
                    
                except Exception as e:
                    results['failed'] += 1
                    error_msg = f"Row {idx}: {str(e)}"
                    results['errors'].append(error_msg)
                    logger.error(error_msg)
                    
                    if len(results['errors']) >= 50:
                        results['errors'].append("... additional errors truncated")
                        break
        
        if results['created'] > 0:
            # Log the action
            log_action('BULK_UPLOAD', f"Bulk created {results['created']} records")
            logger.info(f"Bulk created {results['created']} safety stock records by {created_by}")
            
            return True, f"Successfully created {results['created']} records", results
        else:
            return False, "No records were created", results
            
    except Exception as e:
        logger.error(f"Error in bulk create by {created_by}: {e}")
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
            action_taken, action_reason, review_notes,
            reviewed_by, approved_by
        ) VALUES (
            :safety_stock_level_id, :review_date, :review_type,
            :old_safety_stock_qty, :new_safety_stock_qty,
            :action_taken, :action_reason, :review_notes,
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
                'action_taken': review_data.get('action_taken'),
                'action_reason': review_data.get('action_reason'),
                'review_notes': review_data.get('review_notes'),
                'reviewed_by': reviewed_by,
                'approved_by': review_data.get('approved_by')
            })
        
        # Log the action
        action_desc = f"Reviewed safety stock ID {safety_stock_id}"
        if review_data.get('approved_by'):
            action_desc += " (approved)"
        log_action('REVIEW', action_desc)
        
        logger.info(f"Created review for safety stock ID: {safety_stock_id} by {reviewed_by}")
        return True, "Review created successfully"
        
    except Exception as e:
        logger.error(f"Error creating review by {reviewed_by}: {e}")
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
            review_notes,
            reviewed_by,
            approved_by,
            created_date
        FROM safety_stock_reviews
        WHERE safety_stock_level_id = :id
        ORDER BY review_date DESC
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={'id': safety_stock_id})
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching review history: {e}")
        return pd.DataFrame()

