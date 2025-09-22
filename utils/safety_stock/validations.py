# utils/safety_stock/validations.py
"""
Validation functions for Safety Stock Management
Handles input validation, business rules, and data integrity checks
"""

import pandas as pd
from typing import Dict, List, Tuple, Optional
from datetime import datetime, date
from sqlalchemy import text
from ..db import get_db_engine
import logging

logger = logging.getLogger(__name__)


# ==================== Core Validation Functions ====================

def validate_quantities(data: Dict) -> Tuple[bool, List[str]]:
    """
    Validate quantity fields according to business rules
    
    Args:
        data: Dictionary containing quantity fields
    
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    # Safety stock is required and must be non-negative
    safety_stock = data.get('safety_stock_qty')
    if safety_stock is None:
        errors.append("Safety stock quantity is required")
        return False, errors
    
    if safety_stock < 0:
        errors.append("Safety stock quantity cannot be negative")
    
    # Validate optional quantities
    min_stock = data.get('min_stock_qty')
    max_stock = data.get('max_stock_qty')
    reorder_point = data.get('reorder_point')
    reorder_qty = data.get('reorder_qty')
    
    # Min stock validation
    if min_stock is not None:
        if min_stock < 0:
            errors.append("Minimum stock quantity cannot be negative")
        elif min_stock > safety_stock:
            errors.append("Minimum stock cannot exceed safety stock")
    
    # Max stock validation
    if max_stock is not None:
        if max_stock < 0:
            errors.append("Maximum stock quantity cannot be negative")
        elif max_stock < safety_stock:
            errors.append("Maximum stock cannot be less than safety stock")
        elif min_stock is not None and max_stock < min_stock:
            errors.append("Maximum stock cannot be less than minimum stock")
    
    # Reorder point validation
    if reorder_point is not None:
        if reorder_point < 0:
            errors.append("Reorder point cannot be negative")
        elif reorder_point < safety_stock:
            errors.append("Reorder point should not be less than safety stock")
    
    # Reorder quantity validation
    if reorder_qty is not None and reorder_qty <= 0:
        errors.append("Reorder quantity must be positive")
    
    return len(errors) == 0, errors


def validate_dates(data: Dict) -> Tuple[bool, List[str]]:
    """
    Validate date fields
    
    Args:
        data: Dictionary containing date fields
    
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    effective_from = data.get('effective_from')
    effective_to = data.get('effective_to')
    
    # Effective from is required
    if not effective_from:
        errors.append("Effective from date is required")
        return False, errors
    
    # Convert to date if datetime
    if isinstance(effective_from, datetime):
        effective_from = effective_from.date()
    if isinstance(effective_to, datetime):
        effective_to = effective_to.date()
    
    # Validate date range
    min_allowed_date = date(2020, 1, 1)
    if effective_from < min_allowed_date:
        errors.append(f"Effective from date cannot be before {min_allowed_date}")
    
    if effective_to:
        if effective_to <= effective_from:
            errors.append("Effective to date must be after effective from date")
    
    return len(errors) == 0, errors


def validate_priority(priority_level: int, is_customer_specific: bool) -> Tuple[bool, List[str]]:
    """
    Validate priority level
    
    Args:
        priority_level: Priority value (lower = higher priority)
        is_customer_specific: Whether this is a customer-specific rule
    
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    if priority_level < 0:
        errors.append("Priority level cannot be negative")
    elif priority_level > 9999:
        errors.append("Priority level cannot exceed 9999")
    elif is_customer_specific and priority_level > 500:
        errors.append("Customer-specific rules should have priority level 500 or lower")
    
    return len(errors) == 0, errors


def validate_calculation_parameters(method: str, params: Dict) -> Tuple[bool, List[str]]:
    """
    Validate parameters for specific calculation methods
    
    Args:
        method: Calculation method name
        params: Method parameters
    
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    if method == 'FIXED':
        # No additional validation needed
        pass
    
    elif method == 'DAYS_OF_SUPPLY':
        if not params.get('safety_days'):
            errors.append("Safety days is required for DAYS_OF_SUPPLY method")
        elif params['safety_days'] <= 0:
            errors.append("Safety days must be positive")
    
    elif method == 'DEMAND_PERCENTAGE':
        percentage = params.get('demand_percentage')
        if not percentage:
            errors.append("Demand percentage is required for DEMAND_PERCENTAGE method")
        elif percentage <= 0 or percentage > 100:
            errors.append("Demand percentage must be between 0 and 100")
    
    elif method == 'LEAD_TIME_BASED':
        if not params.get('lead_time_days'):
            errors.append("Lead time is required for LEAD_TIME_BASED method")
        elif params['lead_time_days'] <= 0:
            errors.append("Lead time must be positive")
        
        service_level = params.get('service_level_percent')
        if not service_level:
            errors.append("Service level is required for LEAD_TIME_BASED method")
        elif service_level < 50 or service_level > 99.9:
            errors.append("Service level must be between 50% and 99.9%")
    
    elif method == 'MIN_MAX':
        if not params.get('min_stock_qty'):
            errors.append("Minimum stock is required for MIN_MAX method")
        if not params.get('max_stock_qty'):
            errors.append("Maximum stock is required for MIN_MAX method")
        if params.get('min_stock_qty') and params.get('max_stock_qty'):
            if params['min_stock_qty'] >= params['max_stock_qty']:
                errors.append("Maximum stock must be greater than minimum stock")
    
    elif method == 'STATISTICAL':
        if params.get('historical_days', 90) < 30:
            errors.append("Statistical method requires at least 30 days of history")
    
    elif method not in ['FIXED', 'DAYS_OF_SUPPLY', 'DEMAND_PERCENTAGE', 
                        'LEAD_TIME_BASED', 'MIN_MAX', 'STATISTICAL']:
        errors.append(f"Unknown calculation method: {method}")
    
    return len(errors) == 0, errors


# ==================== Database Validation Functions ====================

def check_date_overlap(
    product_id: int,
    entity_id: int,
    customer_id: Optional[int],
    effective_from: date,
    effective_to: Optional[date],
    exclude_id: Optional[int] = None
) -> Tuple[bool, List[Dict]]:
    """
    Check for overlapping date ranges for the same product/entity/customer
    
    Args:
        product_id: Product ID
        entity_id: Entity ID
        customer_id: Customer ID (None for general rules)
        effective_from: Start date
        effective_to: End date (None for no end date)
        exclude_id: Exclude this safety stock ID (for updates)
    
    Returns:
        Tuple of (has_overlap, overlapping_records)
    """
    try:
        engine = get_db_engine()
        
        # Build query
        query = text("""
        SELECT 
            id,
            effective_from,
            effective_to,
            priority_level
        FROM safety_stock_levels
        WHERE product_id = :product_id
        AND entity_id = :entity_id
        AND (customer_id = :customer_id OR (:customer_id IS NULL AND customer_id IS NULL))
        AND delete_flag = 0
        AND is_active = 1
        AND id != :exclude_id
        AND (
            (:effective_to IS NULL AND (effective_to IS NULL OR effective_to >= :effective_from))
            OR 
            (:effective_to IS NOT NULL AND 
             ((effective_from <= :effective_to) AND (effective_to IS NULL OR effective_to >= :effective_from)))
        )
        ORDER BY priority_level, effective_from
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {
                'product_id': product_id,
                'entity_id': entity_id,
                'customer_id': customer_id,
                'effective_from': effective_from,
                'effective_to': effective_to,
                'exclude_id': exclude_id or -1  # Use -1 if no exclude_id
            }).fetchall()
        
        overlapping = [dict(row._mapping) for row in result]
        return len(overlapping) > 0, overlapping
        
    except Exception as e:
        logger.error(f"Error checking date overlap: {e}")
        return False, []


def validate_entity_product(
    product_id: int,
    entity_id: int
) -> Tuple[bool, List[str]]:
    """
    Validate that product exists and entity is Internal company
    
    Args:
        product_id: Product ID
        entity_id: Entity ID
    
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    try:
        engine = get_db_engine()
        
        # Check product exists
        product_query = text("""
        SELECT id FROM products
        WHERE id = :product_id AND delete_flag = 0
        """)
        
        # Check entity is Internal
        entity_query = text("""
        SELECT c.id
        FROM companies c
        INNER JOIN companies_company_types cct ON c.id = cct.companies_id
        INNER JOIN company_types ct ON cct.company_type_id = ct.id
        WHERE c.id = :entity_id 
        AND ct.name = 'Internal'
        AND c.delete_flag = 0
        """)
        
        with engine.connect() as conn:
            product = conn.execute(product_query, {'product_id': product_id}).fetchone()
            if not product:
                errors.append(f"Product ID {product_id} not found")
            
            entity = conn.execute(entity_query, {'entity_id': entity_id}).fetchone()
            if not entity:
                errors.append(f"Entity ID {entity_id} not found or not an Internal company")
        
    except Exception as e:
        logger.error(f"Error validating entity/product: {e}")
        errors.append(f"Validation error: {str(e)}")
    
    return len(errors) == 0, errors


def validate_customer(customer_id: int) -> Tuple[bool, List[str]]:
    """
    Validate that customer exists and is Customer type
    
    Args:
        customer_id: Customer ID
    
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    try:
        engine = get_db_engine()
        
        query = text("""
        SELECT c.id
        FROM companies c
        INNER JOIN companies_company_types cct ON c.id = cct.companies_id
        INNER JOIN company_types ct ON cct.company_type_id = ct.id
        WHERE c.id = :customer_id
        AND ct.name = 'Customer'
        AND c.delete_flag = 0
        """)
        
        with engine.connect() as conn:
            customer = conn.execute(query, {'customer_id': customer_id}).fetchone()
        
        if not customer:
            errors.append(f"Customer ID {customer_id} not found or not a Customer type")
        
    except Exception as e:
        logger.error(f"Error validating customer: {e}")
        errors.append(f"Validation error: {str(e)}")
    
    return len(errors) == 0, errors


# ==================== Master Validation Function ====================

def validate_safety_stock_data(
    data: Dict,
    mode: str = 'create',
    exclude_id: Optional[int] = None
) -> Tuple[bool, List[str]]:
    """
    Master validation function that runs all applicable validations
    
    Args:
        data: Safety stock data dictionary
        mode: 'create' or 'update'
        exclude_id: ID to exclude for overlap check (used in updates)
    
    Returns:
        Tuple of (is_valid, error_messages)
    """
    all_errors = []
    
    # Validate quantities
    valid, errors = validate_quantities(data)
    if not valid:
        all_errors.extend(errors)
    
    # Validate dates
    valid, errors = validate_dates(data)
    if not valid:
        all_errors.extend(errors)
    
    # Validate priority if provided
    if 'priority_level' in data:
        valid, errors = validate_priority(
            data['priority_level'],
            bool(data.get('customer_id'))
        )
        if not valid:
            all_errors.extend(errors)
    
    # For create mode, validate entity and product
    if mode == 'create':
        if data.get('product_id') and data.get('entity_id'):
            valid, errors = validate_entity_product(
                data['product_id'],
                data['entity_id']
            )
            if not valid:
                all_errors.extend(errors)
    
    # Validate customer if provided
    if data.get('customer_id'):
        valid, errors = validate_customer(data['customer_id'])
        if not valid:
            all_errors.extend(errors)
    
    # Check for date overlaps
    if all(key in data for key in ['product_id', 'entity_id', 'effective_from']):
        has_overlap, overlapping = check_date_overlap(
            data['product_id'],
            data['entity_id'],
            data.get('customer_id'),
            data['effective_from'],
            data.get('effective_to'),
            exclude_id=exclude_id
        )
        
        if has_overlap:
            overlap_info = []
            for rec in overlapping[:3]:  # Show first 3 overlaps
                date_range = f"{rec['effective_from']} to {rec['effective_to'] or 'ongoing'}"
                overlap_info.append(f"ID {rec['id']} ({date_range})")
            all_errors.append(f"Date overlap with existing records: {'; '.join(overlap_info)}")
    
    # Validate calculation parameters if provided
    if 'calculation_method' in data:
        valid, errors = validate_calculation_parameters(
            data['calculation_method'],
            data
        )
        if not valid:
            all_errors.extend(errors)
    
    return len(all_errors) == 0, all_errors


# ==================== Bulk Validation ====================

def validate_bulk_data(df: pd.DataFrame) -> Tuple[bool, pd.DataFrame, List[str]]:
    """
    Validate bulk import data
    
    Args:
        df: DataFrame with safety stock data to import
    
    Returns:
        Tuple of (is_valid, validated_df, error_messages)
    """
    errors = []
    validated_df = df.copy()
    
    # Check required columns
    required_columns = ['product_id', 'entity_id', 'safety_stock_qty', 'effective_from']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        errors.append(f"Missing required columns: {', '.join(missing_columns)}")
        return False, df, errors
    
    # Validate each row
    row_errors = []
    for idx, row in validated_df.iterrows():
        row_dict = row.to_dict()
        
        # Clean up NaN values
        row_dict = {k: v for k, v in row_dict.items() if pd.notna(v)}
        
        # Validate using master validation function
        valid, row_error_list = validate_safety_stock_data(row_dict, mode='create')
        
        if not valid:
            row_errors.append(f"Row {idx + 1}: {'; '.join(row_error_list)}")
    
    # Add row errors to main error list
    if row_errors:
        errors.extend(row_errors[:10])  # Limit to first 10 errors
        if len(row_errors) > 10:
            errors.append(f"... and {len(row_errors) - 10} more errors")
    
    # Check for duplicates within the dataset
    dup_columns = ['product_id', 'entity_id', 'customer_id', 'effective_from']
    dup_columns = [col for col in dup_columns if col in df.columns]
    
    if dup_columns:
        duplicates = validated_df[validated_df.duplicated(subset=dup_columns, keep=False)]
        if not duplicates.empty:
            errors.append(f"Found {len(duplicates)} duplicate rows")
    
    return len(errors) == 0, validated_df, errors


# ==================== Helper Functions ====================

def get_validation_summary(errors: List[str]) -> str:
    """
    Format validation errors for display
    
    Args:
        errors: List of error messages
    
    Returns:
        Formatted error summary
    """
    if not errors:
        return "✅ All validations passed"
    
    summary = f"❌ Found {len(errors)} validation error(s):\n"
    for i, error in enumerate(errors, 1):
        summary += f"{i}. {error}\n"
    
    return summary