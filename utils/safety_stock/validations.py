# utils/safety_stock/validations.py
"""
Validation functions for Safety Stock Management
Handles input validation, business rules, and data integrity checks
"""

import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, date
from sqlalchemy import text
from ..db import get_db_engine
import logging

logger = logging.getLogger(__name__)

# ==================== Quantity Validations ====================

def validate_quantities(data: Dict) -> Tuple[bool, List[str]]:
    """
    Validate quantity fields according to business rules
    
    Args:
        data: Dictionary containing quantity fields
    
    Returns:
        Tuple of (is_valid: bool, errors: List[str])
    """
    errors = []
    
    # Required field
    safety_stock_qty = data.get('safety_stock_qty')
    if safety_stock_qty is None:
        errors.append("Safety stock quantity is required")
    elif safety_stock_qty < 0:
        errors.append("Safety stock quantity cannot be negative")
    
    # Optional fields but must be logical if provided
    min_stock_qty = data.get('min_stock_qty')
    if min_stock_qty is not None:
        if min_stock_qty < 0:
            errors.append("Minimum stock quantity cannot be negative")
        if safety_stock_qty and min_stock_qty > safety_stock_qty:
            errors.append("Minimum stock cannot be greater than safety stock")
    
    max_stock_qty = data.get('max_stock_qty')
    if max_stock_qty is not None:
        if max_stock_qty < 0:
            errors.append("Maximum stock quantity cannot be negative")
        if safety_stock_qty and max_stock_qty < safety_stock_qty:
            errors.append("Maximum stock cannot be less than safety stock")
        if min_stock_qty is not None and max_stock_qty < min_stock_qty:
            errors.append("Maximum stock cannot be less than minimum stock")
    
    reorder_point = data.get('reorder_point')
    if reorder_point is not None:
        if reorder_point < 0:
            errors.append("Reorder point cannot be negative")
        if safety_stock_qty and reorder_point < safety_stock_qty:
            errors.append("Reorder point should not be less than safety stock")
    
    reorder_qty = data.get('reorder_qty')
    if reorder_qty is not None and reorder_qty <= 0:
        errors.append("Reorder quantity must be positive")
    
    return len(errors) == 0, errors


# ==================== Date Validations ====================

def validate_dates(data: Dict) -> Tuple[bool, List[str]]:
    """
    Validate date fields
    
    Args:
        data: Dictionary containing date fields
    
    Returns:
        Tuple of (is_valid: bool, errors: List[str])
    """
    errors = []
    
    effective_from = data.get('effective_from')
    effective_to = data.get('effective_to')
    
    if not effective_from:
        errors.append("Effective from date is required")
    else:
        # Convert to date if datetime
        if isinstance(effective_from, datetime):
            effective_from = effective_from.date()
        
        # Check if date is too far in the past (optional business rule)
        min_date = date(2020, 1, 1)
        if effective_from < min_date:
            errors.append(f"Effective from date cannot be before {min_date}")
    
    if effective_to:
        # Convert to date if datetime
        if isinstance(effective_to, datetime):
            effective_to = effective_to.date()
        
        if effective_from and effective_to <= effective_from:
            errors.append("Effective to date must be after effective from date")
    
    return len(errors) == 0, errors


# ==================== Overlap Validations ====================

def check_date_overlap(
    product_id: int,
    entity_id: int,
    customer_id: Optional[int],
    effective_from: date,
    effective_to: Optional[date],
    exclude_id: Optional[int] = None,
    allow_inactive: bool = False
) -> Tuple[bool, List[Dict]]:
    """
    Check for overlapping date ranges for the same product/entity/customer combination
    
    Args:
        product_id: Product ID
        entity_id: Entity ID
        customer_id: Customer ID (None for general rules)
        effective_from: Start date
        effective_to: End date (None for no end date)
        exclude_id: Exclude this safety stock ID (for updates)
        allow_inactive: Allow overlap with inactive records
    
    Returns:
        Tuple of (has_overlap: bool, overlapping_records: List[Dict])
    """
    try:
        engine = get_db_engine()
        
        # Build query conditions
        conditions = [
            "product_id = :product_id",
            "entity_id = :entity_id",
            "delete_flag = 0"
        ]
        
        if not allow_inactive:
            conditions.append("is_active = 1")
        
        if customer_id:
            conditions.append("customer_id = :customer_id")
        else:
            conditions.append("customer_id IS NULL")
        
        if exclude_id:
            conditions.append("id != :exclude_id")
        
        # Date overlap logic
        if effective_to:
            # Has end date - check for any overlap
            date_condition = """
            AND (
                (effective_from <= :effective_to AND (effective_to IS NULL OR effective_to >= :effective_from))
            )
            """
        else:
            # No end date - overlaps with everything after start date
            date_condition = """
            AND (
                effective_to IS NULL OR effective_to >= :effective_from
            )
            """
        
        query = text(f"""
        SELECT 
            id,
            product_id,
            entity_id,
            customer_id,
            effective_from,
            effective_to,
            priority_level,
            is_active
        FROM safety_stock_levels
        WHERE {' AND '.join(conditions)}
        {date_condition}
        ORDER BY priority_level, effective_from
        """)
        
        params = {
            'product_id': product_id,
            'entity_id': entity_id,
            'effective_from': effective_from,
            'effective_to': effective_to
        }
        
        if customer_id:
            params['customer_id'] = customer_id
        if exclude_id:
            params['exclude_id'] = exclude_id
        
        with engine.connect() as conn:
            result = conn.execute(query, params).fetchall()
        
        overlapping = [dict(row._mapping) for row in result]
        
        return len(overlapping) > 0, overlapping
        
    except Exception as e:
        logger.error(f"Error checking date overlap: {e}")
        return False, []


# ==================== Priority Validations ====================

def validate_priority(
    priority_level: int,
    customer_id: Optional[int] = None
) -> Tuple[bool, List[str]]:
    """
    Validate priority level
    
    Args:
        priority_level: Priority value (lower = higher priority)
        customer_id: Customer ID if customer-specific rule
    
    Returns:
        Tuple of (is_valid: bool, errors: List[str])
    """
    errors = []
    
    if priority_level < 0:
        errors.append("Priority level cannot be negative")
    elif priority_level > 9999:
        errors.append("Priority level cannot exceed 9999")
    
    # Business rule: Customer-specific rules should have higher priority (lower number)
    if customer_id and priority_level > 500:
        errors.append("Customer-specific rules should have priority level 500 or lower")
    
    return len(errors) == 0, errors


# ==================== Customer Rule Validations ====================

def validate_customer_rule(
    product_id: int,
    entity_id: int,
    customer_id: int
) -> Tuple[bool, List[str]]:
    """
    Validate customer-specific rule requirements
    
    Args:
        product_id: Product ID
        entity_id: Entity ID  
        customer_id: Customer ID
    
    Returns:
        Tuple of (is_valid: bool, errors: List[str])
    """
    errors = []
    
    try:
        engine = get_db_engine()
        
        # Check if customer exists and is actually a customer type
        customer_query = text("""
        SELECT c.id, c.english_name
        FROM companies c
        INNER JOIN companies_company_types cct ON c.id = cct.companies_id
        INNER JOIN company_types ct ON cct.company_type_id = ct.id
        WHERE c.id = :customer_id
        AND ct.name = 'Customer'
        AND c.delete_flag = 0
        """)
        
        with engine.connect() as conn:
            customer = conn.execute(customer_query, {'customer_id': customer_id}).fetchone()
        
        if not customer:
            errors.append(f"Customer ID {customer_id} not found or not a customer type")
        
        # Check if general rule exists (recommended but not required)
        general_rule_query = text("""
        SELECT COUNT(*) as count
        FROM safety_stock_levels
        WHERE product_id = :product_id
        AND entity_id = :entity_id
        AND customer_id IS NULL
        AND delete_flag = 0
        AND is_active = 1
        """)
        
        with engine.connect() as conn:
            result = conn.execute(general_rule_query, {
                'product_id': product_id,
                'entity_id': entity_id
            }).fetchone()
        
        if result.count == 0:
            logger.warning(f"No general rule exists for product {product_id} - customer rule may not have fallback")
        
    except Exception as e:
        logger.error(f"Error validating customer rule: {e}")
        errors.append(f"Error validating customer: {str(e)}")
    
    return len(errors) == 0, errors


# ==================== Bulk Data Validations ====================

def validate_bulk_data(df: pd.DataFrame) -> Tuple[bool, pd.DataFrame, List[str]]:
    """
    Validate bulk import data
    
    Args:
        df: DataFrame with safety stock data to import
    
    Returns:
        Tuple of (is_valid: bool, validated_df: pd.DataFrame, errors: List[str])
    """
    errors = []
    validated_df = df.copy()
    
    # Required columns
    required_columns = ['product_id', 'entity_id', 'safety_stock_qty', 'effective_from']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        errors.append(f"Missing required columns: {', '.join(missing_columns)}")
        return False, df, errors
    
    # Validate each row
    row_errors = []
    for idx, row in validated_df.iterrows():
        row_error = []
        
        # Check required fields
        if pd.isna(row['product_id']):
            row_error.append("Missing product_id")
        if pd.isna(row['entity_id']):
            row_error.append("Missing entity_id")
        if pd.isna(row['safety_stock_qty']):
            row_error.append("Missing safety_stock_qty")
        elif row['safety_stock_qty'] < 0:
            row_error.append("Negative safety_stock_qty")
        if pd.isna(row['effective_from']):
            row_error.append("Missing effective_from")
        
        # Validate quantities if present
        if 'min_stock_qty' in row and not pd.isna(row.get('min_stock_qty')):
            if row['min_stock_qty'] < 0:
                row_error.append("Negative min_stock_qty")
        
        if 'max_stock_qty' in row and not pd.isna(row.get('max_stock_qty')):
            if row['max_stock_qty'] < 0:
                row_error.append("Negative max_stock_qty")
            if not pd.isna(row['safety_stock_qty']) and row['max_stock_qty'] < row['safety_stock_qty']:
                row_error.append("max_stock_qty < safety_stock_qty")
        
        if row_error:
            row_errors.append(f"Row {idx + 1}: {'; '.join(row_error)}")
    
    if row_errors:
        errors.extend(row_errors[:10])  # Limit to first 10 errors
        if len(row_errors) > 10:
            errors.append(f"... and {len(row_errors) - 10} more errors")
    
    # Check for duplicates within the dataset
    dup_columns = ['product_id', 'entity_id', 'customer_id', 'effective_from']
    dup_columns = [col for col in dup_columns if col in df.columns]
    duplicates = validated_df[validated_df.duplicated(subset=dup_columns, keep=False)]
    
    if not duplicates.empty:
        errors.append(f"Found {len(duplicates)} duplicate rows based on product/entity/customer/date")
    
    return len(errors) == 0, validated_df, errors


# ==================== Calculation Parameter Validations ====================

def validate_calculation_parameters(method: str, params: Dict) -> Tuple[bool, List[str]]:
    """
    Validate parameters for specific calculation methods
    
    Args:
        method: Calculation method name
        params: Method parameters
    
    Returns:
        Tuple of (is_valid: bool, errors: List[str])
    """
    errors = []
    
    if method == 'FIXED':
        # No additional validation needed for fixed method
        pass
    
    elif method == 'DAYS_OF_SUPPLY':
        if not params.get('safety_days'):
            errors.append("Safety days is required for DAYS_OF_SUPPLY method")
        elif params['safety_days'] <= 0:
            errors.append("Safety days must be positive")
        
        if params.get('avg_daily_demand') and params['avg_daily_demand'] < 0:
            errors.append("Average daily demand cannot be negative")
    
    elif method == 'DEMAND_PERCENTAGE':
        if not params.get('demand_percentage'):
            errors.append("Demand percentage is required for DEMAND_PERCENTAGE method")
        elif params['demand_percentage'] <= 0 or params['demand_percentage'] > 100:
            errors.append("Demand percentage must be between 0 and 100")
    
    elif method == 'LEAD_TIME_BASED':
        if not params.get('lead_time_days'):
            errors.append("Lead time is required for LEAD_TIME_BASED method")
        elif params['lead_time_days'] <= 0:
            errors.append("Lead time must be positive")
        
        if not params.get('service_level_percent'):
            errors.append("Service level is required for LEAD_TIME_BASED method")
        elif params['service_level_percent'] < 50 or params['service_level_percent'] > 99.9:
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
        if params.get('historical_days') and params['historical_days'] < 30:
            errors.append("Statistical method requires at least 30 days of history")
        if params.get('service_level_percent'):
            if params['service_level_percent'] < 50 or params['service_level_percent'] > 99.9:
                errors.append("Service level must be between 50% and 99.9%")
    
    else:
        errors.append(f"Unknown calculation method: {method}")
    
    return len(errors) == 0, errors


# ==================== Entity/Product Validations ====================

def validate_entity_product(
    product_id: int,
    entity_id: int
) -> Tuple[bool, List[str]]:
    """
    Validate that product exists and entity is Internal company
    
    Args:
        product_id: Product ID
        entity_id: Entity ID (Internal company)
    
    Returns:
        Tuple of (is_valid: bool, errors: List[str])
    """
    errors = []
    
    try:
        engine = get_db_engine()
        
        # Check product
        product_query = text("""
        SELECT id, name, pt_code
        FROM products
        WHERE id = :product_id AND delete_flag = 0
        """)
        
        with engine.connect() as conn:
            product = conn.execute(product_query, {'product_id': product_id}).fetchone()
        
        if not product:
            errors.append(f"Product ID {product_id} not found")
        
        # Check entity (must be Internal type company)
        entity_query = text("""
        SELECT 
            c.id, 
            c.english_name, 
            c.company_code,
            COUNT(DISTINCT w.id) as warehouse_count
        FROM companies c
        INNER JOIN companies_company_types cct ON c.id = cct.companies_id
        INNER JOIN company_types ct ON cct.company_type_id = ct.id
        LEFT JOIN warehouses w ON c.id = w.company_id AND w.delete_flag = 0
        WHERE c.id = :entity_id 
        AND ct.name = 'Internal'
        AND c.delete_flag = 0
        GROUP BY c.id, c.english_name, c.company_code
        """)
        
        with engine.connect() as conn:
            entity = conn.execute(entity_query, {'entity_id': entity_id}).fetchone()
        
        if not entity:
            errors.append(f"Entity ID {entity_id} not found or not an Internal company")
        elif entity.warehouse_count == 0:
            logger.warning(f"Internal company {entity.english_name} does not own any warehouses")
        
    except Exception as e:
        logger.error(f"Error validating entity/product: {e}")
        errors.append(f"Error validating: {str(e)}")
    
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
        Tuple of (is_valid: bool, all_errors: List[str])
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
            data.get('customer_id')
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
    
    # Check for date overlaps
    if all(['product_id' in data, 'entity_id' in data, 'effective_from' in data]):
        has_overlap, overlapping = check_date_overlap(
            data['product_id'],
            data['entity_id'],
            data.get('customer_id'),
            data['effective_from'],
            data.get('effective_to'),
            exclude_id=exclude_id
        )
        
        if has_overlap:
            overlap_details = [
                f"ID {rec['id']} ({rec['effective_from']} to {rec['effective_to'] or 'ongoing'})"
                for rec in overlapping[:3]
            ]
            all_errors.append(f"Date overlap with existing records: {'; '.join(overlap_details)}")
    
    # Validate customer rule if customer_id provided
    if data.get('customer_id'):
        valid, errors = validate_customer_rule(
            data['product_id'],
            data['entity_id'],
            data['customer_id']
        )
        if not valid:
            all_errors.extend(errors)
    
    # Validate calculation parameters if provided
    if 'calculation_method' in data:
        valid, errors = validate_calculation_parameters(
            data['calculation_method'],
            data
        )
        if not valid:
            all_errors.extend(errors)
    
    return len(all_errors) == 0, all_errors


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