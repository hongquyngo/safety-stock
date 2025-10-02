# utils/safety_stock/validations.py
"""
Validation functions for Safety Stock Management
Simplified and clean validation logic
"""

import pandas as pd
from typing import Dict, List, Tuple, Optional
from datetime import datetime, date
from sqlalchemy import text
from ..db import get_db_engine
import logging

logger = logging.getLogger(__name__)


def validate_safety_stock_data(
    data: Dict,
    mode: str = 'create',
    exclude_id: Optional[int] = None
) -> Tuple[bool, List[str]]:
    """
    Master validation function for safety stock data
    
    Args:
        data: Data dictionary to validate
        mode: 'create' or 'edit'
        exclude_id: ID to exclude when checking duplicates (for edit mode)
    
    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []
    
    # 1. Validate required fields
    if mode == 'create':
        required_fields = ['product_id', 'entity_id', 'safety_stock_qty', 'effective_from']
        for field in required_fields:
            if field not in data or data[field] is None:
                errors.append(f"Missing required field: {field}")
    
    # 2. Validate quantities
    if 'safety_stock_qty' in data:
        if data['safety_stock_qty'] < 0:
            errors.append("Safety stock quantity cannot be negative")
        elif data['safety_stock_qty'] > 999999:
            errors.append("Safety stock quantity is unreasonably large (max: 999,999)")
    
    if 'reorder_point' in data and data['reorder_point'] is not None:
        if data['reorder_point'] < 0:
            errors.append("Reorder point cannot be negative")
        # Reorder point should be >= safety stock
        if 'safety_stock_qty' in data and data['reorder_point'] < data['safety_stock_qty']:
            errors.append("Reorder point should not be less than safety stock quantity")
    
    if 'reorder_qty' in data and data['reorder_qty'] is not None:
        if data['reorder_qty'] <= 0:
            errors.append("Reorder quantity must be positive")
    
    # 3. Validate dates
    if 'effective_from' in data:
        effective_from = data['effective_from']
        if isinstance(effective_from, str):
            try:
                effective_from = datetime.strptime(effective_from, '%Y-%m-%d').date()
            except ValueError:
                errors.append("Invalid effective_from date format (use YYYY-MM-DD)")
        
        # Check minimum date
        min_date = date(2020, 1, 1)
        if effective_from and effective_from < min_date:
            errors.append(f"Effective from date cannot be before {min_date}")
    
    if 'effective_to' in data and data['effective_to'] is not None:
        effective_to = data['effective_to']
        if isinstance(effective_to, str):
            try:
                effective_to = datetime.strptime(effective_to, '%Y-%m-%d').date()
            except ValueError:
                errors.append("Invalid effective_to date format (use YYYY-MM-DD)")
        
        # Check date range
        if 'effective_from' in data and effective_to:
            if isinstance(data['effective_from'], str):
                effective_from = datetime.strptime(data['effective_from'], '%Y-%m-%d').date()
            else:
                effective_from = data['effective_from']
            
            if effective_to <= effective_from:
                errors.append("Effective to date must be after effective from date")
    
    # 4. Validate priority
    if 'priority_level' in data and data['priority_level'] is not None:
        if data['priority_level'] < 1:
            errors.append("Priority level must be at least 1")
        elif data['priority_level'] > 9999:
            errors.append("Priority level cannot exceed 9999")
        
        # Customer-specific rules should have lower priority number (higher priority)
        if data.get('customer_id') and data['priority_level'] > 500:
            errors.append("Customer-specific rules should have priority level 500 or lower")
    
    # 5. Validate calculation method parameters
    if 'calculation_method' in data:
        method_errors = validate_calculation_parameters(
            data['calculation_method'],
            data
        )
        errors.extend(method_errors)
    
    # 6. Check for existing duplicates
    if mode == 'create' or (mode == 'edit' and 'product_id' in data):
        duplicate_errors = check_for_duplicates(data, exclude_id)
        errors.extend(duplicate_errors)
    
    return len(errors) == 0, errors


def validate_calculation_parameters(method: str, data: Dict) -> List[str]:
    """
    Validate parameters for specific calculation method
    
    Args:
        method: Calculation method (FIXED, DAYS_OF_SUPPLY, LEAD_TIME_BASED)
        data: Parameters dictionary
    
    Returns:
        List of error messages
    """
    errors = []
    
    if method not in ['FIXED', 'DAYS_OF_SUPPLY', 'LEAD_TIME_BASED']:
        errors.append(f"Invalid calculation method: {method}")
        return errors
    
    if method == 'DAYS_OF_SUPPLY':
        if 'safety_days' in data:
            if data['safety_days'] is None or data['safety_days'] <= 0:
                errors.append("Safety days must be positive for DAYS_OF_SUPPLY method")
            elif data['safety_days'] > 365:
                errors.append("Safety days seems too high (>365 days)")
        
        if 'avg_daily_demand' in data and data['avg_daily_demand'] is not None:
            if data['avg_daily_demand'] < 0:
                errors.append("Average daily demand cannot be negative")
    
    elif method == 'LEAD_TIME_BASED':
        if 'lead_time_days' in data:
            if data['lead_time_days'] is None or data['lead_time_days'] <= 0:
                errors.append("Lead time must be positive for LEAD_TIME_BASED method")
            elif data['lead_time_days'] > 365:
                errors.append("Lead time seems too long (>365 days)")
        
        if 'service_level_percent' in data:
            if data['service_level_percent'] is None:
                errors.append("Service level is required for LEAD_TIME_BASED method")
            elif data['service_level_percent'] < 50 or data['service_level_percent'] > 99.9:
                errors.append("Service level must be between 50% and 99.9%")
        
        if 'demand_std_deviation' in data and data['demand_std_deviation'] is not None:
            if data['demand_std_deviation'] < 0:
                errors.append("Demand standard deviation cannot be negative")
    
    return errors


def check_for_duplicates(data: Dict, exclude_id: Optional[int] = None) -> List[str]:
    """
    Check for duplicate/overlapping safety stock rules
    
    Args:
        data: Data to check
        exclude_id: ID to exclude (for updates)
    
    Returns:
        List of error messages
    """
    errors = []
    
    try:
        engine = get_db_engine()
        
        # Check for exact duplicates
        query = text("""
        SELECT COUNT(*) as count
        FROM safety_stock_levels
        WHERE product_id = :product_id
        AND entity_id = :entity_id
        AND (customer_id = :customer_id OR (:customer_id IS NULL AND customer_id IS NULL))
        AND delete_flag = 0
        AND is_active = 1
        AND id != :exclude_id
        AND effective_from = :effective_from
        """)
        
        params = {
            'product_id': data.get('product_id'),
            'entity_id': data.get('entity_id'),
            'customer_id': data.get('customer_id'),
            'effective_from': data.get('effective_from'),
            'exclude_id': exclude_id or -1
        }
        
        with engine.connect() as conn:
            result = conn.execute(query, params).fetchone()
            
            if result and result.count > 0:
                errors.append("A safety stock rule already exists for this product/entity/customer/date combination")
        
        # Check for overlapping date ranges
        if not errors:  # Only check overlaps if no exact duplicate
            overlap_query = text("""
            SELECT id, effective_from, effective_to
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
            LIMIT 3
            """)
            
            overlap_params = {
                **params,
                'effective_to': data.get('effective_to')
            }
            
            result = conn.execute(overlap_query, overlap_params).fetchall()
            
            if result:
                overlap_info = []
                for row in result:
                    date_range = f"{row.effective_from} to {row.effective_to or 'ongoing'}"
                    overlap_info.append(f"ID {row.id} ({date_range})")
                
                errors.append(f"Date range overlaps with existing rules: {'; '.join(overlap_info[:3])}")
    
    except Exception as e:
        logger.error(f"Error checking duplicates: {e}")
        # Don't block on validation error, just log it
    
    return errors


def validate_bulk_data(df: pd.DataFrame) -> Tuple[bool, pd.DataFrame, List[str]]:
    """
    Validate bulk upload data
    
    Args:
        df: DataFrame to validate
    
    Returns:
        Tuple of (is_valid, cleaned_dataframe, error_list)
    """
    errors = []
    validated_df = df.copy()
    
    # Check required columns
    required_columns = ['product_id', 'entity_id', 'safety_stock_qty', 'effective_from']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        errors.append(f"Missing required columns: {', '.join(missing_columns)}")
        return False, df, errors
    
    # Clean and validate each row
    row_errors = []
    rows_to_drop = []
    
    for idx, row in validated_df.iterrows():
        row_dict = row.to_dict()
        
        # Remove NaN values
        row_dict = {k: v for k, v in row_dict.items() if pd.notna(v)}
        
        # Validate row
        is_valid, row_error_list = validate_safety_stock_data(row_dict, mode='create')
        
        if not is_valid:
            row_num = idx + 2  # +1 for 0-index, +1 for header row
            row_errors.append(f"Row {row_num}: {'; '.join(row_error_list)}")
            rows_to_drop.append(idx)
    
    # Add row errors to main error list
    if row_errors:
        errors.extend(row_errors[:20])  # Limit to first 20 errors
        if len(row_errors) > 20:
            errors.append(f"... and {len(row_errors) - 20} more errors")
    
    # Check for duplicates within the file
    if 'product_id' in df.columns and 'entity_id' in df.columns:
        dup_columns = ['product_id', 'entity_id', 'customer_id', 'effective_from']
        dup_columns = [col for col in dup_columns if col in df.columns]
        
        duplicates = validated_df[validated_df.duplicated(subset=dup_columns, keep=False)]
        if not duplicates.empty:
            errors.append(f"Found {len(duplicates)} duplicate rows within the file")
    
    # Drop invalid rows if requested
    if rows_to_drop:
        validated_df = validated_df.drop(rows_to_drop)
        errors.append(f"Removed {len(rows_to_drop)} invalid rows")
    
    return len(errors) == 0, validated_df, errors


def validate_review_data(review_data: Dict) -> Tuple[bool, List[str]]:
    """
    Validate review data
    
    Args:
        review_data: Review data dictionary
    
    Returns:
        Tuple of (is_valid, error_list)
    """
    errors = []
    
    # Required fields
    if not review_data.get('old_safety_stock_qty'):
        errors.append("Old safety stock quantity is required")
    
    if not review_data.get('new_safety_stock_qty'):
        errors.append("New safety stock quantity is required")
    
    if not review_data.get('action_taken'):
        errors.append("Action taken is required")
    
    if not review_data.get('action_reason'):
        errors.append("Reason for change is required")
    elif len(review_data['action_reason'].strip()) < 10:
        errors.append("Reason for change must be at least 10 characters")
    
    # Validate action consistency
    if all(key in review_data for key in ['old_safety_stock_qty', 'new_safety_stock_qty', 'action_taken']):
        old_qty = review_data['old_safety_stock_qty']
        new_qty = review_data['new_safety_stock_qty']
        action = review_data['action_taken']
        
        if action == 'INCREASED' and new_qty <= old_qty:
            errors.append("Action is INCREASED but new quantity is not greater")
        elif action == 'DECREASED' and new_qty >= old_qty:
            errors.append("Action is DECREASED but new quantity is not less")
        elif action == 'NO_CHANGE' and new_qty != old_qty:
            errors.append("Action is NO_CHANGE but quantities are different")
    
    # Validate action_taken value
    valid_actions = ['INCREASED', 'DECREASED', 'NO_CHANGE', 'METHOD_CHANGED']
    if review_data.get('action_taken') and review_data['action_taken'] not in valid_actions:
        errors.append(f"Invalid action. Must be one of: {', '.join(valid_actions)}")
    
    # Validate review_type
    valid_review_types = ['PERIODIC', 'EXCEPTION', 'EMERGENCY', 'ANNUAL']
    if review_data.get('review_type') and review_data['review_type'] not in valid_review_types:
        errors.append(f"Invalid review type. Must be one of: {', '.join(valid_review_types)}")
    
    return len(errors) == 0, errors


def get_validation_summary(errors: List[str]) -> str:
    """
    Format validation errors for display
    
    Args:
        errors: List of error messages
    
    Returns:
        Formatted error summary
    """
    if not errors:
        return "All validations passed"
    
    if len(errors) == 1:
        return f"Validation error: {errors[0]}"
    
    summary = f"Found {len(errors)} validation errors:\n"
    for i, error in enumerate(errors[:10], 1):  # Show max 10 errors
        summary += f"{i}. {error}\n"
    
    if len(errors) > 10:
        summary += f"... and {len(errors) - 10} more errors"
    
    return summary