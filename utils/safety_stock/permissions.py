# utils/safety_stock/permissions.py
"""
Role-Based Access Control for Safety Stock Management
Simple permission system based on user roles
"""

import streamlit as st
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Define role permissions matrix (theo bảng screenshot)
ROLE_PERMISSIONS = {
    'admin': {
        'view': True,
        'create': True,
        'edit': True,
        'delete': True,
        'review': True,
        'bulk_upload': True,
        'approve': True
    },
    'MD': {  # Managing Director
        'view': True,
        'create': True,
        'edit': True,
        'delete': True,
        'review': True,
        'bulk_upload': True,
        'approve': True
    },
    'GM': {  # General Manager
        'view': True,
        'create': True,
        'edit': True,
        'delete': True,
        'review': True,
        'bulk_upload': True,
        'approve': True
    },
    'supply_chain': {
        'view': True,
        'create': True,
        'edit': True,
        'delete': False,
        'review': True,
        'bulk_upload': True,
        'approve': False
    },
    'sales_manager': {
        'view': True,
        'create': True,
        'edit': True,
        'delete': False,
        'review': True,
        'bulk_upload': False,
        'approve': False
    },
    'sales': {
        'view': True,
        'create': False,
        'edit': False,
        'delete': False,
        'review': True,
        'bulk_upload': False,
        'approve': False
    },
    'viewer': {
        'view': True,
        'create': False,
        'edit': False,
        'delete': False,
        'review': False,
        'bulk_upload': False,
        'approve': False
    },
    'customer': {
        'view': True,  # Limited to their own data
        'create': False,
        'edit': False,
        'delete': False,
        'review': False,
        'bulk_upload': False,
        'approve': False
    }
}

# Export row limits by role
EXPORT_ROW_LIMITS = {
    'customer': 1000,
    'sales': 5000,
    'sales_manager': 10000,
    'viewer': 5000,
    'supply_chain': None,  # No limit
    'admin': None,
    'MD': None,
    'GM': None
}


def get_user_role() -> str:
    """Get current user's role from session"""
    return st.session_state.get('user_role', 'viewer')


def has_permission(permission: str) -> bool:
    """
    Check if current user has specific permission
    
    Args:
        permission: Permission name (view, create, edit, delete, review, bulk_upload, approve)
    
    Returns:
        bool: True if user has permission
    """
    role = get_user_role()
    
    # Handle vendor role (not in table, no permissions)
    if role == 'vendor':
        return False
    
    permissions = ROLE_PERMISSIONS.get(role, ROLE_PERMISSIONS['viewer'])
    return permissions.get(permission, False)


def filter_data_for_customer(df: pd.DataFrame, customer_col: str = 'customer_id') -> pd.DataFrame:
    """
    Filter dataframe for customer role (only their data)
    
    Args:
        df: DataFrame to filter
        customer_col: Column name containing customer ID
    
    Returns:
        Filtered DataFrame
    """
    role = get_user_role()
    
    # Only filter for customer role
    if role == 'customer' and customer_col in df.columns:
        # Get customer ID from session (set during login)
        customer_id = st.session_state.get('customer_id')
        if customer_id:
            # Customer can only see their own data
            df = df[df[customer_col] == customer_id]
            logger.info(f"Filtered data for customer ID: {customer_id}")
        else:
            # No customer ID found, return empty
            logger.warning("Customer role but no customer_id in session")
            return pd.DataFrame()
    
    return df


def get_permission_message(permission: str) -> str:
    """
    Get user-friendly message for permission denial
    
    Args:
        permission: Permission that was denied
    
    Returns:
        User-friendly error message
    """
    messages = {
        'view': "Bạn không có quyền xem dữ liệu này",
        'create': "Bạn không có quyền tạo safety stock",
        'edit': "Bạn không có quyền chỉnh sửa safety stock",
        'delete': "Bạn không có quyền xóa safety stock",
        'review': "Bạn không có quyền review safety stock",
        'bulk_upload': "Bạn không có quyền upload hàng loạt",
        'approve': "Bạn không có quyền phê duyệt review"
    }
    return messages.get(permission, f"Bạn không có quyền {permission}")


def get_export_row_limit() -> int:
    """
    Get maximum number of rows user can export
    
    Returns:
        Maximum row count for export (None = no limit)
    """
    role = get_user_role()
    return EXPORT_ROW_LIMITS.get(role, 1000)


def check_permission_and_show_error(permission: str) -> bool:
    """
    Check permission and show error if denied
    
    Args:
        permission: Permission to check
    
    Returns:
        bool: True if allowed, False if denied
    """
    if not has_permission(permission):
        st.error(get_permission_message(permission))
        
        # Log permission denial
        username = st.session_state.get('username', 'unknown')
        role = get_user_role()
        logger.warning(f"Permission denied: {permission} for user {username} (role: {role})")
        
        return False
    return True


def get_user_info_display() -> str:
    """
    Get formatted user info for display
    
    Returns:
        Formatted string with username and role
    """
    username = st.session_state.get('user_fullname') or st.session_state.get('username', 'User')
    role = get_user_role()
    
    # Map role to Vietnamese if needed
    role_display = {
        'admin': 'Quản trị',
        'MD': 'Tổng giám đốc',
        'GM': 'Giám đốc',
        'supply_chain': 'Chuỗi cung ứng',
        'sales_manager': 'Quản lý bán hàng',
        'sales': 'Bán hàng',
        'viewer': 'Xem',
        'customer': 'Khách hàng',
        'vendor': 'Nhà cung cấp'
    }.get(role, role)
    
    return f"👤 {username} ({role_display})"


def apply_export_limit(df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    """
    Apply export row limit based on user role
    
    Args:
        df: DataFrame to limit
    
    Returns:
        Tuple of (limited DataFrame, was_limited boolean)
    """
    limit = get_export_row_limit()
    
    if limit is None or len(df) <= limit:
        return df, False
    
    # Apply limit
    limited_df = df.head(limit)
    return limited_df, True


def log_action(action: str, details: str = None):
    """
    Log user action for audit
    
    Args:
        action: Action performed
        details: Optional details about the action
    """
    username = st.session_state.get('username', 'unknown')
    role = get_user_role()
    
    log_msg = f"Action: {action} by {username} (role: {role})"
    if details:
        log_msg += f" - {details}"
    
    logger.info(log_msg)