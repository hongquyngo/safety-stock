# pages/1_📦_Safety_Stock_Management.py
"""
Safety Stock Management Main Page
Version 2.0 - Updated for simplified DB structure
Changes: Removed min/max stock, simplified reviews
"""

import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import logging
from typing import Dict, Optional

# Import utilities
from utils.auth import AuthManager
from utils.db import get_db_engine
from utils.safety_stock.crud import (
    get_safety_stock_levels,
    get_safety_stock_by_id,
    create_safety_stock,
    update_safety_stock,
    delete_safety_stock,
    create_safety_stock_review,
    get_review_history,
    bulk_create_safety_stock
)
from utils.safety_stock.calculations import calculate_safety_stock, Z_SCORE_MAP
from utils.safety_stock.validations import (
    validate_safety_stock_data,
    validate_bulk_data,
    get_validation_summary
)
from utils.safety_stock.export import (
    export_to_excel,
    create_upload_template,
    generate_review_report
)
from sqlalchemy import text

logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Safety Stock Management",
    page_icon="📦",
    layout="wide"
)

# Initialize auth
auth_manager = AuthManager()
if not auth_manager.check_session():
    st.warning("Please login to access this page")
    st.stop()

# Initialize session state
if 'ss_filters' not in st.session_state:
    st.session_state.ss_filters = {
        'entity_id': None,
        'customer_id': None,
        'product_search': '',
        'status': 'active'
    }

# ==================== Data Loading Functions ====================

@st.cache_data(ttl=300)
def load_entities():
    """Load Internal companies (entities)"""
    try:
        engine = get_db_engine()
        query = text("""
        SELECT DISTINCT 
            c.id, 
            c.company_code, 
            c.english_name,
            COUNT(DISTINCT w.id) as warehouse_count
        FROM companies c
        INNER JOIN companies_company_types cct ON c.id = cct.companies_id
        INNER JOIN company_types ct ON cct.company_type_id = ct.id
        LEFT JOIN warehouses w ON c.id = w.company_id AND w.delete_flag = 0
        WHERE ct.name = 'Internal'
        AND c.delete_flag = 0
        AND c.company_code IS NOT NULL
        GROUP BY c.id, c.company_code, c.english_name
        ORDER BY c.company_code
        """)
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error loading entities: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_customers():
    """Load customer list"""
    try:
        engine = get_db_engine()
        query = text("""
        SELECT DISTINCT 
            c.id, 
            c.company_code, 
            c.english_name 
        FROM companies c
        INNER JOIN companies_company_types cct ON c.id = cct.companies_id
        INNER JOIN company_types ct ON cct.company_type_id = ct.id
        WHERE ct.name = 'Customer'
        AND c.delete_flag = 0
        AND c.company_code IS NOT NULL
        ORDER BY c.company_code
        """)
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error loading customers: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_products():
    """Load products with package size and brand"""
    try:
        engine = get_db_engine()
        query = text("""
        SELECT 
            p.id, 
            p.pt_code, 
            p.name,
            p.package_size,
            p.uom,
            b.brand_name
        FROM products p
        LEFT JOIN brands b ON p.brand_id = b.id
        WHERE p.delete_flag = 0
        AND p.pt_code IS NOT NULL
        ORDER BY p.pt_code
        """)
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            df['display_text'] = df.apply(lambda row: format_product_display(row), axis=1)
            return df
    except Exception as e:
        st.error(f"Error loading products: {e}")
        return pd.DataFrame()

def format_product_display(row):
    """Format: PT_CODE | Name | Package (Brand)"""
    pt_code = str(row['pt_code'])
    name = str(row['name']) if pd.notna(row['name']) else ""
    name = name[:35] + "..." if len(name) > 35 else name
    pkg = str(row['package_size']) if pd.notna(row['package_size']) else ""
    pkg = pkg[:20] + "..." if len(pkg) > 20 else pkg
    brand = str(row['brand_name']) if pd.notna(row['brand_name']) else ""
    
    display = f"{pt_code} | {name}"
    if pkg and brand:
        display += f" | {pkg} ({brand})"
    elif pkg:
        display += f" | {pkg}"
    elif brand:
        display += f" ({brand})"
    
    return display

def get_quick_stats():
    """Get dashboard statistics"""
    try:
        engine = get_db_engine()
        query = text("""
        SELECT 
            COUNT(DISTINCT s.id) as total_items,
            COUNT(DISTINCT CASE WHEN s.customer_id IS NOT NULL THEN s.id END) as customer_rules,
            COUNT(DISTINCT CASE 
                WHEN ssp.last_calculated_date < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
                OR ssp.last_calculated_date IS NULL 
                THEN s.id END) as needs_review,
            COUNT(DISTINCT s.product_id) as unique_products
        FROM safety_stock_levels s
        LEFT JOIN safety_stock_parameters ssp ON s.id = ssp.safety_stock_level_id
        WHERE s.delete_flag = 0 AND s.is_active = 1
        """)
        with engine.connect() as conn:
            return conn.execute(query).fetchone()
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return None

def safe_int(value, default=0):
    """Safely convert to Python int"""
    try:
        if pd.isna(value):
            return default
        if hasattr(value, 'item'):
            return int(value.item())
        return int(value)
    except:
        return default

def safe_float(value, default=0.0):
    """Safely convert to float"""
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except:
        return default

# ==================== Dialog Functions ====================

@st.dialog("Safety Stock Configuration", width="large")
def safety_stock_form(mode='add', record_id=None):
    """Add/Edit safety stock dialog - UPDATED: removed min/max stock"""
    
    existing_data = {}
    if mode == 'edit' and record_id:
        existing_data = get_safety_stock_by_id(record_id) or {}
    
    entities = load_entities()
    customers = load_customers()
    products = load_products()
    
    if entities.empty or products.empty:
        st.error("Unable to load required data")
        return
    
    st.markdown(f"### {'Edit' if mode == 'edit' else 'Add New'} Safety Stock")
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["Basic Information", "Stock Levels", "Calculation Method"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            # Product selection
            if mode == 'add':
                display_products = products.head(200)
                selected_product = st.selectbox(
                    "Product * (type to search)",
                    options=range(len(display_products)),
                    format_func=lambda x: display_products.iloc[x]['display_text'] if x < len(display_products) else "",
                    help="Start typing PT code, name, package size, or brand to filter"
                )
                product_id = display_products.iloc[selected_product]['id']
                st.caption(f"Showing {len(display_products)} products")
            else:
                st.text_input(
                    "Product",
                    value=f"{existing_data.get('pt_code', '')} | {existing_data.get('product_name', '')}",
                    disabled=True
                )
                product_id = existing_data['product_id']
            
            # Entity selection
            entity_options = entities['company_code'] + ' - ' + entities['english_name']
            entity_idx = 0
            if mode == 'edit' and existing_data.get('entity_id'):
                try:
                    matches = entities[entities['id'] == existing_data['entity_id']]
                    if not matches.empty:
                        entity_idx = safe_int(matches.index[0])
                except Exception as e:
                    logger.error(f"Error finding entity: {e}")
            
            selected_entity = st.selectbox(
                "Entity *",
                options=range(len(entities)),
                format_func=lambda x: entity_options.iloc[x],
                index=entity_idx,
                disabled=(mode == 'edit')
            )
            entity_id = entities.iloc[selected_entity]['id']
        
        with col2:
            # Customer selection
            customer_options = ['General Rule (All Customers)'] + \
                              (customers['company_code'] + ' - ' + customers['english_name']).tolist()
            
            customer_idx = 0
            if mode == 'edit' and existing_data.get('customer_id'):
                try:
                    matches = customers[customers['id'] == existing_data['customer_id']]
                    if not matches.empty:
                        customer_idx = safe_int(matches.index[0]) + 1
                except Exception as e:
                    logger.error(f"Error finding customer: {e}")
            
            selected_customer = st.selectbox(
                "Customer (Optional)",
                options=range(len(customer_options)),
                format_func=lambda x: customer_options[x],
                index=customer_idx
            )
            customer_id = None if selected_customer == 0 else customers.iloc[selected_customer - 1]['id']
            
            # Priority
            default_priority = 100 if customer_id is None else 50
            priority_level = st.number_input(
                "Priority Level",
                min_value=1,
                max_value=9999,
                value=safe_int(existing_data.get('priority_level', default_priority)),
                help="Lower = higher priority. Customer rules ≤ 500"
            )
        
        # Dates
        col1, col2 = st.columns(2)
        with col1:
            effective_from = st.date_input(
                "Effective From *",
                value=existing_data.get('effective_from', datetime.now().date())
            )
        with col2:
            effective_to = st.date_input(
                "Effective To (Optional)",
                value=existing_data.get('effective_to')
            )
        
        # Notes
        business_notes = st.text_area(
            "Business Notes",
            value=existing_data.get('business_notes', ''),
            height=100
        )
    
    with tab2:
        
        col1, col2 = st.columns(2)
        
        with col1:
            default_ss_qty = safe_float(existing_data.get('safety_stock_qty', 0))
            if 'calculated_safety_stock' in st.session_state:
                default_ss_qty = st.session_state.calculated_safety_stock
            
            safety_stock_qty = st.number_input(
                "Safety Stock Quantity *",
                min_value=0.0,
                value=default_ss_qty,
                step=1.0,
                help="The buffer quantity to maintain above zero"
            )
            
            if 'calculated_safety_stock' in st.session_state:
                st.success(f"✓ Calculated: {st.session_state.calculated_safety_stock:.2f}")
                if st.button("Clear Calculated Value"):
                    del st.session_state.calculated_safety_stock
                    st.rerun()
        
        with col2:
            reorder_point = st.number_input(
                "Reorder Point",
                min_value=0.0,
                value=safe_float(existing_data.get('reorder_point')),
                step=1.0,
                help="Trigger new purchase order at this level"
            )
            
            reorder_qty = st.number_input(
                "Reorder Quantity",
                min_value=0.0,
                value=safe_float(existing_data.get('reorder_qty')),
                step=1.0,
                help="Suggested order quantity (can be EOQ)"
            )
    
    with tab3:
        methods = ['FIXED', 'DAYS_OF_SUPPLY', 'LEAD_TIME_BASED']
        
        current_method = existing_data.get('calculation_method', 'FIXED')
        method_idx = methods.index(current_method) if current_method in methods else 0
        
        calculation_method = st.selectbox(
            "Calculation Method",
            options=methods,
            index=method_idx,
            help="FIXED: Manual | DAYS_OF_SUPPLY: Days × Demand | LEAD_TIME_BASED: Statistical with service level"
        )
        
        calc_params = {}
        
        if calculation_method == 'DAYS_OF_SUPPLY':
            col1, col2 = st.columns(2)
            with col1:
                # Handle NULL/0 values by using default 14
                safety_days_value = existing_data.get('safety_days')
                if safety_days_value is None or safety_days_value == 0:
                    safety_days_value = 14
                
                calc_params['safety_days'] = st.number_input(
                    "Safety Days",
                    min_value=1,
                    value=safe_int(safety_days_value, default=14)
                )
            with col2:
                calc_params['avg_daily_demand'] = st.number_input(
                    "Avg Daily Demand",
                    min_value=0.0,
                    value=safe_float(existing_data.get('avg_daily_demand', 0))
                )
        
        elif calculation_method == 'LEAD_TIME_BASED':
            col1, col2 = st.columns(2)
            with col1:
                # Handle NULL/0 values by using default 7
                lead_time_value = existing_data.get('lead_time_days')
                if lead_time_value is None or lead_time_value == 0:
                    lead_time_value = 7
                
                calc_params['lead_time_days'] = st.number_input(
                    "Lead Time (days)",
                    min_value=1,
                    value=safe_int(lead_time_value, default=7)
                )
                calc_params['service_level_percent'] = st.selectbox(
                    "Service Level %",
                    options=list(Z_SCORE_MAP.keys()),
                    index=4
                )
            with col2:
                calc_params['demand_std_deviation'] = st.number_input(
                    "Demand Std Dev",
                    min_value=0.0,
                    value=safe_float(existing_data.get('demand_std_deviation', 0))
                )
        
        if calculation_method != 'FIXED':
            if st.button("Calculate", type="primary"):
                params = {
                    'product_id': product_id,
                    'entity_id': entity_id,
                    'customer_id': customer_id,
                    **calc_params
                }
                
                result = calculate_safety_stock(calculation_method, **params)
                
                if 'error' not in result:
                    st.session_state.calculated_safety_stock = result['safety_stock_qty']
                    st.success(f"Result: {result['safety_stock_qty']:.2f}")
                    st.info(result['formula_used'])
                    st.rerun()
                else:
                    st.error(result['error'])
    
    # Buttons
    st.divider()
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        if st.button("Save", type="primary", use_container_width=True):
            data = {
                'product_id': product_id,
                'entity_id': entity_id,
                'customer_id': customer_id,
                'safety_stock_qty': safety_stock_qty,
                'reorder_point': reorder_point if reorder_point > 0 else None,
                'reorder_qty': reorder_qty if reorder_qty > 0 else None,
                'effective_from': effective_from,
                'effective_to': effective_to,
                'priority_level': priority_level,
                'business_notes': business_notes if business_notes else None,
                'calculation_method': calculation_method,
                'is_active': 1,
                **calc_params
            }
            
            is_valid, errors = validate_safety_stock_data(
                data, 
                mode=mode,
                exclude_id=record_id if mode == 'edit' else None
            )
            
            if is_valid:
                if mode == 'add':
                    success, result = create_safety_stock(data, st.session_state.username)
                else:
                    success, result = update_safety_stock(record_id, data, st.session_state.username)
                
                if success:
                    if 'calculated_safety_stock' in st.session_state:
                        del st.session_state.calculated_safety_stock
                    st.success(f"{'Created' if mode == 'add' else 'Updated'} successfully!")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(f"Error: {result}")
            else:
                st.error(get_validation_summary(errors))
    
    with col2:
        if st.button("Cancel", use_container_width=True):
            if 'calculated_safety_stock' in st.session_state:
                del st.session_state.calculated_safety_stock
            st.rerun()
    
    with col3:
        if mode == 'edit':
            if st.button("Create Review", use_container_width=True):
                review_dialog(record_id)


@st.dialog("Review Safety Stock", width="large")
def review_dialog(safety_stock_id):
    """Review and adjust safety stock quantity ONLY - for regular users"""
    
    current_data = get_safety_stock_by_id(safety_stock_id)
    if not current_data:
        st.error("Record not found")
        return
    
    st.markdown("### 📋 Safety Stock Review")
    st.info("ℹ️ Review process: Change quantity with documented reason. Use Edit for other changes.")
    
    # Current context (read-only)
    st.subheader("Current Information")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Product", current_data.get('pt_code', 'N/A'))
    with col2:
        st.metric("Entity", current_data.get('entity_name', 'N/A')[:20])
    with col3:
        st.metric("Current Qty", f"{safe_float(current_data.get('safety_stock_qty')):.0f}")
    with col4:
        st.metric("Method", current_data.get('calculation_method', 'FIXED'))
    
    # Show additional context
    with st.expander("View Current Settings", expanded=False):
        info_col1, info_col2 = st.columns(2)
        with info_col1:
            st.write(f"**Reorder Point:** {safe_float(current_data.get('reorder_point', 0)):.0f}")
            st.write(f"**Effective From:** {current_data.get('effective_from')}")
            st.write(f"**Priority:** {current_data.get('priority_level')}")
        with info_col2:
            st.write(f"**Reorder Qty:** {safe_float(current_data.get('reorder_qty', 0)):.0f}")
            st.write(f"**Effective To:** {current_data.get('effective_to') or 'Ongoing'}")
            customer = current_data.get('customer_name') or 'General Rule'
            st.write(f"**Customer:** {customer}")
    
    st.divider()
    st.subheader("Review Decision")
    
    # Review form - ONLY quantity change
    col1, col2 = st.columns(2)
    
    with col1:
        old_qty = safe_float(current_data.get('safety_stock_qty'))
        new_safety_stock_qty = st.number_input(
            "New Safety Stock Quantity *",
            min_value=0.0,
            value=old_qty,
            step=1.0,
            help="Adjust the safety stock quantity based on performance"
        )
        
        # Auto-determine action
        if new_safety_stock_qty > old_qty:
            default_action = 'INCREASED'
        elif new_safety_stock_qty < old_qty:
            default_action = 'DECREASED'
        else:
            default_action = 'NO_CHANGE'
        
        action_idx = ['NO_CHANGE', 'INCREASED', 'DECREASED', 'METHOD_CHANGED'].index(default_action)
        action_taken = st.selectbox(
            "Action *",
            options=['NO_CHANGE', 'INCREASED', 'DECREASED', 'METHOD_CHANGED'],
            index=action_idx,
            help="System auto-detected based on quantity change"
        )
        
        # Show change summary
        if new_safety_stock_qty != old_qty:
            change = new_safety_stock_qty - old_qty
            pct_change = (change / old_qty * 100) if old_qty > 0 else 0
            if change > 0:
                st.success(f"↑ Increase: +{change:.0f} units (+{pct_change:.1f}%)")
            else:
                st.warning(f"↓ Decrease: {change:.0f} units ({pct_change:.1f}%)")
        else:
            st.info("No quantity change")
    
    with col2:
        review_type = st.selectbox(
            "Review Type",
            options=['PERIODIC', 'EXCEPTION', 'EMERGENCY', 'ANNUAL'],
            help="What triggered this review?"
        )
        
        action_reason = st.text_area(
            "Reason for Change *",
            help="⚠️ REQUIRED: Explain why this change is needed. This is the audit trail.",
            height=120,
            placeholder="Example: Had 3 stockouts last month due to increased demand from new customer campaign..."
        )
    
    review_notes = st.text_area(
        "Additional Notes (Optional)",
        help="Any additional context, observations, or action items",
        height=80,
        placeholder="Optional: Add any supporting information..."
    )
    
    st.divider()
    
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("Submit Review", type="primary", use_container_width=True):
            # Validation
            if not action_reason or len(action_reason.strip()) < 10:
                st.error("⚠️ Please provide a meaningful reason (at least 10 characters)")
                return
            
            # Consistency check
            if action_taken == 'INCREASED' and new_safety_stock_qty <= old_qty:
                st.error("Action is INCREASED but quantity didn't increase")
                return
            elif action_taken == 'DECREASED' and new_safety_stock_qty >= old_qty:
                st.error("Action is DECREASED but quantity didn't decrease")
                return
            
            review_data = {
                'review_date': datetime.now().date(),
                'review_type': review_type,
                'old_safety_stock_qty': old_qty,
                'new_safety_stock_qty': new_safety_stock_qty,
                'action_taken': action_taken,
                'action_reason': action_reason.strip(),
                'review_notes': review_notes.strip() if review_notes else None
            }
            
            # Create review record
            success, message = create_safety_stock_review(
                safety_stock_id,
                review_data,
                st.session_state.username
            )
            
            if success:
                # Update quantity if changed
                if new_safety_stock_qty != old_qty:
                    update_data = {'safety_stock_qty': new_safety_stock_qty}
                    update_safety_stock(safety_stock_id, update_data, st.session_state.username)
                
                st.success("✅ Review submitted and quantity updated successfully!")
                st.cache_data.clear()
                st.rerun()
            else:
                st.error(f"❌ Error: {message}")
    
    with col2:
        if st.button("Cancel", use_container_width=True):
            st.rerun()


@st.dialog("Bulk Upload", width="large")
def bulk_upload_dialog():
    """Bulk upload safety stock data"""
    
    st.markdown("### Bulk Upload Safety Stock")
    
    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("Download Template", use_container_width=True):
            template = create_upload_template(include_sample_data=True)
            st.download_button(
                label="Save Template",
                data=template,
                file_name=f"safety_stock_template_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    
    st.divider()
    
    uploaded_file = st.file_uploader("Choose Excel file", type=['xlsx', 'xls'])
    
    if uploaded_file:
        try:
            with st.spinner("Reading file..."):
                df = pd.read_excel(uploaded_file)
            
            if df.iloc[0].astype(str).str.contains('Required|Optional').any():
                df = df.iloc[1:].reset_index(drop=True)
            
            st.info(f"Found {len(df)} rows")
            st.dataframe(df.head(10), use_container_width=True)
            
            with st.spinner("Validating..."):
                is_valid, validated_df, errors = validate_bulk_data(df)
            
            if not is_valid:
                st.error("Validation failed:")
                for error in errors[:10]:
                    st.write(f"• {error}")
            else:
                st.success("Validation passed")
                
                if st.button("Import Data", type="primary"):
                    with st.spinner("Importing..."):
                        data_list = validated_df.to_dict('records')
                        success, message, results = bulk_create_safety_stock(
                            data_list,
                            st.session_state.username
                        )
                    
                    if success:
                        st.success(message)
                        if results['failed'] > 0:
                            with st.expander("Errors"):
                                for error in results['errors']:
                                    st.write(f"• {error}")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(f"Failed: {message}")
        
        except Exception as e:
            st.error(f"Error: {e}")


# ==================== Main Page ====================

def main():
    st.title("📦 Safety Stock Management")
    
    # Stats
    stats = get_quick_stats()
    if stats:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Active Rules", stats.total_items or 0)
        with col2:
            st.metric("Customer Rules", stats.customer_rules or 0)
        with col3:
            st.metric("Needs Review", stats.needs_review or 0)
        with col4:
            st.metric("Unique Products", stats.unique_products or 0)
    
    st.divider()
    
    # Filters
    with st.container():
        st.subheader("Filters")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            entities = load_entities()
            entity_opts = ['All Entities'] + (entities['company_code'] + ' - ' + entities['english_name']).tolist()
            selected_entity = st.selectbox("Entity", range(len(entity_opts)), format_func=lambda x: entity_opts[x])
            st.session_state.ss_filters['entity_id'] = entities.iloc[selected_entity - 1]['id'] if selected_entity > 0 else None
        
        with col2:
            customers = load_customers()
            customer_opts = ['All Customers', 'General Rules Only'] + (customers['company_code'] + ' - ' + customers['english_name']).tolist()
            selected_customer = st.selectbox("Customer", range(len(customer_opts)), format_func=lambda x: customer_opts[x])
            
            if selected_customer == 0:
                st.session_state.ss_filters['customer_id'] = None
            elif selected_customer == 1:
                st.session_state.ss_filters['customer_id'] = 'general'
            else:
                st.session_state.ss_filters['customer_id'] = customers.iloc[selected_customer - 2]['id']
        
        with col3:
            product_search = st.text_input("Product Search", placeholder="PT Code or Name", value=st.session_state.ss_filters['product_search'])
            st.session_state.ss_filters['product_search'] = product_search
        
        with col4:
            status = st.selectbox("Status", ['active', 'all', 'expired', 'future'], format_func=lambda x: x.title())
            st.session_state.ss_filters['status'] = status
    
    # Actions
    st.divider()
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        if st.button("Add Safety Stock", type="primary", use_container_width=True):
            safety_stock_form('add')
    
    with col2:
        if st.button("Bulk Upload", use_container_width=True):
            bulk_upload_dialog()
    
    with col3:
        if st.button("Export Excel", use_container_width=True):
            df = get_safety_stock_levels(
                entity_id=st.session_state.ss_filters['entity_id'],
                customer_id=None if st.session_state.ss_filters['customer_id'] == 'general' else st.session_state.ss_filters['customer_id'],
                product_search=st.session_state.ss_filters['product_search'],
                status=st.session_state.ss_filters['status']
            )
            
            if not df.empty:
                excel_file = export_to_excel(df)
                st.download_button(
                    "Download",
                    excel_file,
                    f"safety_stock_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            else:
                st.warning("No data")
    
    with col4:
        if st.button("Review Report", use_container_width=True):
            report = generate_review_report()
            st.download_button(
                "Download",
                report,
                f"review_{datetime.now().strftime('%Y%m%d')}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    
    with col5:
        if st.button("Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    # Data table
    st.divider()
    
    df = get_safety_stock_levels(
        entity_id=st.session_state.ss_filters['entity_id'],
        customer_id=None if st.session_state.ss_filters['customer_id'] == 'general' else st.session_state.ss_filters['customer_id'],
        product_search=st.session_state.ss_filters['product_search'],
        status=st.session_state.ss_filters['status']
    )
    
    if df.empty:
        st.info("No records found")
    else:
        # Updated display columns (removed min/max stock)
        display_cols = [
            'pt_code', 'product_name', 'entity_code', 'customer_code',
            'safety_stock_qty', 'reorder_point', 'reorder_qty',
            'calculation_method', 'rule_type', 
            'status', 'effective_from', 'priority_level'
        ]
        
        display_df = df[display_cols].copy()
        display_df['customer_code'] = display_df['customer_code'].fillna('All')
        
        st.subheader(f"Safety Stock Rules ({len(df)} records)")
        
        selected = st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun"
        )
        
        if selected and selected.selection.rows:
            idx = selected.selection.rows[0]
            record = df.iloc[idx]
            
            st.divider()
            st.subheader("Actions")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if st.button("Edit", use_container_width=True):
                    safety_stock_form('edit', record['id'])
            
            with col2:
                if st.button("Review", use_container_width=True):
                    review_dialog(record['id'])
            
            with col3:
                if st.button("History", use_container_width=True):
                    history = get_review_history(record['id'])
                    if not history.empty:
                        st.dataframe(history, use_container_width=True)
                    else:
                        st.info("No history")
            
            with col4:
                if st.button("Delete", type="secondary", use_container_width=True):
                    if st.checkbox("Confirm delete?"):
                        success, msg = delete_safety_stock(record['id'], st.session_state.username)
                        if success:
                            st.success("Deleted")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(msg)


if __name__ == "__main__":
    main()