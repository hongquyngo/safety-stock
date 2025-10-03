# pages/1_📦_Safety_Stock_Management.py
"""
Safety Stock Management Main Page
Version 3.1 - Improved UX with auto-fetch, auto-select method, and better layouts
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
from utils.safety_stock.calculations import (
    calculate_safety_stock, 
    Z_SCORE_MAP,
)
from utils.safety_stock.demand_analysis import (
    fetch_demand_stats,
    get_lead_time_estimate,
)
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
from utils.safety_stock.permissions import (
    get_user_role,
    has_permission,
    filter_data_for_customer,
    get_permission_message,
    get_user_info_display,
    apply_export_limit,
    log_action
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

# Check basic view permission
if not has_permission('view'):
    st.error("You don't have permission to access this page")
    st.stop()

# Initialize session state
if 'ss_filters' not in st.session_state:
    st.session_state.ss_filters = {
        'entity_id': None,
        'customer_id': None,
        'product_id': None,
        'product_search': '',
        'status': 'active'
    }

# Temporary storage for auto-fetched values
if 'temp_demand_data' not in st.session_state:
    st.session_state.temp_demand_data = {}

# ==================== Data Loading Functions ====================

@st.cache_data(ttl=300)
def load_existing_filter_options():
    """Load filter options only from existing safety stock data"""
    try:
        engine = get_db_engine()
        
        # Get entities with safety stock data
        entity_query = text("""
        SELECT DISTINCT 
            e.id,
            e.company_code,
            e.english_name
        FROM safety_stock_levels s
        JOIN companies e ON s.entity_id = e.id
        WHERE s.delete_flag = 0 AND s.is_active = 1
        ORDER BY e.company_code
        """)
        
        # Get customers with safety stock data
        customer_query = text("""
        SELECT DISTINCT 
            c.id,
            c.company_code,
            c.english_name
        FROM safety_stock_levels s
        LEFT JOIN companies c ON s.customer_id = c.id
        WHERE s.delete_flag = 0 AND s.is_active = 1
        AND s.customer_id IS NOT NULL
        ORDER BY c.company_code
        """)
        
        # Get products with safety stock data
        product_query = text("""
        SELECT DISTINCT 
            p.id,
            p.pt_code,
            p.name,
            p.package_size,
            b.brand_name
        FROM safety_stock_levels s
        JOIN products p ON s.product_id = p.id
        LEFT JOIN brands b ON p.brand_id = b.id
        WHERE s.delete_flag = 0 AND s.is_active = 1
        ORDER BY p.pt_code
        """)
        
        with engine.connect() as conn:
            entities_df = pd.read_sql(entity_query, conn)
            customers_df = pd.read_sql(customer_query, conn)
            products_df = pd.read_sql(product_query, conn)
        
        # Format display text
        entities = (entities_df['company_code'] + ' - ' + entities_df['english_name']).tolist()
        entity_ids = entities_df['id'].tolist()
        
        customers = []
        customer_ids = []
        if not customers_df.empty:
            customers = (customers_df['company_code'] + ' - ' + customers_df['english_name']).tolist()
            customer_ids = customers_df['id'].tolist()
        
        products = []
        product_ids = []
        if not products_df.empty:
            for _, row in products_df.iterrows():
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
                
                products.append(display)
                product_ids.append(row['id'])
        
        return {
            'entities': entities,
            'entity_ids': entity_ids,
            'customers': customers,
            'customer_ids': customer_ids,
            'products': products,
            'product_ids': product_ids
        }
        
    except Exception as e:
        logger.error(f"Error loading filter options: {e}")
        return {
            'entities': [],
            'entity_ids': [],
            'customers': [],
            'customer_ids': [],
            'products': [],
            'product_ids': []
        }

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
    """Add/Edit safety stock dialog with improved UX"""
    
    # Check permission
    required_permission = 'create' if mode == 'add' else 'edit'
    if not has_permission(required_permission):
        st.error(get_permission_message(required_permission))
        return
    
    # CLEAR OLD DATA when opening new dialog (FIX BUG 2)
    if mode == 'add':
        # Clear any leftover data from previous sessions
        keys_to_clear = ['temp_demand_data', 'calculated_ss', 'calculated_rop', 'selected_method', 'auto_calculated']
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
    
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
    
    # Initialize method in session state
    if 'selected_method' not in st.session_state:
        st.session_state.selected_method = existing_data.get('calculation_method', 'FIXED')
    
    # Tabs
    tab1, tab2 = st.tabs(["Basic Information", "Stock Levels & Calculation"])
    
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
        # IMPROVED FLOW: Fetch -> Auto-select -> Auto-fill
        st.markdown("#### 📊 Historical Demand Analysis")
        
        # Auto-fetch section
        fetch_expanded = 'temp_demand_data' not in st.session_state
        with st.expander("Auto-Fetch Historical Demand", expanded=fetch_expanded):
            col1, col2, col3 = st.columns([2, 2, 1])
            with col1:
                fetch_days = st.number_input(
                    "Analyze last N days",
                    min_value=30,
                    max_value=365,
                    value=90,
                    step=30
                )
            with col2:
                exclude_pending = st.checkbox("Exclude pending deliveries", value=True)
            with col3:
                if st.button("Fetch Data", type="primary", use_container_width=True):
                    with st.spinner("Fetching from delivery_full_view..."):
                        stats = fetch_demand_stats(
                            product_id=product_id,
                            entity_id=entity_id,
                            customer_id=customer_id,
                            days_back=fetch_days,
                            exclude_pending=exclude_pending
                        )
                        
                        lead_time_info = get_lead_time_estimate(
                            product_id=product_id,
                            entity_id=entity_id,
                            customer_id=customer_id
                        )
                        
                        # Store data
                        st.session_state.temp_demand_data = stats
                        if lead_time_info['sample_size'] > 0:
                            st.session_state.temp_demand_data['lead_time_days'] = lead_time_info['avg_lead_time_days']
                            st.session_state.temp_demand_data['lead_time_info'] = lead_time_info
                        
                        # Auto-select suggested method
                        if stats['data_points'] > 0:
                            st.session_state.selected_method = stats['suggested_method']
                            st.rerun()
            
            # Display fetched data if available
            if 'temp_demand_data' in st.session_state and st.session_state.temp_demand_data.get('data_points', 0) > 0:
                stats = st.session_state.temp_demand_data
                
                st.success(f"✓ Found {stats['data_points']} delivery dates")
                
                # Main metrics in 4 columns
                metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
                
                metric_col1.metric("Data Points", f"{stats['data_points']}")
                metric_col2.metric("Avg/Day", f"{stats['avg_daily_demand']:.1f}")
                metric_col3.metric("Std Dev", f"{stats['demand_std_dev']:.1f}")
                
                cv = stats['cv_percent']
                color = "🟢" if cv < 20 else "🟡" if cv < 50 else "🔴"
                metric_col4.metric("Variability", f"{color} {cv:.0f}%")
                
                # Recommendation with range info
                st.info(f"💡 Suggested method: **{stats['suggested_method']}** | Range: {stats['min_daily_demand']:.0f} - {stats['max_daily_demand']:.0f} units/day")
                
                # Lead time if available
                if 'lead_time_info' in stats:
                    lead_info = stats['lead_time_info']
                    st.success(f"📦 Estimated lead time: **{lead_info['avg_lead_time_days']:.0f} days** (from {lead_info['sample_size']} deliveries)")
        
        st.divider()
        
        # Calculation Method Selection
        st.markdown("#### Calculation Method")
        
        # Get current method (either from session state or existing data)
        current_method = st.session_state.get('selected_method', existing_data.get('calculation_method', 'FIXED'))
        
        # Show auto-selected info
        if 'temp_demand_data' in st.session_state and st.session_state.temp_demand_data.get('data_points', 0) > 0:
            st.info(f"✅ Method auto-selected based on demand analysis: **{current_method}**")
        
        methods = ['FIXED', 'DAYS_OF_SUPPLY', 'LEAD_TIME_BASED']
        calculation_method = st.selectbox(
            "Select Calculation Method",
            options=methods,
            index=methods.index(current_method),
            key="calc_method_selector",  # Add unique key to avoid conflicts
            help="FIXED: Manual input | DAYS_OF_SUPPLY: Days × Demand | LEAD_TIME_BASED: Statistical with service level"
        )
        
        # Update selected method if manually changed
        if calculation_method != current_method:
            st.session_state.selected_method = calculation_method
        
        # Get temp data for auto-fill
        temp_data = st.session_state.get('temp_demand_data', {})
        has_auto_data = bool(temp_data and temp_data.get('data_points', 0) > 0)
        
        # Dynamic Parameters based on method
        st.markdown("#### Parameters")
        
        if calculation_method == 'FIXED':
            # Manual input for everything
            col1, col2 = st.columns(2)
            with col1:
                safety_stock_qty = st.number_input(
                    "Safety Stock Quantity *",
                    min_value=0.0,
                    value=safe_float(existing_data.get('safety_stock_qty', 0)),
                    step=1.0,
                    help="Manually set safety stock buffer"
                )
            with col2:
                reorder_point = st.number_input(
                    "Reorder Point",
                    min_value=0.0,
                    value=safe_float(existing_data.get('reorder_point', 0)),
                    step=1.0,
                    help="Inventory level that triggers reorder"
                )
            
            calc_params = {'calculation_method': 'FIXED'}
            
        elif calculation_method == 'DAYS_OF_SUPPLY':
            if has_auto_data:
                st.caption("📊 Fields auto-filled from historical data analysis")
            
            col1, col2 = st.columns(2)
            with col1:
                safety_days = st.number_input(
                    "Safety Days *",
                    min_value=1,
                    value=safe_int(existing_data.get('safety_days', 14)),
                    help="Number of days of demand to maintain as buffer"
                )
                
                avg_daily_demand = st.number_input(
                    "Avg Daily Demand" + (" ✓" if has_auto_data else ""),
                    min_value=0.0,
                    value=safe_float(temp_data.get('avg_daily_demand', 0) if has_auto_data else existing_data.get('avg_daily_demand', 0)),
                    step=0.1,
                    help="Units per day (editable)"
                )
            
            with col2:
                lead_time_days = st.number_input(
                    "Lead Time (days)" + (" ✓" if has_auto_data and 'lead_time_days' in temp_data else ""),
                    min_value=1,
                    value=safe_int(temp_data.get('lead_time_days', 7) if has_auto_data else existing_data.get('lead_time_days', 7)),
                    help="Days from order to delivery"
                )
            
            # Calculate button
            if st.button("Calculate Safety Stock & Reorder Point", type="primary"):
                result = calculate_safety_stock(
                    method='DAYS_OF_SUPPLY',
                    safety_days=safety_days,
                    avg_daily_demand=avg_daily_demand,
                    lead_time_days=lead_time_days
                )
                
                if 'error' not in result:
                    st.session_state.calculated_ss = result['safety_stock_qty']
                    st.session_state.calculated_rop = result['reorder_point']
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.success(f"✓ Safety Stock: **{result['safety_stock_qty']:.2f}** units")
                    with col2:
                        st.success(f"✓ Reorder Point: **{result['reorder_point']:.2f}** units")
                    
                    st.caption(f"Formula: {result['formula_used']}")
                else:
                    st.error(result['error'])
            
            # Results
            col1, col2 = st.columns(2)
            with col1:
                safety_stock_qty = st.number_input(
                    "Safety Stock Quantity" + (" ✓" if 'calculated_ss' in st.session_state else ""),
                    min_value=0.0,
                    value=safe_float(st.session_state.get('calculated_ss', existing_data.get('safety_stock_qty', 0))),
                    step=1.0,
                    help="Calculated value (editable)"
                )
            
            with col2:
                reorder_point = st.number_input(
                    "Reorder Point" + (" ✓" if 'calculated_rop' in st.session_state else ""),
                    min_value=0.0,
                    value=safe_float(st.session_state.get('calculated_rop', existing_data.get('reorder_point', 0))),
                    step=1.0,
                    help="Calculated value (editable)"
                )
            
            calc_params = {
                'calculation_method': 'DAYS_OF_SUPPLY',
                'safety_days': safety_days,
                'avg_daily_demand': avg_daily_demand,
                'lead_time_days': lead_time_days
            }
            
        elif calculation_method == 'LEAD_TIME_BASED':
            if has_auto_data:
                st.caption("📊 Fields auto-filled from historical data analysis")
            
            col1, col2 = st.columns(2)
            with col1:
                lead_time_days = st.number_input(
                    "Lead Time (days) *" + (" ✓" if has_auto_data and 'lead_time_days' in temp_data else ""),
                    min_value=1,
                    value=safe_int(temp_data.get('lead_time_days', 7) if has_auto_data else existing_data.get('lead_time_days', 7)),
                    help="Days from order to delivery"
                )
                
                service_level_options = list(Z_SCORE_MAP.keys())
                current_sl = existing_data.get('service_level_percent', 95.0)
                sl_idx = service_level_options.index(current_sl) if current_sl in service_level_options else 4
                
                service_level_percent = st.selectbox(
                    "Service Level % *",
                    options=service_level_options,
                    index=sl_idx,
                    help="Target probability of no stockout"
                )
            
            with col2:
                demand_std_deviation = st.number_input(
                    "Demand Std Deviation" + (" ✓" if has_auto_data else ""),
                    min_value=0.0,
                    value=safe_float(temp_data.get('demand_std_dev', 0) if has_auto_data else existing_data.get('demand_std_deviation', 0)),
                    step=0.1,
                    help="Standard deviation of daily demand"
                )
                
                avg_daily_demand = st.number_input(
                    "Avg Daily Demand" + (" ✓" if has_auto_data else ""),
                    min_value=0.0,
                    value=safe_float(temp_data.get('avg_daily_demand', 0) if has_auto_data else existing_data.get('avg_daily_demand', 0)),
                    step=0.1,
                    help="Units per day"
                )
            
            # Calculate button
            if st.button("Calculate Safety Stock & Reorder Point", type="primary"):
                result = calculate_safety_stock(
                    method='LEAD_TIME_BASED',
                    lead_time_days=lead_time_days,
                    service_level_percent=service_level_percent,
                    demand_std_deviation=demand_std_deviation,
                    avg_daily_demand=avg_daily_demand
                )
                
                if 'error' not in result:
                    st.session_state.calculated_ss = result['safety_stock_qty']
                    st.session_state.calculated_rop = result['reorder_point']
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.success(f"✓ Safety Stock: **{result['safety_stock_qty']:.2f}** units")
                    with col2:
                        st.success(f"✓ Reorder Point: **{result['reorder_point']:.2f}** units")
                    
                    st.caption(f"Formula: {result['formula_used']}")
                else:
                    st.error(result['error'])
            
            # Results
            col1, col2 = st.columns(2)
            with col1:
                safety_stock_qty = st.number_input(
                    "Safety Stock Quantity" + (" ✓" if 'calculated_ss' in st.session_state else ""),
                    min_value=0.0,
                    value=safe_float(st.session_state.get('calculated_ss', existing_data.get('safety_stock_qty', 0))),
                    step=1.0,
                    help="Calculated value (editable)"
                )
            
            with col2:
                reorder_point = st.number_input(
                    "Reorder Point" + (" ✓" if 'calculated_rop' in st.session_state else ""),
                    min_value=0.0,
                    value=safe_float(st.session_state.get('calculated_rop', existing_data.get('reorder_point', 0))),
                    step=1.0,
                    help="Calculated value (editable)"
                )
            
            calc_params = {
                'calculation_method': 'LEAD_TIME_BASED',
                'lead_time_days': lead_time_days,
                'service_level_percent': service_level_percent,
                'demand_std_deviation': demand_std_deviation,
                'avg_daily_demand': avg_daily_demand
            }
    
    # Action buttons
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
                'effective_from': effective_from,
                'effective_to': effective_to,
                'priority_level': priority_level,
                'business_notes': business_notes if business_notes else None,
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
                    log_action('CREATE', f"Created safety stock for product {product_id}")
                else:
                    success, result = update_safety_stock(record_id, data, st.session_state.username)
                    log_action('UPDATE', f"Updated safety stock ID {record_id}")
                
                if success:
                    # Clear ALL temp data properly
                    keys_to_clear = ['calculated_ss', 'calculated_rop', 'temp_demand_data', 
                                   'selected_method', 'auto_calculated']
                    for key in keys_to_clear:
                        if key in st.session_state:
                            del st.session_state[key]
                    st.success(f"{'Created' if mode == 'add' else 'Updated'} successfully!")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(f"Error: {result}")
            else:
                st.error(get_validation_summary(errors))
    
    with col2:
        if st.button("Cancel", use_container_width=True):
            # Clear ALL temp data properly
            keys_to_clear = ['calculated_ss', 'calculated_rop', 'temp_demand_data', 
                           'selected_method', 'auto_calculated']
            for key in keys_to_clear:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
    
    with col3:
        if mode == 'edit' and has_permission('review'):
            if st.button("Create Review", use_container_width=True):
                review_dialog(record_id)


@st.dialog("Review Safety Stock", width="large")
def review_dialog(safety_stock_id):
    """Review and adjust safety stock quantity"""
    
    if not has_permission('review'):
        st.error(get_permission_message('review'))
        return
    
    current_data = get_safety_stock_by_id(safety_stock_id)
    if not current_data:
        st.error("Record not found")
        return
    
    st.markdown("### 📋 Safety Stock Review")
    st.info("ℹ️ Review process: Change quantity with documented reason.")
    
    # Current context
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
            st.write(f"**Effective To:** {current_data.get('effective_to') or 'Ongoing'}")
            customer = current_data.get('customer_name') or 'General Rule'
            st.write(f"**Customer:** {customer}")
    
    st.divider()
    st.subheader("Review Decision")
    
    # Review form
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
            help="⚠️ REQUIRED: Explain why this change is needed.",
            height=120,
            placeholder="Example: Had 3 stockouts last month due to increased demand..."
        )
        
        # Approval section
        if has_permission('approve'):
            st.divider()
            approve_review = st.checkbox("Approve this review")
        else:
            approve_review = False
    
    review_notes = st.text_area(
        "Additional Notes (Optional)",
        help="Any additional context or observations",
        height=80
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
                'review_notes': review_notes.strip() if review_notes else None,
                'approved_by': st.session_state.username if approve_review else None
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
                
                log_action('REVIEW', f"Reviewed safety stock ID {safety_stock_id}")
                st.success("✅ Review submitted successfully!")
                st.cache_data.clear()
                st.rerun()
            else:
                st.error(f"⚠️ Error: {message}")
    
    with col2:
        if st.button("Cancel", use_container_width=True):
            st.rerun()


@st.dialog("Bulk Upload", width="large")
def bulk_upload_dialog():
    """Bulk upload safety stock data"""
    
    if not has_permission('bulk_upload'):
        st.error(get_permission_message('bulk_upload'))
        return
    
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
                        log_action('BULK_UPLOAD', f"Uploaded {results['created']} records")
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
    
    # Display user info
    col1, col2 = st.columns([3, 1])
    with col2:
        st.caption(get_user_info_display())
    
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
        
        # Load filter options
        existing_filters = load_existing_filter_options()
        
        with col1:
            # Entity filter
            entity_opts = ['All Entities'] + existing_filters['entities']
            selected_entity = st.selectbox("Entity", entity_opts)
            
            if selected_entity == 'All Entities':
                st.session_state.ss_filters['entity_id'] = None
            else:
                entity_id = existing_filters['entity_ids'][existing_filters['entities'].index(selected_entity)]
                st.session_state.ss_filters['entity_id'] = entity_id
        
        with col2:
            # Customer filter
            customer_opts = ['All Customers', 'General Rules Only'] + existing_filters['customers']
            
            # Filter for customer role
            if get_user_role() == 'customer':
                customer_id = st.session_state.get('customer_id')
                if customer_id:
                    customer_name = next((c for c in existing_filters['customers'] 
                                         if existing_filters['customer_ids'][existing_filters['customers'].index(c)] == customer_id), None)
                    if customer_name:
                        customer_opts = [customer_name]
            
            selected_customer = st.selectbox("Customer", customer_opts)
            
            if selected_customer == 'All Customers':
                st.session_state.ss_filters['customer_id'] = None
            elif selected_customer == 'General Rules Only':
                st.session_state.ss_filters['customer_id'] = 'general'
            else:
                if selected_customer in existing_filters['customers']:
                    customer_id = existing_filters['customer_ids'][existing_filters['customers'].index(selected_customer)]
                    st.session_state.ss_filters['customer_id'] = customer_id
        
        with col3:
            # Product filter
            product_opts = ['All Products'] + existing_filters['products']
            
            if 'product_filter' not in st.session_state:
                st.session_state.product_filter = 'All Products'
            
            selected_product = st.selectbox(
                "Product Search",
                options=product_opts,
                index=0,
                placeholder="Select or type to search...",
                help="Select product or type PT code/name to search"
            )
            
            if selected_product == 'All Products':
                st.session_state.ss_filters['product_id'] = None
                st.session_state.ss_filters['product_search'] = ''
            else:
                if selected_product in existing_filters['products']:
                    product_id = existing_filters['product_ids'][existing_filters['products'].index(selected_product)]
                    st.session_state.ss_filters['product_id'] = product_id
                    st.session_state.ss_filters['product_search'] = ''
        
        with col4:
            # Status filter
            status_options = {
                'Active': 'active',
                'All': 'all', 
                'Expired': 'expired',
                'Future': 'future'
            }
            
            status_display = list(status_options.keys())
            selected_status = st.selectbox(
                "Status",
                options=status_display,
                index=0,
                help="Active: Currently effective | Expired: Past effective date | Future: Not yet effective | All: Show everything"
            )
            
            st.session_state.ss_filters['status'] = status_options[selected_status]
    
    # Actions
    st.divider()
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        if st.button("Add Safety Stock", 
                    type="primary", 
                    use_container_width=True,
                    disabled=not has_permission('create')):
            safety_stock_form('add')
    
    with col2:
        if st.button("Bulk Upload", 
                    use_container_width=True,
                    disabled=not has_permission('bulk_upload')):
            bulk_upload_dialog()
    
    with col3:
        if st.button("Export Excel", use_container_width=True):
            export_filters = {
                'entity_id': st.session_state.ss_filters['entity_id'],
                'customer_id': None if st.session_state.ss_filters['customer_id'] == 'general' else st.session_state.ss_filters['customer_id'],
                'status': st.session_state.ss_filters['status']
            }
            
            if st.session_state.ss_filters.get('product_id'):
                export_filters['product_id'] = st.session_state.ss_filters['product_id']
            
            df = get_safety_stock_levels(**export_filters)
            df = filter_data_for_customer(df)
            df, was_limited = apply_export_limit(df)
            
            if was_limited:
                st.warning(f"Export limited to {len(df)} rows based on your role")
            
            if not df.empty:
                excel_file = export_to_excel(df)
                st.download_button(
                    "Download",
                    excel_file,
                    f"safety_stock_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
                log_action('EXPORT', f"Exported {len(df)} records")
            else:
                st.warning("No data to export")
    
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
    
    # Get filtered data
    filters = {
        'entity_id': st.session_state.ss_filters['entity_id'],
        'customer_id': None if st.session_state.ss_filters['customer_id'] == 'general' else st.session_state.ss_filters['customer_id'],
        'status': st.session_state.ss_filters['status']
    }
    
    if st.session_state.ss_filters.get('product_id'):
        filters['product_id'] = st.session_state.ss_filters['product_id']
    elif st.session_state.ss_filters.get('product_search'):
        filters['product_search'] = st.session_state.ss_filters['product_search']
    
    df = get_safety_stock_levels(**filters)
    df = filter_data_for_customer(df)
    
    if df.empty:
        st.info("No records found")
    else:
        # Display columns
        display_cols = [
            'pt_code', 'product_name', 'entity_code', 'customer_code',
            'safety_stock_qty', 'reorder_point',
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
                if st.button("Edit", 
                           use_container_width=True,
                           disabled=not has_permission('edit')):
                    safety_stock_form('edit', record['id'])
            
            with col2:
                if st.button("Review", 
                           use_container_width=True,
                           disabled=not has_permission('review')):
                    review_dialog(record['id'])
            
            with col3:
                if st.button("History", use_container_width=True):
                    history = get_review_history(record['id'])
                    if not history.empty:
                        st.dataframe(history, use_container_width=True)
                    else:
                        st.info("No review history")
            
            with col4:
                if st.button("Delete", 
                           type="secondary",
                           use_container_width=True,
                           disabled=not has_permission('delete')):
                    if st.checkbox("Confirm delete?"):
                        success, msg = delete_safety_stock(record['id'], st.session_state.username)
                        if success:
                            log_action('DELETE', f"Deleted safety stock ID {record['id']}")
                            st.success("Deleted successfully")
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(msg)


if __name__ == "__main__":
    main()