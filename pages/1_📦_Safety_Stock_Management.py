# pages/1_📦_Safety_Stock_Management.py
"""
Safety Stock Management Main Page
Complete CRUD operations with dialog-based forms
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
    Z_SCORE_MAP
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
from sqlalchemy import text

logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Safety Stock Management",
    page_icon="📦",
    layout="wide"
)

# Initialize auth manager
auth_manager = AuthManager()

# Check authentication
if not auth_manager.check_session():
    st.warning("⚠️ Please login to access this page")
    st.stop()

# Initialize session state
if 'ss_filters' not in st.session_state:
    st.session_state.ss_filters = {
        'entity_id': None,
        'customer_id': None,
        'product_search': '',
        'status': 'active'
    }

# ==================== Helper Functions ====================

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
    """Load product list"""
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
            return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error loading products: {e}")
        return pd.DataFrame()

def get_quick_stats():
    """Get quick statistics for dashboard"""
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

# ==================== Dialog Functions ====================

@st.dialog("Safety Stock Configuration", width="large")
def safety_stock_form(mode='add', record_id=None):
    """Add/Edit safety stock dialog"""
    
    # Clean up any stale session state on dialog open
    if 'dialog_initialized' not in st.session_state:
        # Clear old calculation method form fields
        keys_to_clear = [
            'form_safety_days', 'form_avg_demand', 'form_lead_time', 
            'form_service_level', 'form_std_dev',
            'form_safety_days_dos', 'form_avg_demand_dos',
            'form_lead_time_ltb', 'form_service_level_ltb', 'form_std_dev_ltb'
        ]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        st.session_state.dialog_initialized = True
    
    # Load data for edit mode
    existing_data = {}
    if mode == 'edit' and record_id:
        existing_data = get_safety_stock_by_id(record_id) or {}
    
    # Load reference data
    entities = load_entities()
    customers = load_customers()
    products = load_products()
    
    if entities.empty or products.empty:
        st.error("Unable to load required data")
        return
    
    st.markdown(f"### {'Edit' if mode == 'edit' else 'Add New'} Safety Stock")
    
    # Create tabs for organized input
    tab1, tab2, tab3 = st.tabs(["Basic Information", "Stock Levels", "Calculation Method"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            # Product selection
            if mode == 'add':
                product_options = products['pt_code'] + ' - ' + products['name']
                selected_product = st.selectbox(
                    "Product *",
                    options=range(len(products)),
                    format_func=lambda x: product_options.iloc[x]
                )
                product_id = products.iloc[selected_product]['id']
            else:
                st.text_input(
                    "Product",
                    value=f"{existing_data.get('pt_code', '')} - {existing_data.get('product_name', '')}",
                    disabled=True
                )
                product_id = existing_data['product_id']
            
            # Entity selection
            entity_options = entities['company_code'] + ' - ' + entities['english_name']
            entity_idx = 0
            if mode == 'edit' and existing_data.get('entity_id'):
                try:
                    entity_idx = entities[entities['id'] == existing_data['entity_id']].index[0]
                except:
                    pass
            
            selected_entity = st.selectbox(
                "Entity *",
                options=range(len(entities)),
                format_func=lambda x: entity_options.iloc[x],
                index=int(entity_idx),
                disabled=(mode == 'edit')
            )
            entity_id = entities.iloc[selected_entity]['id']
        
        with col2:
            # Customer selection (optional)
            customer_options = ['General Rule (All Customers)'] + \
                              (customers['company_code'] + ' - ' + customers['english_name']).tolist()
            
            customer_idx = 0
            if mode == 'edit' and existing_data.get('customer_id'):
                try:
                    customer_idx = customers[customers['id'] == existing_data['customer_id']].index[0] + 1
                except:
                    pass
            
            selected_customer = st.selectbox(
                "Customer (Optional)",
                options=range(len(customer_options)),
                format_func=lambda x: customer_options[x],
                index=customer_idx
            )
            customer_id = None if selected_customer == 0 else customers.iloc[selected_customer - 1]['id']
            
            # Priority
            priority_level = st.number_input(
                "Priority Level",
                min_value=1,
                max_value=9999,
                value=existing_data.get('priority_level', 100 if customer_id is None else 50),
                help="Lower number = higher priority. Customer rules should be ≤500"
            )
        
        # Date range
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
        
        # Business notes
        business_notes = st.text_area(
            "Business Notes",
            value=existing_data.get('business_notes', '')
        )
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            # Check for calculated value to use as default
            default_ss_qty = float(existing_data.get('safety_stock_qty', 0))
            if 'calculated_safety_stock' in st.session_state:
                default_ss_qty = st.session_state.calculated_safety_stock
            
            safety_stock_qty = st.number_input(
                "Safety Stock Quantity *",
                min_value=0.0,
                value=default_ss_qty,
                step=1.0,
                key="form_safety_stock"
            )
            
            min_stock_qty = st.number_input(
                "Minimum Stock (Optional)",
                min_value=0.0,
                value=float(existing_data.get('min_stock_qty', 0)) if existing_data.get('min_stock_qty') else 0.0,
                step=1.0
            )
            
            max_stock_qty = st.number_input(
                "Maximum Stock (Optional)",
                min_value=0.0,
                value=float(existing_data.get('max_stock_qty', 0)) if existing_data.get('max_stock_qty') else 0.0,
                step=1.0
            )
        
        with col2:
            reorder_point = st.number_input(
                "Reorder Point (Optional)",
                min_value=0.0,
                value=float(existing_data.get('reorder_point', 0)) if existing_data.get('reorder_point') else 0.0,
                step=1.0
            )
            
            reorder_qty = st.number_input(
                "Reorder Quantity (Optional)",
                min_value=0.0,
                value=float(existing_data.get('reorder_qty', 0)) if existing_data.get('reorder_qty') else 0.0,
                step=1.0
            )
            
            if 'calculated_safety_stock' in st.session_state:
                st.info(f"💡 Using calculated value: {st.session_state.calculated_safety_stock:.2f}")
                if st.button("Clear Calculated Value"):
                    del st.session_state.calculated_safety_stock
                    st.rerun()
    
    with tab3:
        calculation_methods = ['FIXED', 'DAYS_OF_SUPPLY', 'DEMAND_PERCENTAGE', 
                             'LEAD_TIME_BASED', 'MIN_MAX', 'STATISTICAL']
        
        current_method = existing_data.get('calculation_method', 'FIXED')
        method_idx = calculation_methods.index(current_method) if current_method in calculation_methods else 0
        
        # Use a key for the selectbox to track state properly
        calculation_method = st.selectbox(
            "Calculation Method",
            options=calculation_methods,
            index=method_idx,
            key="calc_method_select"
        )
        
        # Store method parameters
        calc_params = {}
        
        # Show method-specific parameters based on current selection
        if calculation_method == 'DAYS_OF_SUPPLY':
            col1, col2 = st.columns(2)
            with col1:
                calc_params['safety_days'] = st.number_input(
                    "Safety Days",
                    min_value=1,
                    value=int(existing_data.get('safety_days', 14)),
                    key="form_safety_days_dos"  # Unique key per method
                )
            with col2:
                calc_params['avg_daily_demand'] = st.number_input(
                    "Average Daily Demand",
                    min_value=0.0,
                    value=float(existing_data.get('avg_daily_demand', 0)),
                    key="form_avg_demand_dos"
                )
        
        elif calculation_method == 'LEAD_TIME_BASED':
            col1, col2 = st.columns(2)
            with col1:
                calc_params['lead_time_days'] = st.number_input(
                    "Lead Time (days)",
                    min_value=1,
                    value=int(existing_data.get('lead_time_days', 7)),
                    key="form_lead_time_ltb"
                )
                calc_params['service_level_percent'] = st.selectbox(
                    "Service Level %",
                    options=list(Z_SCORE_MAP.keys()),
                    index=4,  # Default to 95%
                    key="form_service_level_ltb"
                )
            with col2:
                calc_params['demand_std_deviation'] = st.number_input(
                    "Demand Std Deviation",
                    min_value=0.0,
                    value=float(existing_data.get('demand_std_deviation', 0)),
                    key="form_std_dev_ltb"
                )
        
        # Calculate button
        if calculation_method != 'FIXED':
            if st.button("📊 Calculate Safety Stock", type="primary"):
                params = {
                    'product_id': product_id,
                    'entity_id': entity_id,
                    'customer_id': customer_id,
                    **calc_params
                }
                
                result = calculate_safety_stock(calculation_method, **params)
                
                if 'error' not in result:
                    st.session_state.calculated_safety_stock = result['safety_stock_qty']
                    st.success(f"Calculated: {result['safety_stock_qty']:.2f} units")
                    st.info(f"Formula: {result['formula_used']}")
                    st.warning("Please update the Safety Stock Quantity in the Stock Levels tab")
                else:
                    st.error(result['error'])
    
    # Action buttons
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("💾 Save", type="primary"):
            # Prepare data
            data = {
                'product_id': product_id,
                'entity_id': entity_id,
                'customer_id': customer_id,
                'safety_stock_qty': safety_stock_qty,
                'min_stock_qty': min_stock_qty if min_stock_qty > 0 else None,
                'max_stock_qty': max_stock_qty if max_stock_qty > 0 else None,
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
            
            # Validate
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
                    # Clean up session state
                    if 'calculated_safety_stock' in st.session_state:
                        del st.session_state.calculated_safety_stock
                    if 'dialog_initialized' in st.session_state:
                        del st.session_state.dialog_initialized
                    st.success(f"✅ Safety stock {'created' if mode == 'add' else 'updated'} successfully!")
                    st.rerun()
                else:
                    st.error(f"Error: {result}")
            else:
                st.error(get_validation_summary(errors))
    
    with col2:
        if mode == 'edit' and st.button("📝 Create Review"):
            review_dialog(record_id)
    
    with col3:
        if st.button("❌ Cancel"):
            # Clean up session state
            if 'calculated_safety_stock' in st.session_state:
                del st.session_state.calculated_safety_stock
            if 'dialog_initialized' in st.session_state:
                del st.session_state.dialog_initialized
            st.rerun()


@st.dialog("Review Safety Stock", width="large")
def review_dialog(safety_stock_id):
    """Review and adjust safety stock"""
    
    current_data = get_safety_stock_by_id(safety_stock_id)
    if not current_data:
        st.error("Record not found")
        return
    
    st.markdown("### Safety Stock Review")
    
    # Display current information
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Product", current_data.get('pt_code', 'N/A'))
    with col2:
        st.metric("Current Safety Stock", f"{current_data['safety_stock_qty']:.0f}")
    with col3:
        st.metric("Method", current_data.get('calculation_method', 'FIXED'))
    
    st.divider()
    
    # Performance metrics
    st.subheader("Performance Metrics")
    col1, col2 = st.columns(2)
    
    with col1:
        stockout_incidents = st.number_input("Stockout Incidents", min_value=0, value=0)
        service_level_achieved = st.number_input(
            "Service Level Achieved (%)", 
            min_value=0.0, 
            max_value=100.0, 
            value=95.0
        )
        avg_daily_demand = st.number_input(
            "Actual Avg Daily Demand",
            min_value=0.0,
            value=float(current_data.get('avg_daily_demand', 0))
        )
    
    with col2:
        excess_stock_days = st.number_input("Days with Excess Stock", min_value=0, value=0)
        inventory_turns = st.number_input("Inventory Turns", min_value=0.0, value=0.0)
        holding_cost_usd = st.number_input("Holding Cost (USD)", min_value=0.0, value=0.0)
    
    st.divider()
    
    # Adjustment section
    st.subheader("Adjustment")
    
    col1, col2 = st.columns(2)
    with col1:
        new_safety_stock_qty = st.number_input(
            "New Safety Stock Quantity",
            min_value=0.0,
            value=float(current_data['safety_stock_qty'])
        )
        
        action_taken = st.selectbox(
            "Action",
            options=['NO_CHANGE', 'INCREASED', 'DECREASED', 'METHOD_CHANGED']
        )
    
    with col2:
        action_reason = st.text_input("Reason for Change")
        next_review_date = st.date_input(
            "Next Review Date",
            value=datetime.now().date() + timedelta(days=30)
        )
    
    review_notes = st.text_area("Review Notes")
    
    # Submit button
    if st.button("💾 Submit Review", type="primary"):
        review_data = {
            'review_date': datetime.now().date(),
            'review_type': 'PERIODIC',
            'old_safety_stock_qty': current_data['safety_stock_qty'],
            'new_safety_stock_qty': new_safety_stock_qty,
            'avg_daily_demand': avg_daily_demand,
            'stockout_incidents': stockout_incidents,
            'service_level_achieved': service_level_achieved,
            'excess_stock_days': excess_stock_days,
            'inventory_turns': inventory_turns,
            'holding_cost_usd': holding_cost_usd,
            'action_taken': action_taken,
            'action_reason': action_reason,
            'review_notes': review_notes,
            'next_review_date': next_review_date
        }
        
        success, message = create_safety_stock_review(
            safety_stock_id,
            review_data,
            st.session_state.username
        )
        
        if success:
            if new_safety_stock_qty != current_data['safety_stock_qty']:
                update_data = {'safety_stock_qty': new_safety_stock_qty}
                update_safety_stock(safety_stock_id, update_data, st.session_state.username)
            
            st.success("✅ Review submitted successfully!")
            st.rerun()
        else:
            st.error(f"Error: {message}")


@st.dialog("Bulk Upload", width="large")
def bulk_upload_dialog():
    """Bulk upload safety stock data"""
    
    st.markdown("### Bulk Upload Safety Stock")
    
    # Download template
    if st.button("📥 Download Template"):
        template = create_upload_template(include_sample_data=True)
        st.download_button(
            label="💾 Save Template",
            data=template,
            file_name=f"safety_stock_template_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    # Upload file
    uploaded_file = st.file_uploader("Choose Excel file", type=['xlsx', 'xls'])
    
    if uploaded_file:
        try:
            df = pd.read_excel(uploaded_file)
            
            # Remove instruction row if present
            if df.iloc[0].astype(str).str.contains('Required|Optional').any():
                df = df.iloc[1:].reset_index(drop=True)
            
            st.info(f"Found {len(df)} rows to import")
            
            # Preview
            st.subheader("Preview")
            st.dataframe(df.head(10), use_container_width=True)
            
            # Validate
            is_valid, validated_df, errors = validate_bulk_data(df)
            
            if not is_valid:
                st.error("Validation failed:")
                for error in errors[:10]:
                    st.write(f"• {error}")
            else:
                st.success("✅ Validation passed")
                
                if st.button("📤 Import Data", type="primary"):
                    data_list = validated_df.to_dict('records')
                    success, message, results = bulk_create_safety_stock(
                        data_list,
                        st.session_state.username
                    )
                    
                    if success:
                        st.success(f"✅ {message}")
                        if results['failed'] > 0:
                            st.warning(f"Failed to create {results['failed']} records")
                            with st.expander("Error details"):
                                for error in results['errors']:
                                    st.write(f"• {error}")
                        st.rerun()
                    else:
                        st.error(f"Import failed: {message}")
        
        except Exception as e:
            st.error(f"Error reading file: {e}")


# ==================== Main Page Layout ====================

def main():
    # Header
    st.title("📦 Safety Stock Management")
    
    # Top metrics row
    stats = get_quick_stats()
    if stats:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Active Rules", stats.total_items)
        with col2:
            st.metric("Customer Rules", stats.customer_rules)
        with col3:
            st.metric("Needs Review", stats.needs_review)
        with col4:
            st.metric("Unique Products", stats.unique_products)
    
    st.divider()
    
    # Filters section
    with st.container():
        st.subheader("Filters")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            entities = load_entities()
            entity_options = ['All Entities'] + (entities['company_code'] + ' - ' + entities['english_name']).tolist()
            
            selected_entity = st.selectbox(
                "Entity",
                options=range(len(entity_options)),
                format_func=lambda x: entity_options[x]
            )
            
            st.session_state.ss_filters['entity_id'] = entities.iloc[selected_entity - 1]['id'] if selected_entity > 0 else None
        
        with col2:
            customers = load_customers()
            customer_options = ['All Customers', 'General Rules Only'] + \
                             (customers['company_code'] + ' - ' + customers['english_name']).tolist()
            
            selected_customer = st.selectbox(
                "Customer",
                options=range(len(customer_options)),
                format_func=lambda x: customer_options[x]
            )
            
            if selected_customer == 0:
                st.session_state.ss_filters['customer_id'] = None
            elif selected_customer == 1:
                st.session_state.ss_filters['customer_id'] = 'general'
            else:
                st.session_state.ss_filters['customer_id'] = customers.iloc[selected_customer - 2]['id']
        
        with col3:
            product_search = st.text_input(
                "Product Search",
                placeholder="PT Code or Name",
                value=st.session_state.ss_filters['product_search']
            )
            st.session_state.ss_filters['product_search'] = product_search
        
        with col4:
            status = st.selectbox(
                "Status",
                options=['active', 'all', 'expired', 'future'],
                format_func=lambda x: x.title()
            )
            st.session_state.ss_filters['status'] = status
    
    # Action buttons
    st.divider()
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        if st.button("➕ Add Safety Stock", type="primary"):
            # Clean up any previous dialog state
            if 'dialog_initialized' in st.session_state:
                del st.session_state.dialog_initialized
            if 'calc_method_select' in st.session_state:
                del st.session_state.calc_method_select
            safety_stock_form('add')
    
    with col2:
        if st.button("📤 Bulk Upload"):
            bulk_upload_dialog()
    
    with col3:
        if st.button("📥 Export to Excel"):
            df = get_safety_stock_levels(
                entity_id=st.session_state.ss_filters['entity_id'],
                customer_id=None if st.session_state.ss_filters['customer_id'] == 'general' else st.session_state.ss_filters['customer_id'],
                product_search=st.session_state.ss_filters['product_search'],
                status=st.session_state.ss_filters['status']
            )
            
            if not df.empty:
                excel_file = export_to_excel(df)
                st.download_button(
                    label="💾 Download Excel",
                    data=excel_file,
                    file_name=f"safety_stock_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.warning("No data to export")
    
    with col4:
        if st.button("📊 Review Report"):
            report = generate_review_report()
            st.download_button(
                label="💾 Download Report",
                data=report,
                file_name=f"safety_stock_review_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    
    with col5:
        if st.button("🔄 Refresh"):
            st.cache_data.clear()
            st.rerun()
    
    # Main data table
    st.divider()
    
    # Load data
    df = get_safety_stock_levels(
        entity_id=st.session_state.ss_filters['entity_id'],
        customer_id=None if st.session_state.ss_filters['customer_id'] == 'general' else st.session_state.ss_filters['customer_id'],
        product_search=st.session_state.ss_filters['product_search'],
        status=st.session_state.ss_filters['status']
    )
    
    if df.empty:
        st.info("No safety stock records found. Click '➕ Add Safety Stock' to create one.")
    else:
        # Display columns
        display_columns = [
            'pt_code', 'product_name', 'entity_code', 'customer_code',
            'safety_stock_qty', 'min_stock_qty', 'max_stock_qty',
            'reorder_point', 'calculation_method', 'rule_type', 
            'status', 'effective_from', 'priority_level'
        ]
        
        # Format display
        display_df = df[display_columns].copy()
        display_df['customer_code'] = display_df['customer_code'].fillna('All')
        
        # Show data table with selection
        st.subheader(f"Safety Stock Rules ({len(df)} records)")
        
        selected = st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun"
        )
        
        # Handle row selection
        if selected and selected.selection.rows:
            selected_idx = selected.selection.rows[0]
            selected_record = df.iloc[selected_idx]
            
            st.divider()
            st.subheader("Selected Record Actions")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if st.button("✏️ Edit"):
                    # Clean up any previous dialog state
                    if 'dialog_initialized' in st.session_state:
                        del st.session_state.dialog_initialized
                    if 'calc_method_select' in st.session_state:
                        del st.session_state.calc_method_select
                    safety_stock_form('edit', selected_record['id'])
            
            with col2:
                if st.button("📝 Review"):
                    review_dialog(selected_record['id'])
            
            with col3:
                if st.button("📜 History"):
                    history_df = get_review_history(selected_record['id'])
                    if not history_df.empty:
                        st.dataframe(history_df, use_container_width=True)
                    else:
                        st.info("No review history found")
            
            with col4:
                if st.button("🗑️ Delete", type="secondary"):
                    if st.checkbox("Confirm delete?"):
                        success, message = delete_safety_stock(
                            selected_record['id'],
                            st.session_state.username
                        )
                        if success:
                            st.success("✅ Deleted successfully")
                            st.rerun()
                        else:
                            st.error(f"Error: {message}")


# Run main function
if __name__ == "__main__":
    main()