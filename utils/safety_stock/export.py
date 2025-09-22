# utils/safety_stock/export.py
"""
Export and reporting functions for Safety Stock Management
Handles Excel exports, template generation, and report creation
"""

import pandas as pd
import io
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Tuple
import logging
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.worksheet.table import Table, TableStyleInfo
from sqlalchemy import text
from ..db import get_db_engine

logger = logging.getLogger(__name__)

# ==================== Excel Export Functions ====================

def export_to_excel(
    df: pd.DataFrame,
    include_parameters: bool = True,
    include_metadata: bool = True
) -> io.BytesIO:
    """
    Export safety stock data to formatted Excel file
    
    Args:
        df: DataFrame with safety stock data
        include_parameters: Include calculation parameters sheet
        include_metadata: Include metadata (created by, dates, etc.)
    
    Returns:
        BytesIO object containing Excel file
    """
    output = io.BytesIO()
    
    try:
        # Create workbook and sheets
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Main data sheet
            main_columns = [
                'pt_code', 'product_name', 'brand_name',
                'entity_code', 'entity_name',
                'customer_code', 'customer_name',
                'safety_stock_qty', 'min_stock_qty', 'max_stock_qty',
                'reorder_point', 'reorder_qty',
                'calculation_method', 'rule_type', 'status',
                'effective_from', 'effective_to',
                'priority_level', 'business_notes'
            ]
            
            if include_metadata:
                main_columns.extend(['created_by', 'created_date', 'updated_by', 'updated_date'])
            
            # Select only columns that exist
            export_columns = [col for col in main_columns if col in df.columns]
            main_df = df[export_columns].copy()
            
            # Format dates
            date_columns = ['effective_from', 'effective_to', 'created_date', 'updated_date']
            for col in date_columns:
                if col in main_df.columns:
                    main_df[col] = pd.to_datetime(main_df[col]).dt.strftime('%Y-%m-%d')
            
            # Write main sheet
            main_df.to_excel(writer, sheet_name='Safety Stock Levels', index=False)
            
            # Add parameters sheet if requested
            if include_parameters:
                param_columns = [
                    'pt_code', 'product_name',
                    'entity_code', 'customer_code',
                    'calculation_method',
                    'lead_time_days', 'safety_days',
                    'service_level_percent', 'z_score',
                    'avg_daily_demand', 'demand_std_deviation',
                    'last_calculated_date'
                ]
                param_columns = [col for col in param_columns if col in df.columns]
                
                if param_columns:
                    param_df = df[param_columns].copy()
                    param_df = param_df[param_df['calculation_method'].notna()]
                    
                    if not param_df.empty:
                        param_df.to_excel(writer, sheet_name='Calculation Parameters', index=False)
            
            # Format the Excel file
            workbook = writer.book
            
            # Format main sheet
            format_excel_sheet(workbook['Safety Stock Levels'])
            
            # Format parameters sheet if exists
            if 'Calculation Parameters' in workbook.sheetnames:
                format_excel_sheet(workbook['Calculation Parameters'])
        
        output.seek(0)
        logger.info(f"Exported {len(df)} safety stock records to Excel")
        return output
        
    except Exception as e:
        logger.error(f"Error exporting to Excel: {e}")
        raise


def format_excel_sheet(worksheet):
    """
    Apply formatting to Excel worksheet
    
    Args:
        worksheet: openpyxl worksheet object
    """
    # Header formatting
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    header_alignment = Alignment(horizontal="center", vertical="center")
    
    # Apply header formatting
    for cell in worksheet[1]:
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_alignment
    
    # Auto-adjust column widths
    for column in worksheet.columns:
        max_length = 0
        column_letter = column[0].column_letter
        
        for cell in column:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except:
                pass
        
        adjusted_width = min(max_length + 2, 50)
        worksheet.column_dimensions[column_letter].width = adjusted_width
    
    # Add borders
    border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    for row in worksheet.iter_rows(min_row=1, max_row=worksheet.max_row, 
                                  min_col=1, max_col=worksheet.max_column):
        for cell in row:
            cell.border = border
    
    # Freeze header row
    worksheet.freeze_panes = 'A2'


# ==================== Template Generation ====================

def create_upload_template(
    include_sample_data: bool = False,
    entity_id: Optional[int] = None
) -> io.BytesIO:
    """
    Create Excel template for bulk upload
    
    Args:
        include_sample_data: Include sample rows
        entity_id: Pre-fill entity ID if provided
    
    Returns:
        BytesIO object containing template Excel file
    """
    output = io.BytesIO()
    
    try:
        # Define template structure
        template_data = {
            'product_id': ['Required: Product ID', '', ''],
            'pt_code': ['Optional: Product PT Code (for reference)', '', ''],
            'product_name': ['Optional: Product Name (for reference)', '', ''],
            'entity_id': ['Required: Entity ID', entity_id if entity_id else '', ''],
            'entity_code': ['Optional: Entity Code (for reference)', '', ''],
            'customer_id': ['Optional: Customer ID (leave blank for general rule)', '', ''],
            'customer_code': ['Optional: Customer Code (for reference)', '', ''],
            'safety_stock_qty': ['Required: Safety Stock Quantity', '', ''],
            'min_stock_qty': ['Optional: Minimum Stock', '', ''],
            'max_stock_qty': ['Optional: Maximum Stock', '', ''],
            'reorder_point': ['Optional: Reorder Point', '', ''],
            'reorder_qty': ['Optional: Reorder Quantity', '', ''],
            'calculation_method': ['Optional: FIXED|DAYS_OF_SUPPLY|DEMAND_PERCENTAGE|LEAD_TIME_BASED|MIN_MAX|STATISTICAL', '', ''],
            'lead_time_days': ['Optional: Lead Time (days)', '', ''],
            'safety_days': ['Optional: Safety Days', '', ''],
            'service_level_percent': ['Optional: Service Level % (e.g., 95)', '', ''],
            'avg_daily_demand': ['Optional: Average Daily Demand', '', ''],
            'effective_from': ['Required: Start Date (YYYY-MM-DD)', datetime.now().strftime('%Y-%m-%d'), ''],
            'effective_to': ['Optional: End Date (YYYY-MM-DD)', '', ''],
            'priority_level': ['Optional: Priority (default 100, lower = higher priority)', '100', ''],
            'business_notes': ['Optional: Business Notes', '', '']
        }
        
        # Create DataFrame
        df = pd.DataFrame(template_data)
        
        # Add sample data if requested
        if include_sample_data:
            sample_data = {
                'product_id': [101, 102, 103],
                'pt_code': ['PT001', 'PT002', 'PT003'],
                'product_name': ['Product A', 'Product B', 'Product C'],
                'entity_id': [1, 1, 1],
                'entity_code': ['ENT01', 'ENT01', 'ENT01'],
                'customer_id': ['', 5, ''],
                'customer_code': ['', 'CUST01', ''],
                'safety_stock_qty': [100, 50, 200],
                'min_stock_qty': [50, 25, 100],
                'max_stock_qty': [500, 250, 1000],
                'reorder_point': [150, 75, 300],
                'reorder_qty': [200, 100, 400],
                'calculation_method': ['DAYS_OF_SUPPLY', 'LEAD_TIME_BASED', 'FIXED'],
                'lead_time_days': [7, 14, ''],
                'safety_days': [14, '', ''],
                'service_level_percent': ['', 95, ''],
                'avg_daily_demand': [10, 5, ''],
                'effective_from': ['2025-01-01', '2025-01-01', '2025-01-01'],
                'effective_to': ['', '2025-12-31', ''],
                'priority_level': [100, 50, 100],
                'business_notes': ['Standard rule', 'Customer-specific', 'Manual override']
            }
            sample_df = pd.DataFrame(sample_data)
            df = pd.concat([df, sample_df], ignore_index=True)
        
        # Create workbook
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Write data sheet
            df.to_excel(writer, sheet_name='Safety Stock Import', index=False)
            
            # Add instructions sheet
            instructions = pd.DataFrame({
                'Instructions': [
                    'Safety Stock Bulk Upload Template',
                    '',
                    'Required Fields:',
                    '- product_id: Numeric ID of the product',
                    '- entity_id: Numeric ID of the entity/warehouse',
                    '- safety_stock_qty: Safety stock quantity (must be >= 0)',
                    '- effective_from: Start date in YYYY-MM-DD format',
                    '',
                    'Optional Fields:',
                    '- customer_id: Leave blank for general rules, provide ID for customer-specific',
                    '- min/max_stock_qty: Minimum and maximum stock levels',
                    '- reorder_point: When to trigger reorder',
                    '- reorder_qty: How much to order',
                    '',
                    'Calculation Methods:',
                    '- FIXED: Manual safety stock value',
                    '- DAYS_OF_SUPPLY: Based on days of supply needed',
                    '- DEMAND_PERCENTAGE: Percentage of period demand',
                    '- LEAD_TIME_BASED: Statistical calculation with lead time',
                    '- MIN_MAX: Min-Max inventory system',
                    '- STATISTICAL: Advanced statistical model',
                    '',
                    'Notes:',
                    '- Dates should be in YYYY-MM-DD format',
                    '- Priority: Lower number = higher priority (default 100)',
                    '- Customer-specific rules override general rules',
                    '- PT codes and names are for reference only',
                    '',
                    'Delete the first row (field descriptions) before uploading!'
                ]
            })
            instructions.to_excel(writer, sheet_name='Instructions', index=False)
            
            # Format sheets
            workbook = writer.book
            
            # Format data sheet
            data_sheet = workbook['Safety Stock Import']
            format_excel_sheet(data_sheet)
            
            # Highlight first row as instructions
            for cell in data_sheet[1]:
                cell.font = Font(italic=True, color="FF0000")
            
            # Format instructions sheet
            inst_sheet = workbook['Instructions']
            inst_sheet.column_dimensions['A'].width = 100
            
            # Style instructions
            title_font = Font(bold=True, size=14)
            header_font = Font(bold=True)
            
            inst_sheet['A1'].font = title_font
            for row in [3, 9, 15, 22]:
                if row <= inst_sheet.max_row:
                    inst_sheet[f'A{row}'].font = header_font
        
        output.seek(0)
        logger.info("Created upload template")
        return output
        
    except Exception as e:
        logger.error(f"Error creating template: {e}")
        raise


# ==================== Review Report Generation ====================

def generate_review_report(
    review_period_days: int = 30,
    entity_id: Optional[int] = None
) -> io.BytesIO:
    """
    Generate comprehensive review report with performance metrics
    
    Args:
        review_period_days: Number of days to analyze
        entity_id: Filter by entity
    
    Returns:
        BytesIO object containing report Excel file
    """
    output = io.BytesIO()
    
    try:
        engine = get_db_engine()
        
        # Get items pending review
        pending_query = text("""
        SELECT 
            s.id,
            p.pt_code,
            p.name as product_name,
            e.company_code as entity_code,
            c.company_code as customer_code,
            s.safety_stock_qty,
            ssp.calculation_method,
            ssp.last_calculated_date,
            DATEDIFF(CURRENT_DATE(), ssp.last_calculated_date) as days_since_calc,
            ssr.review_date as last_review_date,
            ssr.next_review_date
        FROM safety_stock_levels s
        JOIN products p ON s.product_id = p.id
        JOIN companies e ON s.entity_id = e.id
        LEFT JOIN companies c ON s.customer_id = c.id
        LEFT JOIN safety_stock_parameters ssp ON s.id = ssp.safety_stock_level_id
        LEFT JOIN (
            SELECT safety_stock_level_id, MAX(review_date) as review_date, 
                   MAX(next_review_date) as next_review_date
            FROM safety_stock_reviews
            GROUP BY safety_stock_level_id
        ) ssr ON s.id = ssr.safety_stock_level_id
        WHERE s.delete_flag = 0 
        AND s.is_active = 1
        """ + ("""
        AND s.entity_id = :entity_id
        """ if entity_id else "") + """
        AND (
            ssr.next_review_date IS NULL 
            OR ssr.next_review_date <= CURRENT_DATE()
            OR DATEDIFF(CURRENT_DATE(), ssp.last_calculated_date) > :review_period
        )
        ORDER BY days_since_calc DESC
        """)
        
        params = {'review_period': review_period_days}
        if entity_id:
            params['entity_id'] = entity_id
        
        with engine.connect() as conn:
            pending_df = pd.read_sql(pending_query, conn, params=params)
        
        # Get recent reviews
        recent_reviews_query = text("""
        SELECT 
            ssr.review_date,
            p.pt_code,
            p.name as product_name,
            e.company_code as entity_code,
            ssr.old_safety_stock_qty,
            ssr.new_safety_stock_qty,
            ssr.change_percentage,
            ssr.action_taken,
            ssr.service_level_achieved,
            ssr.stockout_incidents,
            ssr.reviewed_by
        FROM safety_stock_reviews ssr
        JOIN safety_stock_levels s ON ssr.safety_stock_level_id = s.id
        JOIN products p ON s.product_id = p.id
        JOIN companies e ON s.entity_id = e.id
        WHERE ssr.review_date >= DATE_SUB(CURRENT_DATE(), INTERVAL :days DAY)
        """ + ("""
        AND s.entity_id = :entity_id
        """ if entity_id else "") + """
        ORDER BY ssr.review_date DESC
        """)
        
        params = {'days': review_period_days}
        if entity_id:
            params['entity_id'] = entity_id
        
        with engine.connect() as conn:
            reviews_df = pd.read_sql(recent_reviews_query, conn, params=params)
        
        # Performance summary
        summary_query = text("""
        SELECT 
            COUNT(DISTINCT s.id) as total_items,
            COUNT(DISTINCT CASE WHEN ssp.calculation_method = 'FIXED' THEN s.id END) as fixed_method_count,
            COUNT(DISTINCT CASE WHEN ssp.calculation_method != 'FIXED' THEN s.id END) as calculated_count,
            AVG(CASE WHEN ssr.service_level_achieved IS NOT NULL THEN ssr.service_level_achieved END) as avg_service_level,
            SUM(COALESCE(ssr.stockout_incidents, 0)) as total_stockouts,
            COUNT(DISTINCT CASE WHEN ssr.action_taken = 'INCREASED' THEN ssr.id END) as increases,
            COUNT(DISTINCT CASE WHEN ssr.action_taken = 'DECREASED' THEN ssr.id END) as decreases,
            COUNT(DISTINCT CASE WHEN ssr.action_taken = 'NO_CHANGE' THEN ssr.id END) as no_changes
        FROM safety_stock_levels s
        LEFT JOIN safety_stock_parameters ssp ON s.id = ssp.safety_stock_level_id
        LEFT JOIN safety_stock_reviews ssr ON s.id = ssr.safety_stock_level_id
            AND ssr.review_date >= DATE_SUB(CURRENT_DATE(), INTERVAL :days DAY)
        WHERE s.delete_flag = 0 AND s.is_active = 1
        """ + ("""
        AND s.entity_id = :entity_id
        """ if entity_id else ""))
        
        params = {'days': review_period_days}
        if entity_id:
            params['entity_id'] = entity_id
        
        with engine.connect() as conn:
            summary = conn.execute(summary_query, params).fetchone()
        
        # Create summary data
        summary_data = {
            'Metric': [
                'Total Active Items',
                'Fixed Method Count',
                'Calculated Method Count',
                'Average Service Level',
                'Total Stockout Incidents',
                'Recent Increases',
                'Recent Decreases',
                'No Changes'
            ],
            'Value': [
                summary.total_items,
                summary.fixed_method_count,
                summary.calculated_count,
                f"{summary.avg_service_level:.1f}%" if summary.avg_service_level else "N/A",
                summary.total_stockouts,
                summary.increases,
                summary.decreases,
                summary.no_changes
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        
        # Write to Excel
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Summary sheet
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Pending reviews sheet
            if not pending_df.empty:
                pending_df.to_excel(writer, sheet_name='Pending Reviews', index=False)
            
            # Recent reviews sheet
            if not reviews_df.empty:
                reviews_df.to_excel(writer, sheet_name='Recent Reviews', index=False)
            
            # Format sheets
            workbook = writer.book
            for sheet_name in workbook.sheetnames:
                format_excel_sheet(workbook[sheet_name])
        
        output.seek(0)
        logger.info(f"Generated review report for {review_period_days} days")
        return output
        
    except Exception as e:
        logger.error(f"Error generating review report: {e}")
        raise


# ==================== Quick Export Functions ====================

def export_current_view(df: pd.DataFrame) -> io.BytesIO:
    """
    Quick export of current filtered view
    
    Args:
        df: Current filtered DataFrame
    
    Returns:
        BytesIO object containing Excel file
    """
    output = io.BytesIO()
    
    try:
        # Timestamp for export
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Select relevant columns for quick export
        export_columns = [
            'pt_code', 'product_name', 'entity_code', 'customer_code',
            'safety_stock_qty', 'min_stock_qty', 'max_stock_qty',
            'reorder_point', 'reorder_qty', 'calculation_method',
            'status', 'effective_from', 'effective_to'
        ]
        
        # Use available columns
        available_columns = [col for col in export_columns if col in df.columns]
        export_df = df[available_columns].copy()
        
        # Write to Excel
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            export_df.to_excel(
                writer, 
                sheet_name=f'Safety Stock {timestamp}',
                index=False
            )
            
            # Basic formatting
            worksheet = writer.sheets[f'Safety Stock {timestamp}']
            format_excel_sheet(worksheet)
        
        output.seek(0)
        return output
        
    except Exception as e:
        logger.error(f"Error in quick export: {e}")
        raise


def generate_stockout_report(days_back: int = 30) -> pd.DataFrame:
    """
    Generate report of items with stockouts
    
    Args:
        days_back: Number of days to look back
    
    Returns:
        DataFrame with stockout analysis
    """
    try:
        engine = get_db_engine()
        
        # Query based on inventory_histories table
        # Note: This is a simplified query - you may need to add logic to detect actual stockouts
        query = text("""
        SELECT 
            p.pt_code,
            p.name as product_name,
            wh.name as warehouse_name,
            w_owner.company_code as entity_code,
            s.safety_stock_qty,
            COUNT(DISTINCT DATE(ih.created_date)) as demand_days,
            SUM(ih.quantity) as total_demand_qty,
            AVG(ih.quantity) as avg_demand_qty
        FROM safety_stock_levels s
        JOIN products p ON s.product_id = p.id
        JOIN warehouses wh ON wh.company_id = s.entity_id
        JOIN companies w_owner ON wh.company_id = w_owner.id
        LEFT JOIN inventory_histories ih ON 
            ih.product_id = s.product_id 
            AND ih.warehouse_id = wh.id
            AND ih.type IN ('stockOutDelivery', 'stockOutInternal')
            AND ih.created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL :days DAY)
            AND ih.delete_flag = 0
        WHERE s.delete_flag = 0 
        AND s.is_active = 1
        GROUP BY s.id, p.pt_code, p.name, wh.name, w_owner.company_code, s.safety_stock_qty
        HAVING demand_days > 0
        ORDER BY total_demand_qty DESC
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={'days': days_back})
        
        return df
        
    except Exception as e:
        logger.error(f"Error generating stockout report: {e}")
        return pd.DataFrame()