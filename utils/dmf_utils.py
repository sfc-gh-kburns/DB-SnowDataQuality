"""
Data Metric Functions (DMF) utilities for Snowflake data quality monitoring.
Handles DMF configuration, execution, and monitoring.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
import re
import time

from .database import quote_identifier, get_fully_qualified_name, execute_comment_sql
from .data_fetchers import get_databases, get_schemas, get_tables_and_views, get_columns

# ========================================================================================
# SYSTEM DMF DEFINITIONS AND DATA TYPE MAPPINGS
# ========================================================================================

# Comprehensive system DMFs with their supported data types
SYSTEM_DMFS = {
    # Table-level DMFs
    'ROW_COUNT': {
        'label': 'Row Count',
        'description': 'Total number of rows in the table',
        'level': 'table',
        'data_types': [],  # Table-level, no column data types
        'help': 'Monitors the total row count in the table'
    },
    'FRESHNESS': {
        'label': 'Data Freshness',
        'description': 'Data freshness based on timestamp column',
        'level': 'column',
        'data_types': ['DATE', 'TIME', 'TIMESTAMP', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ'],
        'help': 'Measures how recent the data is based on a timestamp column'
    },
    
    # Column-level DMFs - Basic Quality
    'NULL_COUNT': {
        'label': 'Null Count',
        'description': 'Count of NULL values in a column',
        'level': 'column',
        'data_types': ['ALL'],  # Works with all data types
        'help': 'Counts the number of NULL values in the column'
    },
    'DUPLICATE_COUNT': {
        'label': 'Duplicate Count',
        'description': 'Count of duplicate values in a column',
        'level': 'column',
        'data_types': ['ALL'],  # Works with all data types
        'help': 'Counts the number of duplicate values in the column'
    },
    'UNIQUE_COUNT': {
        'label': 'Unique Count',
        'description': 'Count of unique, non-NULL values in a column',
        'level': 'column',
        'data_types': ['ALL'],  # Works with all data types
        'help': 'Counts the number of unique, non-NULL values in the column'
    },
    
    # Note: Only including verified working system DMFs
    # Additional DMFs can be added here as they are tested and confirmed
}

def get_compatible_dmfs_for_data_type(data_type: str) -> List[str]:
    """Get list of DMF keys that are compatible with the given data type."""
    compatible_dmfs = []
    data_type_upper = data_type.upper()
    
    for dmf_key, dmf_info in SYSTEM_DMFS.items():
        if dmf_info['level'] == 'table':
            continue  # Skip table-level DMFs
            
        # Check if DMF supports all data types
        if 'ALL' in dmf_info['data_types']:
            compatible_dmfs.append(dmf_key)
            continue
            
        # Check if data type matches any of the supported types
        for supported_type in dmf_info['data_types']:
            if supported_type in data_type_upper:
                compatible_dmfs.append(dmf_key)
                break
    
    return compatible_dmfs

def configure_monitoring_schedule(key_prefix: str) -> dict:
    """Configure monitoring schedule with modern UI."""
    
    schedule_type = st.radio(
        "Choose schedule type:",
        options=["⏱️ Periodic", "📅 Daily", "🔄 On Changes"],
        key=f"{key_prefix}_schedule_type",
        horizontal=True,
        help="How often should data quality checks run?"
    )
    
    if schedule_type == "⏱️ Periodic":
        col1, col2 = st.columns(2)
        with col1:
            interval_type = st.selectbox(
                "Interval",
                options=["Minutes", "Hours"],
                key=f"{key_prefix}_interval_type"
            )
        with col2:
            if interval_type == "Minutes":
                interval = st.selectbox(
                    "Every X minutes",
                    options=[5, 15, 30, 60],
                    index=2,
                    key=f"{key_prefix}_minutes"
                )
                return {
                    'schedule_expression': f'{interval} MINUTE',
                    'description': f'Every {interval} minutes'
                }
            else:
                interval = st.selectbox(
                    "Every X hours",
                    options=[1, 2, 4, 6, 8, 12, 24],
                    index=2,
                    key=f"{key_prefix}_hours"
                )
                return {
                    'schedule_expression': f'USING CRON 0 */{interval} * * * UTC',
                    'description': f'Every {interval} hours'
                }
    
    elif schedule_type == "📅 Daily":
        col1, col2 = st.columns(2)
        with col1:
            hour = st.selectbox(
                "Hour (24h format)",
                options=list(range(24)),
                index=6,
                key=f"{key_prefix}_hour"
            )
        with col2:
            minute = st.selectbox(
                "Minute",
                options=[0, 15, 30, 45],
                key=f"{key_prefix}_minute"
            )
        return {
            'schedule_expression': f'USING CRON {minute} {hour} * * * UTC',
            'description': f'Daily at {hour:02d}:{minute:02d} UTC'
        }
    
    else:  # On Changes
        st.info("💡 Triggers when table data changes (INSERT, UPDATE, DELETE)")
        return {
            'schedule_expression': 'TRIGGER_ON_CHANGES',
            'description': 'When data changes'
        }

def configure_table_dmfs(conn, database: str, schema: str, table_name: str, key_prefix: str) -> dict:
    """Configure DMFs for a specific table with smart data type filtering."""
    
    # Get table columns
    refresh_key = st.session_state.get('last_refresh', '')
    columns_df = get_columns(conn, database, schema, table_name, refresh_key)
    
    if columns_df.empty:
        st.warning(f"Could not retrieve columns for {table_name}")
        return None
    
    st.markdown(f"**Table Info:** {len(columns_df)} columns")
    
    config = {
        'table_dmfs': {},
        'column_dmfs': {}
    }
    
    # Table-level DMFs
    st.markdown("##### 🏢 Table-Level Metrics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        config['table_dmfs']['ROW_COUNT'] = st.checkbox(
            f"✅ {SYSTEM_DMFS['ROW_COUNT']['label']}",
            help=SYSTEM_DMFS['ROW_COUNT']['help'],
            key=f"{key_prefix}_row_count"
        )
    
    with col2:
        # Check for timestamp columns
        timestamp_cols = [
            col for col in columns_df['COLUMN_NAME'] 
            if any(word in col.upper() for word in ['DATE', 'TIME', 'TIMESTAMP', 'CREATED', 'UPDATED'])
        ]
        
        if timestamp_cols:
            config['table_dmfs']['FRESHNESS'] = st.checkbox(
                f"✅ {SYSTEM_DMFS['FRESHNESS']['label']}",
                help=SYSTEM_DMFS['FRESHNESS']['help'],
                key=f"{key_prefix}_freshness"
            )
            
            if config['table_dmfs']['FRESHNESS']:
                config['freshness_column'] = st.selectbox(
                    "Timestamp column",
                    options=timestamp_cols,
                    key=f"{key_prefix}_freshness_col"
                )
        else:
            st.info("💡 No timestamp columns found for freshness monitoring")
    
    # Column-level DMFs
    st.markdown("##### 📊 Column-Level Metrics")
    
    # Group columns by data type for better organization
    column_groups = {}
    for _, col_row in columns_df.iterrows():
        col_name = col_row['COLUMN_NAME']
        data_type = col_row['DATA_TYPE']
        
        if data_type not in column_groups:
            column_groups[data_type] = []
        column_groups[data_type].append(col_name)
    
    # Individual column DMF selection - flexible approach
    if column_groups:
        st.markdown("**Configure metrics for individual columns:**")
        
        # Create tabs for each data type for better organization
        data_types = list(column_groups.keys())
        
        if len(data_types) == 1:
            # Single data type - show columns directly
            data_type = data_types[0]
            columns = column_groups[data_type]
            compatible_dmfs = get_compatible_dmfs_for_data_type(data_type)
            
            if compatible_dmfs:
                st.markdown(f"**{data_type}** columns - Select metrics for each column individually:")
                
                # Show each column with its own DMF selection
                for col_name in columns:
                    st.markdown(f"##### 📊 {col_name}")
                    
                    selected_dmfs = []
                    dmf_cols = st.columns(min(len(compatible_dmfs), 4))
                    
                    for i, dmf_key in enumerate(compatible_dmfs):
                        dmf_info = SYSTEM_DMFS[dmf_key]
                        col_idx = i % len(dmf_cols)
                        
                        with dmf_cols[col_idx]:
                            if st.checkbox(
                                dmf_info['label'], 
                                help=dmf_info['help'],
                                key=f"{key_prefix}_{col_name}_{dmf_key}"
                            ):
                                selected_dmfs.append(dmf_key)
                    
                    # Store individual column selections
                    if selected_dmfs:
                        config['column_dmfs'][col_name] = selected_dmfs
                        st.success(f"✅ {col_name}: {', '.join([SYSTEM_DMFS[dmf]['label'] for dmf in selected_dmfs])}")
                    else:
                        st.caption(f"No metrics selected for {col_name}")
        
        else:
            # Multiple data types - use tabs with individual column selection
            tab_names = [f"{dt} ({len(column_groups[dt])})" for dt in data_types]
            tabs = st.tabs(tab_names)
            
            for i, data_type in enumerate(data_types):
                with tabs[i]:
                    columns = column_groups[data_type]
                    compatible_dmfs = get_compatible_dmfs_for_data_type(data_type)
                    
                    if not compatible_dmfs:
                        st.info(f"No compatible metrics available for {data_type} columns")
                        continue
                    
                    st.caption(f"**Available metrics for {data_type}:** {', '.join([SYSTEM_DMFS[dmf]['label'] for dmf in compatible_dmfs])}")
                    
                    st.markdown("**Configure each column individually:**")
                    
                    # Show each column with its own DMF selection
                    for col_name in columns:
                        st.markdown(f"##### 📊 {col_name}")
                        
                        selected_dmfs = []
                        dmf_cols = st.columns(min(len(compatible_dmfs), 4))
                        
                        for j, dmf_key in enumerate(compatible_dmfs):
                            dmf_info = SYSTEM_DMFS[dmf_key]
                            col_idx = j % len(dmf_cols)
                            
                            with dmf_cols[col_idx]:
                                # Check if this DMF was already selected (from bulk action or previous selection)
                                default_value = col_name in config['column_dmfs'] and dmf_key in config['column_dmfs'][col_name]
                            
                            if st.checkbox(
                                dmf_info['label'], 
                                value=default_value,
                                    help=dmf_info['help'],
                                    key=f"{key_prefix}_{col_name}_{dmf_key}"
                            ):
                                    selected_dmfs.append(dmf_key)
                        
                        # Store individual column selections
                        if selected_dmfs:
                            config['column_dmfs'][col_name] = selected_dmfs
                            st.success(f"✅ {col_name}: {', '.join([SYSTEM_DMFS[dmf]['label'] for dmf in selected_dmfs])}")
                        else:
                            # Remove from config if no metrics selected
                            if col_name in config['column_dmfs']:
                                del config['column_dmfs'][col_name]
                            st.caption(f"No metrics selected for {col_name}")
                        
                        st.markdown("")  # Add some spacing between columns
    else:
        st.info("No columns found for this table")
    
    # Show summary
    total_table_dmfs = sum(1 for v in config['table_dmfs'].values() if v)
    total_column_dmfs = len(config['column_dmfs'])
    
    if total_table_dmfs > 0 or total_column_dmfs > 0:
        st.success(f"📊 **Configuration**: {total_table_dmfs} table-level + {total_column_dmfs} column-level metrics")
        return config
    else:
        st.info("No metrics selected for this table")
        return None

def generate_bulk_dmf_sql(database: str, schema: str, schedule_config: dict, table_configs: dict) -> str:
    """Generate SQL for bulk DMF configuration."""
    
    sql_lines = [
        f"-- Bulk DMF Configuration for {len(table_configs)} table(s)",
        f"-- Database: {database}",
        f"-- Schema: {schema}",
        f"-- Schedule: {schedule_config['description']}",
        f"-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        ""
    ]
    
    for table_name, config in table_configs.items():
        full_table_name = get_fully_qualified_name(database, schema, table_name)
        
        sql_lines.extend([
            f"-- ========================================",
            f"-- Configuration for {table_name}",
            f"-- ========================================",
            "",
            "-- Step 1: Set monitoring schedule",
            f"ALTER TABLE {full_table_name} SET DATA_METRIC_SCHEDULE = '{schedule_config['schedule_expression']}';",
            "",
            "-- Step 2: Add Data Metric Functions"
        ])
                
                # Table-level DMFs
        if config['table_dmfs'].get('ROW_COUNT'):
            sql_lines.append(f"ALTER TABLE {full_table_name} ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();")
        
        if config['table_dmfs'].get('FRESHNESS') and 'freshness_column' in config:
            quoted_col = quote_identifier(config['freshness_column'])
            sql_lines.append(f"ALTER TABLE {full_table_name} ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.FRESHNESS ON ({quoted_col});")
        
        # Column-level DMFs
        if config['column_dmfs']:
            sql_lines.append("")
            sql_lines.append("-- Column-level DMFs")
            
            for col_name, dmf_list in config['column_dmfs'].items():
                quoted_col = quote_identifier(col_name)
                for dmf_key in dmf_list:
                    sql_lines.append(f"ALTER TABLE {full_table_name} ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.{dmf_key} ON ({quoted_col});")
        
        sql_lines.extend(["", ""])
    
    sql_lines.extend([
        "-- View results with:",
        "-- SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS",
        "-- ORDER BY MEASUREMENT_TIME DESC;"
    ])
    
    return "\n".join(sql_lines)

def execute_bulk_dmf_configuration(conn, database: str, schema: str, sql_commands: str, table_configs: dict):
    """Execute bulk DMF configuration with progress tracking."""
    
    sql_lines = [line.strip() for line in sql_commands.split('\n') if line.strip() and not line.strip().startswith('--')]
    
    if not sql_lines:
        st.error("No SQL commands to execute")
        return
    
    # Progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    success_count = 0
    error_count = 0
    total_commands = len(sql_lines)
    
    # Execute commands
    for i, sql_line in enumerate(sql_lines):
        progress = (i + 1) / total_commands
        progress_bar.progress(progress)
        status_text.text(f"Executing command {i + 1} of {total_commands}...")
        
        try:
            if execute_comment_sql(conn, sql_line, 'DMF'):
                success_count += 1
                
                # Log DMF history
                if 'ADD DATA METRIC FUNCTION' in sql_line.upper():
                    log_dmf_execution(conn, database, schema, sql_line)
            else:
                error_count += 1
                st.error(f"❌ Failed: {sql_line}")
                
        except Exception as e:
            error_count += 1
            st.error(f"❌ Error in: {sql_line}")
            st.error(f"Details: {str(e)}")
    
    # Final results
    progress_bar.progress(1.0)
    status_text.empty()
    
    if error_count == 0:
        st.success(f"🎉 **Success!** Applied {success_count} DMF configurations to {len(table_configs)} table(s)")
        st.info("💡 View results: `SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS;`")
    else:
        if success_count > 0:
            st.warning(f"⚠️ **Partial Success**: {success_count} succeeded, {error_count} failed")
        else:
            st.error(f"❌ **Failed**: All {error_count} commands failed. Check permissions and table ownership.")

def log_dmf_execution(conn, database: str, schema: str, sql_line: str):
    """Log DMF execution to history table."""
    try:
        # Extract table name
        table_match = re.search(r'ALTER TABLE\s+(?:"?[^".\s]+"?\.)?(?:"?[^".\s]+"?\.)?"?([^".\s]+)"?\s+ADD', sql_line.upper())
        table_name = table_match.group(1).strip('"') if table_match else "UNKNOWN_TABLE"
        
        # Extract DMF type
        dmf_match = re.search(r'SNOWFLAKE\.CORE\.(\w+)', sql_line.upper())
        dmf_type = dmf_match.group(1) if dmf_match else "UNKNOWN_DMF"
        
        # Extract column name if present
        column_match = re.search(r'ON \(([^)]+)\)', sql_line)
        column_name = None
        if column_match and column_match.group(1).strip():
            column_name = column_match.group(1).strip().strip('"').strip("'")
        
        # Log to history (import here to avoid circular imports)
        from .setup import log_dmf_history
        log_dmf_history(conn, database, schema, table_name, dmf_type, column_name, "ADDED")
        
    except Exception as e:
        # Don't fail the main operation if logging fails
        st.warning(f"Could not log DMF history: {str(e)}")

def test_dmf_permissions(conn, database: str, schema: str):
    """Test DMF permissions and setup."""
    
    with st.spinner("Testing permissions and setup..."):
        results = []
        
        # Test 1: Database roles
        try:
            test_query = "SELECT CURRENT_ROLE()"
            if hasattr(conn, 'sql'):
                result = conn.sql(test_query).to_pandas()
                current_role = result.iloc[0, 0]
            else:
                result = pd.read_sql(test_query, conn)
                current_role = result.iloc[0, 0]
            results.append(("✅", "Connection", f"Connected as role: {current_role}"))
        except Exception as e:
            results.append(("❌", "Connection", f"Failed: {str(e)}"))
        
        # Test 2: Database access
        try:
            test_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}'"
            if hasattr(conn, 'sql'):
                result = conn.sql(test_query).to_pandas()
                table_count = result.iloc[0, 0]
            else:
                result = pd.read_sql(test_query, conn)
                table_count = result.iloc[0, 0]
            results.append(("✅", "Database Access", f"Can access {table_count} tables in {database}.{schema}"))
        except Exception as e:
            results.append(("❌", "Database Access", f"Failed: {str(e)}"))
        
        # Test 3: DMF monitoring results access
        try:
            test_query = "SELECT COUNT(*) FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS LIMIT 1"
            if hasattr(conn, 'sql'):
                conn.sql(test_query).to_pandas()
            else:
                pd.read_sql(test_query, conn)
            results.append(("✅", "DMF Results Access", "Can access monitoring results"))
        except Exception as e:
            results.append(("❌", "DMF Results Access", f"Failed: {str(e)}"))
    
    # Display results
    st.markdown("### 🧪 Permission Test Results")
    
    for status, test_name, message in results:
        if status == "✅":
            st.success(f"{status} **{test_name}**: {message}")
        else:
            st.error(f"{status} **{test_name}**: {message}")
    
    # Show recommendations
    failed_tests = [r for r in results if r[0] == "❌"]
    if failed_tests:
        st.markdown("### 💡 Recommendations")
        st.markdown("""
        If you see permission errors, ask your Snowflake administrator to run:
        ```sql
        GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE your_role;
        GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_LOOKUP TO ROLE your_role;
        ```
        """)

def show_dmf_quick_reference():
    """Show quick reference documentation in a collapsible section."""
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🔐 Required Permissions")
        st.code("""
-- Grant required database roles
           GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE your_role;
           GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_LOOKUP TO ROLE your_role;
        """, language="sql")
        
        st.markdown("### 📊 Available System DMFs")
        st.markdown("""
        **Table-Level:**
        - `ROW_COUNT` - Total rows in table
        - `FRESHNESS` - Data freshness (requires timestamp column)
        
        **Column-Level (All Types):**
        - `NULL_COUNT` - Count of NULL values
        - `DUPLICATE_COUNT` - Count of duplicate values  
        - `UNIQUE_COUNT` - Count of unique values
        """)
    
    with col2:
        st.markdown("### 📅 Schedule Examples")
        st.code("""
-- Every 30 minutes
'30 MINUTE'

-- Every 4 hours  
'USING CRON 0 */4 * * * UTC'

-- Daily at 6 AM UTC
'USING CRON 0 6 * * * UTC'

-- On data changes
'TRIGGER_ON_CHANGES'
        """, language="sql")
        
        st.markdown("### 🔍 View Results")
        st.code("""
SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
ORDER BY MEASUREMENT_TIME DESC;
        """, language="sql")
