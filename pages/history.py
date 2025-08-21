"""
History page for Snowflake Data Quality & Documentation App.
Handles historical tracking for descriptions, DMF configurations, and quality monitoring.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Any

from utils.data_fetchers import get_databases, get_schemas


def show_history_page(conn: Any):
    """Display the history page."""
    
    st.markdown("View historical tracking data for description changes and data quality monitoring.")
    
    # Tab selection for different history types
    history_tab1, history_tab2 = st.tabs(["📝 Description History", "🔍 Quality History"])
    
    with history_tab1:
        st.markdown("### Description Changes History")
        
        try:
            # Try to get description history (exclude DMF and contact entries)
            history_query = """
            SELECT 
                DATABASE_NAME,
                SCHEMA_NAME,
                OBJECT_TYPE,
                OBJECT_NAME,
                COLUMN_NAME,
                BEFORE_DESCRIPTION,
                AFTER_DESCRIPTION,
                UPDATED_BY,
                UPDATED_AT
            FROM DB_SNOWTOOLS.PUBLIC.DATA_DESCRIPTION_HISTORY
            WHERE OBJECT_TYPE NOT LIKE 'DMF_%' 
              AND OBJECT_TYPE NOT LIKE 'CONTACT_%'
            ORDER BY UPDATED_AT DESC
            LIMIT 1000
            """
            
            if hasattr(conn, 'sql'):
                history_df = conn.sql(history_query).to_pandas()
            else:
                history_df = pd.read_sql(history_query, conn)
            
            if not history_df.empty:
                # Summary metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Changes", len(history_df))
                with col2:
                    st.metric("Unique Objects", history_df['OBJECT_NAME'].nunique())
                with col3:
                    st.metric("Unique Users", history_df['UPDATED_BY'].nunique())
                
                # Display history
                st.dataframe(
                    history_df,
                    use_container_width=True,
                    column_config={
                        "DATABASE_NAME": "Database",
                        "SCHEMA_NAME": "Schema",
                        "OBJECT_TYPE": "Type",
                        "OBJECT_NAME": "Object",
                        "COLUMN_NAME": "Column",
                        "BEFORE_DESCRIPTION": st.column_config.TextColumn("Before", width="medium"),
                        "AFTER_DESCRIPTION": st.column_config.TextColumn("After", width="medium"),
                        "UPDATED_BY": "Updated By",
                        "UPDATED_AT": "Updated At"
                    }
                )
                
                # Export option
                if st.button("📊 Export Description History to CSV"):
                    csv = history_df.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"description_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            else:
                st.info("No description history found. Start documenting objects to see changes here!")
                
        except Exception as e:
            st.warning("Description history tracking is not yet available.")
            st.info("This will be populated as you use the Data Descriptions feature to update object descriptions.")
    
    with history_tab2:
        # DMF Configuration History Section
        st.markdown("### 🔧 Data Quality Configuration History")
        st.markdown("Track when data quality metrics (DMFs) were added or modified.")
        
        try:
            # Get DMF configuration history
            dmf_history_query = """
            SELECT 
                DATABASE_NAME,
                SCHEMA_NAME,
                OBJECT_TYPE,
                OBJECT_NAME,
                COLUMN_NAME,
                AFTER_DESCRIPTION as ACTION_DESCRIPTION,
                UPDATED_BY,
                UPDATED_AT
            FROM DB_SNOWTOOLS.PUBLIC.DATA_DESCRIPTION_HISTORY
            WHERE OBJECT_TYPE LIKE 'DMF_%'
            ORDER BY UPDATED_AT DESC
            LIMIT 500
            """
            
            if hasattr(conn, 'sql'):
                dmf_history_df = conn.sql(dmf_history_query).to_pandas()
            else:
                dmf_history_df = pd.read_sql(dmf_history_query, conn)
            
            if not dmf_history_df.empty:
                # Summary metrics for DMF history
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("DMF Changes", len(dmf_history_df))
                with col2:
                    st.metric("Tables Configured", dmf_history_df['OBJECT_NAME'].nunique())
                with col3:
                    st.metric("Unique Metrics", dmf_history_df['OBJECT_TYPE'].nunique())
                
                # Display DMF configuration history
                st.dataframe(
                    dmf_history_df,
                    use_container_width=True,
                    column_config={
                        "DATABASE_NAME": "Database",
                        "SCHEMA_NAME": "Schema",
                        "OBJECT_TYPE": "DMF Type",
                        "OBJECT_NAME": "Table",
                        "COLUMN_NAME": "Column",
                        "ACTION_DESCRIPTION": st.column_config.TextColumn("Action", width="medium"),
                        "UPDATED_BY": "Updated By",
                        "UPDATED_AT": st.column_config.DatetimeColumn("Updated At")
                    }
                )
                
                # Export DMF history
                if st.button("📊 Export DMF Configuration History to CSV"):
                    csv = dmf_history_df.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"dmf_configuration_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            else:
                st.info("No DMF configuration history found. Configure data quality metrics to see changes here!")
                
        except Exception as e:
            st.warning("DMF configuration history is not yet available.")
            st.info("This will be populated as you use the Data Quality feature to configure monitoring.")
        
        st.markdown("---")
        
        # Existing Quality Monitoring Dashboard
        st.markdown("### 📊 Data Quality Monitoring Dashboard")
        st.markdown("Monitor and analyze your data quality metrics across all databases and schemas.")
        
        # Filters Section
        with st.expander("🔍 Filters & Settings", expanded=True):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Get available databases for filtering
                try:
                    databases = get_databases(conn)
                    selected_dbs = st.multiselect(
                        "Filter by Database(s)",
                        options=databases,
                        default=[],
                        help="Select specific databases to filter results"
                    )
                except:
                    selected_dbs = []
                    st.info("Could not load databases for filtering")
            
            with col2:
                # Schema filter (populated based on selected databases)
                selected_schemas = []
                if selected_dbs:
                    try:
                        all_schemas = []
                        for db in selected_dbs:
                            schemas = get_schemas(conn, db)
                            all_schemas.extend([f"{db}.{schema}" for schema in schemas])
                        
                        selected_schemas = st.multiselect(
                            "Filter by Schema(s)",
                            options=all_schemas,
                            default=[],
                            help="Select specific schemas to filter results"
                        )
                    except:
                        st.info("Select databases first to filter schemas")
            
            with col3:
                # Time range filter
                time_range = st.selectbox(
                    "Time Range",
                    options=["Last 24 Hours", "Last 7 Days", "Last 30 Days", "All Time"],
                    index=1,
                    help="Filter results by time period"
                )
        
        # Build filter conditions
        filter_conditions = []
        if selected_dbs:
            db_list = "', '".join(selected_dbs)
            filter_conditions.append(f"DATABASE_NAME IN ('{db_list}')")
        
        if selected_schemas:
            schema_conditions = []
            for schema_full in selected_schemas:
                db, schema = schema_full.split('.', 1)
                schema_conditions.append(f"(DATABASE_NAME = '{db}' AND SCHEMA_NAME = '{schema}')")
            filter_conditions.append(f"({' OR '.join(schema_conditions)})")
        
        # Time filter
        if time_range == "Last 24 Hours":
            filter_conditions.append("MEASUREMENT_TIME >= DATEADD(hour, -24, CURRENT_TIMESTAMP())")
        elif time_range == "Last 7 Days":
            filter_conditions.append("MEASUREMENT_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())")
        elif time_range == "Last 30 Days":
            filter_conditions.append("MEASUREMENT_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())")
        
        where_clause = " AND ".join(filter_conditions) if filter_conditions else "1=1"
        
        try:
            # Get quality monitoring results using the actual table structure
            st.markdown("#### 🎯 Data Quality Monitoring Results")
            
            # Build the WHERE clause for MEASUREMENT_TIME instead of UPDATED_AT
            measurement_where_clause = where_clause
            if "MEASUREMENT_TIME" not in where_clause and where_clause != "1=1":
                # Replace any time filters to use MEASUREMENT_TIME
                if "DATEADD" in where_clause:
                    measurement_where_clause = where_clause.replace("UPDATED_AT", "MEASUREMENT_TIME")
            
            # Main quality results query - using Snowflake's native DMF results
            # Extract column names from ARGUMENT_NAMES array when available
            quality_results_query = f"""
            SELECT 
                METRIC_NAME as MONITOR_NAME,
                TABLE_DATABASE as DATABASE_NAME,
                TABLE_SCHEMA as SCHEMA_NAME,
                TABLE_NAME,
                CASE 
                    WHEN ARGUMENT_TYPES IS NOT NULL AND ARRAY_SIZE(ARGUMENT_TYPES) > 0 
                         AND ARGUMENT_TYPES[0]::STRING = 'COLUMN'
                         AND ARGUMENT_NAMES IS NOT NULL AND ARRAY_SIZE(ARGUMENT_NAMES) > 0
                    THEN ARGUMENT_NAMES[0]::STRING
                    ELSE NULL
                END as COLUMN_NAME,
                VALUE as METRIC_VALUE,
                'numeric' as METRIC_UNIT,
                NULL as THRESHOLD_MIN,
                NULL as THRESHOLD_MAX,
                CASE 
                    WHEN VALUE IS NOT NULL THEN 'MEASURED'
                    ELSE 'UNKNOWN'
                END as STATUS,
                MEASUREMENT_TIME,
                MEASUREMENT_TIME as RECORD_INSERTED_AT,
                ARGUMENT_TYPES,
                ARGUMENT_NAMES
            FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
            WHERE {measurement_where_clause.replace('DATABASE_NAME', 'TABLE_DATABASE').replace('SCHEMA_NAME', 'TABLE_SCHEMA')}
            ORDER BY MEASUREMENT_TIME DESC
            LIMIT 1000
            """
            
            if hasattr(conn, 'sql'):
                quality_results_df = conn.sql(quality_results_query).to_pandas()
            else:
                quality_results_df = pd.read_sql(quality_results_query, conn)
            
            # Create a summary of monitored objects from the results
            dmf_config_df = pd.DataFrame()
            if not quality_results_df.empty:
                # Create a summary of what's being monitored based on the results
                dmf_config_df = quality_results_df.groupby(['DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME', 'COLUMN_NAME', 'MONITOR_NAME']).agg({
                    'MEASUREMENT_TIME': 'max',
                    'STATUS': 'last',
                    'RECORD_INSERTED_AT': 'max'
                }).reset_index()
                dmf_config_df.columns = ['DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME', 'COLUMN_NAME', 'MONITOR_TYPE', 'LAST_CHECK', 'LAST_STATUS', 'CONFIGURED_AT']
            
            # Summary KPIs
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_monitors = len(dmf_config_df) if not dmf_config_df.empty else 0
                st.metric(
                    "📊 Active Monitors", 
                    total_monitors,
                    help="Total number of active quality monitors"
                )
            
            with col2:
                unique_tables = quality_results_df['TABLE_NAME'].nunique() if not quality_results_df.empty else 0
                st.metric(
                    "📋 Tables Monitored", 
                    unique_tables,
                    help="Number of unique tables with quality monitoring"
                )
            
            with col3:
                total_checks = len(quality_results_df) if not quality_results_df.empty else 0
                st.metric(
                    "🔍 Quality Checks", 
                    total_checks,
                    help="Total quality check results in selected time range"
                )
            
            with col4:
                if not quality_results_df.empty and 'STATUS' in quality_results_df.columns:
                    pass_rate = (quality_results_df['STATUS'] == 'PASS').mean() * 100
                    st.metric(
                        "✅ Pass Rate", 
                        f"{pass_rate:.1f}%",
                        help="Percentage of quality checks that passed"
                    )
                else:
                    st.metric("✅ Pass Rate", "N/A")
            
            # Active Monitors Overview
            with st.expander("🔧 Active Quality Monitors", expanded=False):
                if not dmf_config_df.empty:
                    st.markdown(f"**{len(dmf_config_df)} active quality monitors**")
                    
                    # Group by monitor type for better visualization
                    monitor_type_counts = dmf_config_df['MONITOR_TYPE'].value_counts()
                    
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.markdown("**Monitor Types Distribution:**")
                        for monitor_type, count in monitor_type_counts.items():
                            st.write(f"• **{monitor_type}**: {count}")
                    
                    with col2:
                        # Show recent activity
                        st.markdown("**Recently Active:**")
                        recent_monitors = dmf_config_df.sort_values('LAST_CHECK', ascending=False).head(5)
                        for _, row in recent_monitors.iterrows():
                            col_info = f".{row['COLUMN_NAME']}" if pd.notna(row['COLUMN_NAME']) else ""
                            status_emoji = "✅" if row['LAST_STATUS'] == 'PASS' else "❌" if row['LAST_STATUS'] == 'FAIL' else "⚠️"
                            st.write(f"• {status_emoji} {row['MONITOR_TYPE']} on {row['TABLE_NAME']}{col_info}")
                    
                    # Full monitors table
                    st.markdown("**All Active Monitors:**")
                    st.dataframe(
                        dmf_config_df,
                        use_container_width=True,
                        column_config={
                            "DATABASE_NAME": "Database",
                            "SCHEMA_NAME": "Schema", 
                            "TABLE_NAME": "Table",
                            "COLUMN_NAME": "Column",
                            "MONITOR_TYPE": "Monitor Type",
                            "LAST_CHECK": st.column_config.DatetimeColumn("Last Check"),
                            "LAST_STATUS": "Last Status",
                            "CONFIGURED_AT": st.column_config.DatetimeColumn("First Seen")
                        }
                    )
                else:
                    st.info("No active quality monitors found. Visit the Data Quality page to set up monitoring.")
            
            # Quality Results Details
            with st.expander("📈 Quality Check Results", expanded=False):
                if not quality_results_df.empty:
                    st.markdown(f"**{len(quality_results_df)} quality check results in selected time range**")
                    
                    # Status distribution
                    if 'STATUS' in quality_results_df.columns:
                        status_counts = quality_results_df['STATUS'].value_counts()
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("**Status Distribution:**")
                            for status, count in status_counts.items():
                                emoji = "✅" if status == "PASS" else "❌" if status == "FAIL" else "⚠️"
                                st.write(f"{emoji} **{status}**: {count}")
                        
                        with col2:
                            # Recent failures (if any)
                            if 'FAIL' in status_counts:
                                st.markdown("**Recent Failures:**")
                                failures = quality_results_df[quality_results_df['STATUS'] == 'FAIL'].head(3)
                                for _, row in failures.iterrows():
                                    st.write(f"❌ {row['TABLE_NAME']} - {row.get('MONITOR_NAME', 'Unknown')}")
                    
                    # Full results table
                    st.markdown("**All Quality Check Results:**")
                    st.dataframe(
                        quality_results_df,
                        use_container_width=True,
                        column_config={
                            "MONITOR_NAME": "Monitor",
                            "DATABASE_NAME": "Database",
                            "SCHEMA_NAME": "Schema",
                            "TABLE_NAME": "Table",
                            "COLUMN_NAME": "Column",
                            "METRIC_VALUE": "Value",
                            "METRIC_UNIT": "Unit",
                            "THRESHOLD_MIN": "Min Threshold",
                            "THRESHOLD_MAX": "Max Threshold",
                            "STATUS": "Status",
                            "MEASUREMENT_TIME": st.column_config.DatetimeColumn("Measured At"),
                            "RECORD_INSERTED_AT": st.column_config.DatetimeColumn("Recorded At"),
                            "ARGUMENT_TYPES": st.column_config.TextColumn("Arg Types", width="small"),
                            "ARGUMENT_NAMES": st.column_config.TextColumn("Arg Names", width="medium")
                        }
                    )
                else:
                    st.info("No quality check results found for the selected filters and time range.")
            
            # Tables with Quality Monitoring
            with st.expander("📊 Tables & Columns with Quality Monitoring", expanded=False):
                if not dmf_config_df.empty:
                    # Group by table to show what's monitored
                    table_summary = dmf_config_df.groupby(['DATABASE_NAME', 'SCHEMA_NAME', 'TABLE_NAME']).agg({
                        'MONITOR_TYPE': lambda x: ', '.join(sorted(set(x))),
                        'COLUMN_NAME': lambda x: len([c for c in x if pd.notna(c)]),
                        'LAST_CHECK': 'max',
                        'LAST_STATUS': lambda x: 'MIXED' if len(set(x)) > 1 else x.iloc[0]
                    }).reset_index()
                    
                    table_summary.columns = ['Database', 'Schema', 'Table', 'Monitor Types', 'Columns Monitored', 'Last Check', 'Overall Status']
                    
                    st.markdown(f"**{len(table_summary)} tables have quality monitoring configured**")
                    
                    st.dataframe(
                        table_summary,
                        use_container_width=True,
                        column_config={
                            "Last Check": st.column_config.DatetimeColumn("Last Check"),
                            "Overall Status": "Status"
                        }
                    )
                else:
                    st.info("No tables have quality monitoring configured yet.")
            
            # Export Options
            st.markdown("#### 📥 Export Options")
            col1, col2 = st.columns(2)
            
            with col1:
                if not dmf_config_df.empty:
                    csv_config = dmf_config_df.to_csv(index=False)
                    st.download_button(
                        label="📊 Export Monitor Summary",
                        data=csv_config,
                        file_name=f"quality_monitors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        help="Download active quality monitors as CSV"
                    )
            
            with col2:
                if not quality_results_df.empty:
                    csv_results = quality_results_df.to_csv(index=False)
                    st.download_button(
                        label="📈 Export Quality Results", 
                        data=csv_results,
                        file_name=f"quality_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        help="Download quality check results as CSV"
                    )
                
        except Exception as e:
            st.warning("⚠️ Could not load data quality information.")
            st.error(f"Error details: {str(e)}")
            st.info("This may be because:")
            st.info("• No DMFs have been configured yet")
            st.info("• The quality monitoring tables don't exist")
            st.info("• There are permission issues accessing the data")