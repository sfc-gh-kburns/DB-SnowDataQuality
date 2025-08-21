"""
Data Quality page for Snowflake Data Quality & Documentation App.
Handles DMF configuration and data quality monitoring.
"""

import streamlit as st
import pandas as pd
import time
from typing import Any

from utils.data_fetchers import get_databases, get_schemas, get_tables_and_views
from utils.dmf_utils import (
    show_dmf_quick_reference, 
    configure_monitoring_schedule, 
    configure_table_dmfs,
    generate_bulk_dmf_sql,
    execute_bulk_dmf_configuration,
    test_dmf_permissions
)


def show_data_quality_page(conn: Any):
    """Display the modern single-page data quality configuration interface."""
    
    st.markdown("# 🔍 Data Quality Monitoring")
    st.markdown("Configure Snowflake Data Metric Functions (DMFs) with smart data type filtering and bulk operations.")
    
    # Quick access to documentation
    with st.expander("📚 **Quick Reference & Documentation**", expanded=False):
        show_dmf_quick_reference()
    
    st.markdown("---")
    
    # Main configuration interface
    show_modern_dmf_interface(conn)


def show_modern_dmf_interface(conn: Any):
    """Modern single-page DMF configuration interface."""
    
    # Step 1: Database and Schema Selection
    st.markdown("## 🎯 Step 1: Select Database and Schema")
    
    col1, col2 = st.columns(2)
    
    with col1:
        databases = get_databases(conn)
        if not databases:
            st.error("❌ No databases accessible. Please check your permissions.")
            return
        
        selected_db = st.selectbox(
            "📁 Database",
            options=[""] + databases,
            key="modern_dmf_database",
            help="Choose a database to explore tables"
        )
    
    with col2:
        if selected_db:
            schemas = get_schemas(conn, selected_db)
            selected_schema = st.selectbox(
                "📂 Schema", 
                options=[""] + schemas,
                key="modern_dmf_schema",
                help="Choose a schema within the selected database"
            )
        else:
            selected_schema = ""
            st.selectbox("📂 Schema", options=[""], disabled=True, help="Select a database first")
    
    if not selected_db or not selected_schema:
        st.info("👆 Please select both a database and schema to continue.")
        return
    
    # Step 2: Table Selection with Modern Grid
    st.markdown("---")
    st.markdown("## 📋 Step 2: Select Tables for Data Quality Monitoring")
    
    # Get tables
    refresh_key = st.session_state.get('last_refresh', '')
    tables_df = get_tables_and_views(conn, selected_db, selected_schema, refresh_key)
    
    if tables_df.empty:
        st.warning(f"No tables found in `{selected_db}.{selected_schema}`. Please check permissions or try a different schema.")
        return
    
    # Table filtering and selection controls
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        show_only_tables = st.checkbox(
            "📊 Tables only", 
            value=True,
            help="Show only base tables (exclude views)",
            key="modern_show_tables_only"
        )
    
    with col2:
        search_term = st.text_input(
            "🔍 Search tables",
            placeholder="Filter by name...",
            key="modern_table_search"
        )
    
    with col3:
        if st.button("🔄 Refresh", help="Refresh table list from Snowflake"):
            st.cache_data.clear()
            st.session_state['last_refresh'] = str(time.time())
            st.rerun()
    
    with col4:
        select_all = st.checkbox("✅ Select All", key="modern_select_all")
    
    # Apply filters
    filtered_df = tables_df.copy()
    
    if show_only_tables:
        filtered_df = filtered_df[filtered_df['OBJECT_TYPE'] == 'BASE TABLE']
    
    if search_term:
        filtered_df = filtered_df[
            filtered_df['OBJECT_NAME'].str.contains(search_term, case=False, na=False)
        ]
    
    if filtered_df.empty:
        st.info("No tables match your current filters. Try adjusting the search term or filters.")
        return
    
    # Add selection column
    filtered_df.insert(0, "Select", select_all)
    
    # Modern table selection grid
    st.markdown(f"**Found {len(filtered_df)} table(s) matching your criteria:**")
    
    edited_df = st.data_editor(
        filtered_df,
        use_container_width=True,
        column_config={
            "Select": st.column_config.CheckboxColumn(
                "Select",
                help="Select tables for DMF configuration",
                default=False
            ),
            "OBJECT_NAME": st.column_config.TextColumn(
                "Table Name",
                help="Name of the table",
                width="medium"
            ),
            "OBJECT_TYPE": st.column_config.TextColumn(
                "Type",
                help="Object type (TABLE or VIEW)",
                width="small"
            ),
            "CURRENT_DESCRIPTION": st.column_config.TextColumn(
                "Description",
                help="Current table description",
                width="large"
            ),
            "HAS_DESCRIPTION": st.column_config.CheckboxColumn(
                "Has Desc",
                help="Whether table has a description",
                width="small"
            )
        },
        hide_index=True,
        key="modern_table_selection_grid"
    )
    
    # Get selected tables
    selected_tables = edited_df[edited_df["Select"] == True]
    
    if selected_tables.empty:
        st.info("👆 Select one or more tables above to configure data quality metrics.")
        return
    
    # Step 3: Configuration for Selected Tables
    st.markdown("---")
    st.markdown(f"## ⚙️ Step 3: Configure DMFs for {len(selected_tables)} Selected Table(s)")
    
    # Show selected tables summary
    with st.expander(f"📋 **Selected Tables ({len(selected_tables)})**", expanded=False):
        for _, table in selected_tables.iterrows():
            st.markdown(f"• **{table['OBJECT_NAME']}** ({table['OBJECT_TYPE']})")
            if table['CURRENT_DESCRIPTION']:
                st.caption(f"  ↳ {table['CURRENT_DESCRIPTION']}")
    
    # Bulk Schedule Configuration
    st.markdown("### 📅 Monitoring Schedule")
    st.markdown("Set the monitoring schedule that will apply to all selected tables.")
    
    schedule_config = configure_monitoring_schedule("modern_bulk")
    
    if not schedule_config:
        st.info("👆 Please configure a monitoring schedule to continue.")
        return
    
    st.success(f"📅 **Schedule**: {schedule_config['description']}")
    
    # Individual Table Configuration
    st.markdown("---")
    st.markdown("### 🔧 Individual Table Configuration")
    st.markdown("Configure specific DMFs for each selected table. Each table shows only compatible metrics based on its column data types.")
    
    # Store all configurations
    table_configurations = {}
    
    # Create expander for each selected table
    for _, table_row in selected_tables.iterrows():
        table_name = table_row['OBJECT_NAME']
        
        with st.expander(f"🏷️ **{table_name}** - Configure DMFs", expanded=True):
            config = configure_table_dmfs(
                conn, selected_db, selected_schema, table_name, 
                key_prefix=f"modern_{table_name}"
            )
            
            if config:
                table_configurations[table_name] = config
    
    # Step 4: Generate and Execute
    if table_configurations:
        st.markdown("---")
        st.markdown("## 🚀 Step 4: Apply Configuration")
        
        # Generate SQL for all tables
        sql_commands = generate_bulk_dmf_sql(
            selected_db, selected_schema, schedule_config, table_configurations
        )
        
        # Show SQL preview
        with st.expander("📄 **Preview Generated SQL**", expanded=False):
            st.code(sql_commands, language="sql")
        
        # Action buttons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.download_button(
                label="📥 Download SQL",
                data=sql_commands,
                file_name=f"dmf_setup_{len(table_configurations)}_tables.sql",
                mime="text/sql",
                help="Download the generated SQL for manual execution"
            )
        
        with col2:
            if st.button(
                "🔧 Apply All DMFs", 
                type="primary",
                help=f"Execute SQL to configure DMFs on {len(table_configurations)} table(s)"
            ):
                execute_bulk_dmf_configuration(
                    conn, selected_db, selected_schema, sql_commands, table_configurations
                )
        
        with col3:
            if st.button("🧪 Test Connection", help="Test database connection and permissions"):
                test_dmf_permissions(conn, selected_db, selected_schema)