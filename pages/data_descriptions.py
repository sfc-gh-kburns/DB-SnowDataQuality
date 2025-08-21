"""
Data Descriptions page for Snowflake Data Quality & Documentation App.
"""

import streamlit as st
import pandas as pd
import time
from typing import Any
from utils.data_fetchers import get_databases, get_schemas, get_tables_and_views, get_columns
from utils.ai_utils import get_available_models
from utils.description_helpers import generate_descriptions_for_objects


def show_data_descriptions_page(conn: Any):
    """Display the data descriptions page."""
    
    st.markdown("Generate AI-powered descriptions for your tables, views, and columns using Snowflake Cortex.")
    
    st.info("**Note:** Data Descriptions are generated using Snowflake Cortex and LLMs. Please be aware of cost implications.")
    
    # Database and Schema Selection
    st.markdown("### Database and Schema Selection")
    
    col1, col2 = st.columns(2)
    
    with col1:
        databases = get_databases(conn)
        if not databases:
            st.error("No databases accessible. Please check permissions.")
            return
        
        # Find current database index
        current_db_index = 0
        if st.session_state.desc_database in databases:
            current_db_index = databases.index(st.session_state.desc_database) + 1
            
        selected_db = st.selectbox(
            "Select Database",
            options=[""] + databases,
            index=current_db_index,
            key="desc_db_selector",
            help="Choose a database to explore"
        )
        
        # Update session state if changed
        if selected_db != st.session_state.desc_database:
            st.session_state.desc_database = selected_db
            st.session_state.desc_schema = ""  # Reset schema when database changes
    
    with col2:
        if selected_db:
            schemas = get_schemas(conn, selected_db)
            
            # Find current schema index
            current_schema_index = 0
            if st.session_state.desc_schema in schemas:
                current_schema_index = schemas.index(st.session_state.desc_schema) + 1
                
            selected_schema = st.selectbox(
                "Select Schema",
                options=[""] + schemas,
                index=current_schema_index,
                key="desc_schema_selector",
                help="Choose a schema within the selected database"
            )
            
            # Update session state if changed
            if selected_schema != st.session_state.desc_schema:
                st.session_state.desc_schema = selected_schema
        else:
            selected_schema = ""
            st.selectbox("Select Schema", options=[""], disabled=True, key="desc_schema_disabled", help="Select a database first")
    
    # Object Listing and Filtering
    if selected_db:
        st.markdown("---")
        st.markdown("### Data Objects")
        
        # Get tables and views (all schemas if no schema selected)
        refresh_key = st.session_state.get('last_refresh', '')
        if selected_schema:
            tables_df = get_tables_and_views(conn, selected_db, selected_schema, refresh_key)
        else:
            tables_df = get_tables_and_views(conn, selected_db, None, refresh_key)
        
        if not tables_df.empty:
            # Filter options
            col1, col2 = st.columns(2)
            with col1:
                show_undocumented_only = st.checkbox(
                    "Only show objects without descriptions",
                    help="Filter to show only tables/views that lack descriptions"
                )
            with col2:
                object_type_filter = st.selectbox(
                    "Object Type",
                    options=["All", "BASE TABLE", "VIEW"],
                    help="Filter by object type"
                )
            
            # Apply filters
            filtered_df = tables_df.copy()
            if show_undocumented_only:
                filtered_df = filtered_df[filtered_df['HAS_DESCRIPTION'] == 'No']
            if object_type_filter != "All":
                filtered_df = filtered_df[filtered_df['OBJECT_TYPE'] == object_type_filter]
            
            # Display filtered results
            if not filtered_df.empty:
                st.markdown("### Select Objects for Description Generation")
                
                # Add Select All checkbox and Refresh button
                col1, col2, col3 = st.columns([1, 1, 2])
                with col1:
                    select_all = st.checkbox("Select All", key="select_all_objects")
                with col2:
                    if st.button("🔄 Refresh Data", help="Refresh table and column data from Snowflake", key="refresh_tables_data"):
                        # Clear all caches to force fresh data from Snowflake
                        st.cache_data.clear()
                        st.cache_resource.clear()
                        
                        # Force cache invalidation with new timestamp
                        st.session_state['last_refresh'] = str(time.time())
                        
                        # Show success message and rerun
                        st.success("✅ Data refreshed! Latest descriptions loaded from Snowflake.")
                        st.rerun()
                
                # Add selection column
                df_with_selection = filtered_df.copy()
                df_with_selection.insert(0, "Select", select_all)
                
                # Configure columns based on whether we're showing schema info
                column_config = {
                    "Select": st.column_config.CheckboxColumn("Select", help="Select objects for description generation"),
                    "OBJECT_NAME": "Object Name",
                    "OBJECT_TYPE": "Type", 
                    "CURRENT_DESCRIPTION": st.column_config.TextColumn("Current Description", width="large"),
                    "HAS_DESCRIPTION": "Has Description"
                }
                
                # Add schema column if showing multiple schemas
                if not selected_schema and 'SCHEMA_NAME' in df_with_selection.columns:
                    column_config["SCHEMA_NAME"] = "Schema"
                
                edited_df = st.data_editor(
                    df_with_selection,
                    use_container_width=True,
                    column_config=column_config,
                    hide_index=True,
                    key="object_selection_table"
                )
                
                # Get selected objects
                selected_rows = edited_df[edited_df["Select"] == True]
                selected_objects = selected_rows['OBJECT_NAME'].tolist()
                
                # Show column details for selected objects
                if selected_objects:
                    st.markdown("### Column Details")
                    st.info(f"Selected {len(selected_objects)} object(s): {', '.join(selected_objects)}")
                    
                    for obj_name in selected_objects:
                        # Find the schema for this object if we're in database-level view
                        if selected_schema:
                            obj_schema = selected_schema
                            expander_title = f"Columns in {obj_name}"
                        else:
                            # Find schema from the selected rows
                            obj_row = selected_rows[selected_rows['OBJECT_NAME'] == obj_name]
                            if not obj_row.empty and 'SCHEMA_NAME' in obj_row.columns:
                                obj_schema = obj_row.iloc[0]['SCHEMA_NAME']
                                expander_title = f"Columns in {obj_schema}.{obj_name}"
                            else:
                                continue  # Skip if we can't find the schema
                        
                        with st.expander(expander_title):
                            columns_df = get_columns(conn, selected_db, obj_schema, obj_name, refresh_key)
                            
                            if not columns_df.empty:
                                show_undoc_cols = st.checkbox(
                                    f"Only show columns without descriptions ({obj_name})",
                                    key=f"undoc_cols_{obj_name}"
                                )
                                
                                display_cols_df = columns_df.copy()
                                if show_undoc_cols:
                                    display_cols_df = display_cols_df[display_cols_df['HAS_DESCRIPTION'] == 'No']
                                
                                st.dataframe(
                                    display_cols_df,
                                    use_container_width=True,
                                    column_config={
                                        "COLUMN_NAME": "Column Name",
                                        "DATA_TYPE": "Data Type",
                                        "CURRENT_DESCRIPTION": st.column_config.TextColumn("Current Description", width="large"),
                                        "HAS_DESCRIPTION": "Has Description"
                                    }
                                )
                
                # LLM Model Selection and Actions
                if selected_objects:
                    st.markdown("---")
                    st.markdown("### AI Description Generation")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        available_models = get_available_models()
                        selected_model = st.selectbox(
                            "Select LLM Model",
                            options=available_models,
                            index=0,
                            key="desc_model",
                            help="Choose the Cortex LLM model for description generation"
                        )
                    
                    with col2:
                        if st.button("🧪 Test Model", help="Test if the selected model is available"):
                            with st.spinner("Testing model..."):
                                try:
                                    test_query = f"""
                                    SELECT SNOWFLAKE.CORTEX.COMPLETE(
                                        '{selected_model}',
                                        'Hello, this is a test.'
                                    ) as test_response
                                    """
                                    
                                    if hasattr(conn, 'sql'):
                                        result = conn.sql(test_query).to_pandas()
                                    else:
                                        result = pd.read_sql(test_query, conn)
                                    
                                    st.success(f"✅ Model {selected_model} is working!")
                                    
                                except Exception as e:
                                    st.error(f"❌ Model {selected_model} failed: {str(e)}")
                    
                    # Action buttons
                    st.markdown("### Generate Descriptions")
                    st.caption(f"Generate AI-powered descriptions for {len(selected_objects)} selected object(s)")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if st.button("Generate Table Descriptions", use_container_width=True):
                            generate_descriptions_for_objects(conn, selected_model, selected_db, selected_schema, selected_objects, selected_rows, "table")
                    
                    with col2:
                        if st.button("Generate Column Descriptions", use_container_width=True):
                            generate_descriptions_for_objects(conn, selected_model, selected_db, selected_schema, selected_objects, selected_rows, "column")
                    
                    with col3:
                        if st.button("Generate Both", type="primary", use_container_width=True):
                            generate_descriptions_for_objects(conn, selected_model, selected_db, selected_schema, selected_objects, selected_rows, "both")
                
                else:
                    st.info("✨ Select objects from the table above using the checkboxes to enable description generation.")
            
            else:
                st.info("No objects found matching the current filters.")
                
        else:
            st.info("No tables or views found in the selected schema.")
    
    else:
        st.info("Please select a database and schema to get started.")
