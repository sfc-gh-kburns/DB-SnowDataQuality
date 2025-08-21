"""
Helper functions for data description generation and management.
"""

import streamlit as st
import pandas as pd
import re
import time
from typing import Any, List, Dict
from .database import get_fully_qualified_name, quote_identifier, execute_comment_sql
from .data_fetchers import get_tables_and_views, get_columns
from .ai_utils import generate_table_description, generate_column_description


def get_view_ddl(conn: Any, database_name: str, schema_name: str, view_name: str) -> str:
    """Get the DDL for a view using GET_DDL function."""
    try:
        fully_qualified_name = get_fully_qualified_name(database_name, schema_name, view_name)
        ddl_query = f"SELECT GET_DDL('VIEW', '{fully_qualified_name}')"
        
        if hasattr(conn, 'sql'):
            result = conn.sql(ddl_query).to_pandas()
            return result.iloc[0, 0] if not result.empty else ""
        else:
            result = pd.read_sql(ddl_query, conn)
            return result.iloc[0, 0] if not result.empty else ""
    except Exception as e:
        st.error(f"Error getting view DDL: {str(e)}")
        return ""


def log_description_history(conn: Any, database: str, schema: str, object_name: str, 
                          object_type: str, before_desc: str, after_desc: str):
    """Log description changes to history table."""
    try:
        # Parse object name for column references
        if '.' in object_name and object_type == 'COLUMN':
            table_name, column_name = object_name.rsplit('.', 1)
        else:
            table_name = object_name
            column_name = None
        
        # Build the SQL for the executed comment
        if object_type == 'TABLE':
            fully_qualified_name = get_fully_qualified_name(database, schema, table_name)
            escaped_desc = after_desc.replace("'", "''") if after_desc else ''
            sql_executed = f"COMMENT ON TABLE {fully_qualified_name} IS '{escaped_desc}';"
        elif object_type == 'COLUMN' and column_name:
            fully_qualified_name = get_fully_qualified_name(database, schema, table_name)
            quoted_col_name = quote_identifier(column_name)
            escaped_desc = after_desc.replace("'", "''") if after_desc else ''
            sql_executed = f"COMMENT ON COLUMN {fully_qualified_name}.{quoted_col_name} IS '{escaped_desc}';"
        else:
            sql_executed = f"-- {object_type} description update"
        
        # Insert into history table
        history_sql = f"""
        INSERT INTO DB_SNOWTOOLS.PUBLIC.DATA_DESCRIPTION_HISTORY 
        (DATABASE_NAME, SCHEMA_NAME, OBJECT_TYPE, OBJECT_NAME, COLUMN_NAME, 
         BEFORE_DESCRIPTION, AFTER_DESCRIPTION, SQL_EXECUTED)
        VALUES ('{database}', '{schema}', '{object_type}', '{table_name}', 
                {f"'{column_name}'" if column_name else 'NULL'}, 
                {f"'{before_desc.replace(chr(39), chr(39)+chr(39))}'" if before_desc else 'NULL'}, 
                {f"'{after_desc.replace(chr(39), chr(39)+chr(39))}'" if after_desc else 'NULL'}, 
                '{sql_executed.replace(chr(39), chr(39)+chr(39))}')
        """
        
        if hasattr(conn, 'sql'):
            conn.sql(history_sql).collect()
        else:
            cursor = conn.cursor()
            cursor.execute(history_sql)
            
    except Exception as e:
        st.warning(f"Could not log to history: {str(e)}")


def update_view_descriptions(conn: Any, database: str, schema: str, view_name: str, 
                           columns_df: pd.DataFrame, model: str, generated_descriptions: List[Dict], 
                           view_description: str = None, generate_columns: bool = True) -> bool:
    """
    Update view and/or column descriptions by recreating the view with comments.
    This is necessary because Snowflake doesn't support COMMENT ON VIEW or COMMENT ON COLUMN for views.
    """
    try:
        st.info(f"🔍 Step 1: Getting DDL for view {database}.{schema}.{view_name}")
        
        # Get the original view DDL using GET_DDL function
        original_ddl = get_view_ddl(conn, database, schema, view_name)
        if not original_ddl:
            st.error(f"❌ Could not retrieve DDL for view {view_name}")
            return False
            
        st.success(f"✅ Retrieved view DDL ({len(original_ddl)} characters)")
        st.info(f"📄 DDL Preview: {original_ddl[:100]}...")
        
        # Generate column descriptions if requested
        column_descriptions = {}
        if generate_columns:
            st.info(f"🔍 Step 2: Generating column descriptions for view {view_name}")
            
            for _, col_row in columns_df.iterrows():
                col_name = col_row['COLUMN_NAME']
                data_type = col_row['DATA_TYPE']
                
                try:
                    new_col_desc = generate_column_description(
                        conn, model, database, schema, view_name, col_name, data_type
                    )
                    
                    if new_col_desc:
                        column_descriptions[col_name] = new_col_desc
                        # Collect for summary display
                        generated_descriptions.append({
                            'type': 'column',
                            'object': f"{view_name}.{col_name}",
                            'description': new_col_desc
                        })
                        
                except Exception as e:
                    st.warning(f"⚠️ Could not generate description for {view_name}.{col_name}: {str(e)}")
            
            if column_descriptions:
                st.success(f"✅ Generated descriptions for {len(column_descriptions)} columns")
                st.info(f"🔍 Will update comments for: {', '.join(column_descriptions.keys())}")
            else:
                st.warning(f"⚠️ No column descriptions generated for view {view_name}")
        
        # Check if we have anything to update
        if not view_description and not column_descriptions:
            st.warning(f"⚠️ No descriptions to update for view {view_name}")
            return False
        
        st.info(f"🔍 Step 3: Parsing view DDL")
        
        # Parse the DDL to extract the view name and SELECT statement
        ddl_upper = original_ddl.upper()
        
        # Find view name - look for pattern: CREATE [OR REPLACE] VIEW schema.view_name
        view_pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([^\s\(]+)'
        view_match = re.search(view_pattern, ddl_upper)
        if not view_match:
            st.error("Could not extract view name from DDL")
            st.error(f"View DDL preview: {original_ddl[:200]}...")
            return False
        
        full_view_name = view_match.group(1)
        
        # More robust AS detection - look for the AS that comes after the view definition
        as_patterns = [
            r'\)\s*(?:COMMENT\s*=\s*[\'"][^\'\"]*[\'"])?\s*AS\s*\(',  # ) [COMMENT='...'] AS (
            r'\)\s*AS\s*\(',  # ) AS (
            r'\)\s*(?:COMMENT\s*=\s*[\'"][^\'\"]*[\'"])?\s*AS\s+SELECT',  # ) [COMMENT='...'] AS SELECT
            r'\)\s*AS\s+SELECT',  # ) AS SELECT  
            r'\)\s*AS\s*SELECT'   # ) AS SELECT (no space)
        ]
        
        as_match = None
        for pattern in as_patterns:
            match = re.search(pattern, ddl_upper, re.DOTALL)
            if match:
                as_match = match
                break
        
        if not as_match:
            st.error("Could not find AS clause after view definition")
            st.error(f"View DDL preview: {original_ddl[:500]}...")
            # Try a simpler approach - just find " AS " in the DDL
            simple_as_pos = ddl_upper.find(' AS ')
            if simple_as_pos != -1:
                st.info(f"Found simple ' AS ' at position {simple_as_pos}")
                as_match = type('Match', (), {'end': lambda: simple_as_pos + 4})()
            else:
                return False
        
        # Extract the SELECT statement starting after the AS clause
        as_end_pos = as_match.end()
        
        # Find where the actual SELECT starts (might be after '(' and comments)
        remaining_ddl = original_ddl[as_end_pos:]
        
        # Look for the SELECT keyword
        select_match = re.search(r'SELECT', remaining_ddl.upper())
        if select_match:
            select_start_in_remaining = select_match.start()
            select_statement = remaining_ddl[select_start_in_remaining:]
        else:
            # Fallback - take everything after AS
            select_statement = remaining_ddl
        
        # Clean up the select statement
        select_statement = select_statement.strip()
        
        st.info(f"🔍 Extracted SELECT statement preview: {select_statement[:100]}...")
        
        # Build the column list with comments (existing + new)
        column_definitions = []
        
        for _, row in columns_df.iterrows():
            col_name = row['COLUMN_NAME']
            current_comment = row['CURRENT_DESCRIPTION']
            
            # Check if this column has a new comment
            if col_name.upper() in [c.upper() for c in column_descriptions.keys()]:
                # Find the matching column comment (case insensitive)
                new_comment = None
                for update_col, update_comment in column_descriptions.items():
                    if update_col.upper() == col_name.upper():
                        new_comment = update_comment
                        break
                
                if new_comment:
                    escaped_comment = new_comment.replace("'", "''")
                    column_definitions.append(f"{col_name} COMMENT '{escaped_comment}'")
            # If the column already has a comment, preserve it
            elif current_comment and current_comment.strip():
                escaped_current = current_comment.replace("'", "''")
                column_definitions.append(f"{col_name} COMMENT '{escaped_current}'")
            # Otherwise, just the column name
            else:
                column_definitions.append(col_name)
        
        st.info(f"🔍 Step 4: Building new view DDL with column comments")
        
        # Build the new CREATE OR REPLACE VIEW statement
        column_list = ",\n        ".join(column_definitions)
        
        # Use the fully qualified view name for safety
        fully_qualified_view_name = get_fully_qualified_name(database, schema, view_name)
        
        # Clean the select statement to ensure proper formatting
        clean_select = select_statement.strip()
        
        # Ensure the SELECT statement starts with SELECT keyword
        if not clean_select.upper().startswith('SELECT'):
            clean_select = f"SELECT {clean_select}"
        
        # Remove any trailing semicolons or extra parentheses from the SELECT statement
        clean_select = clean_select.rstrip(';').rstrip(')')
        
        # Build the complete DDL with proper AS clause and view comment
        view_comment_clause = ""
        if view_description:
            escaped_view_desc = view_description.replace("'", "''")
            view_comment_clause = f" COMMENT = '{escaped_view_desc}'"
        
        new_ddl = f"""CREATE OR REPLACE VIEW {fully_qualified_view_name} (
        {column_list}
    ){view_comment_clause} AS (
    {clean_select}
    )"""
    
        st.info(f"🔍 Step 5: Final DDL generated ({len(new_ddl)} characters)")
        
        # Show a preview of the new DDL
        st.code(new_ddl[:400] + "..." if len(new_ddl) > 400 else new_ddl, language="sql")
        
        # Execute the new DDL
        try:
            st.info(f"🔍 Step 6: Executing view recreation SQL...")
            
            if hasattr(conn, 'sql'):  # Snowpark session
                st.info("🔧 Using Snowpark connection to execute DDL")
                result = conn.sql(new_ddl).collect()
                st.info(f"📊 Snowpark execution result: {len(result)} rows returned")
            else:  # Regular connection
                st.info("🔧 Using regular connection to execute DDL")
                cursor = conn.cursor()
                cursor.execute(new_ddl)
                st.info("📊 Regular connection execution completed")
            
            # Build success message based on what was updated
            updates = []
            if view_description:
                updates.append("view description")
            if column_descriptions:
                updates.append(f"column comments for: {', '.join(column_descriptions.keys())}")
            
            update_msg = " and ".join(updates)
            st.success(f"✅ Successfully recreated view {view_name} with {update_msg}")
            
            # Log view description to history if provided
            if view_description:
                log_description_history(conn, database, schema, view_name, 'VIEW', 
                                      None, view_description)
            
            # Log column descriptions to history
            for col_name, col_desc in column_descriptions.items():
                log_description_history(conn, database, schema, f"{view_name}.{col_name}", 'COLUMN', 
                                      None, col_desc)
            
            return True
            
        except Exception as exec_error:
            st.error(f"❌ Error executing view recreation: {str(exec_error)}")
            st.error("🚨 Failed DDL:")
            st.code(new_ddl, language="sql")
            return False
        
    except Exception as e:
        st.error(f"❌ Error updating view column comments: {str(e)}")
        return False


def generate_descriptions_for_objects(conn: Any, model: str, database: str, schema: str, 
                                    objects: List[str], selected_rows: pd.DataFrame, 
                                    generation_type: str):
    """Generate descriptions for selected objects."""
    
    total_updates = 0
    generated_descriptions = []  # Track all generated descriptions for summary
    
    # Create expander for processing details
    with st.expander("🔍 View Processing Details", expanded=False):
        with st.spinner("Generating descriptions..."):
            
            for obj_name in objects:
                
                # Determine the schema for this object
                if schema:
                    obj_schema = schema
                    display_name = obj_name
                else:
                    # Find schema from the selected rows
                    obj_row = selected_rows[selected_rows['OBJECT_NAME'] == obj_name]
                    if obj_row.empty or 'SCHEMA_NAME' not in obj_row.columns:
                        st.warning(f"⚠️ Could not find schema for {obj_name}, skipping...")
                        continue
                    obj_schema = obj_row.iloc[0]['SCHEMA_NAME']
                    display_name = f"{obj_schema}.{obj_name}"
                
                # Generate table/view descriptions
                if generation_type in ['table', 'both']:
                    st.write(f"Processing table/view: {display_name}")
                    
                    # Get current description
                    refresh_key = st.session_state.get('last_refresh', '')
                    tables_df = get_tables_and_views(conn, database, obj_schema, refresh_key)
                    current_obj = tables_df[tables_df['OBJECT_NAME'] == obj_name]
                    if current_obj.empty:
                        st.warning(f"⚠️ Could not find {obj_name} in {obj_schema}, skipping...")
                        continue
                    current_obj = current_obj.iloc[0]
                    current_desc = current_obj['CURRENT_DESCRIPTION']
                    object_type = current_obj['OBJECT_TYPE']
                    
                    # Generate description
                    try:
                        new_desc = generate_table_description(
                            conn, model, database, obj_schema, obj_name, 
                            'TABLE' if object_type == 'BASE TABLE' else 'VIEW'
                        )
                        
                        if new_desc:
                            if object_type == 'BASE TABLE':
                                # For tables, use COMMENT ON TABLE
                                escaped_desc = new_desc.replace("'", "''")
                                fully_qualified_name = get_fully_qualified_name(database, obj_schema, obj_name)
                                comment_sql = f"COMMENT ON TABLE {fully_qualified_name} IS '{escaped_desc}';"
                                
                                # Execute the comment
                                if execute_comment_sql(conn, comment_sql, 'TABLE'):
                                    st.success(f"✅ Updated description for {obj_name}")
                                    total_updates += 1
                                    # Log to history
                                    log_description_history(conn, database, obj_schema, obj_name, 'TABLE', 
                                                          current_desc, new_desc)
                                    # Collect for summary display
                                    generated_descriptions.append({
                                        'type': 'table',
                                        'object': obj_name,
                                        'description': new_desc
                                    })
                                else:
                                    st.error(f"❌ Failed to update description for {obj_name}")
                            else:
                                # For views, we need to store the description to use with CREATE OR REPLACE VIEW
                                st.session_state[f'view_desc_{obj_name}'] = new_desc
                                st.success(f"✅ Generated description for view {obj_name} (will be applied with view recreation)")
                                # Collect for summary display
                                generated_descriptions.append({
                                    'type': 'table',
                                    'object': obj_name,
                                    'description': new_desc
                                })
                        else:
                            st.warning(f"⚠️ No description generated for {obj_name}")
                            
                    except Exception as e:
                        st.error(f"❌ Error processing {obj_name}: {str(e)}")
                    
                    # Handle view descriptions that were generated but not applied (table-only generation)
                    if generation_type == 'table' and object_type == 'VIEW':
                        view_desc = st.session_state.get(f'view_desc_{obj_name}', None)
                        if view_desc:
                            # Apply the view description immediately since no columns will be processed
                            refresh_key = st.session_state.get('last_refresh', '')
                            columns_df = get_columns(conn, database, obj_schema, obj_name, refresh_key)
                            
                            success = update_view_descriptions(
                                conn, database, obj_schema, obj_name, columns_df, model, generated_descriptions,
                                view_description=view_desc, generate_columns=False
                            )
                            if success:
                                total_updates += 1
                                # Clean up the stored view description
                                del st.session_state[f'view_desc_{obj_name}']
            
                # Generate column descriptions
                if generation_type in ['column', 'both']:
                    st.write(f"Processing columns in: {display_name}")
                    
                    refresh_key = st.session_state.get('last_refresh', '')
                    columns_df = get_columns(conn, database, obj_schema, obj_name, refresh_key)
                    
                    # Get object type to handle views differently
                    tables_df = get_tables_and_views(conn, database, obj_schema, refresh_key)
                    current_obj = tables_df[tables_df['OBJECT_NAME'] == obj_name]
                    if current_obj.empty:
                        st.warning(f"⚠️ Could not find {obj_name} in {obj_schema} for column processing, skipping...")
                        continue
                    object_type = current_obj.iloc[0]['OBJECT_TYPE']
                
                    # For views, we need to handle column descriptions differently
                    if object_type == 'VIEW':
                        # Check if we have a stored view description from table generation
                        view_desc = st.session_state.get(f'view_desc_{obj_name}', None)
                        
                        success = update_view_descriptions(
                            conn, database, obj_schema, obj_name, columns_df, model, generated_descriptions,
                            view_description=view_desc, generate_columns=True
                        )
                        if success:
                            # Count updates: view description (if any) + column descriptions
                            update_count = len(columns_df)
                            if view_desc:
                                update_count += 1  # Add 1 for the view description
                            total_updates += update_count
                            
                            # Clean up the stored view description
                            if view_desc:
                                del st.session_state[f'view_desc_{obj_name}']
                    else:
                        # For tables, use the standard column comment approach
                        for _, col_row in columns_df.iterrows():
                            col_name = col_row['COLUMN_NAME']
                            data_type = col_row['DATA_TYPE']
                            current_col_desc = col_row['CURRENT_DESCRIPTION']
                            
                            try:
                                new_col_desc = generate_column_description(
                                    conn, model, database, obj_schema, obj_name, col_name, data_type
                                )
                                
                                if new_col_desc:
                                    # Create COMMENT SQL for column (tables only)
                                    escaped_col_desc = new_col_desc.replace("'", "''")
                                    fully_qualified_name = get_fully_qualified_name(database, obj_schema, obj_name)
                                    quoted_col_name = quote_identifier(col_name)
                                    comment_sql = f"COMMENT ON COLUMN {fully_qualified_name}.{quoted_col_name} IS '{escaped_col_desc}';"
                                    
                                    # Execute the comment
                                    if execute_comment_sql(conn, comment_sql, 'COLUMN'):
                                        st.success(f"✅ Updated description for {obj_name}.{col_name}")
                                        total_updates += 1
                                        # Log to history
                                        log_description_history(conn, database, obj_schema, f"{obj_name}.{col_name}", 'COLUMN', 
                                                              current_col_desc, new_col_desc)
                                        # Collect for summary display
                                        generated_descriptions.append({
                                            'type': 'column',
                                            'object': f"{obj_name}.{col_name}",
                                            'description': new_col_desc
                                        })
                                    else:
                                        st.error(f"❌ Failed to update description for {obj_name}.{col_name}")
                                else:
                                    st.warning(f"⚠️ No description generated for {obj_name}.{col_name}")
                                    
                            except Exception as e:
                                st.error(f"Error processing {obj_name}.{col_name}: {str(e)}")
    
    st.success(f"Description generation complete! Updated {total_updates} descriptions.")
    
    # Show generated descriptions in a collapsible section
    if generated_descriptions:
        with st.expander("📝 View Generated Descriptions", expanded=False):
            st.markdown("**All generated descriptions from this session:**")
            
            # Group by type for better organization
            table_descriptions = [desc for desc in generated_descriptions if desc['type'] == 'table']
            column_descriptions = [desc for desc in generated_descriptions if desc['type'] == 'column']
            
            if table_descriptions:
                st.markdown("### 📋 Table/View Descriptions")
                for desc in table_descriptions:
                    st.markdown(f"**{desc['object']}:**")
                    st.markdown(f"> {desc['description']}")
                    st.markdown("---")
            
            if column_descriptions:
                st.markdown("### 📊 Column Descriptions")
                for desc in column_descriptions:
                    st.markdown(f"**{desc['object']}:**")
                    st.markdown(f"> {desc['description']}")
                    st.markdown("---")
            
            st.info(f"💡 **Summary:** Generated {len(table_descriptions)} table descriptions and {len(column_descriptions)} column descriptions")
