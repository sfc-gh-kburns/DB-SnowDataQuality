"""
Data Contacts page for Snowflake Data Quality & Documentation App.
Handles contact management and assignment for data governance.
"""

import streamlit as st
import pandas as pd
import time
from typing import Any

from utils.data_fetchers import get_databases, get_schemas, get_tables_and_views, get_all_contacts, get_table_contacts
from utils.database import get_fully_qualified_name, execute_comment_sql
from utils.setup import log_contact_history


def show_data_contacts_page(conn: Any):
    """Display the data contacts page."""
    
    st.markdown("Manage contacts and assignments for data governance, support, and access control.")
    
    st.info("This feature integrates with Snowflake's contact system for data governance workflows.")
    
    with st.expander("📋 **Contact Types**", expanded=False):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**🔍 Data Steward**")
            st.markdown("Responsible for data accuracy, quality, and governance")
        
        with col2:
            st.markdown("**🛠️ Technical Support**")
            st.markdown("Handles technical issues and system maintenance")
        
        with col3:
            st.markdown("**🔐 Access Control**")
            st.markdown("Manages data access permissions and security")
    
    st.markdown("---")
    
    # Database/Schema/Table selection for contact assignment
    st.markdown("### Assign Contacts to Tables")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        databases = get_databases(conn)
        
        # Find current database index
        current_db_index = 0
        if st.session_state.get('contacts_database', '') in databases:
            current_db_index = databases.index(st.session_state.contacts_database) + 1
        
        selected_db = st.selectbox(
            "Database",
            options=[""] + databases,
            index=current_db_index,
            key="contacts_db_selector",
            help="Choose a database"
        )
        
        # Update session state if changed
        if selected_db != st.session_state.get('contacts_database', ''):
            st.session_state.contacts_database = selected_db
            st.session_state.contacts_schema = ""  # Reset schema when database changes
    
    with col2:
        if selected_db:
            schemas = get_schemas(conn, selected_db)
            
            # Find current schema index
            current_schema_index = 0
            if st.session_state.get('contacts_schema', '') in schemas:
                current_schema_index = schemas.index(st.session_state.contacts_schema) + 1
            
            selected_schema = st.selectbox(
                "Schema",
                options=[""] + schemas,
                index=current_schema_index,
                key="contacts_schema_selector",
                help="Choose a schema within the selected database"
            )
            
            # Update session state if changed
            if selected_schema != st.session_state.get('contacts_schema', ''):
                st.session_state.contacts_schema = selected_schema
        else:
            selected_schema = ""
            st.selectbox("Schema", options=[""], disabled=True, key="contacts_schema_disabled", help="Select a database first")
    
    with col3:
        if st.button("🔄 Refresh Tables", help="Refresh table list from Snowflake", key="contacts_refresh_tables"):
            st.cache_data.clear()
            st.session_state['last_refresh'] = str(time.time())
            st.rerun()
    
    # Initialize selected_tables as empty DataFrame to avoid UnboundLocalError
    selected_tables = pd.DataFrame()
    
    if selected_db and selected_schema:
        st.markdown("---")
        st.markdown("### 📋 Select Tables for Contact Assignment")
        
        # Get tables from the selected schema
        refresh_key = st.session_state.get('last_refresh', '')
        tables_df = get_tables_and_views(conn, selected_db, selected_schema, refresh_key)
        
        if not tables_df.empty:
            # Add selection column and prepare for multi-select
            display_df = tables_df[['OBJECT_NAME', 'OBJECT_TYPE', 'CURRENT_DESCRIPTION']].copy()
            display_df['SELECT'] = False  # Add selection column
            
            # Add contact information for each table
            st.info("🔄 Loading contact information for tables...")
            contact_columns = {'DATA_STEWARD': [], 'TECHNICAL_SUPPORT': [], 'ACCESS_APPROVER': []}
            
            for _, row in display_df.iterrows():
                table_name = row['OBJECT_NAME']
                try:
                    # Get existing contacts for this table
                    existing_contacts = get_table_contacts(conn, selected_db, selected_schema, table_name, refresh_key)
                    
                    # Add contact info to columns
                    contact_columns['DATA_STEWARD'].append(existing_contacts.get('STEWARD', '') or 'Not assigned')
                    contact_columns['TECHNICAL_SUPPORT'].append(existing_contacts.get('SUPPORT', '') or 'Not assigned')
                    contact_columns['ACCESS_APPROVER'].append(existing_contacts.get('ACCESS_APPROVAL', '') or 'Not assigned')
                except Exception as e:
                    # If we can't get contacts for a table, show as not assigned
                    contact_columns['DATA_STEWARD'].append('Not assigned')
                    contact_columns['TECHNICAL_SUPPORT'].append('Not assigned')
                    contact_columns['ACCESS_APPROVER'].append('Not assigned')
            
            # Add contact columns to display dataframe
            display_df['DATA_STEWARD'] = contact_columns['DATA_STEWARD']
            display_df['TECHNICAL_SUPPORT'] = contact_columns['TECHNICAL_SUPPORT']
            display_df['ACCESS_APPROVER'] = contact_columns['ACCESS_APPROVER']
            
            # Reorder columns to show contacts after basic info
            display_df = display_df[['SELECT', 'OBJECT_NAME', 'OBJECT_TYPE', 'DATA_STEWARD', 'TECHNICAL_SUPPORT', 'ACCESS_APPROVER', 'CURRENT_DESCRIPTION']]
            
            # Multi-select data editor with contact information
            st.markdown("**Select tables to assign contacts to:**")
            st.caption("💡 Current contact assignments are shown for reference. New assignments will overwrite existing ones.")
            
            edited_df = st.data_editor(
                display_df,
                column_config={
                    "SELECT": st.column_config.CheckboxColumn(
                        "Select",
                        help="Check to select this table for contact assignment",
                        default=False,
                    ),
                    "OBJECT_NAME": "Table/View Name", 
                    "OBJECT_TYPE": "Type",
                    "DATA_STEWARD": st.column_config.TextColumn(
                        "🔍 Data Steward",
                        help="Current Data Steward contact",
                        width="medium"
                    ),
                    "TECHNICAL_SUPPORT": st.column_config.TextColumn(
                        "🛠️ Technical Support", 
                        help="Current Technical Support contact",
                        width="medium"
                    ),
                    "ACCESS_APPROVER": st.column_config.TextColumn(
                        "🔐 Access Approver",
                        help="Current Access Approver contact", 
                        width="medium"
                    ),
                    "CURRENT_DESCRIPTION": st.column_config.TextColumn("Description", width="large")
                },
                disabled=["OBJECT_NAME", "OBJECT_TYPE", "DATA_STEWARD", "TECHNICAL_SUPPORT", "ACCESS_APPROVER", "CURRENT_DESCRIPTION"],
                hide_index=True,
                use_container_width=True,
                key="contacts_table_selector"
            )
            
            # Get selected tables
            selected_tables = edited_df[edited_df['SELECT'] == True]
            
            if not selected_tables.empty:
                st.success(f"✅ Selected {len(selected_tables)} table(s) for contact assignment")
                
                # Show selected tables summary
                with st.expander(f"📋 **Selected Tables ({len(selected_tables)})**", expanded=False):
                    for _, row in selected_tables.iterrows():
                        st.markdown(f"• **{row['OBJECT_NAME']}** ({row['OBJECT_TYPE']})")
            
            st.markdown("---")
            st.markdown("### 📞 Bulk Contact Assignment")
            st.markdown("**Assign the same contacts to all selected tables:**")
        
        # Get available contacts
        available_contacts = get_all_contacts(conn)
        
        # Contact assignment form
        col1, col2, col3 = st.columns(3)
        
        if len(available_contacts) > 1:  # More than just "None"
            with col1:
                steward_contact = st.selectbox(
                    "🔍 Data Steward Contact",
                    options=available_contacts,
                    index=0,
                    help="Contact responsible for data accuracy and reliability",
                    key="bulk_steward_contact"
                )
            
            with col2:
                support_contact = st.selectbox(
                    "🛠️ Technical Support Contact", 
                    options=available_contacts,
                        index=0,
                        help="Contact for technical assistance and support",
                        key="bulk_support_contact"
                )
            
            with col3:
                approver_contact = st.selectbox(
                    "🔐 Access Approver Contact",
                    options=available_contacts,
                    index=0,
                    help="Contact for data access approval and authorization",
                    key="bulk_approver_contact"
                )
        else:
            st.warning("⚠️ No contacts found in your account.")
            st.info("Create contacts first using Snowflake SQL commands, then refresh this page.")
            
            # Show text inputs as fallback when no contacts are available
            with col1:
                steward_contact = st.text_input(
                    "🔍 Data Steward Contact",
                    placeholder="database.schema.contact_name",
                    help="Fully qualified contact name for data stewardship",
                    key="bulk_steward_text"
                )
        
            with col2:
                support_contact = st.text_input(
                    "🛠️ Technical Support Contact", 
                    placeholder="database.schema.contact_name",
                    help="Fully qualified contact name for technical support",
                    key="bulk_support_text"
                )
            
            with col3:
                approver_contact = st.text_input(
                    "🔐 Access Approver Contact",
                    placeholder="database.schema.contact_name", 
                    help="Fully qualified contact name for access approval",
                    key="bulk_approver_text"
                )
        
        # Generate SQL for all selected tables
        contact_assignments = []
        if steward_contact and steward_contact != "None":
            contact_assignments.append(f"STEWARD = {steward_contact}")
        if support_contact and support_contact != "None":
            contact_assignments.append(f"SUPPORT = {support_contact}")
        if approver_contact and approver_contact != "None":
            contact_assignments.append(f"ACCESS_APPROVAL = {approver_contact}")
        
        if contact_assignments and not selected_tables.empty:
                st.markdown("---")
                st.markdown("### 📄 Generated SQL Commands")
                
                # Generate SQL for all selected tables
                sql_commands = []
                sql_commands.append(f"-- Bulk contact assignment for {len(selected_tables)} table(s)")
                sql_commands.append(f"-- Contacts: {', '.join(contact_assignments)}")
                sql_commands.append("")
                
                for _, row in selected_tables.iterrows():
                    table_name = row['OBJECT_NAME']
                    full_table_name = get_fully_qualified_name(selected_db, selected_schema, table_name)
                    
                    sql_commands.append(f"-- Contact assignment for {table_name}")
                    sql_commands.append(f"ALTER TABLE {full_table_name} SET CONTACT {', '.join(contact_assignments)};")
                    sql_commands.append("")
                
                generated_sql = "\n".join(sql_commands)
                
                # Display SQL and actions
                with st.expander("📄 View Generated SQL Commands", expanded=False):
                    st.code(generated_sql, language="sql")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.download_button(
                        label="📥 Download SQL File",
                        data=generated_sql,
                        file_name=f"bulk_contact_assignment_{len(selected_tables)}_tables.sql",
                        mime="text/sql"
                    )
            
                
                with col2:
                    if st.button("🔗 Apply Contact Assignments", type="primary", help="Execute the SQL to set contacts on all selected tables"):
                        with st.spinner(f"Setting contacts for {len(selected_tables)} table(s)..."):
                            success_count = 0
                            error_count = 0
                            
                            # Execute contact assignment for each table
                            for _, row in selected_tables.iterrows():
                                table_name = row['OBJECT_NAME']
                                full_table_name = get_fully_qualified_name(selected_db, selected_schema, table_name)
                                sql_command = f"ALTER TABLE {full_table_name} SET CONTACT {', '.join(contact_assignments)};"
                                
                                try:
                                    if execute_comment_sql(conn, sql_command, 'CONTACT'):
                                        success_count += 1
                                        
                                        # Log contact history for each contact type and table
                                        if steward_contact != "None":
                                            log_contact_history(conn, selected_db, selected_schema, table_name, 
                                                              'STEWARD', 'None', steward_contact)
                                        
                                        if support_contact != "None":
                                            log_contact_history(conn, selected_db, selected_schema, table_name, 
                                                              'SUPPORT', 'None', support_contact)
                                        
                                        if approver_contact != "None":
                                            log_contact_history(conn, selected_db, selected_schema, table_name, 
                                                              'ACCESS_APPROVAL', 'None', approver_contact)
                                    else:
                                        error_count += 1
                                        st.error(f"❌ Failed to set contacts for {table_name}")
                                except Exception as e:
                                    error_count += 1
                                    st.error(f"❌ Error setting contacts for {table_name}: {str(e)}")
                            
                            # Show final results
                            if error_count == 0:
                                st.success(f"✅ Successfully set contacts for all {success_count} table(s)")
                                # st.balloons()
                            else:
                                if success_count > 0:
                                    st.warning(f"⚠️ Partially successful: {success_count} succeeded, {error_count} failed")
                                else:
                                    st.error(f"❌ All {error_count} table(s) failed. Check your permissions and table ownership.")
        else:
            if selected_tables.empty:
                st.info("👆 Select one or more tables above to assign contacts")
            else:
                st.info("👆 Select at least one contact type to assign")
    else:
        st.info("Please select a database and schema to get started.")

    # View existing contacts section
    st.markdown("---")
    st.markdown("### 👥 View Existing Contacts")
    
    try:
        # Try to show contacts (may fail due to permissions)
        show_contacts_query = "SHOW CONTACTS IN ACCOUNT LIMIT 10"
        
        if hasattr(conn, 'sql'):
            contacts_result = conn.sql(show_contacts_query).to_pandas()
        else:
            contacts_result = pd.read_sql(show_contacts_query, conn)
        
        if not contacts_result.empty:
            st.dataframe(contacts_result, use_container_width=True)
        else:
            st.info("No contacts found in your account.")
            
    except Exception as e:
        st.warning("Unable to retrieve contacts - you may need additional permissions.")
        st.markdown("""
        To view and manage contacts, you need:
        - Access to `SHOW CONTACTS IN ACCOUNT`
        - Access to `SNOWFLAKE.ACCOUNT_USAGE.CONTACTS` 
        - Appropriate contact management privileges
        
        Contact your administrator for access.
        """)