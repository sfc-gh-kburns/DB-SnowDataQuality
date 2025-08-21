"""
Database setup utilities for Snowflake Data Quality & Documentation App.
"""

import streamlit as st
from typing import Any


def check_database_exists(conn: Any, database_name: str = "DB_SNOWTOOLS") -> bool:
    """Check if the specified database exists."""
    try:
        query = f"""
        SELECT COUNT(*) as db_count 
        FROM INFORMATION_SCHEMA.DATABASES 
        WHERE DATABASE_NAME = '{database_name.upper()}'
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            result = conn.sql(query).collect()
            return result[0]['DB_COUNT'] > 0
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] > 0
            
    except Exception:
        return False


def setup_database_objects(conn: Any) -> bool:
    """Verify that all required database objects exist (database and tables should be created by setup script)."""
    database_name = "DB_SNOWTOOLS"
    schema_name = "PUBLIC"
    
    # Check if database exists (should already be created by setup script)
    if not check_database_exists(conn, database_name):
        st.error(f"Database {database_name} not found. Please run the Setup_Script.sql first.")
        return False
    
    # Verify tracking tables exist (should already be created by setup script)
    try:
        # Check if required tables exist
        tables_check_sql = f"""
        SELECT COUNT(*) as table_count
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema_name}' 
        AND TABLE_NAME IN ('DATA_DESCRIPTION_HISTORY', 'DATA_QUALITY_RESULTS')
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            result = conn.sql(tables_check_sql).collect()
            table_count = result[0]['TABLE_COUNT']
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute(tables_check_sql)
            result = cursor.fetchone()
            table_count = result[0]
        
        if table_count < 2:
            st.error(f"Required tracking tables not found in {database_name}.{schema_name}. Please run the Setup_Script.sql first.")
            return False
        
    except Exception as e:
        st.error(f"Error verifying tables: {str(e)}")
        return False
    
    # All required objects exist
    return True


def initialize_session_state():
    """Initialize all session state variables to prevent tab jumping on first interaction."""
    
    # Data Descriptions tab session state
    if 'desc_database' not in st.session_state:
        st.session_state.desc_database = ""
    if 'desc_schema' not in st.session_state:
        st.session_state.desc_schema = ""
    
    # Data Quality tab session state  
    if 'dmf_database' not in st.session_state:
        st.session_state.dmf_database = ""
    if 'dmf_schema' not in st.session_state:
        st.session_state.dmf_schema = ""
    if 'dmf_table' not in st.session_state:
        st.session_state.dmf_table = ""
    
    # Data Contacts tab session state
    if 'contacts_database' not in st.session_state:
        st.session_state.contacts_database = ""
    if 'contacts_schema' not in st.session_state:
        st.session_state.contacts_schema = ""
    if 'contacts_table' not in st.session_state:
        st.session_state.contacts_table = ""
    
    # General session state
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = ""
    
    # Tab state management to prevent jumping
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = "Home"


def log_dmf_history(conn, database: str, schema: str, table_name: str, dmf_type: str, 
                   column_name: str = None, action: str = "ADDED", updated_by: str = "Streamlit App"):
    """Log DMF configuration changes to the description history table."""
    try:
        # Use the DATA_DESCRIPTION_HISTORY table to track DMF configuration changes
        # Set OBJECT_TYPE to indicate this is a DMF configuration change
        object_type = f"DMF_{dmf_type}"
        if column_name:
            object_type += f"_COLUMN"
        
        # Create a description of the change
        if action == "ADDED":
            description = f"Added {dmf_type} data quality metric"
            if column_name:
                description += f" to column {column_name}"
        else:
            description = f"{action} {dmf_type} data quality metric"
            if column_name:
                description += f" on column {column_name}"
        
        history_insert = f"""
        INSERT INTO DB_SNOWTOOLS.PUBLIC.DATA_DESCRIPTION_HISTORY (
            DATABASE_NAME,
            SCHEMA_NAME,
            OBJECT_NAME,
            COLUMN_NAME,
            OBJECT_TYPE,
            BEFORE_DESCRIPTION,
            AFTER_DESCRIPTION,
            UPDATED_BY
        ) VALUES (
            '{database}',
            '{schema}',
            '{table_name}',
            {f"'{column_name}'" if column_name else 'NULL'},
            '{object_type}',
            NULL,
            '{description}',
            '{updated_by}'
        )
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            conn.sql(history_insert).collect()
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute(history_insert)
            
        return True
        
    except Exception as e:
        st.warning(f"Could not log DMF history: {str(e)}")
        return False


def log_contact_history(conn, database: str, schema: str, table_name: str, contact_type: str,
                       old_contact: str, new_contact: str, updated_by: str = "Streamlit App"):
    """Log contact assignment changes to the history table."""
    try:
        # For now, we'll log contact changes in the description history table with a special object type
        history_insert = f"""
        INSERT INTO DB_SNOWTOOLS.PUBLIC.DATA_DESCRIPTION_HISTORY (
            DATABASE_NAME,
            SCHEMA_NAME,
            OBJECT_NAME,
            OBJECT_TYPE,
            BEFORE_DESCRIPTION,
            AFTER_DESCRIPTION,
            UPDATED_BY
        ) VALUES (
            '{database}',
            '{schema}',
            '{table_name}',
            'CONTACT_{contact_type}',
            {f"'{old_contact}'" if old_contact and old_contact != 'None' else 'NULL'},
            {f"'{new_contact}'" if new_contact and new_contact != 'None' else 'NULL'},
            '{updated_by}'
        )
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            conn.sql(history_insert).collect()
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute(history_insert)
            
        return True
        
    except Exception as e:
        st.warning(f"Could not log contact history: {str(e)}")
        return False
