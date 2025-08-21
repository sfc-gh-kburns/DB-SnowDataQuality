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
    """Complete setup of all required database objects."""
    database_name = "DB_SNOWTOOLS"
    schema_name = "PUBLIC"
    
    setup_actions = []
    
    # Check if database exists
    if not check_database_exists(conn, database_name):
        try:
            # Create database and schema
            create_db_sql = f"CREATE DATABASE IF NOT EXISTS {database_name}"
            create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}"
            
            if hasattr(conn, 'sql'):  # Snowpark session
                conn.sql(create_db_sql).collect()
                conn.sql(create_schema_sql).collect()
            else:  # Regular connection
                cursor = conn.cursor()
                cursor.execute(create_db_sql)
                cursor.execute(create_schema_sql)
            
            setup_actions.append(f"Created database {database_name}")
        except Exception as e:
            st.error(f"Error creating database: {str(e)}")
            return False
    
    # Create tracking tables
    try:
        # DATA_DESCRIPTION_HISTORY table
        history_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.DATA_DESCRIPTION_HISTORY (
            HISTORY_ID NUMBER AUTOINCREMENT PRIMARY KEY,
            DATABASE_NAME VARCHAR(255) NOT NULL,
            SCHEMA_NAME VARCHAR(255) NOT NULL,
            OBJECT_TYPE VARCHAR(50) NOT NULL,
            OBJECT_NAME VARCHAR(255) NOT NULL,
            COLUMN_NAME VARCHAR(255),
            BEFORE_DESCRIPTION TEXT,
            AFTER_DESCRIPTION TEXT,
            SQL_EXECUTED TEXT,
            UPDATED_BY VARCHAR(255) DEFAULT CURRENT_USER(),
            UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        
        # DATA_QUALITY_RESULTS table
        quality_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.DATA_QUALITY_RESULTS (
            RESULT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
            MONITOR_NAME VARCHAR(255) NOT NULL,
            DATABASE_NAME VARCHAR(255) NOT NULL,
            SCHEMA_NAME VARCHAR(255) NOT NULL,
            TABLE_NAME VARCHAR(255) NOT NULL,
            COLUMN_NAME VARCHAR(255),
            METRIC_VALUE NUMBER,
            METRIC_UNIT VARCHAR(50),
            THRESHOLD_MIN NUMBER,
            THRESHOLD_MAX NUMBER,
            STATUS VARCHAR(20),
            MEASUREMENT_TIME TIMESTAMP_LTZ,
            RECORD_INSERTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            SQL_EXECUTED TEXT
        )
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            conn.sql(history_table_sql).collect()
            conn.sql(quality_table_sql).collect()
        else:  # Regular connection
            cursor = conn.cursor()
            cursor.execute(history_table_sql)
            cursor.execute(quality_table_sql)
        
        setup_actions.append("Created tracking tables")
        
    except Exception as e:
        st.error(f"Error creating tables: {str(e)}")
        return False
    
    # Show setup messages if something was created
    # Return True - setup status is now shown in sidebar
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
