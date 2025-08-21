"""
Database connection and utility functions for Snowflake Data Quality & Documentation App.
"""

import streamlit as st
import pandas as pd
import snowflake.connector
import tomli
from snowflake.snowpark.context import get_active_session
import os
from typing import Any, List, Dict, Optional


def get_snowflake_connection():
    """Get Snowflake connection - either Snowpark session or regular connector."""
    # First try to get active session (for Streamlit in Snowflake)
    print("Getting active session")
    try:
        session = get_active_session()
        if session:
            # Note: ALTER SESSION commands are not supported in SiS due to security restrictions
            # Execute identification query
            try:
                result = session.sql("SELECT 'SNOWDQ_SIS_LAUNCH' AS launch_type").collect()
            except Exception as e:
                print(f"Failed to execute SiS identification query: {str(e)}")
            return session
    except Exception:
        # If get_active_session fails, continue to local connection
        pass
            
    # Try local connection
    try:
        with open(os.path.expanduser('~/.snowflake/connections.toml'), 'rb') as f:
            config = tomli.load(f)

        # Get the default connection name
        default_conn = config.get('kb_demo')
        # print(f'default_conn: {default_conn}')
        if not default_conn:
            print("No default connection specified in connections.toml")
            return None

        conn = snowflake.connector.connect(**default_conn)
        
        # Set query tag for OSS Streamlit
        try:
            cursor = conn.cursor()
            cursor.execute("ALTER SESSION SET QUERY_TAG = 'APP: SNOWDQ_OSS_STREAMLIT'")
        except Exception as e:
            print(f"Failed to set query tag for OSS: {str(e)}")
        
        # Execute identification query
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 'SNOWDQ_OSS_STREAMLIT_LAUNCH' AS launch_type")
            result = cursor.fetchone()
        except Exception as e:
            print(f"Failed to execute OSS identification query: {str(e)}")
        
        return conn
        
    except Exception as e:
        print(f"Failed to connect to Snowflake using local config: {str(e)}")
        return None


def quote_identifier(identifier: str) -> str:
    """Quote a Snowflake identifier if it contains spaces or special characters."""
    if identifier is None or identifier == "":
        return identifier
    
    # If identifier already has quotes, return as-is
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier
    
    # Quote if it contains spaces, special characters, or is a reserved word
    if (' ' in identifier or 
        any(char in identifier for char in ['-', '.', '+', '/', '*', '(', ')', '[', ']', '{', '}']) or
        identifier.upper() in ['TABLE', 'COLUMN', 'VIEW', 'DATABASE', 'SCHEMA', 'SELECT', 'FROM', 'WHERE']):
        return f'"{identifier}"'
    
    return identifier


def get_fully_qualified_name(database: str, schema: str, table: str) -> str:
    """Create a fully qualified table name with proper quoting."""
    return f"{quote_identifier(database)}.{quote_identifier(schema)}.{quote_identifier(table)}"


def get_current_user(_conn: Any) -> str:
    """Get the current Snowflake user."""
    try:
        query = "SELECT CURRENT_USER() as current_user"
        
        if hasattr(_conn, 'sql'):  # Snowpark session
            result = _conn.sql(query).to_pandas()
            return result.iloc[0]['CURRENT_USER']
        else:  # Regular connection
            cursor = _conn.cursor()
            cursor.execute(query)
            return cursor.fetchone()[0]
            
    except Exception:
        return "Unknown"


def execute_comment_sql(_conn: Any, sql_command: str, object_type: str = None) -> bool:
    """Execute a COMMENT ON statement."""
    try:
        if hasattr(_conn, 'sql'):  # Snowpark session
            _conn.sql(sql_command).collect()
        else:  # Regular connection
            cursor = _conn.cursor()
            cursor.execute(sql_command)
        return True
        
    except Exception as e:
        st.error(f"Error executing comment SQL: {str(e)}")
        return False
