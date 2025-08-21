"""
AI and Cortex utilities for Snowflake Data Quality & Documentation App.
"""

import streamlit as st
import pandas as pd
from typing import Any, List, Optional
from .database import get_fully_qualified_name, quote_identifier

# Available LLM models for Cortex COMPLETE
AVAILABLE_MODELS = [
    'claude-4-sonnet',
    'mistral-large2', 
    'llama3-70b',
    'snowflake-arctic',
    'snowflake-llama-3.1-405b'
]


def get_available_models() -> List[str]:
    """Get list of available LLM models for Cortex COMPLETE."""
    return AVAILABLE_MODELS


def generate_table_description(conn: Any, model: str, database_name: str, schema_name: str, 
                             table_name: str, table_type: str = 'TABLE') -> Optional[str]:
    """Generate a description for a table or view using Cortex COMPLETE."""
    try:
        # Get column information for context
        columns_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = '{database_name.upper()}'
        AND TABLE_SCHEMA = '{schema_name.upper()}'
        AND TABLE_NAME = '{table_name.upper()}'
        ORDER BY ORDINAL_POSITION
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            columns_result = conn.sql(columns_query).to_pandas()
        else:  # Regular connection
            columns_result = pd.read_sql(columns_query, conn)
        
        # Build column metadata string
        column_info = []
        for _, row in columns_result.iterrows():
            col_desc = f"- {row['COLUMN_NAME']} ({row['DATA_TYPE']})"
            if row['COMMENT']:
                col_desc += f": {row['COMMENT']}"
            column_info.append(col_desc)
        
        columns_text = "\n".join(column_info) if column_info else "No columns found"
        
        # Get sample data
        sample_query = f"""
        SELECT *
        FROM {database_name}.{schema_name}.{table_name}
        LIMIT 5
        """
        
        try:
            if hasattr(conn, 'sql'):  # Snowpark session
                sample_result = conn.sql(sample_query).to_pandas()
            else:  # Regular connection
                sample_result = pd.read_sql(sample_query, conn)
            
            sample_data = sample_result.to_string(index=False, max_rows=5)
        except Exception:
            sample_data = "Unable to sample data"
        
        # Build the prompt
        prompt = f"""You are an expert data steward and have been tasked with writing descriptions for tables and columns in an enterprise data warehouse. 
Use the provided metadata and the first few rows of data to write a concise, meaningful, and business-centric description. 
This description should be helpful to both business analysts and technical analysts. 
Focus on the purpose of the data, its key contents, and any important context. 
Output only the description text. Keep the description to 150 characters or less.

---METADATA---
{table_type} Name: {table_name}
Schema: {schema_name}
Database: {database_name}
Columns:
{columns_text}

---SAMPLE DATA (LIMIT 5 ROWS)---
{sample_data}

---TASK---
Generate a description for the {table_type.lower()} named {table_name}."""
        
        # Call Cortex COMPLETE
        cortex_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            $${prompt}$$
        ) as generated_description
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            result = conn.sql(cortex_query).to_pandas()
        else:  # Regular connection
            result = pd.read_sql(cortex_query, conn)
        
        description = result.iloc[0]['GENERATED_DESCRIPTION']
        
        # Clean up the description
        description = description.strip()
        if description.startswith('"') and description.endswith('"'):
            description = description[1:-1]
        
        return description
        
    except Exception as e:
        st.error(f"Error generating table description: {str(e)}")
        return None


def generate_column_description(conn: Any, model: str, database_name: str, schema_name: str, 
                              table_name: str, column_name: str, data_type: str) -> Optional[str]:
    """Generate a description for a column using Cortex COMPLETE."""
    try:
        # Get sample data for the specific column
        fully_qualified_table = get_fully_qualified_name(database_name, schema_name, table_name)
        quoted_column = quote_identifier(column_name)
        sample_query = f"""
        SELECT {quoted_column}
        FROM {fully_qualified_table}
        SAMPLE  (10 ROWS);
        """
        
        try:
            if hasattr(conn, 'sql'):  # Snowpark session
                sample_result = conn.sql(sample_query).to_pandas()
            else:  # Regular connection
                sample_result = pd.read_sql(sample_query, conn)
            
            sample_data = sample_result.to_string(index=False, max_rows=10)
        except Exception:
            sample_data = "Unable to sample data"
        
        # Build the prompt
        prompt = f"""You are an expert data steward and have been tasked with writing descriptions for tables and columns in an enterprise data warehouse. 
Use the provided metadata and the first few rows of data to write a concise, meaningful, and business-centric description. 
This description should be helpful to both business analysts and technical analysts. 
Focus on the purpose of the data, its key contents, and any important context. 
Output only the description text. Keep the description to 100 characters or less.

---METADATA---
Column Name: {column_name}
Table Name: {table_name}
Schema: {schema_name}
Database: {database_name}
Data Type: {data_type}

---SAMPLE DATA (LIMIT 10 ROWS)---
{sample_data}

---TASK---
Generate a description for the column named {column_name}."""
        
        # Call Cortex COMPLETE
        cortex_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            $${prompt}$$
        ) as generated_description
        """
        
        if hasattr(conn, 'sql'):  # Snowpark session
            result = conn.sql(cortex_query).to_pandas()
        else:  # Regular connection
            result = pd.read_sql(cortex_query, conn)
        
        description = result.iloc[0]['GENERATED_DESCRIPTION']
        
        # Clean up the description
        description = description.strip()
        if description.startswith('"') and description.endswith('"'):
            description = description[1:-1]
        
        return description
        
    except Exception as e:
        st.error(f"Error generating column description: {str(e)}")
        return None
