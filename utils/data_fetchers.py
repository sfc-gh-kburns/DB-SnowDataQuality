"""
Data fetching utilities for Snowflake Data Quality & Documentation App.
"""

import streamlit as st
import pandas as pd
from typing import Any, List, Dict, Optional
from .database import quote_identifier, get_fully_qualified_name


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_databases(_conn: Any) -> List[str]:
    """Get list of accessible databases."""
    try:
        query = """
        SELECT DATABASE_NAME 
        FROM INFORMATION_SCHEMA.DATABASES 
        WHERE TYPE = 'STANDARD'
        ORDER BY DATABASE_NAME
        """
        
        if hasattr(_conn, 'sql'):  # Snowpark session
            result = _conn.sql(query).to_pandas()
            databases = result['DATABASE_NAME'].tolist()
        else:  # Regular connection
            cursor = _conn.cursor()
            cursor.execute(query)
            databases = [row[0] for row in cursor.fetchall()]
        
        # Filter out system databases
        filtered_databases = [db for db in databases if db not in ['SNOWFLAKE', 'INFORMATION_SCHEMA']]
        return filtered_databases
            
    except Exception as e:
        st.error(f"Error fetching databases: {str(e)}")
        return []


@st.cache_data(ttl=300)
def get_schemas(_conn: Any, database_name: str) -> List[str]:
    """Get list of schemas in a database."""
    try:
        # Try using INFORMATION_SCHEMA first for better SiS compatibility
        info_schema_query = f"""
        SELECT SCHEMA_NAME
        FROM {quote_identifier(database_name)}.INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA')
        ORDER BY SCHEMA_NAME
        """
        
        if hasattr(_conn, 'sql'):  # Snowpark session
            result = _conn.sql(info_schema_query).to_pandas()
            schemas = result['SCHEMA_NAME'].tolist() if not result.empty else []
        else:  # Regular connection
            result = pd.read_sql(info_schema_query, _conn)
            schemas = result['SCHEMA_NAME'].tolist() if not result.empty else []
        
        if schemas:
            return schemas
            
    except Exception as e:
        # If INFORMATION_SCHEMA fails, fall back to SHOW SCHEMAS
        st.warning(f"Could not access INFORMATION_SCHEMA for database {database_name}, trying SHOW command...")
        
        try:
            quoted_database = quote_identifier(database_name)
            query = f"SHOW SCHEMAS IN DATABASE {quoted_database}"
            
            if hasattr(_conn, 'sql'):  # Snowpark session
                result = _conn.sql(query).to_pandas()
                if 'name' in result.columns:
                    schemas = result['name'].tolist()
                elif 'NAME' in result.columns:
                    schemas = result['NAME'].tolist()
                else:
                    schemas = result.iloc[:, 1].tolist() if len(result.columns) > 1 else []
            else:  # Regular connection
                cursor = _conn.cursor()
                cursor.execute(query)
                schemas = [row[1] for row in cursor.fetchall()]
            
            # Filter out system schemas
            filtered_schemas = [schema for schema in schemas if schema not in ['INFORMATION_SCHEMA']]
            return filtered_schemas
                
        except Exception as e2:
            st.error(f"Error fetching schemas: {str(e2)}")
            return []


@st.cache_data(ttl=300)
def get_tables_and_views(_conn: Any, database_name: str, schema_name: str = None, _refresh_key: str = None) -> pd.DataFrame:
    """Get tables and views with their descriptions. If schema_name is None, gets from all schemas."""
    try:
        tables_data = []
        
        # Determine schemas to process
        if schema_name:
            schemas_to_process = [schema_name]
        else:
            # Get all schemas in the database
            schemas_to_process = get_schemas(_conn, database_name)
        
        for current_schema in schemas_to_process:
            try:
                # Use INFORMATION_SCHEMA instead of SHOW commands for better SiS compatibility
                # Get both tables and views in one query using INFORMATION_SCHEMA.TABLES
                info_schema_query = f"""
                SELECT 
                    TABLE_NAME as name,
                    COMMENT as comment,
                    TABLE_TYPE
                FROM {quote_identifier(database_name)}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{current_schema.upper()}'
                  AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                ORDER BY TABLE_NAME
                """
                
                if hasattr(_conn, 'sql'):  # Snowpark session
                    info_schema_result = _conn.sql(info_schema_query).to_pandas()
                else:  # Regular connection
                    info_schema_result = pd.read_sql(info_schema_query, _conn)
                
                for _, row in info_schema_result.iterrows():
                    name = row.get('name', row.get('NAME', ''))
                    comment = row.get('comment', row.get('COMMENT', ''))
                    table_type = row.get('TABLE_TYPE', 'BASE TABLE')
                    
                    # Skip if name is empty
                    if not name:
                        continue
                    
                    # For views, check if they're secure by trying to access them
                    if table_type == 'VIEW':
                        try:
                            # Try a simple query to check if view is accessible
                            test_query = f"SELECT 1 FROM {quote_identifier(database_name)}.{quote_identifier(current_schema)}.{quote_identifier(name)} LIMIT 0"
                            if hasattr(_conn, 'sql'):
                                _conn.sql(test_query).collect()
                            else:
                                pd.read_sql(test_query, _conn)
                        except Exception:
                            # Skip inaccessible views (likely secure views)
                            continue
                    
                    table_data = {
                        'OBJECT_NAME': name,
                        'OBJECT_TYPE': table_type,
                        'CURRENT_DESCRIPTION': comment if comment else None,
                        'HAS_DESCRIPTION': 'Yes' if comment and comment.strip() else 'No'
                    }
                    
                    # Add schema column if showing multiple schemas
                    if not schema_name:
                        table_data['SCHEMA_NAME'] = current_schema
                    
                    tables_data.append(table_data)
                    
            except Exception as e:
                # If INFORMATION_SCHEMA fails, fall back to SHOW commands
                st.warning(f"Could not access INFORMATION_SCHEMA for schema {current_schema}, trying SHOW commands...")
                
                # Fallback: Get tables using SHOW TABLES
                schema_qualified = f"{quote_identifier(database_name)}.{quote_identifier(current_schema)}"
                tables_query = f"SHOW TABLES IN SCHEMA {schema_qualified}"
                try:
                    if hasattr(_conn, 'sql'):  # Snowpark session
                        tables_result = _conn.sql(tables_query).to_pandas()
                    else:  # Regular connection
                        tables_result = pd.read_sql(tables_query, _conn)
                    
                    for _, row in tables_result.iterrows():
                        name = row.get('name', row.get('NAME', ''))
                        comment = row.get('comment', row.get('COMMENT', ''))
                        
                        if name:  # Only add if name exists
                            table_data = {
                                'OBJECT_NAME': name,
                                'OBJECT_TYPE': 'BASE TABLE',
                                'CURRENT_DESCRIPTION': comment if comment else None,
                                'HAS_DESCRIPTION': 'Yes' if comment and comment.strip() else 'No'
                            }
                            
                            # Add schema column if showing multiple schemas
                            if not schema_name:
                                table_data['SCHEMA_NAME'] = current_schema
                            
                            tables_data.append(table_data)
                except Exception:
                    continue  # Skip schemas we can't access
                
                # Fallback: Get views using SHOW VIEWS
                views_query = f"SHOW VIEWS IN SCHEMA {schema_qualified}"
                try:
                    if hasattr(_conn, 'sql'):  # Snowpark session
                        views_result = _conn.sql(views_query).to_pandas()
                    else:  # Regular connection
                        views_result = pd.read_sql(views_query, _conn)
                    
                    for _, row in views_result.iterrows():
                        name = row.get('name', row.get('NAME', ''))
                        comment = row.get('comment', row.get('COMMENT', ''))
                        
                        # Skip secure views
                        is_secure = (
                            row.get('is_secure', '') or 
                            row.get('IS_SECURE', '') or
                            row.get('secure', '') or
                            row.get('SECURE', '')
                        )
                        
                        is_secure_str = str(is_secure).upper()
                        if is_secure_str in ['YES', 'TRUE', 'Y', '1']:
                            continue
                        
                        if name:  # Only add if name exists
                            view_data = {
                                'OBJECT_NAME': name,
                                'OBJECT_TYPE': 'VIEW',
                                'CURRENT_DESCRIPTION': comment if comment else None,
                                'HAS_DESCRIPTION': 'Yes' if comment and comment.strip() else 'No'
                            }
                            
                            # Add schema column if showing multiple schemas
                            if not schema_name:
                                view_data['SCHEMA_NAME'] = current_schema
                            
                            tables_data.append(view_data)
                except Exception:
                    continue  # Skip schemas we can't access
        
        if tables_data:
            df = pd.DataFrame(tables_data)
            if schema_name:
                return df.sort_values('OBJECT_NAME')
            else:
                return df.sort_values(['SCHEMA_NAME', 'OBJECT_NAME'])
        else:
            columns = ['OBJECT_NAME', 'OBJECT_TYPE', 'CURRENT_DESCRIPTION', 'HAS_DESCRIPTION']
            if not schema_name:
                columns.insert(0, 'SCHEMA_NAME')
            return pd.DataFrame(columns=columns)
            
    except Exception as e:
        st.error(f"Error fetching tables/views: {str(e)}")
        columns = ['OBJECT_NAME', 'OBJECT_TYPE', 'CURRENT_DESCRIPTION', 'HAS_DESCRIPTION']
        if not schema_name:
            columns.insert(0, 'SCHEMA_NAME')
        return pd.DataFrame(columns=columns)


@st.cache_data(ttl=300)
def get_columns(_conn: Any, database_name: str, schema_name: str, table_name: str, _refresh_key: str = None) -> pd.DataFrame:
    """Get columns for a specific table/view."""
    try:
        # Try using INFORMATION_SCHEMA first for better SiS compatibility
        info_schema_query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            COMMENT,
            ORDINAL_POSITION
        FROM {quote_identifier(database_name)}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name.upper()}'
          AND TABLE_NAME = '{table_name.upper()}'
        ORDER BY ORDINAL_POSITION
        """
        
        if hasattr(_conn, 'sql'):  # Snowpark session
            result = _conn.sql(info_schema_query).to_pandas()
        else:  # Regular connection
            result = pd.read_sql(info_schema_query, _conn)
        
        columns_data = []
        
        for _, row in result.iterrows():
            column_name = row.get('COLUMN_NAME', '')
            data_type = row.get('DATA_TYPE', '')
            comment = row.get('COMMENT', '')
            
            # Handle null/empty comments
            if pd.isna(comment) or comment == 'null' or comment == 'NULL' or comment == '':
                comment = None
            
            columns_data.append({
                'COLUMN_NAME': column_name,
                'DATA_TYPE': data_type,
                'CURRENT_DESCRIPTION': comment,
                'HAS_DESCRIPTION': 'Yes' if comment and str(comment).strip() else 'No'
            })
        
        if columns_data:
            return pd.DataFrame(columns_data)
            
    except Exception as e:
        # If INFORMATION_SCHEMA fails, fall back to DESC TABLE
        st.warning(f"Could not access INFORMATION_SCHEMA for {table_name}, trying DESC TABLE...")
        
        try:
            fully_qualified_table = get_fully_qualified_name(database_name, schema_name, table_name)
            desc_query = f"DESC TABLE {fully_qualified_table}"
            
            if hasattr(_conn, 'sql'):  # Snowpark session
                result = _conn.sql(desc_query).to_pandas()
            else:  # Regular connection
                result = pd.read_sql(desc_query, _conn)
            
            # Debug: Print column information to understand the structure
            st.info(f"DESC TABLE returned {len(result.columns)} columns: {list(result.columns)}")
            st.info(f"Data shape: {result.shape}")
            
            columns_data = []
            
            for _, row in result.iterrows():
                # Try different possible column names that DESC TABLE might return
                column_name = (
                    row.get('name') or row.get('NAME') or 
                    row.get('column_name') or row.get('COLUMN_NAME') or
                    row.get('Field') or row.get('FIELD') or ''
                )
                
                data_type = (
                    row.get('type') or row.get('TYPE') or 
                    row.get('data_type') or row.get('DATA_TYPE') or
                    row.get('Type') or ''
                )
                
                comment = (
                    row.get('comment') or row.get('COMMENT') or 
                    row.get('Comment') or row.get('description') or
                    row.get('DESCRIPTION') or ''
                )
                
                # Handle null/empty comments
                if pd.isna(comment) or comment == 'null' or comment == 'NULL' or comment == '':
                    comment = None
                
                # Only add if we have at least a column name
                if column_name:
                    columns_data.append({
                        'COLUMN_NAME': column_name,
                        'DATA_TYPE': data_type,
                        'CURRENT_DESCRIPTION': comment,
                        'HAS_DESCRIPTION': 'Yes' if comment and str(comment).strip() else 'No'
                    })
            
            if columns_data:
                return pd.DataFrame(columns_data)
            else:
                st.warning(f"No column data could be extracted from DESC TABLE result for {table_name}")
                
        except Exception as e2:
            st.error(f"Error fetching columns for {table_name}: {str(e2)}")
    
    # Return empty DataFrame with correct structure if all methods fail
    return pd.DataFrame(columns=['COLUMN_NAME', 'DATA_TYPE', 'CURRENT_DESCRIPTION', 'HAS_DESCRIPTION'])


@st.cache_data(ttl=300)
def get_all_contacts(_conn: Any) -> List[str]:
    """Get all contacts in the account with their fully qualified names."""
    try:
        # First try SHOW CONTACTS command
        query = "SHOW CONTACTS IN ACCOUNT"
        
        if hasattr(_conn, 'sql'):  # Snowpark session
            result = _conn.sql(query).to_pandas()
        else:  # Regular connection
            result = pd.read_sql(query, _conn)
        
        contact_options = ["None"]
        
        for _, contact in result.iterrows():
            # Handle different column names from SHOW CONTACTS
            contact_name = (contact.get('name') or contact.get('NAME') or 
                          contact.get('contact_name') or contact.get('CONTACT_NAME'))
            
            db_name = (contact.get('database_name') or contact.get('DATABASE_NAME') or 
                     contact.get('contact_database') or contact.get('CONTACT_DATABASE'))
            
            schema_name = (contact.get('schema_name') or contact.get('SCHEMA_NAME') or
                         contact.get('contact_schema') or contact.get('CONTACT_SCHEMA'))
            
            if contact_name and db_name and schema_name:
                # Create fully qualified name
                full_path = f'{db_name}.{schema_name}."{contact_name}"'
                contact_options.append(full_path)
        
        return contact_options
        
    except Exception as e:
        # If SHOW CONTACTS fails, return empty list with helpful message
        st.warning(f"Unable to retrieve contacts: {str(e)}")
        st.info("You may need permissions to view contacts in the account.")
        return ["None"]


@st.cache_data(ttl=300)
def get_table_contacts(_conn: Any, database: str, schema: str, table: str, _refresh_key: str = None) -> Dict[str, str]:
    """Get existing contacts assigned to a table."""
    try:
        # Query to get table contacts from ACCOUNT_USAGE.CONTACT_REFERENCES
        query = f"""
        SELECT 
            CONTACT_NAME,
            CONTACT_DATABASE,
            CONTACT_SCHEMA,
            CONTACT_PURPOSE
        FROM SNOWFLAKE.ACCOUNT_USAGE.CONTACT_REFERENCES 
        WHERE OBJECT_DATABASE = '{database}'
          AND OBJECT_SCHEMA = '{schema}'
          AND OBJECT_NAME = '{table}'
          AND OBJECT_DELETED IS NULL
        ORDER BY CONTACT_PURPOSE
        """
        
        if hasattr(_conn, 'sql'):  # Snowpark session
            result = _conn.sql(query).to_pandas()
        else:  # Regular connection
            result = pd.read_sql(query, _conn)
        
        contacts = {}
        
        for _, row in result.iterrows():
            contact_name = row.get('CONTACT_NAME', '')
            contact_db = row.get('CONTACT_DATABASE', '')
            contact_schema = row.get('CONTACT_SCHEMA', '')
            purpose = row.get('CONTACT_PURPOSE', 'GENERAL')
            
            if contact_name and contact_db and contact_schema:
                full_contact_path = f'{contact_db}.{contact_schema}."{contact_name}"'
                contacts[purpose] = full_contact_path
        
        return contacts
        
    except Exception as e:
        # If query fails, return empty dict
        st.warning(f"Unable to retrieve table contacts: {str(e)}")
        return {}
