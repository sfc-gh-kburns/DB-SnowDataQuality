"""
KPI and metrics utilities for Snowflake Data Quality & Documentation App.
"""

import streamlit as st
import pandas as pd
from typing import Any, Dict
from .data_fetchers import get_databases, get_schemas


@st.cache_data(ttl=300)
def get_kpi_data(_conn: Any) -> Dict[str, Any]:
    """Get comprehensive KPI data for the dashboard."""
    kpis = {
        'databases': 0,
        'schemas': 0,
        'tables': 0,
        'tables_with_descriptions': 0,
        'description_percentage': 0,
        'dmf_count': 0,
        'tables_with_dmfs': 0,
        'contacts_count': 0,
        'tables_with_contacts': 0,
        'error': None
    }
    
    try:
        # Get database count
        databases = get_databases(_conn)
        kpis['databases'] = len(databases)
        
        # Get accurate table counts from ACCOUNT_USAGE (much more efficient than sampling)
        try:
            table_count_query = """
            SELECT COUNT(*) as total_tables, 
                   COUNT(comment) as tables_with_descriptions 
            FROM snowflake.account_usage.tables 
            WHERE table_catalog NOT IN ('SNOWFLAKE') 
              AND table_catalog IS NOT NULL
              AND table_type ILIKE '%table%'  
              AND owner_role_type <> 'APPLICATION'
            """
            
            if hasattr(_conn, 'sql'):
                result = _conn.sql(table_count_query).to_pandas()
                kpis['tables'] = int(result.iloc[0]['TOTAL_TABLES']) if not result.empty else 0
                kpis['tables_with_descriptions'] = int(result.iloc[0]['TABLES_WITH_DESCRIPTIONS']) if not result.empty else 0
            else:
                result = pd.read_sql(table_count_query, _conn)
                kpis['tables'] = int(result.iloc[0, 0]) if not result.empty else 0
                kpis['tables_with_descriptions'] = int(result.iloc[0, 1]) if not result.empty else 0
                
        except Exception as e:
            # Fallback to estimation if ACCOUNT_USAGE query fails
            kpis['tables'] = 0
            kpis['tables_with_descriptions'] = 0
        
        # Get schema count estimate (sample a few databases for performance)
        sample_databases = databases[:3] if databases else []
        total_schemas = 0
        
        for db in sample_databases:
            try:
                schemas = get_schemas(_conn, db)
                total_schemas += len(schemas)
            except:
                continue
        
        # Extrapolate schema count based on sample
        if sample_databases and len(databases) > 0:
            db_ratio = len(databases) / len(sample_databases)
            kpis['schemas'] = int(total_schemas * db_ratio)
        else:
            kpis['schemas'] = total_schemas
        
        # Calculate description percentage
        if kpis['tables'] > 0:
            kpis['description_percentage'] = round((kpis['tables_with_descriptions'] / kpis['tables']) * 100, 1)
        
        # Try to get DMF and contact counts (these might fail due to permissions)
        try:
            # Check for any DMF monitoring results
            dmf_query = "SELECT COUNT(DISTINCT TABLE_DATABASE || TABLE_SCHEMA || METRIC_NAME) as DMF_COUNT FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS"
            if hasattr(_conn, 'sql'):
                result = _conn.sql(dmf_query).to_pandas()
                kpis['dmf_count'] = int(result.iloc[0]['DMF_COUNT']) if not result.empty else 0
            else:
                result = pd.read_sql(dmf_query, _conn)
                kpis['dmf_count'] = int(result.iloc[0, 0]) if not result.empty else 0
        except:
            kpis['dmf_count'] = 0
        
        try:
            # Check for contacts
            contacts_query = "SHOW CONTACTS IN ACCOUNT"
            if hasattr(_conn, 'sql'):
                result = _conn.sql(contacts_query).to_pandas()
                kpis['contacts_count'] = len(result) if not result.empty else 0
            else:
                result = pd.read_sql(contacts_query, _conn)
                kpis['contacts_count'] = len(result) if not result.empty else 0
        except:
            kpis['contacts_count'] = 0
            
        # Estimate tables with DMFs and contacts (simplified for performance)
        kpis['tables_with_dmfs'] = min(kpis['dmf_count'], kpis['tables'])
        kpis['tables_with_contacts'] = min(kpis['contacts_count'], kpis['tables'])
            
    except Exception as e:
        kpis['error'] = str(e)
    
    return kpis
