"""
Home page for Snowflake Data Quality & Documentation App.
"""

import streamlit as st
from typing import Any
from utils.kpi_utils import get_kpi_data


def show_home_page(conn: Any):
    """Display the home page with modern KPI dashboard."""
    
    st.markdown("# 📊 Data Governance Dashboard")
    st.markdown("*Insights into your data quality and documentation coverage*")
    
    # Loading state
    with st.spinner("Loading dashboard metrics..."):
        kpi_data = get_kpi_data(conn)
    
    if kpi_data['error']:
        st.warning(f"Some metrics may be incomplete: {kpi_data['error']}")
    
    # Show refresh confirmation if requested
    if st.session_state.get('kpi_refresh_requested', False):
        st.success("✅ KPIs refreshed with latest data from Snowflake!")
        st.session_state['kpi_refresh_requested'] = False
    
    # Modern KPI Cards with refresh option
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("### 📈 Key Performance Indicators")
    with col2:
        if st.button("🔄 Refresh KPIs", help="Refresh all KPI data from Snowflake", key="refresh_kpis"):
            # Clear the KPI cache to force fresh data
            st.cache_data.clear()
            st.session_state['kpi_refresh_requested'] = True
            st.rerun()
    
    # Row 1: Data Inventory
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div style="background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); padding: 1.5rem; border-radius: 10px; color: white; margin-bottom: 1rem;">
            <h3 style="margin: 0; font-size: 2.5rem; font-weight: bold;">{:,}</h3>
            <p style="margin: 0; font-size: 1.1rem; opacity: 0.9;">Databases</p>
            <p style="margin: 0; font-size: 0.9rem; opacity: 0.7;">Accessible to your role</p>
        </div>
        """.format(kpi_data['databases']), unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="background: linear-gradient(90deg, #f093fb 0%, #f5576c 100%); padding: 1.5rem; border-radius: 10px; color: white; margin-bottom: 1rem;">
            <h3 style="margin: 0; font-size: 2.5rem; font-weight: bold;">{:,}</h3>
            <p style="margin: 0; font-size: 1.1rem; opacity: 0.9;">Schemas</p>
            <p style="margin: 0; font-size: 0.9rem; opacity: 0.7;">Across all databases</p>
        </div>
        """.format(kpi_data['schemas']), unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div style="background: linear-gradient(90deg, #4facfe 0%, #00f2fe 100%); padding: 1.5rem; border-radius: 10px; color: white; margin-bottom: 1rem;">
            <h3 style="margin: 0; font-size: 2.5rem; font-weight: bold;">{:,}</h3>
            <p style="margin: 0; font-size: 1.1rem; opacity: 0.9;">Tables & Views</p>
            <p style="margin: 0; font-size: 0.9rem; opacity: 0.7;">Total data objects</p>
        </div>
        """.format(kpi_data['tables']), unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Row 2: Documentation Coverage
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="background: linear-gradient(90deg, #43e97b 0%, #38f9d7 100%); padding: 1.5rem; border-radius: 10px; color: white; margin-bottom: 1rem;">
            <h3 style="margin: 0; font-size: 2.5rem; font-weight: bold;">{:,}</h3>
            <p style="margin: 0; font-size: 1.1rem; opacity: 0.9;">Tables with Descriptions</p>
            <p style="margin: 0; font-size: 0.9rem; opacity: 0.7;">{}% coverage rate</p>
        </div>
        """.format(kpi_data['tables_with_descriptions'], kpi_data['description_percentage']), unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="background: linear-gradient(90deg, #fa709a 0%, #fee140 100%); padding: 1.5rem; border-radius: 10px; color: white; margin-bottom: 1rem;">
            <h3 style="margin: 0; font-size: 2.5rem; font-weight: bold;">{}%</h3>
            <p style="margin: 0; font-size: 1.1rem; opacity: 0.9;">Documentation Coverage</p>
            <p style="margin: 0; font-size: 0.9rem; opacity: 0.7;">{:,} of {:,} documented</p>
        </div>
        """.format(kpi_data['description_percentage'], kpi_data['tables_with_descriptions'], kpi_data['tables']), unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Row 3: Quality & Governance
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style="background: linear-gradient(90deg, #a8edea 0%, #fed6e3 100%); padding: 1.5rem; border-radius: 10px; color: #333; margin-bottom: 1rem;">
            <h3 style="margin: 0; font-size: 2.5rem; font-weight: bold;">{:,}</h3>
            <p style="margin: 0; font-size: 1.1rem; opacity: 0.8;">Data Quality Metrics</p>
            <p style="margin: 0; font-size: 0.9rem; opacity: 0.6;">Active DMF monitors on tables</p>
        </div>
        """.format(kpi_data['dmf_count']), unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="background: linear-gradient(90deg, #d299c2 0%, #fef9d7 100%); padding: 1.5rem; border-radius: 10px; color: #333; margin-bottom: 1rem;">
            <h3 style="margin: 0; font-size: 2.5rem; font-weight: bold;">{:,}</h3>
            <p style="margin: 0; font-size: 1.1rem; opacity: 0.8;">Defined Contacts</p>
            <p style="margin: 0; font-size: 0.9rem; opacity: 0.6;">For governance & support</p>
        </div>
        """.format(kpi_data['contacts_count']), unsafe_allow_html=True)
