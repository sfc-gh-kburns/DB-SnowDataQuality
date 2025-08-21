"""
Snowflake Data Quality & Documentation App - Modular Version
A comprehensive Streamlit application for managing data documentation and quality monitoring in Snowflake.

Optimized for Streamlit in Snowflake (SiS) deployment with fallback support for local development.

SiS Compatibility Features:
- Uses INFORMATION_SCHEMA queries instead of SHOW commands for better permission compatibility
- Implements fallback mechanisms for environments with restricted SHOW command access
- Handles Owner's Rights Model limitations in SiS environments
"""

import streamlit as st

# Import components and utilities
from components.styles import apply_main_styles, apply_additional_styles
from utils.database import get_snowflake_connection, get_current_user
from utils.setup import setup_database_objects, initialize_session_state

# Import page modules
from pages.home import show_home_page
from pages.data_descriptions import show_data_descriptions_page
from pages.data_quality import show_data_quality_page
from pages.data_contacts import show_data_contacts_page
from pages.history import show_history_page


# ========================================================================================
# PAGE CONFIG AND STYLING
# ========================================================================================

st.set_page_config(
    page_title="Snowflake Data Quality & Documentation",
    page_icon="🔺",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Apply CSS styles
apply_main_styles()


def main():
    """Main application function."""
    
    # Apply additional CSS for better spacing and UX
    apply_additional_styles()
    
    # Initialize ALL session state variables at the very beginning to prevent tab jumping
    initialize_session_state()
    
    # Compact Header
    st.markdown("""
    <div style="text-align: center; padding: 0.2rem 0; margin-bottom: .2rem;">
        <h2 style="margin: 0; color: #1f77b4; font-size: 1.8rem;">📘 Snowflake Data Quality & Documentation</h2>
        <p style="margin: 0; color: #666; font-size: 0.9rem;">AI-powered data governance and quality monitoring</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Get Snowflake connection
    conn = get_snowflake_connection()
    if not conn:
        st.error("Failed to connect to Snowflake. Please check your connection parameters.")
        st.stop()
    
    # Setup database objects (only shows messages if creation is needed)
    if 'setup_complete' not in st.session_state:
        setup_success = setup_database_objects(conn)
        if setup_success:
            st.session_state.setup_complete = True
    else:
        setup_success = True
    
    if not setup_success:
        st.error("Database setup failed. Please check permissions and try again.")
        st.stop()
    
    # Display current user in sidebar
    with st.sidebar:
        st.markdown("### 🔺 Connection Manager")
        
        try:
            current_user = get_current_user(conn)
            st.success(f"Connected as: **{current_user}**")
        except:
            st.warning("Status: Connected")
    
        # System Information (moved from Home tab)
        st.markdown("---")
        with st.expander("📊 System Information", expanded=False):
            st.markdown("**Connection Details**")
            
            if hasattr(conn, 'sql'):
                st.success("Using Snowpark session (SiS)")
                st.caption("App running within Snowflake's managed environment")
            else:
                st.info("Using standard connector")
                st.caption("Local development mode")
            
            try:
                # Get Snowflake system info
                info_query = "SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_VERSION()"
                if hasattr(conn, 'sql'):
                    result = conn.sql(info_query).to_pandas()
                else:
                    import pandas as pd
                    result = pd.read_sql(info_query, conn)
                
                st.markdown("**Environment Details:**")
                st.write(f"• **Account:** {result.iloc[0, 0]}")
                st.write(f"• **Region:** {result.iloc[0, 1]}")
                st.write(f"• **Version:** {result.iloc[0, 2]}")
                    
            except Exception as e:
                st.warning(f"Could not retrieve system info: {str(e)}")
        
        # Platform Overview (moved from Home tab)
        with st.expander("🏗️ Platform Overview", expanded=False):
            st.markdown("""
            **Snowflake Data Quality & Documentation Platform**
            
            A comprehensive solution for:
            • AI-powered data documentation
            • Automated quality monitoring  
            • Contact management & governance
            • Historical tracking & reporting
            
            Built with Streamlit and Snowflake Cortex
            """)
            
            # Quick feature overview
            st.markdown("**Key Features:**")
            st.write("📝 **Data Descriptions** - AI-generated documentation")
            st.write("🔍 **Data Quality** - DMF setup and monitoring")
            st.write("👥 **Data Contacts** - Governance assignments")
            st.write("📈 **History** - Change tracking and reports")
        
        # Database Setup Status (moved from main area)
        if 'setup_complete' in st.session_state and st.session_state.setup_complete:
            with st.expander("🔧 Database Setup Status", expanded=False):
                st.success("✅ All required database objects are ready")
                st.info("DB_SNOWTOOLS database and tracking tables configured")
                st.caption("Setup completed successfully during initialization")
        
        # Quick Actions (moved from Home tab)
        with st.expander("🚀 Quick Actions", expanded=False):
            st.markdown("**Navigate directly to key features:**")
            
            if st.button("📝 Generate Descriptions", use_container_width=True, type="primary", key="sidebar_desc"):
                st.session_state.active_tab = "Data Descriptions"
                st.rerun()
                
            if st.button("🔍 Setup Quality Checks", use_container_width=True, type="secondary", key="sidebar_quality"):
                st.session_state.active_tab = "Data Quality"
                st.rerun()
                
            if st.button("👥 Manage Contacts", use_container_width=True, type="secondary", key="sidebar_contacts"):
                st.session_state.active_tab = "Data Contacts"
                st.rerun()
                
            if st.button("📈 View History", use_container_width=True, type="secondary", key="sidebar_history"):
                st.session_state.active_tab = "History"
                st.rerun()
            
            st.markdown("---")
            st.caption("💡 **Tip:** Use these buttons for quick navigation between features")
    
    # Navigation using radio buttons (more stable than tabs)
    tab_options = [
        "🏠 Home", 
        "📝 Data Descriptions", 
        "🔍 Data Quality", 
        "👥 Data Contacts",
        "📈 History"
    ]
    
    st.markdown("---")

    # Map display names to keys
    tab_keys = ["Home", "Data Descriptions", "Data Quality", "Data Contacts", "History"]
    
    # Find current index
    try:
        current_index = tab_keys.index(st.session_state.active_tab)
    except ValueError:
        current_index = 0
        st.session_state.active_tab = "Home"
    
    # Navigation radio buttons
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        if st.button("🏠 Home", use_container_width=True, type="primary" if st.session_state.active_tab == "Home" else "secondary"):
            st.session_state.active_tab = "Home"
            st.rerun()
    
    with col2:
        if st.button("📝 Data Descriptions", use_container_width=True, type="primary" if st.session_state.active_tab == "Data Descriptions" else "secondary"):
            st.session_state.active_tab = "Data Descriptions"
            st.rerun()
    
    with col3:
        if st.button("🔍 Data Quality", use_container_width=True, type="primary" if st.session_state.active_tab == "Data Quality" else "secondary"):
            st.session_state.active_tab = "Data Quality"
            st.rerun()
    
    with col4:
        if st.button("👥 Data Contacts", use_container_width=True, type="primary" if st.session_state.active_tab == "Data Contacts" else "secondary"):
            st.session_state.active_tab = "Data Contacts"
            st.rerun()
    
    with col5:
        if st.button("📈 History", use_container_width=True, type="primary" if st.session_state.active_tab == "History" else "secondary"):
            st.session_state.active_tab = "History"
            st.rerun()
    
    # Show content based on active tab
    if st.session_state.active_tab == "Home":
        show_home_page(conn)
    elif st.session_state.active_tab == "Data Descriptions":
        show_data_descriptions_page(conn)
    elif st.session_state.active_tab == "Data Quality":
        show_data_quality_page(conn)
    elif st.session_state.active_tab == "Data Contacts":
        show_data_contacts_page(conn)
    elif st.session_state.active_tab == "History":
        show_history_page(conn)


if __name__ == "__main__":
    main()
