"""
CSS styles and styling components for Streamlit app.
"""

import streamlit as st


def apply_main_styles():
    """Apply the main CSS styles to the Streamlit app."""
    st.markdown("""
<style>
    /* Import modern fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Root variables for consistent theming */
    :root {
        --primary-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        --secondary-gradient: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        --accent-gradient: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        --surface-color: #ffffff;
        --surface-secondary: #f8fafc;
        --text-primary: #1e293b;
        --text-secondary: #64748b;
        --text-muted: #94a3b8;
        --border-color: #e2e8f0;
        --border-light: #f1f5f9;
        --success-color: #10b981;
        --warning-color: #f59e0b;
        --error-color: #ef4444;
        --info-color: #3b82f6;
    }
    
    /* Global styles */
    .stApp {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        background: var(--surface-color);
    }
    
    /* Header styling */
    .main-header {
        background: #ffffff;
        color: black;
        margin: -1rem -1rem 2rem -1rem;
        text-align: center;
        padding: 2rem;
        border-bottom: 1px solid var(--border-light);
    }
    
    .main-header h1 {
        font-size: 3rem;
        font-weight: 700;
        background: var(--primary-gradient);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin: 0;
        text-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
    
    .main-header p {
        font-size: 1.1rem;
        margin: 0.5rem 0 0 0;
        color: var(--text-secondary);
        font-weight: 400;
    }
    
    /* Button styling */
    .stButton > button {
        font-family: 'Inter', sans-serif;
        font-weight: 500;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        transition: all 0.3s ease;
        border: 2px solid transparent;
        position: relative;
    }
    
    /* Primary (active) navigation buttons */
    .stButton > button[kind="primary"] {
        background: var(--primary-gradient) !important;
        color: white !important;
        border: 2px solid #5a67d8 !important;
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3) !important;
        transform: translateY(-2px) !important;
    }
    
    /* Secondary (inactive) navigation buttons */
    .stButton > button[kind="secondary"] {
        background: var(--surface-secondary) !important;
        color: var(--text-primary) !important;
        border: 2px solid var(--border-color) !important;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1) !important;
    }
    
    /* Hover effects */
    .stButton > button:hover {
        transform: translateY(-1px) !important;
        box-shadow: 0 6px 16px rgba(0,0,0,0.15) !important;
    }
    
    .stButton > button[kind="primary"]:hover {
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4) !important;
        transform: translateY(-3px) !important;
    }
    
    /* Spacing improvements */
    .main .block-container {
        padding-top: 1rem !important;
        padding-bottom: 1rem !important;
    }
    
    /* Better spacing for metrics */
    .metric-container {
        margin-top: 0.5rem !important;
        margin-bottom: 0.75rem !important;
    }
    
    /* Improved data editor spacing */
    .stDataFrame {
        padding: 0 0.25rem !important;
    }
    
    /* KPI card styling */
    .kpi-card {
        background: var(--surface-color);
        border: 1px solid var(--border-light);
        border-radius: 12px;
        padding: 1.5rem;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
        transition: all 0.3s ease;
        height: 100%;
    }
    
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.1);
    }
    
    .kpi-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: var(--text-primary);
        margin: 0;
        line-height: 1;
    }
    
    .kpi-label {
        font-size: 0.9rem;
        color: var(--text-secondary);
        margin: 0.5rem 0 0 0;
        font-weight: 500;
    }
    
    .kpi-change {
        font-size: 0.8rem;
        margin: 0.25rem 0 0 0;
        font-weight: 500;
    }
    
    .kpi-change.positive {
        color: var(--success-color);
    }
    
    .kpi-change.negative {
        color: var(--error-color);
    }
    
    /* Feature card styling */
    .feature-card {
        background: var(--surface-color);
        border: 1px solid var(--border-light);
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
        transition: all 0.3s ease;
        height: 100%;
    }
    
    .feature-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.1);
        border-color: var(--border-color);
    }
    
    .feature-icon {
        font-size: 2.5rem;
        margin-bottom: 1rem;
        display: block;
    }
    
    .feature-title {
        font-size: 1.25rem;
        font-weight: 600;
        color: var(--text-primary);
        margin: 0 0 0.5rem 0;
    }
    
    .feature-description {
        color: var(--text-secondary);
        font-size: 0.9rem;
        line-height: 1.5;
        margin: 0;
    }
    
    /* Alert styling improvements */
    .stAlert {
        border-radius: 8px;
        border: none;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
    }
    
    /* Sidebar improvements */
    .css-1d391kg {
        padding-top: 1rem;
    }
    
    /* Expander styling */
    .streamlit-expanderHeader {
        background: var(--surface-secondary);
        border-radius: 8px;
        padding: 0.75rem 1rem;
        border: 1px solid var(--border-light);
    }
    
    /* Data editor improvements */
    .stDataEditor {
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
    }
    
    /* Progress bar styling */
    .stProgress > div > div > div > div {
        background: var(--primary-gradient);
    }
    
    /* Selectbox and input styling */
    .stSelectbox > div > div {
        border-radius: 8px;
    }
    
    .stTextInput > div > div {
        border-radius: 8px;
    }
    
    /* Tab styling improvements */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 0.5rem 1rem;
    }
</style>
""", unsafe_allow_html=True)


def apply_additional_styles():
    """Apply additional CSS for better spacing and UX."""
    st.markdown("""
    <style>
    /* Add bottom margin to prevent buttons from being at the very bottom */
    .main .block-container {
        padding-bottom: 5rem !important;
    }
    
    /* Improve button spacing */
    .stButton > button {
        margin-bottom: 0.5rem;
    }
    
    /* Better spacing for expanders */
    .streamlit-expanderHeader {
        margin-bottom: 0.5rem;
    }
    
    /* Add some breathing room for data editors */
    .stDataFrame {
        margin-bottom: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)
