"""
Configuration constants for DCS Snowflake Application
"""

# Environment detection
ENV_CONFIG = {
    'native_app': {
        'metadata_database': '',
        'metadata_schema': 'app_schema',
        'metadata_table': 'dcs_metadata_store'
    },
    'local': {
        'metadata_database': 'dcs_db',
        'metadata_schema': 'dcsazure_metadata_store',
        'metadata_table': 'dcs_metadata_store'
    }
}

# DCS API constants
DEFAULT_AZURE_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

# Batch processing constants
DEFAULT_BATCH_SIZE = 5000
MAX_BATCH_SIZE = 10000
MIN_BATCH_SIZE = 100

# UI constants
PAGE_CONFIG = {
    "page_title": "Delphix Compliance Service - Powered by Perforce",
    "page_icon": "🛡️", 
    "layout": "wide",
    "initial_sidebar_state": "expanded"
}

# Color scheme
DELPHIX_COLORS = {
    'purple': '#4C00FF',
    'dark_purple': '#3A00CC',
    'light_purple': '#6B33FF',
    'blue': '#007BFF',
    'green': '#28A745',
    'gray': '#6C757D',
    'light_gray': '#F8F9FA',
    'dark_gray': '#343A40',
    'white': '#FFFFFF'
}