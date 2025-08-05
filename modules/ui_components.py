"""
UI Components Module - DattaAble Template Implementation

This module contains Material UI-inspired components matching the DattaAble dashboard template.
"""

import streamlit as st
import pandas as pd
from typing import Dict, Any, List, Optional
from .snowflake_ops import list_available_databases, list_available_schemas
from .metadata_store import load_algorithms_from_database, get_discovery_metadata
# Import constants - handle both local and Snowflake environments
try:
    from config.constants import DELPHIX_COLORS
except ImportError:
    # Fallback for Snowflake environment
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


def apply_custom_css():
    """Apply DattaAble Material Design CSS styling."""
    st.html("""
    <style>
        /* Import Material Design Icons and Fonts */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
        @import url('https://fonts.googleapis.com/icon?family=Material+Icons');
        @import url('https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css');
        
        /* DattaAble Color Variables */
        :root {
            --primary: #04a9f5;
            --primary-light: #e3f4fd;
            --primary-dark: #038bcd;
            --secondary: #6c757d;
            --success: #2ed8b6;
            --info: #04a9f5;
            --warning: #ffb64d;
            --danger: #ff5722;
            --light: #f4f6f9;
            --dark: #212529;
            --white: #ffffff;
            --border: #e6ebf1;
            --text-muted: #8492a6;
            --sidebar-width: 280px;
            --topbar-height: 70px;
            --shadow-sm: 0 2px 6px rgba(0,0,0,0.05);
            --shadow-md: 0 4px 12px rgba(0,0,0,0.1);
            --shadow-lg: 0 8px 24px rgba(0,0,0,0.15);
            --border-radius: 8px;
            --border-radius-lg: 12px;
        }
        
        /* Global Reset and Base Styles */
        .stApp {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--light);
            color: var(--dark);
        }
        
        /* Hide Streamlit Elements */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        .stDeployButton {display: none;}
        
        /* Add proper margins to main content */
        .main .block-container {
            padding: 24px 96px;
            max-width: none;
        }
        
        /* Style default sidebar */
        .css-1d391kg {
            background: var(--white) !important;
            border-right: 1px solid var(--border) !important;
            box-shadow: var(--shadow-sm) !important;
        }
        
        /* Main Content Layout */
        .main-content-wrapper {
            background: var(--light);
            min-height: 100vh;
            padding: 24px 96px;
        }
        
        /* Top Bar */
        .dattaable-topbar {
            background: var(--white);
            border-bottom: 1px solid var(--border);
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 20px 24px;
            box-shadow: var(--shadow-sm);
            margin: 0 -96px 24px -96px;
            border-radius: var(--border-radius-lg);
        }
        
        .topbar-left {
            display: flex;
            align-items: center;
            gap: 16px;
        }
        
        .topbar-title {
            font-size: 20px;
            font-weight: 600;
            color: var(--dark);
            margin: 0;
        }
        
        .topbar-right {
            display: flex;
            align-items: center;
            gap: 16px;
        }
        
        .topbar-user {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 8px 12px;
            border-radius: var(--border-radius);
            background: var(--light);
            color: var(--text-muted);
            font-size: 14px;
        }
        
        .topbar-user i {
            color: var(--primary);
        }
        
        /* Main Content Container */
        .stApp > .main {
            background: var(--light);
        }
        
        /* Page Header */
        .page-header {
            background: var(--white);
            padding: 24px;
            margin-bottom: 24px;
            border-radius: var(--border-radius-lg);
            box-shadow: var(--shadow-sm);
            border: 1px solid var(--border);
        }
        
        .page-title {
            font-size: 24px;
            font-weight: 600;
            color: var(--dark);
            margin: 0 0 8px 0;
            display: flex;
            align-items: center;
            gap: 12px;
        }
        
        .page-title i {
            color: var(--primary);
        }
        
        .page-subtitle {
            color: var(--text-muted);
            font-size: 14px;
            margin: 0;
        }
        
        /* Cards */
        .material-card {
            background: var(--white);
            border-radius: var(--border-radius-lg);
            box-shadow: var(--shadow-sm);
            border: 1px solid var(--border);
            overflow: hidden;
            transition: all 0.2s ease;
        }
        
        .material-card:hover {
            box-shadow: var(--shadow-md);
            transform: translateY(-2px);
        }
        
        .card-header {
            padding: 20px 24px;
            border-bottom: 1px solid var(--border);
            background: var(--white);
        }
        
        .card-title {
            font-size: 16px;
            font-weight: 600;
            color: var(--dark);
            margin: 0;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .card-title i {
            color: var(--primary);
        }
        
        .card-body {
            padding: 24px;
        }
        
        /* Feature Cards */
        .feature-card {
            background: var(--white);
            border-radius: var(--border-radius-lg);
            padding: 24px;
            box-shadow: var(--shadow-sm);
            border: 1px solid var(--border);
            transition: all 0.2s ease;
            text-align: center;
        }
        
        .feature-card:hover {
            box-shadow: var(--shadow-md);
            transform: translateY(-2px);
        }
        
        .feature-icon {
            width: 60px;
            height: 60px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0 auto 16px;
            font-size: 24px;
            color: var(--white);
            background: var(--primary);
        }
        
        .feature-title {
            font-size: 18px;
            font-weight: 600;
            color: var(--dark);
            margin: 0 0 8px 0;
        }
        
        .feature-description {
            color: var(--text-muted);
            font-size: 14px;
            line-height: 1.5;
        }
        
        /* Metric Cards */
        .metric-card {
            background: var(--white);
            border-radius: var(--border-radius-lg);
            padding: 24px;
            box-shadow: var(--shadow-sm);
            border: 1px solid var(--border);
            text-align: center;
            transition: all 0.2s ease;
        }
        
        .metric-card:hover {
            box-shadow: var(--shadow-md);
            transform: translateY(-2px);
        }
        
        .metric-value {
            font-size: 32px;
            font-weight: 700;
            color: var(--primary);
            margin: 8px 0;
        }
        
        .metric-label {
            color: var(--text-muted);
            font-size: 14px;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .metric-icon {
            width: 48px;
            height: 48px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0 auto 16px;
            font-size: 20px;
            color: var(--white);
        }
        
        .metric-icon.primary { background: var(--primary); }
        .metric-icon.success { background: var(--success); }
        .metric-icon.warning { background: var(--warning); }
        .metric-icon.danger { background: var(--danger); }
        
        /* Status Badges */
        .status-badge {
            display: inline-flex;
            align-items: center;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .status-badge.success {
            background: rgba(46, 216, 182, 0.1);
            color: var(--success);
        }
        
        .status-badge.warning {
            background: rgba(255, 182, 77, 0.1);
            color: var(--warning);
        }
        
        .status-badge.danger {
            background: rgba(255, 87, 34, 0.1);
            color: var(--danger);
        }
        
        .status-badge.info {
            background: rgba(4, 169, 245, 0.1);
            color: var(--info);
        }
        
        /* Buttons */
        .stButton > button {
            background: var(--primary);
            color: var(--white);
            border: none;
            border-radius: var(--border-radius);
            font-weight: 500;
            font-size: 14px;
            padding: 10px 20px;
            transition: all 0.2s ease;
            box-shadow: var(--shadow-sm);
        }
        
        .stButton > button:hover {
            background: var(--primary-dark);
            box-shadow: var(--shadow-md);
            transform: translateY(-1px);
        }
        
        /* Form Elements */
        .stSelectbox > div > div {
            background: var(--white);
            border: 1px solid var(--border);
            border-radius: var(--border-radius);
            font-size: 14px;
        }
        
        .stTextInput > div > div > input {
            background: var(--white);
            border: 1px solid var(--border);
            border-radius: var(--border-radius);
            font-size: 14px;
        }
        
        /* Data Tables */
        .stDataFrame {
            border: 1px solid var(--border);
            border-radius: var(--border-radius-lg);
            overflow: hidden;
            box-shadow: var(--shadow-sm);
        }
        
        /* Progress Bars */
        .stProgress > div > div > div > div {
            background: var(--primary);
            border-radius: 4px;
        }
        
        /* Alerts */
        .stAlert {
            border-radius: var(--border-radius);
            border: none;
            box-shadow: var(--shadow-sm);
        }
        
        /* Responsive Design */
        @media (max-width: 768px) {
            .dattaable-sidebar {
                transform: translateX(-100%);
                transition: transform 0.3s ease;
            }
            
            .dattaable-main {
                margin-left: 0;
            }
            
            .topbar-title {
                font-size: 18px;
            }
            
            .page-title {
                font-size: 20px;
            }
            
            /* Reduce margins on mobile */
            .main .block-container {
                padding: 16px 24px;
            }
            
            .main-content-wrapper {
                padding: 16px 24px;
            }
            
            .dattaable-topbar {
                margin: 0 -24px 16px -24px;
            }
        }
        
        /* Tablet view adjustments */
        @media (max-width: 1024px) and (min-width: 769px) {
            .main .block-container {
                padding: 20px 48px;
            }
            
            .main-content-wrapper {
                padding: 20px 48px;
            }
            
            .dattaable-topbar {
                margin: 0 -48px 20px -48px;
            }
        }
    </style>
    """)


def create_dattaable_layout():
    """Create the main DattaAble layout structure."""
    
    # Initialize navigation state
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 'discovery'
    if 'current_subpage' not in st.session_state:
        st.session_state.current_subpage = None
    
    # Create sidebar navigation
    with st.sidebar:
        st.html("""
        <div style="text-align: center; padding: 1rem 0; border-bottom: 1px solid #e9ecef; margin-bottom: 1rem;">
            <h2 style="color: #04a9f5; margin: 0; font-weight: 600;">
                <i class="fas fa-shield-alt"></i> DCS Platform
            </h2>
        </div>
        """)
        
        # Navigation items
        nav_items = [
            ('discovery', 'Discovery', 'fas fa-search'),
            ('masking', 'Masking', 'fas fa-mask'),
            ('monitoring', 'Monitoring', 'fas fa-chart-line'),
            ('configuration', 'Configuration', 'fas fa-cog'),
            ('settings', 'Settings', 'fas fa-sliders-h')
        ]
        
        for page_key, title, icon in nav_items:
            is_active = st.session_state.current_page == page_key
            button_type = "primary" if is_active else "secondary"
            
            if st.button(f"{title}", 
                        key=f"nav_{page_key}", 
                        type=button_type, 
                        use_container_width=True):
                st.session_state.current_page = page_key
                st.session_state.current_subpage = None
                st.rerun()
            
            # Add masking submenu
            if page_key == 'masking' and is_active:
                st.html('<div style="margin-left: 1rem; border-left: 2px solid #e9ecef; padding-left: 1rem;">')
                
                if st.button("Mask & Deliver", 
                           key="nav_mask_deliver",
                           type="primary" if st.session_state.current_subpage == 'mask_deliver' else "secondary",
                           use_container_width=True):
                    st.session_state.current_subpage = 'mask_deliver'
                    st.rerun()
                
                if st.button("In-Place Masking", 
                           key="nav_in_place",
                           type="primary" if st.session_state.current_subpage == 'in_place' else "secondary",
                           use_container_width=True):
                    st.session_state.current_subpage = 'in_place'
                    st.rerun()
                
                st.html('</div>')
    
    # Create main content area with top bar
    st.html("""
    <div class="dattaable-topbar">
        <div class="topbar-left">
        </div>
        <div class="topbar-center" style="display: flex; flex-direction: column; align-items: center; text-align: center;">
            <h1 class="topbar-title" style="margin: 0;">Delphix Compliance Service</h1>
            <span style="font-size: 14px; color: var(--text-muted); margin-top: 2px;">In Snowflake</span>
        </div>
        <div class="topbar-right">
            <div class="topbar-user">
                <i class="fas fa-user-circle"></i>
                <span>Snowflake User</span>
            </div>
        </div>
    </div>
    """)
    
    # Debug: Show current page (commented out for production)
    # st.write(f"DEBUG: Current page: {st.session_state.current_page}")
    # st.write(f"DEBUG: Current subpage: {st.session_state.current_subpage}")
    
    # Return the main content container
    return st.container()


def create_page_header(title: str, subtitle: str = "", icon: str = ""):
    """Create a DattaAble-style page header."""
    
    icon_html = f'<i class="{icon}"></i>' if icon else ""
    subtitle_html = f'<p class="page-subtitle">{subtitle}</p>' if subtitle else ""
    
    header_html = f"""
    <div class="page-header">
        <h1 class="page-title">
            {icon_html}
            {title}
        </h1>
        {subtitle_html}
    </div>
    """
    
    st.html(header_html)


def create_material_card(title: str = "", icon: str = ""):
    """Create a Material Design card with optional title."""
    
    if title:
        icon_html = f'<i class="{icon}"></i>' if icon else ""
        return f"""
        <div class="material-card">
            <div class="card-header">
                <h3 class="card-title">
                    {icon_html}
                    {title}
                </h3>
            </div>
            <div class="card-body">
        """
    else:
        return '<div class="material-card"><div class="card-body">'


def create_feature_card(title: str, description: str, icon: str, card_type: str = "primary"):
    """Create a professional feature highlight card."""
    
    # Check if icon is an image URL or base64 data
    if icon.startswith('data:image') or icon.startswith('http') or icon.endswith(('.png', '.jpg', '.jpeg', '.svg', '.gif')):
        icon_html = f'<img src="{icon}" alt="{title} icon" style="width: 24px; height: 24px; filter: brightness(0) invert(1);" />'
    else:
        icon_html = f'<i class="{icon}"></i>'
    
    card_html = f"""
    <div class="feature-card">
        <div class="feature-icon {card_type}">
            {icon_html}
        </div>
        <h3 class="feature-title">{title}</h3>
        <p class="feature-description">{description}</p>
    </div>
    """
    
    st.html(card_html)


def create_metric_card(title: str, value: str, icon: str, card_type: str = "primary"):
    """Create a metric display card."""
    
    card_html = f"""
    <div class="metric-card">
        <div class="metric-icon {card_type}">
            <i class="{icon}"></i>
        </div>
        <div class="metric-value">{value}</div>
        <div class="metric-label">{title}</div>
    </div>
    """
    
    st.html(card_html)


def create_status_badge(status: str, text: str = None):
    """Create a status badge."""
    
    display_text = text or status.title()
    
    status_classes = {
        'success': 'success',
        'completed': 'success',
        'error': 'danger',
        'failed': 'danger',
        'warning': 'warning',
        'pending': 'warning',
        'info': 'info',
        'running': 'info',
        'started': 'info'
    }
    
    badge_class = status_classes.get(status.lower(), 'info')
    
    return f'<span class="status-badge {badge_class}">{display_text}</span>'


def create_professional_container(content_func, title: str = "", icon: str = ""):
    """Create a professional container with optional title and icon."""
    
    card_start = create_material_card(title, icon)
    st.html(card_start)
    content_func()
    st.html('</div></div>')


# Navigation helper functions
def get_current_page():
    """Get the current active page."""
    return st.session_state.get('current_page', 'discovery')


def get_current_subpage():
    """Get the current active subpage."""
    return st.session_state.get('current_subpage', None)


# Import existing functions from the original implementation
def source_target_inputs(session, input_type: str = "source"):
    """Create source or target database/schema/table input widgets."""
    prefix = input_type.title()
    key_prefix = input_type.lower()
    
    st.subheader(f"{prefix} Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Database selection
        databases = list_available_databases(session)
        if databases:
            selected_db = st.selectbox(
                f"{prefix} Database",
                databases,
                key=f"{key_prefix}_database",
                help=f"Select the {input_type} database"
            )
        else:
            st.error("No databases available")
            return None, None, None
    
    with col2:
        # Schema selection
        if selected_db:
            schemas = list_available_schemas(session, selected_db)
            if schemas:
                selected_schema = st.selectbox(
                    f"{prefix} Schema",
                    schemas,
                    key=f"{key_prefix}_schema",
                    help=f"Select the {input_type} schema"
                )
            else:
                st.error(f"No schemas found in {selected_db}")
                return selected_db, None, None
        else:
            selected_schema = None
    
    # For source, just return database and schema (no table selection)
    # For target, show info message
    if selected_db and selected_schema and input_type == "target":
        st.info("Target tables will be created automatically based on source table names")
    
    return selected_db, selected_schema, None


def display_discovery_results(discovery_df: pd.DataFrame, database: str, schema: str):
    """Display discovery results in a formatted table with editing capabilities."""
    
    if discovery_df.empty:
        st.warning("No discovery results to display")
        return discovery_df
    
    st.subheader("🔍 Discovery Results")
    
    # Get available algorithms for dropdowns
    session = st.session_state.get('snowflake_session')
    available_algorithms = load_algorithms_from_database(session) if session else []
    
    # Add "None" option for no masking
    algorithm_options = [""] + available_algorithms
    
    # Group by table for better organization
    tables = discovery_df['IDENTIFIED_TABLE'].unique()
    
    for table_name in tables:
        with st.expander(f"📋 {table_name}", expanded=True):
            table_df = discovery_df[discovery_df['IDENTIFIED_TABLE'] == table_name].copy()
            
            # Create editable interface
            edited_df = table_df.copy()
            
            for idx, row in table_df.iterrows():
                col1, col2, col3, col4 = st.columns([3, 2, 2, 3])
                
                with col1:
                    st.text(f"🏷️ {row['IDENTIFIED_COLUMN']}")
                    st.caption(f"Type: {row['IDENTIFIED_COLUMN_TYPE']}")
                
                with col2:
                    if pd.notna(row['PROFILED_ALGORITHM']):
                        st.text(f"📊 {row['PROFILED_ALGORITHM']}")
                        confidence = row.get('CONFIDENCE_SCORE', 0)
                        if confidence > 0:
                            st.caption(f"Confidence: {confidence:.1%}")
                    else:
                        st.text("No classification")
                
                with col3:
                    # Algorithm selection dropdown
                    current_algorithm = row.get('ASSIGNED_ALGORITHM', '')
                    if pd.isna(current_algorithm):
                        current_algorithm = ''
                    
                    try:
                        current_index = algorithm_options.index(current_algorithm) if current_algorithm in algorithm_options else 0
                    except ValueError:
                        current_index = 0
                    
                    new_algorithm = st.selectbox(
                        "Mask Algorithm",
                        algorithm_options,
                        index=current_index,
                        key=f"algo_{table_name}_{row['IDENTIFIED_COLUMN']}",
                        label_visibility="collapsed"
                    )
                    
                    # Update the algorithm if changed
                    if new_algorithm != current_algorithm:
                        edited_df.loc[idx, 'ASSIGNED_ALGORITHM'] = new_algorithm
                
                with col4:
                    if new_algorithm and new_algorithm != '':
                        st.success("✅ Will be masked")
                    else:
                        st.info("ℹ️ No masking")
    
    return edited_df



def create_progress_tracker(operation: str, current: int, total: int, details: str = ""):
    """Create a progress tracking display."""
    
    progress = current / max(total, 1)
    
    st.subheader(f"⏳ {operation} Progress")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.progress(progress)
        if details:
            st.caption(details)
    
    with col2:
        st.metric("Progress", f"{current}/{total}")
        if total > 0:
            st.caption(f"{progress:.1%} complete")


def display_operation_results(results: List[Dict[str, Any]], operation_type: str):
    """Display results from discovery or masking operations."""
    
    if not results:
        st.info("No results to display")
        return
    
    st.subheader(f"📋 {operation_type} Results")
    
    # Separate successful and failed operations
    successful = [r for r in results if r.get('success', False)]
    failed = [r for r in results if not r.get('success', False)]
    
    # Summary metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Operations", len(results))
    
    with col2:
        st.metric("Successful", len(successful), delta=len(successful))
    
    with col3:
        st.metric("Failed", len(failed), delta=-len(failed) if failed else 0)
    
    # Detailed results
    if successful:
        with st.expander(f"✅ Successful {operation_type} ({len(successful)})", expanded=True):
            for result in successful:
                table_name = result.get('table', 'Unknown')
                
                if operation_type == "Discovery":
                    sensitive_cols = result.get('sensitive_columns', 0)
                    total_cols = result.get('columns_analyzed', 0)
                    st.success(f"🔍 **{table_name}**: {sensitive_cols}/{total_cols} sensitive columns found")
                
                elif operation_type == "Masking":
                    rows_processed = result.get('rows_processed', 0)
                    st.success(f"🎭 **{table_name}**: {rows_processed:,} rows masked successfully")
    
    if failed:
        with st.expander(f"❌ Failed {operation_type} ({len(failed)})", expanded=True):
            for result in failed:
                table_name = result.get('table', 'Unknown')
                error = result.get('error', 'Unknown error')
                st.error(f"💥 **{table_name}**: {error}")


def test_external_access(api_client):
    """Test external access configuration with detailed feedback."""
    
    st.subheader("🔗 External Access Test")
    
    if st.button("Test API Connectivity", type="primary", use_container_width=True):
        with st.spinner("Testing external access..."):
            try:
                # Test Azure AD authentication
                st.info("1️⃣ Testing Azure AD authentication...")
                api_client.get_azure_ad_token()
                st.success("✅ Azure AD authentication successful")
                
                # Test DCS API connectivity (simple test with minimal data)
                st.info("2️⃣ Testing DCS API connectivity...")
                test_data = {"test_column": ["test_value_1", "test_value_2"]}
                api_client.profile_data_raw(test_data)
                st.success("✅ DCS API connectivity successful")
                
                st.balloons()
                st.success("🎉 All external access tests passed!")
                
            except Exception as e:
                st.error(f"❌ External access test failed: {str(e)}")
                
                # Provide specific guidance based on error
                error_str = str(e).lower()
                if "failed to resolve" in error_str:
                    st.error("🔧 **Fix**: Configure Network Rules for external domains")
                elif "authentication failed" in error_str:
                    st.error("🔧 **Fix**: Check Azure AD credentials")
                elif "external access integration" in error_str:
                    st.error("🔧 **Fix**: Create and bind External Access Integration")
                
                st.info("📖 See Setup Instructions in the sidebar for complete configuration steps")


def display_existing_discovery_results(session, database, schema, selected_tables=None):
    """Display existing discovery results in a clean table format with filtering, sorting, and editable algorithm assignments."""
    from .metadata_store import get_existing_discovery_results, get_active_algorithms, batch_update_assigned_algorithms
    
    # Get existing discovery results filtered by selected tables
    original_df = get_existing_discovery_results(session, database, schema, selected_tables)
    
    if original_df.empty:
        st.info("🔍 **No discovery results found.** Start discovery to capture sensitive fields.")
        return
    
    st.subheader("🔍 Existing Discovery Results")
    st.write(f"Found **{len(original_df)}** columns with discovery results for `{database}.{schema}`")
    
    # Get active algorithms for dropdowns
    active_algorithms = get_active_algorithms(session)
    
    # Initialize session state for algorithm changes
    if 'algorithm_changes' not in st.session_state:
        st.session_state.algorithm_changes = {}
    
    # Initialize session state for filters, sorting, and pagination
    if 'discovery_filters' not in st.session_state:
        st.session_state.discovery_filters = {
            'table_name': '',
            'column_name': '',
            'column_type': '',
            'discovery_algorithm': '',
            'assigned_algorithm': ''
        }
    
    if 'discovery_sort' not in st.session_state:
        st.session_state.discovery_sort = {
            'column': 'Table Name',
            'ascending': True
        }
    
    if 'discovery_page' not in st.session_state:
        st.session_state.discovery_page = 1
    
    # Reset page to 1 when filters change
    page_size = 15
    
    # Ensure discovery_filters is properly initialized
    if not isinstance(st.session_state.discovery_filters, dict):
        st.session_state.discovery_filters = {
            'table_name': '',
            'column_name': '',
            'column_type': '',
            'discovery_algorithm': '',
            'assigned_algorithm': ''
        }
    
    
    # Apply filters
    filtered_df = original_df.copy()
    
    # Case-insensitive filtering
    if st.session_state.discovery_filters['table_name']:
        filtered_df = filtered_df[filtered_df['Table Name'].str.contains(st.session_state.discovery_filters['table_name'], case=False, na=False)]
    
    if st.session_state.discovery_filters['column_name']:
        filtered_df = filtered_df[filtered_df['Column Name'].str.contains(st.session_state.discovery_filters['column_name'], case=False, na=False)]
    
    if st.session_state.discovery_filters['column_type']:
        filtered_df = filtered_df[filtered_df['Column Type'].str.contains(st.session_state.discovery_filters['column_type'], case=False, na=False)]
    
    if st.session_state.discovery_filters['discovery_algorithm']:
        filtered_df = filtered_df[filtered_df['Discovery Algorithm'].fillna('').str.contains(st.session_state.discovery_filters['discovery_algorithm'], case=False, na=False)]
    
    if st.session_state.discovery_filters['assigned_algorithm']:
        filtered_df = filtered_df[filtered_df['Assigned Algorithm'].fillna('').str.contains(st.session_state.discovery_filters['assigned_algorithm'], case=False, na=False)]
    
    # Apply default sorting by Table Name, then Column Name
    if not filtered_df.empty:
        filtered_df = filtered_df.sort_values(
            by=['Table Name', 'Column Name'], 
            ascending=[True, True],
            na_position='last'
        )
    
    # Reset index after filtering and sorting
    filtered_df = filtered_df.reset_index(drop=True)
    
    # Calculate pagination
    total_results = len(filtered_df)
    total_pages = (total_results + page_size - 1) // page_size  # Ceiling division
    current_page = st.session_state.discovery_page
    
    # Ensure current page is valid
    if current_page > total_pages:
        st.session_state.discovery_page = 1
        current_page = 1
    
    # Show filtered results count
    if len(filtered_df) != len(original_df):
        st.write(f"Showing **{len(filtered_df)}** of **{len(original_df)}** results")
    
    if filtered_df.empty:
        st.warning("🔍 No results match the current filters.")
        return
    
    # Add pagination controls at the top
    if total_pages > 1:
        col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
        
        with col1:
            if st.button("⏮️", disabled=(current_page == 1)):
                st.session_state.discovery_page = 1
                st.rerun()
        
        with col2:
            if st.button("⬅️", disabled=(current_page == 1)):
                st.session_state.discovery_page = current_page - 1
                st.rerun()
        
        with col3:
            st.write(f"**Page {current_page} of {total_pages}** ({total_results} total results)")
        
        with col4:
            if st.button("➡️", disabled=(current_page == total_pages)):
                st.session_state.discovery_page = current_page + 1
                st.rerun()
        
        with col5:
            if st.button("⏭️", disabled=(current_page == total_pages)):
                st.session_state.discovery_page = total_pages
                st.rerun()
    
    # Add custom CSS for table styling
    st.html("""
    <style>
        .discovery-table {
            width: 100%;
            border-collapse: collapse;
            font-family: 'Inter', sans-serif;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        .discovery-table th {
            background: #f8f9fa;
            color: #495057;
            font-weight: 600;
            text-align: left;
            padding: 8px 6px;
            border-bottom: 2px solid #dee2e6;
            border-right: 1px solid #dee2e6;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .discovery-table td {
            padding: 6px;
            border-bottom: 1px solid #f1f3f4;
            border-right: 1px solid #f1f3f4;
            vertical-align: middle;
        }
        
        .discovery-results-container {
            border: 1px solid #dee2e6;
            border-radius: 8px;
            margin: 0;
            background: white;
            padding: 8px;
        }
        
        /* Make selectboxes in discovery results smaller */
        .discovery-results-container .stSelectbox > div > div {
            font-size: 14px !important;
            min-height: 32px !important;
            padding: 6px 10px !important;
        }
        
        .discovery-results-container .stSelectbox select {
            font-size: 14px !important;
        }
        
        /* Style the remove algorithm X buttons */
        .discovery-results-container .stButton > button {
            height: 32px !important;
            min-height: 32px !important;
            padding: 6px 8px !important;
            font-size: 14px !important;
            background-color: #dc3545 !important;
            color: white !important;
            border: 1px solid #dc3545 !important;
            border-radius: 4px !important;
        }
        
        .discovery-results-container .stButton > button:hover {
            background-color: #c82333 !important;
            border-color: #bd2130 !important;
        }
        
        .discovery-table tr:hover {
            background-color: #f8f9fa;
        }
        
        .discovery-table th:last-child,
        .discovery-table td:last-child {
            border-right: none;
        }
        
        /* Column widths */
        .col-table { width: 15%; }
        .col-column { width: 20%; }
        .col-type { width: 12%; }
        .col-length { width: 8%; }
        .col-discovery { width: 15%; }
        .col-confidence { width: 10%; }
        .col-assigned { width: 15%; }
        .col-actions { width: 5%; }
        
        /* Confidence score colors based on ranges */
        .confidence-high {
            color: #28a745;
            font-weight: 500;
        }
        
        .confidence-medium {
            color: #b8860b;
            font-weight: 500;
        }
        
        .confidence-low {
            color: #000000;
            font-weight: 500;
        }
        
        .algorithm-name {
            color: #495057;
            font-weight: 500;
        }
        
        .clear-btn {
            background: none;
            border: none;
            color: #dc3545;
            cursor: pointer;
            font-size: 14px;
            padding: 2px 4px;
            border-radius: 3px;
            transition: background-color 0.2s;
        }
        
        .clear-btn:hover {
            background-color: #f8d7da;
        }
        
        /* Filter section styling - positioned below headers */
        .stTextInput > div > div > input {
            font-size: 12px !important;
            padding: 6px 8px !important;
            height: 28px !important;
            border: 1px solid #dee2e6 !important;
            border-radius: 4px !important;
            background: #f8f9fa !important;
        }
        
        .stTextInput > div > div > input:focus {
            border-color: #04a9f5 !important;
            background: white !important;
            box-shadow: 0 0 0 2px rgba(4, 169, 245, 0.1) !important;
        }
        
        .stSelectbox > div > div {
            font-size: 12px !important;
        }
        
        /* Improve table row styling */
        .discovery-table-row {
            border-bottom: 1px solid #f1f3f4;
            padding: 4px 0;
        }
        
        .discovery-table-row:hover {
            background-color: #f8f9fa;
        }
        
        /* Style the clear buttons */
        button[kind="secondary"] {
            background: rgba(220, 53, 69, 0.1) !important;
            border: 1px solid rgba(220, 53, 69, 0.2) !important;
            color: #dc3545 !important;
            font-size: 14px !important;
            font-weight: 600 !important;
            border-radius: 50% !important;
            min-height: 24px !important;
            max-height: 24px !important;
            width: 24px !important;
            padding: 0 !important;
            margin: 0 !important;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            cursor: pointer !important;
        }
        
        button[kind="secondary"]:hover {
            background: rgba(220, 53, 69, 0.2) !important;
            border-color: rgba(220, 53, 69, 0.4) !important;
            color: #b71c1c !important;
        }
        
        /* Improve filter input responsiveness */
        .stTextInput input {
            transition: all 0.2s ease !important;
        }
        
        .stTextInput input:focus {
            border-color: #4C00FF !important;
            box-shadow: 0 0 0 2px rgba(76, 0, 255, 0.1) !important;
        }
    </style>
    """)
    
    # Create scrollable container for the table
    st.html('<div class="discovery-results-container">')
    
    # Debug info (remove in production)
    # st.write(f"DEBUG: Algorithm changes: {st.session_state.algorithm_changes}")
    
    # Create the table headers
    st.html("""
    <table class="discovery-table">
        <thead>
            <tr>
                <th class="col-table">TABLE NAME</th>
                <th class="col-column">COLUMN NAME</th>
                <th class="col-type">COLUMN TYPE</th>
                <th class="col-length">LENGTH</th>
                <th class="col-discovery">DISCOVERY ALGORITHM</th>
                <th class="col-confidence">CONFIDENCE</th>
                <th class="col-assigned">ASSIGNED ALGORITHM</th>
                <th class="col-actions">ACTIONS</th>
            </tr>
        </thead>
    </table>
    """)
    
    # Add filter inputs below headers
    filter_col1, filter_col2, filter_col3, filter_col4, filter_col5, filter_col6, filter_col7, filter_col8 = st.columns([0.15, 0.20, 0.12, 0.08, 0.15, 0.10, 0.15, 0.05])
    
    # Initialize clear flags if they don't exist
    if 'clear_table_flag' not in st.session_state:
        st.session_state.clear_table_flag = False
    if 'clear_column_flag' not in st.session_state:
        st.session_state.clear_column_flag = False
    if 'clear_type_flag' not in st.session_state:
        st.session_state.clear_type_flag = False
    if 'clear_discovery_flag' not in st.session_state:
        st.session_state.clear_discovery_flag = False
    if 'clear_assigned_flag' not in st.session_state:
        st.session_state.clear_assigned_flag = False
    
    # Create filter system that avoids session state conflicts
    with filter_col1:
        input_col, clear_col = st.columns([0.8, 0.2])
        with input_col:
            # Check if we need to clear this filter
            initial_value = "" if st.session_state.clear_table_flag else st.session_state.discovery_filters.get('table_name', '')
            table_filter = st.text_input(
                "Filter Table",
                value=initial_value,
                key="table_filter_key",
                placeholder="Filter...",
                label_visibility="collapsed"
            )
            # Reset clear flag after using it
            if st.session_state.clear_table_flag:
                st.session_state.clear_table_flag = False
        with clear_col:
            if table_filter and st.button("×", key="clear_table", help="Clear filter"):
                st.session_state.clear_table_flag = True
                st.session_state.discovery_filters['table_name'] = ""
                st.rerun()
        st.session_state.discovery_filters['table_name'] = table_filter
    
    with filter_col2:
        input_col, clear_col = st.columns([0.8, 0.2])
        with input_col:
            initial_value = "" if st.session_state.clear_column_flag else st.session_state.discovery_filters.get('column_name', '')
            column_filter = st.text_input(
                "Filter Column",
                value=initial_value,
                key="column_filter_key",
                placeholder="Filter...",
                label_visibility="collapsed"
            )
            if st.session_state.clear_column_flag:
                st.session_state.clear_column_flag = False
        with clear_col:
            if column_filter and st.button("×", key="clear_column", help="Clear filter"):
                st.session_state.clear_column_flag = True
                st.session_state.discovery_filters['column_name'] = ""
                st.rerun()
        st.session_state.discovery_filters['column_name'] = column_filter
    
    with filter_col3:
        input_col, clear_col = st.columns([0.8, 0.2])
        with input_col:
            initial_value = "" if st.session_state.clear_type_flag else st.session_state.discovery_filters.get('column_type', '')
            type_filter = st.text_input(
                "Filter Type",
                value=initial_value,
                key="type_filter_key",
                placeholder="Filter...",
                label_visibility="collapsed"
            )
            if st.session_state.clear_type_flag:
                st.session_state.clear_type_flag = False
        with clear_col:
            if type_filter and st.button("×", key="clear_type", help="Clear filter"):
                st.session_state.clear_type_flag = True
                st.session_state.discovery_filters['column_type'] = ""
                st.rerun()
        st.session_state.discovery_filters['column_type'] = type_filter
    
    with filter_col4:
        st.write("")  # No filter for LENGTH column
    
    with filter_col5:
        input_col, clear_col = st.columns([0.8, 0.2])
        with input_col:
            initial_value = "" if st.session_state.clear_discovery_flag else st.session_state.discovery_filters.get('discovery_algorithm', '')
            discovery_filter = st.text_input(
                "Filter Discovery",
                value=initial_value,
                key="discovery_filter_key",
                placeholder="Filter...",
                label_visibility="collapsed"
            )
            if st.session_state.clear_discovery_flag:
                st.session_state.clear_discovery_flag = False
        with clear_col:
            if discovery_filter and st.button("×", key="clear_discovery", help="Clear filter"):
                st.session_state.clear_discovery_flag = True
                st.session_state.discovery_filters['discovery_algorithm'] = ""
                st.rerun()
        st.session_state.discovery_filters['discovery_algorithm'] = discovery_filter
        
    with filter_col6:
        st.write("")  # No filter for CONFIDENCE column
        
    with filter_col7:
        input_col, clear_col = st.columns([0.8, 0.2])
        with input_col:
            initial_value = "" if st.session_state.clear_assigned_flag else st.session_state.discovery_filters.get('assigned_algorithm', '')
            assigned_filter = st.text_input(
                "Filter Assigned",
                value=initial_value,
                key="assigned_filter_key",
                placeholder="Filter...",
                label_visibility="collapsed"
            )
            if st.session_state.clear_assigned_flag:
                st.session_state.clear_assigned_flag = False
        with clear_col:
            if assigned_filter and st.button("×", key="clear_assigned", help="Clear filter"):
                st.session_state.clear_assigned_flag = True
                st.session_state.discovery_filters['assigned_algorithm'] = ""
                st.rerun()
        st.session_state.discovery_filters['assigned_algorithm'] = assigned_filter
    
    with filter_col8:
        st.write("")  # No filter for ACTIONS column
    
    # Calculate pagination slice
    start_idx = (current_page - 1) * page_size
    end_idx = start_idx + page_size
    display_df = filtered_df.iloc[start_idx:end_idx]
    
    for idx, row in display_df.iterrows():
        table_name = row['Table Name']
        column_name = row['Column Name']
        current_assigned = row['Assigned Algorithm']
        if pd.isna(current_assigned):
            current_assigned = ""
        
        # Create columns for each row
        col1, col2, col3, col4, col5, col6, col7, col8 = st.columns([0.15, 0.20, 0.12, 0.08, 0.15, 0.10, 0.15, 0.05])
        
        with col1:
            st.markdown(f'<div style="font-size: 14px;">{table_name}</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown(f'<div style="font-size: 14px;">{column_name}</div>', unsafe_allow_html=True)
        
        with col3:
            st.markdown(f'<div style="font-size: 14px;">{row["Column Type"]}</div>', unsafe_allow_html=True)
        
        with col4:
            length_val = row['Column Length']
            if pd.isna(length_val) or length_val == -1:
                st.markdown('<div style="font-size: 14px;">-</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div style="font-size: 14px;">{str(int(length_val))}</div>', unsafe_allow_html=True)
        
        with col5:
            discovery_algo = row['Discovery Algorithm']
            if pd.isna(discovery_algo) or discovery_algo == "":
                st.markdown('<div style="font-size: 14px;">-</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div style="font-size: 14px;">{discovery_algo}</div>', unsafe_allow_html=True)
        
        with col6:
            confidence = row['Confidence Score']
            if pd.isna(confidence) or confidence == 0:
                st.markdown('<div style="font-size: 14px;">-</div>', unsafe_allow_html=True)
            else:
                # Apply color coding based on confidence ranges
                confidence_percent = confidence * 100  # Convert to percentage for comparison
                if confidence_percent >= 60:
                    color = "#28a745"  # Green
                elif confidence_percent >= 30:
                    color = "#ffc107"  # Yellow
                else:
                    color = "#dc3545"  # Red
                
                st.markdown(f'<div style="font-size: 14px; color: {color}; font-weight: 600;">●{confidence:.1%}</div>', unsafe_allow_html=True)
        
        with col7:
            # Algorithm dropdown
            try:
                current_index = active_algorithms.index(current_assigned) if current_assigned in active_algorithms else 0
            except ValueError:
                current_index = 0
            
            change_key = f"{table_name}_{column_name}"
            
            new_algorithm = st.selectbox(
                "Algorithm",
                active_algorithms,
                index=current_index,
                key=f"algo_select_{change_key}_{idx}",
                label_visibility="collapsed",
                help=f"Select masking algorithm for {column_name}"
            )
            
            # Track changes
            if new_algorithm != current_assigned:
                st.session_state.algorithm_changes[change_key] = {
                    'table_name': table_name,
                    'column_name': column_name,
                    'old_algorithm': current_assigned,
                    'new_algorithm': new_algorithm
                }
            elif change_key in st.session_state.algorithm_changes:
                # Remove from changes if reverted to original
                del st.session_state.algorithm_changes[change_key]
        
        with col8:
            # Clear algorithm button (X icon)
            clear_key = f"clear_{table_name}_{column_name}_{idx}"
            if current_assigned and current_assigned != "":
                if st.button("❌", key=clear_key, help="Clear assigned algorithm"):
                    # Add to changes to clear the algorithm (set to empty/NULL)
                    st.session_state.algorithm_changes[change_key] = {
                        'table_name': table_name,
                        'column_name': column_name,
                        'old_algorithm': current_assigned,
                        'new_algorithm': ""  # Empty string will be converted to NULL
                    }
                    st.rerun()
            else:
                st.write("")  # Empty space when no algorithm assigned
    
    # Add pagination controls at the bottom
    if total_pages > 1:
        col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
        
        with col1:
            if st.button("⏮️", key="bottom_first", disabled=(current_page == 1)):
                st.session_state.discovery_page = 1
                st.rerun()
        
        with col2:
            if st.button("⬅️", key="bottom_prev", disabled=(current_page == 1)):
                st.session_state.discovery_page = current_page - 1
                st.rerun()
        
        with col3:
            st.write(f"**Page {current_page} of {total_pages}**")
        
        with col4:
            if st.button("➡️", key="bottom_next", disabled=(current_page == total_pages)):
                st.session_state.discovery_page = current_page + 1
                st.rerun()
        
        with col5:
            if st.button("⏭️", key="bottom_last", disabled=(current_page == total_pages)):
                st.session_state.discovery_page = total_pages
                st.rerun()
    
    # Show summary of changes and submit button
    # Debug: Show current algorithm changes count
    changes_count = len(st.session_state.algorithm_changes) if st.session_state.algorithm_changes else 0
    if changes_count > 0:
        st.write(f"**Debug**: Found {changes_count} pending changes")
    
    if st.session_state.algorithm_changes:
        st.divider()
        
        # Show pending changes summary
        st.write(f"**Pending Changes:** {len(st.session_state.algorithm_changes)} algorithm assignments")
        
        # Show change details
        with st.expander("View Change Details", expanded=False):
            for change_key, change_info in st.session_state.algorithm_changes.items():
                old_algo = change_info['old_algorithm'] or "(None)"
                new_algo = change_info['new_algorithm'] or "(Clear/NULL)"
                st.write(f"• **{change_info['table_name']}.{change_info['column_name']}**: {old_algo} → {new_algo}")
        
        # Action buttons in a single row with proper spacing
        submit_col, spacer_col, reset_col = st.columns([4, 0.2, 1])
        
        with submit_col:
            if st.button("💾 Submit Algorithm Changes", type="primary", use_container_width=True):
                # Convert changes to update list
                algorithm_updates = list(st.session_state.algorithm_changes.values())
                
                with st.spinner("Updating algorithm assignments..."):
                    result = batch_update_assigned_algorithms(session, database, schema, algorithm_updates)
                
                if result['success']:
                    st.success(f"✅ Successfully updated {result['updates_made']} algorithm assignments!")
                    if result['errors']:
                        st.warning(f"⚠️ {len(result['errors'])} errors occurred:")
                        for error in result['errors']:
                            st.error(error)
                    
                    # Clear changes
                    st.session_state.algorithm_changes = {}
                    st.rerun()
                else:
                    st.error(f"❌ Failed to update algorithms: {result.get('error', 'Unknown error')}")
        
        with spacer_col:
            st.write("")  # Empty spacer column
        
        with reset_col:
            if st.button("🔄", help="Reset Changes", use_container_width=True):
                st.session_state.algorithm_changes = {}
                st.rerun()
    else:
        st.info("💡 **To make changes**: Use the dropdown menus in the 'Assigned Algorithm' column above to modify algorithm assignments. The 'Submit Algorithm Changes' button will appear once you make changes.")
    
    # Close the discovery results container
    st.html('</div>')


def display_available_tables(session, database: str, schema: str):
    """Display available tables with selection checkboxes and editable sample sizes - matching Existing Discovery Results design."""
    
    if not database or not schema:
        return []
    
    try:
        # Load tables with metadata
        from .snowflake_ops import list_available_tables
        tables_df = list_available_tables(session, database, schema)
        
        if tables_df.empty:
            st.info("No tables found in the selected database and schema")
            return []
        
        # Initialize session state for table selection if not exists
        if 'selected_tables_for_discovery' not in st.session_state:
            st.session_state.selected_tables_for_discovery = {}
        
        if 'table_sample_sizes' not in st.session_state:
            st.session_state.table_sample_sizes = {}
        
        # Show table count
        st.subheader("📋 Available Tables")
        st.write(f"Found **{len(tables_df)}** tables in `{database}.{schema}`")
        # Add same CSS styling as existing discovery results
        st.html("""
        <style>
            .available-tables-container {
                border: 1px solid #dee2e6;
                border-radius: 8px;
                margin: 0;
                background: white;
                padding: 8px;
            }
            
            .available-tables-table {
                width: 100%;
                border-collapse: collapse;
                font-family: 'Inter', sans-serif;
                background: white;
                border-radius: 8px;
                overflow: hidden;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            }
            
            .available-tables-table th {
                background: #f8f9fa;
                color: #495057;
                font-weight: 600;
                text-align: left;
                padding: 8px 6px;
                border-bottom: 2px solid #dee2e6;
                border-right: 1px solid #dee2e6;
                font-size: 11px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            
            .available-tables-table td {
                padding: 6px;
                border-bottom: 1px solid #f1f3f4;
                border-right: 1px solid #f1f3f4;
                vertical-align: middle;
            }
            
            .available-tables-table tr:hover {
                background-color: #f8f9fa;
            }
            
            .available-tables-table th:last-child,
            .available-tables-table td:last-child {
                border-right: none;
            }
            
            /* Column widths for available tables */
            .col-checkbox { width: 8%; }
            .col-table-name { width: 40%; }
            .col-row-count { width: 25%; }
            .col-sample-size { width: 27%; }
        </style>
        """)
        # Create scrollable container with headers
        st.html("""
        <div class="available-tables-container">
            <table class="available-tables-table">
                <thead>
                    <tr>
                        <th class="col-checkbox">SELECT</th>
                        <th class="col-table-name">TABLE NAME</th>
                        <th class="col-row-count">NUMBER OF ROWS</th>
                        <th class="col-sample-size">DISCOVERY SAMPLE</th>
                    </tr>
                </thead>
            </table>
        """)
        
        # Master checkbox row for select all/none
        col1, col2, col3, col4 = st.columns([0.08, 0.40, 0.25, 0.27])
        
        with col1:
            # Master checkbox logic
            all_selected = all(
                st.session_state.selected_tables_for_discovery.get(f"{database}.{schema}.{row['TABLE_NAME']}", False)
                for _, row in tables_df.iterrows()
            )
            
            select_all = st.checkbox(
                "",
                value=all_selected,
                key="select_all_tables_master",
                help="Select/Deselect all tables"
            )
            
            # Handle master checkbox change
            if select_all != all_selected:
                for _, row in tables_df.iterrows():
                    table_key = f"{database}.{schema}.{row['TABLE_NAME']}"
                    st.session_state.selected_tables_for_discovery[table_key] = select_all
                    # Set default sample size if selecting
                    if select_all and table_key not in st.session_state.table_sample_sizes:
                        st.session_state.table_sample_sizes[table_key] = 1000
                st.rerun()
        
        with col2:
            st.markdown('<div style="font-size: 14px; font-weight: 600; color: #666;">All Tables</div>', unsafe_allow_html=True)
        
        with col3:
            total_rows = tables_df['ROW_COUNT'].sum() if 'ROW_COUNT' in tables_df.columns else 0
            st.markdown(f'<div style="font-size: 14px; color: #666;">{total_rows:,} total</div>', unsafe_allow_html=True)
        
        with col4:
            st.markdown('<div style="font-size: 14px; color: #666;">1000 default</div>', unsafe_allow_html=True)
        
        # Initialize pagination state for available tables
        if 'available_tables_page' not in st.session_state:
            st.session_state.available_tables_page = 1
        
        # Calculate pagination
        page_size = 12
        total_tables = len(tables_df)
        total_pages = (total_tables + page_size - 1) // page_size  # Ceiling division
        current_page = st.session_state.available_tables_page
        
        # Ensure current page is valid
        if current_page > total_pages and total_pages > 0:
            st.session_state.available_tables_page = 1
            current_page = 1
        
        # Add pagination controls at the top
        if total_pages > 1:
            col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
            
            with col1:
                if st.button("⏮️", disabled=(current_page == 1), key="first_page_tables"):
                    st.session_state.available_tables_page = 1
                    st.rerun()
            
            with col2:
                if st.button("⬅️", disabled=(current_page == 1), key="prev_page_tables"):
                    st.session_state.available_tables_page = current_page - 1
                    st.rerun()
            
            with col3:
                st.write(f"**Page {current_page} of {total_pages}** ({total_tables} total tables)")
            
            with col4:
                if st.button("➡️", disabled=(current_page == total_pages), key="next_page_tables"):
                    st.session_state.available_tables_page = current_page + 1
                    st.rerun()
            
            with col5:
                if st.button("⏭️", disabled=(current_page == total_pages), key="last_page_tables"):
                    st.session_state.available_tables_page = total_pages
                    st.rerun()
        
        # Calculate pagination slice
        start_idx = (current_page - 1) * page_size
        end_idx = start_idx + page_size
        display_tables_df = tables_df.iloc[start_idx:end_idx]
        
        # Table rows - using same structure as discovery results
        for idx, row in display_tables_df.iterrows():
            table_name = row['TABLE_NAME']
            table_key = f"{database}.{schema}.{table_name}"
            row_count = row.get('ROW_COUNT', 0) or 0
            
            # Initialize if not exists
            if table_key not in st.session_state.selected_tables_for_discovery:
                st.session_state.selected_tables_for_discovery[table_key] = False
            if table_key not in st.session_state.table_sample_sizes:
                st.session_state.table_sample_sizes[table_key] = 1000
            
            # Create columns for each row - matching discovery results layout
            col1, col2, col3, col4 = st.columns([0.08, 0.40, 0.25, 0.27])
            
            with col1:
                # Individual table checkbox
                selected = st.checkbox(
                    "",
                    value=st.session_state.selected_tables_for_discovery[table_key],
                    key=f"table_select_{table_key}_{idx}",
                    help=f"Select {table_name} for discovery"
                )
                st.session_state.selected_tables_for_discovery[table_key] = selected
            
            with col2:
                st.markdown(f'<div style="font-size: 14px;">{table_name}</div>', unsafe_allow_html=True)
            
            with col3:
                # Format row count with commas
                formatted_count = f"{row_count:,}" if row_count > 0 else "0"
                st.markdown(f'<div style="font-size: 14px;">{formatted_count}</div>', unsafe_allow_html=True)
            
            with col4:
                # Sample size control - only using built-in number input controls
                current_sample = st.session_state.table_sample_sizes[table_key]
                
                # Direct input for sample size with built-in +/- controls
                new_sample = st.number_input(
                    "",
                    min_value=100,
                    max_value=50000,
                    value=current_sample,
                    step=100,
                    key=f"sample_input_{table_key}_{idx}",
                    label_visibility="collapsed"
                )
                st.session_state.table_sample_sizes[table_key] = new_sample
        
        # Add pagination controls at the bottom
        if total_pages > 1:
            col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
            
            with col1:
                if st.button("⏮️", disabled=(current_page == 1), key="first_page_tables_bottom"):
                    st.session_state.available_tables_page = 1
                    st.rerun()
            
            with col2:
                if st.button("⬅️", disabled=(current_page == 1), key="prev_page_tables_bottom"):
                    st.session_state.available_tables_page = current_page - 1
                    st.rerun()
            
            with col3:
                st.write(f"**Page {current_page} of {total_pages}** ({total_tables} total tables)")
            
            with col4:
                if st.button("➡️", disabled=(current_page == total_pages), key="next_page_tables_bottom"):
                    st.session_state.available_tables_page = current_page + 1
                    st.rerun()
            
            with col5:
                if st.button("⏭️", disabled=(current_page == total_pages), key="last_page_tables_bottom"):
                    st.session_state.available_tables_page = total_pages
                    st.rerun()
        
        # Show current page info
        if total_pages > 1:
            start_record = start_idx + 1
            end_record = min(end_idx, total_tables)
            st.caption(f"Showing tables {start_record}-{end_record} of {total_tables} total (Page {current_page} of {total_pages})")
        else:
            st.caption(f"Showing {len(tables_df)} available tables")
        
        # Close the available tables container
        st.html('</div>')
        
        # Return selected tables with their sample sizes
        selected_tables = []
        for table_key, is_selected in st.session_state.selected_tables_for_discovery.items():
            if is_selected and table_key.startswith(f"{database}.{schema}."):
                table_name = table_key.split(".")[-1]
                sample_size = st.session_state.table_sample_sizes.get(table_key, 1000)
                selected_tables.append({
                    'table_name': table_name,
                    'sample_size': sample_size
                })
        
        return selected_tables
        
    except Exception as e:
        st.error(f"Failed to load tables: {str(e)}")
        return []


def display_filtered_existing_discovery_results(session, database: str, schema: str, selected_tables: List[Dict]):
    """Display existing discovery results filtered by selected tables."""
    
    if not database or not schema:
        return
    
    try:
        # Get selected table names
        selected_table_names = [table['table_name'] for table in selected_tables] if selected_tables else []
        
        # Load discovery results for the database and schema
        from .metadata_store import get_existing_discovery_results
        discovery_df = get_existing_discovery_results(session, database, schema, selected_table_names)
        
        if not discovery_df.empty:
            # Show discovery results for selected tables
            st.subheader("🔍 Existing Discovery Results")
            st.info(f"Showing discovery results for {len(selected_table_names)} selected table(s)")
            
            # Use the existing display function
            display_existing_discovery_results_formatted(discovery_df, database, schema)
        else:
            # Check which tables have no discovery results
            if selected_table_names:
                # Check which tables actually have discovery data
                all_discovery_df = get_existing_discovery_results(session, database, schema)
                tables_with_discovery = all_discovery_df['Table Name'].unique().tolist() if not all_discovery_df.empty else []
                
                tables_without_discovery = [
                    table for table in selected_table_names 
                    if table not in tables_with_discovery
                ]
                
                if tables_without_discovery:
                    st.subheader("⚠️ Discovery Status")
                    st.warning(f"No sensitive data discovery has been executed on the following selected table(s): **{', '.join(tables_without_discovery)}**")
                    st.info("Run discovery on these tables first to see sensitive data identification results.")
                else:
                    st.info("No sensitive data was found in the selected tables.")
            else:
                st.info("Please select tables above to view their discovery results.")
                
    except Exception as e:
        st.error(f"Failed to load discovery results: {str(e)}")


def display_masking_discovery_results_formatted(discovery_df: pd.DataFrame, database: str, schema: str):
    """Display discovery results in Mask & Deliver page with the same design as Discovery page."""
    
    if discovery_df.empty:
        st.info("No discovery results found.")
        return
    
    # Get session for algorithm loading
    session = st.session_state.get('snowflake_session')
    if not session:
        st.error("❌ Snowflake session not available")
        return
    
    # Get active algorithms for dropdowns
    from .metadata_store import get_active_algorithms, batch_update_assigned_algorithms
    active_algorithms = get_active_algorithms(session)
    
    # Initialize session state for algorithm changes (using masking-specific keys)
    if 'masking_algorithm_changes' not in st.session_state:
        st.session_state.masking_algorithm_changes = {}
    
    # Initialize session state for filters with masking prefix
    if 'masking_discovery_filters' not in st.session_state:
        st.session_state.masking_discovery_filters = {
            'table_name': '',
            'column_name': '',
            'column_type': '',
            'discovery_algorithm': '',
            'assigned_algorithm': ''
        }
    
    # Initialize pagination state with masking prefix
    if 'masking_discovery_page' not in st.session_state:
        st.session_state.masking_discovery_page = 1
    
    # Rename the dataframe columns to match the expected format
    original_df = discovery_df.copy()
    
    # Ensure we have the right column names
    expected_columns = ['IDENTIFIED_TABLE', 'IDENTIFIED_COLUMN', 'IDENTIFIED_COLUMN_TYPE', 
                       'IDENTIFIED_COLUMN_MAX_LENGTH', 'PROFILED_ALGORITHM', 'CONFIDENCE_SCORE', 'ASSIGNED_ALGORITHM']
    
    # Check if discovery_df has the expected columns or needs to be renamed from display format
    if 'Table Name' in discovery_df.columns:
        # Already in display format, keep as is
        original_df = discovery_df.copy()
    else:
        # Convert from database format to display format
        original_df = original_df.rename(columns={
            'IDENTIFIED_TABLE': 'Table Name',
            'IDENTIFIED_COLUMN': 'Column Name',
            'IDENTIFIED_COLUMN_TYPE': 'Column Type',
            'IDENTIFIED_COLUMN_MAX_LENGTH': 'Column Length',
            'PROFILED_ALGORITHM': 'Discovery Algorithm',
            'CONFIDENCE_SCORE': 'Confidence Score',
            'ASSIGNED_ALGORITHM': 'Assigned Algorithm'
        })
    
    st.subheader("🔍 Discovery Results")
    st.write(f"Found **{len(original_df)}** columns with discovery results for `{database}.{schema}`")
    
    # Apply filters (using masking-specific filters)
    filtered_df = original_df.copy()
    
    if st.session_state.masking_discovery_filters['table_name']:
        filtered_df = filtered_df[filtered_df['Table Name'].str.contains(st.session_state.masking_discovery_filters['table_name'], case=False, na=False)]
    
    if st.session_state.masking_discovery_filters['column_name']:
        filtered_df = filtered_df[filtered_df['Column Name'].str.contains(st.session_state.masking_discovery_filters['column_name'], case=False, na=False)]
    
    if st.session_state.masking_discovery_filters['column_type']:
        filtered_df = filtered_df[filtered_df['Column Type'].str.contains(st.session_state.masking_discovery_filters['column_type'], case=False, na=False)]
    
    if st.session_state.masking_discovery_filters['discovery_algorithm']:
        filtered_df = filtered_df[filtered_df['Discovery Algorithm'].fillna('').str.contains(st.session_state.masking_discovery_filters['discovery_algorithm'], case=False, na=False)]
    
    if st.session_state.masking_discovery_filters['assigned_algorithm']:
        filtered_df = filtered_df[filtered_df['Assigned Algorithm'].fillna('').str.contains(st.session_state.masking_discovery_filters['assigned_algorithm'], case=False, na=False)]
    
    # Apply default sorting by Table Name, then Column Name
    if not filtered_df.empty:
        filtered_df = filtered_df.sort_values(
            by=['Table Name', 'Column Name'], 
            ascending=[True, True],
            na_position='last'
        )
    
    # Reset index after filtering and sorting
    filtered_df = filtered_df.reset_index(drop=True)
    
    # Calculate pagination
    page_size = 15
    total_results = len(filtered_df)
    total_pages = (total_results + page_size - 1) // page_size  # Ceiling division
    current_page = st.session_state.masking_discovery_page
    
    # Ensure current page is valid
    if current_page > total_pages:
        st.session_state.masking_discovery_page = 1
        current_page = 1
    
    # Show filtered results count
    if len(filtered_df) != len(original_df):
        st.write(f"Showing **{len(filtered_df)}** of **{len(original_df)}** results")
    
    if filtered_df.empty:
        st.warning("🔍 No results match the current filters.")
        return
    
    # Add pagination controls at the top
    if total_pages > 1:
        col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
        
        with col1:
            if st.button("⏮️", disabled=(current_page == 1), key="masking_first_page"):
                st.session_state.masking_discovery_page = 1
                st.rerun()
        
        with col2:
            if st.button("⬅️", disabled=(current_page == 1), key="masking_prev_page"):
                st.session_state.masking_discovery_page = current_page - 1
                st.rerun()
        
        with col3:
            st.write(f"**Page {current_page} of {total_pages}** ({total_results} total results)")
        
        with col4:
            if st.button("➡️", disabled=(current_page == total_pages), key="masking_next_page"):
                st.session_state.masking_discovery_page = current_page + 1
                st.rerun()
        
        with col5:
            if st.button("⏭️", disabled=(current_page == total_pages), key="masking_last_page"):
                st.session_state.masking_discovery_page = total_pages
                st.rerun()
    
    # Add the same CSS styling as the Discovery page
    st.html("""
    <style>
        .masking-discovery-results-container {
            border: 1px solid #dee2e6;
            border-radius: 8px;
            margin: 0;
            background: white;
            padding: 8px;
        }
        
        .masking-discovery-table {
            width: 100%;
            border-collapse: collapse;
            font-family: 'Inter', sans-serif;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        .masking-discovery-table th {
            background: #f8f9fa;
            color: #495057;
            font-weight: 600;
            text-align: left;
            padding: 8px 6px;
            border-bottom: 2px solid #dee2e6;
            border-right: 1px solid #dee2e6;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .masking-discovery-table td {
            padding: 6px;
            border-bottom: 1px solid #f1f3f4;
            border-right: 1px solid #f1f3f4;
            vertical-align: middle;
        }
        
        .masking-discovery-table tr:hover {
            background-color: #f8f9fa;
        }
        
        .masking-discovery-table th:last-child,
        .masking-discovery-table td:last-child {
            border-right: none;
        }
        
        /* Column widths */
        .col-table { width: 15%; }
        .col-column { width: 20%; }
        .col-type { width: 12%; }
        .col-length { width: 8%; }
        .col-discovery { width: 15%; }
        .col-confidence { width: 10%; }
        .col-assigned { width: 15%; }
        .col-actions { width: 5%; }
    </style>
    """)
    
    # Create the table headers with container
    st.html("""
    <div class="masking-discovery-results-container">
    <table class="masking-discovery-table">
        <thead>
            <tr>
                <th class="col-table">TABLE NAME</th>
                <th class="col-column">COLUMN NAME</th>
                <th class="col-type">COLUMN TYPE</th>
                <th class="col-length">LENGTH</th>
                <th class="col-discovery">DISCOVERY ALGORITHM</th>
                <th class="col-confidence">CONFIDENCE</th>
                <th class="col-assigned">ASSIGNED ALGORITHM</th>
                <th class="col-actions">ACTIONS</th>
            </tr>
        </thead>
    </table>
    """)
    
    # Add filter inputs below headers
    filter_col1, filter_col2, filter_col3, filter_col4, filter_col5, filter_col6, filter_col7, filter_col8 = st.columns([0.15, 0.20, 0.12, 0.08, 0.15, 0.10, 0.15, 0.05])
    
    with filter_col1:
        table_filter = st.text_input(
            "Filter Table",
            value=st.session_state.masking_discovery_filters['table_name'],
            key="masking_table_filter",
            placeholder="Filter...",
            label_visibility="collapsed"
        )
        st.session_state.masking_discovery_filters['table_name'] = table_filter
    
    with filter_col2:
        column_filter = st.text_input(
            "Filter Column",
            value=st.session_state.masking_discovery_filters['column_name'],
            key="masking_column_filter",
            placeholder="Filter...",
            label_visibility="collapsed"
        )
        st.session_state.masking_discovery_filters['column_name'] = column_filter
    
    with filter_col3:
        type_filter = st.text_input(
            "Filter Type",
            value=st.session_state.masking_discovery_filters['column_type'],
            key="masking_type_filter",
            placeholder="Filter...",
            label_visibility="collapsed"
        )
        st.session_state.masking_discovery_filters['column_type'] = type_filter
    
    with filter_col4:
        st.write("")  # Empty for length column
    
    with filter_col5:
        discovery_filter = st.text_input(
            "Filter Discovery",
            value=st.session_state.masking_discovery_filters['discovery_algorithm'],
            key="masking_discovery_filter",
            placeholder="Filter...",
            label_visibility="collapsed"
        )
        st.session_state.masking_discovery_filters['discovery_algorithm'] = discovery_filter
    
    with filter_col6:
        st.write("")  # Empty for confidence column
    
    with filter_col7:
        assigned_filter = st.text_input(
            "Filter Assigned",
            value=st.session_state.masking_discovery_filters['assigned_algorithm'],
            key="masking_assigned_filter",
            placeholder="Filter...",
            label_visibility="collapsed"
        )
        st.session_state.masking_discovery_filters['assigned_algorithm'] = assigned_filter
    
    with filter_col8:
        st.write("")  # Empty for actions column
    
    # Calculate pagination slice
    start_idx = (current_page - 1) * page_size
    end_idx = start_idx + page_size
    display_df = filtered_df.iloc[start_idx:end_idx]
    
    # Display each row with dropdowns
    for idx, row in display_df.iterrows():
        table_name = row['Table Name']
        column_name = row['Column Name']
        current_assigned = row['Assigned Algorithm']
        if pd.isna(current_assigned):
            current_assigned = ""
        
        # Create columns for each row - matching header proportions
        col1, col2, col3, col4, col5, col6, col7, col8 = st.columns([0.15, 0.20, 0.12, 0.08, 0.15, 0.10, 0.15, 0.05])
        
        with col1:
            st.markdown(f'<div style="font-size: 14px; padding: 8px 0;">{table_name}</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown(f'<div style="font-size: 14px; padding: 8px 0;">{column_name}</div>', unsafe_allow_html=True)
        
        with col3:
            st.markdown(f'<div style="font-size: 14px; padding: 8px 0;">{row["Column Type"]}</div>', unsafe_allow_html=True)
        
        with col4:
            length_val = row['Column Length']
            if pd.isna(length_val) or length_val == -1:
                st.markdown('<div style="font-size: 14px; padding: 8px 0;">-</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div style="font-size: 14px; padding: 8px 0;">{str(int(length_val))}</div>', unsafe_allow_html=True)
        
        with col5:
            discovery_algo = row['Discovery Algorithm']
            if pd.isna(discovery_algo) or discovery_algo == "":
                st.markdown('<div style="font-size: 14px; padding: 8px 0;">-</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div style="font-size: 14px; padding: 8px 0;">{discovery_algo}</div>', unsafe_allow_html=True)
        
        with col6:
            confidence = row['Confidence Score']
            if pd.isna(confidence) or confidence == 0:
                st.markdown('<div style="font-size: 14px; padding: 8px 0;">-</div>', unsafe_allow_html=True)
            else:
                # Apply color coding based on confidence ranges
                confidence_percent = confidence * 100
                if confidence_percent >= 60:
                    color = "#28a745"  # Green
                elif confidence_percent >= 30:
                    color = "#ffc107"  # Yellow
                else:
                    color = "#dc3545"  # Red
                
                st.markdown(f'<div style="font-size: 14px; padding: 8px 0; color: {color}; font-weight: 600;">{confidence:.1%}</div>', unsafe_allow_html=True)
        
        with col7:
            # Algorithm dropdown
            change_key = f"{table_name}_{column_name}"
            
            # Check if this algorithm has been changed
            display_algorithm = current_assigned
            if change_key in st.session_state.masking_algorithm_changes:
                display_algorithm = st.session_state.masking_algorithm_changes[change_key]['new_algorithm']
            
            try:
                current_index = active_algorithms.index(display_algorithm) if display_algorithm in active_algorithms else 0
            except ValueError:
                current_index = 0
            
            new_algorithm = st.selectbox(
                "Algorithm",
                active_algorithms,
                index=current_index,
                key=f"masking_algo_select_{change_key}_{idx}",
                label_visibility="collapsed",
                help=f"Select masking algorithm for {column_name}"
            )
            
            # Track changes from dropdown
            if new_algorithm != current_assigned:
                st.session_state.masking_algorithm_changes[change_key] = {
                    'table_name': table_name,
                    'column_name': column_name,
                    'old_algorithm': current_assigned,
                    'new_algorithm': new_algorithm
                }
            elif change_key in st.session_state.masking_algorithm_changes:
                # Remove from changes if reverted to original
                del st.session_state.masking_algorithm_changes[change_key]
        
        with col8:
            # X button to remove assigned algorithm (only show if there's an algorithm currently)
            if display_algorithm and display_algorithm.strip():
                if st.button("✖", key=f"masking_remove_algo_{change_key}_{idx}", help=f"Remove assigned algorithm for {column_name}"):
                    # Set to remove the algorithm (set to empty/None)
                    st.session_state.masking_algorithm_changes[change_key] = {
                        'table_name': table_name,
                        'column_name': column_name,
                        'old_algorithm': current_assigned,
                        'new_algorithm': ''  # Empty string means remove/NULL
                    }
                    st.rerun()
            else:
                # Empty space when no algorithm is assigned
                st.write("")
    
    # Close the container
    st.html('</div>')
    
    # Add pagination controls at the bottom
    if total_pages > 1:
        col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
        
        with col1:
            if st.button("⏮️", disabled=(current_page == 1), key="masking_first_page_bottom"):
                st.session_state.masking_discovery_page = 1
                st.rerun()
        
        with col2:
            if st.button("⬅️", disabled=(current_page == 1), key="masking_prev_page_bottom"):
                st.session_state.masking_discovery_page = current_page - 1
                st.rerun()
        
        with col3:
            st.write(f"**Page {current_page} of {total_pages}** ({total_results} total results)")
        
        with col4:
            if st.button("➡️", disabled=(current_page == total_pages), key="masking_next_page_bottom"):
                st.session_state.masking_discovery_page = current_page + 1
                st.rerun()
        
        with col5:
            if st.button("⏭️", disabled=(current_page == total_pages), key="masking_last_page_bottom"):
                st.session_state.masking_discovery_page = total_pages
                st.rerun()
    
    # Show current page info
    if total_pages > 1:
        start_record = start_idx + 1
        end_record = min(end_idx, total_results)
        st.caption(f"Showing records {start_record}-{end_record} of {total_results} results (Page {current_page} of {total_pages})")
    else:
        st.caption(f"Showing {len(display_df)} of {len(filtered_df)} discovery results")
    
    # Show summary of changes and submit button
    if st.session_state.masking_algorithm_changes:
        st.divider()
        
        # Show pending changes summary
        st.write(f"**Pending Changes:** {len(st.session_state.masking_algorithm_changes)} algorithm assignments")
        
        # Show change details
        with st.expander("View Change Details", expanded=False):
            for change_key, change_info in st.session_state.masking_algorithm_changes.items():
                old_algo = change_info['old_algorithm'] or "(None)"
                new_algo = change_info['new_algorithm'] or "(Clear/NULL)"
                st.write(f"• **{change_info['table_name']}.{change_info['column_name']}**: {old_algo} → {new_algo}")
        
        # Action buttons in a single row with proper spacing
        submit_col, spacer_col, reset_col = st.columns([4, 0.2, 1])
        
        with submit_col:
            if st.button("💾 Submit Algorithm Changes", type="primary", use_container_width=True, key="masking_submit_changes"):
                # Convert changes to update list
                algorithm_updates = list(st.session_state.masking_algorithm_changes.values())
                
                with st.spinner("Updating algorithm assignments..."):
                    result = batch_update_assigned_algorithms(session, database, schema, algorithm_updates)
                
                if result['success']:
                    st.success(f"✅ Successfully updated {result['updates_made']} algorithm assignments!")
                    if result['errors']:
                        st.warning(f"⚠️ {len(result['errors'])} errors occurred:")
                        for error in result['errors']:
                            st.error(error)
                    
                    # Clear changes
                    st.session_state.masking_algorithm_changes = {}
                    st.rerun()
                else:
                    st.error(f"❌ Failed to update algorithms: {result.get('error', 'Unknown error')}")
        
        with spacer_col:
            st.write("")  # Empty spacer column
        
        with reset_col:
            if st.button("🔄", help="Reset Changes", use_container_width=True, key="masking_reset_changes"):
                st.session_state.masking_algorithm_changes = {}
                st.rerun()
    else:
        st.info("💡 **To make changes**: Use the dropdown menus in the 'Assigned Algorithm' column above to modify algorithm assignments. The 'Submit Algorithm Changes' button will appear once you make changes.")


def display_existing_discovery_results_formatted(discovery_df: pd.DataFrame, database: str, schema: str):
    """Display existing discovery results with interactive dropdowns for algorithm assignment."""
    
    if discovery_df.empty:
        st.info("No discovery results found.")
        return
    
    # Get session for algorithm loading
    session = st.session_state.get('snowflake_session')
    if not session:
        st.error("❌ Snowflake session not available")
        return
    
    # Get active algorithms for dropdowns
    from .metadata_store import get_active_algorithms, batch_update_assigned_algorithms
    active_algorithms = get_active_algorithms(session)
    
    # Initialize session state for algorithm changes
    if 'algorithm_changes' not in st.session_state:
        st.session_state.algorithm_changes = {}
    
    # Initialize session state for filters if not exists
    if 'discovery_filters' not in st.session_state:
        st.session_state.discovery_filters = {
            'table_name': '',
            'column_name': '',
            'column_type': '',
            'discovery_algorithm': '',
            'assigned_algorithm': ''
        }
    
    # Create filters row
    filter_col1, filter_col2, filter_col3, filter_col4, filter_col5 = st.columns(5)
    
    with filter_col1:
        st.session_state.discovery_filters['table_name'] = st.text_input(
            "Filter Table Name",
            value=st.session_state.discovery_filters['table_name'],
            key="filter_table_existing_formatted"
        )
    
    with filter_col2:
        st.session_state.discovery_filters['column_name'] = st.text_input(
            "Filter Column Name", 
            value=st.session_state.discovery_filters['column_name'],
            key="filter_column_existing_formatted"
        )
    
    with filter_col3:
        st.session_state.discovery_filters['column_type'] = st.text_input(
            "Filter Column Type",
            value=st.session_state.discovery_filters['column_type'],
            key="filter_type_existing_formatted"
        )
    
    with filter_col4:
        st.session_state.discovery_filters['discovery_algorithm'] = st.text_input(
            "Filter Discovery Algorithm",
            value=st.session_state.discovery_filters['discovery_algorithm'],
            key="filter_discovery_existing_formatted"
        )
    
    with filter_col5:
        st.session_state.discovery_filters['assigned_algorithm'] = st.text_input(
            "Filter Assigned Algorithm",
            value=st.session_state.discovery_filters['assigned_algorithm'],
            key="filter_assigned_existing_formatted"
        )
    
    # Initialize pagination state
    if 'discovery_page' not in st.session_state:
        st.session_state.discovery_page = 1
    
    # Apply filters
    filtered_df = discovery_df.copy()
    
    if st.session_state.discovery_filters['table_name']:
        filtered_df = filtered_df[filtered_df['Table Name'].str.contains(st.session_state.discovery_filters['table_name'], case=False, na=False)]
    
    if st.session_state.discovery_filters['column_name']:
        filtered_df = filtered_df[filtered_df['Column Name'].str.contains(st.session_state.discovery_filters['column_name'], case=False, na=False)]
    
    if st.session_state.discovery_filters['column_type']:
        filtered_df = filtered_df[filtered_df['Column Type'].str.contains(st.session_state.discovery_filters['column_type'], case=False, na=False)]
    
    if st.session_state.discovery_filters['discovery_algorithm']:
        filtered_df = filtered_df[filtered_df['Discovery Algorithm'].fillna('').str.contains(st.session_state.discovery_filters['discovery_algorithm'], case=False, na=False)]
    
    if st.session_state.discovery_filters['assigned_algorithm']:
        filtered_df = filtered_df[filtered_df['Assigned Algorithm'].fillna('').str.contains(st.session_state.discovery_filters['assigned_algorithm'], case=False, na=False)]
    
    # Reset index after filtering
    filtered_df = filtered_df.reset_index(drop=True)
    
    # Calculate pagination
    page_size = 12
    total_results = len(filtered_df)
    total_pages = (total_results + page_size - 1) // page_size  # Ceiling division
    current_page = st.session_state.discovery_page
    
    # Ensure current page is valid
    if current_page > total_pages and total_pages > 0:
        st.session_state.discovery_page = 1
        current_page = 1
    
    # Display results with interactive dropdowns
    if not filtered_df.empty:
        # Show filtered results count
        if len(filtered_df) != len(discovery_df):
            st.write(f"Showing **{len(filtered_df)}** of **{len(discovery_df)}** results")
        else:
            st.write(f"Found **{len(discovery_df)}** discovery results")
        
        # Add pagination controls at the top
        if total_pages > 1:
            col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
            
            with col1:
                if st.button("⏮️", disabled=(current_page == 1), key="first_page_formatted"):
                    st.session_state.discovery_page = 1
                    st.rerun()
            
            with col2:
                if st.button("⬅️", disabled=(current_page == 1), key="prev_page_formatted"):
                    st.session_state.discovery_page = current_page - 1
                    st.rerun()
            
            with col3:
                st.write(f"**Page {current_page} of {total_pages}** ({total_results} total results)")
            
            with col4:
                if st.button("➡️", disabled=(current_page == total_pages), key="next_page_formatted"):
                    st.session_state.discovery_page = current_page + 1
                    st.rerun()
            
            with col5:
                if st.button("⏭️", disabled=(current_page == total_pages), key="last_page_formatted"):
                    st.session_state.discovery_page = total_pages
                    st.rerun()
        # Add same CSS styling as available tables
        st.html("""
        <style>
            .discovery-results-container {
                border: 1px solid #dee2e6;
                border-radius: 8px;
                margin: 0;
                background: white;
                padding: 8px;
            }
            
            .discovery-results-table {
                width: 100%;
                border-collapse: collapse;
                font-family: 'Inter', sans-serif;
                background: white;
                border-radius: 8px;
                overflow: hidden;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            }
            
            .discovery-results-table th {
                background: #f8f9fa;
                color: #495057;
                font-weight: 600;
                text-align: left;
                padding: 8px 6px;
                border-bottom: 2px solid #dee2e6;
                border-right: 1px solid #dee2e6;
                font-size: 11px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            
            .discovery-results-table td {
                padding: 6px;
                border-bottom: 1px solid #f1f3f4;
                border-right: 1px solid #f1f3f4;
                vertical-align: middle;
            }
            
            .discovery-results-table tr:hover {
                background-color: #f8f9fa;
            }
            
            .discovery-results-table th:last-child,
            .discovery-results-table td:last-child {
                border-right: none;
            }
            
            /* Column widths for discovery results */
            .col-table-name { width: 15%; }
            .col-column-name { width: 20%; }
            .col-column-type { width: 12%; }
            .col-column-length { width: 8%; }
            .col-discovery-algo { width: 15%; }
            .col-confidence { width: 10%; }
            .col-assigned-algo { width: 20%; }
        </style>
        """)
        # Create scrollable container with headers
        st.html("""
        <div class="discovery-results-container">
            <table class="discovery-results-table">
                <thead>
                    <tr>
                        <th class="col-table-name">TABLE NAME</th>
                        <th class="col-column-name">COLUMN NAME</th>
                        <th class="col-column-type">COLUMN TYPE</th>
                        <th class="col-column-length">LENGTH</th>
                        <th class="col-discovery-algo">DISCOVERY ALGORITHM</th>
                        <th class="col-confidence">CONFIDENCE</th>
                        <th class="col-assigned-algo">ASSIGNED ALGORITHM</th>
                    </tr>
                </thead>
            </table>
        """)
        
        # Calculate pagination slice
        start_idx = (current_page - 1) * page_size
        end_idx = start_idx + page_size
        display_df = filtered_df.iloc[start_idx:end_idx]
        
        # Display each row with dropdowns
        for idx, row in display_df.iterrows():
            table_name = row['Table Name']
            column_name = row['Column Name']
            current_assigned = row['Assigned Algorithm']
            if pd.isna(current_assigned):
                current_assigned = ""
            
            # Create columns for each row - matching header proportions
            col1, col2, col3, col4, col5, col6, col7 = st.columns([0.15, 0.20, 0.12, 0.08, 0.15, 0.10, 0.20])
            
            with col1:
                st.markdown(f'<div style="font-size: 14px;">{table_name}</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown(f'<div style="font-size: 14px;">{column_name}</div>', unsafe_allow_html=True)
            
            with col3:
                st.markdown(f'<div style="font-size: 14px;">{row["Column Type"]}</div>', unsafe_allow_html=True)
            
            with col4:
                length_val = row['Column Length']
                if pd.isna(length_val) or length_val == -1:
                    st.markdown('<div style="font-size: 14px;">-</div>', unsafe_allow_html=True)
                else:
                    st.markdown(f'<div style="font-size: 14px;">{str(int(length_val))}</div>', unsafe_allow_html=True)
            
            with col5:
                discovery_algo = row['Discovery Algorithm']
                if pd.isna(discovery_algo) or discovery_algo == "":
                    st.markdown('<div style="font-size: 14px;">-</div>', unsafe_allow_html=True)
                else:
                    st.markdown(f'<div style="font-size: 14px;">{discovery_algo}</div>', unsafe_allow_html=True)
            
            with col6:
                confidence = row['Confidence Score']
                if pd.isna(confidence) or confidence == 0:
                    st.markdown('<div style="font-size: 14px;">-</div>', unsafe_allow_html=True)
                else:
                    # Apply color coding based on confidence ranges
                    confidence_percent = confidence * 100
                    if confidence_percent >= 60:
                        color = "#28a745"  # Green
                    elif confidence_percent >= 30:
                        color = "#ffc107"  # Yellow
                    else:
                        color = "#dc3545"  # Red
                    
                    st.markdown(f'<div style="font-size: 14px; color: {color}; font-weight: 600;">{confidence:.1%}</div>', unsafe_allow_html=True)
            
            with col7:
                # Algorithm dropdown with X button for removal
                dropdown_col, remove_col = st.columns([0.85, 0.15])
                
                with dropdown_col:
                    change_key = f"{table_name}_{column_name}"
                    
                    # Check if this algorithm has been cleared via X button
                    display_algorithm = current_assigned
                    if change_key in st.session_state.algorithm_changes:
                        if st.session_state.algorithm_changes[change_key]['new_algorithm'] == '':
                            display_algorithm = ''  # Show as cleared
                        else:
                            display_algorithm = st.session_state.algorithm_changes[change_key]['new_algorithm']
                    
                    try:
                        current_index = active_algorithms.index(display_algorithm) if display_algorithm in active_algorithms else 0
                    except ValueError:
                        current_index = 0
                    
                    new_algorithm = st.selectbox(
                        "Algorithm",
                        active_algorithms,
                        index=current_index,
                        key=f"algo_select_formatted_{change_key}_{idx}",
                        label_visibility="collapsed",
                        help=f"Select masking algorithm for {column_name}"
                    )
                
                with remove_col:
                    # X button to remove assigned algorithm (only show if there's an algorithm currently displayed)
                    show_x_button = display_algorithm and display_algorithm.strip()
                    if show_x_button:
                        if st.button("✖", key=f"remove_algo_{change_key}_{idx}", help=f"Remove assigned algorithm for {column_name}", use_container_width=True):
                            # Set to remove the algorithm (set to empty/None)
                            st.session_state.algorithm_changes[change_key] = {
                                'table_name': table_name,
                                'column_name': column_name,
                                'old_algorithm': current_assigned,
                                'new_algorithm': ''  # Empty string means remove/NULL
                            }
                            st.rerun()
                    else:
                        # Empty space when no algorithm is assigned
                        st.write("")
                
                # Track changes from dropdown
                if new_algorithm != current_assigned:
                    st.session_state.algorithm_changes[change_key] = {
                        'table_name': table_name,
                        'column_name': column_name,
                        'old_algorithm': current_assigned,
                        'new_algorithm': new_algorithm
                    }
                elif change_key in st.session_state.algorithm_changes:
                    # Remove from changes if reverted to original (unless it's a removal action)
                    if st.session_state.algorithm_changes[change_key]['new_algorithm'] != '':
                        del st.session_state.algorithm_changes[change_key]
        
        # Close the container
        st.html('</div>')
        
        # Add pagination controls at the bottom
        if total_pages > 1:
            col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
            
            with col1:
                if st.button("⏮️", disabled=(current_page == 1), key="first_page_bottom_formatted"):
                    st.session_state.discovery_page = 1
                    st.rerun()
            
            with col2:
                if st.button("⬅️", disabled=(current_page == 1), key="prev_page_bottom_formatted"):
                    st.session_state.discovery_page = current_page - 1
                    st.rerun()
            
            with col3:
                st.write(f"**Page {current_page} of {total_pages}** ({total_results} total results)")
            
            with col4:
                if st.button("➡️", disabled=(current_page == total_pages), key="next_page_bottom_formatted"):
                    st.session_state.discovery_page = current_page + 1
                    st.rerun()
            
            with col5:
                if st.button("⏭️", disabled=(current_page == total_pages), key="last_page_bottom_formatted"):
                    st.session_state.discovery_page = total_pages
                    st.rerun()
        
        # Show current page info
        if total_pages > 1:
            start_record = start_idx + 1
            end_record = min(end_idx, total_results)
            st.caption(f"Showing records {start_record}-{end_record} of {total_results} results (Page {current_page} of {total_pages})")
        else:
            st.caption(f"Showing {len(display_df)} of {len(discovery_df)} discovery results")
    else:
        st.warning("No results match the current filters.")
    
    # Show summary of changes and submit button
    if st.session_state.algorithm_changes:
        st.divider()
        
        # Show pending changes summary
        st.write(f"**Pending Changes:** {len(st.session_state.algorithm_changes)} algorithm assignments")
        
        # Show change details
        with st.expander("View Change Details", expanded=False):
            for change_key, change_info in st.session_state.algorithm_changes.items():
                old_algo = change_info['old_algorithm'] or "(None)"
                new_algo = change_info['new_algorithm'] or "(Clear/NULL)"
                st.write(f"• **{change_info['table_name']}.{change_info['column_name']}**: {old_algo} → {new_algo}")
        
        # Action buttons in a single row with proper spacing
        submit_col, spacer_col, reset_col = st.columns([4, 0.2, 1])
        
        with submit_col:
            if st.button("💾 Submit Algorithm Changes", type="primary", use_container_width=True):
                # Convert changes to update list
                algorithm_updates = list(st.session_state.algorithm_changes.values())
                
                with st.spinner("Updating algorithm assignments..."):
                    result = batch_update_assigned_algorithms(session, database, schema, algorithm_updates)
                
                if result['success']:
                    st.success(f"✅ Successfully updated {result['updates_made']} algorithm assignments!")
                    if result['errors']:
                        st.warning(f"⚠️ {len(result['errors'])} errors occurred:")
                        for error in result['errors']:
                            st.error(error)
                    
                    # Clear changes
                    st.session_state.algorithm_changes = {}
                    st.rerun()
                else:
                    st.error(f"❌ Failed to update algorithms: {result.get('error', 'Unknown error')}")
        
        with spacer_col:
            st.write("")  # Empty spacer column
        
        with reset_col:
            if st.button("🔄", help="Reset Changes", use_container_width=True):
                st.session_state.algorithm_changes = {}
                st.rerun()
    else:
        st.info("💡 **To make changes**: Use the dropdown menus in the 'Assigned Algorithm' column above to modify algorithm assignments. The 'Submit Algorithm Changes' button will appear once you make changes.")


def display_monitoring_events_table(session):
    """Display monitoring events table with exact Discovery Results design."""
    import streamlit as st
    import pandas as pd
    from .metadata_store import METADATA_CONFIG
    
    # Get all events data ordered by execution_start_time descending
    try:
        events_query = f"""
        SELECT 
            execution_id,
            run_id,
            run_status,
            run_type,
            execution_start_time,
            execution_end_time,
            source_database,
            source_schema,
            source_table,
            dest_database,
            dest_schema,
            dest_table,
            error_message
        FROM {METADATA_CONFIG["dcs_events_log"]}
        ORDER BY execution_start_time DESC
        """
        original_df = session.sql(events_query).to_pandas()
        
    except Exception as e:
        st.error(f"Error loading monitoring events: {str(e)}")
        return
    
    if original_df.empty:
        st.info("🔍 **No monitoring events found.** Execute discovery or masking operations to see job tracking data.")
        return
    
    st.subheader("🔍 Job Monitoring Events")
    st.write(f"Found **{len(original_df)}** job events in the system")
    
    # Initialize session state for filters, sorting, and pagination
    if "monitoring_filters" not in st.session_state:
        st.session_state.monitoring_filters = {
            "execution_id": "",
            "run_status": "",
            "source_table": "",
            "target_table": ""
        }
    
    if "monitoring_sort" not in st.session_state:
        st.session_state.monitoring_sort = {
            "column": "Execution Start Time",
            "ascending": False
        }
    
    if "monitoring_page" not in st.session_state:
        st.session_state.monitoring_page = 1
    
    # Reset page to 1 when filters change
    page_size = 15
    
    # Format DataFrame for display
    display_df = original_df.copy()
    display_df = display_df.rename(columns={
        "EXECUTION_ID": "Execution ID",
        "RUN_ID": "Run ID", 
        "RUN_STATUS": "Status",
        "RUN_TYPE": "Type",
        "EXECUTION_START_TIME": "Start Time",
        "EXECUTION_END_TIME": "End Time",
        "SOURCE_DATABASE": "Source DB",
        "SOURCE_SCHEMA": "Source Schema",
        "SOURCE_TABLE": "Source Table",
        "DEST_DATABASE": "Target DB",
        "DEST_SCHEMA": "Target Schema", 
        "DEST_TABLE": "Target Table",
        "ERROR_MESSAGE": "Error Message"
    })
    
    # Apply filters
    filtered_df = display_df.copy()
    
    # Case-insensitive filtering
    if st.session_state.monitoring_filters["execution_id"]:
        filtered_df = filtered_df[filtered_df["Execution ID"].str.contains(st.session_state.monitoring_filters["execution_id"], case=False, na=False)]
    
    if st.session_state.monitoring_filters["run_status"]:
        filtered_df = filtered_df[filtered_df["Status"].str.contains(st.session_state.monitoring_filters["run_status"], case=False, na=False)]
    
    if st.session_state.monitoring_filters["source_table"]:
        filtered_df = filtered_df[filtered_df["Source Table"].str.contains(st.session_state.monitoring_filters["source_table"], case=False, na=False)]
    
    if st.session_state.monitoring_filters["target_table"]:
        filtered_df = filtered_df[filtered_df["Target Table"].fillna("").str.contains(st.session_state.monitoring_filters["target_table"], case=False, na=False)]
    
    # Apply sorting - default by Start Time descending
    if not filtered_df.empty:
        filtered_df = filtered_df.sort_values(
            by="Start Time", 
            ascending=False,
            na_position="last"
        )
    
    # Reset index after filtering and sorting
    filtered_df = filtered_df.reset_index(drop=True)
    
    # Calculate pagination
    total_results = len(filtered_df)
    total_pages = (total_results + page_size - 1) // page_size  # Ceiling division
    current_page = st.session_state.monitoring_page
    
    # Ensure current page is valid
    if current_page > total_pages:
        st.session_state.monitoring_page = 1
        current_page = 1
    
    # Show filtered results count
    if len(filtered_df) != len(display_df):
        st.write(f"Showing **{len(filtered_df)}** of **{len(display_df)}** results")
    
    if filtered_df.empty:
        st.warning("🔍 No results match the current filters.")
        return
    
    # Add pagination controls at the top
    if total_pages > 1:
        col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
        
        with col1:
            if st.button("⏮️", disabled=(current_page == 1), key="mon_first"):
                st.session_state.monitoring_page = 1
                st.rerun()
        
        with col2:
            if st.button("⬅️", disabled=(current_page == 1), key="mon_prev"):
                st.session_state.monitoring_page = current_page - 1
                st.rerun()
        
        with col3:
            st.write(f"**Page {current_page} of {total_pages}** ({total_results} total results)")
        
        with col4:
            if st.button("➡️", disabled=(current_page == total_pages), key="mon_next"):
                st.session_state.monitoring_page = current_page + 1
                st.rerun()
        
        with col5:
            if st.button("⏭️", disabled=(current_page == total_pages), key="mon_last"):
                st.session_state.monitoring_page = total_pages
                st.rerun()
    
    # Add the same CSS styling as Discovery Results
    st.html("""
    <style>
        .monitoring-table {
            width: 100%;
            border-collapse: collapse;
            font-family: "Inter", sans-serif;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        .monitoring-table th {
            background: #f8f9fa;
            color: #495057;
            font-weight: 600;
            text-align: left;
            padding: 8px 6px;
            border-bottom: 2px solid #dee2e6;
            border-right: 1px solid #dee2e6;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .monitoring-table td {
            padding: 8px 6px;
            border-bottom: 1px solid #f1f3f4;
            border-right: 1px solid #f1f3f4;
            vertical-align: middle;
            word-wrap: break-word;
            overflow-wrap: break-word;
            max-width: 0;
        }
        
        .monitoring-results-container {
            border: 1px solid #dee2e6;
            border-radius: 8px;
            margin: 0;
            background: white;
            padding: 8px;
        }
        
        .monitoring-table tr:hover {
            background-color: #f8f9fa;
        }
        
        .monitoring-table th:last-child,
        .monitoring-table td:last-child {
            border-right: none;
        }
        
        /* Column widths for monitoring table */
        .monitoring-table {
            table-layout: fixed;
        }
        
        .col-exec-id { 
            width: 14%; 
            font-size: 11px;
        }
        .col-run-id { 
            width: 16%; 
            font-size: 11px;
        }
        .col-status { 
            width: 8%; 
            text-align: center;
        }
        .col-type { 
            width: 10%; 
            font-size: 11px;
        }
        .col-start-time { 
            width: 13%; 
            font-size: 11px;
        }
        .col-end-time { 
            width: 13%; 
            font-size: 11px;
        }
        .col-source-table { 
            width: 13%; 
            font-size: 12px;
            font-weight: 500;
        }
        .col-target-table { 
            width: 13%; 
            font-size: 12px;
            font-weight: 500;
        }
        
        /* Status colors */
        .status-completed {
            color: #28a745;
            font-weight: 600;
            background-color: rgba(40, 167, 69, 0.1);
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 11px;
        }
        
        .status-failed {
            color: #dc3545;
            font-weight: 600;
            background-color: rgba(220, 53, 69, 0.1);
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 11px;
        }
        
        .status-in-progress {
            color: #007bff;
            font-weight: 600;
            background-color: rgba(0, 123, 255, 0.1);
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 11px;
        }
        
        .status-waiting {
            color: #ffc107;
            font-weight: 600;
            background-color: rgba(255, 193, 7, 0.1);
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 11px;
        }
        
        /* Filter styling matching Discovery Results */
        .stTextInput > div > div > input {
            font-size: 12px !important;
            padding: 6px 8px !important;
            height: 28px !important;
            border: 1px solid #dee2e6 !important;
            border-radius: 4px !important;
            background: #f8f9fa !important;
        }
        
        .stTextInput > div > div > input:focus {
            border-color: #04a9f5 !important;
            background: white !important;
            box-shadow: 0 0 0 2px rgba(4, 169, 245, 0.1) !important;
        }
    </style>
    """)
    
    # Create scrollable container for the table
    st.html("<div class=\"monitoring-results-container\">")
    
    # Create the table headers
    st.html("""
    <table class="monitoring-table">
        <thead>
            <tr>
                <th class="col-exec-id">EXECUTION ID</th>
                <th class="col-run-id">RUN ID</th>
                <th class="col-status">STATUS</th>
                <th class="col-type">TYPE</th>
                <th class="col-start-time">START TIME</th>
                <th class="col-end-time">END TIME</th>
                <th class="col-source-table">SOURCE TABLE</th>
                <th class="col-target-table">TARGET TABLE</th>
            </tr>
        </thead>
    </table>
    """)
    
    # Add filter inputs below headers  
    filter_col1, filter_col2, filter_col3, filter_col4, filter_col5, filter_col6, filter_col7, filter_col8 = st.columns([0.14, 0.16, 0.08, 0.10, 0.13, 0.13, 0.13, 0.13])
    
    with filter_col1:
        exec_id_filter = st.text_input(
            "Filter Execution ID",
            value=st.session_state.monitoring_filters.get("execution_id", ""),
            key="exec_id_filter_key",
            placeholder="Filter...",
            label_visibility="collapsed"
        )
        if exec_id_filter != st.session_state.monitoring_filters.get("execution_id", ""):
            st.session_state.monitoring_filters["execution_id"] = exec_id_filter
            st.session_state.monitoring_page = 1
            st.rerun()
    
    with filter_col2:
        st.write("")  # Run ID - not filterable for simplicity
    
    with filter_col3:
        status_filter = st.text_input(
            "Filter Status",
            value=st.session_state.monitoring_filters.get("run_status", ""),
            key="status_filter_key",
            placeholder="Filter...",
            label_visibility="collapsed"
        )
        if status_filter != st.session_state.monitoring_filters.get("run_status", ""):
            st.session_state.monitoring_filters["run_status"] = status_filter
            st.session_state.monitoring_page = 1
            st.rerun()
    
    with filter_col4:
        st.write("")  # Type - not filterable for simplicity
    
    with filter_col5:
        st.write("")  # Start Time - not filterable for simplicity
    
    with filter_col6:
        st.write("")  # End Time - not filterable for simplicity
    
    with filter_col7:
        source_table_filter = st.text_input(
            "Filter Source Table",
            value=st.session_state.monitoring_filters.get("source_table", ""),
            key="source_table_filter_key",
            placeholder="Filter...",
            label_visibility="collapsed"
        )
        if source_table_filter != st.session_state.monitoring_filters.get("source_table", ""):
            st.session_state.monitoring_filters["source_table"] = source_table_filter
            st.session_state.monitoring_page = 1
            st.rerun()
    
    with filter_col8:
        target_table_filter = st.text_input(
            "Filter Target Table",
            value=st.session_state.monitoring_filters.get("target_table", ""),
            key="target_table_filter_key",
            placeholder="Filter...",
            label_visibility="collapsed"
        )
        if target_table_filter != st.session_state.monitoring_filters.get("target_table", ""):
            st.session_state.monitoring_filters["target_table"] = target_table_filter
            st.session_state.monitoring_page = 1
            st.rerun()
    
    # Calculate the data for current page
    start_idx = (current_page - 1) * page_size
    end_idx = start_idx + page_size
    page_df = filtered_df.iloc[start_idx:end_idx]
    
    # Create complete HTML table
    table_rows = []
    
    for idx, row in page_df.iterrows():
        # Format status with colored badge
        status = row["Status"]
        if status == "COMPLETED":
            status_html = f"<span class=\"status-completed\">{status}</span>"
        elif status == "FAILED":
            status_html = f"<span class=\"status-failed\">{status}</span>"
        elif status == "IN PROGRESS":
            status_html = f"<span class=\"status-in-progress\">{status}</span>"
        elif status == "WAITING":
            status_html = f"<span class=\"status-waiting\">{status}</span>"
        else:
            status_html = f"<span>{status}</span>"
        
        # Format timestamps
        start_time = str(row["Start Time"])[:19] if pd.notna(row["Start Time"]) else "-"
        end_time = str(row["End Time"])[:19] if pd.notna(row["End Time"]) else "-"
        
        # Format target table (handle NaN for in-place masking)
        target_table = row["Target Table"] if pd.notna(row["Target Table"]) and str(row["Target Table"]) != "nan" else "-"
        
        # Escape any HTML characters in the data
        exec_id = str(row["Execution ID"]).replace("<", "&lt;").replace(">", "&gt;")
        run_id = str(row["Run ID"]).replace("<", "&lt;").replace(">", "&gt;")
        job_type = str(row["Type"]).replace("<", "&lt;").replace(">", "&gt;")
        source_table = str(row["Source Table"]).replace("<", "&lt;").replace(">", "&gt;")
        target_table = str(target_table).replace("<", "&lt;").replace(">", "&gt;")
        
        table_rows.append(f"""
        <tr>
            <td class="col-exec-id">{exec_id}</td>
            <td class="col-run-id">{run_id}</td>
            <td class="col-status">{status_html}</td>
            <td class="col-type">{job_type}</td>
            <td class="col-start-time">{start_time}</td>
            <td class="col-end-time">{end_time}</td>
            <td class="col-source-table">{source_table}</td>
            <td class="col-target-table">{target_table}</td>
        </tr>
        """)
    
    # Render the complete table
    complete_table_html = f"""
    <table class="monitoring-table">
        <tbody>
            {''.join(table_rows)}
        </tbody>
    </table>
    """
    
    st.html(complete_table_html)
    st.html("</div>")
    
    # Show pagination again at bottom if needed
    if total_pages > 1:
        st.write("")
        col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
        
        with col1:
            if st.button("⏮️", disabled=(current_page == 1), key="mon_first_bottom"):
                st.session_state.monitoring_page = 1
                st.rerun()
        
        with col2:
            if st.button("⬅️", disabled=(current_page == 1), key="mon_prev_bottom"):
                st.session_state.monitoring_page = current_page - 1
                st.rerun()
        
        with col3:
            st.write(f"**Page {current_page} of {total_pages}**")
        
        with col4:
            if st.button("➡️", disabled=(current_page == total_pages), key="mon_next_bottom"):
                st.session_state.monitoring_page = current_page + 1
                st.rerun()
        
        with col5:
            if st.button("⏭️", disabled=(current_page == total_pages), key="mon_last_bottom"):
                st.session_state.monitoring_page = total_pages
                st.rerun()

