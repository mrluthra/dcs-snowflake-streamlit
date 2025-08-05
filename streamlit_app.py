#!/usr/bin/env python3
"""
Delphix Compliance Service (DCS) - Powered by Perforce
Snowflake Data Discovery & Masking Application - REFACTORED VERSION

This is the refactored main application file that imports and uses modular components.
The original monolithic 6,927-line file has been broken down into manageable modules.

SETUP REQUIREMENTS:
1. Create External Access Integration (run as ACCOUNTADMIN)
2. Upload this file to a Snowflake stage  
3. Create Streamlit app with external access integration
4. Configure DCS credentials and run workflows

See setup instructions in the app sidebar for complete SQL commands.
"""

import streamlit as st
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Import modular components
from modules.dcs_client import DCSConfig, DCSAPIClient
from modules.snowflake_ops import get_snowflake_session, get_environment_config
from modules.metadata_store import (
    ensure_metadata_store_table, 
    insert_table_metadata,
    get_discovery_metadata,
    load_algorithms_from_database,
    update_assigned_algorithm
)
from modules.business_engines import (
    discover_table_parallel,
    execute_masking_workflow,
    process_single_table_masking
)
from modules.job_manager import (
    generate_execution_id,
    get_job_statistics,
    get_events_log,
    create_metrics_dashboard,
    log_discovery_job_start,
    update_execution_end_time_for_all,
    log_masking_job_start,
    log_masking_job_in_progress,
    log_masking_job_completion
)
from modules.ui_components import (
    apply_custom_css,
    create_feature_card,
    source_target_inputs,
    display_discovery_results,
    create_progress_tracker,
    display_operation_results,
    test_external_access,
    create_status_badge,
    create_professional_container,
    get_current_page,
    get_current_subpage,
    create_dattaable_layout,
    create_page_header,
    display_existing_discovery_results,
    display_available_tables,
    display_filtered_existing_discovery_results,
    display_existing_discovery_results_formatted,
    display_masking_discovery_results_formatted,
    display_monitoring_events_table
)
# Import constants - handle both local and Snowflake environments
try:
    from config.constants import PAGE_CONFIG, DEFAULT_AZURE_SCOPE
except ImportError:
    # Fallback for Snowflake environment
    PAGE_CONFIG = {
        "page_title": "Delphix Compliance Service - Powered by Perforce",
        "page_icon": "🛡️",
        "layout": "wide",
        "initial_sidebar_state": "expanded"
    }
    DEFAULT_AZURE_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

# Configure page
st.set_page_config(**PAGE_CONFIG)

# Apply custom styling
apply_custom_css()


def init_session_state():
    """Initialize session state variables."""
    
    # Core session variables
    if 'snowflake_session' not in st.session_state:
        st.session_state.snowflake_session = get_snowflake_session()
    
    if 'environment_config' not in st.session_state:
        st.session_state.environment_config = get_environment_config()
    
    # DCS configuration
    if 'dcs_config' not in st.session_state:
        st.session_state.dcs_config = None
        
    if 'dcs_client' not in st.session_state:
        st.session_state.dcs_client = None
    
    # Execution tracking
    if 'current_execution_id' not in st.session_state:
        st.session_state.current_execution_id = generate_execution_id()
    
    # Discovery results
    if 'discovery_results' not in st.session_state:
        st.session_state.discovery_results = {}
    
    # Selected data
    if 'selected_source_tables' not in st.session_state:
        st.session_state.selected_source_tables = []
    
    # Table selection variables (for new functionality)
    if 'selected_tables' not in st.session_state:
        st.session_state.selected_tables = []
    
    if 'table_sample_sizes' not in st.session_state:
        st.session_state.table_sample_sizes = {}
    
    # Discovery filters
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
    
    if 'algorithm_changes' not in st.session_state:
        st.session_state.algorithm_changes = {}
    
    # Table sorting variables
    if 'table_sort_column' not in st.session_state:
        st.session_state.table_sort_column = 'TABLE_NAME'
    
    if 'table_sort_ascending' not in st.session_state:
        st.session_state.table_sort_ascending = True


def sidebar_configuration():
    """Create sidebar with global settings and configuration."""
    
    with st.sidebar:
        st.html("""
        <div style="text-align: center; padding: 1rem 0; border-bottom: 1px solid var(--border-color); margin-bottom: 1rem;">
            <h2 style="color: var(--primary-color); margin: 0; font-weight: 600;">
                <i class="fas fa-cog"></i> Configuration
            </h2>
        </div>
        """)
        
        # DCS API Configuration
        with st.expander("🔧 DCS API Settings", expanded=True):
            dcs_api_url = st.text_input(
                "DCS API URL",
                help="Enter your DCS API endpoint URL"
            )
            
            azure_tenant_id = st.text_input(
                "Azure Tenant ID",
                type="password",
                help="Azure AD tenant ID for authentication"
            )
            
            azure_client_id = st.text_input(
                "Azure Client ID", 
                type="password",
                help="Azure AD application client ID"
            )
            
            azure_client_secret = st.text_input(
                "Azure Client Secret",
                type="password", 
                help="Azure AD application client secret"
            )
            
            azure_scope = st.text_input(
                "Azure Scope",
                value=DEFAULT_AZURE_SCOPE,
                help="Azure AD scope for API access"
            )
            
            if st.button("💾 Save Configuration", type="primary", use_container_width=True):
                if all([dcs_api_url, azure_tenant_id, azure_client_id, azure_client_secret]):
                    # Create DCS configuration
                    st.session_state.dcs_config = DCSConfig(
                        dcs_api_url=dcs_api_url,
                        azure_tenant_id=azure_tenant_id,
                        azure_client_id=azure_client_id,
                        azure_client_secret=azure_client_secret,
                        azure_scope=azure_scope
                    )
                    
                    # Create DCS client
                    st.session_state.dcs_client = DCSAPIClient(st.session_state.dcs_config)
                    
                    st.success("✅ Configuration saved!")
                    st.rerun()
                else:
                    st.error("❌ Please fill in all required fields")
        
        # Test connectivity if configured
        if st.session_state.dcs_client:
            st.divider()
            test_external_access(st.session_state.dcs_client)
        
        # Environment info
        st.divider()
        st.html("""
        <div style="margin: 1rem 0;">
            <h3 style="color: var(--primary-color); margin: 0 0 1rem 0; font-weight: 600;">
                <i class="fas fa-server"></i> Environment Info
            </h3>
        </div>
        """)
        env_config = st.session_state.environment_config
        st.info(f"**Environment**: {env_config.get('environment', 'Unknown')}")
        st.info(f"**Execution ID**: {st.session_state.current_execution_id}")
        
        # Reset button
        if st.button("🔄 New Execution ID", use_container_width=True):
            st.session_state.current_execution_id = generate_execution_id()
            st.rerun()


def discovery_content():
    """Discovery page content."""
    
    create_feature_card(
        "Intelligent Discovery",
        "Automatically identify sensitive data patterns across your Snowflake tables using advanced ML algorithms.",
        "data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjQiIGhlaWdodD0iMjQiIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTEwIDJDMTQuNDE4MyAyIDE4IDUuNTgxNzIgMTggMTBDMTggMTEuODQ4NyAxNy40MjkzIDEzLjU1MTIgMTYuNDczMSAxNC45MzMxTDIwLjc3MTkgMTkuMjMxOUMyMS4xNjI0IDE5LjYyMjQgMjEuMTYyNCAyMC4yNTU2IDIwLjc3MTkgMjAuNjQ2MUMyMC4zODE0IDIxLjAzNjYgMTkuNzQ4MiAyMS4wMzY2IDE5LjM1NzggMjAuNjQ2MUwxNS4wNiAxNi4zNDgzQzEzLjY3NzggMTcuMzA0NSAxMi4wMDA5IDE3Ljg3NSAxMC4xMjUgMTcuODc1QzUuNzA2NzIgMTcuODc1IDIuMTI1IDE0LjI5MzMgMi4xMjUgOS44NzVDMi4xMjUgNS40NTY3MiA1LjcwNjcyIDIgMTAuMTI1IDJaTTEwLjEyNSA0QzYuODExMjIgNCA0LjEyNSA2LjY4NjI4IDQuMTI1IDEwQzQuMTI1IDEzLjMxMzcgNi44MTEyMiAxNiAxMC4xMjUgMTZDMTMuNDM4OCAxNiAxNi4xMjUgMTMuMzEzNyAxNi4xMjUgMTBDMTYuMTI1IDYuNjg2MjggMTMuNDM4OCA0IDEwLjEyNSA0WiIgZmlsbD0iIzAwMDAwMCIvPgo8L3N2Zz4K",
        "primary"
    )
    
    if not st.session_state.get('dcs_discovery_client'):
        st.warning("⚠️ Please configure Discovery API settings in Configuration tab first.")
        return
    
    session = st.session_state.snowflake_session
    if not session:
        st.error("❌ Snowflake session not available")
        return
    
    # Source configuration
    source_db, source_schema, _ = source_target_inputs(session, "source")
    
    # Available Tables Selection and Discovery Results (when database and schema are selected)
    if source_db and source_schema:
        # Available Tables Selection - display directly without container wrapper
        display_available_tables(session, source_db, source_schema)
        
        # Get selected tables from session state for discovery results display
        selected_tables = []
        if 'selected_tables_for_discovery' in st.session_state:
            for table_key, is_selected in st.session_state.selected_tables_for_discovery.items():
                if is_selected and table_key.startswith(f"{source_db}.{source_schema}."):
                    table_name = table_key.split(".")[-1]
                    sample_size = st.session_state.get('table_sample_sizes', {}).get(table_key, 1000)
                    selected_tables.append({
                        'table_name': table_name,
                        'sample_size': sample_size
                    })
        
        # Show existing discovery results for the selected tables - display directly without container wrapper
        try:
            display_filtered_existing_discovery_results(session, source_db, source_schema, selected_tables)
        except Exception as e:
            st.error(f"Error loading discovery results: {str(e)}")
    
    # Discovery execution section
    if source_db and source_schema:
        
        # Discovery settings - display directly without container wrapper
        if selected_tables:
            st.info(f"📝 Discovery will process {len(selected_tables)} selected table(s) with their individual sample sizes.")
            
            # Show selected tables summary
            st.write("**Selected Tables:**")
            for table in selected_tables:
                st.write(f"• {table['table_name']} ({table['sample_size']} rows)")
        else:
            st.warning("⚠️ No tables selected. Please select tables above to run discovery.")
        
        max_workers = st.number_input(
            "Parallel Workers",
            min_value=1,
            max_value=25,
            value=8,
            help="Number of tables to process in parallel"
        )
        
        # Store values in session state
        st.session_state.temp_max_workers_discovery = max_workers
        
        # Discovery execution button
        if st.button("🚀 Start Discovery", type="primary", use_container_width=True):
            
            # Check if tables are selected
            if not selected_tables:
                st.error("❌ No tables selected. Please select tables above to run discovery.")
                return
            
            # Get selected table names
            selected_table_names = [table['table_name'] for table in selected_tables]
            
            # Step 1: Generate execution_id and display it
            execution_id = generate_execution_id()
            st.info(f"🆔 **Execution ID**: `{execution_id}`")
            st.session_state.current_execution_id = execution_id
            
            # Ensure metadata table exists
            if not ensure_metadata_store_table(session):
                st.error("❌ Metadata store not accessible")
                return
            
            # Insert table metadata
            st.info("📝 Updating table metadata...")
            metadata_result = insert_table_metadata(session, source_db, source_schema, selected_table_names)
            
            if not metadata_result['success']:
                st.error(f"❌ Failed to update metadata: {metadata_result['error']}")
                return
            
            st.success(f"✅ Metadata updated: {metadata_result['columns_processed']} columns processed")
            
            # Step 2: Insert WAITING entries for all tables
            st.info("📝 Logging discovery jobs...")
            table_run_ids = {}
            for table_info in selected_tables:
                table_name = table_info['table_name']
                run_id = log_discovery_job_start(session, execution_id, source_db, source_schema, table_name)
                if run_id:
                    table_run_ids[table_name] = run_id
            
            st.success(f"✅ Logged {len(table_run_ids)} discovery jobs with status WAITING")
            
            # Step 3: Execute discovery
            st.info("🔍 Starting parallel discovery process...")
            
            progress_placeholder = st.empty()
            results_placeholder = st.empty()
            
            # Process tables in parallel
            with ThreadPoolExecutor(max_workers=st.session_state.get('temp_max_workers_discovery', 3)) as executor:
                
                # Submit discovery jobs with individual sample sizes
                future_to_table = {}
                
                for table_info in selected_tables:
                    table_name = table_info['table_name']
                    table_sample_size = table_info['sample_size']
                    run_id = table_run_ids.get(table_name)
                    
                    future = executor.submit(
                        discover_table_parallel,
                        session,
                        st.session_state.dcs_discovery_client,
                        source_db,
                        source_schema,
                        table_name,
                        execution_id,
                        table_sample_size,
                        run_id
                    )
                    future_to_table[future] = table_name
                
                # Collect results
                completed = 0
                results = []
                
                for future in future_to_table:
                    table_name = future_to_table[future]
                    
                    with progress_placeholder.container():
                        create_progress_tracker(
                            "Discovery",
                            completed,
                            len(selected_tables),
                            f"Processing {table_name}..."
                        )
                    
                    try:
                        result = future.result()
                        results.append(result)
                        completed += 1
                        
                        # Update progress
                        with progress_placeholder.container():
                            create_progress_tracker(
                                "Discovery", 
                                completed,
                                len(selected_tables),
                                f"Completed {table_name}"
                            )
                        
                    except Exception as exc:
                        st.error(f"❌ {table_name}: {str(exc)}")
                        results.append({
                            'table': table_name,
                            'success': False,
                            'error': str(exc)
                        })
                        completed += 1
            
            # Step 5: Update execution_end_time for all entries
            st.info("✅ Finalizing execution logs...")
            update_execution_end_time_for_all(session, execution_id)
            
            # Store results
            st.session_state.discovery_results = {
                'source_db': source_db,
                'source_schema': source_schema,
                'tables': selected_table_names,
                'results': results,
                'execution_id': execution_id
            }
            
            # Display results
            with results_placeholder.container():
                display_operation_results(results, "Discovery")
            
                st.success("🎉 Discovery process completed!")


def masking_overview_content():
    """Masking overview page content."""
    
    # Overview of masking options
    st.write("Choose a masking approach:")
    
    col1, col2 = st.columns(2)
    
    with col1:
        create_feature_card(
            "Mask & Deliver", 
            "Create masked copies of your data in destination tables while preserving original data integrity.",
            "➤",
            "success"
        )
        if st.button("Go to Mask & Deliver", key="nav_to_mask_deliver", type="primary", use_container_width=True):
            st.session_state.current_subpage = 'mask_deliver'
            st.rerun()
    
    with col2:
        create_feature_card(
            "In-Place Masking",
            "Directly mask sensitive data in your existing tables. ⚠️ This operation modifies original data permanently.",
            "fas fa-bolt",
            "warning"
        )
        if st.button("Go to In-Place Masking", key="nav_to_in_place", type="primary", use_container_width=True):
            st.session_state.current_subpage = 'in_place'
            st.rerun()


def mask_and_deliver_content():
    """Mask and deliver page content."""
    
    create_feature_card(
        "Mask & Deliver", 
        "Create masked copies of your data in destination tables while preserving original data integrity.",
        "➤",
        "success"
    )
    
    if not st.session_state.get('dcs_masking_client'):
        st.warning("⚠️ Please configure Masking API settings in Configuration tab first.")
        return
    
    session = st.session_state.snowflake_session
    if not session:
        st.error("❌ Snowflake session not available")
        return
    
    # Configuration section in professional container
    def configuration_content():
        col1, col2 = st.columns(2)
        
        with col1:
            source_db, source_schema, _ = source_target_inputs(session, "source")
        
        with col2:
            dest_db, dest_schema, _ = source_target_inputs(session, "target")
        
        return source_db, source_schema, dest_db, dest_schema
    
    source_db, source_schema, dest_db, dest_schema = configuration_content()
    
    if source_db and source_schema and dest_db and dest_schema:
        
        # Display masking rules directly
        all_discovery_df = get_discovery_metadata(session, source_db, source_schema)
        
        if not all_discovery_df.empty:
            # Display discovery results using the same formatted design as the Discovery page
            display_masking_discovery_results_formatted(all_discovery_df, source_db, source_schema)
        else:
            st.warning("⚠️ No discovery metadata found. Please run discovery first.")
        
        # Check if we have discovery metadata for the schema
        has_metadata = not get_discovery_metadata(session, source_db, source_schema).empty
        
        if has_metadata:
            # Get unique table names from discovery results for validation
            discovery_df = get_discovery_metadata(session, source_db, source_schema)
            tables_with_discovery = discovery_df['IDENTIFIED_TABLE'].unique().tolist() if not discovery_df.empty else []
            
            # Check target table existence
            if tables_with_discovery:
                existing_tables = []
                missing_tables = []
                
                for table_name in tables_with_discovery:
                    # Check if target table exists
                    table_exists = False
                    try:
                        check_query = f"""
                        SELECT COUNT(*) as table_count 
                        FROM {dest_db}.INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_NAME = '{table_name}' 
                        AND TABLE_SCHEMA = '{dest_schema}'
                        """
                        result = session.sql(check_query).collect()
                        table_exists = result[0]['TABLE_COUNT'] > 0 if result else False
                    except:
                        table_exists = False
                    
                    if table_exists:
                        existing_tables.append(table_name)
                    else:
                        missing_tables.append(table_name)
                
                # Summary
                st.write("### 📊 **Target Table Summary**")
                if existing_tables:
                    st.success(f"✅ **Existing tables**: {len(existing_tables)} tables - {existing_tables}")
                if missing_tables:
                    st.info(f"🔧 **Tables to be created**: {len(missing_tables)} tables - {missing_tables}")
                    st.info("   💡 Missing tables will be created automatically with source table structure during masking workflow")
            
            # Execution settings displayed directly like in Discovery page
            max_workers = st.number_input(
                "Parallel Workers",
                min_value=1,
                max_value=4,
                value=2,
                help="Number of tables to process in parallel (reduced for Snowflake memory limits)"
            )
            
            st.info("ℹ️ **Batch Size**: Automatically calculated based on sensitive column sizes (1.8MB limit with safety buffer)")
            
            # Write mode selection
            st.markdown("**📝 Write Mode:**")
            write_mode_option = st.radio(
                "Select how to handle existing data in destination tables:",
                options=[
                    "Clean up destination table (DELETE all data first)",
                    "Append to destination table (keep existing data)"
                ],
                index=0,  # Default to first option (clean up)
                help="Choose whether to clear existing data before loading masked data or append to existing data"
            )
            
            # Store values in session state to access them
            st.session_state.temp_max_workers = max_workers
            st.session_state.temp_write_mode = "overwrite" if "Clean up" in write_mode_option else "append"
            
            # Masking execution button
            if st.button("🚀 Start Masking Workflow", type="primary", use_container_width=True):
                
                # Step 1: Generate execution_id and display it
                execution_id = generate_execution_id()
                st.info(f"🆔 **Execution ID**: `{execution_id}`")
                st.session_state.current_execution_id = execution_id
                
                # Get all tables that have discovery results
                discovery_df = get_discovery_metadata(session, source_db, source_schema)
                if discovery_df.empty:
                    st.error("❌ No discovery results found. Please run discovery first.")
                    return
                
                # Get unique table names from discovery results
                tables_with_discovery = discovery_df['IDENTIFIED_TABLE'].unique().tolist()
                
                # Step 2: Initialize job tracking
                table_run_ids = {}
                for table_name in tables_with_discovery:
                    run_id = log_masking_job_start(session, execution_id, source_db, source_schema, table_name, dest_db, dest_schema)
                    if run_id:
                        table_run_ids[table_name] = run_id
                
                # Step 3: Pre-execution validation (simplified)
                tables_ready_for_masking = []
                tables_with_issues = []
                
                for table_name in tables_with_discovery:
                    # Get discovery metadata for this table
                    table_discovery_df = get_discovery_metadata(session, source_db, source_schema, table_name)
                    
                    if table_discovery_df.empty:
                        tables_with_issues.append(table_name)
                        continue
                    
                    # Analyze column assignments
                    assigned_columns = table_discovery_df[
                        table_discovery_df['ASSIGNED_ALGORITHM'].notna() & 
                        (table_discovery_df['ASSIGNED_ALGORITHM'] != '')
                    ]
                    num_assigned = len(assigned_columns)
                    
                    if num_assigned > 0:
                        tables_ready_for_masking.append(table_name)
                    else:
                        tables_with_issues.append(table_name)
                
                # Step 4: Create missing target tables (simplified output)
                tables_created = []
                table_creation_errors = []
                
                from modules.business_engines import create_target_table_with_source_structure
                
                for table_name in tables_with_discovery:
                    # Check if target table exists
                    table_exists = False
                    try:
                        check_query = f"""
                        SELECT COUNT(*) as table_count 
                        FROM {dest_db}.INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_NAME = '{table_name}' 
                        AND TABLE_SCHEMA = '{dest_schema}'
                        """
                        result = session.sql(check_query).collect()
                        table_exists = result[0]['TABLE_COUNT'] > 0 if result else False
                    except:
                        table_exists = False
                    
                    if not table_exists:
                        success, message = create_target_table_with_source_structure(
                            session, source_db, source_schema, table_name, dest_db, dest_schema, table_name
                        )
                        
                        if success:
                            tables_created.append(table_name)
                        else:
                            table_creation_errors.append(f"{table_name}: {message}")
                
                # Show table creation summary (only if there were changes)
                if tables_created:
                    st.success(f"✅ Created {len(tables_created)} target tables: {tables_created}")
                if table_creation_errors:
                    st.error(f"❌ Failed to create {len(table_creation_errors)} tables:")
                    for error in table_creation_errors:
                        st.error(f"   • {error}")
                    st.error("⛔ Cannot proceed with masking workflow due to table creation failures")
                    return
                
                # Execute masking workflow
                st.info("🎭 Starting masking workflow...")
                
                workflow_result = execute_masking_workflow(
                    session,
                    st.session_state.dcs_masking_client,
                    source_db,
                    source_schema,
                    dest_db, 
                    dest_schema,
                    tables_with_discovery,
                    execution_id,
                    st.session_state.get('temp_max_workers', 3),
                    st.session_state.get('temp_write_mode', 'overwrite'),
                    table_run_ids
                )
                
                # Step 4: Update job status based on results
                if 'results' in workflow_result:
                    for result in workflow_result['results']:
                        table_name = result.get('table')
                        run_id = table_run_ids.get(table_name)
                        if run_id:
                            success = result.get('success', False)
                            error_message = result.get('error', '') if not success else None
                            log_masking_job_completion(session, execution_id, run_id, success, error_message)
                
                # Step 5: Update execution_end_time for all entries
                st.info("✅ Finalizing execution logs...")
                update_execution_end_time_for_all(session, execution_id)
                
                if workflow_result['success']:
                    st.success("🎉 Masking workflow completed successfully!")
                else:
                    st.error(f"❌ Masking workflow failed: {workflow_result.get('error', 'Unknown error')}")
                
                # Display detailed results
                if 'results' in workflow_result:
                    display_operation_results(workflow_result['results'], "Masking")


def in_place_masking_content():
    """In-place masking page content."""
    
    create_feature_card(
        "In-Place Masking",
        "Directly mask sensitive data in your existing tables. ⚠️ This operation modifies original data permanently.",
        "fas fa-bolt",
        "warning"
    )
    
    if not st.session_state.get('dcs_masking_client'):
        st.warning("⚠️ Please configure Masking API settings in Configuration tab first.")
        return
    
    session = st.session_state.snowflake_session
    if not session:
        st.error("❌ Snowflake session not available")
        return
    
    # Warning section
    st.warning("⚠️ **WARNING**: In-place masking permanently modifies your original data. Ensure you have proper backups before proceeding.")
    
    # Source Configuration section (same as Mask & Deliver but only source)
    def configuration_content():
        # Only source configuration - no target needed for in-place
        source_db, source_schema, _ = source_target_inputs(session, "source")
        return source_db, source_schema
    
    source_db, source_schema = configuration_content()
    
    if source_db and source_schema:
        
        # Display masking rules directly (same as Mask & Deliver)
        all_discovery_df = get_discovery_metadata(session, source_db, source_schema)
        
        if not all_discovery_df.empty:
            # Display discovery results using the same formatted design as the Discovery page
            display_masking_discovery_results_formatted(all_discovery_df, source_db, source_schema)
        else:
            st.warning("⚠️ No discovery metadata found. Please run discovery first.")
        
        # Check if we have discovery metadata for the schema
        has_metadata = not get_discovery_metadata(session, source_db, source_schema).empty
        
        if has_metadata:
            # Get unique table names from discovery results for validation
            discovery_df = get_discovery_metadata(session, source_db, source_schema)
            tables_with_discovery = discovery_df['IDENTIFIED_TABLE'].unique().tolist() if not discovery_df.empty else []
            
            # No target table validation needed for in-place masking
            
            # Execution settings displayed directly like in Discovery page (same as Mask & Deliver)
            max_workers = st.number_input(
                "Parallel Workers",
                min_value=1,
                max_value=4,
                value=2,
                help="Number of tables to process in parallel (reduced for Snowflake memory limits)"
            )
            
            st.info("ℹ️ **Batch Size**: Automatically calculated based on sensitive column sizes (1.8MB limit with safety buffer)")
            
            # Write mode selection - modified for in-place (no "Clean up" option since we're updating in place)
            st.markdown("**📝 Update Mode:**")
            write_mode_option = st.radio(
                "Choose update mode:",
                [
                    "Update in place (directly modify existing data)",
                    "Backup then update (create backup table first)"
                ],
                index=0,  # Default to direct update
                help="Choose whether to update data directly or create a backup first"
            )
            
            # Store values in session state to access them
            st.session_state.temp_max_workers = max_workers
            st.session_state.temp_update_mode = "direct" if "directly modify" in write_mode_option else "backup"
            
            # In-place masking execution button
            if st.button("🚀 Start In-Place Masking Workflow", type="primary", use_container_width=True):
                
                # Step 1: Generate execution_id and display it
                execution_id = generate_execution_id()
                st.info(f"🆔 **Execution ID**: `{execution_id}`")
                st.session_state.current_execution_id = execution_id
                
                # Validation check (same as Mask & Deliver)
                if not tables_with_discovery:
                    st.error("❌ No tables found with discovery metadata. Please run discovery first.")
                    return
                
                # Step 2: Initialize job tracking for in-place masking
                table_run_ids = {}
                for table_name in tables_with_discovery:
                    run_id = log_masking_job_start(session, execution_id, source_db, source_schema, table_name, source_db, source_schema)  # source and dest are same for in-place
                    if run_id:
                        table_run_ids[table_name] = run_id
                
                # Step 3: Pre-execution validation (simplified)
                tables_ready_for_masking = []
                tables_with_issues = []
                
                for table_name in tables_with_discovery:
                    # Get discovery metadata for this table
                    table_discovery_df = get_discovery_metadata(session, source_db, source_schema, table_name)
                    
                    if table_discovery_df.empty:
                        tables_with_issues.append(table_name)
                        continue
                    
                    # Analyze column assignments
                    assigned_columns = table_discovery_df[
                        table_discovery_df['ASSIGNED_ALGORITHM'].notna() & 
                        (table_discovery_df['ASSIGNED_ALGORITHM'] != '')
                    ]
                    num_assigned = len(assigned_columns)
                    
                    if num_assigned > 0:
                        tables_ready_for_masking.append(table_name)
                    else:
                        tables_with_issues.append(table_name)
                
                # Step 4: No table creation needed for in-place masking (tables already exist)
                
                # Execute in-place masking workflow
                st.info("🎭 Starting in-place masking workflow...")
                
                # Call the in-place masking workflow function
                from modules.business_engines import execute_inplace_masking_workflow
                workflow_result = execute_inplace_masking_workflow(
                    session, st.session_state.dcs_masking_client, source_db, source_schema, tables_ready_for_masking,
                    execution_id, st.session_state.get('temp_max_workers', 3), st.session_state.get('temp_update_mode', 'direct')
                )
                
                # Step 4: Update job status based on results
                if 'results' in workflow_result:
                    for result in workflow_result['results']:
                        table_name = result.get('table')
                        run_id = table_run_ids.get(table_name)
                        if run_id:
                            success = result.get('success', False)
                            error_message = result.get('error', '') if not success else None
                            log_masking_job_completion(session, execution_id, run_id, success, error_message)
                
                # Step 5: Update execution_end_time for all entries
                st.info("✅ Finalizing execution logs...")
                update_execution_end_time_for_all(session, execution_id)
                
                if workflow_result['success']:
                    st.success("🎉 In-place masking workflow completed successfully!")
                else:
                    st.error(f"❌ In-place masking workflow failed: {workflow_result.get('error', 'Unknown error')}")
                
                # Display detailed results
                if 'results' in workflow_result:
                    display_operation_results(workflow_result['results'], "In-Place Masking")
        
        else:
            st.info("💡 Run discovery first to identify sensitive data before starting masking.")


def monitoring_content():
    """Monitoring and analytics page content with status tiles and detailed events log."""
    
    session = st.session_state.snowflake_session
    if not session:
        st.error("❌ Snowflake session not available")
        return
    
    # Get the latest events data
    from modules.job_manager import get_events_log
    from modules.metadata_store import METADATA_CONFIG
    
    # Get all events for status counting
    try:
        status_query = f"""
        SELECT 
            run_status,
            COUNT(*) as count
        FROM {METADATA_CONFIG['dcs_events_log']}
        GROUP BY run_status
        ORDER BY run_status
        """
        status_df = session.sql(status_query).to_pandas()
        
        # Convert to dictionary for easier access
        status_counts = {}
        if not status_df.empty:
            for _, row in status_df.iterrows():
                status_counts[row['RUN_STATUS']] = row['COUNT']
                
    except Exception as e:
        st.error(f"Error loading status counts: {str(e)}")
        status_counts = {}
    
    # Display status tiles in one horizontal line
    st.subheader("📊 Job Status Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        completed_count = status_counts.get('COMPLETED', 0)
        st.html(f"""
        <div style="
            background: linear-gradient(135deg, #28a745, #20c997);
            color: white;
            padding: 20px;
            border-radius: 12px;
            text-align: center;
            box-shadow: 0 4px 12px rgba(40, 167, 69, 0.3);
            margin-bottom: 15px;
        ">
            <div style="font-size: 32px; font-weight: bold; margin-bottom: 8px;">{completed_count}</div>
            <div style="font-size: 14px; font-weight: 500; text-transform: uppercase; letter-spacing: 1px;">COMPLETED</div>
        </div>
        """)
    
    with col2:
        failed_count = status_counts.get('FAILED', 0)
        st.html(f"""
        <div style="
            background: linear-gradient(135deg, #dc3545, #e74c3c);
            color: white;
            padding: 20px;
            border-radius: 12px;
            text-align: center;
            box-shadow: 0 4px 12px rgba(220, 53, 69, 0.3);
            margin-bottom: 15px;
        ">
            <div style="font-size: 32px; font-weight: bold; margin-bottom: 8px;">{failed_count}</div>
            <div style="font-size: 14px; font-weight: 500; text-transform: uppercase; letter-spacing: 1px;">FAILED</div>
        </div>
        """)
    
    with col3:
        in_progress_count = status_counts.get('IN PROGRESS', 0)
        st.html(f"""
        <div style="
            background: linear-gradient(135deg, #007bff, #17a2b8);
            color: white;
            padding: 20px;
            border-radius: 12px;
            text-align: center;
            box-shadow: 0 4px 12px rgba(0, 123, 255, 0.3);
            margin-bottom: 15px;
        ">
            <div style="font-size: 32px; font-weight: bold; margin-bottom: 8px;">{in_progress_count}</div>
            <div style="font-size: 14px; font-weight: 500; text-transform: uppercase; letter-spacing: 1px;">IN PROGRESS</div>
        </div>
        """)
    
    with col4:
        waiting_count = status_counts.get('WAITING', 0)
        st.html(f"""
        <div style="
            background: linear-gradient(135deg, #ffc107, #fd7e14);
            color: white;
            padding: 20px;
            border-radius: 12px;
            text-align: center;
            box-shadow: 0 4px 12px rgba(255, 193, 7, 0.3);
            margin-bottom: 15px;
        ">
            <div style="font-size: 32px; font-weight: bold; margin-bottom: 8px;">{waiting_count}</div>
            <div style="font-size: 14px; font-weight: 500; text-transform: uppercase; letter-spacing: 1px;">WAITING</div>
        </div>
        """)
    
    # Add some spacing
    st.write("")
    
    # Display monitoring events table with exact Discovery Results design
    display_monitoring_events_table(session)


def configuration_content():
    """Configuration page content (DCS API configuration) - Discovery and Masking endpoints."""
    
    # Create two columns for Discovery and Masking configurations
    col1, col2 = st.columns(2)
    
    # Discovery Configuration (Left Column)
    with col1:
        st.subheader("🔍 Discovery Configuration")
        
        # Initialize session state for discovery config if not exists
        if 'dcs_discovery_config' not in st.session_state:
            st.session_state.dcs_discovery_config = None
        if 'dcs_discovery_client' not in st.session_state:
            st.session_state.dcs_discovery_client = None
        
        # Discovery endpoint inputs
        discovery_api_url = st.text_input(
            "DCS Discovery Endpoint",
            help="Enter your DCS Discovery API endpoint URL",
            key="discovery_api_url"
        )
        
        discovery_tenant_id = st.text_input(
            "Azure Tenant ID",
            type="password",
            help="Azure AD tenant ID for discovery authentication",
            key="discovery_tenant_id"
        )
        
        discovery_client_id = st.text_input(
            "Azure Client ID", 
            type="password",
            help="Azure AD application client ID for discovery",
            key="discovery_client_id"
        )
        
        discovery_client_secret = st.text_input(
            "Azure Client Secret",
            type="password", 
            help="Azure AD application client secret for discovery",
            key="discovery_client_secret"
        )
        
        discovery_scope = st.text_input(
            "Azure Scope",
            value=DEFAULT_AZURE_SCOPE,
            help="Azure AD scope for discovery API access",
            key="discovery_scope"
        )
        
        # Discovery configuration buttons
        col1_1, col1_2 = st.columns(2)
        
        with col1_1:
            if st.button("💾 Save Discovery Configuration", type="primary", use_container_width=True, key="save_discovery"):
                if all([discovery_api_url, discovery_tenant_id, discovery_client_id, discovery_client_secret]):
                    # Create Discovery DCS configuration
                    st.session_state.dcs_discovery_config = DCSConfig(
                        dcs_api_url=discovery_api_url,
                        azure_tenant_id=discovery_tenant_id,
                        azure_client_id=discovery_client_id,
                        azure_client_secret=discovery_client_secret,
                        azure_scope=discovery_scope
                    )
                    
                    # Create Discovery DCS client
                    st.session_state.dcs_discovery_client = DCSAPIClient(st.session_state.dcs_discovery_config)
                    
                    st.success("✅ Discovery configuration saved!")
                    st.rerun()
                else:
                    st.error("❌ Please fill in all required fields")
        
        with col1_2:
            if st.button("🧪 Test Discovery Endpoint", use_container_width=True, key="test_discovery"):
                if st.session_state.dcs_discovery_client:
                    with st.spinner("Testing discovery endpoint..."):
                        try:
                            # Test Discovery API connectivity
                            st.session_state.dcs_discovery_client.get_azure_ad_token()
                            test_data = {"test_column": ["test_value_1", "test_value_2"]}
                            st.session_state.dcs_discovery_client.profile_data_raw(test_data)
                            st.success("✅ Discovery endpoint test successful!")
                        except Exception as e:
                            st.error(f"❌ Discovery endpoint test failed: {str(e)}")
                else:
                    st.warning("⚠️ Please save discovery configuration first")
    
    # Masking Configuration (Right Column)
    with col2:
        st.subheader("🎭 Masking Configuration")
        
        # Initialize session state for masking config if not exists
        if 'dcs_masking_config' not in st.session_state:
            st.session_state.dcs_masking_config = None
        if 'dcs_masking_client' not in st.session_state:
            st.session_state.dcs_masking_client = None
        
        # Masking endpoint inputs
        masking_api_url = st.text_input(
            "DCS Masking Endpoint",
            help="Enter your DCS Masking API endpoint URL",
            key="masking_api_url"
        )
        
        masking_tenant_id = st.text_input(
            "Azure Tenant ID",
            type="password",
            help="Azure AD tenant ID for masking authentication",
            key="masking_tenant_id"
        )
        
        masking_client_id = st.text_input(
            "Azure Client ID", 
            type="password",
            help="Azure AD application client ID for masking",
            key="masking_client_id"
        )
        
        masking_client_secret = st.text_input(
            "Azure Client Secret",
            type="password", 
            help="Azure AD application client secret for masking",
            key="masking_client_secret"
        )
        
        masking_scope = st.text_input(
            "Azure Scope",
            value=DEFAULT_AZURE_SCOPE,
            help="Azure AD scope for masking API access",
            key="masking_scope"
        )
        
        # Masking configuration buttons
        col2_1, col2_2 = st.columns(2)
        
        with col2_1:
            if st.button("💾 Save Masking Configuration", type="primary", use_container_width=True, key="save_masking"):
                if all([masking_api_url, masking_tenant_id, masking_client_id, masking_client_secret]):
                    # Create Masking DCS configuration
                    st.session_state.dcs_masking_config = DCSConfig(
                        dcs_api_url=masking_api_url,
                        azure_tenant_id=masking_tenant_id,
                        azure_client_id=masking_client_id,
                        azure_client_secret=masking_client_secret,
                        azure_scope=masking_scope
                    )
                    
                    # Create Masking DCS client
                    st.session_state.dcs_masking_client = DCSAPIClient(st.session_state.dcs_masking_config)
                    
                    st.success("✅ Masking configuration saved!")
                    st.rerun()
                else:
                    st.error("❌ Please fill in all required fields")
        
        with col2_2:
            if st.button("🧪 Test Masking Endpoint", use_container_width=True, key="test_masking"):
                if st.session_state.dcs_masking_client:
                    with st.spinner("Testing masking endpoint..."):
                        try:
                            # Test Masking API connectivity using proper masking endpoint with valid algorithm
                            st.session_state.dcs_masking_client.get_azure_ad_token()
                            test_data = [{"test_column": "123 Main St"}]
                            test_rules = {"test_column": "dlpx-core:CM Alpha-Numeric"}
                            st.session_state.dcs_masking_client.mask_data_raw_powerquery_format(test_data, test_rules)
                            st.success("✅ Masking endpoint test successful!")
                        except Exception as e:
                            st.error(f"❌ Masking endpoint test failed: {str(e)}")
                else:
                    st.warning("⚠️ Please save masking configuration first")
    
    # Maintain backward compatibility with old dcs_client for existing functionality
    if st.session_state.dcs_discovery_client and not hasattr(st.session_state, 'dcs_client'):
        st.session_state.dcs_client = st.session_state.dcs_discovery_client
    elif st.session_state.dcs_masking_client and not hasattr(st.session_state, 'dcs_client'):
        st.session_state.dcs_client = st.session_state.dcs_masking_client
    
    # Environment info
    def environment_info_content():
        env_config = st.session_state.environment_config
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"**Environment**: {env_config.get('environment', 'Unknown')}")
            st.info(f"**Execution ID**: {st.session_state.current_execution_id}")
        
        with col2:
            st.info(f"**Metadata Schema**: {env_config.get('discovered_ruleset', 'Not configured')}")
            st.info(f"**Events Log**: {env_config.get('dcs_events_log', 'Not configured')}")
        
        # Reset button
        if st.button("🔄 New Execution ID", use_container_width=True):
            st.session_state.current_execution_id = generate_execution_id()
            st.rerun()
    
    create_professional_container(
        environment_info_content,
        "Environment Information",
        "fas fa-server"
    )


def settings_content():
    """Settings and configuration page content."""
    
    # Environment information in professional container
    def environment_info_content():
        env_config = st.session_state.environment_config
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"**Environment Type**: {env_config.get('environment', 'Unknown')}")
            st.info(f"**Current Execution ID**: {st.session_state.current_execution_id}")
        
        with col2:
            st.info(f"**Metadata Schema**: {env_config.get('discovered_ruleset', 'Not configured')}")
            st.info(f"**Events Log**: {env_config.get('dcs_events_log', 'Not configured')}")
    
    create_professional_container(
        environment_info_content,
        "Environment Information",
        "fas fa-server"
    )
    
    # Setup instructions in professional container
    def setup_instructions_content():
        st.markdown("""
        ## External Access Integration Setup
        
        Run these SQL commands as ACCOUNTRADMIN:
        
        ```sql
        -- 1. Create Network Rule
        CREATE OR REPLACE NETWORK RULE dcs_api_network_rule
        MODE = EGRESS
        TYPE = HOST_PORT
        VALUE_LIST = ('your-dcs-api.com:443', 'login.microsoftonline.com:443');
        
        -- 2. Create External Access Integration  
        CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dcs_api_integration
        ALLOWED_NETWORK_RULES = (dcs_api_network_rule)
        ENABLED = true;
        
        -- 3. Grant Usage
        GRANT USAGE ON INTEGRATION dcs_api_integration TO ROLE your_role;
        
        -- 4. Update Streamlit App
        ALTER STREAMLIT your_streamlit_app SET EXTERNAL_ACCESS_INTEGRATIONS = (dcs_api_integration);
        ```
        """)
    
    create_professional_container(
        setup_instructions_content,
        "Setup Instructions",
        "fas fa-book"
    )
    
    # Advanced settings in professional container
    def advanced_settings_content():
        st.checkbox("Enable Debug Mode", help="Show additional debugging information")
        st.checkbox("Enable Performance Profiling", help="Track detailed performance metrics")
        st.slider("Default Batch Size", min_value=1000, max_value=10000, value=5000)
        st.slider("Max Parallel Workers", min_value=1, max_value=25, value=8)
    
    create_professional_container(
        advanced_settings_content,
        "Advanced Settings",
        "fas fa-sliders-h"
    )


def main():
    """Main application entry point with DattaAble layout."""
    
    # Initialize session state
    init_session_state()
    
    # Create DattaAble layout and get content container
    content_container = create_dattaable_layout()
    
    # Display content in the main content area based on current page
    with content_container:
        current_page = get_current_page()
        current_subpage = get_current_subpage()
        
        # Add page headers based on current page
        if current_page == 'discovery':
            discovery_content()
        elif current_page == 'masking':
            if current_subpage == 'mask_deliver':
                mask_and_deliver_content()
            elif current_subpage == 'in_place':
                create_page_header("In-Place Masking", "Directly mask sensitive data in your existing tables", "fas fa-bolt")
                in_place_masking_content()
            else:
                create_page_header("Data Masking", "Apply intelligent masking algorithms to protect sensitive data", "fas fa-mask")
                masking_overview_content()
        elif current_page == 'monitoring':
            create_page_header("Monitoring & Analytics", "Track job performance and analyze operational metrics", "fas fa-chart-line")
            monitoring_content()
        elif current_page == 'configuration':
            create_page_header("Configuration", "Configure DCS API settings and test connectivity", "fas fa-cog")
            configuration_content()
        elif current_page == 'settings':
            create_page_header("Settings", "Configure system settings and view environment information", "fas fa-sliders-h")
            settings_content()
        else:
            # Default to discovery
            discovery_content()


if __name__ == "__main__":
    main()