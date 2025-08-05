"""
Business Logic Engines Module

This module contains the core business logic for DCS operations including
discovery engine and masking engines (in-place and copy operations).
"""

import streamlit as st
import pandas as pd
import time
import threading
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from .snowflake_ops import get_snowflake_table_data, safe_dataframe_to_records, save_masked_data_to_snowflake
from .metadata_store import update_discovery_results
from .job_manager import PerformanceTimer, log_discovery_job_in_progress, log_discovery_job_completion, log_masking_job_in_progress, log_masking_job_completion


def create_target_table_with_source_structure(session, source_db, source_schema, source_table, dest_db, dest_schema, dest_table):
    """Create target table with same structure as source table using INFORMATION_SCHEMA approach."""
    try:
        # Get source table definition using INFORMATION_SCHEMA
        from .snowflake_ops import get_table_definition
        table_def = get_table_definition(session, source_db, source_schema, source_table)
        
        if not table_def or table_def['columns'].empty:
            st.error(f"Could not get structure of source table {source_db}.{source_schema}.{source_table}")
            return False, f"Could not get structure of source table {source_db}.{source_schema}.{source_table}"
        
        # Build CREATE TABLE statement
        columns_ddl = []
        
        for _, col in table_def['columns'].iterrows():
            col_name = col['COLUMN_NAME']
            data_type = col['DATA_TYPE']
            
            # Handle data type with length/precision
            if col['CHARACTER_MAXIMUM_LENGTH'] and not pd.isna(col['CHARACTER_MAXIMUM_LENGTH']):
                data_type += f"({int(col['CHARACTER_MAXIMUM_LENGTH'])})"
            elif col['NUMERIC_PRECISION'] and not pd.isna(col['NUMERIC_PRECISION']):
                if col['NUMERIC_SCALE'] and not pd.isna(col['NUMERIC_SCALE']):
                    data_type += f"({int(col['NUMERIC_PRECISION'])},{int(col['NUMERIC_SCALE'])})"
                else:
                    data_type += f"({int(col['NUMERIC_PRECISION'])})"
            
            # Handle nullable
            nullable = "" if col['IS_NULLABLE'] == 'YES' else " NOT NULL"
            
            # Handle default value
            default = ""
            if col['COLUMN_DEFAULT'] and not pd.isna(col['COLUMN_DEFAULT']):
                default = f" DEFAULT {col['COLUMN_DEFAULT']}"
            
            column_ddl = f"{col_name} {data_type}{nullable}{default}"
            columns_ddl.append(column_ddl)
        
        # Add constraints information
        if not table_def['constraints'].empty:
            st.info(f"Found {len(table_def['constraints'])} constraints - basic table structure will be created")
        
        # Build final CREATE TABLE statement
        ddl_separator = ',\n            '
        ddl_joined = ddl_separator.join(columns_ddl)
        create_table_sql = f"""
        CREATE TABLE {dest_db}.{dest_schema}.{dest_table} (
            {ddl_joined}
        )
        """
        
        # Execute CREATE TABLE
        try:
            session.sql(create_table_sql).collect()
            
            # Verify the table was created successfully
            verify_sql = f"SELECT 1 FROM {dest_db}.{dest_schema}.{dest_table} LIMIT 1"
            session.sql(verify_sql).collect()
            
            st.success(f"✅ Created target table {dest_db}.{dest_schema}.{dest_table} with same structure as source")
            return True, f"Successfully created {dest_table} with same structure as source"
            
        except Exception as create_error:
            st.error(f"❌ Failed to create table {dest_db}.{dest_schema}.{dest_table}")
            if "permission" in str(create_error).lower() or "access" in str(create_error).lower():
                st.error(f"   → Permission issue: {str(create_error)}")
            elif "does not exist" in str(create_error).lower():
                st.error(f"   → Database/schema issue: {str(create_error)}")
            else:
                st.error(f"   → Error details: {str(create_error)}")
            
            # Show the DDL that failed for debugging
            st.code(create_table_sql, language="sql")
            return False, f"Failed to create table: {str(create_error)}"
        
    except Exception as e:
        st.error(f"❌ Failed to prepare table creation: {str(e)}")
        return False, f"Failed to create table: {str(e)}"


def calculate_optimal_batch_size(session, database, schema, table_name, sensitive_columns_dict, max_batch_size_mb=1.8):
    """
    Calculate optimal batch size for masking API calls with memory-safe limits for Snowflake functions.
    
    Args:
        session: Snowflake session
        database: Database name
        schema: Schema name 
        table_name: Table name
        sensitive_columns_dict: Dict of {column_name: algorithm} for sensitive columns
        max_batch_size_mb: Maximum batch size in MB (default 1.8 for optimal cost efficiency)
    
    Returns:
        dict: {
            'batch_size': int,           # Number of rows per batch
            'estimated_size_mb': float,  # Estimated size per batch in MB
            'total_rows': int,           # Total rows in table
            'total_batches': int,        # Estimated number of batches
            'reasoning': str             # Explanation of calculation
        }
    """
    
    try:
        # Get total row count
        count_query = f"SELECT COUNT(*) as row_count FROM {database}.{schema}.{table_name}"
        count_result = session.sql(count_query).to_pandas()
        total_rows = count_result.iloc[0]['ROW_COUNT']
        
        if total_rows == 0:
            return {
                'batch_size': 1000,
                'estimated_size_mb': 0,
                'total_rows': 0,
                'total_batches': 0,
                'reasoning': "Table is empty, using default batch size"
            }
        
        # Get column information for sensitive columns only
        sensitive_columns = list(sensitive_columns_dict.keys())
        if not sensitive_columns:
            return {
                'batch_size': min(5000, total_rows),
                'estimated_size_mb': 0,
                'total_rows': total_rows,
                'total_batches': 1,
                'reasoning': "No sensitive columns, using default batch size"
            }
        
        # Query column metadata for sensitive columns
        columns_condition = "', '".join(sensitive_columns)
        metadata_query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION
        FROM {database}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_CATALOG = '{database}' 
        AND TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table_name}'
        AND COLUMN_NAME IN ('{columns_condition}')
        """
        
        metadata_df = session.sql(metadata_query).to_pandas()
        
        # Calculate estimated bytes per row for sensitive columns only
        estimated_bytes_per_row = 0
        column_details = []
        
        for _, col_info in metadata_df.iterrows():
            col_name = col_info['COLUMN_NAME']
            data_type = col_info['DATA_TYPE']
            max_length = col_info['CHARACTER_MAXIMUM_LENGTH']
            precision = col_info['NUMERIC_PRECISION']
            
            # Estimate bytes per column based on data type
            if data_type in ['VARCHAR', 'CHAR', 'TEXT', 'STRING']:
                if pd.notna(max_length) and max_length > 0:
                    col_bytes = min(max_length, 1000)  # Cap at 1KB for very large text fields
                else:
                    col_bytes = 100  # Default for unlimited VARCHAR
            elif data_type in ['NUMBER', 'DECIMAL', 'NUMERIC']:
                if pd.notna(precision) and precision > 0:
                    col_bytes = max(8, precision // 2)  # Rough estimate
                else:
                    col_bytes = 16  # Default for unlimited precision
            elif data_type in ['INTEGER', 'BIGINT']:
                col_bytes = 8
            elif data_type in ['FLOAT', 'DOUBLE']:
                col_bytes = 8
            elif data_type in ['BOOLEAN']:
                col_bytes = 1
            elif data_type in ['DATE']:
                col_bytes = 10
            elif data_type in ['TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ']:
                col_bytes = 25
            elif data_type in ['TIME']:
                col_bytes = 12
            else:
                col_bytes = 50  # Default for unknown types
            
            estimated_bytes_per_row += col_bytes
            column_details.append(f"{col_name}: {col_bytes}B")
        
        # Convert to KB (more readable)
        estimated_kb_per_row = estimated_bytes_per_row / 1024
        
        # Calculate max rows that fit in the size limit
        max_batch_size_bytes = max_batch_size_mb * 1024 * 1024  # Convert MB to bytes
        max_rows_per_batch = int(max_batch_size_bytes / estimated_bytes_per_row)
        
        # Apply minimum constraint and round down to nearest tens
        min_batch_size = 10      # Never go below 10 rows
        calculated_batch_size = max(min_batch_size, max_rows_per_batch)
        
        # Round down to nearest tens (e.g., 8,347 becomes 8,340)
        optimal_batch_size = (calculated_batch_size // 10) * 10
        
        # Ensure we don't round down below minimum
        if optimal_batch_size < min_batch_size:
            optimal_batch_size = min_batch_size
        
        # If the calculated batch size would handle all rows, adjust it
        if optimal_batch_size >= total_rows:
            optimal_batch_size = total_rows
            total_batches = 1
        else:
            total_batches = (total_rows + optimal_batch_size - 1) // optimal_batch_size
        
        # Estimated actual size per batch
        estimated_size_mb = (optimal_batch_size * estimated_bytes_per_row) / (1024 * 1024)
        
        # Create reasoning explanation
        reasoning = (
            f"Table has {total_rows:,} rows with {len(sensitive_columns)} sensitive columns. "
            f"Estimated {estimated_kb_per_row:.1f}KB per row "
            f"({estimated_bytes_per_row:,} bytes based on column sizes only). "
            f"Calculated {max_rows_per_batch:,} max rows per {max_batch_size_mb}MB (safety buffer), rounded down to {optimal_batch_size:,} rows. "
            f"Batch size = ~{estimated_size_mb:.2f}MB per batch, requiring {total_batches} batches total."
        )
        
        return {
            'batch_size': optimal_batch_size,
            'estimated_size_mb': estimated_size_mb,
            'total_rows': total_rows,
            'total_batches': total_batches,
            'reasoning': reasoning,
            'column_details': column_details,
            'sensitive_columns_count': len(sensitive_columns),
            'estimated_kb_per_row': estimated_kb_per_row
        }
        
    except Exception as e:
        # Fallback to conservative default
        fallback_batch_size = min(1000, total_rows if 'total_rows' in locals() else 1000)
        return {
            'batch_size': fallback_batch_size,
            'estimated_size_mb': 1.0,
            'total_rows': total_rows if 'total_rows' in locals() else 0,
            'total_batches': max(1, (total_rows // fallback_batch_size) if 'total_rows' in locals() else 1),
            'reasoning': f"Error calculating optimal batch size ({str(e)}), using conservative default",
            'error': str(e)
        }


def discover_table_parallel(session, discovery_client, database, schema, table_name, execution_id, sample_size=1000, run_id=None):
    """Perform discovery on a single table (for parallel execution) with detailed timing."""
    
    # Initialize performance timer
    timer = PerformanceTimer("discovery", table_name, execution_id)
    
    try:
        # Update job status to IN PROGRESS
        if run_id:
            log_discovery_job_in_progress(session, execution_id, run_id)
        
        # Step 1: Load data from Snowflake
        timer.start_step("data_loading", f"Loading {sample_size} rows from {database}.{schema}.{table_name}")
        df = get_snowflake_table_data(session, database, schema, table_name, sample_size)
        if df is None:
            timer.end_step()
            # Log failure
            if run_id:
                log_discovery_job_completion(session, execution_id, run_id, success=False, error_message='Failed to load data')
            return {
                'table': table_name, 
                'success': False, 
                'error': 'Failed to load data',
                'timing_summary': timer.get_timing_summary()
            }
        timer.end_step()
        
        # Step 2: Data transformation for DCS API format
        timer.start_step("data_transformation", f"Converting {len(df.columns)} columns to DCS API format")
        formatted_data = {}
        for col in df.columns:
            column_data = df[col].fillna("").astype(str).tolist()
            formatted_data[col] = column_data
        timer.end_step()
        
        # Step 3: DCS API call
        timer.start_step("dcs_api_call", f"Sending {len(formatted_data)} columns to DCS discovery API")
        api_start_time = time.time()
        response = discovery_client.profile_data_raw(formatted_data)
        api_duration = time.time() - api_start_time
        api_run_id = response.get('run_id')  # DCS API run_id (different from our logging run_id)
        timer.end_step()
        
        # Step 4: API run_id received (job logging handled separately)
        timer.start_step("api_tracking", "Processing DCS API run_id")
        # Note: DCS API run_id logged separately, main job logging handled at workflow level
        timer.end_step()
        
        # Step 5: Parse discovery results
        timer.start_step("result_parsing", "Processing DCS API response and extracting sensitive columns")
        discovery_data = {}
        if 'items' in response and 'details' in response['items']:
            for item in response['items']['details']:
                column = item.get('columnName', '')
                algorithm = item.get('algorithm', '')
                domain = item.get('domain', '')
                confidence = float(item.get('confidence', 0.0))
                
                if algorithm:
                    discovery_data[column] = {
                        'algorithm': algorithm,
                        'domain': domain,
                        'confidence': confidence
                    }
        timer.end_step()
        
        # Step 6: Update metadata store
        timer.start_step("metadata_update", f"Updating metadata store with {len(discovery_data)} sensitive columns")
        insert_result = update_discovery_results(session, database, schema, table_name, discovery_data)
        timer.end_step()
        
        # Step 7: Final job logging (use original run_id for status updates)
        timer.start_step("completion_logging", "Recording discovery job completion")
        if run_id:
            log_discovery_job_completion(session, execution_id, run_id, success=True)
        timer.end_step()
        
        # Get final timing summary
        timing_summary = timer.get_timing_summary()
        
        return {
            'table': table_name, 
            'success': True, 
            'discovery_data': discovery_data,
            'metadata_result': insert_result,
            'columns_analyzed': len(df.columns),
            'sensitive_columns': len(discovery_data),
            'run_id': run_id,  # Return the original run_id for tracking
            'timing_summary': timing_summary,
            'api_duration_seconds': round(api_duration, 3)
        }
        
    except Exception as e:
        # End current step if active
        if timer.current_step:
            timer.end_step()
            
        # Log job failure (use original run_id for status updates)
        if run_id:
            log_discovery_job_completion(session, execution_id, run_id, success=False, error_message=str(e))
        
        return {
            'table': table_name, 
            'success': False, 
            'error': str(e),
            'timing_summary': timer.get_timing_summary() if timer else {}
        }


def process_single_batch_masking(session, masking_client, source_db, source_schema, dest_db, dest_schema,
                                table_name, batch_data, batch_number, total_batches, column_rules, 
                                write_mode, execution_id):
    """Process a single batch of data for masking with comprehensive error handling and timing."""
    
    batch_timer = PerformanceTimer("batch_masking", f"{table_name}_batch_{batch_number}", execution_id)
    run_id = None
    
    try:
        # Step 1: Data preparation and validation  
        batch_timer.start_step("data_validation", f"Validating batch {batch_number} data")
        
        if batch_data is None or len(batch_data) == 0:
            st.info(f"  🔄 Batch {batch_number}/{total_batches}: Processing 0 rows")
            batch_timer.end_step()
            st.warning(f"  ⚠️ Batch {batch_number}: No data to process")
            return {
                'batch_number': batch_number,
                'success': False, 
                'error': 'No data in batch',
                'rows_processed': 0,
                'timing_summary': batch_timer.get_timing_summary()
            }
        
        # Convert to DataFrame if needed
        if not isinstance(batch_data, pd.DataFrame):
            batch_df = pd.DataFrame(batch_data)
        else:
            batch_df = batch_data.copy()
        
        st.info(f"  🔄 Batch {batch_number}/{total_batches}: Processing {len(batch_df)} rows")
            
        # Check for required columns
        missing_columns = set(column_rules.keys()) - set(batch_df.columns)
        if missing_columns:
            batch_timer.end_step()
            error_msg = f"Missing columns in batch data: {missing_columns}"
            st.error(f"  ❌ Batch {batch_number}: {error_msg}")
            return {
                'batch_number': batch_number,
                'success': False, 
                'error': error_msg,
                'rows_processed': 0,
                'timing_summary': batch_timer.get_timing_summary()
            }
        
        batch_timer.end_step()
        
        # Step 2: Extract only sensitive columns for DCS API
        batch_timer.start_step("sensitive_data_extraction", f"Extracting {len(column_rules)} sensitive columns for DCS API")
        try:
            # Extract only the sensitive columns for masking
            sensitive_columns_df = batch_df[list(column_rules.keys())].copy()
            
            # Convert sensitive columns to records for DCS API
            sensitive_data_records = safe_dataframe_to_records(sensitive_columns_df)
            if sensitive_data_records is None or len(sensitive_data_records) == 0:
                batch_timer.end_step()
                return {
                    'batch_number': batch_number,
                    'success': False, 
                    'error': 'Failed to extract sensitive columns data',
                    'rows_processed': 0,
                    'timing_summary': batch_timer.get_timing_summary()
                }
            
            # Columns extracted for masking (reduced verbosity)
            
        except Exception as convert_error:
            batch_timer.end_step()
            st.error(f"  ❌ Batch {batch_number}: Sensitive data extraction failed: {str(convert_error)}")
            return {
                'batch_number': batch_number,
                'success': False, 
                'error': f'Sensitive data extraction failed: {str(convert_error)}',
                'rows_processed': 0,
                'timing_summary': batch_timer.get_timing_summary()
            }
        batch_timer.end_step()
        
        # Step 3: DCS API masking call
        batch_timer.start_step("dcs_masking_api", f"Masking {len(sensitive_data_records)} records via DCS API")
        api_start_time = time.time()
        
        try:
            masking_response = masking_client.mask_data_raw_powerquery_format(sensitive_data_records, column_rules)
            api_duration = time.time() - api_start_time
            run_id = masking_response.get('run_id')
            
            # Enhanced API response validation with detailed logging
            if not masking_response:
                batch_timer.end_step()
                error_msg = "DCS API returned empty response"
                st.error(f"  ❌ Batch {batch_number}: {error_msg}")
                st.error(f"     🔍 Column rules sent: {list(column_rules.keys())}")
                st.error(f"     🔍 Sensitive data records count: {len(sensitive_data_records)}")
                return {
                    'batch_number': batch_number,
                    'success': False, 
                    'error': error_msg,
                    'rows_processed': 0,
                    'api_duration_seconds': round(api_duration, 3),
                    'timing_summary': batch_timer.get_timing_summary()
                }
            
            if 'masked_records' not in masking_response:
                batch_timer.end_step()
                error_msg = f"Invalid API response format - missing 'masked_records' key"
                st.error(f"  ❌ Batch {batch_number}: {error_msg}")
                st.error(f"     🔍 API response keys: {list(masking_response.keys()) if isinstance(masking_response, dict) else 'Not a dict'}")
                st.error(f"     🔍 API response content: {str(masking_response)[:500]}...")
                return {
                    'batch_number': batch_number,
                    'success': False, 
                    'error': error_msg,
                    'rows_processed': 0,
                    'api_duration_seconds': round(api_duration, 3),
                    'timing_summary': batch_timer.get_timing_summary()
                }
            
            masked_records = masking_response['masked_records']
            
            # Validate masked records
            if masked_records is None or len(masked_records) == 0:
                batch_timer.end_step()
                error_msg = "DCS API returned empty masked_records"
                st.error(f"  ❌ Batch {batch_number}: {error_msg}")
                st.error(f"     🔍 Original sensitive records count: {len(sensitive_data_records)}")
                st.error(f"     🔍 Masked records count: 0")
                return {
                    'batch_number': batch_number,
                    'success': False, 
                    'error': error_msg,
                    'rows_processed': 0,
                    'api_duration_seconds': round(api_duration, 3),
                    'timing_summary': batch_timer.get_timing_summary()
                }
            
        except Exception as api_error:
            api_duration = time.time() - api_start_time
            batch_timer.end_step()
            
            # Enhanced error logging with detailed diagnostics
            error_type = type(api_error).__name__
            error_msg = str(api_error)
            
            st.error(f"  ❌ Batch {batch_number}: DCS API call failed")
            st.error(f"     🔍 Error Type: {error_type}")
            st.error(f"     🔍 Error Message: {error_msg}")
            st.error(f"     🔍 Table: {table_name}")
            st.error(f"     🔍 Batch size: {len(sensitive_data_records)} records")
            st.error(f"     🔍 Columns to mask: {list(column_rules.keys())}")
            st.error(f"     🔍 Algorithms: {list(column_rules.values())}")
            
            # Check for common error patterns
            if "authentication" in error_msg.lower() or "token" in error_msg.lower():
                st.error(f"     💡 Suggestion: Check DCS API credentials in sidebar")
            elif "network" in error_msg.lower() or "connection" in error_msg.lower():
                st.error(f"     💡 Suggestion: Check network connectivity and External Access Integration")
            elif "algorithm" in error_msg.lower():
                st.error(f"     💡 Suggestion: Verify assigned algorithms exist in DCS system")
            elif "data" in error_msg.lower() or "format" in error_msg.lower():
                st.error(f"     💡 Suggestion: Check data types and null values in source table")
            
            return {
                'batch_number': batch_number,
                'success': False, 
                'error': f'DCS API call failed ({error_type}): {error_msg}',
                'rows_processed': 0,
                'api_duration_seconds': round(api_duration, 3),
                'timing_summary': batch_timer.get_timing_summary()
            }
        
        batch_timer.end_step()
        
        # Step 4: Merge masked sensitive data with original data
        batch_timer.start_step("data_merge", f"Merging {len(masked_records)} masked sensitive columns with original data")
        
        try:
            # Convert masked records (sensitive columns only) back to DataFrame
            masked_sensitive_df = pd.DataFrame(masked_records)
            
            # Debug: Show what we got from the API
            st.info(f"  🔍 API Response Analysis:")
            st.info(f"     • Masked records count: {len(masked_records)}")
            st.info(f"     • Masked DataFrame shape: {masked_sensitive_df.shape}")
            st.info(f"     • Masked DataFrame columns: {list(masked_sensitive_df.columns)}")
            
            # Check for empty/null values in masked data
            for col_name in column_rules.keys():
                if col_name in masked_sensitive_df.columns:
                    col_data = masked_sensitive_df[col_name]
                    non_null_count = col_data.notna().sum()
                    non_empty_count = (col_data != '').sum() if col_data.dtype == 'object' else non_null_count
                    sample_vals = col_data.dropna().head(3).tolist() if non_null_count > 0 else []
                    st.info(f"     • {col_name}: {non_null_count} non-null, {non_empty_count} non-empty, samples: {sample_vals}")
                    
                    if non_null_count == 0:
                        st.warning(f"     ⚠️ {col_name}: ALL VALUES ARE NULL in masked data!")
                    elif non_empty_count == 0:
                        st.warning(f"     ⚠️ {col_name}: ALL VALUES ARE EMPTY in masked data!")
                else:
                    st.warning(f"     ⚠️ {col_name}: Missing from API response!")
            
            # Create final DataFrame by starting with original batch data
            final_df = batch_df.copy()
            
            # Replace sensitive columns with masked versions (with proper type handling)
            replacement_count = 0
            for col_name in column_rules.keys():
                if col_name in masked_sensitive_df.columns:
                    original_sample = final_df[col_name].head(3).tolist()
                    
                    # Get the masked column values
                    masked_values = masked_sensitive_df[col_name]
                    
                    # Handle date columns that might have been returned as strings from API
                    # Check if original column appears to be a date column
                    if col_name.upper().endswith('DOB') or 'DATE' in col_name.upper():
                        try:
                            # Convert string dates back to proper date objects if needed
                            masked_values = pd.to_datetime(masked_values, errors='coerce')
                            st.info(f"     • Converted {col_name} string dates back to datetime objects")
                        except Exception as date_conv_error:
                            st.warning(f"     ⚠️ Could not convert {col_name} to dates: {date_conv_error}")
                    
                    # Assign the processed masked values
                    final_df[col_name] = masked_values
                    new_sample = final_df[col_name].head(3).tolist()
                    replacement_count += 1
                    st.info(f"     • Replaced {col_name}:")
                    st.info(f"       - Original: {original_sample}")  
                    st.info(f"       - Masked: {new_sample}")
                    st.info(f"       - Final dtype: {final_df[col_name].dtype}")
                else:
                    st.warning(f"     ⚠️ Could not replace {col_name} - not in masked data")
            
            st.info(f"  🔄 Batch {batch_number}: Successfully replaced {replacement_count}/{len(column_rules)} columns")
            
        except Exception as merge_error:
            batch_timer.end_step()
            st.error(f"  ❌ Batch {batch_number}: Data merge failed: {str(merge_error)}")
            return {
                'batch_number': batch_number,
                'success': False, 
                'error': f'Data merge failed: {str(merge_error)}',
                'rows_processed': len(sensitive_data_records),
                'api_duration_seconds': round(api_duration, 3),
                'timing_summary': batch_timer.get_timing_summary()
            }
        
        batch_timer.end_step()
        
        # Step 5: Data save to Snowflake
        batch_timer.start_step("data_save", f"Saving {len(final_df)} complete records to Snowflake")
        
        try:
            # Validate final DataFrame
            if final_df.empty:
                batch_timer.end_step()
                error_msg = "Final DataFrame is empty after merging"
                st.error(f"  ❌ Batch {batch_number}: {error_msg}")
                st.error(f"     🔍 Original batch records: {len(batch_df)}")
                st.error(f"     🔍 Masked sensitive records from API: {len(masked_records)}")
                return {
                    'batch_number': batch_number,
                    'success': False, 
                    'error': error_msg,
                    'rows_processed': len(batch_df),
                    'api_duration_seconds': round(api_duration, 3),
                    'timing_summary': batch_timer.get_timing_summary()
                }
            
            # Data merged and ready for saving (reduced verbosity)
            
            # Save to destination table
            save_success = save_masked_data_to_snowflake(
                session, final_df, dest_db, dest_schema, table_name, write_mode
            )
            
            if not save_success:
                batch_timer.end_step()
                error_msg = f"Data save failed - save operation returned False"
                st.error(f"  ❌ Batch {batch_number}: {error_msg}")
                st.error(f"     🔍 Destination: {dest_db}.{dest_schema}.{table_name}")
                st.error(f"     🔍 Write mode: {write_mode}")
                st.error(f"     🔍 DataFrame shape: {final_df.shape}")
                st.error(f"     💡 Suggestion: Check table permissions and schema existence")
                return {
                    'batch_number': batch_number,
                    'success': False, 
                    'error': error_msg,
                    'rows_processed': len(batch_df),
                    'api_duration_seconds': round(api_duration, 3),
                    'timing_summary': batch_timer.get_timing_summary()
                }
                
        except Exception as save_error:
            batch_timer.end_step()
            error_type = type(save_error).__name__
            error_msg = str(save_error)
            
            st.error(f"  ❌ Batch {batch_number}: Save operation failed")
            st.error(f"     🔍 Error Type: {error_type}")
            st.error(f"     🔍 Error Message: {error_msg}")
            st.error(f"     🔍 Destination: {dest_db}.{dest_schema}.{table_name}")
            st.error(f"     🔍 Write mode: {write_mode}")
            st.error(f"     🔍 DataFrame shape: {final_df.shape if 'final_df' in locals() else 'Unknown'}")
            
            # Check for common save error patterns
            if "permission" in error_msg.lower() or "access" in error_msg.lower():
                st.error(f"     💡 Suggestion: Check database/schema permissions")
            elif "schema" in error_msg.lower() or "database" in error_msg.lower():
                st.error(f"     💡 Suggestion: Verify destination database and schema exist")
            elif "table" in error_msg.lower():
                st.error(f"     💡 Suggestion: Check if destination table structure matches source")
            elif "column" in error_msg.lower():
                st.error(f"     💡 Suggestion: Verify column names and data types match")
                
            return {
                'batch_number': batch_number,
                'success': False, 
                'error': f'Save failed ({error_type}): {error_msg}',
                'rows_processed': len(batch_df) if 'batch_df' in locals() else 0,
                'api_duration_seconds': round(api_duration, 3),
                'timing_summary': batch_timer.get_timing_summary()
            }
        
        batch_timer.end_step()
        
        # Step 6: Batch completion (job logging handled at table level)
        batch_timer.start_step("batch_completion", "Finalizing batch processing")
        # Note: Job logging is handled at the table level, not per batch
        batch_timer.end_step()
        
        # Success
        timing_summary = batch_timer.get_timing_summary()
        # Batch completed successfully (reduced verbosity)
        
        return {
            'batch_number': batch_number,
            'success': True,
            'rows_processed': len(final_df),
            'columns_masked': len(column_rules),
            'api_duration_seconds': round(api_duration, 3),
            'run_id': run_id,
            'timing_summary': timing_summary
        }
        
    except Exception as e:
        # End current step if active
        if batch_timer.current_step:
            batch_timer.end_step()
            
        # Note: Job logging failures are handled at table level, not per batch
        
        st.error(f"  ❌ Batch {batch_number}: Unexpected error: {str(e)}")
        return {
            'batch_number': batch_number,
            'success': False, 
            'error': f'Unexpected error: {str(e)}',
            'rows_processed': 0,
            'timing_summary': batch_timer.get_timing_summary() if batch_timer else {}
        }


def process_single_table_masking(session, masking_client, source_db, source_schema, dest_db, dest_schema,
                                table_name, execution_id, write_mode="overwrite", progress_callback=None, run_id=None):
    """Process complete table masking with batching and comprehensive error handling."""
    
    table_timer = PerformanceTimer("table_masking", table_name, execution_id)
    
    try:
        # Update job status to IN_PROGRESS in database
        if run_id:
            log_masking_job_in_progress(session, execution_id, run_id)
        
        # Report initial progress
        if progress_callback:
            progress_callback(table_name, 0, "Loading metadata...")
        
        # Step 1: Load masking rules from metadata
        table_timer.start_step("metadata_loading", f"Loading masking rules for {table_name}")
        
        from .metadata_store import get_discovery_metadata
        discovery_df = get_discovery_metadata(session, source_db, source_schema, table_name)
        
        if discovery_df.empty:
            table_timer.end_step()
            if progress_callback:
                progress_callback(table_name, 100, "No discovery metadata found")
            
            # Update job status to FAILED in database
            if run_id:
                log_masking_job_completion(session, execution_id, run_id, success=False, error_message="No discovery metadata found")
            
            return {
                'table': table_name,
                'success': False,
                'error': 'No discovery metadata found',
                'timing_summary': table_timer.get_timing_summary()
            }
        
        # Extract column rules (only columns with assigned algorithms)
        column_rules = {}
        discovered_columns = []
        unassigned_columns = []
        
        for _, row in discovery_df.iterrows():
            column_name = row['IDENTIFIED_COLUMN']
            discovered_algorithm = row.get('PROFILED_ALGORITHM', '')
            assigned_algorithm = row.get('ASSIGNED_ALGORITHM', '')
            
            discovered_columns.append(column_name)
            
            if pd.notna(assigned_algorithm) and str(assigned_algorithm).strip():
                column_rules[column_name] = str(assigned_algorithm).strip()
            else:
                unassigned_columns.append({
                    'column': column_name,
                    'discovered': discovered_algorithm if pd.notna(discovered_algorithm) else 'None'
                })
        
        # Enhanced logging for debugging
        st.info(f"  📋 {table_name} - Discovery Analysis:")
        st.info(f"     • Total columns with discovery data: {len(discovered_columns)}")
        st.info(f"     • Columns with assigned algorithms: {len(column_rules)}")
        st.info(f"     • Columns without assigned algorithms: {len(unassigned_columns)}")
        
        if column_rules:
            st.info(f"     • Masking rules: {dict(list(column_rules.items())[:3])}{'...' if len(column_rules) > 3 else ''}")
        
        if unassigned_columns:
            st.warning(f"     ⚠️ Unassigned columns (first 3): {[col['column'] for col in unassigned_columns[:3]]}")
            for col in unassigned_columns[:3]:
                st.warning(f"       - {col['column']}: discovered='{col['discovered']}'")
        
        if not column_rules:
            table_timer.end_step()
            if progress_callback:
                progress_callback(table_name, 50, "No masking needed - copying table...")
            
            # Step 2a: Copy table as-is using CTAS (Create Table As Select)
            table_timer.start_step("ctas_copy", f"Copying {table_name} as-is using CTAS")
            
            try:
                # Create target table by copying source table structure and data
                ctas_query = f"""
                CREATE OR REPLACE TABLE {dest_db}.{dest_schema}.{table_name} AS
                SELECT * FROM {source_db}.{source_schema}.{table_name}
                """
                
                st.info(f"  🔄 Executing: CREATE TABLE AS SELECT for {table_name}")
                result = session.sql(ctas_query).collect()
                
                # Get row count of copied table
                count_query = f"SELECT COUNT(*) as row_count FROM {dest_db}.{dest_schema}.{table_name}"
                count_result = session.sql(count_query).collect()
                rows_copied = count_result[0]['ROW_COUNT'] if count_result else 0
                
                table_timer.end_step()
                
                if progress_callback:
                    progress_callback(table_name, 100, f"Completed - {rows_copied:,} rows copied")
                
                # Update job status to COMPLETED in database
                if run_id:
                    log_masking_job_completion(session, execution_id, run_id, success=True)
                
                return {
                    'table': table_name,
                    'success': True,
                    'total_rows': rows_copied,
                    'rows_processed': rows_copied,
                    'method': 'CTAS',
                    'columns_masked': 0,
                    'timing_summary': table_timer.get_timing_summary()
                }
                
            except Exception as e:
                table_timer.end_step()
                if progress_callback:
                    progress_callback(table_name, 100, f"CTAS failed: {str(e)}")
                
                # Update job status to FAILED in database
                if run_id:
                    log_masking_job_completion(session, execution_id, run_id, success=False, error_message=f"CTAS failed: {str(e)}")
                
                return {
                    'table': table_name,
                    'success': False,
                    'error': f'CTAS failed: {str(e)}',
                    'method': 'CTAS',
                    'timing_summary': table_timer.get_timing_summary()
                }
        
        table_timer.end_step()
        
        if progress_callback:
            progress_callback(table_name, 10, f"Loading {len(column_rules)} columns to mask...")
        
        # Step 2: Load table data in chunks to avoid memory exhaustion
        table_timer.start_step("data_loading", f"Loading table data for {table_name}")
        
        # First, get total row count
        count_query = f"SELECT COUNT(*) as row_count FROM {source_db}.{source_schema}.{table_name}"
        count_result = session.sql(count_query).collect()
        total_rows = count_result[0]['ROW_COUNT'] if count_result else 0
        
        if total_rows == 0:
            table_timer.end_step()
            if progress_callback:
                progress_callback(table_name, 100, "No data found in table")
            
            # Update job status to COMPLETED in database (empty table is successful)
            if run_id:
                log_masking_job_completion(session, execution_id, run_id, success=True)
            
            return {
                'table': table_name,
                'success': False,
                'error': 'No data in table',
                'timing_summary': table_timer.get_timing_summary()
            }
        
        table_timer.end_step()
        
        # Step 3: Calculate optimal batch size for sensitive columns only
        table_timer.start_step("batch_planning", "Calculating optimal batch size based on sensitive columns")
        
        batch_calc = calculate_optimal_batch_size(session, source_db, source_schema, table_name, column_rules)
        optimal_batch_size = batch_calc['batch_size']
        
        # No artificial row limit - only respect the 1.8MB API limit for cost efficiency
        
        total_batches = (total_rows + optimal_batch_size - 1) // optimal_batch_size
        
        st.info(f"  📊 Cost-Optimized Batch Planning:")
        st.info(f"     • Rows per batch: {optimal_batch_size:,} (1.8MB API limit)")
        st.info(f"     • Total batches: {total_batches}")
        st.info(f"     • Processing strategy: Chunked loading")
        
        if progress_callback:
            progress_callback(table_name, 20, f"Processing {total_batches} batches...")
        
        table_timer.end_step()
        
        # Step 3.5: Handle target table data based on write mode
        if write_mode == "overwrite":
            table_timer.start_step("table_truncate", f"Clearing target table {dest_db}.{dest_schema}.{table_name}")
            try:
                truncate_query = f"DELETE FROM {dest_db}.{dest_schema}.{table_name}"
                session.sql(truncate_query).collect()
                st.info(f"  🗑️ Cleared target table {dest_db}.{dest_schema}.{table_name}")
            except Exception as truncate_error:
                st.warning(f"  ⚠️ Could not clear target table: {str(truncate_error)} - proceeding with append")
            table_timer.end_step()
        else:
            st.info(f"  📝 Append mode: Keeping existing data in {dest_db}.{dest_schema}.{table_name}")
        
        # Step 4: Process batches
        table_timer.start_step("batch_processing", f"Processing {total_batches} batches")
        
        successful_batches = 0
        failed_batches = []
        total_rows_processed = 0
        
        # Always use append mode since target table is pre-created with exact structure
        # The table was created with correct data types using GET_DDL, so we must preserve it
        write_mode = "append"
        
        for batch_num in range(1, total_batches + 1):
            # Calculate offset for this batch
            offset = (batch_num - 1) * optimal_batch_size
            
            # Load only this batch's data to minimize memory usage
            batch_query = f"""
            SELECT * FROM {source_db}.{source_schema}.{table_name}
            LIMIT {optimal_batch_size} OFFSET {offset}
            """
            
            try:
                batch_df = session.sql(batch_query).to_pandas()
                if batch_df.empty:
                    st.warning(f"  ⚠️ Batch {batch_num}: No data loaded")
                    continue
                    
            except Exception as e:
                st.error(f"  ❌ Batch {batch_num}: Failed to load data: {str(e)}")
                failed_batches.append({
                    'batch_number': batch_num,
                    'error': f'Data loading failed: {str(e)}'
                })
                continue
            
            # Always use append mode to preserve the pre-created table structure
            current_write_mode = "append"
            
            batch_result = process_single_batch_masking(
                session, masking_client, source_db, source_schema, dest_db, dest_schema,
                table_name, batch_df, batch_num, total_batches, column_rules, 
                current_write_mode, execution_id
            )
            
            # Explicitly delete batch DataFrame to free memory immediately
            del batch_df
            
            if batch_result['success']:
                successful_batches += 1
                total_rows_processed += batch_result['rows_processed']
            else:
                failed_batches.append({
                    'batch_number': batch_num,
                    'error': batch_result['error']
                })
            
            # Update progress after each batch
            if progress_callback:
                progress_percent = 20 + (batch_num / total_batches) * 70  # 20% to 90%
                progress_callback(table_name, progress_percent, f"Batch {batch_num}/{total_batches} processed")
        
        table_timer.end_step()
        
        # Final results
        timing_summary = table_timer.get_timing_summary()
        
        if successful_batches == total_batches:
            # Table processing completed successfully
            if progress_callback:
                progress_callback(table_name, 100, f"Completed - {total_rows_processed:,} rows processed")
            
            # Update job status to COMPLETED in database
            if run_id:
                log_masking_job_completion(session, execution_id, run_id, success=True)
            
            return {
                'table': table_name,
                'success': True,
                'total_rows': total_rows,
                'rows_processed': total_rows_processed,
                'batches_processed': successful_batches,
                'total_batches': total_batches,
                'columns_masked': len(column_rules),
                'timing_summary': timing_summary
            }
        else:
            if progress_callback:
                progress_callback(table_name, 100, f"Failed - {len(failed_batches)} batches failed")
            
            # Update job status to FAILED in database
            if run_id:
                error_message = f'{len(failed_batches)} batches failed'
                log_masking_job_completion(session, execution_id, run_id, success=False, error_message=error_message)
            
            return {
                'table': table_name,
                'success': False,
                'total_rows': total_rows,
                'rows_processed': total_rows_processed,
                'batches_processed': successful_batches,
                'total_batches': total_batches,
                'failed_batches': failed_batches,
                'error': f'{len(failed_batches)} batches failed',
                'timing_summary': timing_summary
            }
        
    except Exception as e:
        # End current step if active
        if table_timer.current_step:
            table_timer.end_step()
        
        if progress_callback:
            progress_callback(table_name, 100, f"Error - {str(e)}")
        
        # Update job status to FAILED in database
        if run_id:
            log_masking_job_completion(session, execution_id, run_id, success=False, error_message=str(e))
        
        return {
            'table': table_name,
            'success': False,
            'error': f'Unexpected error: {str(e)}',
            'timing_summary': table_timer.get_timing_summary() if table_timer else {}
        }


def execute_masking_workflow(session, masking_client, source_db, source_schema, dest_db, dest_schema,
                           selected_tables, execution_id, max_workers=2, write_mode="overwrite", table_run_ids=None):
    """Execute complete masking workflow for multiple tables with parallel processing."""
    
    workflow_timer = PerformanceTimer("masking_workflow", "multi_table", execution_id)
    
    try:
        st.write(f"🚀 **Starting masking workflow for {len(selected_tables)} tables**")
        
        workflow_timer.start_step("workflow_initialization", f"Initializing masking workflow for {len(selected_tables)} tables")
        
        # Initialize results tracking
        results = []
        successful_tables = 0
        failed_tables = []
        total_rows_processed = 0
        
        workflow_timer.end_step()
        
        # Process tables in parallel for improved performance
        workflow_timer.start_step("parallel_processing", f"Processing {len(selected_tables)} tables in parallel (max_workers={max_workers})")
        
        # Create organized progress display
        st.write("## 🎯 **Masking Progress**")
        
        # Create placeholders for each table's progress display
        table_placeholders = {}
        progress_placeholders = {}
        status_placeholders = {}
        summary_placeholders = {}
        timing_placeholders = {}
        
        for table_name in selected_tables:
            col1, col2, col3 = st.columns([2, 5, 3])
            with col1:
                st.write(f"**{table_name}**")
            with col2:
                progress_placeholders[table_name] = st.progress(0)
            with col3:
                status_placeholders[table_name] = st.empty()
            
            # Add summary and timing placeholders below each table
            summary_placeholders[table_name] = st.empty()
            timing_placeholders[table_name] = st.empty()
        
        # Overall progress
        st.write("---")
        overall_progress = st.progress(0)
        overall_status = st.empty()
        
        # Shared progress tracking with thread safety
        progress_lock = threading.Lock()
        table_progress = {table: 0 for table in selected_tables}
        table_status = {table: "Waiting..." for table in selected_tables}
        
        # Initialize all status displays
        for table_name in selected_tables:
            status_placeholders[table_name].info("⏳ Waiting...")
        
        def update_progress(table_name, percent, status):
            """Thread-safe progress update function - only updates data, not UI directly"""
            with progress_lock:
                table_progress[table_name] = percent
                table_status[table_name] = status
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all masking jobs with progress callback
            future_to_table = {}
            
            for table_name in selected_tables:
                # Get run_id for this table
                run_id = table_run_ids.get(table_name) if table_run_ids else None
                
                future = executor.submit(
                    process_single_table_masking,
                    session, masking_client, source_db, source_schema, dest_db, dest_schema,
                    table_name, execution_id, write_mode, update_progress, run_id
                )
                future_to_table[future] = table_name
            
            # Real-time UI updates while jobs are running
            import time as time_module
            while any(not future.done() for future in future_to_table.keys()):
                # Update UI based on current progress state
                with progress_lock:
                    for table_name in selected_tables:
                        percent = table_progress[table_name]
                        status = table_status[table_name]
                        
                        # Update progress bar
                        progress_placeholders[table_name].progress(percent / 100.0)
                        
                        # Update status with appropriate icon
                        if percent >= 100:
                            if "Completed" in status:
                                status_placeholders[table_name].success(f"✅ {status}")
                            else:
                                status_placeholders[table_name].error(f"❌ {status}")
                        elif percent > 0:
                            status_placeholders[table_name].info(f"🔄 {status}")
                        else:
                            status_placeholders[table_name].info(f"⏳ {status}")
                    
                    # Update overall progress
                    avg_progress = sum(table_progress.values()) / len(table_progress)
                    overall_progress.progress(avg_progress / 100.0)
                    completed_tables = sum(1 for p in table_progress.values() if p >= 100)
                    overall_status.text(f"Progress: {completed_tables}/{len(selected_tables)} tables completed")
                
                # Sleep briefly to avoid overwhelming the UI
                time_module.sleep(0.5)
            
            # Wait for all futures to complete and collect results
            for future in concurrent.futures.as_completed(future_to_table):
                table_name = future_to_table[future]
                
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        successful_tables += 1
                        total_rows_processed += result.get('rows_processed', 0)
                    else:
                        failed_tables.append({
                            'table': table_name,
                            'error': result.get('error', 'Unknown error')
                        })
                        
                except Exception as exc:
                    failed_tables.append({
                        'table': table_name,
                        'error': f'Exception during parallel processing: {str(exc)}'
                    })
                    results.append({
                        'table': table_name,
                        'success': False,
                        'error': f'Exception during parallel processing: {str(exc)}'
                    })
        
        # Final UI update to ensure all statuses are correct and show timing breakdown
        with progress_lock:
            for table_name in selected_tables:
                percent = table_progress[table_name]
                status = table_status[table_name]
                
                # Update progress bar
                progress_placeholders[table_name].progress(percent / 100.0)
                
                # Update status with appropriate icon
                if percent >= 100:
                    if "Completed" in status:
                        status_placeholders[table_name].success(f"✅ {status}")
                    else:
                        status_placeholders[table_name].error(f"❌ {status}")
                elif percent > 0:
                    status_placeholders[table_name].info(f"🔄 {status}")
                else:
                    status_placeholders[table_name].info(f"⏳ {status}")
                
                # Find the result for this table to show timing breakdown
                table_result = None
                for result in results:
                    if result.get('table') == table_name:
                        table_result = result
                        break
                
                if table_result and table_result.get('success'):
                    # Show summary information
                    batches = table_result.get('total_batches', table_result.get('batches_processed', 1))
                    rows = table_result.get('rows_processed', 0)
                    
                    # Calculate batch size
                    batch_size = rows // batches if batches > 0 else 0
                    
                    # Get batch size in MB from the original calculation
                    # We need to recalculate this based on the actual batch size used
                    try:
                        # Get column rules for this table to estimate MB size
                        from .metadata_store import get_discovery_metadata
                        discovery_df = get_discovery_metadata(session, source_db, source_schema, table_name)
                        
                        column_rules = {}
                        for _, row in discovery_df.iterrows():
                            assigned_algorithm = row.get('ASSIGNED_ALGORITHM', '')
                            if pd.notna(assigned_algorithm) and str(assigned_algorithm).strip():
                                column_rules[row['IDENTIFIED_COLUMN']] = str(assigned_algorithm).strip()
                        
                        if column_rules:
                            # Recalculate batch size estimation based on actual batch size
                            batch_calc = calculate_optimal_batch_size(session, source_db, source_schema, table_name, column_rules)
                            # Scale the estimated MB size based on actual vs calculated batch size
                            calculated_batch_size = batch_calc.get('batch_size', batch_size)
                            estimated_mb_per_calculated_batch = batch_calc.get('estimated_size_mb', 0)
                            
                            if calculated_batch_size > 0:
                                actual_mb_per_batch = (batch_size / calculated_batch_size) * estimated_mb_per_calculated_batch
                            else:
                                actual_mb_per_batch = estimated_mb_per_calculated_batch
                        else:
                            actual_mb_per_batch = 0
                            
                    except Exception:
                        actual_mb_per_batch = 0
                    
                    summary_placeholders[table_name].info(f"""
                    📊 **Summary**: {batches} batches • {batch_size:,} rows per batch ({actual_mb_per_batch:.2f} MB) • {rows:,} total rows processed
                    """)
                    
                    # Extract timing data and show breakdown
                    timing = table_result.get('timing_summary', {})
                    steps = timing.get('steps', [])
                    step_times = {step['name']: step['duration_seconds'] for step in steps}
                    
                    # Calculate phase durations based on actual step timings
                    data_read_time = step_times.get('data_loading', 0) + step_times.get('batch_planning', 0)
                    batch_processing_time = step_times.get('batch_processing', 0)
                    
                    # Split batch processing into masking (70%) and loading (30%)
                    if batch_processing_time > 0:
                        masking_time = batch_processing_time * 0.7
                        data_load_time = batch_processing_time * 0.3
                    else:
                        masking_time = 0
                        data_load_time = 0
                    
                    total_duration = data_read_time + masking_time + data_load_time
                    
                    timing_placeholders[table_name].info(f"""
                    ⏱️ **Timing Breakdown**:
                    • Data Read & Batching: {data_read_time:.1f}s
                    • Data Masking: {masking_time:.1f}s  
                    • Masked Data Load: {data_load_time:.1f}s
                    • **Total Duration**: {total_duration:.1f}s
                    """)
                elif table_result and not table_result.get('success'):
                    # Show error summary
                    error = table_result.get('error', 'Unknown error')
                    summary_placeholders[table_name].error(f"❌ **Failed**: {error}")
                    timing_placeholders[table_name].empty()  # Clear timing for failed tables
        
        # Final progress update
        overall_progress.progress(1.0)
        overall_status.text("✅ All tables completed!")
        
        workflow_timer.end_step()
        
        # Final workflow summary
        timing_summary = workflow_timer.get_timing_summary()
        
        st.write("---") 
        st.write("## 📊 **Summary**")
        
        # Concise results summary
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if successful_tables > 0:
                st.metric("✅ Successful Tables", successful_tables)
            else:
                st.metric("✅ Successful Tables", 0)
        
        with col2:
            if failed_tables:
                st.metric("❌ Failed Tables", len(failed_tables))
            else:
                st.metric("❌ Failed Tables", 0)
        
        with col3:
            st.metric("📈 Total Rows", f"{total_rows_processed:,}")
        
        # Show failed table details if any
        if failed_tables:
            st.error("**Failed Tables:**")
            for failed in failed_tables:
                st.error(f"• {failed['table']}: {failed['error']}")
        
        return {
            'success': len(failed_tables) == 0,
            'total_tables': len(selected_tables),
            'successful_tables': successful_tables,
            'failed_tables': len(failed_tables),
            'total_rows_processed': total_rows_processed,
            'results': results,
            'failed_table_details': failed_tables,
            'timing_summary': timing_summary
        }
        
    except Exception as e:
        # End current step if active
        if workflow_timer.current_step:
            workflow_timer.end_step()
        
        st.error(f"❌ Masking workflow failed: {str(e)}")
        return {
            'success': False,
            'error': f'Workflow failed: {str(e)}',
            'timing_summary': workflow_timer.get_timing_summary() if workflow_timer else {}
        }


def execute_inplace_masking_workflow(session, masking_client, source_db, source_schema, 
                                   selected_tables, execution_id, max_workers=2, update_mode="direct", table_run_ids=None):
    """Execute complete in-place masking workflow for multiple tables with parallel processing."""
    
    workflow_timer = PerformanceTimer("inplace_masking_workflow", "multi_table", execution_id)
    
    try:
        st.write(f"🚀 **Starting in-place masking workflow for {len(selected_tables)} tables**")
        
        workflow_timer.start_step("workflow_initialization", f"Initializing in-place masking workflow for {len(selected_tables)} tables")
        
        # Initialize results tracking
        results = []
        successful_tables = 0
        failed_tables = []
        total_rows_processed = 0
        
        workflow_timer.end_step()
        
        # Process tables in parallel for improved performance
        workflow_timer.start_step("parallel_processing", f"Processing {len(selected_tables)} tables in parallel (max_workers={max_workers})")
        
        # Create organized progress display
        st.write("## 🎯 **In-Place Masking Progress**")
        
        # Create placeholders for each table's progress display
        table_placeholders = {}
        progress_placeholders = {}
        status_placeholders = {}
        summary_placeholders = {}
        timing_placeholders = {}
        
        for table_name in selected_tables:
            col1, col2, col3 = st.columns([2, 5, 3])
            with col1:
                st.write(f"**{table_name}**")
            with col2:
                progress_placeholders[table_name] = st.progress(0)
            with col3:
                status_placeholders[table_name] = st.empty()
            
            # Add summary and timing placeholders below each table
            summary_placeholders[table_name] = st.empty()
            timing_placeholders[table_name] = st.empty()
        
        # Overall progress
        st.write("---")
        overall_progress = st.progress(0)
        overall_status = st.empty()
        
        # Shared progress tracking with thread safety
        progress_lock = threading.Lock()
        table_progress = {table: 0 for table in selected_tables}
        table_status = {table: "Waiting..." for table in selected_tables}
        
        # Initialize all status displays
        for table_name in selected_tables:
            status_placeholders[table_name].info("⏳ Waiting...")
        
        def update_progress(table_name, percent, status):
            """Thread-safe progress update function - only updates data, not UI directly"""
            with progress_lock:
                table_progress[table_name] = percent
                table_status[table_name] = status
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all in-place masking jobs with progress callback
            future_to_table = {}
            
            for table_index, table_name in enumerate(selected_tables):
                # Get run_id for this table
                run_id = table_run_ids.get(table_name) if table_run_ids else None
                
                future = executor.submit(
                    process_single_table_inplace_masking,
                    session, masking_client, source_db, source_schema,
                    table_name, execution_id, update_mode, update_progress, table_index, len(selected_tables)
                )
                future_to_table[future] = table_name
            
            # Real-time UI updates while jobs are running
            import time as time_module
            while any(not future.done() for future in future_to_table.keys()):
                # Update UI based on current progress state
                with progress_lock:
                    for table_name in selected_tables:
                        percent = table_progress[table_name]
                        status = table_status[table_name]
                        
                        # Update progress bar
                        progress_placeholders[table_name].progress(percent / 100.0)
                        
                        # Update status with appropriate icon
                        if percent >= 100:
                            if "Completed" in status:
                                status_placeholders[table_name].success(f"✅ {status}")
                            else:
                                status_placeholders[table_name].error(f"❌ {status}")
                        elif percent > 0:
                            status_placeholders[table_name].info(f"🔄 {status}")
                        else:
                            status_placeholders[table_name].info(f"⏳ {status}")
                    
                    # Update overall progress
                    avg_progress = sum(table_progress.values()) / len(table_progress)
                    overall_progress.progress(avg_progress / 100.0)
                    completed_tables = sum(1 for p in table_progress.values() if p >= 100)
                    overall_status.text(f"Progress: {completed_tables}/{len(selected_tables)} tables completed")
                
                # Sleep briefly to avoid overwhelming the UI
                time_module.sleep(0.5)
            
            # Wait for all futures to complete and collect results
            for future in concurrent.futures.as_completed(future_to_table):
                table_name = future_to_table[future]
                
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        successful_tables += 1
                        total_rows_processed += result.get('rows_processed', 0)
                    else:
                        failed_tables.append({
                            'table': table_name,
                            'error': result.get('error', 'Unknown error')
                        })
                        
                except Exception as exc:
                    failed_tables.append({
                        'table': table_name,
                        'error': f'Exception during parallel processing: {str(exc)}'
                    })
                    results.append({
                        'table': table_name,
                        'success': False,
                        'error': f'Exception during parallel processing: {str(exc)}'
                    })
        
        # Final UI update to ensure all statuses are correct and show timing breakdown
        with progress_lock:
            for table_name in selected_tables:
                percent = table_progress[table_name]
                status = table_status[table_name]
                
                # Update progress bar
                progress_placeholders[table_name].progress(percent / 100.0)
                
                # Update status with appropriate icon
                if percent >= 100:
                    if "Completed" in status:
                        status_placeholders[table_name].success(f"✅ {status}")
                    else:
                        status_placeholders[table_name].error(f"❌ {status}")
                elif percent > 0:
                    status_placeholders[table_name].info(f"🔄 {status}")
                else:
                    status_placeholders[table_name].info(f"⏳ {status}")
                
                # Find the result for this table to show timing breakdown
                table_result = None
                for result in results:
                    if result.get('table') == table_name:
                        table_result = result
                        break
                
                if table_result and table_result.get('success'):
                    # Show summary information
                    batches = table_result.get('total_batches', table_result.get('batches_processed', 1))
                    rows = table_result.get('rows_processed', 0)
                    
                    # Calculate batch size
                    batch_size = rows // batches if batches > 0 else 0
                    
                    # Get batch size in MB from the original calculation
                    try:
                        # Get column rules for this table to estimate MB size
                        from .metadata_store import get_discovery_metadata
                        discovery_df = get_discovery_metadata(session, source_db, source_schema, table_name)
                        
                        column_rules = {}
                        for _, row in discovery_df.iterrows():
                            assigned_algorithm = row.get('ASSIGNED_ALGORITHM', '')
                            if pd.notna(assigned_algorithm) and str(assigned_algorithm).strip():
                                column_rules[row['IDENTIFIED_COLUMN']] = str(assigned_algorithm).strip()
                        
                        if column_rules:
                            # Recalculate batch size estimation based on actual batch size
                            batch_calc = calculate_optimal_batch_size(session, source_db, source_schema, table_name, column_rules)
                            # Scale the estimated MB size based on actual vs calculated batch size
                            calculated_batch_size = batch_calc.get('batch_size', batch_size)
                            estimated_mb_per_calculated_batch = batch_calc.get('estimated_size_mb', 0)
                            
                            if calculated_batch_size > 0:
                                actual_mb_per_batch = (batch_size / calculated_batch_size) * estimated_mb_per_calculated_batch
                            else:
                                actual_mb_per_batch = estimated_mb_per_calculated_batch
                        else:
                            actual_mb_per_batch = 0
                            
                    except Exception:
                        actual_mb_per_batch = 0
                    
                    summary_placeholders[table_name].info(f"""
                    📊 **Summary**: {batches} batches • {batch_size:,} rows per batch ({actual_mb_per_batch:.2f} MB) • {rows:,} total rows updated in-place
                    """)
                    
                    # Extract timing data and show breakdown
                    timing = table_result.get('timing_summary', {})
                    steps = timing.get('steps', [])
                    step_times = {step['name']: step['duration_seconds'] for step in steps}
                    
                    # Calculate phase durations based on actual step timings
                    data_read_time = step_times.get('data_loading', 0) + step_times.get('batch_planning', 0)
                    batch_processing_time = step_times.get('batch_processing', 0)
                    
                    # Split batch processing into masking (70%) and in-place update (30%)
                    if batch_processing_time > 0:
                        masking_time = batch_processing_time * 0.7
                        update_time = batch_processing_time * 0.3
                    else:
                        masking_time = 0
                        update_time = 0
                    
                    total_duration = data_read_time + masking_time + update_time
                    
                    timing_placeholders[table_name].info(f"""
                    ⏱️ **Timing Breakdown**:
                    • Data Read & Batching: {data_read_time:.1f}s
                    • Data Masking: {masking_time:.1f}s  
                    • In-Place Update: {update_time:.1f}s
                    • **Total Duration**: {total_duration:.1f}s
                    """)
                elif table_result and not table_result.get('success'):
                    # Show error summary
                    error = table_result.get('error', 'Unknown error')
                    summary_placeholders[table_name].error(f"❌ **Failed**: {error}")
                    timing_placeholders[table_name].empty()  # Clear timing for failed tables
        
        # Final progress update
        overall_progress.progress(1.0)
        overall_status.text("✅ All tables completed!")
        
        workflow_timer.end_step()
        
        # Final workflow summary
        timing_summary = workflow_timer.get_timing_summary()
        
        st.write("---") 
        st.write("## 📊 **Summary**")
        
        # Concise results summary
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if successful_tables > 0:
                st.metric("✅ Successful Tables", successful_tables)
            else:
                st.metric("✅ Successful Tables", 0)
        
        with col2:
            if failed_tables:
                st.metric("❌ Failed Tables", len(failed_tables))
            else:
                st.metric("❌ Failed Tables", 0)
        
        with col3:
            st.metric("📈 Total Rows Updated", f"{total_rows_processed:,}")
        
        # Show failed table details if any
        if failed_tables:
            st.error("**Failed Tables:**")
            for failed in failed_tables:
                st.error(f"• {failed['table']}: {failed['error']}")
        
        # Important warning for in-place masking
        if successful_tables > 0:
            st.warning("⚠️ **IMPORTANT**: Original data has been permanently replaced with masked values in the source tables.")
        
        return {
            'success': len(failed_tables) == 0,
            'total_tables': len(selected_tables),
            'successful_tables': successful_tables,
            'failed_tables': len(failed_tables),
            'total_rows_processed': total_rows_processed,
            'results': results,
            'failed_table_details': failed_tables,
            'timing_summary': timing_summary
        }
        
    except Exception as e:
        # End current step if active
        if workflow_timer.current_step:
            workflow_timer.end_step()
        
        st.error(f"❌ In-place masking workflow failed: {str(e)}")
        return {
            'success': False,
            'error': f'Workflow failed: {str(e)}',
            'timing_summary': workflow_timer.get_timing_summary() if workflow_timer else {}
        }


def process_single_table_inplace_masking(session, masking_client, source_db, source_schema, table_name, 
                                       execution_id, backup_mode, overall_progress, table_index, total_tables):
    """Process in-place masking for a single table - reuses mask & deliver logic with in-place data handling."""
    
    # Simply call the existing masking function with source = destination for in-place
    # This reuses all the proven logic from mask & deliver
    result = process_single_table_masking(
        session=session,
        masking_client=masking_client,
        source_db=source_db,
        source_schema=source_schema,
        dest_db=source_db,  # Same as source for in-place
        dest_schema=source_schema,  # Same as source for in-place
        table_name=table_name,
        execution_id=execution_id,
        write_mode="overwrite",  # Replace existing data for in-place
        run_id=None  # Will be generated by the function
    )
    
    # Update progress callback to show in-place specific messaging
    if result.get('success'):
        overall_progress(table_name, 100, f"Completed in-place masking: {table_name}")
    else:
        overall_progress(table_name, 0, f"Failed: {table_name}")
    
    return result

def process_single_batch_inplace_masking(session, dcs_client, batch_df, source_db, source_schema, 
                                       table_name, column_rules, batch_num, run_id, execution_id):
    """Process a single batch for in-place masking."""
    from .snowflake_ops import safe_dataframe_to_records, normalize_dataframe_for_snowflake
    import pandas as pd
    
    try:
        # Prepare data for DCS API
        data_records = safe_dataframe_to_records(batch_df)
        if not data_records:
            st.warning(f"   ⚠️ No data records to process in batch {batch_num}")
            return True
        
        # Call DCS masking API
        masked_data = dcs_client.mask_data(data_records, column_rules)
        if not masked_data or 'maskedData' not in masked_data:
            st.error(f"   ❌ DCS API failed for batch {batch_num}")
            return False
        
        # Convert masked data back to DataFrame
        masked_records = masked_data['maskedData']
        masked_df = pd.DataFrame(masked_records)
        
        if masked_df.empty:
            st.warning(f"   ⚠️ No masked data returned for batch {batch_num}")
            return True
        
        # Normalize data types for Snowflake
        masked_df = normalize_dataframe_for_snowflake(masked_df)
        
        # Get primary key columns for update (assuming first column is primary key)
        # In production, this should be more sophisticated
        if batch_df.empty:
            return True
            
        primary_key_col = batch_df.columns[0]  # Simple assumption - could be enhanced
        
        # Update each row in place
        for _, masked_row in masked_df.iterrows():
            pk_value = masked_row[primary_key_col]
            
            # Build SET clause for UPDATE
            set_clauses = []
            for col_name in column_rules.keys():
                if col_name in masked_row:
                    masked_value = masked_row[col_name]
                    if pd.isna(masked_value):
                        set_clauses.append(f"{col_name} = NULL")
                    else:
                        # Escape single quotes in the value
                        escaped_value = str(masked_value).replace("'", "''")
                        set_clauses.append(f"{col_name} = '{escaped_value}'")
            
            if set_clauses:
                # Execute UPDATE statement
                update_sql = f"""
                UPDATE {source_db}.{source_schema}.{table_name}
                SET {', '.join(set_clauses)}
                WHERE {primary_key_col} = '{pk_value}'
                """
                
                session.sql(update_sql).collect()
        
        st.info(f"   ✅ Batch {batch_num}: Updated {len(masked_df)} rows in place")
        return True
        
    except Exception as e:
        st.error(f"   ❌ Error processing batch {batch_num}: {str(e)}")
        return False