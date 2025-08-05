"""
Job Management Module

This module handles job tracking, performance monitoring, and execution management
for DCS operations.
"""

import streamlit as st
import pandas as pd
import time
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from .metadata_store import METADATA_CONFIG


def generate_execution_id() -> str:
    """Generate unique execution ID for tracking job batches."""
    unique_id = str(uuid.uuid4())
    return f"exec-{unique_id}"


def generate_run_id(table_name: str) -> str:
    """Generate unique run ID for individual table processing."""
    timestamp = datetime.now().strftime("%m%d%Y%H%M%S")
    return f"{table_name}-{timestamp}"


def log_discovery_job_start(session, execution_id: str, source_db: str, source_schema: str, table_name: str):
    """Log the start of a discovery job for a specific table."""
    try:
        run_id = generate_run_id(table_name)
        
        insert_sql = f"""
        INSERT INTO {METADATA_CONFIG['dcs_events_log']} 
        (execution_id, run_id, run_status, run_type, execution_start_time, 
         source_database, source_schema, source_table, dest_database, dest_schema, dest_table)
        VALUES ('{execution_id}', '{run_id}', 'WAITING', 'DISCOVERY', CURRENT_TIMESTAMP(),
                '{source_db}', '{source_schema}', '{table_name}', 'NA', 'NA', 'NA')
        """
        session.sql(insert_sql).collect()
        return run_id
        
    except Exception as e:
        st.warning(f"Failed to log discovery job start for {table_name}: {str(e)}")
        return None


def log_discovery_job_in_progress(session, execution_id: str, run_id: str):
    """Update discovery job status to IN PROGRESS."""
    try:
        update_sql = f"""
        UPDATE {METADATA_CONFIG['dcs_events_log']} 
        SET run_status = 'IN PROGRESS'
        WHERE execution_id = '{execution_id}' AND run_id = '{run_id}'
        """
        result = session.sql(update_sql).collect()
        
        # Check if any rows were updated
        check_sql = f"""
        SELECT COUNT(*) as updated_count 
        FROM {METADATA_CONFIG['dcs_events_log']} 
        WHERE execution_id = '{execution_id}' AND run_id = '{run_id}' AND run_status = 'IN PROGRESS'
        """
        check_result = session.sql(check_sql).collect()
        updated_count = check_result[0]['UPDATED_COUNT'] if check_result else 0
        
        if updated_count == 0:
            st.warning(f"⚠️ No rows updated to IN PROGRESS for execution_id: {execution_id}, run_id: {run_id}")
        else:
            st.info(f"✅ Updated job status to IN PROGRESS for run_id: {run_id}")
        
    except Exception as e:
        st.warning(f"Failed to update job status to IN PROGRESS for run_id {run_id}: {str(e)}")


def log_discovery_job_completion(session, execution_id: str, run_id: str, success: bool, error_message: str = None):
    """Log the completion of a discovery job."""
    try:
        status = 'COMPLETED' if success else 'FAILED'
        if error_message:
            escaped_error = error_message.replace("'", "''")
            error_clause = f", error_message = '{escaped_error}'"
        else:
            error_clause = ""
        
        update_sql = f"""
        UPDATE {METADATA_CONFIG['dcs_events_log']} 
        SET run_status = '{status}',
            execution_end_time = CURRENT_TIMESTAMP()
            {error_clause}
        WHERE execution_id = '{execution_id}' AND run_id = '{run_id}'
        """
        result = session.sql(update_sql).collect()
        
        # Check if any rows were updated
        check_sql = f"""
        SELECT COUNT(*) as updated_count 
        FROM {METADATA_CONFIG['dcs_events_log']} 
        WHERE execution_id = '{execution_id}' AND run_id = '{run_id}' AND run_status = '{status}'
        """
        check_result = session.sql(check_sql).collect()
        updated_count = check_result[0]['UPDATED_COUNT'] if check_result else 0
        
        if updated_count == 0:
            st.warning(f"⚠️ No rows updated to {status} for execution_id: {execution_id}, run_id: {run_id}")
            # Show existing records for debugging
            debug_sql = f"""
            SELECT execution_id, run_id, run_status 
            FROM {METADATA_CONFIG['dcs_events_log']} 
            WHERE execution_id = '{execution_id}' OR run_id = '{run_id}'
            """
            debug_result = session.sql(debug_sql).collect()
            st.info(f"🔍 Debug - Found {len(debug_result)} records with matching execution_id or run_id")
            for record in debug_result:
                st.info(f"   Record: execution_id={record['EXECUTION_ID']}, run_id={record['RUN_ID']}, status={record['RUN_STATUS']}")
        else:
            st.success(f"✅ Updated job status to {status} for run_id: {run_id}")
        
    except Exception as e:
        st.warning(f"Failed to log discovery job completion for run_id {run_id}: {str(e)}")


def update_execution_end_time_for_all(session, execution_id: str):
    """Update execution_end_time for all entries in an execution batch."""
    try:
        # Count rows before update
        count_sql = f"""
        SELECT COUNT(*) as rows_to_update 
        FROM {METADATA_CONFIG['dcs_events_log']} 
        WHERE execution_id = '{execution_id}' 
        AND execution_end_time IS NULL
        """
        count_result = session.sql(count_sql).collect()
        rows_to_update = count_result[0]['ROWS_TO_UPDATE'] if count_result else 0
        
        if rows_to_update > 0:
            update_sql = f"""
            UPDATE {METADATA_CONFIG['dcs_events_log']} 
            SET execution_end_time = CURRENT_TIMESTAMP()
            WHERE execution_id = '{execution_id}' 
            AND execution_end_time IS NULL
            """
            session.sql(update_sql).collect()
            st.success(f"✅ Updated execution_end_time for {rows_to_update} records in execution: {execution_id}")
        else:
            st.info(f"ℹ️ No records found to update execution_end_time for execution: {execution_id}")
        
    except Exception as e:
        st.warning(f"Failed to update execution end time for execution_id {execution_id}: {str(e)}")


def log_masking_job_start(session, execution_id: str, source_db: str, source_schema: str, table_name: str, dest_db: str, dest_schema: str):
    """Log the start of a masking job for a specific table."""
    try:
        run_id = generate_run_id(table_name)
        
        insert_sql = f"""
        INSERT INTO {METADATA_CONFIG['dcs_events_log']} 
        (execution_id, run_id, run_status, run_type, execution_start_time, 
         source_database, source_schema, source_table, dest_database, dest_schema, dest_table)
        VALUES ('{execution_id}', '{run_id}', 'WAITING', 'MASK_DELIVER', CURRENT_TIMESTAMP(),
                '{source_db}', '{source_schema}', '{table_name}', '{dest_db}', '{dest_schema}', '{table_name}')
        """
        session.sql(insert_sql).collect()
        return run_id
        
    except Exception as e:
        st.warning(f"Failed to log masking job start for {table_name}: {str(e)}")
        return None


def log_masking_job_in_progress(session, execution_id: str, run_id: str):
    """Update masking job status to IN PROGRESS."""
    try:
        update_sql = f"""
        UPDATE {METADATA_CONFIG['dcs_events_log']} 
        SET run_status = 'IN PROGRESS'
        WHERE execution_id = '{execution_id}' AND run_id = '{run_id}'
        """
        result = session.sql(update_sql).collect()
        
        # Check if any rows were updated
        check_sql = f"""
        SELECT COUNT(*) as updated_count 
        FROM {METADATA_CONFIG['dcs_events_log']} 
        WHERE execution_id = '{execution_id}' AND run_id = '{run_id}' AND run_status = 'IN PROGRESS'
        """
        check_result = session.sql(check_sql).collect()
        updated_count = check_result[0]['UPDATED_COUNT'] if check_result else 0
        
        if updated_count == 0:
            st.warning(f"⚠️ No rows updated to IN PROGRESS for execution_id: {execution_id}, run_id: {run_id}")
        else:
            st.info(f"✅ Updated masking job status to IN PROGRESS for run_id: {run_id}")
        
    except Exception as e:
        st.warning(f"Failed to update masking job status to IN PROGRESS for run_id {run_id}: {str(e)}")


def log_masking_job_completion(session, execution_id: str, run_id: str, success: bool, error_message: str = None):
    """Log the completion of a masking job."""
    try:
        status = 'COMPLETED' if success else 'FAILED'
        if error_message:
            escaped_error = error_message.replace("'", "''")
            error_clause = f", error_message = '{escaped_error}'"
        else:
            error_clause = ""
        
        update_sql = f"""
        UPDATE {METADATA_CONFIG['dcs_events_log']} 
        SET run_status = '{status}',
            execution_end_time = CURRENT_TIMESTAMP()
            {error_clause}
        WHERE execution_id = '{execution_id}' AND run_id = '{run_id}'
        """
        result = session.sql(update_sql).collect()
        
        # Check if any rows were updated
        check_sql = f"""
        SELECT COUNT(*) as updated_count 
        FROM {METADATA_CONFIG['dcs_events_log']} 
        WHERE execution_id = '{execution_id}' AND run_id = '{run_id}' AND run_status = '{status}'
        """
        check_result = session.sql(check_sql).collect()
        updated_count = check_result[0]['UPDATED_COUNT'] if check_result else 0
        
        if updated_count == 0:
            st.warning(f"⚠️ No rows updated to {status} for execution_id: {execution_id}, run_id: {run_id}")
            # Show existing records for debugging
            debug_sql = f"""
            SELECT execution_id, run_id, run_status 
            FROM {METADATA_CONFIG['dcs_events_log']} 
            WHERE execution_id = '{execution_id}' OR run_id = '{run_id}'
            """
            debug_result = session.sql(debug_sql).collect()
            st.info(f"🔍 Debug - Found {len(debug_result)} records with matching execution_id or run_id")
            for record in debug_result:
                st.info(f"   Record: execution_id={record['EXECUTION_ID']}, run_id={record['RUN_ID']}, status={record['RUN_STATUS']}")
        else:
            st.success(f"✅ Updated masking job status to {status} for run_id: {run_id}")
        
    except Exception as e:
        st.warning(f"Failed to log masking job completion for run_id {run_id}: {str(e)}")


def log_inplace_masking_job_start(session, execution_id: str, source_db: str, source_schema: str, table_name: str):
    """Log the start of an in-place masking job for a specific table."""
    try:
        run_id = generate_run_id(table_name)
        
        insert_sql = f"""
        INSERT INTO {METADATA_CONFIG['dcs_events_log']} 
        (execution_id, run_id, run_status, run_type, execution_start_time, 
         source_database, source_schema, source_table, dest_database, dest_schema, dest_table)
        VALUES ('{execution_id}', '{run_id}', 'WAITING', 'IN_PLACE_MASK', CURRENT_TIMESTAMP(),
                '{source_db}', '{source_schema}', '{table_name}', '{source_db}', '{source_schema}', '{table_name}')
        """
        session.sql(insert_sql).collect()
        return run_id
        
    except Exception as e:
        st.warning(f"Failed to log in-place masking job start for {table_name}: {str(e)}")
        return None


def log_inplace_masking_job_in_progress(session, execution_id: str, run_id: str):
    """Update in-place masking job status to IN PROGRESS."""
    try:
        update_sql = f"""
        UPDATE {METADATA_CONFIG['dcs_events_log']} 
        SET run_status = 'IN PROGRESS'
        WHERE execution_id = '{execution_id}' AND run_id = '{run_id}'
        """
        result = session.sql(update_sql).collect()
        
        # Check if any rows were updated
        check_sql = f"""
        SELECT COUNT(*) as updated_count 
        FROM {METADATA_CONFIG['dcs_events_log']} 
        WHERE execution_id = '{execution_id}' AND run_id = '{run_id}' AND run_status = 'IN PROGRESS'
        """
        check_result = session.sql(check_sql).collect()
        updated_count = check_result[0]['UPDATED_COUNT'] if check_result else 0
        
        if updated_count == 0:
            st.warning(f"⚠️ No rows updated to IN PROGRESS for execution_id: {execution_id}, run_id: {run_id}")
        else:
            st.info(f"✅ Updated in-place masking job status to IN PROGRESS for run_id: {run_id}")
        
    except Exception as e:
        st.warning(f"Failed to update in-place masking job status to IN PROGRESS for run_id {run_id}: {str(e)}")


def log_inplace_masking_job_completion(session, execution_id: str, run_id: str, success: bool, error_message: str = None):
    """Log the completion of an in-place masking job."""
    try:
        status = 'COMPLETED' if success else 'FAILED'
        if error_message:
            escaped_error = error_message.replace("'", "''")
            error_clause = f", error_message = '{escaped_error}'"
        else:
            error_clause = ""
        
        update_sql = f"""
        UPDATE {METADATA_CONFIG['dcs_events_log']} 
        SET run_status = '{status}',
            execution_end_time = CURRENT_TIMESTAMP()
            {error_clause}
        WHERE execution_id = '{execution_id}' AND run_id = '{run_id}'
        """
        result = session.sql(update_sql).collect()
        
        # Check if any rows were updated
        check_sql = f"""
        SELECT COUNT(*) as updated_count 
        FROM {METADATA_CONFIG['dcs_events_log']} 
        WHERE execution_id = '{execution_id}' AND run_id = '{run_id}' AND run_status = '{status}'
        """
        check_result = session.sql(check_sql).collect()
        updated_count = check_result[0]['UPDATED_COUNT'] if check_result else 0
        
        if updated_count == 0:
            st.warning(f"⚠️ No rows updated to {status} for execution_id: {execution_id}, run_id: {run_id}")
            # Show existing records for debugging
            debug_sql = f"""
            SELECT execution_id, run_id, run_status 
            FROM {METADATA_CONFIG['dcs_events_log']} 
            WHERE execution_id = '{execution_id}' OR run_id = '{run_id}'
            """
            debug_result = session.sql(debug_sql).collect()
            st.info(f"🔍 Debug - Found {len(debug_result)} records with matching execution_id or run_id")
            for record in debug_result:
                st.info(f"   Record: execution_id={record['EXECUTION_ID']}, run_id={record['RUN_ID']}, status={record['RUN_STATUS']}")
        else:
            st.success(f"✅ Updated in-place masking job status to {status} for run_id: {run_id}")
        
    except Exception as e:
        st.warning(f"Failed to log in-place masking job completion for run_id {run_id}: {str(e)}")


class PerformanceTimer:
    """Performance timing utility for tracking operation durations."""
    
    def __init__(self, operation_type: str, operation_name: str, execution_id: str):
        self.operation_type = operation_type
        self.operation_name = operation_name
        self.execution_id = execution_id
        self.start_time = time.time()
        self.steps = []
        self.current_step = None
        
    def start_step(self, step_name: str, description: str = ""):
        """Start timing a step within the operation."""
        if self.current_step:
            self.end_step()  # End previous step if not ended
            
        self.current_step = {
            'name': step_name,
            'description': description,
            'start_time': time.time()
        }
        
    def end_step(self):
        """End timing the current step."""
        if self.current_step:
            self.current_step['end_time'] = time.time()
            self.current_step['duration'] = self.current_step['end_time'] - self.current_step['start_time']
            self.steps.append(self.current_step)
            self.current_step = None
            
    def get_timing_summary(self) -> Dict[str, Any]:
        """Get complete timing summary for the operation."""
        total_duration = time.time() - self.start_time
        
        return {
            'operation_type': self.operation_type,
            'operation_name': self.operation_name,
            'execution_id': self.execution_id,
            'total_duration_seconds': round(total_duration, 3),
            'steps': [
                {
                    'name': step['name'],
                    'description': step['description'],
                    'duration_seconds': round(step['duration'], 3)
                }
                for step in self.steps
            ],
            'step_count': len(self.steps)
        }


def log_job_start(session, run_id: str, operation_type: str, database: str, schema: str, 
                 table_name: str, execution_id: str):
    """Log the start of a DCS job operation."""
    try:
        insert_sql = f"""
        INSERT INTO {METADATA_CONFIG['dcs_events_log']} 
        (run_id, operation_type, source_database, source_schema, source_table, 
         execution_id, job_status, start_timestamp)
        VALUES ('{run_id}', '{operation_type}', '{database}', '{schema}', '{table_name}', 
                '{execution_id}', 'STARTED', CURRENT_TIMESTAMP())
        """
        session.sql(insert_sql).collect()
        
    except Exception as e:
        st.warning(f"Failed to log job start: {str(e)}")


def log_job_completion(session, run_id: str, database: str, schema: str, table_name: str, 
                      success: bool, error_message: str = None):
    """Log the completion of a DCS job operation."""
    try:
        status = 'COMPLETED' if success else 'FAILED'
        if error_message:
            # Escape single quotes in error message for SQL
            escaped_error = error_message.replace("'", "''")
            error_clause = f", error_message = '{escaped_error}'"
        else:
            error_clause = ""
        
        update_sql = f"""
        UPDATE {METADATA_CONFIG['dcs_events_log']} 
        SET job_status = '{status}',
            end_timestamp = CURRENT_TIMESTAMP(),
            duration_seconds = DATEDIFF(second, start_timestamp, CURRENT_TIMESTAMP())
            {error_clause}
        WHERE run_id = '{run_id}'
        """
        session.sql(update_sql).collect()
        
    except Exception as e:
        st.warning(f"Failed to log job completion: {str(e)}")


def get_events_log(session, limit: int = 100) -> pd.DataFrame:
    """Retrieve recent events from the DCS events log."""
    try:
        query = f"""
        SELECT 
            run_id,
            operation_type,
            source_database,
            source_schema,
            source_table,
            execution_id,
            job_status,
            start_timestamp,
            end_timestamp,
            duration_seconds,
            error_message
        FROM {METADATA_CONFIG['dcs_events_log']}
        ORDER BY start_timestamp DESC
        LIMIT {limit}
        """
        
        return session.sql(query).to_pandas()
        
    except Exception as e:
        st.error(f"Failed to retrieve events log: {str(e)}")
        return pd.DataFrame()


def get_job_statistics(session, execution_id: str = None) -> Dict[str, Any]:
    """Get job statistics and performance metrics."""
    try:
        where_clause = f"WHERE execution_id = '{execution_id}'" if execution_id else ""
        
        stats_query = f"""
        SELECT 
            COUNT(*) as total_jobs,
            SUM(CASE WHEN job_status = 'COMPLETED' THEN 1 ELSE 0 END) as successful_jobs,
            SUM(CASE WHEN job_status = 'FAILED' THEN 1 ELSE 0 END) as failed_jobs,
            SUM(CASE WHEN job_status = 'STARTED' THEN 1 ELSE 0 END) as running_jobs,
            AVG(duration_seconds) as avg_duration_seconds,
            MAX(duration_seconds) as max_duration_seconds,
            MIN(duration_seconds) as min_duration_seconds
        FROM {METADATA_CONFIG['dcs_events_log']}
        {where_clause}
        """
        
        stats_df = session.sql(stats_query).to_pandas()
        
        if not stats_df.empty:
            stats = stats_df.iloc[0].to_dict()
            return {
                'total_jobs': int(stats.get('TOTAL_JOBS', 0)),
                'successful_jobs': int(stats.get('SUCCESSFUL_JOBS', 0)),
                'failed_jobs': int(stats.get('FAILED_JOBS', 0)),
                'running_jobs': int(stats.get('RUNNING_JOBS', 0)),
                'avg_duration_seconds': float(stats.get('AVG_DURATION_SECONDS', 0)) if stats.get('AVG_DURATION_SECONDS') else 0,
                'max_duration_seconds': float(stats.get('MAX_DURATION_SECONDS', 0)) if stats.get('MAX_DURATION_SECONDS') else 0,
                'min_duration_seconds': float(stats.get('MIN_DURATION_SECONDS', 0)) if stats.get('MIN_DURATION_SECONDS') else 0
            }
        else:
            return {
                'total_jobs': 0,
                'successful_jobs': 0,
                'failed_jobs': 0,
                'running_jobs': 0,
                'avg_duration_seconds': 0,
                'max_duration_seconds': 0,
                'min_duration_seconds': 0
            }
            
    except Exception as e:
        st.error(f"Failed to get job statistics: {str(e)}")
        return {
            'total_jobs': 0,
            'successful_jobs': 0,
            'failed_jobs': 0,
            'running_jobs': 0,
            'avg_duration_seconds': 0,
            'max_duration_seconds': 0,
            'min_duration_seconds': 0
        }


def calculate_optimal_batch_size(total_rows: int, num_columns_to_mask: int) -> int:
    """Calculate optimal batch size based on data volume and complexity."""
    # Import constants - handle both local and Snowflake environments
    try:
        from config.constants import DEFAULT_BATCH_SIZE, MAX_BATCH_SIZE, MIN_BATCH_SIZE
    except ImportError:
        # Fallback for Snowflake environment
        DEFAULT_BATCH_SIZE = 5000
        MAX_BATCH_SIZE = 10000
        MIN_BATCH_SIZE = 100
    
    # Base batch size
    base_batch_size = DEFAULT_BATCH_SIZE
    
    # Adjust based on total rows
    if total_rows < 1000:
        size_factor = 0.5  # Smaller batches for small datasets
    elif total_rows < 10000:
        size_factor = 0.8
    elif total_rows < 100000:
        size_factor = 1.0
    elif total_rows < 1000000:
        size_factor = 1.2
    else:
        size_factor = 1.5  # Larger batches for very large datasets
    
    # Adjust based on number of columns to mask
    if num_columns_to_mask <= 2:
        column_factor = 1.2  # Larger batches for fewer columns
    elif num_columns_to_mask <= 5:
        column_factor = 1.0
    elif num_columns_to_mask <= 10:
        column_factor = 0.8
    else:
        column_factor = 0.6  # Smaller batches for many columns
    
    # Calculate optimal size
    optimal_size = int(base_batch_size * size_factor * column_factor)
    
    # Apply bounds
    optimal_size = max(MIN_BATCH_SIZE, min(MAX_BATCH_SIZE, optimal_size))
    
    return optimal_size


def create_progress_card(title: str, value: str, delta: str = None, help_text: str = None):
    """Create a styled progress card for metrics display."""
    delta_html = f"<p style='color: #28a745; font-size: 14px; margin: 0;'>{delta}</p>" if delta else ""
    help_html = f"<p style='color: #6c757d; font-size: 12px; margin: 5px 0 0 0;'>{help_text}</p>" if help_text else ""
    
    card_html = f"""
    <div style='
        background: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #4C00FF;
        margin: 10px 0;
    '>
        <h3 style='color: #4C00FF; margin: 0 0 10px 0; font-size: 16px;'>{title}</h3>
        <p style='font-size: 24px; font-weight: bold; margin: 0; color: #343a40;'>{value}</p>
        {delta_html}
        {help_html}
    </div>
    """
    
    return card_html


def create_metrics_dashboard(stats: Dict[str, Any], execution_id: str = None):
    """Create a professional metrics dashboard."""
    
    st.subheader("📊 Job Performance Metrics")
    
    # Import the UI components for metrics
    from .ui_components import create_metric_card
    
    # Ensure stats has all required keys with default values
    default_stats = {
        'total_jobs': 0,
        'successful_jobs': 0,
        'failed_jobs': 0,
        'running_jobs': 0,
        'avg_duration_seconds': 0,
        'max_duration_seconds': 0,
        'min_duration_seconds': 0
    }
    
    # Merge with provided stats, using defaults for missing keys
    safe_stats = {**default_stats, **stats}
    
    # Create metrics columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        success_rate = (safe_stats['successful_jobs'] / max(safe_stats['total_jobs'], 1)) * 100
        create_metric_card(
            "Success Rate",
            f"{success_rate:.1f}%",
            "fas fa-check-circle",
            "success",
            f"{safe_stats['successful_jobs']} completed"
        )
    
    with col2:
        create_metric_card(
            "Total Jobs",
            str(safe_stats['total_jobs']),
            "fas fa-tasks",
            "primary",
            f"{safe_stats['running_jobs']} running" if safe_stats['running_jobs'] > 0 else "All completed"
        )
    
    with col3:
        avg_duration = safe_stats['avg_duration_seconds']
        if avg_duration > 60:
            duration_display = f"{avg_duration/60:.1f}m"
        else:
            duration_display = f"{avg_duration:.1f}s"
            
        create_metric_card(
            "Avg Duration",
            duration_display,
            "fas fa-clock",
            "info",
            f"Max: {safe_stats['max_duration_seconds']:.1f}s"
        )
    
    with col4:
        card_type = "warning" if safe_stats['failed_jobs'] > 0 else "success"
        create_metric_card(
            "Failed Jobs", 
            str(safe_stats['failed_jobs']),
            "fas fa-exclamation-triangle",
            card_type,
            "Click Monitoring for details" if safe_stats['failed_jobs'] > 0 else "No failures"
        )
    
    # Show execution ID if provided
    if execution_id:
        st.info(f"🆔 **Current Execution ID**: `{execution_id}`")
        
    return safe_stats