
"""
Metadata Store Module

This module handles all metadata storage operations including table metadata,
discovery results, and algorithm management.
"""

import streamlit as st
import pandas as pd
# Import constants - handle both local and Snowflake environments
try:
    from config.constants import ENV_CONFIG
except ImportError:
    # Fallback for Snowflake environment
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


def get_environment_config():
    """
    Detect environment and return appropriate database/schema configuration.
    Returns dict with table prefixes for metadata tables.
    """
    try:
        from snowflake.snowpark.context import get_active_session
        session = get_active_session()
        if session:
            # Try to detect Native App environment by checking for app_schema
            try:
                session.sql("DESCRIBE SCHEMA app_schema").collect()
                # We're in a Native App environment
                return {
                    "environment": "native_app",
                    "discovered_ruleset": "app_schema.discovered_ruleset",
                    "dcs_events_log": "app_schema.dcs_events_log", 
                    "dcs_algorithms": "app_schema.dcs_algorithms"
                }
            except:
                # We're in local/external development environment
                return {
                    "environment": "local",
                    "discovered_ruleset": "dcs_db.dcsazure_metadata_store.discovered_ruleset",
                    "dcs_events_log": "dcs_db.dcsazure_metadata_store.dcs_events_log",
                    "dcs_algorithms": "dcs_db.dcsazure_metadata_store.dcs_algorithms"
                }
    except Exception as e:
        # Fallback to local environment
        return ENV_CONFIG['local']


# Initialize environment configuration
METADATA_CONFIG = get_environment_config()


def ensure_metadata_store_table(session):
    """Ensure the metadata store table exists (assumes table already exists)."""
    try:
        # Since table already exists, just test if we can write to it
        # Test with proper NOT NULL values for the existing table structure
        test_insert = f"""
        INSERT INTO {METADATA_CONFIG['discovered_ruleset']} 
        (specified_database, specified_schema, identified_table, identified_column, 
         identified_column_type, identified_column_max_length, ordinal_position, 
         row_count, discovery_complete, latest_event)
        VALUES ('TEST_DB', 'TEST_SCHEMA', 'TEST_TABLE', 'TEST_COLUMN', 
                'VARCHAR', 255, 1, 0, 1, 'test_insert')
        """
        session.sql(test_insert).collect()
        
        # Delete the test record
        test_delete = f"DELETE FROM {METADATA_CONFIG['discovered_ruleset']} WHERE specified_database = 'TEST_DB'"
        session.sql(test_delete).collect()
        
        st.success("✅ Metadata table write test successful")
        return True
        
    except Exception as e:
        st.error(f"❌ Metadata table write test failed: {str(e)}")
        return False


def insert_table_metadata(session, database, schema, selected_tables):
    """Merge table and column metadata for selected tables (preserves existing discovery data)."""
    try:
        # Create the tables list for the IN clause
        tables_list = "', '".join(selected_tables)
        
        # Use INFORMATION_SCHEMA query to get metadata
        metadata_query = f"""
        SELECT 
            T.TABLE_CATALOG AS specified_database,
            C.TABLE_SCHEMA AS specified_schema,
            C.TABLE_NAME AS identified_table,
            C.COLUMN_NAME AS identified_column,
            C.DATA_TYPE AS identified_column_type,
            IFNULL(C.CHARACTER_MAXIMUM_LENGTH,-1) AS identified_column_max_length,
            C.ORDINAL_POSITION AS ordinal_position,
            T.ROW_COUNT AS row_count
        FROM
            {database}.INFORMATION_SCHEMA.COLUMNS C,
            {database}.INFORMATION_SCHEMA.TABLES T
        WHERE
            UPPER(T.TABLE_CATALOG) = UPPER('{database}')
            AND UPPER(C.TABLE_SCHEMA) = UPPER('{schema}')
            AND C.TABLE_NAME = T.TABLE_NAME
            AND T.TABLE_NAME IN ('{tables_list}')
            AND C.TABLE_SCHEMA = T.TABLE_SCHEMA
            AND T.TABLE_TYPE = 'BASE TABLE'
        ORDER BY C.TABLE_NAME, C.ORDINAL_POSITION
        """
        
        # Get the metadata
        metadata_df = session.sql(metadata_query).to_pandas()
        
        if metadata_df.empty:
            return {'success': False, 'error': 'No metadata found for selected tables'}
        
        # Use MERGE to preserve existing discovery data while updating metadata
        columns_processed = 0
        columns_inserted = 0
        columns_updated = 0
        errors = []
        
        for _, row in metadata_df.iterrows():
            try:
                # Use MERGE to insert new or update existing metadata while preserving discovery fields
                merge_sql = f"""
                MERGE INTO {METADATA_CONFIG['discovered_ruleset']} AS target
                USING (
                    SELECT 
                        '{row['SPECIFIED_DATABASE']}' AS specified_database,
                        '{row['SPECIFIED_SCHEMA']}' AS specified_schema,
                        '{row['IDENTIFIED_TABLE']}' AS identified_table,
                        '{row['IDENTIFIED_COLUMN']}' AS identified_column,
                        '{row['IDENTIFIED_COLUMN_TYPE']}' AS identified_column_type,
                        {row['IDENTIFIED_COLUMN_MAX_LENGTH']} AS identified_column_max_length,
                        {row['ORDINAL_POSITION']} AS ordinal_position,
                        {row['ROW_COUNT']} AS row_count
                ) AS source
                ON target.specified_database = source.specified_database
                   AND target.specified_schema = source.specified_schema
                   AND target.identified_table = source.identified_table
                   AND target.identified_column = source.identified_column
                WHEN MATCHED THEN
                    UPDATE SET 
                        identified_column_type = source.identified_column_type,
                        identified_column_max_length = source.identified_column_max_length,
                        ordinal_position = source.ordinal_position,
                        row_count = source.row_count,
                        latest_event = 'metadata_updated',
                        last_profiled_updated_timestamp = CURRENT_TIMESTAMP()
                        -- Note: Preserves profiled_algorithm, confidence_score, assigned_algorithm
                WHEN NOT MATCHED THEN
                    INSERT (specified_database, specified_schema, identified_table, identified_column,
                            identified_column_type, identified_column_max_length, ordinal_position, row_count,
                            discovery_complete, latest_event, last_profiled_updated_timestamp)
                    VALUES (source.specified_database, source.specified_schema, source.identified_table, source.identified_column,
                            source.identified_column_type, source.identified_column_max_length, source.ordinal_position, source.row_count,
                            0, 'metadata_loaded', CURRENT_TIMESTAMP())
                """
                
                result = session.sql(merge_sql).collect()
                columns_processed += 1
                
                # Check if this was an insert or update by checking if record existed
                check_sql = f"""
                SELECT COUNT(*) as record_existed FROM {METADATA_CONFIG['discovered_ruleset']} 
                WHERE specified_database = '{database}' 
                AND specified_schema = '{schema}' 
                AND identified_table = '{row['IDENTIFIED_TABLE']}' 
                AND identified_column = '{row['IDENTIFIED_COLUMN']}'
                AND latest_event IN ('metadata_updated', 'discovery_complete')
                """
                
                existed_result = session.sql(check_sql).to_pandas()
                if existed_result.iloc[0]['RECORD_EXISTED'] > 0:
                    columns_updated += 1
                else:
                    columns_inserted += 1
                    
            except Exception as e:
                errors.append(f"Column {row['IDENTIFIED_COLUMN']}: {str(e)}")
        
        return {
            'success': True,
            'columns_processed': columns_processed,
            'columns_inserted': columns_inserted,
            'columns_updated': columns_updated,
            'total_columns': len(metadata_df),
            'tables_processed': len(selected_tables),
            'errors': errors
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}


def update_discovery_results(session, database, schema, table_name, discovery_data):
    """Update discovery results for a specific table (preserves existing assigned_algorithm)."""
    try:
        updates_made = 0
        errors = []
        
        for column_name, col_data in discovery_data.items():
            profiled_domain = col_data.get('domain', '').replace("'", "''")
            profiled_algorithm = col_data.get('algorithm', '').replace("'", "''")
            confidence_score = col_data.get('confidence', 0.0)
            
            try:
                # Only update assigned_algorithm if it's currently empty/null
                # This preserves user manual assignments from previous runs
                update_sql = f"""
                UPDATE {METADATA_CONFIG['discovered_ruleset']} 
                SET profiled_domain = '{profiled_domain}',
                    profiled_algorithm = '{profiled_algorithm}',
                    confidence_score = {confidence_score},
                    assigned_algorithm = CASE 
                        WHEN assigned_algorithm IS NULL OR assigned_algorithm = '' 
                        THEN '{profiled_algorithm.replace("'", "''")}'
                        ELSE assigned_algorithm
                    END,
                    discovery_complete = 1,
                    latest_event = 'discovery_completed',
                    last_profiled_updated_timestamp = CURRENT_TIMESTAMP()
                WHERE specified_database = '{database}' 
                AND specified_schema = '{schema}' 
                AND identified_table = '{table_name}'
                AND identified_column = '{column_name}'
                """
                session.sql(update_sql).collect()
                updates_made += 1
            except Exception as e:
                errors.append(f"Column {column_name}: {str(e)}")
        
        return {
            'success': True,
            'updates_made': updates_made,
            'sensitive_columns': len(discovery_data),
            'errors': errors
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}


def get_discovery_metadata(session, database=None, schema=None, table_name=None):
    """Retrieve discovery metadata from store."""
    try:
        where_clauses = []
        if database:
            where_clauses.append(f"specified_database = '{database}'")
        if schema:
            where_clauses.append(f"specified_schema = '{schema}'")
        if table_name:
            where_clauses.append(f"identified_table = '{table_name}'")
        
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        query = f"""
        SELECT 
            identified_table,
            identified_column,
            identified_column_type,
            identified_column_max_length,
            profiled_algorithm,
            confidence_score,
            assigned_algorithm
        FROM {METADATA_CONFIG['discovered_ruleset']}
        WHERE {where_clause}
        ORDER BY identified_table, ordinal_position
        """
        
        result_df = session.sql(query).to_pandas()
        
        # Removed debug output - results are shown in the UI component instead
        
        return result_df
        
    except Exception as e:
        st.error(f"Could not retrieve discovery metadata: {str(e)}")
        return pd.DataFrame()


def load_algorithms_from_database(session):
    """Load active algorithms from database with fallback to defaults."""
    try:
        algorithms_query = f"""
        SELECT algorithm_name 
        FROM {METADATA_CONFIG['dcs_algorithms']}
        WHERE is_active = true
        ORDER BY algorithm_name
        """
        algorithms_df = session.sql(algorithms_query).to_pandas()
        
        if not algorithms_df.empty:
            return algorithms_df['ALGORITHM_NAME'].tolist()
        else:
            st.warning("⚠️ No active algorithms found in database. Contact administrator to configure algorithms.")
            return []
    except Exception as e:
        st.warning(f"⚠️ Could not load algorithms from database: {str(e)}. Contact administrator.")
        return []


def update_assigned_algorithm(session, database, schema, table_name, column_name, new_algorithm):
    """Update assigned algorithm for a specific column."""
    try:
        # Handle None/null values properly
        if new_algorithm is None:
            algorithm_value = "NULL"
        elif new_algorithm == '':
            algorithm_value = "NULL"
        else:
            algorithm_value = f"'{new_algorithm}'"
        
        update_sql = f"""
        UPDATE {METADATA_CONFIG['discovered_ruleset']} 
        SET assigned_algorithm = {algorithm_value},
            latest_event = 'algorithm_updated'
        WHERE specified_database = '{database}' 
        AND specified_schema = '{schema}' 
        AND identified_table = '{table_name}'
        AND identified_column = '{column_name}'
        """
        
        # Debug: Show the SQL being executed
        st.write(f"**SQL Debug:** {update_sql}")
        
        result = session.sql(update_sql).collect()
        
        # Check if any rows were updated
        rows_updated = len(result) if result else 0
        st.write(f"**Rows updated:** {rows_updated}")
        
        return True
        
    except Exception as e:
        st.error(f"Failed to update assigned algorithm: {str(e)}")
        return False


def get_existing_discovery_results(session, database, schema, selected_tables=None):
    """Get all columns for a specific database and schema, optionally filtered by selected tables."""
    try:
        # Build the WHERE clause - removed discovery_complete = 1 to show ALL columns
        where_clauses = [
            f"specified_database = '{database}'",
            f"specified_schema = '{schema}'"
        ]
        
        # Add table filter if selected_tables is provided
        if selected_tables and len(selected_tables) > 0:
            tables_list = "', '".join(selected_tables)
            where_clauses.append(f"identified_table IN ('{tables_list}')")
        
        where_clause = " AND ".join(where_clauses)
        
        query = f"""
        SELECT 
            identified_table as "Table Name",
            identified_column as "Column Name", 
            identified_column_type as "Column Type",
            identified_column_max_length as "Column Length",
            profiled_algorithm as "Discovery Algorithm",
            confidence_score as "Confidence Score",
            assigned_algorithm as "Assigned Algorithm"
        FROM {METADATA_CONFIG['discovered_ruleset']}
        WHERE {where_clause}
        ORDER BY identified_table, ordinal_position
        """
        
        result_df = session.sql(query).to_pandas()
        return result_df
        
    except Exception as e:
        st.error(f"Could not retrieve existing discovery results: {str(e)}")
        return pd.DataFrame()


def get_active_algorithms(session):
    """Get list of active algorithms from dcs_algorithms table."""
    try:
        query = f"""
        SELECT algorithm_name 
        FROM {METADATA_CONFIG['dcs_algorithms']}
        WHERE is_active = true
        ORDER BY algorithm_name
        """
        
        result_df = session.sql(query).to_pandas()
        
        if not result_df.empty:
            return [""] + result_df['ALGORITHM_NAME'].tolist()  # Add empty option
        else:
            st.warning("⚠️ No active algorithms found in database.")
            return [""]
            
    except Exception as e:
        st.warning(f"⚠️ Could not load algorithms: {str(e)}")
        return [""]


def batch_update_assigned_algorithms(session, database, schema, algorithm_updates):
    """Batch update assigned algorithms for multiple columns."""
    try:
        updates_made = 0
        errors = []
        
        for update_info in algorithm_updates:
            table_name = update_info['table_name']
            column_name = update_info['column_name']
            new_algorithm = update_info['new_algorithm']
            
            try:
                # Handle None/null values properly
                if new_algorithm is None or new_algorithm == '':
                    algorithm_value = "NULL"
                else:
                    escaped_algorithm = new_algorithm.replace("'", "''")
                    algorithm_value = f"'{escaped_algorithm}'"
                
                update_sql = f"""
                UPDATE {METADATA_CONFIG['discovered_ruleset']} 
                SET assigned_algorithm = {algorithm_value},
                    latest_event = 'algorithm_updated',
                    last_profiled_updated_timestamp = CURRENT_TIMESTAMP()
                WHERE specified_database = '{database}' 
                AND specified_schema = '{schema}' 
                AND identified_table = '{table_name}'
                AND identified_column = '{column_name}'
                """
                
                session.sql(update_sql).collect()
                updates_made += 1
                
            except Exception as e:
                errors.append(f"Error updating {table_name}.{column_name}: {str(e)}")
        
        return {
            'success': True,
            'updates_made': updates_made,
            'total_requested': len(algorithm_updates),
            'errors': errors
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'updates_made': 0,
            'total_requested': len(algorithm_updates) if algorithm_updates else 0
        }