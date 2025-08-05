"""
Snowflake Operations Module

This module contains all Snowflake database operations including session management,
data manipulation, and table operations.
"""

import streamlit as st
import pandas as pd
import time
from snowflake.snowpark.context import get_active_session
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


def get_snowflake_session():
    """Get active Snowflake session - available natively in Snowflake Streamlit."""
    try:
        return get_active_session()
    except Exception as e:
        st.error(f"Failed to get Snowflake session: {str(e)}")
        return None


def get_environment_config():
    """
    Detect environment and return appropriate database/schema configuration.
    Returns dict with table prefixes for metadata tables.
    """
    try:
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


def safe_to_pandas(snowpark_df, convert_large_ints=True):
    """
    Safely convert Snowpark DataFrame to pandas with proper data types.
    Prevents int8 overflow by ensuring large integers use appropriate types.
    """
    try:
        if snowpark_df is None:
            return None
            
        # Convert to pandas
        pandas_df = snowpark_df.to_pandas()
        
        if convert_large_ints and not pandas_df.empty:
            # Fix potential int8 overflow issues by converting small int types to larger ones
            for col in pandas_df.columns:
                if pandas_df[col].dtype == 'int8':
                    # Check if any values would overflow int8 (-128 to 127)
                    max_val = pandas_df[col].max()
                    min_val = pandas_df[col].min()
                    if max_val > 127 or min_val < -128:
                        pandas_df[col] = pandas_df[col].astype('int64')
                    else:
                        pandas_df[col] = pandas_df[col].astype('int32')  # Use int32 as safe default
                elif pandas_df[col].dtype == 'int16':
                    # Convert int16 to int32 for safety
                    pandas_df[col] = pandas_df[col].astype('int32')
        
        return pandas_df
        
    except Exception as e:
        st.error(f"Error converting Snowpark DataFrame to pandas: {str(e)}")
        return None


def normalize_date_value(value):
    """Normalize various date formats to YYYY-MM-DD string format."""
    if pd.isna(value) or value is None:
        return None
    
    # Convert to string first
    str_value = str(value).strip()
    
    if not str_value or str_value.lower() in ['nan', 'none', 'null']:
        return None
    
    try:
        # Handle various date formats
        if len(str_value) == 8 and str_value.isdigit():  # Format: 08032016
            # Convert MMDDYYYY to YYYY-MM-DD
            month = str_value[:2]
            day = str_value[2:4]
            year = str_value[4:]
            return f"{year}-{month}-{day}"
        elif len(str_value) == 10 and str_value.count('-') == 2:  # Format: 2016-11-04
            return str_value
        elif hasattr(value, 'strftime'):  # datetime object
            return value.strftime('%Y-%m-%d')
        else:
            # Try pandas date parsing as last resort
            parsed_date = pd.to_datetime(str_value, errors='coerce')
            if pd.notna(parsed_date):
                return parsed_date.strftime('%Y-%m-%d')
            else:
                # Return as string if can't parse
                return str_value
    except Exception:
        return str_value


def normalize_dataframe_for_snowflake(df):
    """
    Normalize DataFrame to handle schema inconsistencies and mixed data types.
    This fixes the root cause of save failures for tables with inconsistent schemas.
    """
    if df is None or df.empty:
        return df
    
    df_normalized = df.copy()
    
    try:
        for col in df_normalized.columns:
            col_dtype = str(df_normalized[col].dtype)
            
            # Handle object columns that might contain mixed types
            if col_dtype == 'object':
                # Check for mixed date formats (like VEHICLE_REGISTRY PURCHASE_DATE)
                if col.upper().endswith('DATE') or 'DATE' in col.upper() or 'DOB' in col.upper():
                    # Normalize date formats
                    df_normalized[col] = df_normalized[col].apply(normalize_date_value)
                else:
                    # Convert all object columns to consistent strings
                    df_normalized[col] = df_normalized[col].astype(str)
            
            # Handle datetime columns
            elif 'datetime64' in col_dtype:
                df_normalized[col] = df_normalized[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Handle date columns
            elif col_dtype.startswith('date'):
                df_normalized[col] = df_normalized[col].apply(
                    lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) and hasattr(x, 'strftime') else str(x) if pd.notna(x) else None
                )
            
            # Ensure consistent numeric types
            elif 'int' in col_dtype or 'float' in col_dtype:
                # Convert to string to avoid type conflicts in Snowflake
                df_normalized[col] = df_normalized[col].astype(str)
        
        return df_normalized
        
    except Exception as e:
        st.error(f"Error normalizing DataFrame: {str(e)}")
        # Fallback: convert everything to strings
        try:
            for col in df_normalized.columns:
                df_normalized[col] = df_normalized[col].astype(str)
            return df_normalized
        except Exception as fallback_error:
            st.error(f"Fallback normalization failed: {str(fallback_error)}")
            return df


def safe_dataframe_to_records(df):
    """
    Safely convert DataFrame to records format for API calls.
    Uses the exact same approach as legacy code for proper date handling.
    """
    if df is None or df.empty:
        return []
    
    try:
        data_records = []
        
        # Process each row exactly like the legacy code
        for _, row in df.iterrows():
            record = {}
            for col in df.columns:
                value = row[col]
                # Handle various data types exactly like legacy code
                if pd.isna(value):
                    record[col] = ""
                elif hasattr(value, 'isoformat'):  # Handle dates - EXACT LEGACY LOGIC
                    record[col] = value.isoformat()
                else:
                    record[col] = str(value)
            data_records.append(record)
        
        return data_records
        
    except Exception as e:
        st.error(f"Error converting DataFrame to records: {str(e)}")
        # Fallback: try the old approach
        try:
            df_normalized = normalize_dataframe_for_snowflake(df)
            return df_normalized.to_dict('records')
        except Exception as fallback_error:
            st.error(f"Fallback conversion also failed: {str(fallback_error)}")
            return []


def get_snowflake_table_data(session, database, schema, table_name, sample_size=1000):
    """Load sample data from Snowflake table."""
    try:
        # Try different table name formats
        table_formats = [
            f"{database}.{schema}.{table_name}",
            f'{database.upper()}.{schema.upper()}.{table_name.upper()}'
        ]
        
        for table_format in table_formats:
            try:
                df = session.table(table_format).sample(n=sample_size)
                return safe_to_pandas(df)
            except:
                continue
        
        # If all formats fail
        st.error(f"Could not access table {database}.{schema}.{table_name}")
        return None
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None


def list_available_databases(session):
    """List all available databases using INFORMATION_SCHEMA."""
    try:
        # Use INFORMATION_SCHEMA.DATABASES view
        databases_query = "SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES ORDER BY DATABASE_NAME"
        databases_df = session.sql(databases_query).to_pandas()
        
        if not databases_df.empty:
            return databases_df['DATABASE_NAME'].tolist()
        else:
            st.info("No databases found")
            return []
            
    except Exception as e:
        st.warning(f"INFORMATION_SCHEMA.DATABASES failed: {str(e)}")
        
        # Try alternative approach using current database context
        try:
            current_db = session.get_current_database()
            if current_db:
                st.info(f"Using current database: {current_db}")
                return [current_db]
        except Exception as e2:
            st.warning(f"Could not get current database: {str(e2)}")
        
        # Last resort - provide some common database names
        st.info("Using fallback database list")
        return ["SNOWFLAKE_SAMPLE_DATA", "UTIL_DB", "TEST_DB"]


def list_available_schemas(session, database):
    """List all schemas in the specified database using INFORMATION_SCHEMA."""
    try:
        # Use INFORMATION_SCHEMA.SCHEMATA view
        schemas_query = f"""
        SELECT SCHEMA_NAME 
        FROM {database}.INFORMATION_SCHEMA.SCHEMATA 
        WHERE CATALOG_NAME = '{database}' 
        ORDER BY SCHEMA_NAME
        """
        schemas_df = session.sql(schemas_query).to_pandas()
        
        if not schemas_df.empty:
            return schemas_df['SCHEMA_NAME'].tolist()
        else:
            st.info(f"No schemas found in database {database}")
            return []
            
    except Exception as e:
        st.warning(f"INFORMATION_SCHEMA.SCHEMATA failed for {database}: {str(e)}")
        
        # Try alternative approach using current schema context
        try:
            current_schema = session.get_current_schema()
            if current_schema:
                st.info(f"Using current schema: {current_schema}")
                return [current_schema]
        except:
            pass
        
        # Last resort - provide some common schema names
        return ["PUBLIC", "INFORMATION_SCHEMA"]


def list_available_tables(session, database=None, schema=None):
    """List all tables in the specified database and schema using INFORMATION_SCHEMA."""
    try:
        # Get current database and schema if not provided
        if not database:
            database = session.get_current_database()
        if not schema:
            schema = session.get_current_schema()
        
        # Return empty DataFrame with message if database or schema is None
        if not database or not schema:
            st.info("No database or schema selected. Please check your connection.")
            return pd.DataFrame()
        
        # Use INFORMATION_SCHEMA.TABLES view
        tables_query = f"""
        SELECT 
            TABLE_NAME,
            TABLE_TYPE,
            ROW_COUNT,
            CREATED,
            LAST_ALTERED,
            COMMENT
        FROM {database}.INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_CATALOG = '{database}' 
        AND TABLE_SCHEMA = '{schema}'
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        
        tables_df = session.sql(tables_query).to_pandas()
        
        if tables_df.empty:
            st.info(f"No tables found in {database}.{schema} or insufficient permissions")
        
        return tables_df
        
    except Exception as e:
        st.warning(f"Could not list tables: {str(e)}")
        st.info("No tables available or insufficient permissions")
        return pd.DataFrame()


def get_table_definition(session, database, schema, table_name):
    """Get complete table definition including constraints using INFORMATION_SCHEMA."""
    try:
        # Get column definitions from INFORMATION_SCHEMA.COLUMNS
        columns_query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            ORDINAL_POSITION
        FROM {database}.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_CATALOG = '{database}' 
        AND TABLE_SCHEMA = '{schema}' 
        AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        
        columns_df = session.sql(columns_query).to_pandas()
        
        if columns_df.empty:
            st.warning(f"No columns found for table {database}.{schema}.{table_name}")
            return None
        
        # Get table constraints from INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        constraints_query = f"""
        SELECT 
            CONSTRAINT_NAME,
            CONSTRAINT_TYPE,
            COMMENT
        FROM {database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        WHERE TABLE_CATALOG = '{database}'
        AND TABLE_SCHEMA = '{schema}'
        AND TABLE_NAME = '{table_name}'
        ORDER BY CONSTRAINT_NAME
        """
        
        try:
            constraints_df = session.sql(constraints_query).to_pandas()
        except Exception as e:
            # If constraints query fails, continue without constraints
            constraints_df = pd.DataFrame()
            st.info(f"Note: Constraints query failed: {str(e)}")
        
        return {
            'columns': columns_df,
            'constraints': constraints_df
        }
        
    except Exception as e:
        st.warning(f"Could not get table definition: {str(e)}")
        st.info(f"Please check that the table {database}.{schema}.{table_name} exists and you have permissions to access it")
        return None


def create_target_table_with_structure(session, source_db, source_schema, source_table, 
                                      target_db, target_schema, target_table):
    """Create target table with same structure as source table."""
    try:
        # Get source table definition
        table_def = get_table_definition(session, source_db, source_schema, source_table)
        
        if not table_def or table_def['columns'].empty:
            st.error(f"Could not get structure of source table {source_db}.{source_schema}.{source_table}")
            return False
        
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
        CREATE TABLE {target_db}.{target_schema}.{target_table} (
            {ddl_joined}
        )
        """
        
        # Debug: Show the actual DDL being executed
        st.info(f"   🔍 **DDL being executed:**")
        st.code(create_table_sql, language="sql")
        
        # Execute CREATE TABLE
        try:
            session.sql(create_table_sql).collect()
            
            # Verify the table was created successfully and show its structure
            verify_sql = f"DESCRIBE TABLE {target_db}.{target_schema}.{target_table}"
            verify_result = session.sql(verify_sql).to_pandas()
            
            st.success(f"✅ Created target table {target_db}.{target_schema}.{target_table} with same structure as source")
            st.info(f"   🔍 **Created table structure ({len(verify_result)} columns):**")
            
            # Show the first few columns and their types
            for i, (_, row) in enumerate(verify_result.head(10).iterrows()):
                col_name = row.get('name', row.get('NAME', 'UNKNOWN'))
                col_type = row.get('type', row.get('TYPE', 'UNKNOWN'))
                col_nullable = row.get('null?', row.get('NULL?', 'Y'))
                st.info(f"      • {col_name}: {col_type} {'(nullable)' if col_nullable == 'Y' else '(not null)'}")
            
            if len(verify_result) > 10:
                st.info(f"      ... and {len(verify_result) - 10} more columns")
            
            return True
            
        except Exception as create_error:
            st.error(f"❌ Failed to create table {target_db}.{target_schema}.{target_table}")
            if "permission" in str(create_error).lower() or "access" in str(create_error).lower():
                st.error(f"   → Permission issue: {str(create_error)}")
            elif "does not exist" in str(create_error).lower():
                st.error(f"   → Database/schema issue: {str(create_error)}")
            else:
                st.error(f"   → Error details: {str(create_error)}")
            
            # Show the DDL that failed for debugging
            st.code(create_table_sql, language="sql")
            return False
        
    except Exception as e:
        st.error(f"❌ Failed to prepare table creation: {str(e)}")
        return False



def save_masked_data_to_snowflake(session, masked_df, database, schema, target_table, write_mode="append"):
    """
    Save masked data back to Snowflake using optimized bulk loading with COPY INTO.
    Uses internal staging and COPY INTO for maximum performance on large datasets.
    """
    try:
        # For small datasets (< 10K rows), use direct Snowpark DataFrame for simplicity
        if len(masked_df) < 10000:
            return _save_small_dataset_direct(session, masked_df, database, schema, target_table, write_mode)
        
        # For large datasets, try bulk loading with automatic fallback
        try:
            return _save_large_dataset_bulk(session, masked_df, database, schema, target_table, write_mode)
        except Exception as bulk_error:
            if "temporary STAGE" in str(bulk_error) or "Unsupported statement type" in str(bulk_error):
                st.warning(f"❌ Bulk loading failed: {str(bulk_error)}")
                st.info(f"💡 Falling back to direct method for this dataset")
                return _save_small_dataset_direct(session, masked_df, database, schema, target_table, write_mode)
            else:
                # Re-raise other types of errors
                raise bulk_error
        
    except Exception as e:
        st.error(f"❌ Error saving data: {str(e)}")
        return False

def _save_small_dataset_direct(session, masked_df, database, schema, target_table, write_mode):
    """Direct save for small datasets using Snowpark DataFrame."""
    try:
        st.info(f"   🔍 **Direct save debugging:**")
        st.info(f"      • Input DataFrame shape: {masked_df.shape}")
        st.info(f"      • Input DataFrame dtypes: {dict(masked_df.dtypes)}")
        
        # Show sample data before saving
        st.info(f"   🔍 **Sample data before saving:**")
        for col in masked_df.columns[:5]:  # Show first 5 columns
            sample_vals = masked_df[col].head(3).tolist()
            st.info(f"      • {col}: {sample_vals}")
        
        # Get target table structure before saving
        table_format = f"{database}.{schema}.{target_table}"
        target_table_columns = {}
        try:
            describe_query = f"DESCRIBE TABLE {table_format}"
            table_structure = session.sql(describe_query).to_pandas()
            st.info(f"   🔍 **Target table structure before saving:**")
            for _, row in table_structure.head(5).iterrows():
                col_name = row.get('name', row.get('NAME', 'UNKNOWN'))
                col_type = row.get('type', row.get('TYPE', 'UNKNOWN'))
                target_table_columns[col_name.upper()] = col_type.upper()
                st.info(f"      • {col_name}: {col_type}")
        except Exception as describe_error:
            st.warning(f"   ⚠️ Could not describe target table: {describe_error}")
        
        # Create Snowpark DataFrame from pandas DataFrame (legacy approach)
        snowpark_df = session.create_dataframe(masked_df)
        st.info(f"   🔍 **Snowpark DataFrame schema:**")
        snowpark_schema = snowpark_df.schema
        for field in snowpark_schema.fields[:5]:  # Show first 5 fields
            st.info(f"      • {field.name}: {field.datatype}")
        
        # Try different table name formats
        table_formats = [
            f"{database}.{schema}.{target_table}",
            f'{database.upper()}.{schema.upper()}.{target_table.upper()}'
        ]
        
        # Try to save to existing table
        for table_format in table_formats:
            try:
                st.info(f"   🔄 **Attempting to save to:** {table_format}")
                st.info(f"   🔄 **Write mode:** {write_mode}")
                
                snowpark_df.write.mode(write_mode).save_as_table(table_format)
                st.success(f"✅ Successfully saved {len(masked_df):,} rows using direct method")
                
                # Verify what was actually saved
                verify_query = f"SELECT * FROM {table_format} LIMIT 3"
                saved_data = session.sql(verify_query).to_pandas()
                st.info(f"   🔍 **Verification - Saved data sample:**")
                for col in saved_data.columns[:5]:  # Show first 5 columns
                    sample_vals = saved_data[col].head(3).tolist()
                    st.info(f"      • {col}: {sample_vals}")
                
                return True
            except Exception as e:
                if "does not exist" in str(e).lower():
                    st.error(f"❌ Target table {database}.{schema}.{target_table} does not exist")
                    return False
                else:
                    st.warning(f"   ⚠️ Failed with table format {table_format}: {str(e)}")
                    continue
        
        st.error(f"❌ Could not save to table {database}.{schema}.{target_table}")
        return False
        
    except Exception as e:
        st.error(f"❌ Error in direct save: {str(e)}")
        return False

def _save_large_dataset_bulk(session, masked_df, database, schema, target_table, write_mode):
    """
    Optimized bulk loading for large datasets using Snowflake COPY INTO command.
    Significantly faster than row-by-row inserts for analytical workloads.
    """
    import tempfile
    import os
    import uuid
    
    try:
        # Generate unique identifiers for this operation
        operation_id = str(uuid.uuid4())[:8]
        stage_name = f"dcs_bulk_stage_{operation_id}"
        file_name = f"masked_data_{operation_id}.csv"
        
        st.info(f"🚀 Using bulk loading for {len(masked_df):,} rows (Operation ID: {operation_id})")
        
        # Step 1: Create temporary internal stage (simplified approach)
        create_stage_sql = f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}"
        
        session.sql(create_stage_sql).collect()
        st.info(f"✅ Created temporary stage: {stage_name}")
        
        # Step 2: Export DataFrame to CSV and stage it
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as temp_file:
            # Write CSV with proper escaping for Snowflake (legacy approach)
            masked_df.to_csv(temp_file.name, index=False, quoting=1, doublequote=True)
            temp_file_path = temp_file.name
        
        try:
            # Use PUT command to upload file to internal stage
            put_sql = f"PUT 'file://{temp_file_path}' @{stage_name}/{file_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            session.sql(put_sql).collect()
            st.info(f"✅ Staged file: {file_name}")
            
            # Step 3: Determine target table format and clear if needed
            full_table_name = f"{database}.{schema}.{target_table}"
            
            if write_mode.lower() == "overwrite":
                # Clear existing data
                truncate_sql = f"TRUNCATE TABLE {full_table_name}"
                session.sql(truncate_sql).collect()
                st.info(f"✅ Truncated target table for overwrite mode")
            
            # Step 4: Bulk load using COPY INTO with optimized settings
            copy_sql = f"""
            COPY INTO {full_table_name}
            FROM @{stage_name}/{file_name}
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                RECORD_DELIMITER = '\\n'
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                ESCAPE_DOUBLE_QUOTES = TRUE
                NULL_IF = ('', 'NULL', 'null')
                EMPTY_FIELD_AS_NULL = TRUE
                ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            )
            FORCE = TRUE
            PURGE = TRUE
            ON_ERROR = 'ABORT_STATEMENT'
            """
            
            # Execute COPY INTO and capture results
            copy_result = session.sql(copy_sql).collect()
            
            # Parse results for success metrics
            if copy_result:
                result_row = copy_result[0]
                rows_loaded = result_row['rows_loaded'] if 'rows_loaded' in result_row else len(masked_df)
                status = result_row['status'] if 'status' in result_row else 'LOADED'
                
                if status == 'LOADED' and rows_loaded > 0:
                    st.success(f"🎉 Bulk loading completed successfully!")
                    st.success(f"📊 Loaded {rows_loaded:,} rows into {full_table_name}")
                    return True
                else:
                    st.error(f"❌ Bulk loading failed: Status={status}, Rows={rows_loaded}")
                    return False
            else:
                st.error("❌ No results returned from COPY INTO operation")
                return False
                
        finally:
            # Step 5: Cleanup
            try:
                # Remove temporary file
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)
                
                # Drop temporary stage (auto-dropped for temporary stages, but explicit is better)
                drop_stage_sql = f"DROP STAGE IF EXISTS {stage_name}"
                session.sql(drop_stage_sql).collect()
                st.info(f"🧹 Cleaned up temporary resources")
                
            except Exception as cleanup_error:
                st.warning(f"⚠️ Cleanup warning: {str(cleanup_error)}")
            
    except Exception as e:
        st.error(f"❌ Bulk loading failed: {str(e)}")
        st.error("💡 Falling back to direct method for smaller datasets")
        
        # Fallback to direct method
        if len(masked_df) < 50000:  # Reasonable fallback limit
            return _save_small_dataset_direct(session, masked_df, database, schema, target_table, write_mode)
        else:
            st.error("❌ Dataset too large for fallback method")
            return False