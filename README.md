# dcs-snowflake-streamlit
Streamlit App in Snowflake to Discover and Mask sensitive data using Delphix Compliance Service

### Dependencies
- Python dependencies managed via conda (environment.yml)
- Core dependencies: streamlit=1.35.0, pandas=2.0.3, requests=2.31.0
- Uses Snowflake's native Python environment when deployed

### Core Architecture
- **Main App**: `streamlit_app.py` - Entry point with DattaAble UI layout
- **Modular Design**: Business logic split into focused modules under `/modules/`
- **Dual Environment Support**: Runs in both local development and Snowflake Native App contexts
- **External Integration**: Connects to DCS API via Azure AD authentication

### Key Modules
- **`dcs_client.py`**: DCS API client with Azure AD authentication and dual HTTP handling (requests vs Snowflake HTTP)
- **`snowflake_ops.py`**: All Snowflake database operations, session management, and data manipulation
- **`business_engines.py`**: Core discovery and masking workflow engines with parallel processing
- **`metadata_store.py`**: Metadata persistence and discovery results management
- **`job_manager.py`**: Job tracking, performance monitoring, and execution management
- **`ui_components.py`**: Reusable UI components and custom styling

### Data Flow
1. **Discovery**: Load table samples → DCS API profiling → Store results in metadata tables
2. **Masking**: Retrieve discovery metadata → Batch processing → DCS masking API → Save to target tables
3. **Monitoring**: Track job performance, execution metrics, and audit trails

### Environment Detection
- **Native App**: Detects `app_schema` existence for Snowflake Native App deployment
- **Local/External**: Uses `dcs_db.dcsazure_metadata_store` schema for development
- Automatic fallback configuration in each module

### Key Features
- **Parallel Processing**: ThreadPoolExecutor for concurrent table operations (max 4 workers for Snowflake memory limits)
- **Real-Time Progress Tracking**: Live progress bars and status updates with thread-safe UI updates
- **Chunked Data Processing**: Memory-efficient data loading in batches instead of loading full tables
- **Cost-Optimized Batch Processing**: Dynamic batch sizing based on 1.8MB API limit for optimal cost efficiency
- **Performance Monitoring**: Detailed timing breakdown (Data Read & Batching, Data Masking, Masked Data Load)
- **Real-Time Database Status Updates**: dcs_events_log synced with UI progress (WAITING → IN_PROGRESS → COMPLETED/FAILED)
- **Error Handling**: Comprehensive error handling with user-friendly messages
- **Dual HTTP Support**: Native Snowflake HTTP functions for Native Apps, requests library for local development

## External Access Integration Requirements

For Snowflake deployment, External Access Integration must be configured:

```sql
-- 1. Create Network Rule (run as ACCOUNTADMIN)
CREATE OR REPLACE NETWORK RULE dcs_api_network_rule
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('azure.apps.delphix.io:443', 'login.microsoftonline.com:443');

-- 2. Create External Access Integration  
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dcs_api_integration
ALLOWED_NETWORK_RULES = (dcs_api_network_rule)
ENABLED = true;

-- 3. Grant Usage
GRANT USAGE ON INTEGRATION dcs_api_integration TO ROLE your_role;

-- 4. Update Streamlit App
ALTER STREAMLIT your_streamlit_app SET EXTERNAL_ACCESS_INTEGRATIONS = (dcs_api_integration);
```

## Configuration

### SQL Metadata Configuration

For Snowflake deployment, metadata and event tables must be configured to persist data:

```sql
-- 1. Create Metadata Table
create TABLE 
    discovered_ruleset 
    ( 
        specified_database              VARCHAR(255),
        specified_schema                VARCHAR(255),
        identified_table                VARCHAR(255),
        identified_column               VARCHAR(255),
        identified_column_type          VARCHAR(100),
        identified_column_max_length    INT NOT NULL, 
        ordinal_position                INT NOT NULL, 
        row_count                       BIGINT, 
        source_metadata                 TEXT ,
        profiled_domain                 VARCHAR(100) ,
        profiled_algorithm              VARCHAR(100),
        confidence_score                DECIMAL(6,5), 
        rows_profiled                   BIGINT DEFAULT 0, 
        assigned_algorithm              TEXT  ,
        last_profiled_updated_timestamp DATETIME, 
        discovery_complete              INT, 
        latest_event VARCHAR(100), 
        algorithm_metadata TEXT,
        CONSTRAINT discovered_ruleset_pk PRIMARY KEY (specified_database, specified_schema , identified_table, identified_column));

-- 2. Algorithms table
create TABLE 
    dcs_algorithms (
    algorithm_name varchar(100),
    is_active boolean
    );

--Sample Entries
insert into dcs_algorithms values
('dlpx-core:FullName', true),('dlpx-core:FirstName', true),('dlpx-core:LastName', true),('DateShiftDiscrete', true),
('dlpx-core:CM Numeric', true), ('AddrLookup', true),('USCitiesLookup', true), ('USstatecodesLookup', true),
('dlpx-core:CM Alpha-Numeric', true),('dlpx-core:Phone Unique', true), ('dlpx-core:Email Unique', true),
('NullValueLookup', true);  

-- 3. Events Table
CREATE OR REPLACE TABLE 
    dcs_events_log 
    ( 
        execution_id varchar(100) NOT NULL,
        run_id varchar(100) NOT NULL, 
        run_status varchar(100) NOT NULL,
        run_type varchar(100) NOT NULL,
        execution_start_time timestamp, 
        execution_end_time timestamp, 
        source_database  VARCHAR(100), 
        source_schema    VARCHAR(100), 
        source_table     VARCHAR(100), 
        dest_database    VARCHAR(255), 
        dest_schema      VARCHAR(100), 
        dest_table       VARCHAR(100),
        error_message   text 
    );

```

### DCS API Configuration
- Azure Tenant ID, Client ID, Client Secret required
- Default scope: `https://analysis.windows.net/powerbi/api/.default`
- Configurable via sidebar or `config/constants.py`

### Environment Variables
Key constants in `config/constants.py`:
- `ENV_CONFIG`: Environment-specific database/schema mappings
- `PAGE_CONFIG`: Streamlit page configuration
- Batch processing limits and defaults

## Development Notes

### File Structure
```
/modules/
├── business_engines.py     # Core discovery/masking logic
├── dcs_client.py          # DCS API integration
├── job_manager.py         # Performance tracking
├── metadata_store.py      # Data persistence
├── snowflake_ops.py       # Database operations
└── ui_components.py       # UI components

/config/
└── constants.py           # Configuration constants

streamlit_app.py           # Main application entry point
environment.yml            # Conda environment specification
```

