"""
DCS API Client Module

This module contains the DCS configuration and API client classes for 
integrating with Delphix Compliance Service.
"""

import json
import uuid
import requests
from typing import Dict, Any, List
from snowflake.snowpark.context import get_active_session
# Import constants - handle both local and Snowflake environments
try:
    from config.constants import DEFAULT_AZURE_SCOPE
except ImportError:
    # Fallback for Snowflake environment
    DEFAULT_AZURE_SCOPE = "https://analysis.windows.net/powerbi/api/.default"



class DCSConfig:
    """DCS configuration for API access."""
    
    def __init__(self, dcs_api_url: str, azure_tenant_id: str = None, 
                 azure_client_id: str = None, azure_client_secret: str = None,
                 azure_scope: str = DEFAULT_AZURE_SCOPE,
                 timeout: int = 120):
        self.dcs_api_url = dcs_api_url.rstrip('/')
        self.azure_tenant_id = azure_tenant_id
        self.azure_client_id = azure_client_id
        self.azure_client_secret = azure_client_secret
        self.azure_scope = azure_scope
        self.timeout = timeout


class DCSAPIClient:
    """Client for DCS REST API operations - Streamlit compatible version."""
    
    def __init__(self, config: DCSConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })
        # Check if running in Native App context
        self.is_native_app = self._detect_native_app_context()
    
    def _detect_native_app_context(self) -> bool:
        """Detect if running in Snowflake Native App context."""
        try:
            session = get_active_session()
            # In Native App, check if we have access to app-specific objects
            result = session.sql("SELECT CURRENT_DATABASE()").collect()
            current_db = result[0][0] if result else None
            # Native apps typically have database names that don't follow standard patterns
            return current_db and ('APPLICATION_' in current_db or 'APP_' in current_db)
        except:
            return False
    
    def _make_http_request(self, method: str, url: str, data=None, headers=None):
        """Make HTTP request using appropriate method for context."""
        if self.is_native_app:
            return self._make_snowflake_http_request(method, url, data, headers)
        else:
            return self._make_requests_http_request(method, url, data, headers)
    
    def _make_snowflake_http_request(self, method: str, url: str, data=None, headers=None):
        """Make HTTP request using Snowflake's HTTP functions for Native App."""
        try:
            session = get_active_session()
            
            # Prepare headers for Snowflake SQL
            headers_dict = headers or {}
            headers_sql = ", ".join([f"'{k}': '{v}'" for k, v in headers_dict.items()])
            headers_object = f"OBJECT_CONSTRUCT({headers_sql})" if headers_sql else "OBJECT_CONSTRUCT()"
            
            if method.upper() == 'POST':
                # Escape single quotes in data for SQL
                data_escaped = data.replace("'", "''") if data else ""
                sql = f"""
                SELECT
                  status_code,
                  headers,
                  body
                FROM TABLE(
                  SNOWFLAKE.CORE.HTTP_POST(
                    url => '{url}',
                    headers => {headers_object},
                    data => '{data_escaped}'
                  )
                )
                """
            else:  # GET
                sql = f"""
                SELECT
                  status_code,
                  headers,
                  body
                FROM TABLE(
                  SNOWFLAKE.CORE.HTTP_GET(
                    url => '{url}',
                    headers => {headers_object}
                  )
                )
                """
            
            result = session.sql(sql).collect()
            if result:
                return result[0]  # Return the response
            else:
                raise Exception("No response from HTTP request")
                
        except Exception as e:
            # If Snowflake HTTP fails, provide guidance for Native App setup
            raise Exception(
                f"Native App HTTP request failed: {str(e)}\n"
                "This indicates the External Access Integration is not properly configured.\n"
                "Please ensure the consumer has:\n"
                "1. Created network rules for external domains\n"
                "2. Created external access integration with these rules\n"
                "3. Bound the integration to this application using ALTER APPLICATION"
            )
    
    def _make_requests_http_request(self, method: str, url: str, data=None, headers=None):
        """Make HTTP request using Python requests library."""
        if method.upper() == 'POST':
            return requests.post(url, data=data, headers=headers, timeout=30)
        else:
            return requests.get(url, headers=headers, timeout=30)

    def get_azure_ad_token(self) -> str:
        """Get Azure AD OAuth2 token using service principal."""
        try:
            body = (
                f"client_id={self.config.azure_client_id}"
                f"&scope={self.config.azure_scope}"
                f"&client_secret={self.config.azure_client_secret}"
                f"&grant_type=client_credentials"
            )
            
            token_url = f"https://login.microsoftonline.com/{self.config.azure_tenant_id}/oauth2/v2.0/token/"
            
            response = self._make_http_request(
                'POST',
                token_url,
                data=body,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            
            # Handle response based on context
            if self.is_native_app:
                # Snowflake HTTP response format (Row object from SQL result)
                status_code = response[0] if response and len(response) > 0 else 500  # STATUS_CODE column
                response_text = response[2] if response and len(response) > 2 else ''  # BODY column
                
                if status_code != 200:
                    raise Exception(f"Azure AD authentication failed: {status_code} - {response_text}")
                
                try:
                    token_data = json.loads(response_text)
                except json.JSONDecodeError:
                    raise Exception(f"Invalid JSON response from Azure AD: {response_text}")
            else:
                # Regular requests response
                if response.status_code != 200:
                    raise Exception(f"Azure AD authentication failed: {response.status_code} - {response.text}")
                token_data = response.json()
            
            access_token = token_data.get('access_token')
            
            if not access_token:
                raise Exception("No access token in Azure AD response")
            
            return f"Bearer {access_token}"
            
        except requests.exceptions.ConnectionError as e:
            if "Failed to resolve" in str(e):
                raise Exception(
                    "❌ Cannot reach login.microsoftonline.com\n"
                    "This indicates External Access Integration is not configured.\n"
                    "Please run the setup SQL commands as ACCOUNTADMIN:"
                    "1. Create Network Rule for login.microsoftonline.com"
                    "2. Create External Access Integration"
                    "3. Grant usage to your role"
                    "4. Update Streamlit app to use the integration\n"
                    "See 'Setup Instructions' in the sidebar for complete SQL commands."
                )
            else:
                raise Exception(f"Network connection error: {str(e)}")
        except Exception as e:
            raise Exception(f"Authentication error: {str(e)}")
    
    def profile_data_raw(self, column_data: Dict[str, List[Any]]) -> Dict[str, Any]:
        """Profile data using DCS discovery API."""
        try:
            url = f"{self.config.dcs_api_url}/v1/discovery/profileByColumn"
            run_id = 'sf-'+ str(uuid.uuid4())
            
            headers = {
                'Authorization': self.get_azure_ad_token(),
                'Content-Type': 'application/json',
                'Run-Id': run_id
            }
            
            response = self._make_http_request(
                'POST',
                url,
                data=json.dumps(column_data),
                headers=headers
            )
            
            # Handle response based on context
            if self.is_native_app:
                # Snowflake HTTP response format (Row object from SQL result)
                status_code = response[0] if response and len(response) > 0 else 500  # STATUS_CODE column
                response_text = response[2] if response and len(response) > 2 else ''  # BODY column
                
                if status_code != 200:
                    raise Exception(f"DCS API error: {status_code} - {response_text}")
                
                try:
                    result = json.loads(response_text)
                except json.JSONDecodeError:
                    raise Exception(f"Invalid JSON response from DCS API: {response_text}")
            else:
                if response.status_code != 200:
                    raise Exception(f"DCS API error: {response.status_code} - {response.text}")
                result = response.json()
            
            result['run_id'] = run_id  # Add run_id to response for logging
            return result
            
        except requests.exceptions.ConnectionError as e:
            if "Failed to resolve" in str(e):
                domain = self.config.dcs_api_url.replace('https://', '').replace('http://', '').split('/')[0]
                raise Exception(
                    f"❌ Cannot reach {domain}\n"
                    "This indicates External Access Integration is missing your DCS API domain.\n"
                    "Please update the Network Rule to include your DCS API domain:\n"
                    f"ALTER NETWORK RULE dcs_api_network_rule"
                    f"SET VALUE_LIST = ('{domain}:443', 'login.microsoftonline.com:443');"
                )
            else:
                raise Exception(f"Network connection error: {str(e)}")
        except Exception as e:
            raise Exception(f"Discovery API error: {str(e)}")
    
    def mask_data_raw_powerquery_format(self, data_records: List[Dict[str, Any]], 
                                       column_rules: Dict[str, str]) -> List[Dict[str, Any]]:
        """Mask data using DCS masking API with Power Query format."""
        
        # Add batch IDs
        for i, record in enumerate(data_records):
            record["DELPHIX_COMPLIANCE_SERVICE_BATCH_ID"] = i + 1
        
        # Create column arrays
        columns_to_mask = list(column_rules.keys())
        column_lists = {}
        
        for column_name in columns_to_mask:
            column_values = [record.get(column_name) for record in data_records]
            column_lists[column_name] = column_values
        
        # Add batch ID column (required by DCS API)
        batch_id_values = [record["DELPHIX_COMPLIANCE_SERVICE_BATCH_ID"] for record in data_records]
        column_lists["DELPHIX_COMPLIANCE_SERVICE_BATCH_ID"] = batch_id_values
        
        # Build API request
        url = f"{self.config.dcs_api_url}/v1/masking/batchMaskByColumn"
        run_id = 'sf-'+ str(uuid.uuid4())
        
        headers = {
            'Authorization': self.get_azure_ad_token(),
            'Content-Type': 'application/json',
            'Run-Id': run_id,
            'Field-Algorithm-Assignment': json.dumps(column_rules)
        }
        
        response = self._make_http_request(
            'POST',
            url,
            data=json.dumps(column_lists),
            headers=headers
        )
        
        # Handle response based on context
        if self.is_native_app:
            # Snowflake HTTP response format (Row object from SQL result)
            status_code = response[0] if response and len(response) > 0 else 500  # STATUS_CODE column
            response_text = response[2] if response and len(response) > 2 else ''  # BODY column
            
            if status_code != 200:
                raise Exception(f"DCS Masking API error: {status_code} - {response_text}")
            
            try:
                masked_column_lists = json.loads(response_text)
            except json.JSONDecodeError:
                raise Exception(f"Invalid JSON response from DCS Masking API: {response_text}")
        else:
            if response.status_code != 200:
                raise Exception(f"DCS Masking API error: {response.status_code} - {response.text}")
            masked_column_lists = response.json()
        
        # Parse API response (use the already parsed response)
        api_response = masked_column_lists
        
        if isinstance(api_response, dict) and 'items' in api_response:
            masked_records = api_response['items']
        else:
            raise Exception(f"Unexpected API response format: {type(api_response)}")
        
        # Add batch IDs back to masked records
        for i, record in enumerate(masked_records):
            record["DELPHIX_COMPLIANCE_SERVICE_BATCH_ID"] = i + 1
        
        # Merge with unmasked columns
        columns_unmasked = []
        for record in data_records:
            for column_name in record.keys():
                if column_name not in columns_to_mask and column_name not in columns_unmasked:
                    columns_unmasked.append(column_name)
        
        # Get original column order from first record
        original_columns = list(data_records[0].keys()) if data_records else []
        
        # Create final records by merging masked and unmasked data
        final_records = []
        for i, masked_record in enumerate(masked_records):
            final_record = {}
            original_record = data_records[i]
            
            # Preserve original column order
            for column_name in original_columns:
                if column_name in columns_to_mask and column_name in masked_record:
                    # Use masked value
                    final_record[column_name] = masked_record[column_name]
                elif column_name in original_record:
                    # Use original value
                    final_record[column_name] = original_record[column_name]
            
            final_records.append(final_record)
        
        # Remove batch IDs
        for record in final_records:
            if "DELPHIX_COMPLIANCE_SERVICE_BATCH_ID" in record:
                del record["DELPHIX_COMPLIANCE_SERVICE_BATCH_ID"]
        
        return {"masked_records": final_records, "run_id": run_id}