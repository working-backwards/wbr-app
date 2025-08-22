import json
import logging
import os

import boto3
from botocore.exceptions import ClientError


class SecretLoader:
    """
    Abstract base class for loading secrets from different cloud providers.
    
    This class defines the interface that all secret loader implementations
    must follow. Subclasses should implement the load_secret() method to
    retrieve secrets from their respective cloud platforms.
    """

    def __init__(self, secret_config):
        """
        Initialize the SecretLoader with configuration.
        
        Args:
            secret_config (dict): Configuration dictionary containing
                provider-specific settings for secret retrieval.
        """
        self.secret_config = secret_config
    
    def load_secret(self):
        """
        Load and return the secret value.
        
        This method must be implemented by subclasses to retrieve
        secrets from their respective cloud platforms.
        
        Returns:
            dict or str: The loaded secret value, typically as a JSON object
                or string depending on how the secret was stored.
                
        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """
        raise NotImplementedError("Subclasses must implement load_secret()")


class AwsSecretLoader(SecretLoader):
    """
    AWS Secrets Manager implementation for loading secrets.
    
    This class handles authentication and secret retrieval from AWS Secrets Manager
    using boto3. It supports retrieving secrets stored as JSON strings and
    automatically parses them into Python dictionaries.
    """

    def __init__(self, secret_config):
        """
        Initialize the AWS Secret Loader with AWS Secrets Manager client.
        
        Args:
            secret_config (dict): Configuration dictionary containing:
                - secret_name (str): Name of the secret in AWS Secrets Manager
                
        Note:
            Uses default AWS credentials from environment variables, IAM roles,
            or AWS credentials file. Region is set to 'us-east-1'.
        """
        super().__init__(secret_config)
        # Default to us-east-1 region for AWS Secrets Manager
        self.region_name = "us-east-1"
        session = boto3.session.Session()
        self.client = session.client(
            service_name='secretsmanager',
            region_name=self.region_name
        )

    def load_secret(self):
        """
        Retrieve and parse the secret from AWS Secrets Manager.
        
        Fetches the secret value using the configured secret name and
        attempts to parse it as JSON. If parsing fails, returns the raw string.
        
        Returns:
            dict: The parsed secret value as a dictionary (if JSON)
            
        Raises:
            ClientError: If there's an error accessing the secret from AWS
            KeyError: If 'secret_name' is not provided in secret_config
        """
        secret_name = self.secret_config["secret_name"]

        try:
            # Retrieve the secret value from AWS Secrets Manager
            get_secret_value_response = self.client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            raise e

        # Extract the secret string from the response
        secret = get_secret_value_response['SecretString']
        # Parse the JSON string into a Python dictionary
        loaded_data = json.loads(secret)

        return loaded_data


class GcpSecretLoader(SecretLoader):
    """
    Google Cloud Platform Secret Manager implementation for loading secrets.
    
    This class handles authentication and secret retrieval from GCP Secret Manager.
    It supports both service account authentication and default credentials.
    Secrets can be specified by full resource path or by project_id + secret_name.
    """

    def __init__(self, secret_config):
        """
        Initialize the GCP Secret Loader with Secret Manager client.
        
        Args:
            secret_config (dict): Configuration dictionary containing:
                - secret_name (str): Name of the secret in GCP Secret Manager
                - project_id (str): GCP project ID (required if secret_name is not full path)
                - version (str, optional): Secret version, defaults to 'latest'
                
        Raises:
            ImportError: If google-cloud-secret-manager is not installed
        """
        super().__init__(secret_config)
        try:
            from google.cloud import secretmanager  # type: ignore
        except Exception as import_error:
            raise ImportError(
                "google-cloud-secret-manager is required for GCP secrets. Install with 'pip install google-cloud-secret-manager'"
            ) from import_error

        # Check for service account credentials file path
        gcp_service_account_json_file = os.getenv("GCP_SERVICE_ACCOUNT_PATH")  # JSON file path

        # Initialize client with service account if provided, otherwise use default credentials
        self._sm_client = get_gcp_client_for_credentials(gcp_service_account_json_file) if gcp_service_account_json_file else (
            secretmanager.SecretManagerServiceClient())

    def _build_resource_name(self):
        """
        Build the full resource name for the GCP secret.
        
        Constructs the complete resource path for accessing the secret.
        Supports both full resource paths and project_id + secret_name combinations.
        
        Returns:
            str: The complete resource name for the secret
            
        Raises:
            ValueError: If required configuration parameters are missing
        """
        print("Fetching the secret name")
        name = self.secret_config.get("secret_name")
        if not name:
            raise ValueError("secret_name must be provided in secret_config for GCP")

        # If user provided a full resource path, use it as-is
        if isinstance(name, str) and name.startswith("projects/") and "/secrets/" in name:
            return name

        # Otherwise, construct the full path using project_id and secret_name
        project_id = self.secret_config.get("project_id")
        if not project_id:
            raise ValueError("project_id must be provided in secret_config when secret_name is not a full resource path")

        version = self.secret_config.get("version", "latest")
        secret_id = name
        return f"projects/{project_id}/secrets/{secret_id}/versions/{version}"

    def load_secret(self):
        """
        Retrieve and parse the secret from GCP Secret Manager.
        
        Fetches the secret value using the constructed resource name and
        attempts to parse it as JSON. If parsing fails, returns the raw string.
        
        Returns:
            dict or str: The parsed secret value as a dictionary (if JSON) or raw string
            
        Raises:
            ValueError: If required configuration parameters are missing
        """
        name = self._build_resource_name()
        print(f"Fetched {name} as the secret name")
        # Perform API call to access the secret version
        response = self._sm_client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("utf-8")

        print("Fetched payload from gcp")
        try:
            # Attempt to parse as JSON, return raw string if parsing fails
            return json.loads(payload)
        except Exception:
            return payload


class AzureSecretLoader(SecretLoader):
    """
    Azure Key Vault implementation for loading secrets.
    
    This class handles authentication and secret retrieval from Azure Key Vault
    using the Azure SDK. It supports both versioned and non-versioned secrets.
    """

    def __init__(self, secret_config):
        """
        Initialize the Azure Secret Loader with Key Vault client.
        
        Args:
            secret_config (dict): Configuration dictionary containing:
                - vault_url (str): URL of the Azure Key Vault
                - secret_name (str): Name of the secret in the Key Vault
                - version (str, optional): Secret version (optional)
                
        Raises:
            ImportError: If azure-identity or azure-keyvault-secrets are not installed
            ValueError: If vault_url is not provided
        """
        super().__init__(secret_config)
        try:
            from azure.identity import DefaultAzureCredential  # type: ignore
            from azure.keyvault.secrets import SecretClient  # type: ignore
        except Exception as import_error:
            raise ImportError(
                "azure-identity and azure-keyvault-secrets are required for Azure secrets. Install with 'pip install azure-identity azure-keyvault-secrets'"
            ) from import_error

        vault_url = self.secret_config.get("vault_url")
        if not vault_url:
            raise ValueError("vault_url must be provided in secret_config for Azure")

        # Use default Azure credentials (managed identity, service principal, etc.)
        credential = DefaultAzureCredential()
        self._secret_client = SecretClient(vault_url=vault_url, credential=credential)

    def load_secret(self):
        """
        Retrieve and parse the secret from Azure Key Vault.
        
        Fetches the secret value using the configured secret name and optional version.
        Attempts to parse the value as JSON. If parsing fails, returns the raw string.
        
        Returns:
            dict or str: The parsed secret value as a dictionary (if JSON) or raw string
            
        Raises:
            ValueError: If secret_name is not provided in secret_config
        """
        secret_name = self.secret_config.get("secret_name")
        if not secret_name:
            raise ValueError("secret_name must be provided in secret_config for Azure")

        version = self.secret_config.get("version")  # optional
        if version:
            # Retrieve specific version of the secret
            secret = self._secret_client.get_secret(secret_name, version=version)
        else:
            # Retrieve the latest version of the secret
            secret = self._secret_client.get_secret(secret_name)

        value = secret.value
        try:
            # Attempt to parse as JSON, return raw string if parsing fails
            return json.loads(value)
        except Exception:
            return value


def get_gcp_client_for_credentials(credentials_json_file):
    """
    Initialize a GCP Secret Manager client using a service account JSON file.
    
    This function creates a GCP Secret Manager client authenticated with
    the provided service account credentials file. This is useful when
    default credentials are not available or when specific service account
    permissions are required.
    
    Args:
        credentials_json_file (str): Path to the JSON file containing the
            GCP service account credentials.
    
    Returns:
        google.cloud.secretmanager.SecretManagerServiceClient: The GCP Secret Manager
            client initialized with the provided service account credentials.
    
    Raises:
        ImportError: If google-cloud-secret-manager is not installed
        Exception: If the client initialization fails due to invalid credentials
            or file access issues.
    """
    try:
        from google.cloud import secretmanager  # type: ignore
    except Exception as import_error:
        raise ImportError(
            "google-cloud-secret-manager is required for GCP secrets. Install with 'pip install google-cloud-secret-manager'"
        ) from import_error

    try:
        with open(credentials_json_file, mode="r") as credentials_json:
            # Initialize GCP client with the IAM credentials file
            return secretmanager.SecretManagerServiceClient().from_service_account_json(credentials_json.name)

    except Exception as e:
        logging.error(f"Failed to upload to GCP: {str(e)}")
        raise

# Mapping of service names to their corresponding loader classes
_SERVICE_MAP = {
    "aws": AwsSecretLoader,
    # TODO: Add GCP secrets Manager and Azure Secret Vault processors after testing
    # "gcp": GcpSecretLoader,
    # "azure": AzureSecretLoader,
}


def get_loader(config) -> SecretLoader:
    """
    Factory function to create the appropriate secret loader based on configuration.
    
    This function acts as a factory that creates and returns the appropriate
    SecretLoader implementation based on the 'service' field in the configuration.
    Supported services are 'aws', 'gcp', and 'azure'.
    
    Args:
        config (dict): Configuration dictionary containing:
            - service (str): The cloud service provider ('aws', 'gcp', or 'azure')
            - Additional provider-specific configuration parameters
    
    Returns:
        SecretLoader: An instance of the appropriate SecretLoader subclass
            configured with the provided configuration.
    
    Raises:
        Exception: If an unknown or unsupported service is specified in the config.
    
    Example:
        >>> config = {"service": "aws", "secret_name": "my-secret"}
        >>> loader = get_loader(config)
        >>> secret = loader.load_secret()
    """
    if config["service"] not in _SERVICE_MAP:
        raise Exception(f"Unknown service provided {config["service"]}")
    
    return _SERVICE_MAP[config["service"]](config)
