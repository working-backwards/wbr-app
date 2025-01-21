import json
import logging
import os
from pathlib import Path

import boto3
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from google.cloud import storage


class PublishWbr:
    """
    Class to handle uploading and downloading data from various object storage services
    such as AWS S3, GCP, and Azure. If no storage option is provided, files are saved locally.

    Attributes:
        storage_option (str): The storage service to use ('s3', 'gcp', 'azure', or local).
        object_storage_bucket (str): The bucket or container name in the chosen storage service.
        s3_client (boto3.client): The client object for S3 interaction (used if storage_option is 's3').
        gcp_client (google.cloud.storage.Client): The client object for GCP interaction (used if storage_option is 'gcp').
        azure_client (azure.storage.blob.BlobServiceClient): The client object for Azure interaction (used if storage_option is 'azure').
    """

    def __init__(self, storage_option, object_storage_bucket):
        """
        Initializes the PublishWbr class based on the chosen storage option.

        Args:
           storage_option (str): The storage service to use ('s3', 'gcp', 'azure', or local).
           object_storage_bucket (str): The bucket or container name in the chosen storage service.

        Raises:
           Warning: Logs a warning if no storage option is provided.
        """
        self.s3_client = None
        self.gcp_client = None
        self.azure_client = None
        self.object_storage_bucket = object_storage_bucket
        self.storage_option = storage_option

        if storage_option == "s3":
            aws_access_key_id = os.environ.get("S3_STORAGE_KEY") or None
            s3config = {
                "region_name": os.environ.get("S3_REGION_NAME") or "",
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": os.environ.get("S3_STORAGE_SECRET") or ""
            }
            if os.environ.get("S3_STORAGE_ENDPOINT"):
                s3config["endpoint_url"] = os.environ.get("S3_STORAGE_ENDPOINT")
            self.s3_client = boto3.client('s3', **s3config) if aws_access_key_id else boto3.client('s3')

        elif storage_option == "gcp":
            gcp_service_account_json_file = os.getenv("GCP_SERVICE_ACCOUNT_PATH")  # JSON file path
            self.gcp_client = get_gcp_client_for_credentials(gcp_service_account_json_file) \
                if gcp_service_account_json_file else get_gcp_client_for_iam()

        elif storage_option == "azure":
            azure_connection_string = os.getenv("AZURE_CONNECTION_STRING")
            self.azure_client = BlobServiceClient.from_connection_string(azure_connection_string) \
                if azure_connection_string else get_azure_from_default_credentials()

        else:
            logging.warning("No OBJECT_STORAGE_OPTION is provided hence the published report will be saved locally")

    def upload(self, data, destination_file_path):
        """
        Uploads data to the selected object storage or saves it locally if no storage option is selected.

        Args:
            data (list|dict): The data to upload.
            destination_file_path (str): The destination file path in the object storage or local directory.

        Raises:
            Exception: Raises exceptions specific to the storage service (if any occur).
        """
        if self.storage_option == "s3":
            byte_data = bytes(json.dumps(data).encode('utf-8'))
            self.s3_client.put_object(Body=byte_data, Bucket=self.object_storage_bucket, Key=destination_file_path)

        elif self.storage_option == "gcp":
            byte_data = bytes(json.dumps(data).encode('utf-8'))
            bucket = self.gcp_client.bucket(self.object_storage_bucket)
            blob = bucket.blob(destination_file_path)
            blob.upload_from_string(byte_data, content_type='application/json')

        elif self.storage_option == "azure":
            byte_data = bytes(json.dumps(data).encode('utf-8'))
            blob_client = self.azure_client.get_blob_client(container=self.object_storage_bucket,
                                                            blob=destination_file_path)
            blob_client.upload_blob(byte_data)

        else:
            path = str(Path(os.path.dirname(__file__)).parent)
            file_path = path + '/publish/' + destination_file_path
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w') as json_file:
                json.dump(data, json_file, indent=4)

    def download(self, path):
        """
        Downloads data from the selected object storage or locally if no storage option is selected.

        Args:
            path (str): The file path in the object storage or local directory.

        Returns:
            dict: The data loaded from the storage or local file.

        Raises:
            Exception: Raises exceptions specific to the storage service (if any occur).
        """
        if self.storage_option == "s3":
            response = self.s3_client.get_object(Bucket=self.object_storage_bucket, Key=path)
            json_file_content = response['Body'].read()
            return json.loads(json_file_content)

        elif self.storage_option == "gcp":
            bucket = self.gcp_client.bucket(self.object_storage_bucket)
            blob = bucket.blob(path)
            return json.loads(blob.download_as_string(client=None))

        elif self.storage_option == "azure":
            blob_client = self.azure_client.get_blob_client(container=self.object_storage_bucket,
                                                            blob=path)
            stream = blob_client.download_blob()
            return json.loads(stream.readall())

        else:
            base_path = str(Path(os.path.dirname(__file__)).parent)
            file = base_path + '/publish/' + path
            current_file = open(file)
            return json.load(current_file)


def get_gcp_client_for_iam():
    """
    Initializes a GCP storage client using IAM credentials.

    Returns:
        google.cloud.storage.Client: The GCP storage client initialized with IAM credentials.
    """
    return storage.Client()


def get_gcp_client_for_credentials(credentials_json_file):
    """
    Initializes a GCP storage client using a service account JSON.

    Args:
        credentials_json_file (str): The JSON string containing the GCP service account credentials.

    Returns:
        google.cloud.storage.Client: The GCP storage client initialized with the provided service account JSON.

    Raises:
        Exception: If the client initialization fails, an exception is raised and logged.
    """
    try:
        with open(credentials_json_file, mode="r") as credentials_json:
            # Initialize GCP client with the IAM credentials file
            return storage.Client.from_service_account_json(credentials_json.name)

    except Exception as e:
        logging.error(f"Failed to upload to GCP: {str(e)}")


def get_azure_from_default_credentials():
    """
    Initializes an Azure BlobServiceClient using the DefaultAzureCredential for authentication.

    Returns:
        azure.storage.blob.BlobServiceClient: The Azure BlobServiceClient initialized with the default credentials.
    """
    default_credential = DefaultAzureCredential()
    account_url = os.getenv("AZURE_ACCOUNT_URL")
    return BlobServiceClient(account_url, credential=default_credential)
