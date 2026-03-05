import logging
import time

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from .base import BaseConnector

logger = logging.getLogger(__name__)


class AthenaConnector(BaseConnector):
    """
    Connector for Amazon Athena.
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.client = None
        self.s3_staging_dir = self.config.get("s3_staging_dir")
        if not self.s3_staging_dir:
            raise ValueError("s3_staging_dir is required in Athena configuration.")

    def connect(self):
        """
        Establishes a connection (client) to Athena.
        """
        try:
            # For Athena, "connecting" means creating a boto3 client.
            # Credentials can be sourced from environment variables, IAM roles, or explicitly in config.
            # It's best practice to rely on IAM roles or environment variables for credentials.
            self.client = boto3.client(
                "athena",
                region_name=self.config.get("region_name"),
                aws_access_key_id=self.config.get("aws_access_key_id"),  # Optional, prefers IAM role/env
                aws_secret_access_key=self.config.get("aws_secret_access_key"),  # Optional, prefers IAM role/env
            )
            logger.info(f"Successfully created Athena client for region: {self.config.get('region_name')}")
        except ClientError as e:
            logger.error(f"Error creating Athena client: {e}")
            raise ConnectionError(f"Could not create Athena client: {e}")
        except Exception as e:  # Catch other potential errors like missing region_name
            logger.error(f"An unexpected error occurred while creating Athena client: {e}")
            raise ConnectionError(f"Unexpected error creating Athena client: {e}")

    def disconnect(self):
        """
        For Athena, there's no persistent connection to close.
        The client doesn't need explicit closing.
        """
        self.client = None  # Allow garbage collection
        logger.info("Athena client session ended (no explicit disconnect needed).")

    def _start_query_execution(self, query: str) -> str:
        """Helper to start query execution."""
        try:
            response = self.client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    "Database": self.config.get("database"),
                    "Catalog": self.config.get("catalog", "AwsDataCatalog"),
                },
                ResultConfiguration={
                    "OutputLocation": self.s3_staging_dir,
                },
                WorkGroup=self.config.get("workgroup", "primary"),
            )
            return response["QueryExecutionId"]
        except ClientError as e:
            logger.error(f"Error starting Athena query execution: {e}\nQuery: {query}")
            raise RuntimeError(f"Could not start Athena query: {e}")

    def _wait_for_query_completion(self, query_execution_id: str) -> dict:
        """Helper to poll for query completion."""
        while True:
            try:
                response = self.client.get_query_execution(QueryExecutionId=query_execution_id)
                state = response["QueryExecution"]["Status"]["State"]

                if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                    if state == "FAILED":
                        error_message = response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
                        logger.error(f"Athena query {query_execution_id} failed: {error_message}")
                        raise RuntimeError(f"Athena query {query_execution_id} failed: {error_message}")
                    if state == "CANCELLED":
                        logger.warning(f"Athena query {query_execution_id} was cancelled.")
                        raise RuntimeError(f"Athena query {query_execution_id} was cancelled.")
                    return response

                time.sleep(self.config.get("poll_interval_seconds", 1))  # Poll interval
            except ClientError as e:
                logger.error(f"Error checking Athena query status for {query_execution_id}: {e}")
                raise RuntimeError(f"Could not check Athena query status: {e}")

    def _get_query_results(self, query_execution_id: str) -> pd.DataFrame:
        """Helper to fetch query results and convert to DataFrame."""
        try:
            results_paginator = self.client.get_paginator("get_query_results")
            results_iter = results_paginator.paginate(
                QueryExecutionId=query_execution_id, PaginationConfig={"PageSize": 1000}
            )

            rows = []
            column_names = None
            for results_page in results_iter:
                if column_names is None:
                    column_info = results_page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
                    column_names = [info["Name"] for info in column_info]

                for row_data in results_page["ResultSet"]["Rows"]:
                    # Skip header row if present (Athena often includes it in the first page of results)
                    if (
                        not column_names
                        and len(row_data["Data"]) > 0
                        and all(d.get("VarCharValue") == c for d, c in zip(row_data["Data"], column_names))
                    ):
                        continue

                    # Check if this is the actual header row based on values matching column names
                    # This is a common pattern for the first row in Athena's CSV-like output
                    is_header_row = True
                    if len(row_data["Data"]) == len(column_names):
                        for i, cell in enumerate(row_data["Data"]):
                            if cell.get("VarCharValue") != column_names[i]:
                                is_header_row = False
                                break
                    else:  # Mismatch in length, cannot be header
                        is_header_row = False

                    if (
                        is_header_row and not rows
                    ):  # Only skip if it's the first row being processed and looks like a header
                        continue

                    rows.append([d.get("VarCharValue") for d in row_data["Data"]])

            if (
                not column_names and rows
            ):  # If column_names were not set due to empty metadata but rows exist (e.g. from header row)
                column_names = rows.pop(0)  # Assume first row is header

            df = pd.DataFrame(rows, columns=column_names)

            # Athena returns all columns as strings by default from S3 results.
            # We need to infer types. The _rename_date_column will handle the date column.
            # For other columns, pd.to_numeric and other conversions might be needed if type inference is poor.
            # For now, we rely on _rename_date_column for the date part.

            return df

        except ClientError as e:
            logger.error(f"Error fetching Athena query results for {query_execution_id}: {e}")
            raise RuntimeError(f"Could not fetch Athena query results: {e}")

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Executes a SQL query on Athena and returns the results as a pandas DataFrame.
        Relies on the query to alias the date column as "Date".

        Args:
            query (str): The SQL query to execute.

        Returns:
            pd.DataFrame: A DataFrame containing the query results.
        """
        if not self.client:
            self.connect()

        logger.debug(f"Executing query on Athena: {query}")
        query_execution_id = self._start_query_execution(query)
        logger.info(f"Athena query started with Execution ID: {query_execution_id}")

        self._wait_for_query_completion(query_execution_id)
        logger.info(f"Athena query {query_execution_id} succeeded.")

        df = self._get_query_results(query_execution_id)

        # Validate and parse the "Date" column
        df = self._validate_and_parse_date_column(df)

        logger.info(f"Successfully executed query on Athena. Fetched {len(df)} rows.")
        return df


# Example Usage (for testing purposes)
if __name__ == "__main__":
    # This is a placeholder for actual testing. Requires:
    #   - AWS credentials configured (e.g., via environment variables or IAM role)
    #   - An S3 bucket for Athena query results
    #   - An Athena database and table to query
    #
    # config_ath = {
    #     "aws_access_key_id": None, # Let boto3 find from env or IAM role
    #     "aws_secret_access_key": None, # Let boto3 find from env or IAM role
    #     "region_name": "your-aws-region", # e.g., us-east-1
    #     "s3_staging_dir": "s3://your-athena-query-results-bucket/path/",
    #     "database": "your_athena_database",
    #     "workgroup": "primary", # optional
    #     "poll_interval_seconds": 2
    # }
    #
    # # Make sure 'my_table' exists in 'your_athena_database' and has 'event_date' and 'some_value' columns
    # query_example_ath = "SELECT event_date, SUM(some_value) as total_value FROM my_table GROUP BY event_date ORDER BY event_date;"
    #
    # try:
    #     with AthenaConnector(config_ath) as ath_conn:
    #         df_results_ath = ath_conn.execute_query(query_example_ath, date_column="event_date")
    #         print("\nAthena Query Results:")
    #         print(df_results_ath)
    #         print(df_results_ath.info())
    # except Exception as e:
    #     print(f"Athena Error: {e}")
    pass
