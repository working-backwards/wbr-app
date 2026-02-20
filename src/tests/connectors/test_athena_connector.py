import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
from botocore.exceptions import ClientError

from src.connectors.athena import AthenaConnector

# Sample data for Athena query results
MOCK_ATHENA_COLUMN_INFO = [
    {"Name": "event_date", "Type": "date"},  # boto3 typically returns string for type
    {"Name": "metric_value", "Type": "integer"},
]
MOCK_ATHENA_ROWS_PAGE1 = [
    {"Data": [{"VarCharValue": "event_date"}, {"VarCharValue": "metric_value"}]},  # Header row
    {"Data": [{"VarCharValue": "2023-01-01"}, {"VarCharValue": "100"}]},
    {"Data": [{"VarCharValue": "2023-01-02"}, {"VarCharValue": "200"}]},
]
MOCK_ATHENA_ROWS_PAGE2 = [  # Simulating pagination
    {"Data": [{"VarCharValue": "2023-01-03"}, {"VarCharValue": "300"}]},
]


class TestAthenaConnector(unittest.TestCase):
    def setUp(self):
        self.config = {
            "region_name": "us-east-1",
            "s3_staging_dir": "s3://test-bucket/athena_results/",
            "database": "test_db",
            "workgroup": "primary",
            "poll_interval_seconds": 0.01,  # Fast polling for tests
        }
        # Minimal config for connection only
        self.min_config = {"s3_staging_dir": "s3://test-bucket/"}

    @patch("boto3.client")
    def test_connect_success(self, mock_boto_client):
        mock_athena_client_obj = MagicMock()
        mock_boto_client.return_value = mock_athena_client_obj

        connector = AthenaConnector(self.config)
        connector.connect()

        mock_boto_client.assert_called_once_with(
            "athena",
            region_name=self.config["region_name"],
            aws_access_key_id=None,  # Assuming not in config, so uses env/IAM
            aws_secret_access_key=None,
        )
        self.assertEqual(connector.client, mock_athena_client_obj)
        connector.disconnect()  # Should just nullify client

    @patch(
        "boto3.client",
        side_effect=ClientError({"Error": {"Code": "SomeError", "Message": "Boto Client Error"}}, "operation_name"),
    )
    def test_connect_failure(self, mock_boto_client):
        connector = AthenaConnector(self.config)
        with self.assertRaisesRegex(ConnectionError, "Could not create Athena client"):
            connector.connect()
        mock_boto_client.assert_called_once()

    def test_s3_staging_dir_validation(self):
        with self.assertRaisesRegex(ValueError, "s3_staging_dir is required"):
            AthenaConnector({"region_name": "us-east-1"})

    @patch("boto3.client")
    @patch("time.sleep", return_value=None)
    def test_execute_query_success(self, mock_sleep, mock_boto_client):
        # Mock data should now have the "Date" column
        mock_column_info = [{"Name": "Date", "Type": "date"}, {"Name": "metric_value", "Type": "integer"}]
        mock_rows_page1 = [
            {"Data": [{"VarCharValue": "Date"}, {"VarCharValue": "metric_value"}]},  # Header
            {"Data": [{"VarCharValue": "2023-01-01"}, {"VarCharValue": "100"}]},
        ]

        mock_athena_client = MagicMock()
        mock_boto_client.return_value = mock_athena_client
        mock_athena_client.start_query_execution.return_value = {"QueryExecutionId": "test-exec-id"}
        mock_athena_client.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": mock_column_info}, "Rows": mock_rows_page1}},
        ]
        mock_athena_client.get_paginator.return_value = mock_paginator

        query = 'SELECT event_date as "Date", metric_value FROM test_table;'

        with AthenaConnector(self.config) as connector:
            df = connector.execute_query(query)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertIn("Date", df.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df["Date"]))
        self.assertListEqual(df.columns.tolist(), ["Date", "metric_value"])
        self.assertEqual(df["metric_value"].iloc[0], "100")

    @patch("boto3.client")
    @patch("time.sleep", return_value=None)
    def test_execute_query_failed_state(self, mock_sleep, mock_boto_client):
        mock_athena_client = MagicMock()
        mock_boto_client.return_value = mock_athena_client

        mock_athena_client.start_query_execution.return_value = {"QueryExecutionId": "fail-exec-id"}
        mock_athena_client.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "FAILED", "StateChangeReason": "Syntax error"}}
        }

        query = "SELECT fail;"
        with AthenaConnector(self.config) as connector:
            with self.assertRaisesRegex(RuntimeError, "Athena query fail-exec-id failed: Syntax error"):
                connector.execute_query(query)

        self.assertEqual(mock_athena_client.get_query_execution.call_count, 1)

    @patch("boto3.client")
    def test_execute_query_start_query_fails(self, mock_boto_client):
        mock_athena_client = MagicMock()
        mock_boto_client.return_value = mock_athena_client
        mock_athena_client.start_query_execution.side_effect = ClientError(
            {"Error": {"Code": "InvalidRequest", "Message": "Query is bad"}}, "StartQueryExecution"
        )

        query = "SELECT bad;"
        with AthenaConnector(self.config) as connector:
            with self.assertRaisesRegex(RuntimeError, "Could not start Athena query"):
                connector.execute_query(query)

    @patch("boto3.client")
    @patch("time.sleep", return_value=None)
    def test_execute_query_get_results_fails(self, mock_sleep, mock_boto_client):
        mock_athena_client = MagicMock()
        mock_boto_client.return_value = mock_athena_client

        mock_athena_client.start_query_execution.return_value = {"QueryExecutionId": "results-fail-id"}
        mock_athena_client.get_query_execution.return_value = {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

        mock_paginator = MagicMock()
        mock_paginator.paginate.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Cannot read S3"}}, "GetQueryResults"
        )
        mock_athena_client.get_paginator.return_value = mock_paginator

        query = "SELECT ok;"
        with AthenaConnector(self.config) as connector:
            with self.assertRaisesRegex(RuntimeError, "Could not fetch Athena query results"):
                connector.execute_query(query)


if __name__ == "__main__":
    unittest.main()
