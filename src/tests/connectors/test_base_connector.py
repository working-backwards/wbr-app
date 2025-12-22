import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime

# Assuming src is in PYTHONPATH or tests are run from root
from src.connectors.base import BaseConnector
from src.connectors import get_connector
from src.connectors.postgres import PostgresConnector # Import one for factory testing
from src.connectors.snowflake import SnowflakeConnector # Import one for factory testing
from src.connectors.athena import AthenaConnector
from src.connectors.redshift import RedshiftConnector


class ConcreteConnector(BaseConnector):
    """A concrete implementation of BaseConnector for testing purposes."""
    def __init__(self, config: dict):
        super().__init__(config)
        self.mock_connection = None

    def connect(self):
        self.mock_connection = MagicMock()
        # print(f"ConcreteConnector {id(self)} connected with mock {id(self.mock_connection)}")

    def disconnect(self):
        if self.mock_connection:
            self.mock_connection.close()
            # print(f"ConcreteConnector {id(self)} disconnected mock {id(self.mock_connection)}")
        self.mock_connection = None

    def execute_query(self, query: str) -> pd.DataFrame:
        if not self.mock_connection:
            raise ConnectionError("Not connected")

        # Simulate fetching data
        if query == "SELECT event_date as \"Date\", value FROM test_table":
            data = {'Date': [datetime(2023, 1, 1), datetime(2023, 1, 2)], 'value': [10, 20]}
            df = pd.DataFrame(data)
        elif query == "SELECT nodate_col, value FROM nodate_table":
            data = {'nodate_col': ["a", "b"], 'value': [1, 2]}
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame()

        # Call the utility validation function
        df = self._validate_and_parse_date_column(df)
        return df


class TestBaseConnector(unittest.TestCase):

    def test_init(self):
        config = {"host": "localhost"}
        connector = ConcreteConnector(config)
        self.assertEqual(connector.config, config)

    def test_context_manager(self):
        config = {"host": "localhost"}
        with ConcreteConnector(config) as connector:
            self.assertIsNotNone(connector.mock_connection, "Connector should be connected within context")
        self.assertIsNone(connector.mock_connection, "Connector should be disconnected after context")

    def test_validate_date_column_success(self):
        connector = ConcreteConnector({})
        data = {'Date': ['2023-01-01', '2023-01-02'], 'metric': [1, 2]}
        df = pd.DataFrame(data)

        validated_df = connector._validate_and_parse_date_column(df)
        self.assertIn('Date', validated_df.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(validated_df['Date']))

    def test_validate_date_column_missing(self):
        connector = ConcreteConnector({})
        data = {'other_column': ['2023-01-01'], 'metric': [1]}
        df = pd.DataFrame(data)
        with self.assertRaisesRegex(ValueError, "Query results must include a 'Date' column"):
            connector._validate_and_parse_date_column(df)

    def test_validate_date_column_bad_conversion(self):
        connector = ConcreteConnector({})
        data = {'Date': ['not-a-date', '2023-01-02']}
        df = pd.DataFrame(data)
        with self.assertRaisesRegex(ValueError, "Could not convert 'Date' column to datetime"):
            connector._validate_and_parse_date_column(df)

    def test_execute_query_with_date_validation(self):
        connector = ConcreteConnector({})
        with connector:
            df = connector.execute_query("SELECT event_date as \"Date\", value FROM test_table")
        self.assertIn("Date", df.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['Date']))
        self.assertEqual(len(df), 2)

    def test_execute_query_raises_if_date_is_missing(self):
        connector = ConcreteConnector({})
        with connector:
            with self.assertRaises(ValueError):
                connector.execute_query("SELECT nodate_col, value FROM nodate_table")


class TestConnectorFactory(unittest.TestCase):

    def test_get_postgres_connector(self):
        config = {"host": "db.postgres.com"}
        connector = get_connector("postgres", config)
        self.assertIsInstance(connector, PostgresConnector)
        self.assertEqual(connector.config, config)

    def test_get_snowflake_connector(self):
        config = {"account": "myaccount"}
        connector = get_connector("snowflake", config)
        self.assertIsInstance(connector, SnowflakeConnector)
        self.assertEqual(connector.config, config)

    def test_get_athena_connector(self):
        config = {"s3_staging_dir": "s3://bucket/"} # Required config
        connector = get_connector("athena", config)
        self.assertIsInstance(connector, AthenaConnector)
        self.assertEqual(connector.config, config)

    def test_get_redshift_connector(self):
        config = {"host": "db.redshift.com"}
        connector = get_connector("redshift", config)
        self.assertIsInstance(connector, RedshiftConnector)
        self.assertEqual(connector.config, config)

    def test_get_unsupported_connector(self):
        with self.assertRaisesRegex(ValueError, "Unsupported database connection type: mysql"):
            get_connector("mysql", {})

    def test_get_connector_case_insensitive(self):
        config = {"host": "db.postgres.com"}
        # Using "Postgres" which lowercases to "postgres", matching the key in _CONNECTOR_MAP
        connector = get_connector("Postgres", config)
        self.assertIsInstance(connector, PostgresConnector)


if __name__ == '__main__':
    unittest.main()
