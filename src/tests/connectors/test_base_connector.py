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

    def execute_query(self, query: str, date_column: str = "Date") -> pd.DataFrame:
        if not self.mock_connection:
            raise ConnectionError("Not connected")

        # Simulate fetching data
        if query == "SELECT my_date_col, value FROM test_table":
            data = {'my_date_col': [datetime(2023, 1, 1), datetime(2023, 1, 2)], 'value': [10, 20]}
            df = pd.DataFrame(data)
        elif query == "SELECT date, value FROM another_table": # 'date' already correctly named
            data = {'date': [datetime(2023, 2, 1), datetime(2023, 2, 2)], 'value': [100, 200]}
            df = pd.DataFrame(data)
        elif query == "SELECT nodate_col, value FROM nodate_table":
            data = {'nodate_col': ["a", "b"], 'value': [1,2]}
            df = pd.DataFrame(data)
        elif query == "SELECT wrong_date_format, value FROM bad_date_table":
            data = {'wrong_date_format': ["2023/01/01", "2023/01/02"], 'value': [1,2]} # strings not datetimes
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame()

        # Call the utility renaming function, passing the date_column from the query config
        if date_column and date_column in df.columns:
            df = self._rename_date_column(df, date_column_name=date_column)
        elif date_column and date_column not in df.columns and not df.empty:
             raise ValueError(f"Specified date column '{date_column}' not found in query results for testing.")

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

    def test_rename_date_column_success(self):
        connector = ConcreteConnector({})
        data = {'event_date': ['2023-01-01', '2023-01-02'], 'metric': [1, 2]}
        df = pd.DataFrame(data)
        # Convert to datetime first, as this is what _rename_date_column expects after initial load
        df['event_date'] = pd.to_datetime(df['event_date'])

        renamed_df = connector._rename_date_column(df, date_column_name='event_date', desired_date_column='Date')
        self.assertIn('Date', renamed_df.columns)
        self.assertNotIn('event_date', renamed_df.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(renamed_df['Date']))

    def test_rename_date_column_already_named_date(self):
        connector = ConcreteConnector({})
        data = {'Date': ['2023-01-01', '2023-01-02'], 'metric': [1, 2]}
        df = pd.DataFrame(data)
        df['Date'] = pd.to_datetime(df['Date'])

        renamed_df = connector._rename_date_column(df, date_column_name='Date', desired_date_column='Date')
        self.assertIn('Date', renamed_df.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(renamed_df['Date']))

    def test_rename_date_column_missing_original_column(self):
        connector = ConcreteConnector({})
        data = {'other_column': ['2023-01-01', '2023-01-02'], 'metric': [1, 2]}
        df = pd.DataFrame(data)
        with self.assertRaisesRegex(ValueError, "Specified date column 'event_date' not found"):
            connector._rename_date_column(df, date_column_name='event_date')

    def test_rename_date_column_bad_datetime_conversion(self):
        connector = ConcreteConnector({})
        # Simulate data that can't be converted AFTER renaming
        # The _rename_date_column itself has a try-except for pd.to_datetime
        data = {'event_date': ['not-a-date', '2023-01-02']}
        df = pd.DataFrame(data)
        # df['event_date'] = pd.to_datetime(df['event_date']) # This would fail here, which is not the point of this specific test for _rename_date_column internal conversion

        with self.assertRaisesRegex(ValueError, "Could not convert column 'Date' to datetime"):
             connector._rename_date_column(df, date_column_name='event_date', desired_date_column='Date')


    def test_execute_query_with_date_rename(self):
        connector = ConcreteConnector({})
        with connector: # Ensure connected
            df = connector.execute_query("SELECT my_date_col, value FROM test_table", date_column="my_date_col")
        self.assertIn("Date", df.columns)
        self.assertNotIn("my_date_col", df.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['Date']))
        self.assertEqual(len(df), 2)

    def test_execute_query_date_column_already_correct(self):
        connector = ConcreteConnector({})
        with connector:
            df = connector.execute_query("SELECT date, value FROM another_table", date_column="date") # or date_column="Date" if it's already 'Date'
        self.assertIn("Date", df.columns) # Should still be 'Date'
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['Date']))
        self.assertEqual(len(df), 2)

    def test_execute_query_no_date_column_in_data(self):
        connector = ConcreteConnector({})
        with connector: # Ensure connected
            # Query for data that doesn't have 'my_date_col'
            with self.assertRaisesRegex(ValueError, "Specified date column 'my_date_col' not found"):
                 connector.execute_query("SELECT nodate_col, value FROM nodate_table", date_column="my_date_col")


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
