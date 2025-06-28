import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime

from src.connectors.snowflake import SnowflakeConnector
import snowflake.connector # To mock its exceptions and objects

# Sample data to be returned by cursor.fetch_pandas_all()
# Note: Snowflake column names are often uppercase by default in results
MOCK_SNOWFLAKE_DATA_DICT = {
    'EVENT_TIMESTAMP': [datetime(2023, 1, 1), datetime(2023, 1, 2)],
    'VALUE': [100, 150],
    'CATEGORY': ['alpha', 'beta']
}
MOCK_SNOWFLAKE_DF = pd.DataFrame(MOCK_SNOWFLAKE_DATA_DICT)


class TestSnowflakeConnector(unittest.TestCase):

    def setUp(self):
        self.config = {
            "user": "sfuser",
            "password": "sfpassword",
            "account": "sfaccount",
            "warehouse": "sfwarehouse",
            "database": "sfdatabase",
            "schema": "sfschema",
            "role": "sfrole"
        }

    @patch('snowflake.connector.connect')
    def test_connect_success(self, mock_connect):
        mock_connection_obj = MagicMock()
        mock_connect.return_value = mock_connection_obj

        connector = SnowflakeConnector(self.config)
        connector.connect()

        mock_connect.assert_called_once_with(
            user="sfuser",
            password="sfpassword",
            account="sfaccount",
            warehouse="sfwarehouse",
            database="sfdatabase",
            schema="sfschema",
            role="sfrole"
        )
        self.assertEqual(connector.connection, mock_connection_obj)
        connector.disconnect()

    @patch('snowflake.connector.connect', side_effect=snowflake.connector.errors.DatabaseError("Connection failed"))
    def test_connect_failure(self, mock_connect):
        connector = SnowflakeConnector(self.config)
        with self.assertRaisesRegex(ConnectionError, "Could not connect to Snowflake: Connection failed"):
            connector.connect()
        mock_connect.assert_called_once()

    @patch('snowflake.connector.connect')
    def test_disconnect(self, mock_connect):
        mock_connection_obj = MagicMock()
        mock_connect.return_value = mock_connection_obj

        connector = SnowflakeConnector(self.config)
        connector.connect()
        self.assertIsNotNone(connector.connection)

        connector.disconnect()
        mock_connection_obj.close.assert_called_once()
        self.assertIsNone(connector.connection)

    @patch('snowflake.connector.connect')
    def test_execute_query_success(self, mock_connect):
        mock_cursor_obj = MagicMock()
        # fetch_pandas_all() is a method of the cursor in snowflake.connector
        mock_cursor_obj.fetch_pandas_all.return_value = MOCK_SNOWFLAKE_DF.copy() # Return a copy

        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connection_obj.is_closed.return_value = False # Mock that connection is not closed
        mock_connect.return_value = mock_connection_obj

        query = "SELECT EVENT_TIMESTAMP, VALUE, CATEGORY FROM events;"

        # The connector converts column names to lower case.
        # The date_column passed to execute_query should be the original (e.g., uppercase)
        # name from the query, and the connector handles lowercasing internally for lookup.
        with SnowflakeConnector(self.config) as connector:
            df = connector.execute_query(query, date_column="EVENT_TIMESTAMP")

        mock_connection_obj.cursor.assert_called_once()
        mock_cursor_obj.execute.assert_called_once_with(query)
        mock_cursor_obj.fetch_pandas_all.assert_called_once()

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), len(MOCK_SNOWFLAKE_DF))
        # After _rename_date_column, the target is "Date" (capitalized)
        self.assertIn("Date", df.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['Date']))
        # Original columns are lowercased, then 'event_timestamp' becomes 'Date'
        self.assertListEqual(sorted(df.columns.tolist()), sorted(["Date", "value", "category"]))
        self.assertEqual(df['value'].iloc[0], 100)


    @patch('snowflake.connector.connect')
    def test_execute_query_db_error(self, mock_connect):
        mock_cursor_obj = MagicMock()
        mock_cursor_obj.execute.side_effect = snowflake.connector.errors.ProgrammingError("Syntax error")

        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connection_obj.is_closed.return_value = False
        mock_connect.return_value = mock_connection_obj

        query = "SELECT * FROM events WHERE;" # Invalid query
        with SnowflakeConnector(self.config) as connector:
            with self.assertRaisesRegex(RuntimeError, "Could not execute query on Snowflake: Syntax error"):
                connector.execute_query(query, date_column="EVENT_TIMESTAMP")

        mock_cursor_obj.close.assert_called_once()


    @patch('snowflake.connector.connect')
    def test_execute_query_date_column_not_in_results(self, mock_connect):
        mock_cursor_obj = MagicMock()
        # Return data that doesn't have 'EVENT_TIMESTAMP'
        temp_df = pd.DataFrame({'OTHER_COL': [1,2], 'VALUE': [10,20]})
        mock_cursor_obj.fetch_pandas_all.return_value = temp_df

        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connection_obj.is_closed.return_value = False
        mock_connect.return_value = mock_connection_obj

        query = "SELECT OTHER_COL, VALUE FROM events;"
        with SnowflakeConnector(self.config) as connector:
            with self.assertLogs(level='WARNING') as log_watcher:
                 # date_column "EVENT_TIMESTAMP" is passed, but query result (temp_df) doesn't have it
                df = connector.execute_query(query, date_column="EVENT_TIMESTAMP")
            self.assertTrue(any("Specified date_column 'EVENT_TIMESTAMP' (as 'event_timestamp') not found" in message for message in log_watcher.output))

        self.assertIsInstance(df, pd.DataFrame)
        # Columns should be lowercased by the connector
        self.assertListEqual(sorted(df.columns.tolist()), sorted(['other_col', 'value']))
        self.assertNotIn("Date", df.columns)


if __name__ == '__main__':
    unittest.main()
