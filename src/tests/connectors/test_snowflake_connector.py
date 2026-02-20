import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import snowflake.connector  # To mock its exceptions and objects

from src.connectors.snowflake import SnowflakeConnector

# Sample data to be returned by cursor.fetch_pandas_all()
# Note: Snowflake column names are often uppercase by default in results
MOCK_SNOWFLAKE_DATA_DICT = {
    "EVENT_TIMESTAMP": [datetime(2023, 1, 1), datetime(2023, 1, 2)],
    "VALUE": [100, 150],
    "CATEGORY": ["alpha", "beta"],
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
            "role": "sfrole",
        }

    @patch("snowflake.connector.connect")
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
            role="sfrole",
        )
        self.assertEqual(connector.connection, mock_connection_obj)
        connector.disconnect()

    @patch("snowflake.connector.connect", side_effect=snowflake.connector.errors.DatabaseError("Connection failed"))
    def test_connect_failure(self, mock_connect):
        connector = SnowflakeConnector(self.config)
        with self.assertRaisesRegex(ConnectionError, "Could not connect to Snowflake: Connection failed"):
            connector.connect()
        mock_connect.assert_called_once()

    @patch("snowflake.connector.connect")
    def test_disconnect(self, mock_connect):
        mock_connection_obj = MagicMock()
        mock_connect.return_value = mock_connection_obj

        connector = SnowflakeConnector(self.config)
        connector.connect()
        self.assertIsNotNone(connector.connection)

        connector.disconnect()
        mock_connection_obj.close.assert_called_once()
        self.assertIsNone(connector.connection)

    @patch("snowflake.connector.connect")
    def test_execute_query_success(self, mock_connect):
        # Test case where query aliases as "DATE" (uppercase)
        mock_df_upper = pd.DataFrame({"DATE": [datetime(2023, 1, 1)], "VALUE": [100]})
        # Test case where query aliases as "Date" (exact case)
        mock_df_exact = pd.DataFrame({"Date": [datetime(2023, 1, 2)], "VALUE": [200]})

        mock_cursor_obj = MagicMock()
        # Return the uppercase version first
        mock_cursor_obj.fetch_pandas_all.return_value = mock_df_upper

        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connection_obj.is_closed.return_value = False
        mock_connect.return_value = mock_connection_obj

        # Test 1: Snowflake returns uppercase 'DATE'
        query_upper = 'SELECT event_timestamp as "DATE", value as "VALUE" FROM events;'
        with SnowflakeConnector(self.config) as connector:
            df_upper = connector.execute_query(query_upper)

        self.assertIn("Date", df_upper.columns)
        self.assertNotIn("DATE", df_upper.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df_upper["Date"]))

        # Test 2: Snowflake returns exact case 'Date'
        mock_cursor_obj.fetch_pandas_all.return_value = mock_df_exact
        query_exact = 'SELECT event_timestamp as "Date", value as "VALUE" FROM events;'
        with SnowflakeConnector(self.config) as connector:
            df_exact = connector.execute_query(query_exact)

        self.assertIn("Date", df_exact.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df_exact["Date"]))

    @patch("snowflake.connector.connect")
    def test_execute_query_db_error(self, mock_connect):
        mock_cursor_obj = MagicMock()
        mock_cursor_obj.execute.side_effect = snowflake.connector.errors.ProgrammingError("Syntax error")

        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connection_obj.is_closed.return_value = False
        mock_connect.return_value = mock_connection_obj

        query = "SELECT * FROM events WHERE;"
        with SnowflakeConnector(self.config) as connector:
            with self.assertRaisesRegex(RuntimeError, "Could not execute query on Snowflake: Syntax error"):
                connector.execute_query(query)

        mock_cursor_obj.close.assert_called_once()

    @patch("snowflake.connector.connect")
    def test_execute_query_missing_date_column(self, mock_connect):
        mock_cursor_obj = MagicMock()
        temp_df = pd.DataFrame({"OTHER_COL": [1, 2], "VALUE": [10, 20]})
        mock_cursor_obj.fetch_pandas_all.return_value = temp_df

        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connection_obj.is_closed.return_value = False
        mock_connect.return_value = mock_connection_obj

        query = "SELECT OTHER_COL, VALUE FROM events;"
        with SnowflakeConnector(self.config) as connector:
            with self.assertRaisesRegex(ValueError, "Query results must include a 'Date' column"):
                connector.execute_query(query)


if __name__ == "__main__":
    unittest.main()
