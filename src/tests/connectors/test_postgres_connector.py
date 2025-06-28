import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime

from src.connectors.postgres import PostgresConnector
import psycopg2 # To mock its exceptions and objects

# Sample data to be returned by cursor.fetchall()
MOCK_DB_DATA = [
    (datetime(2023, 1, 1), 100, 'alpha'),
    (datetime(2023, 1, 2), 150, 'beta'),
]
MOCK_DB_COLUMNS = ['event_timestamp', 'value', 'category']

class TestPostgresConnector(unittest.TestCase):

    def setUp(self):
        self.config = {
            "host": "localhost",
            "port": 5432,
            "username": "testuser",
            "password": "testpassword",
            "database": "testdb"
        }

    @patch('psycopg2.connect')
    def test_connect_success(self, mock_connect):
        mock_connection_obj = MagicMock()
        mock_connect.return_value = mock_connection_obj

        connector = PostgresConnector(self.config)
        connector.connect()

        mock_connect.assert_called_once_with(
            host="localhost",
            port=5432,
            user="testuser",
            password="testpassword",
            dbname="testdb"
        )
        self.assertEqual(connector.connection, mock_connection_obj)
        connector.disconnect()

    @patch('psycopg2.connect', side_effect=psycopg2.OperationalError("Connection failed"))
    def test_connect_failure(self, mock_connect):
        connector = PostgresConnector(self.config)
        with self.assertRaisesRegex(ConnectionError, "Could not connect to PostgreSQL: Connection failed"):
            connector.connect()
        mock_connect.assert_called_once()

    @patch('psycopg2.connect')
    def test_disconnect(self, mock_connect):
        mock_connection_obj = MagicMock()
        mock_connect.return_value = mock_connection_obj

        connector = PostgresConnector(self.config)
        connector.connect()
        self.assertIsNotNone(connector.connection)

        connector.disconnect()
        mock_connection_obj.close.assert_called_once()
        self.assertIsNone(connector.connection)

    @patch('psycopg2.connect')
    def test_execute_query_success(self, mock_connect):
        mock_cursor_obj = MagicMock()
        mock_cursor_obj.fetchall.return_value = MOCK_DB_DATA
        mock_cursor_obj.description = [(col,) for col in MOCK_DB_COLUMNS] # Simplified description

        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connect.return_value = mock_connection_obj

        query = "SELECT event_timestamp, value, category FROM events;"

        with PostgresConnector(self.config) as connector:
            df = connector.execute_query(query, date_column="event_timestamp")

        mock_connection_obj.cursor.assert_called_once()
        mock_cursor_obj.execute.assert_called_once() # sql.SQL object makes direct query string comparison tricky here
        mock_cursor_obj.fetchall.assert_called_once()
        mock_connection_obj.commit.assert_called_once()

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), len(MOCK_DB_DATA))
        self.assertIn("Date", df.columns) # Renamed from event_timestamp
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['Date']))
        self.assertListEqual(df.columns.tolist(), ["Date", "value", "category"])
        self.assertEqual(df['value'].iloc[0], 100)


    @patch('psycopg2.connect')
    def test_execute_query_with_pre_connected_connector(self, mock_connect):
        mock_cursor_obj = MagicMock()
        mock_cursor_obj.fetchall.return_value = MOCK_DB_DATA
        mock_cursor_obj.description = [(col,) for col in MOCK_DB_COLUMNS]

        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connect.return_value = mock_connection_obj

        query = "SELECT event_timestamp, value, category FROM events;"

        connector = PostgresConnector(self.config)
        connector.connect() # Connect manually
        df = connector.execute_query(query, date_column="event_timestamp")
        connector.disconnect()

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), len(MOCK_DB_DATA))
        self.assertIn("Date", df.columns)


    @patch('psycopg2.connect')
    def test_execute_query_db_error(self, mock_connect):
        mock_cursor_obj = MagicMock()
        mock_cursor_obj.execute.side_effect = psycopg2.Error("Query syntax error")

        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connect.return_value = mock_connection_obj

        query = "SELECT * FROM events WHERE;" # Invalid query
        with PostgresConnector(self.config) as connector:
            with self.assertRaisesRegex(RuntimeError, "Could not execute query on PostgreSQL: Query syntax error"):
                connector.execute_query(query, date_column="event_timestamp")

        mock_connection_obj.rollback.assert_called_once()
        mock_cursor_obj.close.assert_called_once()


    @patch('psycopg2.connect')
    def test_execute_query_no_date_column_specified(self, mock_connect):
        # Data that doesn't have a column named "Date" by default
        custom_data = [(1, 'a'), (2, 'b')]
        custom_cols = ['id', 'name']

        mock_cursor_obj = MagicMock()
        mock_cursor_obj.fetchall.return_value = custom_data
        mock_cursor_obj.description = [(col,) for col in custom_cols]

        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connect.return_value = mock_connection_obj

        query = "SELECT id, name FROM some_table;"
        with PostgresConnector(self.config) as connector:
            # Call execute_query without a date_column, or with one that doesn't exist
            df = connector.execute_query(query, date_column="non_existent_date_col")

        self.assertIsInstance(df, pd.DataFrame)
        self.assertListEqual(df.columns.tolist(), custom_cols) # No 'Date' column created/renamed
        # Ensure _rename_date_column was not effectively called to create 'Date'
        self.assertNotIn("Date", df.columns)


    @patch('psycopg2.connect')
    def test_execute_query_date_column_not_in_results(self, mock_connect):
        mock_cursor_obj = MagicMock()
        mock_cursor_obj.fetchall.return_value = MOCK_DB_DATA # Returns 'event_timestamp'
        mock_cursor_obj.description = [(col,) for col in MOCK_DB_COLUMNS]

        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connect.return_value = mock_connection_obj

        query = "SELECT event_timestamp, value, category FROM events;"
        with PostgresConnector(self.config) as connector:
            # We expect a warning log, but the code should proceed
            # The _rename_date_column in base.py will raise error if date_column is mandatory and not found.
            # Here, the connector's execute_query itself logs a warning.
            # If _rename_date_column is called by execute_query, and it can't find 'wrong_date_col', it will raise ValueError
            # Let's test the warning part. The actual error is from _rename_date_column if it's called.
            # The current PostgresConnector calls _rename_date_column if date_column is specified.
             with self.assertLogs(level='WARNING') as log_watcher:
                df = connector.execute_query(query, date_column="wrong_date_col")
             self.assertTrue(any("Specified date_column 'wrong_date_col' not found" in message for message in log_watcher.output))
             self.assertNotIn("Date", df.columns) # No renaming should have happened

if __name__ == '__main__':
    unittest.main()
