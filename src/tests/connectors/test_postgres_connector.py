import unittest
from datetime import datetime
from unittest.mock import patch, MagicMock

import pandas as pd
import psycopg2  # To mock its exceptions and objects

from src.connectors.postgres import PostgresConnector

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
        # The mock data now needs to have the "Date" column directly
        mock_data = [
            (datetime(2023, 1, 1), 100, 'alpha'),
            (datetime(2023, 1, 2), 150, 'beta'),
        ]
        mock_columns = ['Date', 'value', 'category']

        mock_cursor_obj = MagicMock()
        mock_cursor_obj.fetchall.return_value = mock_data
        mock_cursor_obj.description = [(col,) for col in mock_columns]

        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connect.return_value = mock_connection_obj

        query = "SELECT event_timestamp as \"Date\", value, category FROM events;"

        with PostgresConnector(self.config) as connector:
            df = connector.execute_query(query)

        mock_connection_obj.cursor.assert_called_once()
        mock_cursor_obj.execute.assert_called_once()
        mock_cursor_obj.fetchall.assert_called_once()
        mock_connection_obj.commit.assert_called_once()

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), len(mock_data))
        self.assertIn("Date", df.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['Date']))
        self.assertListEqual(df.columns.tolist(), mock_columns)
        self.assertEqual(df['value'].iloc[0], 100)

    @patch('psycopg2.connect')
    def test_execute_query_db_error(self, mock_connect):
        mock_cursor_obj = MagicMock()
        mock_cursor_obj.execute.side_effect = psycopg2.Error("Query syntax error")

        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connect.return_value = mock_connection_obj

        query = "SELECT * FROM events WHERE;"
        with PostgresConnector(self.config) as connector:
            with self.assertRaisesRegex(RuntimeError, "Could not execute query on PostgreSQL: Query syntax error"):
                connector.execute_query(query)

        mock_connection_obj.rollback.assert_called_once()
        mock_cursor_obj.close.assert_called_once()

    @patch('psycopg2.connect')
    def test_execute_query_missing_date_column(self, mock_connect):
        # Data that doesn't have a column named "Date"
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
            with self.assertRaisesRegex(ValueError, "Query results must include a 'Date' column"):
                connector.execute_query(query)


if __name__ == '__main__':
    unittest.main()
