import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime

from src.connectors.redshift import RedshiftConnector # Changed import
import psycopg2 # To mock its exceptions and objects (as Redshift connector uses it)

# Sample data to be returned by cursor.fetchall()
# Redshift column names are typically lowercase unless quoted.
MOCK_DB_DATA_RS = [
    (datetime(2023, 1, 1), 100, 'alpha'),
    (datetime(2023, 1, 2), 150, 'beta'),
]
MOCK_DB_COLUMNS_RS = ['event_day', 'value', 'type'] # Lowercase column names

class TestRedshiftConnector(unittest.TestCase):

    def setUp(self):
        self.config = {
            "host": "test.redshift.amazonaws.com",
            "port": 5439,
            "username": "rsuser",
            "password": "rspassword",
            "database": "rsdb"
        }

    @patch('psycopg2.connect') # Still patching psycopg2.connect
    def test_connect_success(self, mock_connect):
        mock_connection_obj = MagicMock()
        mock_connect.return_value = mock_connection_obj

        connector = RedshiftConnector(self.config)
        connector.connect()

        mock_connect.assert_called_once_with(
            host="test.redshift.amazonaws.com",
            port=5439,
            user="rsuser",
            password="rspassword",
            dbname="rsdb"
        )
        self.assertEqual(connector.connection, mock_connection_obj)
        connector.disconnect()

    @patch('psycopg2.connect', side_effect=psycopg2.OperationalError("RS Connection failed"))
    def test_connect_failure(self, mock_connect):
        connector = RedshiftConnector(self.config)
        with self.assertRaisesRegex(ConnectionError, "Could not connect to Redshift: RS Connection failed"):
            connector.connect()
        mock_connect.assert_called_once()

    @patch('psycopg2.connect')
    def test_disconnect(self, mock_connect):
        mock_connection_obj = MagicMock()
        mock_connect.return_value = mock_connection_obj

        connector = RedshiftConnector(self.config)
        connector.connect()
        self.assertIsNotNone(connector.connection)

        connector.disconnect()
        mock_connection_obj.close.assert_called_once()
        self.assertIsNone(connector.connection)

    @patch('psycopg2.connect')
    def test_execute_query_success(self, mock_connect):
        mock_cursor_obj = MagicMock()
        mock_cursor_obj.fetchall.return_value = MOCK_DB_DATA_RS
        mock_cursor_obj.description = [(col,) for col in MOCK_DB_COLUMNS_RS]

        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connect.return_value = mock_connection_obj

        query = "SELECT event_day, value, type FROM rs_events;"

        with RedshiftConnector(self.config) as connector:
            # date_column is 'event_day' (lowercase) as per MOCK_DB_COLUMNS_RS
            df = connector.execute_query(query, date_column="event_day")

        mock_connection_obj.cursor.assert_called_once()
        # sql.SQL object makes direct query string comparison tricky here
        mock_cursor_obj.execute.assert_called_once()
        mock_cursor_obj.fetchall.assert_called_once()
        mock_connection_obj.commit.assert_called_once()

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), len(MOCK_DB_DATA_RS))
        self.assertIn("Date", df.columns) # Renamed from event_day
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['Date']))
        # Original columns lowercased by connector, 'event_day' becomes 'Date'
        self.assertListEqual(sorted(df.columns.tolist()), sorted(["Date", "value", "type"]))
        self.assertEqual(df['value'].iloc[0], 100)


    @patch('psycopg2.connect')
    def test_execute_query_db_error(self, mock_connect):
        mock_cursor_obj = MagicMock()
        mock_cursor_obj.execute.side_effect = psycopg2.Error("RS Query syntax error")

        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connect.return_value = mock_connection_obj

        query = "SELECT * FROM rs_events WHERE;" # Invalid query
        with RedshiftConnector(self.config) as connector:
            with self.assertRaisesRegex(RuntimeError, "Could not execute query on Redshift: RS Query syntax error"):
                connector.execute_query(query, date_column="event_day")

        mock_connection_obj.rollback.assert_called_once()
        mock_cursor_obj.close.assert_called_once()


    @patch('psycopg2.connect')
    def test_execute_query_date_column_not_in_results(self, mock_connect):
        mock_cursor_obj = MagicMock() # Corrected typo
        # Data that doesn't have 'event_day'
        custom_data = [(1, 'zeta'), (2, 'iota')]
        custom_cols = ['item_id', 'item_name']
        mock_cursor_obj.fetchall.return_value = custom_data
        mock_cursor_obj.description = [(col,) for col in custom_cols]


        mock_connection_obj = MagicMock()
        mock_connection_obj.cursor.return_value = mock_cursor_obj
        mock_connect.return_value = mock_connection_obj

        query = "SELECT item_id, item_name FROM items;"
        with RedshiftConnector(self.config) as connector:
            with self.assertLogs(level='WARNING') as log_watcher:
                # date_column "event_day" is passed, but query result doesn't have it
                df = connector.execute_query(query, date_column="event_day")
            self.assertTrue(any("Specified date_column 'event_day' (as 'event_day') not found" in message for message in log_watcher.output))

        self.assertIsInstance(df, pd.DataFrame)
        # Columns should be lowercased by the connector
        self.assertListEqual(sorted(df.columns.tolist()), sorted(['item_id', 'item_name']))
        self.assertNotIn("Date", df.columns)


if __name__ == '__main__':
    unittest.main()
