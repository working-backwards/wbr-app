# SPDX-License-Identifier: Apache-2.0
import io
import unittest
from unittest.mock import patch, MagicMock

import pandas as pd

# Assuming src is in PYTHONPATH
from src.data_loader import DataLoader


class TestDataLoader(unittest.TestCase):

    def setUp(self):
        self.base_config = {
            "setup": {
                "week_ending": "01-JAN-2023",
                "week_number": 1,
                "title": "Test WBR",
                "fiscal_year_end_month": "DEC"
            },
            "metrics": {},
            "deck": []
        }

    def test_csv_data_takes_precedence(self):
        """
        Tests that if CSV data is provided, it's used even if db_config_url exists.
        """
        csv_content = "Date,MetricA\n2023-01-01,100\n"
        csv_stream = io.StringIO(csv_content)

        config_with_db = self.base_config.copy()
        # db_config_url is now expected under setup for DataLoader
        config_with_db.setdefault("setup", {}).setdefault(
            "db_config_url", "http://example.com/connections.yaml"
        )

        # We expect that pd.read_csv is called and the DB logic is NOT.
        with patch('pandas.read_csv') as mock_read_csv:
            mock_read_csv.return_value = pd.DataFrame(
                {'Date': pd.to_datetime(['2023-01-01']), 'MetricA': [100]}
            )

            with patch('src.data_loader._load_connections_from_url_or_path') as mock_load_conns:
                loader = DataLoader(cfg=config_with_db, csv_data=csv_stream)

                mock_read_csv.assert_called_once()
                mock_load_conns.assert_not_called()  # Crucial: DB logic should be skipped
                self.assertFalse(loader.daily_df.empty)
                self.assertEqual(loader.daily_df['MetricA'].iloc[0], 100)

    @patch('src.data_loader._load_connections_from_url_or_path')
    @patch('src.data_loader.get_connector')
    def test_db_config_url_is_used_when_no_csv(self, mock_get_connector, mock_load_conns):
        """
        Tests that if no CSV is provided, the db_config_url is used to fetch data.
        """
        # 1. Setup mocks for the entire DB loading flow
        mock_connections = {"MyPostgres": {"type": "postgres", "config": {}}}
        mock_load_conns.return_value = mock_connections

        mock_connector_instance = MagicMock()
        mock_connector_instance.execute_query.return_value = pd.DataFrame({
            'Date': pd.to_datetime(['2023-01-02']),
            'MetricB': [200]
        })

        # The factory function `get_connector` returns a context manager
        mock_connector_cm = MagicMock()
        mock_connector_cm.__enter__.return_value = mock_connector_instance
        mock_get_connector.return_value = mock_connector_cm

        # 2. Prepare the config with db_config_url and data_sources
        config_with_db = self.base_config.copy()
        config_with_db.setdefault("setup", {})["db_config_url"] = "http://example.com/connections.yaml"
        config_with_db['data_sources'] = {
            "MyPostgres": {
                "main_query": {
                    "description": "Test query",
                    "query": "SELECT event_date as \"Date\", value as \"MetricB\" FROM test"
                }
            }
        }

        # 3. Instantiate the DataLoader with no CSV data
        loader = DataLoader(cfg=config_with_db, csv_data=None)

        # 4. Assertions
        mock_load_conns.assert_called_once_with("http://example.com/connections.yaml")
        mock_get_connector.assert_called_once_with("postgres", {})
        mock_connector_instance.execute_query.assert_called_once_with(
            "SELECT event_date as \"Date\", value as \"MetricB\" FROM test"
        )
        self.assertFalse(loader.daily_df.empty)
        # DataLoader prefixes metric columns with the query name (e.g. "main_query.MetricB")
        self.assertIn("main_query.MetricB", loader.daily_df.columns)
        self.assertEqual(loader.daily_df["main_query.MetricB"].iloc[0], 200)

    def test_error_if_no_data_source_provided(self):
        """
        Tests that an error is raised if no CSV and no db_config_url are provided.
        """
        # Config without db_config_url
        config_no_source = self.base_config.copy()

        with self.assertRaisesRegex(
                ValueError,
                "No data source provided. Please provide either a CSV file or a 'db_config_url' in your YAML config.",
        ):
            DataLoader(cfg=config_no_source, csv_data=None)


if __name__ == '__main__':
    unittest.main()
