import io
import unittest
from unittest.mock import patch, MagicMock

import pandas as pd

# Assuming src is in PYTHONPATH
from src.validator import WBRValidator


class TestWBRValidator(unittest.TestCase):

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
        csv_file = io.StringIO(csv_content)

        config_with_db = self.base_config.copy()
        config_with_db['db_config_url'] = 'http://example.com/connections.yaml'

        # We expect that pd.read_csv is called and the DB logic is NOT.
        with patch('pandas.read_csv') as mock_read_csv:
            mock_read_csv.return_value = pd.DataFrame({'Date': pd.to_datetime(['2023-01-01']), 'MetricA': [100]})

            with patch('src.validator.load_connections_from_url_or_path') as mock_load_conns:
                validator = WBRValidator(cfg=config_with_db, daily_df=csv_file)

                mock_read_csv.assert_called_once()
                mock_load_conns.assert_not_called()  # Crucial: DB logic should be skipped
                self.assertFalse(validator.daily_df.empty)
                self.assertEqual(validator.daily_df['MetricA'].iloc[0], 100)

    @patch('src.validator.load_connections_from_url_or_path')
    @patch('src.validator.get_connector')
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
        config_with_db['db_config_url'] = 'http://example.com/connections.yaml'
        config_with_db['data_sources'] = {
            "MyPostgres": {
                "queries": {
                    "main_query": {
                        "description": "Test query",
                        "query": "SELECT event_date as \"Date\", value as \"MetricB\" FROM test"
                    }
                }
            }
        }

        # 3. Instantiate the validator with no CSV data
        validator = WBRValidator(cfg=config_with_db, daily_df=None)

        # 4. Assertions
        mock_load_conns.assert_called_once_with('http://example.com/connections.yaml')
        mock_get_connector.assert_called_once_with("postgres", {})
        mock_connector_instance.execute_query.assert_called_once_with(
            "SELECT event_date as \"Date\", value as \"MetricB\" FROM test")
        self.assertFalse(validator.daily_df.empty)
        self.assertEqual(validator.daily_df['MetricB'].iloc[0], 200)

    def test_error_if_no_data_source_provided(self):
        """
        Tests that an error is raised if no CSV and no db_config_url are provided.
        """
        # Config without db_config_url
        config_no_source = self.base_config.copy()

        with self.assertRaisesRegex(ValueError,
                                    "No data source provided. Please provide either a CSV file or a 'db_config_url' in your YAML config."):
            WBRValidator(cfg=config_no_source, daily_df=None)


if __name__ == '__main__':
    unittest.main()
