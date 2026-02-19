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


class TestAnnotationsLoading(unittest.TestCase):

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

    @patch('src.data_loader._load_annotation_csv')
    def test_list_format_loads_csv_files(self, mock_load_csv):
        """List-format annotations config loads CSV files."""
        mock_load_csv.return_value = pd.DataFrame({
            'Date': pd.to_datetime(['2023-01-01']),
            'MetricName': ['Impressions'],
            'EventDescription': ['Test event']
        })

        csv_content = "Date,MetricA\n2023-01-01,100\n"
        config = self.base_config.copy()
        config['annotations'] = ['/path/to/annotations.csv']

        with patch('pandas.read_csv') as mock_read_csv:
            mock_read_csv.return_value = pd.DataFrame(
                {'Date': pd.to_datetime(['2023-01-01']), 'MetricA': [100]}
            )
            loader = DataLoader(cfg=config, csv_data=io.StringIO(csv_content))

        mock_load_csv.assert_called_once_with('/path/to/annotations.csv')
        self.assertIsNotNone(loader.annotations)
        self.assertEqual(len(loader.annotations), 1)

    @patch('src.data_loader._load_connections_from_url_or_path')
    @patch('src.data_loader.get_connector')
    def test_dict_format_loads_from_data_sources(self, mock_get_connector, mock_load_conns):
        """Dict-format annotations config loads from database data sources."""
        mock_connections = {"MyPostgres": {"type": "postgres", "config": {}}}
        mock_load_conns.return_value = mock_connections

        mock_connector_instance = MagicMock()
        mock_connector_instance.execute_query.return_value = pd.DataFrame({
            'Date': pd.to_datetime(['2023-01-05']),
            'MetricName': ['Clicks'],
            'EventDescription': ['New campaign launched']
        })
        mock_connector_cm = MagicMock()
        mock_connector_cm.__enter__.return_value = mock_connector_instance
        mock_get_connector.return_value = mock_connector_cm

        csv_content = "Date,MetricA\n2023-01-01,100\n"
        config = self.base_config.copy()
        config['setup']['db_config_url'] = 'http://example.com/connections.yaml'
        config['annotations'] = {
            'data_sources': {
                'MyPostgres': {
                    'recent_annotations': {
                        'query': 'SELECT event_date as "Date", metric_name as "MetricName", description as "EventDescription" FROM annotations;'
                    }
                }
            }
        }

        with patch('pandas.read_csv') as mock_read_csv:
            mock_read_csv.return_value = pd.DataFrame(
                {'Date': pd.to_datetime(['2023-01-01']), 'MetricA': [100]}
            )
            loader = DataLoader(cfg=config, csv_data=io.StringIO(csv_content))

        self.assertIsNotNone(loader.annotations)
        self.assertEqual(len(loader.annotations), 1)
        self.assertEqual(loader.annotations['MetricName'].iloc[0], 'Clicks')
        self.assertEqual(loader.annotations['EventDescription'].iloc[0], 'New campaign launched')

    @patch('src.data_loader._load_connections_from_url_or_path')
    @patch('src.data_loader.get_connector')
    def test_dict_format_missing_columns_raises_error(self, mock_get_connector, mock_load_conns):
        """Dict-format annotations raises ValueError when required columns are missing."""
        mock_connections = {"MyPostgres": {"type": "postgres", "config": {}}}
        mock_load_conns.return_value = mock_connections

        mock_connector_instance = MagicMock()
        # Missing MetricName and EventDescription columns
        mock_connector_instance.execute_query.return_value = pd.DataFrame({
            'Date': pd.to_datetime(['2023-01-05']),
            'SomeOtherColumn': ['value']
        })
        mock_connector_cm = MagicMock()
        mock_connector_cm.__enter__.return_value = mock_connector_instance
        mock_get_connector.return_value = mock_connector_cm

        csv_content = "Date,MetricA\n2023-01-01,100\n"
        config = self.base_config.copy()
        config['setup']['db_config_url'] = 'http://example.com/connections.yaml'
        config['annotations'] = {
            'data_sources': {
                'MyPostgres': {
                    'bad_query': {
                        'query': 'SELECT event_date as "Date", value FROM annotations;'
                    }
                }
            }
        }

        with patch('pandas.read_csv') as mock_read_csv:
            mock_read_csv.return_value = pd.DataFrame(
                {'Date': pd.to_datetime(['2023-01-01']), 'MetricA': [100]}
            )
            with self.assertRaises(ValueError) as ctx:
                DataLoader(cfg=config, csv_data=io.StringIO(csv_content))

        self.assertIn("missing required columns", str(ctx.exception))

    @patch('src.data_loader._load_annotation_csv')
    def test_dict_format_with_csv_files_key(self, mock_load_csv):
        """Dict-format annotations with csv_files key loads CSV files."""
        mock_load_csv.return_value = pd.DataFrame({
            'Date': pd.to_datetime(['2023-01-01']),
            'MetricName': ['Impressions'],
            'EventDescription': ['Test event']
        })

        csv_content = "Date,MetricA\n2023-01-01,100\n"
        config = self.base_config.copy()
        config['annotations'] = {
            'csv_files': ['/path/to/annotations.csv']
        }

        with patch('pandas.read_csv') as mock_read_csv:
            mock_read_csv.return_value = pd.DataFrame(
                {'Date': pd.to_datetime(['2023-01-01']), 'MetricA': [100]}
            )
            loader = DataLoader(cfg=config, csv_data=io.StringIO(csv_content))

        mock_load_csv.assert_called_once_with('/path/to/annotations.csv')
        self.assertIsNotNone(loader.annotations)

    def test_no_annotations_config_returns_none(self):
        """No annotations config returns None."""
        csv_content = "Date,MetricA\n2023-01-01,100\n"
        config = self.base_config.copy()

        with patch('pandas.read_csv') as mock_read_csv:
            mock_read_csv.return_value = pd.DataFrame(
                {'Date': pd.to_datetime(['2023-01-01']), 'MetricA': [100]}
            )
            loader = DataLoader(cfg=config, csv_data=io.StringIO(csv_content))

        self.assertIsNone(loader.annotations)


if __name__ == '__main__':
    unittest.main()
