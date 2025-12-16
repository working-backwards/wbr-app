import unittest
from unittest.mock import patch, mock_open, MagicMock

import requests
import yaml

from src.controller_utility import load_connections_from_url_or_path, SafeLineLoader

VALID_CONNECTIONS_YAML = """
version: 1.0
connections:
  - name: "Postgres_Prod"
    type: "postgres"
    description: "Production PostgreSQL"
    config:
      host: "pg.example.com"
      port: 5432
      username: "prod_user"
      database: "proddb"
  - name: "Snowflake_DW"
    type: "snowflake"
    config:
      account: "sfaccount"
      user: "sf_user"
"""

DUPLICATE_NAME_YAML = """
version: 1.0
connections:
  - name: "Postgres_Prod"
    type: "postgres"
    config: {host: "pg1"}
  - name: "Postgres_Prod"
    type: "postgres"
    config: {host: "pg2"}
"""

MISSING_KEYS_YAML = """
version: 1.0
connections:
  - name: "Incomplete_PG"
    config: {host: "pg1"}
"""

NO_CONNECTIONS_KEY_YAML = """
version: 1.0
other_config: []
"""

CONNECTIONS_NOT_A_LIST_YAML = """
version: 1.0
connections: "This should be a list"
"""

INVALID_YAML_SYNTAX = """
version: 1.0
connections:
  - name: "ReallyBad"
    type: [ unterminated list
"""


class TestControllerUtility_Connections(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open, read_data=VALID_CONNECTIONS_YAML)
    def test_load_from_local_path_success(self, mock_file):
        loaded_conns = load_connections_from_url_or_path("dummy_path.yaml")
        self.assertIn("Postgres_Prod", loaded_conns)
        self.assertEqual(loaded_conns["Postgres_Prod"]["type"], "postgres")
        mock_file.assert_called_once_with("dummy_path.yaml", 'r')

    @patch("requests.get")
    def test_load_from_url_success(self, mock_requests_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content.decode.return_value = VALID_CONNECTIONS_YAML
        mock_requests_get.return_value = mock_response

        loaded_conns = load_connections_from_url_or_path("https://example.com/connections.yaml")
        self.assertIn("Snowflake_DW", loaded_conns)
        self.assertEqual(loaded_conns["Snowflake_DW"]["config"]["account"], "sfaccount")
        mock_requests_get.assert_called_once_with("https://example.com/connections.yaml", allow_redirects=True)

    @patch("requests.get")
    def test_load_from_url_fails(self, mock_requests_get):
        mock_requests_get.side_effect = requests.exceptions.RequestException("Network Error")
        with self.assertRaises(ConnectionError):
            load_connections_from_url_or_path("https://example.com/connections.yaml")

    @patch("builtins.open", side_effect=FileNotFoundError)
    def test_load_from_local_path_not_found(self, mock_file):
        with self.assertRaises(FileNotFoundError):
            load_connections_from_url_or_path("non_existent.yaml")

    @patch("builtins.open", new_callable=mock_open, read_data=INVALID_YAML_SYNTAX)
    def test_load_yaml_error(self, mock_file):
        with self.assertRaisesRegex(ValueError, "Error parsing connections YAML"):
            load_connections_from_url_or_path("bad_syntax.yaml")

    @patch("builtins.open", new_callable=mock_open, read_data=DUPLICATE_NAME_YAML)
    def test_load_duplicate_names(self, mock_file):
        with self.assertRaisesRegex(ValueError, "Duplicate connection name 'Postgres_Prod'"):
            load_connections_from_url_or_path("duplicates.yaml")

    @patch("builtins.open", new_callable=mock_open, read_data=MISSING_KEYS_YAML)
    def test_load_missing_required_keys(self, mock_file):
        with self.assertRaisesRegex(ValueError, "Each connection must have 'name', 'type', and 'config'"):
            load_connections_from_url_or_path("missing_keys.yaml")

    @patch("builtins.open", new_callable=mock_open, read_data=NO_CONNECTIONS_KEY_YAML)
    def test_load_no_connections_key(self, mock_file):
        with self.assertRaisesRegex(ValueError, "Missing 'connections' key"):
            load_connections_from_url_or_path("no_connections_key.yaml")

    @patch("builtins.open", new_callable=mock_open, read_data=CONNECTIONS_NOT_A_LIST_YAML)
    def test_load_connections_not_a_list(self, mock_file):
        with self.assertRaisesRegex(ValueError, "'connections' must be a list"):
            load_connections_from_url_or_path("connections_not_list.yaml")


if __name__ == '__main__':
    unittest.main()
