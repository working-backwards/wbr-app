import unittest
from unittest.mock import patch, mock_open
import yaml # For creating mock YAML content and potential errors

# Assuming src is in PYTHONPATH or tests are run from root
from src.controller_utility import load_connections_config, SafeLineLoader

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
  - name: "Postgres_Prod" # Duplicate name
    type: "postgres"
    config: {host: "pg2"}
"""

MISSING_KEYS_YAML = """
version: 1.0
connections:
  - name: "Incomplete_PG"
    # type: "postgres" # Missing type
    config: {host: "pg1"}
"""

NO_CONNECTIONS_KEY_YAML = """
version: 1.0
# connections: key is missing
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
# This should cause a YAMLError during parsing by yaml.load
"""

class TestControllerUtility_Connections(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open, read_data=VALID_CONNECTIONS_YAML)
    def test_load_connections_config_success(self, mock_file):
        expected_connections = {
            "Postgres_Prod": {
                "name": "Postgres_Prod", "type": "postgres",
                "description": "Production PostgreSQL",
                "config": {"host": "pg.example.com", "port": 5432, "username": "prod_user", "database": "proddb"}
            },
            "Snowflake_DW": {
                "name": "Snowflake_DW", "type": "snowflake",
                "config": {"account": "sfaccount", "user": "sf_user"}
            }
        }
        # PyYAML SafeLineLoader adds __line__ to mappings, so we need to account for that if we do a direct dict comparison.
        # For simplicity, we'll check key presence and core config values.

        loaded_conns = load_connections_config("dummy_path.yaml")

        self.assertIn("Postgres_Prod", loaded_conns)
        self.assertEqual(loaded_conns["Postgres_Prod"]["type"], "postgres")
        self.assertEqual(loaded_conns["Postgres_Prod"]["config"]["host"], "pg.example.com")

        self.assertIn("Snowflake_DW", loaded_conns)
        self.assertEqual(loaded_conns["Snowflake_DW"]["type"], "snowflake")
        self.assertEqual(loaded_conns["Snowflake_DW"]["config"]["account"], "sfaccount")

        mock_file.assert_called_once_with("dummy_path.yaml", 'r')

    @patch("builtins.open", side_effect=FileNotFoundError("File not found"))
    def test_load_connections_config_file_not_found(self, mock_file):
        with self.assertRaisesRegex(FileNotFoundError, "Connections configuration file not found"):
            load_connections_config("non_existent.yaml")
        mock_file.assert_called_once_with("non_existent.yaml", 'r')

    @patch("builtins.open", new_callable=mock_open, read_data=INVALID_YAML_SYNTAX)
    def test_load_connections_config_yaml_error(self, mock_file):
        with self.assertRaisesRegex(Exception, "Error parsing connections YAML"):
            load_connections_config("bad_syntax.yaml")

    @patch("builtins.open", new_callable=mock_open, read_data=DUPLICATE_NAME_YAML)
    def test_load_connections_config_duplicate_names(self, mock_file):
        with self.assertRaisesRegex(Exception, "Duplicate connection name 'Postgres_Prod' found"):
            load_connections_config("duplicates.yaml")

    @patch("builtins.open", new_callable=mock_open, read_data=MISSING_KEYS_YAML)
    def test_load_connections_config_missing_required_keys(self, mock_file):
        with self.assertRaisesRegex(Exception, "Each connection must have 'name', 'type', and 'config'"):
            load_connections_config("missing_keys.yaml")

    @patch("builtins.open", new_callable=mock_open, read_data=NO_CONNECTIONS_KEY_YAML)
    def test_load_connections_config_no_connections_key(self, mock_file):
        with self.assertRaisesRegex(Exception, "Invalid connections YAML structure.*Missing 'connections' top-level key"):
            load_connections_config("no_connections_key.yaml")

    @patch("builtins.open", new_callable=mock_open, read_data=CONNECTIONS_NOT_A_LIST_YAML)
    def test_load_connections_config_connections_not_a_list(self, mock_file):
        with self.assertRaisesRegex(Exception, "Invalid connections YAML structure.*'connections' should be a list"):
            load_connections_config("connections_not_list.yaml")

    # Test SafeLineLoader inclusion of line numbers in error messages (conceptual)
    # This is harder to assert directly without deeper mocking of yaml internals or specific error content
    # but the use of SafeLineLoader in the function is what enables this.
    # For example, if MISSING_KEYS_YAML had one good entry then one bad one, the error for the bad one should show its line.
    # The current MISSING_KEYS_YAML has the error on the first entry.

    # Example of a more complex YAML for line number check
    YAML_FOR_LINE_NUMBER_CHECK = """
version: 1.0
connections:
  - name: "GoodConn"
    type: "dummy"
    config: {key: "val"} # Line 5
  - name: "BadConn_MissingType"
    # type: "dummy" # This would be line 7 if uncommented
    config: {key: "val"} # Line 8
"""
    @patch("builtins.open", new_callable=mock_open, read_data=YAML_FOR_LINE_NUMBER_CHECK)
    def test_load_connections_config_line_numbers_in_error(self, mock_file):
        # The error message from SafeLineLoader should ideally point to line 7
        # for the "BadConn_MissingType" entry, as the 'name' is on line 6, 'type' (missing) would be line 7.
        # The error "Each connection must have 'name', 'type', and 'config'" implies it parsed the item
        # and then found keys missing. The __line__ attribute given by SafeLineLoader for the problematic dict
        # will be the starting line of that dict.
        with self.assertRaisesRegex(Exception, "near line 7"): # Line numbers are 1-based. The dict starts on line 6, error reported for line of name.
            load_connections_config("line_check.yaml")


if __name__ == '__main__':
    unittest.main()
