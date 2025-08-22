import logging

import pandas as pd
import requests
import yaml
from yaml._yaml import ScannerError

from src.connectors import get_connector  # Import connector factory
from src.controller_utility import SafeLineLoader
from src.publish_utility import PublishWbr

logger = logging.getLogger(__name__)


class DataLoader:

    def __init__(self, cfg: dict, csv_data: any = None, publisher: PublishWbr = None):

        """
        Initializes the DataLoader that loads data based on the fallback logic:
        1. Use `csv_data` if provided.
        2. If not, use `db_config_url` from the `cfg`.
        3. If neither is available, raise an error.

        Args:
            cfg (dict): The main WBR YAML configuration.
            csv_data (any, optional): A file stream for a CSV file. Defaults to None.
        """

        self.db_connections = None  # Will be loaded on-demand
        self.cfg = cfg
        self.publisher = publisher

        if csv_data:
            logger.info("CSV data provided. Using CSV as the primary data source.")
            self.daily_df = pd.read_csv(csv_data, parse_dates=['Date'], thousands=',').sort_values(by='Date')
        else:
            logger.info("No CSV data provided. Attempting to load data from database source.")
            db_config_url = self.cfg.get("setup").get("db_config_url")
            if not db_config_url:
                raise ValueError(
                    "No data source provided. Please provide either a CSV file or a 'db_config_url' in your YAML config.")

            # Load connections on-demand from the URL/path
            self.db_connections = _load_connections_from_url_or_path(db_config_url)
            self.daily_df = self._load_and_combine_data_from_db()

    def _load_and_combine_data_from_db(self) -> pd.DataFrame:
        """
        Loads data from the database sources defined in the WBR config.
        This uses the new dictionary-based `data_sources` format.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the combined data, sorted by 'Date'.
        """
        data_sources = self.cfg.get('data_sources')
        if not data_sources or not isinstance(data_sources, dict):
            raise ValueError("'data_sources' must be a dictionary in the WBR config.")

        # --- Simplification: Process the first connection and first query found ---
        # This can be expanded later to merge data from multiple queries/connections.

        connection_name = next(iter(data_sources))
        connection_config = self.db_connections.get(connection_name)
        if not connection_config:
            raise ValueError(
                f"Connection '{connection_name}' defined in 'data_sources' not found in the connections file.")

        queries = data_sources[connection_name].get('queries')
        if not queries or not isinstance(queries, dict):
            raise ValueError(f"No 'queries' dictionary found for connection '{connection_name}'.")

        query_name = next(iter(queries))
        query_details = queries[query_name]
        query = query_details.get("query")
        description = query_details.get("description", "No description")

        if not query:
            raise ValueError(f"No 'query' found for query '{query_name}' under connection '{connection_name}'.")

        logger.info(f"Loading data from query '{query_name}' ({description}) using connection '{connection_name}'.")

        try:
            connector_type = connection_config.get("type")
            connector_config_params = connection_config.get("config")

            with get_connector(connector_type, connector_config_params) as connector:
                # The 'date_column' parameter is removed. Convention over configuration.
                # The query MUST alias the date column as "Date".
                df = connector.execute_query(query)
        except Exception as e:
            logger.error(f"Failed to load data for query '{query_name}' using connection '{connection_name}': {e}",
                         exc_info=True)
            raise RuntimeError(f"Failed to load data for query '{query_name}': {e}")

        # Validate that the query results adhere to the new convention.
        if "Date" not in df.columns:
            raise ValueError(
                f"Query results for '{query_name}' did not produce a 'Date' column. Please alias your date column as \"Date\" in your SQL query.")

        try:
            df["Date"] = pd.to_datetime(df["Date"])
        except Exception as e:
            raise ValueError(f"Could not convert 'Date' column to datetime for query '{query_name}': {e}")

        df = df.sort_values(by='Date')

        logger.info(f"Successfully loaded and processed data from DB. Resulting DataFrame has {len(df)} rows.")
        return df


def _load_connections_from_url_or_path(url_or_path: str) -> dict:
    """
    Loads and parses a connections YAML file from a URL or a local file path.

    Args:
        url_or_path (str): The URL or local file path to the connections YAML file.

    Returns:
        dict: A dictionary mapping connection names to their configurations.
    """
    if url_or_path.lower().startswith(('http://', 'https://')):
        try:
            response = requests.get(url_or_path, allow_redirects=True)
            response.raise_for_status()  # Raise an exception for bad status codes
            content = response.content.decode("utf-8")
            config_data = yaml.load(content, Loader=SafeLineLoader)
            logging.info(f"Successfully fetched connections file from URL: {url_or_path}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch connections file from URL: {url_or_path}. Error: {e}", exc_info=True)
            raise ConnectionError(f"Failed to fetch connections file from URL: {url_or_path}")
        except (ScannerError, yaml.YAMLError) as e:
            logging.error(f"Error parsing connections YAML from {url_or_path}: {e}", exc_info=True)
            raise ValueError(f"Error parsing connections YAML from {url_or_path}: {e}")
    else:
        # Treat as a local file path
        try:
            with open(url_or_path, 'r') as f:
                content = f.read()
                config_data = yaml.load(content, Loader=SafeLineLoader)
            logging.info(f"Successfully read connections file from local path: {url_or_path}")
        except FileNotFoundError:
            logger.error(f"Connections configuration file not found at local path: {url_or_path}")
            raise FileNotFoundError(f"Connections configuration file not found at: {url_or_path}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while reading local connections file {url_or_path}: {e}",
                         exc_info=True)
            raise
        except (ScannerError, yaml.YAMLError) as e:
            logging.error(f"Error parsing connections YAML from {url_or_path}: {e}", exc_info=True)
            raise ValueError(f"Error parsing connections YAML from {url_or_path}: {e}")

    # Validate the structure
    if not isinstance(config_data, dict) or "connections" not in config_data:
        raise ValueError(f"Invalid connections YAML structure from {url_or_path}. Missing 'connections' key.")
    if not isinstance(config_data["connections"], list):
        raise ValueError(f"Invalid connections YAML structure from {url_or_path}. 'connections' must be a list.")

    connections_map = {}
    for conn in config_data["connections"]:
        if not isinstance(conn, dict) or "name" not in conn or "type" not in conn or "config" not in conn:
            line = conn.get('__line__', 'N/A')
            raise ValueError(
                f"Invalid connection entry in {url_or_path} near line {line}. Each connection must have 'name', 'type', and 'config'.")
        if conn["name"] in connections_map:
            line = conn.get('__line__', 'N/A')
            raise ValueError(f"Duplicate connection name '{conn['name']}' found in {url_or_path} near line {line}.")
        connections_map[conn["name"]] = conn

    logging.info(f"Successfully loaded {len(connections_map)} connections from {url_or_path}.")
    return connections_map
