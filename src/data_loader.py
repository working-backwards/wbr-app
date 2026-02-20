import logging

import pandas as pd
import requests
import yaml
from pandas import DataFrame
from yaml._yaml import ScannerError

from src.connectors import get_connector  # Import connector factory
from src.controller_utility import SafeLineLoader

logger = logging.getLogger(__name__)


class DataLoader:

    def __init__(self, cfg: dict, csv_data: any = None):

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

        self.events = self._load_annotations_data()

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

        df_list = []

        for source_name, data_source in data_sources.items():
            if source_name == "__line__":
                continue
            elif source_name == "csv_files":
                _get_df_from_csv_source(data_source, df_list)
            else:
                self._get_df_from_external_connection(data_source, df_list, source_name)

        return _merge_dataframes(df_list, on="Date")

    def _get_df_from_external_connection(self, data_source, df_list, source_name):
        connection_config = self.db_connections.get(source_name)
        if not connection_config:
            raise ValueError(
                f"Connection '{source_name}' defined in 'data_sources' not found in the connections file.")

        if not data_source or not isinstance(data_source, dict):
            raise ValueError(f"No 'queries' dictionary found for connection '{source_name}'.")

        for query_name, query_details in data_source.items():
            if query_name == "__line__":
                continue

            query = query_details.get("query")

            if not query:
                raise ValueError(
                    f"No 'query' found for query '{query_name}' under connection '{source_name}'.")

            logger.info(
                f"Loading data from query '{query_name}' using connection '{source_name}'.")

            try:
                connector_type = connection_config.get("type")
                connector_config_params = connection_config.get("config")

                with get_connector(connector_type, connector_config_params) as connector:
                    # The 'date_column' parameter is removed. Convention over configuration.
                    # The query MUST alias the date column as "Date".
                    df = connector.execute_query(query)
            except Exception as e:
                logger.error(
                    f"Failed to load data for query '{query_name}' using connection '{source_name}': {e}",
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

            col_alias = {col: f"{query_name}.{col}" for col in df.columns if col != "Date"}
            df = df.rename(columns=col_alias)

            df_list.append(df)
            logger.info(
                f"Successfully loaded and processed data from DB. Resulting DataFrame has {len(df)} rows.")

    def _load_annotations_data(self) -> DataFrame | None:
        if "annotations" not in self.cfg:
            return None

        annotation_sources: list = self.cfg.get('annotations')

        df_list = []
        for annotations_file in annotation_sources:
            if annotations_file.lower().startswith(('http://', 'https://')):
                try:
                    response = requests.get(annotations_file, allow_redirects=True)
                    response.raise_for_status()  # Raise an exception for bad status codes
                    content = response.content.decode("utf-8")
                    df = pd.read_csv(content, parse_dates=['Date'], thousands=',').sort_values(by='Date')
                    logger.info(f"Successfully fetched annotations csv file from URL: {annotations_file}")
                except requests.exceptions.RequestException as e:
                    logger.error(f"Failed to fetch annotations csv file from URL: {annotations_file}. Error: {e}",
                                 exc_info=True)
                    raise ConnectionError(f"Failed to fetch annotations csv file from URL: {annotations_file}")
                except Exception as e:
                    logger.error(
                        f"An unexpected error occurred while reading annotations csv data from {annotations_file}: {e}",
                        exc_info=True)
                    raise
            else:
                try:
                    df = pd.read_csv(annotations_file, parse_dates=['Date'], thousands=',').sort_values(by='Date')
                    logger.info(f"Successfully read annotations csv file from local path: {annotations_file}")
                except FileNotFoundError:
                    logger.error(f"Annotations csv file not found at local path: {annotations_file}")
                    raise FileNotFoundError(f"Annotations csv file not found at: {annotations_file}")
                except Exception as e:
                    logger.error(
                        f"An unexpected error occurred while reading local annotations csv file {annotations_file}: {e}",
                        exc_info=True)
                    raise

            df_list.append(df)

        return pd.concat(df_list, axis=1)


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
            logger.info(f"Successfully fetched connections file from URL: {url_or_path}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch connections file from URL: {url_or_path}. Error: {e}", exc_info=True)
            raise ConnectionError(f"Failed to fetch connections file from URL: {url_or_path}")
        except (ScannerError, yaml.YAMLError) as e:
            logger.error(f"Error parsing connections YAML from {url_or_path}: {e}", exc_info=True)
            raise ValueError(f"Error parsing connections YAML from {url_or_path}: {e}")
    else:
        # Treat as a local file path
        try:
            with open(url_or_path, 'r') as f:
                content = f.read()
                config_data = yaml.load(content, Loader=SafeLineLoader)
            logger.info(f"Successfully read connections file from local path: {url_or_path}")
        except FileNotFoundError:
            logger.error(f"Connections configuration file not found at local path: {url_or_path}")
            raise FileNotFoundError(f"Connections configuration file not found at: {url_or_path}")
        except (ScannerError, yaml.YAMLError) as e:
            logger.error(f"Error parsing connections YAML from {url_or_path}: {e}", exc_info=True)
            raise ValueError(f"Error parsing connections YAML from {url_or_path}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while reading local connections file {url_or_path}: {e}",
                         exc_info=True)
            raise

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

    logger.info(f"Successfully loaded {len(connections_map)} connections from {url_or_path}.")
    return connections_map


def _get_df_from_csv_source(data_source, df_list):
    for csv_source_name, source_config in data_source.items():
        if csv_source_name == "__line__":
            continue

        url_or_path = source_config["url_or_path"]
        if source_config["url_or_path"].lower().startswith(('http://', 'https://')):
            try:
                response = requests.get(url_or_path, allow_redirects=True)
                response.raise_for_status()  # Raise an exception for bad status codes
                content = response.content.decode("utf-8")
                df = pd.read_csv(content, parse_dates=['Date'], thousands=',').sort_values(by='Date')
                logger.info(f"Successfully fetched csv file from URL: {url_or_path}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch csv file from URL: {url_or_path}. Error: {e}", exc_info=True)
                raise ConnectionError(f"Failed to fetch csv file from URL: {url_or_path}")
            except Exception as e:
                logger.error(f"An unexpected error occurred while reading csv data from {url_or_path}: {e}",
                              exc_info=True)
                raise
        else:
            try:
                df = pd.read_csv(url_or_path, parse_dates=['Date'], thousands=',').sort_values(by='Date')
                logger.info(f"Successfully read csv file from local path: {url_or_path}")
            except FileNotFoundError:
                logger.error(f"Data csv file not found at local path: {url_or_path}")
                raise FileNotFoundError(f"Data csv file not found at: {url_or_path}")
            except Exception as e:
                logger.error(f"An unexpected error occurred while reading local csv file {url_or_path}: {e}",
                             exc_info=True)
                raise

        col_alias = {col: f"{csv_source_name}.{col}" for col in df.columns if col != "Date"}
        df = df.rename(columns=col_alias)
        df_list.append(df)


def _merge_dataframes(dataframes, on="date", sort=True):
    """
    Hybrid join:
      - Outer-join the FIRST row per date from each source.
      - Append any additional rows from each source for that same date,
        with NaNs for other sources' columns.
    """
    first_occurrences = []
    extras_list = []

    used_columns = {on}

    for idx, df in enumerate(dataframes):
        df = df.copy()
        # Ensure deterministic "first" per date
        df = df.sort_values(on, kind="stable")

        rename_map = {}
        for col in df.columns:
            if col == on:
                continue
            if col in used_columns:
                # Collision → add suffix
                rename_map[col] = f"{col}_src{idx + 1}"
            else:
                used_columns.add(col)

        if rename_map:
            df = df.rename(columns=rename_map)

        # First occurrence per date
        first = df[~df.duplicated(on, keep='first')].copy()
        first_occurrences.append(first)

        # Additional rows for the same date (beyond first)
        extra = df[df.duplicated(on, keep='first')].copy()
        extras_list.append(extra)

    # Outer-join the first occurrences across sources
    merged = first_occurrences[0]
    for other in first_occurrences[1:]:
        merged = merged.merge(other, on=on, how='outer', sort=False)

    # Append aligned extras: reindex to merged columns to avoid KeyError
    for extra in extras_list:
        extra_aligned = extra.reindex(columns=merged.columns)
        merged = pd.concat([merged, extra_aligned], ignore_index=True, sort=False)

    if sort:
        merged = merged.sort_values(on, kind="stable").reset_index(drop=True)
    return merged


if __name__ == "__main__":
    df1 = pd.DataFrame({
        "date": ["2025-01-15", "2025-01-15", "2025-01-16"],
        "metric1": [10, 20, 30]
    })

    df2 = pd.DataFrame({
        "date": ["2025-01-15", "2025-01-17"],
        "metric2": [100, 200]
    })

    result = _merge_dataframes([df1, df2], on="date")
    print(result)
