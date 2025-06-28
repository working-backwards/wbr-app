from datetime import datetime
import pandas as pd
import logging

from src.connectors import get_connector, BaseConnector # Import connector factory

logger = logging.getLogger(__name__)
week_ending_date_format = '%d-%b-%Y'


# This function seems unused after refactoring, consider removing if not needed elsewhere.
# def check_params(config):
#     return 'function' not in config and \
#         ("aggf" in config and ('column' in config or 'filter' in config))


class WBRValidator:
    def __init__(self, data_sources_config: list, wbr_yaml_config: dict, all_connections: dict):
        """
        Initializes the WBRValidator.
        Instead of a CSV, it now takes data_sources_config (from WBR YAML),
        the full WBR YAML config, and all available database connections.

        Args:
            data_sources_config (list): List of data source configurations
                                       (each specifying connection_name, query, date_column).
            wbr_yaml_config (dict): The main WBR YAML configuration.
            all_connections (dict): A dictionary of all loaded database connections
                                    (from connections.yaml), keyed by connection name.
        """
        self.cfg = wbr_yaml_config # This is the main WBR config (formerly just 'cfg')
        self.data_sources_config = data_sources_config
        self.all_connections = all_connections
        self.daily_df = self._load_and_combine_data_sources()

    def _load_and_combine_data_sources(self) -> pd.DataFrame:
        """
        Loads data from all configured data sources and combines them.
        For now, it assumes a single data source for simplicity, matching the previous
        behavior of a single CSV. This can be expanded later to merge multiple sources.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the combined data, sorted by 'Date'.

        Raises:
            ValueError: If no data sources are defined or if a connection is not found.
        """
        if not self.data_sources_config or not isinstance(self.data_sources_config, list) or len(self.data_sources_config) == 0:
            # If allowing no data sources (e.g. for a static report), return empty DF with 'Date'
            # For now, let's assume at least one data source is typical for WBR.
            logger.error("No data sources defined in the WBR configuration.")
            raise ValueError("No data sources defined in the WBR configuration.")

        # --- Simplification for initial refactor: Assume ONE data source ---
        # This part can be expanded to loop through data_sources_config,
        # fetch each DataFrame, and then intelligently merge them based on 'Date'
        # or other strategies. For now, we take the first one.
        if len(self.data_sources_config) > 1:
            logger.warning("Multiple data sources found. For initial refactor, only the first one will be processed.")

        source_config = self.data_sources_config[0]
        connection_name = source_config.get("connection_name")
        query = source_config.get("query")
        date_column = source_config.get("date_column", "Date") # Default to "Date" if not specified

        if not connection_name or not query:
            line = source_config.get('__line__', 'N/A')
            raise ValueError(f"Data source config near line {line} is missing 'connection_name' or 'query'.")

        connection_details = self.all_connections.get(connection_name)
        if not connection_details:
            raise ValueError(f"Connection '{connection_name}' not found in connections configuration.")

        logger.info(f"Loading data for source '{source_config.get('name', 'Unnamed')}' using connection '{connection_name}'.")

        try:
            connector_type = connection_details.get("type")
            connector_config = connection_details.get("config")

            # The 'get_connector' function returns a context manager enabled connector
            with get_connector(connector_type, connector_config) as connector:
                # The date_column from source_config is passed to execute_query
                # so the connector can handle renaming and parsing.
                df = connector.execute_query(query, date_column=date_column)
        except Exception as e:
            logger.error(f"Failed to load data for source '{source_config.get('name', 'Unnamed')}' using connection '{connection_name}': {e}", exc_info=True)
            # Depending on desired behavior, could raise, or return empty/partial data
            raise RuntimeError(f"Failed to load data for source '{source_config.get('name', 'Unnamed')}': {e}")

        if "Date" not in df.columns:
            # This should ideally be caught by the connector's _rename_date_column,
            # but as a safeguard:
            raise ValueError(f"Query results for source '{source_config.get('name', 'Unnamed')}' using connection '{connection_name}' did not produce a 'Date' column after processing. Ensure your query selects a date column and it's correctly specified as 'date_column' in the config.")

        # Ensure 'Date' column is datetime
        try:
            df["Date"] = pd.to_datetime(df["Date"])
        except Exception as e:
            raise ValueError(f"Could not convert 'Date' column to datetime for source '{source_config.get('name', 'Unnamed')}': {e}")

        # Sort by date, which is critical for WBR logic
        df = df.sort_values(by='Date')

        # TODO: Future enhancement - Handle multiple data sources and merge them.
        # For now, `self.daily_df` will be the DataFrame from the first data source.

        logger.info(f"Successfully loaded and processed data source. Resulting DataFrame has {len(df)} rows.")
        return df

    def validate_yaml(self):
        # The structure of self.cfg (the WBR YAML) is assumed to be validated
        # by the time it's passed here, or specific checks for 'setup', 'metrics', 'deck'
        # can be added if WBRValidator is also responsible for that.
        # For now, this focuses on the data loading part.
        self.check_week_ending()
        self.validate_aggf()

    def check_week_ending(self):
        """
        Checks the week ending date in the configuration.

        Raises:
            Exception: If the 'week_ending' key is missing in the 'setup' section of the configuration.
            ValueError: If the 'week_ending' date is not in the correct format.
        """
        if 'week_ending' not in self.cfg['setup']:
            raise Exception(f"Error in SETUP section for week_ending at line {self.cfg['setup']['__line__']}")
        try:
            # Validate the week ending date format
            datetime.strptime(self.cfg['setup']['week_ending'], week_ending_date_format)
        except ValueError:
            raise ValueError(f"week_ending is in an invalid format, example of correct format: 25-SEP-2012, at line: "
                             f"{self.cfg['setup']['__line__']}")

    def validate_aggf(self):
        """
        Validates the aggregate function (aggf) configuration for metrics.

        This method checks each metric's configuration for required parameters and validates
        the metric comparison method.

        Raises:
            KeyError: If required parameters for metric configuration are missing or if an invalid comparison method is
             found.
        """
        for metric, config in self.cfg['metrics'].items():
            # Skip the line indicator key
            if metric == '__line__':
                continue

            # Check for required metric config parameters
            if 'function' not in config and (
                    "aggf" not in config or ('column' not in config and 'filter' not in config)):
                raise KeyError(
                    f"One of the required metric config parameters from the list [aggf, column, filter] is missing for"
                    f" the metric {metric} at line: {config['__line__']}")

            # Validate the metric comparison method
            if 'metric_comparison_method' in config and config['metric_comparison_method'] != 'bps':
                raise KeyError(
                    f"Invalid value provided for metric_comparison_method {config['metric_comparison_method']} for"
                    f" the metric {metric} at line: {config['__line__']}")