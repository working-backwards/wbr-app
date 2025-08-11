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


from src.controller_utility import load_connections_from_url_or_path

class WBRValidator:
    def __init__(self, cfg: dict, csv_data: any = None):
        """
        Initializes the WBRValidator and loads data based on the fallback logic:
        1. Use `csv_data` if provided.
        2. If not, use `db_config_url` from the `cfg`.
        3. If neither is available, raise an error.

        Args:
            cfg (dict): The main WBR YAML configuration.
            csv_data (any, optional): A file stream for a CSV file. Defaults to None.
        """
        self.cfg = cfg
        self.db_connections = None # Will be loaded on-demand

        if csv_data:
            logger.info("CSV data provided. Using CSV as the primary data source.")
            self.daily_df = pd.read_csv(csv_data, parse_dates=['Date'], thousands=',').sort_values(by='Date')
        else:
            logger.info("No CSV data provided. Attempting to load data from database source.")
            db_config_url = self.cfg.get("setup").get("db_config_url")
            if not db_config_url:
                raise ValueError("No data source provided. Please provide either a CSV file or a 'db_config_url' in your YAML config.")

            # Load connections on-demand from the URL/path
            self.db_connections = load_connections_from_url_or_path(db_config_url)
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
            raise ValueError(f"Connection '{connection_name}' defined in 'data_sources' not found in the connections file.")

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
            logger.error(f"Failed to load data for query '{query_name}' using connection '{connection_name}': {e}", exc_info=True)
            raise RuntimeError(f"Failed to load data for query '{query_name}': {e}")

        # Validate that the query results adhere to the new convention.
        if "Date" not in df.columns:
            raise ValueError(f"Query results for '{query_name}' did not produce a 'Date' column. Please alias your date column as \"Date\" in your SQL query.")

        try:
            df["Date"] = pd.to_datetime(df["Date"])
        except Exception as e:
            raise ValueError(f"Could not convert 'Date' column to datetime for query '{query_name}': {e}")

        df = df.sort_values(by='Date')

        logger.info(f"Successfully loaded and processed data from DB. Resulting DataFrame has {len(df)} rows.")
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