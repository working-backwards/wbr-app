import logging

import pandas as pd
import snowflake.connector

from .base import BaseConnector

logger = logging.getLogger(__name__)


class SnowflakeConnector(BaseConnector):
    """
    Connector for Snowflake data warehouse.
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.connection = None

    def connect(self):
        """
        Establishes a connection to Snowflake.
        """
        try:
            self.connection = snowflake.connector.connect(
                user=self.config.get("user"),
                password=self.config.get("password"),
                account=self.config.get("account"),
                warehouse=self.config.get("warehouse"),
                database=self.config.get("database"),
                schema=self.config.get("schema"),
                role=self.config.get("role"),
                # You can add other snowflake-connector-python specific parameters here
            )
            logger.info(
                f"Successfully connected to Snowflake account: {self.config.get('account')}, database: {self.config.get('database')}"
            )
        except snowflake.connector.errors.Error as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            raise ConnectionError(f"Could not connect to Snowflake: {e}")

    def disconnect(self):
        """
        Closes the Snowflake connection.
        """
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info(
                f"Disconnected from Snowflake account: {self.config.get('account')}, database: {self.config.get('database')}"
            )

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Executes a SQL query on Snowflake and returns the results as a pandas DataFrame.
        Relies on the query to alias the date column as "Date".

        Args:
            query (str): The SQL query to execute.

        Returns:
            pd.DataFrame: A DataFrame containing the query results.
        """
        if not self.connection or self.connection.is_closed():
            self.connect()

        cursor = None
        try:
            cursor = self.connection.cursor()
            logger.debug(f"Executing query on Snowflake: {query}")
            cursor.execute(query)

            df = cursor.fetch_pandas_all()

            # Snowflake column names are case-sensitive and preserved by fetch_pandas_all().
            # Users are instructed to alias their date column as "Date".
            # We will check for "Date" and if not present, check for its uppercase version "DATE"
            # for robustness, before passing to the validator.
            if "Date" not in df.columns and "DATE" in df.columns:
                df = df.rename(columns={"DATE": "Date"})

            # The base method now expects "Date" (case-sensitive).
            df = self._validate_and_parse_date_column(df)

            logger.info(f"Successfully executed query on Snowflake. Fetched {len(df)} rows.")
            return df
        except snowflake.connector.errors.Error as e:
            logger.error(f"Error executing query on Snowflake: {e}\nQuery: {query}")
            raise RuntimeError(f"Could not execute query on Snowflake: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during query execution on Snowflake: {e}\nQuery: {query}")
            raise
        finally:
            if cursor:
                cursor.close()


# Example Usage (for testing purposes)
if __name__ == "__main__":
    # This is a placeholder for actual testing, requires a running Snowflake instance
    # and appropriate configuration.
    #
    # config_sf = {
    #     "user": "your_sf_user",
    #     "password": "your_sf_password",
    #     "account": "your_sf_account_identifier", # e.g., xy12345.us-east-1
    #     "warehouse": "COMPUTE_WH",
    #     "database": "YOUR_DB",
    #     "schema": "PUBLIC"
    # }
    #
    # # Note: Snowflake column names are typically uppercase. Query accordingly.
    # query_example_sf = "SELECT TO_DATE('2023-01-01') AS EVENT_DAY, 100 AS VALUE UNION ALL SELECT TO_DATE('2023-01-02'), 150;"
    #
    # try:
    #     with SnowflakeConnector(config_sf) as sf_conn:
    #         # Pass the original uppercase column name as date_column
    #         df_results_sf = sf_conn.execute_query(query_example_sf, date_column="EVENT_DAY")
    #         print("\nSnowflake Query Results:")
    #         print(df_results_sf)
    #         print(df_results_sf.info())
    # except Exception as e:
    #     print(f"Snowflake Error: {e}")
    pass
