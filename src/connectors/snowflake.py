import pandas as pd
import snowflake.connector
from .base import BaseConnector
import logging

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
            logger.info(f"Successfully connected to Snowflake account: {self.config.get('account')}, database: {self.config.get('database')}")
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
            logger.info(f"Disconnected from Snowflake account: {self.config.get('account')}, database: {self.config.get('database')}")

    def execute_query(self, query: str, date_column: str = "Date") -> pd.DataFrame:
        """
        Executes a SQL query on Snowflake and returns the results as a pandas DataFrame.

        Args:
            query (str): The SQL query to execute.
            date_column (str): The name of the column in the query result that represents the date.
                               This column will be renamed to "Date" and parsed as datetime.

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

            # Fetch results into a pandas DataFrame
            # Snowflake connector's fetch_pandas_all() is efficient for this
            df = cursor.fetch_pandas_all()

            # Column names in Snowflake are often uppercase by default.
            # Standardize them to lowercase for easier handling, matching pandas default.
            df.columns = [col.lower() for col in df.columns]

            # Rename and parse the specified date column
            # Adjust date_column to lowercase as well, as df columns were lowercased
            effective_date_column = date_column.lower()

            if date_column and effective_date_column in df.columns:
                df = self._rename_date_column(df, date_column_name=effective_date_column) # Pass lowercased name
            elif date_column and effective_date_column not in df.columns:
                logger.warning(f"Specified date_column '{date_column}' (as '{effective_date_column}') not found in query results. Columns are: {df.columns.tolist()}")

            logger.info(f"Successfully executed query on Snowflake. Fetched {len(df)} rows.")
            return df
        except snowflake.connector.errors.Error as e:
            logger.error(f"Error executing query on Snowflake: {e}\nQuery: {query}")
            # Decide on rollback if applicable, though Snowflake handles transactions differently
            raise RuntimeError(f"Could not execute query on Snowflake: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during query execution on Snowflake: {e}\nQuery: {query}")
            raise
        finally:
            if cursor:
                cursor.close()

    def _rename_date_column(self, df: pd.DataFrame, date_column_name: str, desired_date_column: str = "Date") -> pd.DataFrame:
        """
        Renames the specified date column to the desired name (default 'Date')
        and ensures it's parsed as datetime.
        Overrides base method to ensure desired_date_column is also case-standardized if necessary,
        though the base implementation should handle this if df columns are already standardized.
        Here, we ensure the output 'Date' column is capitalized as per convention for 'daily_df'.
        """
        # Standardize to lowercase first as per Snowflake convention, then rename to desired capitalized 'Date'
        df_renamed_lower = super()._rename_date_column(df, date_column_name.lower(), desired_date_column.lower())

        # Ensure the final 'Date' column is capitalized
        if desired_date_column.lower() in df_renamed_lower.columns and desired_date_column.lower() != desired_date_column:
             df_renamed_lower = df_renamed_lower.rename(columns={desired_date_column.lower(): desired_date_column})
        elif desired_date_column not in df_renamed_lower.columns and desired_date_column.lower() in df_renamed_lower.columns:
             # This case might happen if desired_date_column was already lowercase
             df_renamed_lower = df_renamed_lower.rename(columns={desired_date_column.lower(): desired_date_column})


        return df_renamed_lower

# Example Usage (for testing purposes)
if __name__ == '__main__':
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
