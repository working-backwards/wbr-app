import pandas as pd
import psycopg2 # Using psycopg2 for Redshift, as it's largely compatible
                # Alternatively, 'redshift_connector' could be used for specific features
from psycopg2 import sql
from .base import BaseConnector
import logging

logger = logging.getLogger(__name__)

class RedshiftConnector(BaseConnector):
    """
    Connector for Amazon Redshift.
    Uses psycopg2, similar to PostgreSQL.
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.connection = None
        self.cursor = None

    def connect(self):
        """
        Establishes a connection to the Redshift database.
        """
        try:
            self.connection = psycopg2.connect(
                host=self.config.get("host"),
                port=self.config.get("port", 5439), # Default Redshift port
                user=self.config.get("username"),
                password=self.config.get("password"),
                dbname=self.config.get("database"),
                # Redshift often requires SSL
                # sslmode=self.config.get("sslmode", "require"),
                # connect_timeout=self.config.get("connect_timeout", 10)
            )
            logger.info(f"Successfully connected to Redshift database: {self.config.get('database')} at {self.config.get('host')}")
        except psycopg2.Error as e:
            logger.error(f"Error connecting to Redshift: {e}")
            raise ConnectionError(f"Could not connect to Redshift: {e}")

    def disconnect(self):
        """
        Closes the database connection.
        """
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info(f"Disconnected from Redshift database: {self.config.get('database')} at {self.config.get('host')}")

    def execute_query(self, query: str, date_column: str = "Date") -> pd.DataFrame:
        """
        Executes a SQL query on Redshift and returns the results as a pandas DataFrame.

        Args:
            query (str): The SQL query to execute.
            date_column (str): The name of the column in the query result that represents the date.
                               This column will be renamed to "Date" and parsed as datetime.

        Returns:
            pd.DataFrame: A DataFrame containing the query results.
        """
        if not self.connection:
            self.connect()

        try:
            self.cursor = self.connection.cursor()
            logger.debug(f"Executing query on Redshift: {query}")
            # Redshift might have specific considerations for query syntax or performance.
            # Using sql.SQL for safety if parts of query could be dynamic in other contexts.
            self.cursor.execute(sql.SQL(query))

            colnames = [desc[0] for desc in self.cursor.description]
            results = self.cursor.fetchall()
            df = pd.DataFrame(results, columns=colnames)

            self.connection.commit() # Or rollback on error

            # Redshift column names are typically lowercase unless quoted.
            # Standardize them to lowercase for easier handling.
            df.columns = [col.lower() for col in df.columns]
            effective_date_column = date_column.lower()

            # Rename and parse the specified date column
            if date_column and effective_date_column in df.columns:
                df = self._rename_date_column(df, date_column_name=effective_date_column)
            elif date_column and effective_date_column not in df.columns:
                logger.warning(f"Specified date_column '{date_column}' (as '{effective_date_column}') not found in Redshift query results. Columns are: {df.columns.tolist()}")


            logger.info(f"Successfully executed query on Redshift. Fetched {len(df)} rows.")
            return df
        except psycopg2.Error as e:
            logger.error(f"Error executing query on Redshift: {e}\nQuery: {query}")
            if self.connection:
                self.connection.rollback()
            raise RuntimeError(f"Could not execute query on Redshift: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during query execution on Redshift: {e}\nQuery: {query}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            if self.cursor:
                self.cursor.close()
                self.cursor = None

    def _rename_date_column(self, df: pd.DataFrame, date_column_name: str, desired_date_column: str = "Date") -> pd.DataFrame:
        """
        Renames the specified date column to the desired name (default 'Date')
        and ensures it's parsed as datetime.
        Overrides base method to ensure desired_date_column is also case-standardized if necessary,
        and ensures the final 'Date' column is capitalized.
        """
        # Standardize to lowercase first, then rename to desired capitalized 'Date'
        df_renamed_lower = super()._rename_date_column(df, date_column_name.lower(), desired_date_column.lower())

        # Ensure the final 'Date' column is capitalized
        if desired_date_column.lower() in df_renamed_lower.columns and desired_date_column.lower() != desired_date_column:
             df_renamed_lower = df_renamed_lower.rename(columns={desired_date_column.lower(): desired_date_column})
        elif desired_date_column not in df_renamed_lower.columns and desired_date_column.lower() in df_renamed_lower.columns:
             df_renamed_lower = df_renamed_lower.rename(columns={desired_date_column.lower(): desired_date_column})

        return df_renamed_lower

# Example Usage (for testing purposes)
if __name__ == '__main__':
    # This is a placeholder for actual testing, requires a running Redshift instance
    # and appropriate configuration.
    #
    # config_rs = {
    #     "host": "your-redshift-cluster.xxxx.your-region.redshift.amazonaws.com",
    #     "port": 5439,
    #     "username": "your_rs_user",
    #     "password": "your_rs_password",
    #     "database": "your_rs_db"
    # }
    #
    # # Redshift column names are typically lowercase.
    # query_example_rs = "SELECT '2023-01-01'::date as event_day, 100 as value UNION ALL SELECT '2023-01-02'::date, 150 as value;"
    #
    # try:
    #     with RedshiftConnector(config_rs) as rs_conn:
    #         df_results_rs = rs_conn.execute_query(query_example_rs, date_column="event_day")
    #         print("\nRedshift Query Results:")
    #         print(df_results_rs)
    #         print(df_results_rs.info())
    # except Exception as e:
    #     print(f"Redshift Error: {e}")
    pass
