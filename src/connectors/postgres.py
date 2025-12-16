import logging

import pandas as pd
import psycopg2
from psycopg2 import sql

from .base import BaseConnector

logger = logging.getLogger(__name__)


class PostgresConnector(BaseConnector):
    """
    Connector for PostgreSQL databases.
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.connection = None
        self.cursor = None

    def connect(self):
        """
        Establishes a connection to the PostgreSQL database.
        """
        try:
            self.connection = psycopg2.connect(
                host=self.config.get("host"),
                port=self.config.get("port", 5432),
                user=self.config.get("username"),
                password=self.config.get("password"),
                dbname=self.config.get("database"),
                # You can add other psycopg2 specific parameters here from self.config if needed
                # e.g., sslmode=self.config.get("sslmode")
            )
            logger.info(
                f"Successfully connected to PostgreSQL database: {self.config.get('database')} at {self.config.get('host')}")
        except psycopg2.Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise ConnectionError(f"Could not connect to PostgreSQL: {e}")

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
            logger.info(
                f"Disconnected from PostgreSQL database: {self.config.get('database')} at {self.config.get('host')}")

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Executes a SQL query and returns the results as a pandas DataFrame.
        Relies on the query to alias the date column as "Date".

        Args:
            query (str): The SQL query to execute.

        Returns:
            pd.DataFrame: A DataFrame containing the query results.
        """
        if not self.connection:
            self.connect()

        try:
            self.cursor = self.connection.cursor()
            logger.debug(f"Executing query on PostgreSQL: {query}")
            self.cursor.execute(sql.SQL(query))

            colnames = [desc[0] for desc in self.cursor.description]
            results = self.cursor.fetchall()
            df = pd.DataFrame(results, columns=colnames)

            self.connection.commit()

            # Validate and parse the "Date" column
            df = self._validate_and_parse_date_column(df)

            logger.info(f"Successfully executed query on PostgreSQL. Fetched {len(df)} rows.")
            return df
        except psycopg2.Error as e:
            logger.error(f"Error executing query on PostgreSQL: {e}\nQuery: {query}")
            if self.connection:
                self.connection.rollback()
            raise RuntimeError(f"Could not execute query on PostgreSQL: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during query execution on PostgreSQL: {e}\nQuery: {query}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            if self.cursor:
                self.cursor.close()
                self.cursor = None


# Example Usage (for testing purposes, typically not here)
if __name__ == '__main__':
    # This is a placeholder for actual testing, requires a running Postgres instance
    # and appropriate configuration.
    #
    # config_pg = {
    #     "host": "localhost",
    #     "port": 5432,
    #     "username": "your_user",
    #     "password": "your_password",
    #     "database": "your_db"
    # }
    #
    # query_example = "SELECT '2023-01-01'::date as event_day, 100 as value UNION ALL SELECT '2023-01-02'::date, 150 as value;"
    #
    # try:
    #     with PostgresConnector(config_pg) as pg_conn:
    #         df_results = pg_conn.execute_query(query_example, date_column="event_day")
    #         print("PostgreSQL Query Results:")
    #         print(df_results)
    #         print(df_results.info())
    # except Exception as e:
    #     print(f"Error: {e}")
    pass
