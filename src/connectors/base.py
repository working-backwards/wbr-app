from abc import ABC, abstractmethod

import pandas as pd


class BaseConnector(ABC):
    """
    Abstract base class for database connectors.
    Each connector should be able to execute a query and return a pandas DataFrame.
    """

    def __init__(self, config: dict):
        """
        Initialize the connector with its configuration.

        Args:
            config (dict): Database-specific connection parameters.
        """
        self.config = config

    @abstractmethod
    def connect(self):
        """
        Establish a connection to the database.
        This method should store the connection object in an instance variable.
        """
        pass

    @abstractmethod
    def disconnect(self):
        """
        Close the database connection.
        """
        pass

    @abstractmethod
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return the results as a pandas DataFrame.
        The first column of the query result must be the date column, aliased as "Date".

        Args:
            query (str): The SQL query to execute.

        Returns:
            pd.DataFrame: A DataFrame containing the query results.
        """
        pass

    def __enter__(self):
        """
        Context management entry point.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context management exit point. Ensures disconnection.
        """
        self.disconnect()

    def _validate_and_parse_date_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validates that a "Date" column exists and ensures it's parsed as datetime.
        This replaces the old `_rename_date_column` method.

        Args:
            df (pd.DataFrame): The input DataFrame from a query result.

        Returns:
            pd.DataFrame: The DataFrame with the "Date" column validated and parsed.

        Raises:
            ValueError: If the "Date" column is not found or cannot be parsed.
        """
        if "Date" not in df.columns:
            raise ValueError(
                f"Query results must include a 'Date' column. Please alias your date column as \"Date\". Available columns: {df.columns.tolist()}"
            )

        # Attempt to convert to datetime, handling potential errors
        try:
            df["Date"] = pd.to_datetime(df["Date"])
        except Exception as e:
            raise ValueError(f"Could not convert 'Date' column to datetime: {e}")

        return df
