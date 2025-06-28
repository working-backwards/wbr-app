import pandas as pd
from abc import ABC, abstractmethod

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

    def _rename_date_column(self, df: pd.DataFrame, date_column_name: str, desired_date_column: str = "Date") -> pd.DataFrame:
        """
        Renames the specified date column to the desired name (default 'Date')
        and ensures it's parsed as datetime.

        Args:
            df (pd.DataFrame): The input DataFrame.
            date_column_name (str): The original name of the date column in the query result.
            desired_date_column (str): The name the date column should have.

        Returns:
            pd.DataFrame: The DataFrame with the date column renamed and parsed.

        Raises:
            ValueError: If the specified date_column_name is not found in the DataFrame.
        """
        if date_column_name not in df.columns:
            raise ValueError(f"Specified date column '{date_column_name}' not found in query results. Available columns: {df.columns.tolist()}")

        if date_column_name != desired_date_column:
            df = df.rename(columns={date_column_name: desired_date_column})

        # Attempt to convert to datetime, handling potential errors
        try:
            df[desired_date_column] = pd.to_datetime(df[desired_date_column])
        except Exception as e:
            raise ValueError(f"Could not convert column '{desired_date_column}' to datetime: {e}")

        return df
