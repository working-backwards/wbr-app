from .base import BaseConnector
from .postgres import PostgresConnector
from .snowflake import SnowflakeConnector
from .athena import AthenaConnector
from .redshift import RedshiftConnector

import logging
logger = logging.getLogger(__name__)

_CONNECTOR_MAP = {
    "postgres": PostgresConnector,
    "snowflake": SnowflakeConnector,
    "athena": AthenaConnector,
    "redshift": RedshiftConnector,
}

def get_connector(connection_type: str, config: dict) -> BaseConnector:
    """
    Factory function to get a database connector instance.

    Args:
        connection_type (str): The type of database connection
                               (e.g., "postgres", "snowflake").
        config (dict): The configuration dictionary for the connector.

    Returns:
        BaseConnector: An instance of the appropriate connector.

    Raises:
        ValueError: If the connection_type is not supported.
    """
    connector_class = _CONNECTOR_MAP.get(connection_type.lower())
    if not connector_class:
        logger.error(f"Unsupported database connection type: {connection_type}")
        raise ValueError(f"Unsupported database connection type: {connection_type}. Supported types are: {list(_CONNECTOR_MAP.keys())}")

    logger.info(f"Creating connector of type: {connection_type}")
    return connector_class(config)

__all__ = [
    "BaseConnector",
    "PostgresConnector",
    "SnowflakeConnector",
    "AthenaConnector",
    "RedshiftConnector",
    "get_connector",
]
