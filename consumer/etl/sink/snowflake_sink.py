
# consumer/etl/sink/snowflake_sink.py

import json
import time
from typing import Dict, List, Optional, Sequence, Tuple, Union

try:
    import snowflake.connector
    from snowflake.connector import SnowflakeConnection
    from snowflake.connector.errors import Error as SnowflakeError
except ImportError:
    # Allow module import for environments where the connector isn't installed yet.
    SnowflakeConnection = object
    class SnowflakeError(Exception):
        pass


Record = Dict[str, object]
Records = List[Record]


class SnowflakeSink:
    """
    Snowflake Sink responsibilities:
    - Initialize a Snowflake connection using configuration
    - Provide a write(table: str, record: dict) method to insert records
    - Support batching for performance (optional/bonus)
    - Provide retry logic for transient failures (optional/bonus)
    - Should not hardcode SQL table columns; use either a JSON column or
      a configurable column mapping in config.yaml

    Modes:
      - 'json' (default): Insert the whole record as JSON into a VARIANT column.
                          Requires `json_column` in ctor (default: 'payload').
      - 'mapping': Insert using a configured mapping {record_key -> table_column}.
                   Provide `column_mapping` in ctor. Missing keys become NULL.

    Batching:
      - If `batch_size` > 1, write(records) accumulates and auto-flushes.
      - Call `flush()` or use context manager to ensure pending writes are committed.

    Retry:
      - Simple exponential backoff. Automatically reconnects on failure.
    """

    def __init__(
        self,
        connection_config: dict,
        *,
        mode: str = "json",
        json_column: str = "payload",
        column_mapping: Optional[Dict[str, str]] = None,
        batch_size: int = 1,
        max_retries: int = 3,
        backoff_initial: float = 0.5,
        backoff_max: float = 8.0,
        autocommit: bool = True,
    ):
        """
        connection_config examples:
        {
          "user": "...",
          "password": "...",
          "account": "...",
          "warehouse": "...",
          "database": "...",
          "schema": "...",
          # Optionally:
          "role": "...",
          "client_session_keep_alive": True
        }
        Initialize the connector here.
        """
        if mode not in {"json", "mapping"}:
            raise ValueError("mode must be 'json' or 'mapping'")

        if mode == "json" and not json_column:
            raise ValueError("json mode requires json_column name")

        if mode == "mapping" and not column_mapping:
            raise ValueError("mapping mode requires column_mapping dict")

        self.mode = mode
        self.json_column = json_column
        self.column_mapping = column_mapping or {}

        self.batch_size = max(1, int(batch_size))
        self.max_retries = max(0, int(max_retries))
        self.backoff_initial = float(backoff_initial)
        self.backoff_max = float(backoff_max)
        self.autocommit = autocommit

        self._conn: Optional[SnowflakeConnection] = None
