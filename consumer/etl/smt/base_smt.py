
# consumer/etl/smt/base_smt.py

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any


class BaseSMT(ABC):
    """
    Abstract Base Class (ABC) for Single Message Transforms (SMTs).

    Purpose:
    - Define a common interface for transformations applied to individual records.
    - Each SMT receives a single message (as a Python dict) and either:
        - returns a transformed dict to pass downstream, or
        - returns None to drop the record.

    Contract:
    - Subclasses must implement `apply(record: dict) -> dict | None`.

    Notes:
    - Keep SMTs stateless when possible; if configuration is needed, pass it via __init__.
    - SMTs should be small, composable, and predictable.
    """

    @abstractmethod
    def apply(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Apply the transform to the given record.

        Args:
            record: dict representation of the message (already deserialized from Protobuf)

        Returns:
            - dict: transformed record to be passed downstream
            - None: to drop the record from the pipeline

        Raises:
            - Optionally, a custom exception if the SMT cannot process the record.
              (e.g., TransformationError), but returning None for "filter/drop" cases
              is the usual pattern.
        """
        raise NotImplementedError("Subclasses must implement 'apply()'.")
