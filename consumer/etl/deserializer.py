
# consumer/etl/deserializer.py

from __future__ import annotations

import importlib
from typing import Any, Dict, Optional, Type, Union

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message
from google.protobuf.message import DecodeError


class UnknownTopicError(Exception):
    """Raised when the topic is not present in the schema registry."""
    pass


class DeserializationError(Exception):
    """Raised when raw bytes cannot be parsed into a protobuf message."""
    pass


class PoisonMessageError(Exception):
    """Raised when a message is identified as a poison pill."""
    pass


class ProtobufDeserializer:
    """
    Responsibilities:
    - Maintain a mapping (schema registry) from topic -> Python Protobuf class.
    - Given (topic, raw_bytes) return a Python dict representing the message.
      e.g., {"field1": "value", "business_unit": "BU1", ...}
    - Detect poison messages or decode failures and surface them as controlled exceptions
      so the pipeline can either send to DLQ or skip.
    - Should be generic: adding a new topic + proto should not require code changes,
      only updating the mapping (config).

    Notes:
    - `schema_registry` can contain either actual protobuf classes or dotted import strings,
      e.g. "producer.generated.business_event_pb2.BusinessEvent".
    - This class does NOT depend on Kafka; it only handles bytes -> dict conversion.
    """

    def __init__(self, schema_registry: Dict[str, Union[str, Type[Message]]]):
        """
        Initialize the deserializer with a config-driven schema registry.

        Args:
            schema_registry: dict mapping topic -> protobuf class or dotted string
            Example:
            {
                "business-events": business_event_pb2.BusinessEvent,
                "customer-events": "producer.generated.customer_event_pb2.CustomerEvent",
                ...
            }

        Raises:
            ValueError: if any registry entry cannot be resolved to a protobuf Message subclass.
        """
        self._registry: Dict[str, Type[Message]] = {}

        for topic, cls_or_path in schema_registry.items():
            resolved = self._resolve_protobuf_class(cls_or_path)
            if not isinstance(resolved, type) or not issubclass(resolved, Message):
                raise ValueError(
                    f"Registry value for topic '{topic}' is not a protobuf Message class: {resolved}"
                )
            self._registry[topic] = resolved

    def deserialize(self, topic: str, raw_bytes: bytes) -> Dict[str, Any]:
        """
        Convert raw Kafka bytes into a Python dict using the mapped protobuf schema.

        Steps:
        - lookup the protobuf class for the topic
        - parse raw_bytes into a protobuf message instance
        - detect poison messages (by schema name)
        - convert message -> dict (JSON-like) with proto field names preserved
        - return dict on success

        Args:
            topic: Kafka topic name
            raw_bytes: serialized protobuf payload

        Returns:
            dict: JSON-like representation of the protobuf message

        Raises:
            UnknownTopicError: if the topic is not in the registry
            DeserializationError: if ParseFromString fails
            PoisonMessageError: if the topic maps to a poison/pill schema
        """
        pb_class = self._registry.get(topic)
        if pb_class is None:
            raise UnknownTopicError(f"Topic '{topic}' not found in schema registry.")

        # If this topic is known to be poison, raise immediately
        # (You can choose to make this behavior configurable if needed.)
        class_name_lower = pb_class.__name__.lower()
        if class_name_lower.endswith("poisonevent") or "poison" in class_name_lower:
            raise PoisonMessageError(f"Poison schema mapped for topic '{topic}'.")

        msg = pb_class()
        try:
            msg.ParseFromString(raw_bytes)
        except DecodeError as e:
            # protobuf-specific decode error
            raise DeserializationError(f"Failed to decode message for topic '{topic}': {e}") from e
        except Exception as e:
            # any other parsing-related issue
            raise DeserializationError(f"Unexpected deserialization error on topic '{topic}': {e}") from e

        # Convert to dict preserving proto field names (snake_case from .proto)
        record: Dict[str, Any] = MessageToDict(
            msg,
            preserving_proto_field_name=True,
            including_default_value_fields=False  # toggle if you need defaults included
        )

        # Optional heuristic: if a message looks like PoisonEvent even if the topic/class wasn't named that.
        # This keeps behavior robust if poison pills are sent on normal topics.
        # (Adjust/remove if you prefer strict schema-name-only detection.)
        descriptor_name = getattr(msg, "DESCRIPTOR", None).name.lower() if hasattr(msg, "DESCRIPTOR") else ""
        if "poison" in descriptor_name and "reason" in record and len(record) <= 1:
            raise PoisonMessageError(f"Poison-like payload detected on topic '{topic}'.")

        return record

    @staticmethod
    def _resolve_protobuf_class(cls_or_path: Union[str, Type[Message]]) -> Type[Message]:
        """
        Resolve either an already-imported protobuf class or a dotted path string.

        Args:
            cls_or_path: protobuf Message subclass or dotted import string

        Returns:
            Type[Message]: resolved protobuf Message class

        Raises:
            ValueError: if the dotted path cannot be imported or the attribute is missing
        """
        if isinstance(cls_or_path, str):
            # Expect "package.module.ClassName"
            try:
                module_path, class_name = cls_or_path.rsplit(".", 1)
            except ValueError as e:
                raise ValueError(
                    f"Invalid schema path '{cls_or_path}'. Expected 'module.ClassName'."
                ) from e

            try:
                module = importlib.import_module(module_path)
            except ImportError as e:
                raise ValueError(f"Cannot import module '{module_path}' for '{cls_or_path}'.") from e

            try:
                cls = getattr(module, class_name)
            except AttributeError as e:
                raise ValueError(
                    f"Class '{class_name}' not found in module '{module_path}' for '{cls_or_path}'."
                ) from e

            return cls

        # Already a class
        return cls_or_path
