from __future__ import annotations

from datetime import date, datetime
from ipaddress import IPv4Address
from typing import Any, Mapping
from uuid import UUID

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

serializer = TypeSerializer()
deserializer = TypeDeserializer()


def _serialize_to_basic_types(data: Any) -> str | dict | set | list:
    match data:
        case datetime() | date():
            return data.isoformat()
        case UUID() | IPv4Address():
            return str(data)
        case tuple() | list():
            serialized = [_serialize_to_basic_types(v) for v in data]
            if any(isinstance(v, (dict, list)) for v in serialized):
                return serialized
            try:
                return set(serialized)
            except TypeError:
                return serialized
        case set():
            return set(_serialize_to_basic_types(v) for v in data)
        case dict():
            return {k: _serialize_to_basic_types(v) for k, v in data.items()}
        case _:
            return data


def serialize(data: Mapping[str, Any], exclude_none: bool = False) -> dict:
    return {
        k: serializer.serialize(_serialize_to_basic_types(v))
        for k, v in data.items()
        if not exclude_none or v is not None
    }


def deserialize(data: Mapping[str, Any]) -> dict:
    return {k: deserializer.deserialize(v) for k, v in data.items()}


def to_dict(data: Any | None) -> dict[str, Any] | None:
    if data is None:
        return None

    # Convert from Pydantic v2 model
    if hasattr(data, 'model_dump') and callable(data.model_dump):
        return data.model_dump()  # type: ignore[no-any-return]

    # Convert from dataclass
    if hasattr(data, '__dataclass_fields__'):
        import dataclasses

        return dataclasses.asdict(data)

    # Return plain dict as-is
    if isinstance(data, dict):
        return data

    raise TypeError('Unable to convert object to dict')
