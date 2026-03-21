from __future__ import annotations

from datetime import date, datetime
from ipaddress import IPv4Address
from typing import TYPE_CHECKING, Any, Mapping
from uuid import UUID

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

if TYPE_CHECKING:
    from .expressions import Expression

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

    if hasattr(data, 'model_dump') and callable(data.model_dump):
        dumped = data.model_dump()
        if isinstance(dumped, dict):
            return {str(k): v for k, v in dumped.items()}

        raise TypeError('model_dump() must return a dict')

    if hasattr(data, '__dataclass_fields__'):
        import dataclasses

        return dataclasses.asdict(data)

    if isinstance(data, dict):
        return data

    raise TypeError('Unable to convert object to dict')


def _is_ddb_attribute_value(value: Any) -> bool:
    """Check if value is already in DynamoDB attribute value format."""
    if not isinstance(value, dict) or len(value) != 1:
        return False
    ddb_attr_types = {
        'S', 'N', 'B', 'SS', 'NS', 'BS', 'M', 'L', 'NULL', 'BOOL'
    }
    return next(iter(value.keys())) in ddb_attr_types


def normalize_expression(
    expr: str | Expression | None,
    expr_attr_names: dict | None = None,
    expr_attr_values: dict | None = None,
) -> tuple[str | None, dict | None, dict | None]:
    """Extract (expr_string, names, values) from Expression or string."""
    names = dict(expr_attr_names or {})
    values = dict(expr_attr_values or {})

    # Import here to avoid circular dependency
    from .expressions import Expression

    if isinstance(expr, Expression):
        names.update(expr.names or {})
        values.update(expr.values or {})
        expr = expr.expr

    # Serialize values, but skip already-serialized DDB values
    if values:
        serialized_values = {
            k: v for k, v in values.items() if _is_ddb_attribute_value(v)
        }
        raw_values = {
            k: v for k, v in values.items() if not _is_ddb_attribute_value(v)
        }
        if raw_values:
            serialized_values.update(serialize(raw_values))
        values = serialized_values

    return expr or None, names or None, values or None
