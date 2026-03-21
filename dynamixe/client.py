from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Type, TypedDict

import boto3

from .expressions import Expression
from .types import deserialize, normalize_expression, serialize, to_dict

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient as Boto3DynamoDBClient
else:
    Boto3DynamoDBClient = Any


class ConfigDict(TypedDict, total=False):
    """Configuration for DynamoDB mapping."""

    table: str
    partition_key: str | None
    sort_key: str | None


def _get_dynamodb_config(obj: Any) -> ConfigDict | None:
    """Extract DynamoDB config from model_config or __dynamodb_config__."""
    model_config = getattr(obj, 'model_config', None)

    if model_config and isinstance(model_config, dict) and 'table' in model_config:
        return ConfigDict(
            table=model_config.get('table', ''),
            partition_key=model_config.get('partition_key'),
            sort_key=model_config.get('sort_key'),
        )

    return getattr(obj, '__dynamodb_config__', None)


class DynamoDBClient:
    """Table-scoped DynamoDB client."""

    def __init__(
        self,
        table_name: str,
        *,
        client: Boto3DynamoDBClient | None = None,
        **client_kwargs: Any,
    ) -> None:
        self._table_name = table_name
        self._client = client or boto3.client('dynamodb', **client_kwargs)

    def get_item(
        self,
        key: dict,
        *,
        table_name: str | None = None,
        expr_attr_names: dict | None = None,
        projection_expr: str | None = None,
        raise_on_error: bool = True,
        exc_cls: Type[Exception] = Exception,
        default: Any = None,
    ) -> dict | Any:
        """Get a single item by primary key."""
        attrs: dict[str, Any] = {
            'TableName': table_name or self._table_name,
            'Key': serialize(key),
        }

        if expr_attr_names:
            attrs['ExpressionAttributeNames'] = expr_attr_names

        if projection_expr:
            attrs['ProjectionExpression'] = projection_expr

        output = self._client.get_item(**attrs)
        item = deserialize(output.get('Item', {}))

        if raise_on_error and not item:
            raise exc_cls(f'Item not found ({key!r})')

        return item or default

    def put_item(
        self,
        item: Any,
        *,
        cond_expr: str | Expression | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        table_name: str | None = None,
        return_values: Any | None = None,
        return_on_cond_fail: Any | None = None,
    ) -> Any:
        """Put an item with optional condition expression."""

        config = _get_dynamodb_config(item.__class__)
        data = to_dict(item) if config else item

        if not isinstance(data, Mapping):
            raise TypeError('item must be a mapping or model that serializes to one')

        attrs: dict[str, Any] = {
            'TableName': table_name or self._table_name,
            'Item': serialize(data),
        }
        condition_expr, merged_names, merged_values = normalize_expression(
            cond_expr, expr_attr_names, expr_attr_values
        )

        if condition_expr:
            attrs['ConditionExpression'] = condition_expr

        if merged_names:
            attrs['ExpressionAttributeNames'] = merged_names

        if merged_values:
            attrs['ExpressionAttributeValues'] = merged_values

        if return_values:
            attrs['ReturnValues'] = return_values

        if return_on_cond_fail:
            attrs['ReturnValuesOnConditionCheckFailure'] = return_on_cond_fail

        return self._client.put_item(**attrs)

    def update_item(
        self,
        key: dict,
        *,
        update_expr: str,
        cond_expr: str | Expression | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        table_name: str | None = None,
        return_values: Any | None = None,
        return_on_cond_fail: Any | None = None,
    ) -> dict | None:
        """Update an item with update expression."""
        attrs: dict[str, Any] = {
            'TableName': table_name or self._table_name,
            'Key': serialize(key),
            'UpdateExpression': update_expr,
        }

        condition_expr, merged_names, merged_values = normalize_expression(
            cond_expr, expr_attr_names, expr_attr_values
        )

        if condition_expr:
            attrs['ConditionExpression'] = condition_expr

        if merged_names:
            attrs['ExpressionAttributeNames'] = merged_names

        if merged_values:
            attrs['ExpressionAttributeValues'] = merged_values

        if return_values:
            attrs['ReturnValues'] = return_values

        if return_on_cond_fail:
            attrs['ReturnValuesOnConditionCheckFailure'] = return_on_cond_fail

        output = self._client.update_item(**attrs)
        if return_values:
            return deserialize(output.get('Attributes', {}))
        return None

    def delete_item(
        self,
        key: dict,
        *,
        cond_expr: str | Expression | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        table_name: str | None = None,
        return_values: Any | None = None,
        return_on_cond_fail: Any | None = None,
    ) -> None:
        """Delete an item with optional condition expression."""
        attrs: dict[str, Any] = {
            'TableName': table_name or self._table_name,
            'Key': serialize(key),
        }

        condition_expr, merged_names, merged_values = normalize_expression(
            cond_expr, expr_attr_names, expr_attr_values
        )

        if condition_expr:
            attrs['ConditionExpression'] = condition_expr

        if merged_names:
            attrs['ExpressionAttributeNames'] = merged_names

        if merged_values:
            attrs['ExpressionAttributeValues'] = merged_values

        if return_values:
            attrs['ReturnValues'] = return_values

        if return_on_cond_fail:
            attrs['ReturnValuesOnConditionCheckFailure'] = return_on_cond_fail

        self._client.delete_item(**attrs)

    def scan(
        self,
        *,
        filter_expr: str | Expression | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        projection_expr: str | None = None,
        limit: int | None = None,
        exclusive_start_key: str | dict | None = None,
        table_name: str | None = None,
    ) -> list[dict]:
        """Scan items with optional filter expression."""
        filter_condition_expr, filter_names, filter_values = normalize_expression(
            filter_expr, expr_attr_names, expr_attr_values
        )

        attrs: dict[str, Any] = {
            'TableName': table_name or self._table_name,
        }

        if limit:
            attrs['Limit'] = limit

        if filter_names:
            attrs['ExpressionAttributeNames'] = filter_names

        if filter_values:
            attrs['ExpressionAttributeValues'] = filter_values

        if exclusive_start_key:
            if isinstance(exclusive_start_key, str):
                attrs['ExclusiveStartKey'] = _startkey_b64decode(exclusive_start_key)
            else:
                attrs['ExclusiveStartKey'] = exclusive_start_key

        if filter_condition_expr:
            attrs['FilterExpression'] = filter_condition_expr

        if projection_expr:
            attrs['ProjectionExpression'] = projection_expr

        output = self._client.scan(**attrs)
        return [deserialize(item) for item in output.get('Items', [])]

    def query(
        self,
        key_expr: str | Expression,
        *,
        select: str | None = None,
        filter_expr: str | Expression | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        projection_expr: str | None = None,
        limit: int | None = None,
        scan_index_forward: bool = True,
        exclusive_start_key: str | dict | None = None,
        table_name: str | None = None,
    ) -> list[dict]:
        """Query items with key condition expression."""
        from .types import deserialize

        key_condition_expr, key_names, key_values = normalize_expression(
            key_expr, expr_attr_names, expr_attr_values
        )
        filter_condition_expr, filter_names, filter_values = normalize_expression(
            filter_expr, key_names, key_values
        )

        attrs: dict[str, Any] = {
            'TableName': table_name or self._table_name,
            'KeyConditionExpression': key_condition_expr,
            'ScanIndexForward': scan_index_forward,
        }
        if select:
            attrs['Select'] = select

        if limit:
            attrs['Limit'] = limit

        if filter_names:
            attrs['ExpressionAttributeNames'] = filter_names

        if filter_values:
            attrs['ExpressionAttributeValues'] = filter_values

        if exclusive_start_key:
            if isinstance(exclusive_start_key, str):
                attrs['ExclusiveStartKey'] = _startkey_b64decode(exclusive_start_key)
            else:
                attrs['ExclusiveStartKey'] = exclusive_start_key

        if filter_condition_expr:
            attrs['FilterExpression'] = filter_condition_expr

        if projection_expr:
            attrs['ProjectionExpression'] = projection_expr

        output = self._client.query(**attrs)
        return [deserialize(item) for item in output.get('Items', [])]

    def transact_get(self) -> TransactGet:
        """Create a transactional read operations client."""
        from .transact_get import TransactGet
        return TransactGet(self._table_name, self._client)


def _startkey_b64decode(key: str) -> dict:
    import base64
    import json
    return json.loads(base64.b64decode(key).decode('utf-8'))
