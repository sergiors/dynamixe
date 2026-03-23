from __future__ import annotations

import base64
import json
import urllib.parse as parse
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypedDict

import boto3
import jmespath

from .expressions import Expression, extract_expression
from .transact_get import TransactGet
from .transact_writer import TransactWriter
from .types import deserialize, serialize, to_dict

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient as Boto3DynamoDBClient
    from mypy_boto3_dynamodb.literals import (
        ReturnValuesOnConditionCheckFailureType,
        ReturnValueType,
        SelectType,
    )
else:
    Boto3DynamoDBClient = Any
    ReturnValuesOnConditionCheckFailureType = Any
    ReturnValueType = Any
    SelectType = Any


class ConfigDict(TypedDict, total=False):
    """Configuration for DynamoDB mapping."""

    table: str
    partition_key: str | None
    sort_key: str | None


class QueryOutput:
    def __init__(
        self,
        items: list[dict[str, Any]],
        count: int,
        last_key: str | None = None,
    ) -> None:
        self.items = items
        self.count = count
        self.last_key = last_key

    def __getitem__(self, key: str) -> list | int | str:
        if key not in {'items', 'count', 'last_key'}:
            raise KeyError(key)
        return getattr(self, key)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)

    def __len__(self) -> int:
        return len(self.items)

    def jmespath(self, expr: str) -> Any:
        """Apply JMESPath expression to result list.

        Args:
            expr: JMESPath expression (e.g., '[*].name', '[0]', '[?active == `true`]').

        Returns:
            Transformed result from JMESPath search.
            Returns raw JMESPath output (list, dict, scalar) without wrapping.
        """
        return jmespath.search(expr, self.items)


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
        exc_cls: type[Exception] = Exception,
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
        return_values: ReturnValueType | None = None,
        return_on_cond_fail: ReturnValuesOnConditionCheckFailureType | None = None,
    ) -> dict | None:
        """Put an item with optional condition expression."""

        config = _get_dynamodb_config(item.__class__)
        data = to_dict(item) if config else item

        if not isinstance(data, Mapping):
            raise TypeError('item must be a mapping or model that serializes to one')

        attrs: dict[str, Any] = {
            'TableName': table_name or self._table_name,
            'Item': serialize(data),
        }
        cond, names, values = extract_expression(
            cond_expr, expr_attr_names, expr_attr_values
        )

        if cond:
            attrs['ConditionExpression'] = cond

        if names:
            attrs['ExpressionAttributeNames'] = names

        if values:
            attrs['ExpressionAttributeValues'] = serialize(values)

        if return_values:
            attrs['ReturnValues'] = return_values

        if return_on_cond_fail:
            attrs['ReturnValuesOnConditionCheckFailure'] = return_on_cond_fail

        output = self._client.put_item(**attrs)

        if return_values:
            return deserialize(output.get('Attributes', {}))

        return None

    def update_item(
        self,
        key: dict,
        *,
        update_expr: str,
        cond_expr: str | Expression | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        table_name: str | None = None,
        return_values: ReturnValueType | None = None,
        return_on_cond_fail: ReturnValuesOnConditionCheckFailureType | None = None,
    ) -> dict | None:
        """Update an item with update expression."""
        attrs: dict[str, Any] = {
            'TableName': table_name or self._table_name,
            'Key': serialize(key),
            'UpdateExpression': update_expr,
        }

        cond, names, values = extract_expression(
            cond_expr, expr_attr_names, expr_attr_values
        )

        if cond:
            attrs['ConditionExpression'] = cond

        if names:
            attrs['ExpressionAttributeNames'] = names

        if values:
            attrs['ExpressionAttributeValues'] = serialize(values)

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
        return_values: ReturnValueType | None = None,
        return_on_cond_fail: ReturnValuesOnConditionCheckFailureType | None = None,
    ) -> dict | None:
        """Delete an item with optional condition expression."""
        attrs: dict[str, Any] = {
            'TableName': table_name or self._table_name,
            'Key': serialize(key),
        }

        cond, names, values = extract_expression(
            cond_expr, expr_attr_names, expr_attr_values
        )

        if cond:
            attrs['ConditionExpression'] = cond

        if names:
            attrs['ExpressionAttributeNames'] = names

        if values:
            attrs['ExpressionAttributeValues'] = serialize(values)

        if return_values:
            attrs['ReturnValues'] = return_values

        if return_on_cond_fail:
            attrs['ReturnValuesOnConditionCheckFailure'] = return_on_cond_fail

        output = self._client.delete_item(**attrs)

        if return_values:
            return deserialize(output.get('Attributes', {}))

        return None

    def scan(
        self,
        *,
        filter_expr: str | Expression | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        projection_expr: str | None = None,
        limit: int | None = None,
        exclusive_start_key: str | None = None,
        table_name: str | None = None,
    ) -> list[dict]:
        """Scan items with optional filter expression."""
        filter_cond, names, values = extract_expression(
            filter_expr, expr_attr_names, expr_attr_values
        )

        attrs: dict[str, Any] = {
            'TableName': table_name or self._table_name,
        }

        if limit:
            attrs['Limit'] = limit

        if names:
            attrs['ExpressionAttributeNames'] = names

        if values:
            attrs['ExpressionAttributeValues'] = serialize(values)

        if exclusive_start_key:
            attrs['ExclusiveStartKey'] = _startkey_b64decode(exclusive_start_key)

        if filter_cond:
            attrs['FilterExpression'] = filter_cond

        if projection_expr:
            attrs['ProjectionExpression'] = projection_expr

        output = self._client.scan(**attrs)

        return [deserialize(item) for item in output.get('Items', [])]

    def query(
        self,
        key_expr: str | Expression,
        *,
        select: SelectType | None = None,
        filter_expr: str | Expression | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        projection_expr: str | None = None,
        limit: int | None = None,
        scan_index_forward: bool = True,
        exclusive_start_key: str | None = None,
        table_name: str | None = None,
    ) -> QueryOutput:
        """Query items with key condition expression."""
        key_cond, key_names, key_values = extract_expression(
            key_expr, expr_attr_names, expr_attr_values
        )
        filter_cond, filter_names, filter_values = extract_expression(
            filter_expr, key_names, key_values
        )

        attrs: dict[str, Any] = {
            'TableName': table_name or self._table_name,
            'KeyConditionExpression': key_cond,
            'ScanIndexForward': scan_index_forward,
        }

        if select:
            attrs['Select'] = select

        if limit:
            attrs['Limit'] = limit

        if filter_names:
            attrs['ExpressionAttributeNames'] = filter_names

        if filter_values:
            attrs['ExpressionAttributeValues'] = serialize(filter_values)

        if exclusive_start_key:
            attrs['ExclusiveStartKey'] = _startkey_b64decode(exclusive_start_key)

        if filter_cond:
            attrs['FilterExpression'] = filter_cond

        if projection_expr:
            attrs['ProjectionExpression'] = projection_expr

        output = self._client.query(**attrs)

        return QueryOutput(
            items=[deserialize(item) for item in output.get('Items', [])],
            count=output.get('Count', 0),
            last_key=_startkey_b64encode(output.get('LastEvaluatedKey')),
        )

    def transact_get(self) -> TransactGet:
        """Create a transactional read operations client."""
        return TransactGet(self._table_name, self._client)

    def transact_writer(
        self,
        flush_amount: int = 50,
        fail_fast: bool = True,
    ) -> TransactWriter:
        """Create a transactional write operations context manager.

        Args:
            flush_amount: Number of operations to batch before flushing (default: 50).
                DynamoDB transact_write_items limit is 100 operations per transaction.
            fail_fast: If True, raise on first cancellation reason. If False,
                continue processing and attach cancellation reasons to exceptions.

        Returns:
            TransactWriter context manager for batching transactional writes.

        Example:
            >>> with client.transact_writer() as tx:
            ...     tx.put_item(User(id='USER#1', name='Alice'))
            ...     tx.delete_item({'id': 'USER#2', 'sk': '0'})
        """
        return TransactWriter(self._table_name, self._client, flush_amount, fail_fast)


def _startkey_b64decode(key: str) -> dict:
    return json.loads(base64.b64decode(key).decode('utf-8'))


def _startkey_b64encode(obj: dict[str, Any] | None) -> str | None:
    if not obj:
        return None

    json_str = json.dumps(obj)
    encoded = base64.urlsafe_b64encode(json_str.encode('utf-8')).decode('utf-8')
    return parse.quote(encoded)
