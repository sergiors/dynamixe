from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .expressions import Expression
from .models import Model
from .types import deserialize, serialize

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient as Boto3DynamoDBClient
else:
    Boto3DynamoDBClient = Any


class TransactGet:
    """Transactional read operations."""

    def __init__(
        self,
        table_name: str,
        client: Boto3DynamoDBClient,
    ) -> None:
        self._table_name = table_name
        self._client = client

    def get_items(
        self,
        *keys: dict[str, Any],
        flatten_top: bool = True,
    ) -> dict | list:
        """Get multiple items in a single transaction.

        Args:
            keys: Key dictionaries.
            flatten_top: If True and single item, return item directly.

        Returns:
            Single item dict or list of item dicts.
        """
        items: list[dict] = []

        for key in keys:
            attrs = {
                'TableName': self._table_name,
                'Key': serialize(key),
            }
            output = self._client.get_item(**attrs)
            item = deserialize(output.get('Item', {}))
            if item:
                items.append(item)

        if flatten_top and len(items) == 1:
            return items[0]

        return items

    def get_item_from_key(
        self,
        model_cls: type[Model],
        **key_values: Any,
    ) -> dict | None:
        """Get item using model class and key values.

        Builds the key dict from model configuration and provided values.
        """
        pk = model_cls.get_partition_key()
        sk = model_cls.get_sort_key()

        key: dict[str, Any] = {}
        if pk and pk in key_values:
            key[pk] = key_values[pk]
        if sk and sk in key_values:
            key[sk] = key_values[sk]

        if not key:
            raise ValueError('No key values provided')

        result = self.get_items(key, flatten_top=True)
        return result if isinstance(result, dict) else None

    def get_item_from_expr(
        self,
        model_cls: type[Model],
        *key_exprs: Expression,
    ) -> dict | None:
        """Get item using key expressions.

        Builds the key dict from equality expressions on partition/sort keys.

        Args:
            model_cls: Model class with DynamoDB config.
            key_exprs: Equality expressions for key attributes.

        Returns:
            Item dict or None if not found.

        Example:
            item = client.transact_get().get_item_from_expr(
                User,
                User.id == 'USER#10',
                User.sk == '0',
            )
        """
        from .expressions import ComparisonExpression

        pk = model_cls.get_partition_key()
        sk = model_cls.get_sort_key()

        key: dict[str, Any] = {}

        for expr in key_exprs:
            # Only ComparisonExpression has attr_name via left (AttrExpression)
            if not isinstance(expr, ComparisonExpression):
                continue

            # Extract attr_name from the left side of comparison
            attr_name = expr.left.attr_name

            if attr_name not in (pk, sk):
                continue

            # Extract value from expression
            if hasattr(expr, 'raw_value'):
                key[attr_name] = expr.raw_value
            else:
                # Fallback: extract from values dict
                for val_key, val in expr.values.items():
                    if val_key.startswith(f':{attr_name}'):
                        key[attr_name] = val
                        break

        if not key:
            raise ValueError('No valid key expressions provided')

        result = self.get_items(key, flatten_top=True)
        return result if isinstance(result, dict) else None
