from __future__ import annotations

from typing import TYPE_CHECKING, Any

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
