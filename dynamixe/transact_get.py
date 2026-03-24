from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Self

from dynamixe._jmespath import JMESPathMixin

from .expressions import AttrExpression, Expression
from .types import deserialize, serialize

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient as Boto3DynamoDBClient
    from mypy_boto3_dynamodb.type_defs import GetTypeDef, TransactGetItemTypeDef

__all__ = ['GetItem', 'get', 'TransactGet', 'TransactGetResult']


@dataclass
class GetItem:
    key: dict[str, str]
    table_name: str | None = None
    projection_expr: str | None = None
    expr_attr_names: dict[str, str] | None = None

    def project(
        self,
        *attrs: AttrExpression | str,
        expr_attr_names: dict[str, str] | None = None,
    ) -> GetItem:
        """Build projection expression from attributes.

        Args:
            *attrs: Attributes to project (strings or expressions).
            expr_attr_names: Optional expression attribute names mapping.

        Returns:
            New GetItem with projection_expr and expr_attr_names set.
            Returns self unchanged if no attrs provided (expr_attr_names ignored).
        """
        if not attrs:
            return self

        exprs: list[str] = []
        names: dict[str, str] = expr_attr_names if expr_attr_names else {}

        for attr in attrs:
            if isinstance(attr, str):
                exprs.append(attr)
            else:
                exprs.append(attr.expr)
                names.update(attr.names)

        return replace(
            self,
            projection_expr=', '.join(exprs),
            expr_attr_names=names if names else None,
        )

    def table(self, table: str) -> GetItem:
        return replace(self, table_name=table)


def get(key: dict[str, str] | Expression) -> GetItem:
    if not isinstance(key, Expression):
        return GetItem(key=key)

    if not key.names or not key.values:
        raise ValueError(
            'Expression must contain attribute names and values to build key'
        )

    attr_names = list(key.names.values())
    attr_values = list(key.values.values())
    return GetItem(key=dict(zip(attr_names, attr_values)))


class TransactGetResult(list[dict], JMESPathMixin):
    """Wrapper for transact_get results with JMESPath support."""

    def __init__(self, items: list[dict]) -> None:
        super().__init__(items)

    @property
    def jmespath_target(self) -> Self:
        return self

    def __repr__(self) -> str:
        return f'TransactGetResult({list(self)!r})'


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
        *items: GetItem,
    ) -> TransactGetResult:
        """Execute transactional get operations.

        Args:
            *items: GetItem configurations with keys and optional projections.

        Returns:
            TransactGetResult wrapper containing list of deserialized items.
            Use .jmespath() to transform results with JMESPath expressions.
        """
        transact_items = [_build_get_item(item, self._table_name) for item in items]
        output = self._client.transact_get_items(TransactItems=transact_items)

        return TransactGetResult(
            [
                deserialize(response['Item'])
                for response in output.get('Responses', [])
                if 'Item' in response
            ]
        )


def _build_get_item(item: GetItem, table_name: str) -> TransactGetItemTypeDef:
    attrs: GetTypeDef = {
        'TableName': item.table_name or table_name,
        'Key': serialize(item.key),
    }

    if item.projection_expr:
        attrs['ProjectionExpression'] = item.projection_expr

    if item.expr_attr_names:
        attrs['ExpressionAttributeNames'] = item.expr_attr_names

    return {'Get': attrs}
