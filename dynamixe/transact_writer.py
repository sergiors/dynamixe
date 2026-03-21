from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, cast

from botocore.exceptions import ClientError

from .expressions import Expression
from .types import normalize_expression, serialize

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient as Boto3DynamoDBClient
else:
    Boto3DynamoDBClient = Any


class TransactionOperationFailed(Exception):
    """Base exception for transaction operation failures."""

    def __init__(self, message: str, old_item: dict | None = None) -> None:
        super().__init__(message)
        self.old_item = old_item
        self.reason = {'old_item': old_item} if old_item else {}


class TransactionCanceledException(Exception):
    """Exception for cancelled transactions."""

    def __init__(self, reasons: List[Any]) -> None:
        super().__init__('Transaction cancelled')
        self.reasons = reasons


class TransactWriter:
    """Transactional write operations."""

    def __init__(
        self,
        table_name: str,
        client: Boto3DynamoDBClient,
        flush_amount: int = 50,
        fail_fast: bool = True,
    ) -> None:
        self._table_name = table_name
        self._client = client
        self._flush_amount = flush_amount
        self._fail_fast = fail_fast
        self._operations: List[dict] = []

    def put(
        self,
        item: dict,
        cond_expr: str | Expression | None = None,
        exc_cls: type | None = None,
        return_on_cond_fail: str | None = None,
    ) -> None:
        """Add a put operation to the transaction."""
        op = {
            'Put': {
                'TableName': self._table_name,
                'Item': serialize(item),
            },
        }

        condition_expr, expr_attr_names, expr_attr_values = normalize_expression(
            cond_expr
        )

        if condition_expr:
            op['Put']['ConditionExpression'] = condition_expr

        if expr_attr_names:
            op['Put']['ExpressionAttributeNames'] = expr_attr_names

        if expr_attr_values:
            op['Put']['ExpressionAttributeValues'] = serialize(expr_attr_values)

        if return_on_cond_fail:
            op['Put']['ReturnValuesOnConditionCheckFailure'] = return_on_cond_fail

        self._operations.append({'op': op, 'exc_cls': exc_cls})

        if self._fail_fast and len(self._operations) >= self._flush_amount:
            self.flush()

    def flush(self) -> None:
        """Execute the pending operations."""
        if not self._operations:
            return

        transact_items = [item['op'] for item in self._operations]

        try:
            self._client.transact_write_items(TransactItems=transact_items)
        except ClientError as e:
            response = getattr(e, 'response', None)
            reasons: list[Any] = []

            if isinstance(response, dict):
                reasons = cast(List[Any], response.get('CancellationReasons', []))

            raise TransactionCanceledException(reasons) from e

        self._operations = []

    def __enter__(self) -> TransactWriter:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is None:
            self.flush()
