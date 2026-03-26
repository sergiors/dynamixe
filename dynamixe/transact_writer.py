from __future__ import annotations

from typing import TYPE_CHECKING, Self, TypedDict

import jmespath

from .expressions import Expression, extract_expression
from .types import deserialize, serialize

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient as Boto3DynamoDBClient
    from mypy_boto3_dynamodb.literals import ReturnValuesOnConditionCheckFailureType
    from mypy_boto3_dynamodb.type_defs import TransactWriteItemTypeDef


class TransactionCanceledReason(TypedDict):
    code: str
    message: str
    operation: dict
    old_item: dict


class TransactionOperationFailed(Exception):
    """Base exception for transaction operation failures."""

    def __init__(self, message: str, reason: TransactionCanceledReason) -> None:
        super().__init__(message)
        self.reason = reason


class TransactionCanceledException(Exception):
    """Exception for cancelled transactions."""

    def __init__(self, reasons: list[TransactionCanceledReason]) -> None:
        super().__init__('Transaction cancelled')
        self.reasons = reasons


class TransactOperation:
    def __init__(
        self,
        operation: dict,
        exc_cls: type[Exception] | None = None,
    ) -> None:
        self.operation = operation
        self.exc_cls = exc_cls


def _build_condition_attrs(
    cond_expr: str | Expression | None,
    expr_attr_names: dict | None,
    expr_attr_values: dict | None,
    return_on_cond_fail: ReturnValuesOnConditionCheckFailureType | None = None,
) -> dict:
    """Build DynamoDB attributes for condition expressions.

    Returns:
        Dict with ConditionExpression (if provided) and related attributes.
    """
    attrs: dict = {}

    cond_expr_str, names, values = extract_expression(
        cond_expr, expr_attr_names, expr_attr_values
    )

    if cond_expr_str:
        attrs['ConditionExpression'] = cond_expr_str

    if names:
        attrs['ExpressionAttributeNames'] = names

    if values:
        attrs['ExpressionAttributeValues'] = serialize(values)

    if return_on_cond_fail:
        attrs['ReturnValuesOnConditionCheckFailure'] = return_on_cond_fail

    return attrs


class TransactWriter:
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
        self._items_buffer: list[TransactOperation] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_details) -> None:
        # When we exit, we need to keep flushing whatever's left
        # until there's nothing left in our items buffer.
        while self._items_buffer:
            self._flush()

    def condition(
        self,
        key: dict,
        cond_expr: str | Expression,
        *,
        table_name: str | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        return_on_cond_fail: ReturnValuesOnConditionCheckFailureType | None = None,
        exc_cls: type[Exception] | None = None,
    ) -> None:
        attrs = _build_condition_attrs(
            cond_expr, expr_attr_names, expr_attr_values, return_on_cond_fail
        )

        self._add_op_and_process(
            TransactOperation(
                {
                    'ConditionCheck': dict(
                        TableName=table_name or self._table_name,
                        Key=serialize(key),
                        **attrs,
                    )
                },
                exc_cls,
            )
        )

    def put(
        self,
        item: dict,
        *,
        table_name: str | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        cond_expr: str | Expression | None = None,
        return_on_cond_fail: ReturnValuesOnConditionCheckFailureType | None = None,
        exc_cls: type[Exception] | None = None,
    ) -> None:
        attrs = _build_condition_attrs(
            cond_expr, expr_attr_names, expr_attr_values, return_on_cond_fail
        )

        self._add_op_and_process(
            TransactOperation(
                {
                    'Put': dict(
                        TableName=table_name or self._table_name,
                        Item=serialize(item),
                        **attrs,
                    )
                },
                exc_cls,
            ),
        )

    def delete(
        self,
        key: dict,
        *,
        table_name: str | None = None,
        cond_expr: str | Expression | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        return_on_cond_fail: ReturnValuesOnConditionCheckFailureType | None = None,
        exc_cls: type[Exception] | None = None,
    ) -> None:
        attrs = _build_condition_attrs(
            cond_expr, expr_attr_names, expr_attr_values, return_on_cond_fail
        )

        self._add_op_and_process(
            TransactOperation(
                {
                    'Delete': dict(
                        TableName=table_name or self._table_name,
                        Key=serialize(key),
                        **attrs,
                    )
                },
                exc_cls,
            ),
        )

    def update(
        self,
        key: dict,
        update_expr: str,
        *,
        cond_expr: str | Expression | None = None,
        table_name: str | None = None,
        expr_attr_names: dict | None = None,
        expr_attr_values: dict | None = None,
        return_on_cond_fail: ReturnValuesOnConditionCheckFailureType | None = None,
        exc_cls: type[Exception] | None = None,
    ) -> None:
        attrs = _build_condition_attrs(
            cond_expr, expr_attr_names, expr_attr_values, return_on_cond_fail
        )

        self._add_op_and_process(
            TransactOperation(
                {
                    'Update': dict(
                        TableName=table_name or self._table_name,
                        Key=serialize(key),
                        UpdateExpression=update_expr,
                        **attrs,
                    )
                },
                exc_cls,
            )
        )

    def _add_op_and_process(self, op: TransactOperation) -> None:
        """Add an operation to the buffer and flush if needed."""
        self._items_buffer.append(op)
        self._flush_if_needed()

    def _flush_if_needed(self) -> None:
        if len(self._items_buffer) >= self._flush_amount:
            self._flush()

    def _flush(self) -> bool:
        """Flush buffered operations to DynamoDB."""
        batch_items = self._items_buffer[: self._flush_amount]
        self._items_buffer = self._items_buffer[self._flush_amount :]

        transact_items: list[TransactWriteItemTypeDef] = [
            transact_op.operation  # type: ignore
            for transact_op in batch_items
        ]

        try:
            self._client.transact_write_items(TransactItems=transact_items)
        except self._client.exceptions.TransactionCanceledException as err:
            error_msg = jmespath.search("Error.Message || 'Unknown'", err.response)
            cancellations = err.response.get('CancellationReasons', [])
            reasons: list[TransactionCanceledReason] = []

            for idx, reason in enumerate(cancellations):
                if 'Message' not in reason:
                    continue

                transact_op = batch_items[idx]
                cancellation_reason = TransactionCanceledReason(
                    code=reason['Code'],  # type: ignore
                    message=reason['Message'],
                    operation=transact_op.operation,
                    old_item=deserialize(reason.get('Item', {})),
                )

                if self._fail_fast:
                    exc_cls = transact_op.exc_cls or TransactionOperationFailed
                    raise _build_tx_exception(
                        exc_cls, error_msg, cancellation_reason
                    ) from err

                reasons.append(cancellation_reason)

            raise TransactionCanceledException(reasons) from err

        return True


def _build_tx_exception(
    exc_cls: type[Exception],
    msg: str,
    reason: TransactionCanceledReason,
) -> Exception:
    """Build an exception with transaction cancellation reason attached."""
    if issubclass(exc_cls, TransactionOperationFailed):
        return exc_cls(msg, reason=reason)

    exc = exc_cls(msg)
    setattr(exc, '__reason__', reason)
    return exc
