from __future__ import annotations

from enum import Enum
from typing import Any, Generic, TypeVar


class Operator(Enum):
    EQ = '='
    NE = '<>'
    LT = '<'
    LE = '<='
    GT = '>'
    GE = '>='
    AND = 'AND'
    OR = 'OR'
    BEGINS_WITH = 'begins_with'
    ATTRIBUTE_NOT_EXISTS = 'attribute_not_exists'
    ATTRIBUTE_EXISTS = 'attribute_exists'


class Expression:
    def __init__(
        self,
        expr: str,
        names: dict | None = None,
        values: dict | None = None,
    ):
        self.expr = expr
        self.names = names or {}
        self.values = values or {}

    def __and__(self, other: Expression) -> Expression:
        return _combine_exprs(self, other, 'AND')

    def __or__(self, other: Expression) -> Expression:
        return _combine_exprs(self, other, 'OR')

    def __invert__(self) -> Expression:
        return Expression(f'NOT ({self.expr})', self.names, self.values)


class AttrExpression(Expression):
    def __init__(self, attr_name: str, model_cls: type | None = None):
        self.attr_name = attr_name
        self.model_cls = model_cls

        super().__init__(
            expr=f'#{attr_name}',
            names={
                f'#{attr_name}': attr_name,
            },
        )

    def __eq__(self, other: Any) -> Expression:  # type: ignore[override]
        return ComparisonExpression(self, Operator.EQ, other)

    def __ne__(self, other: Any) -> Expression:  # type: ignore[override]
        return ComparisonExpression(self, Operator.NE, other)

    def __lt__(self, other: Any) -> Expression:
        return ComparisonExpression(self, Operator.LT, other)

    def __le__(self, other: Any) -> Expression:
        return ComparisonExpression(self, Operator.LE, other)

    def __gt__(self, other: Any) -> Expression:
        return ComparisonExpression(self, Operator.GT, other)

    def __ge__(self, other: Any) -> Expression:
        return ComparisonExpression(self, Operator.GE, other)

    def not_exists(self) -> Expression:
        return Expression(
            expr=f'attribute_not_exists({self.expr})',
            names=self.names,
            values=None,
        )

    def exists(self) -> Expression:
        return Expression(
            expr=f'attribute_exists({self.expr})',
            names=self.names,
            values=None,
        )

    def begins_with(self, value: str) -> Expression:
        vk = f':{self.attr_name}_begins'
        return Expression(
            expr=f'begins_with({self.expr}, {vk})',
            names=self.names,
            values={vk: value},
        )

    def between(self, low: Any, high: Any) -> Expression:
        lk, hk = f':{self.attr_name}_low', f':{self.attr_name}_high'
        return Expression(
            expr=f'{self.expr} BETWEEN {lk} AND {hk}',
            names=self.names,
            values={lk: low, hk: high},
        )


class ComparisonExpression(Expression):
    def __init__(self, left: AttrExpression, op: Operator, right: Any):
        vk = f':{left.attr_name}_{op.name.lower()}'

        # Store raw value for key extraction
        self.raw_value = right
        self.left = left

        super().__init__(
            f'{left.expr} {op.value} {vk}',
            dict(left.names),
            {**dict(left.values or {}), vk: right},
        )


def _combine_exprs(left: Expression, right: Expression, op: str) -> Expression:
    return Expression(
        f'({left.expr}) {op} ({right.expr})',
        {**left.names, **right.names},
        {**left.values, **right.values},
    )


T = TypeVar('T')


class AttrDescriptor(Generic[T]):
    def __init__(self, name: str):
        self.name = name

    def __get__(
        self,
        obj: Any,
        objtype: type | None = None,
    ) -> AttrExpression:
        return AttrExpression(self.name, objtype)

    def __set__(self, obj: Any, value: T) -> None:
        pass


def expr_field(name: str) -> AttrDescriptor:
    return AttrDescriptor(name)


def extract_expression(
    expr: str | Expression | None,
    expr_attr_names: dict | None = None,
    expr_attr_values: dict | None = None,
) -> tuple[str | None, dict | None, dict | None]:
    """Extract (expr_string, names, values) from Expression or string.

    Values are returned as raw Python values, not serialized.
    Serialization happens at the client level when calling boto3.
    """
    names = dict(expr_attr_names or {})
    values = dict(expr_attr_values or {})

    if isinstance(expr, Expression):
        names.update(expr.names or {})
        values.update(expr.values or {})
        expr = expr.expr

    return expr or None, names or None, values or None
