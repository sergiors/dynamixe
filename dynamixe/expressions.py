from __future__ import annotations

from enum import Enum
from typing import Any, Generic, TypeVar

from .types import serialize


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
        self, expr: str, names: dict | None = None, values: dict | None = None
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

        super().__init__(f'#{attr_name}', {f'#{attr_name}': attr_name})

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
        return Expression(f'attribute_not_exists({self.expr})', self.names, None)

    def exists(self) -> Expression:
        return Expression(f'attribute_exists({self.expr})', self.names, None)

    def begins_with(self, value: str) -> Expression:
        vk = f':{self.attr_name}_begins'
        return Expression(f'begins_with({self.expr}, {vk})', self.names, {vk: value})

    def between(self, low: Any, high: Any) -> Expression:
        lk, hk = f':{self.attr_name}_low', f':{self.attr_name}_high'
        return Expression(
            f'{self.expr} BETWEEN {lk} AND {hk}', self.names, {lk: low, hk: high}
        )


class ComparisonExpression(Expression):
    def __init__(self, left: AttrExpression, op: Operator, right: Any):
        vk = f':{left.attr_name}_{op.name.lower()}'
        sv = serialize({vk: right})[vk]

        # Store raw value for key extraction
        self.raw_value = right
        self.left = left

        super().__init__(
            f'{left.expr} {op.value} {vk}',
            dict(left.names),
            {**dict(left.values or {}), vk: sv},
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

    def __get__(self, obj: Any, objtype: type | None = None) -> AttrExpression:
        return AttrExpression(self.name, objtype)

    def __set__(self, obj: Any, value: T) -> None:
        pass


def expr_field(name: str) -> AttrDescriptor:
    return AttrDescriptor(name)
