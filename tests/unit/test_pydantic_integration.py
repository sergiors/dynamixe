"""Tests for Pydantic BaseModel integration."""

from typing import Any

from dynamixe import ConfigDict
from dynamixe.expressions import AttrExpression
from dynamixe.models import Model as BaseModel


def test_pydantic_model_with_expressions():
    class PydanticUser(BaseModel):
        model_config: Any = ConfigDict(
            table='users',
            partition_key='id',
            sort_key='sk',
        )
        id: str
        sk: str
        name: str

    expr = PydanticUser.id
    assert isinstance(expr, AttrExpression)
    assert expr.attr_name == 'id'
