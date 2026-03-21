from dynamixe import ConfigDict, Model
from dynamixe.expressions import AttrExpression


class User(Model):
    model_config = ConfigDict(
        table='users',
        partition_key='id',
        sort_key='sk',
    )
    id: str
    sk: str
    name: str


def test_attribute_returns_attr_expression():
    expr = User.id
    assert isinstance(expr, AttrExpression)
    assert expr.attr_name == 'id'
    assert expr.expr == '#id'
    assert expr.names == {'#id': 'id'}


def test_different_attributes():
    sk_expr = User.sk
    name_expr = User.name
    assert sk_expr.attr_name == 'sk'
    assert name_expr.attr_name == 'name'
