from dynamixe import ConfigDict, Model
from dynamixe.expressions import Expression


class User(Model):
    model_config = ConfigDict(
        table='users',
        partition_key='id',
        sort_key='sk',
    )
    id: str
    sk: str
    name: str


def test_equality():
    expr = User.id == 'USER#10'
    assert isinstance(expr, Expression)
    assert '#id' in expr.expr
    assert '= :id_eq' in expr.expr
    assert ':id_eq' in expr.values


def test_not_equal():
    expr = User.id != 'USER#10'
    assert '<>' in expr.expr


def test_less_than():
    expr = User.sk < '100'
    assert '< :sk_lt' in expr.expr


def test_greater_than():
    expr = User.sk > '0'
    assert '> :sk_gt' in expr.expr
