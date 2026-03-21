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


def test_and_combination():
    combined = (User.id == 'USER#10') & (User.sk == '0')
    assert isinstance(combined, Expression)
    assert 'AND' in combined.expr
    assert '#id' in combined.expr
    assert '#sk' in combined.expr


def test_or_combination():
    combined = (User.id == 'USER#10') | (User.id == 'USER#20')
    assert 'OR' in combined.expr


def test_invert():
    expr = User.sk.not_exists()
    negated = ~expr
    assert negated.expr == 'NOT (attribute_not_exists(#sk))'
