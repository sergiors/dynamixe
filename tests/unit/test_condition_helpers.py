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


def test_not_exists():
    cond = User.sk.not_exists()
    assert isinstance(cond, Expression)
    assert cond.expr == 'attribute_not_exists(#sk)'
    assert cond.names == {'#sk': 'sk'}


def test_exists():
    cond = User.sk.exists()
    assert cond.expr == 'attribute_exists(#sk)'


def test_begins_with():
    cond = User.sk.begins_with('USER#')
    assert 'begins_with(#sk, :sk_begins)' in cond.expr
    assert ':sk_begins' in cond.values


def test_between():
    cond = User.sk.between('0', '100')
    assert '#sk BETWEEN :sk_low AND :sk_high' in cond.expr
    assert ':sk_low' in cond.values
    assert ':sk_high' in cond.values
