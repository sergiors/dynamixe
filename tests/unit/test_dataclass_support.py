from dataclasses import dataclass

from dynamixe import ConfigDict


@dataclass
class User:
    __dynamodb_config__ = ConfigDict(
        table='users',
        partition_key='id',
        sort_key='sk',
    )
    id: str
    sk: str
    name: str


def test_dataclass_with_dynamodb_config():
    # Dataclasses use __dynamodb_config__ directly (not inherited from Model)
    assert hasattr(User, '__dynamodb_config__')
    assert User.__dynamodb_config__['table'] == 'users'
    assert User.__dynamodb_config__['partition_key'] == 'id'
    assert User.__dynamodb_config__['sort_key'] == 'sk'
