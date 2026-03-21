from dynamixe import ConfigDict, Model


class User(Model):
    model_config = ConfigDict(
        table='users',
        partition_key='id',
        sort_key='sk',
    )

    id: str
    sk: str
    name: str


def test_model_uses_dynamodb_config_attr():
    assert User.get_table() == 'users'
