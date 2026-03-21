"""Tests for model configuration methods."""

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


def test_get_table():
    assert User.get_table() == 'users'


def test_get_partition_key():
    assert User.get_partition_key() == 'id'


def test_get_sort_key():
    assert User.get_sort_key() == 'sk'
