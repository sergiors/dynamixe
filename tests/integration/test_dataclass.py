from dataclasses import dataclass

import pytest
from botocore.exceptions import ClientError

from dynamixe import ConfigDict
from dynamixe.expressions import AttrExpression


def test_dataclass_with_dynamodb_config(client, settings):
    @dataclass
    class User:
        __dynamodb_config__ = ConfigDict(
            table=settings['table_name'],
            partition_key=settings['partition_key'],
            sort_key=settings['sort_key'],
        )
        id: str
        sk: str
        value: str

    for name in User.__annotations__:
        if not name.startswith('_'):
            setattr(User, name, AttrExpression(name, User))

    item = User(id='TEST#1', sk='0', value='test')
    client.put_item(
        item,
        cond_expr=User.sk.not_exists(),
    )

    stored = client.get_item({'id': 'TEST#1', 'sk': '0'})
    assert stored['value'] == 'test'


def test_dataclass_with_condition(client, settings):
    @dataclass
    class Item:
        __dynamodb_config__ = ConfigDict(
            table=settings['table_name'],
            partition_key=settings['partition_key'],
            sort_key=settings['sort_key'],
        )
        id: str
        sk: str
        data: str

    for name in Item.__annotations__:
        if not name.startswith('_'):
            setattr(Item, name, AttrExpression(name, Item))

    item = Item(id='COND#1', sk='0', data='initial')

    client.put_item(item, cond_expr=Item.sk.not_exists())

    item2 = Item(id='COND#1', sk='0', data='updated')
    with pytest.raises(ClientError):
        client.put_item(
            item2,
            cond_expr=Item.sk.not_exists(),
        )
