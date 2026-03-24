from typing import Any

from pydantic import BaseModel, ConfigDict

from dynamixe import Attr, Model


def test_pydantic_with_expression_condition(client, settings):
    class PydanticUser(BaseModel):
        model_config = ConfigDict(  # type: ignore
            table=settings['table_name'],
            partition_key='id',
            sort_key='sk',
        )
        id: str
        sk: str
        name: str

    user = PydanticUser(id='PYDANTIC#1', sk='0', name='Pydantic User')

    class User(Model):
        model_config = ConfigDict(  # type: ignore
            table=settings['table_name'],
            partition_key='id',
            sort_key='sk',
        )
        id: Attr
        sk: Attr
        name: Attr

    client.put_item(
        user,
        cond_expr=User.sk.not_exists(),
    )

    stored = client.get_item({'id': 'PYDANTIC#1', 'sk': '0'})
    assert stored['name'] == 'Pydantic User'


def test_pydantic_query_with_filter(client, settings):
    class PydanticItem(BaseModel):
        model_config: Any = ConfigDict(  # type: ignore
            table=settings['table_name'],
            partition_key='id',
            sort_key='sk',
        )
        id: str
        sk: str
        value: str

    class ExprItem(Model):
        model_config = ConfigDict(  # type: ignore
            table=settings['table_name'],
            partition_key='id',
            sort_key='sk',
        )
        id: Attr
        sk: Attr
        value: Attr

    client.put_item(PydanticItem(id='ITEM#1', sk='0', value='A'))
    client.put_item(PydanticItem(id='ITEM#1', sk='1', value='B'))
    client.put_item(PydanticItem(id='ITEM#1', sk='2', value='C'))

    result = client.query(
        ExprItem.id == 'ITEM#1',
        filter_expr=ExprItem.value.begins_with('A'),
    )

    items = result['items']
    assert len(items) == 1
    assert items[0]['value'] == 'A'
