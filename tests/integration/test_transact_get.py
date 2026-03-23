from dynamixe import ConfigDict, Model
from dynamixe.transact_get import get


class User(Model):
    model_config = ConfigDict(
        table='users',
        partition_key='id',
        sort_key='sk',
    )
    id: str
    sk: str
    name: str
    email: str


def test_get_items_multiple_keys(client, transact_get):
    client.put_item(
        {
            'id': 'USER#A',
            'sk': '0',
            'name': 'Alice',
            'email': 'alice@example.com',
        }
    )
    client.put_item(
        {
            'id': 'USER#B',
            'sk': '0',
            'name': 'Bob',
            'email': 'bob@example.com',
        }
    )

    result = transact_get.get_items(
        get((User.id == 'USER#A') & (User.sk == '0')).project(User.name, User.email),
        get((User.id == 'USER#B') & (User.sk == '0')).project(User.email),
    )

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]['name'] == 'Alice'
    assert result[1]['email'] == 'bob@example.com'


def test_get_items_single_key(client, transact_get):
    client.put_item({'id': 'USER#10', 'sk': '0', 'name': 'Single'})
    client.put_item(
        {
            'id': 'USER#10',
            'sk': 'TEMPORARY_PASSWORD',
            'password': '123@345',
        }
    )

    result = transact_get.get_items(
        get({'id': 'USER#10', 'sk': '0'}),
        get({'id': 'USER#10', 'sk': 'TEMPORARY_PASSWORD'}),
    )

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]['name'] == 'Single'
    assert result[1]['password'] == '123@345'


def test_get_items_returns_empty_list_when_none_found(client, transact_get):
    result = transact_get.get_items(
        get({'id': 'USER#NOTFOUND', 'sk': '0'}),
    )

    assert result == []


def test_get_items_with_project(client, transact_get):
    client.put_item(
        {
            'id': 'USER#PROJ',
            'sk': '0',
            'name': 'Projected User',
            'email': 'proj@example.com',
            'age': 25,
        }
    )

    results = transact_get.get_items(
        get({'id': 'USER#PROJ', 'sk': '0'}).project(
            '#id, #name',
            expr_attr_names={
                '#id': 'id',
                '#name': 'name',
            },
        ),
    )

    assert len(results) == 1
    assert results[0]['id'] == 'USER#PROJ'
    assert results[0]['name'] == 'Projected User'


def test_get_items_with_model_expression(client, transact_get):
    client.put_item(
        {
            'id': 'USER#EXPR',
            'sk': '0',
            'name': 'Expression User',
            'email': 'expr@example.com',
        }
    )

    results = transact_get.get_items(
        get((User.id == 'USER#EXPR') & (User.sk == '0')),
    )

    assert len(results) == 1
    assert results[0]['id'] == 'USER#EXPR'
    assert results[0]['name'] == 'Expression User'


def test_get_items_with_project_on_expression(client, transact_get):
    client.put_item(
        {
            'id': 'USER#PROJ2',
            'sk': '0',
            'name': 'Projected Expr',
            'email': 'proj2@example.com',
        }
    )

    results = transact_get.get_items(
        get((User.id == 'USER#PROJ2') & (User.sk == '0')).project(
            '#id, #name',
            expr_attr_names={
                '#id': 'id',
                '#name': 'name',
            },
        ),
    )

    assert len(results) == 1
    assert results[0]['id'] == 'USER#PROJ2'
    assert results[0]['name'] == 'Projected Expr'
