import pytest

from dynamixe import ConfigDict, Model
from dynamixe.client import DynamoDBClient
from dynamixe.expressions import Attr


class User(Model):
    model_config = ConfigDict(table='pytest', partition_key='id', sort_key='sk')
    id: Attr
    sk: str
    name: str


def test_query_with_limit(client: DynamoDBClient):
    for i in range(5):
        client.put_item({'id': 'USER#PAGE', 'sk': str(i), 'name': f'User {i}'})

    result = client.query(User.id == 'USER#PAGE', limit=2)
    assert result['count'] == 2


def test_query_returns_empty_last_key_when_complete(client: DynamoDBClient):
    client.put_item({'id': 'USER#SINGLE', 'sk': '0', 'name': 'Single'})

    result = client.query(User.id == 'USER#SINGLE')
    assert result['count'] == 1
    assert result['last_key'] is None


def test_scan_with_limit(client: DynamoDBClient):
    for i in range(4):
        client.put_item({'id': f'USER#SCAN{i}', 'sk': '0', 'name': f'Scan {i}'})

    all_items = client.scan()
    assert len(all_items) == 4

    first_page = client.scan(limit=2)
    assert len(first_page) == 2


def test_get_item_with_projection_expression(client: DynamoDBClient):
    client.put_item(
        {
            'id': 'USER#PROJ',
            'sk': '0',
            'name': 'Projected',
            'email': 'proj@example.com',
            'age': 30,
        }
    )

    result = client.get_item(
        {'id': 'USER#PROJ', 'sk': '0'},
        projection_expr='#name, #email',
        expr_attr_names={'#name': 'name', '#email': 'email'},
    )
    assert result
    assert 'name' in result
    assert 'email' in result
    assert 'age' not in result


def test_get_item_raises_on_missing_by_default(client: DynamoDBClient):
    with pytest.raises(Exception):
        client.get_item({'id': 'USER#NOTFOUND', 'sk': '0'})


def test_get_item_returns_none_when_raise_disabled(client: DynamoDBClient):
    result = client.get_item(
        {'id': 'USER#NOTFOUND', 'sk': '0'},
        raise_on_error=False,
    )
    assert result is None


def test_get_item_returns_default_when_provided(client: DynamoDBClient):
    result = client.get_item(
        {'id': 'USER#NOTFOUND', 'sk': '0'},
        raise_on_error=False,
        default={'id': 'DEFAULT'},
    )
    assert result['id'] == 'DEFAULT'
