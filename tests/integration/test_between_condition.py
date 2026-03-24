from dynamixe import ConfigDict, Model
from dynamixe.client import DynamoDBClient
from dynamixe.expressions import Attr


class User(Model):
    model_config = ConfigDict(
        table='pytest',
        partition_key='id',
        sort_key='sk',
    )
    id: Attr
    sk: Attr
    name: Attr


def test_query_with_between_on_sort_key(client: DynamoDBClient):
    client.put_item({'id': 'USER#1', 'sk': '00', 'name': 'Zero'})
    client.put_item({'id': 'USER#1', 'sk': '10', 'name': 'Alice'})
    client.put_item({'id': 'USER#1', 'sk': '20', 'name': 'Bob'})
    client.put_item({'id': 'USER#1', 'sk': '30', 'name': 'Charlie'})
    client.put_item({'id': 'USER#1', 'sk': '50', 'name': 'Eve'})

    result = client.query(
        (User.id == 'USER#1') & (User.sk.between('10', '30')),
    )

    assert result is not None
    items = result['items']
    assert len(items) == 3
    names = {item['name'] for item in items}
    assert names == {'Alice', 'Bob', 'Charlie'}


def test_query_with_between_exclusive_bounds(client: DynamoDBClient):
    client.put_item({'id': 'USER#2', 'sk': '000', 'name': 'Zero'})
    client.put_item({'id': 'USER#2', 'sk': '050', 'name': 'Fifty'})
    client.put_item({'id': 'USER#2', 'sk': '100', 'name': 'Hundred'})

    result = client.query(
        (User.id == 'USER#2') & (User.sk.between('000', '100')),
    )

    assert result is not None
    items = result['items']
    assert len(items) == 3
    assert {item['name'] for item in items} == {'Zero', 'Fifty', 'Hundred'}


def test_query_with_between_returns_subset(client: DynamoDBClient):
    client.put_item({'id': 'USER#3', 'sk': '001', 'name': 'One'})
    client.put_item({'id': 'USER#3', 'sk': '005', 'name': 'Five'})
    client.put_item({'id': 'USER#3', 'sk': '010', 'name': 'Ten'})
    client.put_item({'id': 'USER#3', 'sk': '015', 'name': 'Fifteen'})
    client.put_item({'id': 'USER#3', 'sk': '020', 'name': 'Twenty'})

    result = client.query(
        (User.id == 'USER#3') & (User.sk.between('005', '015')),
    )

    assert result is not None
    items = result['items']
    assert len(items) == 3
    names = {item['name'] for item in items}
    assert names == {'Five', 'Ten', 'Fifteen'}


def test_between_with_string_sort_keys(client: DynamoDBClient):
    client.put_item({'id': 'USER#4', 'sk': '2024-01-01', 'name': 'Jan'})
    client.put_item({'id': 'USER#4', 'sk': '2024-06-01', 'name': 'Jun'})
    client.put_item({'id': 'USER#4', 'sk': '2024-12-01', 'name': 'Dec'})
    client.put_item({'id': 'USER#4', 'sk': '2023-12-01', 'name': 'Prev'})
    client.put_item({'id': 'USER#4', 'sk': '2025-01-01', 'name': 'Next'})

    result = client.query(
        (User.id == 'USER#4') & (User.sk.between('2024-01-01', '2024-12-01')),
    )

    assert result is not None
    items = result['items']
    assert len(items) == 3
    assert {item['name'] for item in items} == {'Jan', 'Jun', 'Dec'}


def test_between_empty_result(client: DynamoDBClient):
    client.put_item({'id': 'USER#5', 'sk': '100', 'name': 'Hundred'})
    client.put_item({'id': 'USER#5', 'sk': '200', 'name': 'Two Hundred'})

    result = client.query(
        (User.id == 'USER#5') & (User.sk.between('300', '400')),
    )

    assert result is not None
    items = result['items']
    assert len(items) == 0
    assert result['count'] == 0
