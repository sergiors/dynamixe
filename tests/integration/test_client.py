from dynamixe import ConfigDict, Model


class User(Model):
    model_config = ConfigDict(table='pytest', partition_key='id', sort_key='sk')
    id: str
    sk: str
    name: str


def test_put_item_with_dict(client):
    item = {'id': 'USER#PUT', 'sk': '0', 'name': 'Put User'}
    result = client.put_item(item, cond_expr=User.sk.not_exists())
    assert result
    stored = client.get_item({'id': 'USER#PUT', 'sk': '0'})
    assert stored['name'] == 'Put User'


def test_query_with_dict_data(client):
    client.put_item({'id': 'USER#QUERY', 'sk': '0', 'name': 'Alice'})
    client.put_item({'id': 'USER#QUERY', 'sk': '1', 'name': 'Ada'})
    client.put_item({'id': 'USER#QUERY', 'sk': '2', 'name': 'Bob'})

    result = client.query(
        User.id == 'USER#QUERY', filter_expr=User.name.begins_with('A')
    )

    assert isinstance(result, dict)
    assert 'items' in result
    assert 'count' in result
    assert 'last_key' in result

    items = result['items']
    assert len(items) == 2
    assert {item['name'] for item in items} == {'Alice', 'Ada'}


def test_update_item_with_dict(client):
    client.put_item({'id': 'USER#UPDATE', 'sk': '0', 'name': 'Before'})

    output = client.update_item(
        {'id': 'USER#UPDATE', 'sk': '0'},
        update_expr='SET #name = :name',
        cond_expr=User.id == 'USER#UPDATE',
        expr_attr_names={'#name': 'name'},
        expr_attr_values={':name': 'After'},
        return_values='ALL_NEW',
    )

    assert output is not None
    assert output['name'] == 'After'


def test_scan_returns_all_items(client):
    client.put_item({'id': 'USER#1', 'sk': '0', 'name': 'Alice'})
    client.put_item({'id': 'USER#2', 'sk': '0', 'name': 'Bob'})
    client.put_item({'id': 'USER#3', 'sk': '0', 'name': 'Charlie'})

    items = client.scan()

    assert len(items) == 3
    assert {item['name'] for item in items} == {'Alice', 'Bob', 'Charlie'}


def test_scan_with_filter_expr(client):
    client.put_item({'id': 'USER#1', 'sk': '0', 'name': 'Alice'})
    client.put_item({'id': 'USER#2', 'sk': '0', 'name': 'Bob'})
    client.put_item({'id': 'USER#3', 'sk': '0', 'name': 'Charlie'})

    items = client.scan(
        filter_expr=User.name.begins_with('A'),
    )

    assert len(items) == 1
    assert items[0]['name'] == 'Alice'


def test_scan_with_limit(client):
    client.put_item({'id': 'USER#1', 'sk': '0', 'name': 'Alice'})
    client.put_item({'id': 'USER#2', 'sk': '0', 'name': 'Bob'})
    client.put_item({'id': 'USER#3', 'sk': '0', 'name': 'Charlie'})

    items = client.scan(limit=2)

    assert len(items) == 2


def test_scan_with_projection_expr(client):
    client.put_item(
        {'id': 'USER#1', 'sk': '0', 'name': 'Alice', 'email': 'alice@example.com'}
    )
    client.put_item(
        {'id': 'USER#2', 'sk': '0', 'name': 'Bob', 'email': 'bob@example.com'}
    )

    items = client.scan(projection_expr='#name', expr_attr_names={'#name': 'name'})

    assert len(items) == 2
    assert 'name' in items[0]
    assert 'email' not in items[0]


def test_scan_with_expression_filter(client):
    client.put_item({'id': 'USER#1', 'sk': '0', 'name': 'Alice'})
    client.put_item({'id': 'USER#2', 'sk': '0', 'name': 'Bob'})
    client.put_item({'id': 'USER#3', 'sk': '0', 'name': 'Charlie'})

    items = client.scan(filter_expr=User.name != 'Bob')

    assert len(items) == 2
    names = {item['name'] for item in items}
    assert 'Bob' not in names
    assert names == {'Alice', 'Charlie'}
