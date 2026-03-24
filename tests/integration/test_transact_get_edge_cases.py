from dynamixe import ConfigDict, Model
from dynamixe.client import DynamoDBClient
from dynamixe.transact_get import TransactGet, TransactGetResult, get


class User(Model):
    model_config = ConfigDict(table='pytest', partition_key='id', sort_key='sk')
    id: str
    sk: str
    name: str


def test_get_items_with_plain_dict(transact_get: TransactGet):
    result = transact_get.get_items(
        get({'id': 'USER#PLAIN', 'sk': '0'}),
    )
    assert isinstance(result, TransactGetResult)
    assert len(result) == 0


def test_get_items_with_model_expression(
    client: DynamoDBClient,
    transact_get: TransactGet,
):
    
    client.put_item({'id': 'USER#MODEL', 'sk': '0', 'name': 'Model User'})

    result = transact_get.get_items(
        get((User.id == 'USER#MODEL') & (User.sk == '0')),
    )
    assert isinstance(result, TransactGetResult)
    assert len(result) == 1
    assert result[0]['name'] == 'Model User'


def test_get_items_with_projection(
    client: DynamoDBClient,
    transact_get: TransactGet,
):
    
    client.put_item(
        {
            'id': 'USER#PROJ',
            'sk': '0',
            'name': 'Projected',
            'email': 'proj@example.com',
        }
    )

    result = transact_get.get_items(
        get({'id': 'USER#PROJ', 'sk': '0'}).project(
            '#name', expr_attr_names={'#name': 'name'}
        ),
    )
    assert len(result) == 1
    assert 'name' in result[0]
    assert 'email' not in result[0]


def test_get_items_returns_empty_when_not_found(transact_get: TransactGet):
    result = transact_get.get_items(
        get({'id': 'USER#NOTFOUND', 'sk': '0'}),
    )
    assert len(result) == 0
    assert result == []


def test_get_items_multiple_keys(
    client: DynamoDBClient,
    transact_get: TransactGet,
):
    
    client.put_item({'id': 'USER#1', 'sk': '0', 'name': 'Alice'})
    client.put_item({'id': 'USER#2', 'sk': '0', 'name': 'Bob'})

    result = transact_get.get_items(
        get({'id': 'USER#1', 'sk': '0'}),
        get({'id': 'USER#2', 'sk': '0'}),
    )
    assert len(result) == 2
    assert result[0]['name'] == 'Alice'
    assert result[1]['name'] == 'Bob'


def test_get_items_result_is_iterable(
    client: DynamoDBClient,
    transact_get: TransactGet,
):
    
    client.put_item({'id': 'USER#ITER', 'sk': '0', 'name': 'Iter User'})

    result = transact_get.get_items(get({'id': 'USER#ITER', 'sk': '0'}))
    names = [item['name'] for item in result]
    assert names == ['Iter User']


def test_get_items_result_supports_indexing(
    client: DynamoDBClient,
    transact_get: TransactGet,
):
    
    client.put_item({'id': 'USER#IDX', 'sk': '0', 'name': 'Indexed'})

    result = transact_get.get_items(get({'id': 'USER#IDX', 'sk': '0'}))
    assert result[0]['name'] == 'Indexed'
    assert len(result) == 1


def test_get_items_with_jmespath(
    client: DynamoDBClient,
    transact_get: TransactGet,
):
    
    client.put_item({'id': 'USER#JP', 'sk': '0', 'name': 'JMESPath User'})

    result = transact_get.get_items(
        get({'id': 'USER#JP', 'sk': '0'}),
    ).jmespath('[0].name')

    assert result == 'JMESPath User'
