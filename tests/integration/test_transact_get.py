from dynamixe import Attr, ConfigDict, DynamoDBClient, Model, TransactGet
from dynamixe.transact_get import TransactGetResult, get


class User(Model):
    model_config = ConfigDict(
        table='users',
        partition_key='id',
        sort_key='sk',
    )
    id: Attr
    sk: Attr
    name: Attr
    email: Attr


def test_get_items_multiple_keys(client: DynamoDBClient, transact_get: TransactGet):
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

    assert isinstance(result, TransactGetResult)
    assert len(result) == 2
    assert result[0]['name'] == 'Alice'
    assert result[1]['email'] == 'bob@example.com'


def test_get_items_single_key(client: DynamoDBClient, transact_get: TransactGet):
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

    assert isinstance(result, TransactGetResult)
    assert len(result) == 2
    assert result[0]['name'] == 'Single'
    assert result[1]['password'] == '123@345'


def test_get_items_returns_empty_list_when_none_found(transact_get):
    result = transact_get.get_items(
        get({'id': 'USER#NOTFOUND', 'sk': '0'}),
    )

    assert len(result) == 0
    assert result == []


def test_get_items_with_project(client: DynamoDBClient, transact_get: TransactGet):
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


def test_get_items_with_model_expression(
    client: DynamoDBClient, transact_get: TransactGet
):
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


def test_get_items_with_project_on_expression(
    client: DynamoDBClient, transact_get: TransactGet
):
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


def test_get_items_with_jmespath_extract_all_names(
    client: DynamoDBClient, transact_get: TransactGet
):
    client.put_item(
        {
            'id': 'USER#JMESPATH1',
            'sk': '0',
            'name': 'User One',
            'email': 'one@example.com',
        }
    )
    client.put_item(
        {
            'id': 'USER#JMESPATH2',
            'sk': '0',
            'name': 'User Two',
            'email': 'two@example.com',
        }
    )

    result = transact_get.get_items(
        get({'id': 'USER#JMESPATH1', 'sk': '0'}),
        get({'id': 'USER#JMESPATH2', 'sk': '0'}),
    ).jmespath('[*].name')

    assert result == ['User One', 'User Two']


def test_get_items_with_jmespath_first_item(
    client: DynamoDBClient, transact_get: TransactGet
):
    client.put_item(
        {
            'id': 'USER#FIRST',
            'sk': '0',
            'name': 'First User',
            'email': 'first@example.com',
        }
    )
    client.put_item(
        {
            'id': 'USER#SECOND',
            'sk': '0',
            'name': 'Second User',
            'email': 'second@example.com',
        }
    )

    result = transact_get.get_items(
        get({'id': 'USER#FIRST', 'sk': '0'}),
        get({'id': 'USER#SECOND', 'sk': '0'}),
    ).jmespath('[0]')

    assert result == {
        'id': 'USER#FIRST',
        'sk': '0',
        'name': 'First User',
        'email': 'first@example.com',
    }


def test_get_items_with_jmespath_filter(
    client: DynamoDBClient, transact_get: TransactGet
):
    client.put_item(
        {
            'id': 'USER#FILTER1',
            'sk': '0',
            'name': 'Alice',
            'active': True,
        }
    )
    client.put_item(
        {
            'id': 'USER#FILTER2',
            'sk': '0',
            'name': 'Bob',
            'active': False,
        }
    )

    result = transact_get.get_items(
        get({'id': 'USER#FILTER1', 'sk': '0'}),
        get({'id': 'USER#FILTER2', 'sk': '0'}),
    ).jmespath('[?active == `true`].name')

    assert result == ['Alice']


def test_get_items_without_jmespath_unchanged(
    client: DynamoDBClient, transact_get: TransactGet
):
    client.put_item(
        {
            'id': 'USER#NOJMESPATH',
            'sk': '0',
            'name': 'No JMESPath',
            'email': 'nojmespath@example.com',
        }
    )

    results = transact_get.get_items(
        get({'id': 'USER#NOJMESPATH', 'sk': '0'}),
    )

    assert isinstance(results, TransactGetResult)
    assert len(results) == 1
    assert results[0]['name'] == 'No JMESPath'
    assert results[0]['email'] == 'nojmespath@example.com'


def test_get_items_with_jmespath_nested_projection(
    client: DynamoDBClient, transact_get: TransactGet
):
    client.put_item(
        {
            'id': 'USER#NESTED1',
            'sk': '0',
            'name': 'Nested 1',
            'profile': {'bio': 'Dev 1', 'level': 'Senior'},
        }
    )
    client.put_item(
        {
            'id': 'USER#NESTED2',
            'sk': '0',
            'name': 'Nested 2',
            'profile': {'bio': 'Dev 2', 'level': 'Junior'},
        }
    )

    result = transact_get.get_items(
        get({'id': 'USER#NESTED1', 'sk': '0'}),
        get({'id': 'USER#NESTED2', 'sk': '0'}),
    ).jmespath('[*].profile.level')

    assert result == ['Senior', 'Junior']


def test_get_items_result_is_iterable(
    client: DynamoDBClient, transact_get: TransactGet
):
    client.put_item(
        {
            'id': 'USER#ITER1',
            'sk': '0',
            'name': 'Iter 1',
        }
    )
    client.put_item(
        {
            'id': 'USER#ITER2',
            'sk': '0',
            'name': 'Iter 2',
        }
    )

    results = transact_get.get_items(
        get({'id': 'USER#ITER1', 'sk': '0'}),
        get({'id': 'USER#ITER2', 'sk': '0'}),
    )

    names = [item['name'] for item in results]
    assert names == ['Iter 1', 'Iter 2']


def test_get_items_result_supports_indexing(
    client: DynamoDBClient, transact_get: TransactGet
):
    client.put_item(
        {
            'id': 'USER#IDX',
            'sk': '0',
            'name': 'Indexed User',
        }
    )

    results = transact_get.get_items(
        get({'id': 'USER#IDX', 'sk': '0'}),
    )

    assert results[0]['name'] == 'Indexed User'
    assert len(results) == 1
