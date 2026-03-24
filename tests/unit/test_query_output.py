from dynamixe.client import QueryOutput


def test_query_output_is_dict_like():
    items = [
        {'id': 'USER#1', 'name': 'Alice'},
        {'id': 'USER#2', 'name': 'Bob'},
    ]
    output = QueryOutput(items=items, count=2, last_key=None)

    assert isinstance(output, dict)
    assert output['items'] == items
    assert output['count'] == 2
    assert output['last_key'] is None


def test_query_output_jmespath():
    items = [
        {'id': 'USER#1', 'name': 'Alice', 'active': True},
        {'id': 'USER#2', 'name': 'Bob', 'active': False},
    ]
    output = QueryOutput(items=items, count=2)

    result = output.jmespath('[*].name')
    assert result == ['Alice', 'Bob']


def test_query_output_dict_methods():
    items = [{'id': 'USER#1', 'name': 'Alice'}]
    output = QueryOutput(items=items, count=1)

    assert list(output.keys()) == ['items', 'count', 'last_key']
    assert 'items' in output
    assert output.get('count') == 1
    assert len(output) == 3
