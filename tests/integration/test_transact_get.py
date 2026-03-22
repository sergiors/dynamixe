def test_get_items_multiple_keys(client, transact_get):
    client.put_item({'id': 'USER#A', 'sk': '0', 'name': 'Alice'})
    client.put_item({'id': 'USER#B', 'sk': '0', 'name': 'Bob'})

    results = transact_get.get_items(
        {'id': 'USER#A', 'sk': '0'},
        {'id': 'USER#B', 'sk': '0'},
        flatten_top=False,
    )

    assert isinstance(results, list)
    assert len(results) == 2
    assert results[0]['name'] == 'Alice'
    assert results[1]['name'] == 'Bob'


def test_get_items_single_key_flattened(client, transact_get):
    client.put_item(
        {'id': 'USER#SINGLE', 'sk': '0', 'name': 'Single'},
    )

    result = transact_get.get_items(
        {'id': 'USER#SINGLE', 'sk': '0'},
        flatten_top=True,
    )

    assert isinstance(result, dict)
    assert result['name'] == 'Single'


def test_get_items_returns_empty_list_when_none_found(client, transact_get):
    result = transact_get.get_items(
        {'id': 'USER#NOTFOUND', 'sk': '0'},
        flatten_top=False,
    )

    assert result == []
