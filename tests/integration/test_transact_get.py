"""Tests for transact_get module."""

from dynamixe import ConfigDict, DynamoDBClient, Model
from dynamixe.expressions import AttrExpression


class User(Model):
    model_config = ConfigDict(table='pytest', partition_key='id', sort_key='sk')
    id: AttrExpression
    sk: AttrExpression
    name: AttrExpression


def test_get_item_from_expr_with_pk_and_sk(client, transact_get):
    # Arrange: Put an item
    item = {'id': 'USER#10', 'sk': '0', 'name': 'Alice'}
    client.put_item(item)

    # Act: Get item using expressions
    result = transact_get.get_item_from_expr(
        User,
        User.id == 'USER#10',
        User.sk == '0',
    )

    # Assert
    assert result is not None
    assert result['id'] == 'USER#10'
    assert result['sk'] == '0'
    assert result['name'] == 'Alice'


def test_get_item_from_expr_returns_none_when_not_found(transact_get):
    # Act: Try to get non-existent item
    result = transact_get.get_item_from_expr(
        User,
        User.id == 'USER#NOTFOUND',
        User.sk == '0',
    )

    # Assert
    assert result is None


def test_get_item_from_key_with_pk_and_sk(client, transact_get):
    # Arrange: Put an item
    item = {'id': 'USER#20', 'sk': '1', 'name': 'Bob'}
    client.put_item(item)

    # Act: Get item using key values
    result = transact_get.get_item_from_key(
        User,
        id='USER#20',
        sk='1',
    )

    # Assert
    assert result is not None
    assert result['id'] == 'USER#20'
    assert result['name'] == 'Bob'


def test_get_item_from_key_with_pk_only(client, transact_get):
    # Arrange: Put an item
    item = {'id': 'USER#30', 'sk': '0', 'name': 'Charlie'}
    client.put_item(item)

    # Act: Get item using key values
    result = transact_get.get_item_from_key(
        User,
        id='USER#30',
        sk='0',
    )

    # Assert
    assert result is not None
    assert result['id'] == 'USER#30'
    assert result['name'] == 'Charlie'


def test_get_item_from_key_returns_none_when_not_found(transact_get):
    # Act: Try to get non-existent item
    result = transact_get.get_item_from_key(
        User,
        id='USER#NOTFOUND',
        sk='0',
    )

    # Assert
    assert result is None


def test_get_item_from_key_raises_on_no_key_values(transact_get):
    # Act & Assert
    import pytest
    with pytest.raises(ValueError, match='No key values provided'):
        transact_get.get_item_from_key(User)


def test_get_item_from_expr_raises_on_no_valid_expr(transact_get):
    # Act & Assert
    import pytest
    with pytest.raises(ValueError, match='No valid key expressions provided'):
        transact_get.get_item_from_expr(User)


def test_get_items_multiple_keys(client, transact_get):
    # Arrange: Put multiple items
    client.put_item({'id': 'USER#A', 'sk': '0', 'name': 'Alice'})
    client.put_item({'id': 'USER#B', 'sk': '0', 'name': 'Bob'})

    # Act: Get multiple items
    results = transact_get.get_items(
        {'id': 'USER#A', 'sk': '0'},
        {'id': 'USER#B', 'sk': '0'},
        flatten_top=False,
    )

    # Assert
    assert isinstance(results, list)
    assert len(results) == 2
    assert results[0]['name'] == 'Alice'
    assert results[1]['name'] == 'Bob'


def test_get_items_single_key_flattened(client, transact_get):
    # Arrange: Put an item
    client.put_item({'id': 'USER#SINGLE', 'sk': '0', 'name': 'Single'})

    # Act: Get single item (should be flattened)
    result = transact_get.get_items(
        {'id': 'USER#SINGLE', 'sk': '0'},
        flatten_top=True,
    )

    # Assert
    assert isinstance(result, dict)
    assert result['name'] == 'Single'


def test_get_items_returns_empty_list_when_none_found(client, transact_get):
    # Act: Try to get non-existent items
    result = transact_get.get_items(
        {'id': 'USER#NOTFOUND', 'sk': '0'},
        flatten_top=False,
    )

    # Assert
    assert result == []
