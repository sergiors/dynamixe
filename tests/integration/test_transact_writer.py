import pytest

from dynamixe import ConfigDict, Model
from dynamixe.expressions import AttrExpression


class User(Model):
    model_config = ConfigDict(table='pytest', partition_key='id', sort_key='sk')
    id: AttrExpression
    sk: AttrExpression
    name: AttrExpression


def test_transact_writer_put_multiple_items(client, transact_writer):
    item1 = {'id': 'USER#1', 'sk': '0', 'name': 'Alice'}
    item2 = {'id': 'USER#2', 'sk': '0', 'name': 'Bob'}

    with transact_writer as tx:
        tx.put(item1)
        tx.put(item2)

    result1 = client.get_item({'id': 'USER#1', 'sk': '0'})
    result2 = client.get_item({'id': 'USER#2', 'sk': '0'})
    assert result1['name'] == 'Alice'
    assert result2['name'] == 'Bob'


def test_transact_writer_with_condition(client, transact_writer):
    client.put_item({'id': 'USER#COND', 'sk': '0', 'name': 'Original'})

    with pytest.raises(Exception):
        with transact_writer as tx:
            tx.put(
                {'id': 'USER#COND', 'sk': '0', 'name': 'Overwrite'},
                cond_expr=User.sk.not_exists(),
            )

    result = client.get_item({'id': 'USER#COND', 'sk': '0'})
    assert result['name'] == 'Original'


def test_transact_writer_flush_on_exit(client, transact_writer):
    item = {'id': 'USER#FLUSH', 'sk': '0', 'name': 'Flushed'}

    with transact_writer as tx:
        tx.put(item)

    result = client.get_item({'id': 'USER#FLUSH', 'sk': '0'})
    assert result['name'] == 'Flushed'


def test_transact_writer_manual_flush(client, transact_writer):
    item = {'id': 'USER#MANUAL', 'sk': '0', 'name': 'Manual'}

    with transact_writer as tx:
        tx.put(item)
        tx.flush()

    result = client.get_item({'id': 'USER#MANUAL', 'sk': '0'})
    assert result is not None
    assert result['name'] == 'Manual'


def test_transact_writer_empty_transaction(transact_writer):
    with transact_writer as tx:
        tx.flush()
