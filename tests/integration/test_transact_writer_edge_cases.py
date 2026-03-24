import pytest

from dynamixe import ConfigDict, Model
from dynamixe.client import DynamoDBClient
from dynamixe.expressions import Expression, extract_expression
from dynamixe.transact_writer import (
    TransactionCanceledException,
    TransactionOperationFailed,
    TransactWriter,
)


class User(Model):
    model_config = ConfigDict(table='pytest', partition_key='id', sort_key='sk')
    id: str
    sk: str
    name: str
    count: int = 0


def test_transact_writer_flushes_buffer_on_exit(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    item = {'id': 'USER#FLUSH', 'sk': '0', 'name': 'Flush Test'}

    with transact_writer as tx:
        tx.put(item)

    result = client.get_item({'id': 'USER#FLUSH', 'sk': '0'})
    assert result['name'] == 'Flush Test'


def test_transact_writer_condition_check_fails(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):

    client.put_item({'id': 'USER#COND', 'sk': '0', 'name': 'Original'})

    with pytest.raises((TransactionOperationFailed, TransactionCanceledException)):
        with transact_writer as tx:
            tx.condition(
                {'id': 'USER#COND', 'sk': '0'},
                cond_expr=User.name == 'Different',
            )


def test_transact_writer_put_fails_on_condition(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    client.put_item({'id': 'USER#EXIST', 'sk': '0', 'name': 'Existing'})

    with pytest.raises((TransactionOperationFailed, TransactionCanceledException)):
        with transact_writer as tx:
            tx.put(
                {'id': 'USER#EXIST', 'sk': '0', 'name': 'Overwrite'},
                cond_expr=User.sk.not_exists(),
            )

    result = client.get_item({'id': 'USER#EXIST', 'sk': '0'})
    assert result['name'] == 'Existing'


def test_transact_writer_delete_fails_on_condition(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    client.put_item({'id': 'USER#DEL', 'sk': '0', 'name': 'Keep'})

    with pytest.raises((TransactionOperationFailed, TransactionCanceledException)):
        with transact_writer as tx:
            tx.delete(
                {'id': 'USER#DEL', 'sk': '0'},
                cond_expr=User.id == 'USER#WRONG',
            )

    result = client.get_item({'id': 'USER#DEL', 'sk': '0'})
    assert result is not None


def test_transact_writer_update_fails_on_condition(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):

    client.put_item({'id': 'USER#UPD', 'sk': '0', 'name': 'Original', 'count': 5})

    with pytest.raises((TransactionOperationFailed, TransactionCanceledException)):
        with transact_writer as tx:
            tx.update(
                {'id': 'USER#UPD', 'sk': '0'},
                update_expr='SET #name = :name',
                cond_expr=User.count == 0,
                expr_attr_names={'#name': 'name'},
                expr_attr_values={':name': 'Updated'},
            )

    result = client.get_item({'id': 'USER#UPD', 'sk': '0'})
    assert result['name'] == 'Original'


def test_transact_writer_with_custom_exception_class(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):

    client.put_item({'id': 'USER#1', 'sk': '0', 'name': 'Alice'})

    class CustomError(TransactionOperationFailed):
        pass

    with pytest.raises(CustomError):
        with transact_writer as tx:
            tx.put(
                {'id': 'USER#1', 'sk': '0', 'name': 'Alice Updated'},
                cond_expr=User.sk.not_exists(),
                exc_cls=CustomError,
            )


def test_transact_writer_success_with_expression_condition(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    client.put_item({'id': 'USER#SUCCESS', 'sk': '0', 'name': 'Before', 'count': 0})

    with transact_writer as tx:
        tx.update(
            {'id': 'USER#SUCCESS', 'sk': '0'},
            update_expr='SET #name = :name',
            cond_expr=User.count == 0,
            expr_attr_names={'#name': 'name'},
            expr_attr_values={':name': 'After'},
        )

    result = client.get_item({'id': 'USER#SUCCESS', 'sk': '0'})
    assert result['name'] == 'After'


def test_transact_writer_combined_and_condition(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    client.put_item({'id': 'USER#AND', 'sk': '0', 'name': 'And Test', 'count': 1})

    with transact_writer as tx:
        tx.put(
            {'id': 'USER#AND', 'sk': '0', 'name': 'And Updated'},
            cond_expr=(User.id == 'USER#AND') & (User.count == 1),
        )

    result = client.get_item({'id': 'USER#AND', 'sk': '0'})
    assert result['name'] == 'And Updated'


def test_transact_writer_combined_or_condition(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):

    client.put_item({'id': 'USER#OR', 'sk': '0', 'name': 'Or Test', 'count': 0})

    with transact_writer as tx:
        tx.put(
            {'id': 'USER#OR', 'sk': '0', 'name': 'Or Updated'},
            cond_expr=(User.count == 0) | (User.name == 'Or Test'),
        )

    result = client.get_item({'id': 'USER#OR', 'sk': '0'})
    assert result['name'] == 'Or Updated'


def test_extract_expression_with_none(client: DynamoDBClient):
    result = extract_expression(None, None, None)
    assert result == (None, None, None)


def test_extract_expression_with_expression_object(client: DynamoDBClient):
    expr = Expression(expr='#id = :val', names={'#id': 'id'}, values=None)
    result = extract_expression(expr, None, None)
    assert result[0] == '#id = :val'
    assert result[1] == {'#id': 'id'}
    assert result[2] is None
