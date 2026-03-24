import pytest

from dynamixe import ConfigDict, Model, TransactionOperationFailed
from dynamixe.client import DynamoDBClient
from dynamixe.transact_writer import TransactWriter


class User(Model):
    model_config = ConfigDict(table='pytest', partition_key='id', sort_key='sk')
    id: str
    sk: str
    name: str
    count: int = 0
    status: str = 'inactive'


def test_transact_writer_put_multiple_items(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    item1 = {'id': 'USER#1', 'sk': '0', 'name': 'Alice'}
    item2 = {'id': 'USER#2', 'sk': '0', 'name': 'Bob'}

    with transact_writer as tx:
        tx.put(item1)
        tx.put(item2)

    result1 = client.get_item({'id': 'USER#1', 'sk': '0'})
    result2 = client.get_item({'id': 'USER#2', 'sk': '0'})
    assert result1['name'] == 'Alice'
    assert result2['name'] == 'Bob'


def test_transact_writer_put_with_expression_condition(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    client.put_item({'id': 'USER#EXIST', 'sk': '0', 'name': 'Existing'})

    with pytest.raises(Exception):
        with transact_writer as tx:
            tx.put(
                {'id': 'USER#EXIST', 'sk': '0', 'name': 'Overwrite'},
                cond_expr=User.sk.not_exists(),
            )

    result = client.get_item({'id': 'USER#EXIST', 'sk': '0'})
    assert result['name'] == 'Existing'


def test_transact_writer_delete_with_expression_condition(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    client.put_item({'id': 'USER#DEL', 'sk': '0', 'name': 'To Delete'})

    with transact_writer as tx:
        tx.delete(
            {'id': 'USER#DEL', 'sk': '0'},
            cond_expr=User.id == 'USER#DEL',
        )

    with pytest.raises(Exception):
        client.get_item({'id': 'USER#DEL', 'sk': '0'})


def test_transact_writer_delete_with_expression_fails(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    client.put_item({'id': 'USER#NDEL', 'sk': '0', 'name': 'Keep'})

    with pytest.raises(Exception):
        with transact_writer as tx:
            tx.delete(
                {'id': 'USER#NDEL', 'sk': '0'},
                cond_expr=User.id == 'USER#WRONG',
            )

    result = client.get_item({'id': 'USER#NDEL', 'sk': '0'})
    assert result is not None


def test_transact_writer_update_with_expression_condition(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    client.put_item({'id': 'USER#UPD', 'sk': '0', 'name': 'Original', 'count': 0})

    with transact_writer as tx:
        tx.update(
            {'id': 'USER#UPD', 'sk': '0'},
            update_expr='SET #name = :name',
            cond_expr=User.count == 0,
            expr_attr_names={'#name': 'name'},
            expr_attr_values={':name': 'Updated'},
        )

    result = client.get_item({'id': 'USER#UPD', 'sk': '0'})
    assert result['name'] == 'Updated'


def test_transact_writer_update_with_expression_fails(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    client.put_item({'id': 'USER#NUD', 'sk': '0', 'name': 'Keep', 'count': 5})

    with pytest.raises(Exception):
        with transact_writer as tx:
            tx.update(
                {'id': 'USER#NUD', 'sk': '0'},
                update_expr='SET #name = :name',
                cond_expr=User.count == 0,
                expr_attr_names={'#name': 'name'},
                expr_attr_values={':name': 'Should Not Apply'},
            )

    result = client.get_item({'id': 'USER#NUD', 'sk': '0'})
    assert result['name'] == 'Keep'


def test_transact_writer_condition_check_with_expression(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    client.put_item({'id': 'USER#COND', 'sk': '0', 'name': 'Original'})

    with transact_writer as tx:
        tx.condition(
            {'id': 'USER#COND', 'sk': '0'},
            cond_expr=User.name == 'Original',
        )


def test_transact_writer_condition_check_with_expression_fails(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    client.put_item({'id': 'USER#CFAIL', 'sk': '0', 'name': 'Original'})

    with pytest.raises(Exception):
        with transact_writer as tx:
            tx.condition(
                {'id': 'USER#CFAIL', 'sk': '0'},
                cond_expr=User.name == 'Different',
            )


def test_transact_writer_with_combined_expressions(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    client.put_item({'id': 'USER#COMBO', 'sk': '0', 'name': 'Combo', 'count': 1})

    with transact_writer as tx:
        tx.put(
            {'id': 'USER#COMBO', 'sk': '0', 'name': 'Combo Updated'},
            cond_expr=(User.id == 'USER#COMBO') & (User.count == 1),
        )

    result = client.get_item({'id': 'USER#COMBO', 'sk': '0'})
    assert result['name'] == 'Combo Updated'


def test_transact_writer_with_or_expression(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    client.put_item({'id': 'USER#OR', 'sk': '0', 'name': 'Or Test', 'status': 'active'})

    with transact_writer as tx:
        tx.put(
            {'id': 'USER#OR', 'sk': '0', 'name': 'Or Updated'},
            cond_expr=(User.name == 'Or Test') | (User.status == 'active'),
        )

    result = client.get_item({'id': 'USER#OR', 'sk': '0'})
    assert result['name'] == 'Or Updated'


def test_transact_writer_with_custom_exception(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    client.put_item({'id': 'USER#1', 'sk': '0', 'name': 'Alice'})
    client.put_item({'id': 'EMAIL', 'sk': 'alice@example.com'})

    class EmailConflictError(TransactionOperationFailed):
        pass

    with pytest.raises(EmailConflictError):
        with transact_writer as tx:
            tx.put(item={'id': 'USER#1', 'sk': '0', 'name': 'Alice Updated'})
            tx.put(item={'id': 'USER#1', 'sk': 'EMAIL@alice@example.com'})
            tx.put(
                item={'id': 'EMAIL', 'sk': 'alice@example.com'},
                cond_expr=User.sk.not_exists(),
                exc_cls=EmailConflictError,
            )

    result = client.get_item({'id': 'USER#1', 'sk': '0'})
    assert result['name'] == 'Alice'


def test_transact_writer_pytest_expression_access(
    client: DynamoDBClient,
    transact_writer: TransactWriter,
):
    expr = User.id
    assert expr.attr_name == 'id'

    client.put_item({'id': 'USER#EXPR', 'sk': '0', 'name': 'Expr Test'})

    with transact_writer as tx:
        tx.put(
            {'id': 'USER#EXPR', 'sk': '0', 'name': 'Expr Updated'},
            cond_expr=User.id == 'USER#EXPR',
        )

    result = client.get_item({'id': 'USER#EXPR', 'sk': '0'})
    assert result['name'] == 'Expr Updated'
