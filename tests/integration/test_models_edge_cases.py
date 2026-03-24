import pytest

from dynamixe import ConfigDict, Model
from dynamixe.client import DynamoDBClient
from dynamixe.expressions import AttrExpression, Expression


class UserWithoutConfig(Model):
    id: str
    name: str


class UserWithConfig(Model):
    model_config = ConfigDict(table='pytest', partition_key='id', sort_key='sk')
    id: str
    sk: str
    name: str


class UserWithMultipleAttrs(Model):
    model_config = ConfigDict(table='pytest', partition_key='id', sort_key='sk')
    id: str
    sk: str
    name: str
    email: str
    status: str


def test_model_without_config_has_empty_table(client: DynamoDBClient):
    assert UserWithoutConfig.get_table() == ''
    assert UserWithoutConfig.get_partition_key() == ''
    assert UserWithoutConfig.get_sort_key() is None


def test_model_with_config_returns_table_name(client: DynamoDBClient):
    assert UserWithConfig.get_table() == 'pytest'
    assert UserWithConfig.get_partition_key() == 'id'
    assert UserWithConfig.get_sort_key() == 'sk'


def test_model_annotations_create_descriptors(client: DynamoDBClient):
    assert hasattr(UserWithMultipleAttrs, 'id')
    assert hasattr(UserWithMultipleAttrs, 'name')
    assert hasattr(UserWithMultipleAttrs, 'email')

    expr = UserWithMultipleAttrs.id
    assert isinstance(expr, AttrExpression)
    assert expr.attr_name == 'id'


def test_model_expression_creates_condition(client: DynamoDBClient):
    expr = UserWithConfig.id == 'USER#TEST'
    assert isinstance(expr, Expression)
    assert expr.expr is not None
    assert expr.names is not None


def test_model_expression_not_exists(client: DynamoDBClient):
    expr = UserWithConfig.sk.not_exists()
    assert 'attribute_not_exists' in expr.expr


def test_model_expression_begins_with(client: DynamoDBClient):
    expr = UserWithConfig.name.begins_with('A')
    assert 'begins_with' in expr.expr


def test_model_expression_combined_and(client: DynamoDBClient):
    expr1 = UserWithMultipleAttrs.id == 'USER#1'
    expr2 = UserWithMultipleAttrs.status == 'active'
    combined = expr1 & expr2
    assert isinstance(combined, Expression)
    assert 'AND' in combined.expr


def test_model_expression_combined_or(client: DynamoDBClient):
    expr1 = UserWithMultipleAttrs.status == 'active'
    expr2 = UserWithMultipleAttrs.status == 'inactive'
    combined = expr1 | expr2
    assert isinstance(combined, Expression)
    assert 'OR' in combined.expr


def test_model_put_item_with_dict(client: DynamoDBClient):
    item = {'id': 'USER#MODEL', 'sk': '0', 'name': 'Model User'}
    client.put_item(item, cond_expr=UserWithConfig.sk.not_exists())

    stored = client.get_item({'id': 'USER#MODEL', 'sk': '0'})
    assert stored['name'] == 'Model User'


def test_model_query_with_expression(client: DynamoDBClient):
    client.put_item({'id': 'USER#Q', 'sk': '0', 'name': 'Q1'})
    client.put_item({'id': 'USER#Q', 'sk': '1', 'name': 'Q2'})

    result = client.query(UserWithConfig.id == 'USER#Q')
    assert result['count'] == 2
