import pytest

from dynamixe.client import DynamoDBClient


@pytest.fixture
def client(settings, boto3_dynamodb_client):
    return DynamoDBClient(
        table_name=settings['table_name'], client=boto3_dynamodb_client
    )


@pytest.fixture
def transact_get(client):
    return client.transact_get()


@pytest.fixture
def transact_writer(client):
    return client.transact_writer()
