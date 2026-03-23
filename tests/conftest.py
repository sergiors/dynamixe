import json
import os
from typing import TYPE_CHECKING, Generator

import boto3
import pytest

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient as Boto3DynamoDBClient
else:
    Boto3DynamoDBClient = object


@pytest.fixture
def boto3_dynamodb_client(
    settings,
) -> Generator[Boto3DynamoDBClient, None, None]:
    table_name = settings['table_name']
    pk = settings['partition_key']
    sk = settings['sort_key']

    client = boto3.client(
        'dynamodb',
        endpoint_url='http://localhost:8000',
        region_name='us-east-1',
    )

    client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {'AttributeName': pk, 'AttributeType': 'S'},
            {'AttributeName': sk, 'AttributeType': 'S'},
        ],
        KeySchema=[
            {'AttributeName': pk, 'KeyType': 'HASH'},
            {'AttributeName': sk, 'KeyType': 'RANGE'},
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 123,
            'WriteCapacityUnits': 123,
        },
    )

    yield client

    client.delete_table(TableName=table_name)


@pytest.fixture
def settings():
    return {
        'table_name': 'pytest',
        'partition_key': 'id',
        'sort_key': 'sk',
    }


@pytest.fixture
def seeds(request, settings, boto3_dynamodb_client):
    seed_file = request.param
    table_name = settings['table_name']
    seed_path = os.path.join(os.path.dirname(__file__), 'seeds', seed_file)

    if os.path.exists(seed_path):
        with open(seed_path, 'r') as f:
            for line in f:
                if line.strip():
                    item = json.loads(line)
                    boto3_dynamodb_client.put_item(
                        TableName=table_name,
                        Item=item,
                    )
    return seed_file
