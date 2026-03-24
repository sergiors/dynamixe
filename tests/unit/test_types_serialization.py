import pytest

from dynamixe.types import deserialize, serialize, to_dict


def test_serialize_datetime():
    from datetime import datetime

    dt = datetime(2024, 1, 15, 10, 30, 0)
    result = serialize({'created': dt})
    assert 'created' in result
    assert result['created']['S'] == '2024-01-15T10:30:00'


def test_serialize_date():
    from datetime import date

    d = date(2024, 1, 15)
    result = serialize({'birth_date': d})
    assert 'birth_date' in result
    assert result['birth_date']['S'] == '2024-01-15'


def test_serialize_uuid():
    from uuid import uuid4

    uid = uuid4()
    result = serialize({'uuid': uid})
    assert 'uuid' in result
    assert result['uuid']['S'] == str(uid)


def test_serialize_ipv4():
    from ipaddress import IPv4Address

    ip = IPv4Address('192.168.1.1')
    result = serialize({'ip': ip})
    assert 'ip' in result
    assert result['ip']['S'] == '192.168.1.1'


def test_serialize_nested_dict():
    data = {'profile': {'name': 'Test', 'level': 'Senior'}}
    result = serialize(data)
    assert 'profile' in result
    assert result['profile']['M']['name']['S'] == 'Test'
    assert result['profile']['M']['level']['S'] == 'Senior'


def test_serialize_list_with_nested():
    data = {'tags': [{'name': 'tag1'}, {'name': 'tag2'}]}
    result = serialize(data)
    assert 'tags' in result
    assert len(result['tags']['L']) == 2
    assert result['tags']['L'][0]['M']['name']['S'] == 'tag1'


def test_deserialize_from_dynamodb():
    data = {
        'value': {'N': '123'},
        'active': {'BOOL': True},
        'name': {'S': 'Test'},
    }
    result = deserialize(data)
    assert result['value'] == 123
    assert result['active'] is True
    assert result['name'] == 'Test'


def test_serialize_exclude_none():
    data = {'a': 1, 'b': None, 'c': 'test'}
    result = serialize(data, exclude_none=True)
    assert 'b' not in result
    assert 'a' in result
    assert 'c' in result


def test_to_dict_with_none():
    assert to_dict(None) is None


def test_to_dict_with_dataclass():
    from dataclasses import dataclass

    @dataclass
    class Item:
        id: str
        name: str

    item = Item(id='TEST', name='Test Item')
    result = to_dict(item)
    assert result == {'id': 'TEST', 'name': 'Test Item'}


def test_to_dict_with_pydantic():
    from pydantic import BaseModel

    class Item(BaseModel):
        id: str
        name: str

    item = Item(id='TEST', name='Test Item')
    result = to_dict(item)
    assert result == {'id': 'TEST', 'name': 'Test Item'}


def test_serialize_invalid_type_raises():
    with pytest.raises(TypeError):
        serialize({'key': object()})  # type: ignore[arg-type]
