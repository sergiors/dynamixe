# Dynamixe Development Guide

## Project Overview

Dynamixe is a SQLAlchemy-style DynamoDB ORM with an expression-based API. It provides type-safe, Pythonic access to DynamoDB operations.

## Architecture

### Core Modules

-   **`expressions.py`** - Expression algebra (`Expression`, `AttrExpression`, operators)
-   **`models.py`** - `Model` base class with descriptor-based attribute access
-   **`client.py`** - `DynamoDBClient` with ConfigDict pattern
-   **`keys.py`** - `PrimaryKey`, `SortKey`, `PartitionKey` for key operations
-   **`types.py`** - Serialization/deserialization utilities
-   **`transact_get.py`** - Transactional read operations
-   **`transact_writer.py`** - Transactional write operations

### Design Patterns

1. **Descriptor Pattern** - Model attributes use `AttrDescriptor` to return `AttrExpression` on access
2. **Expression Algebra** - Operators (`==`, `!=`, `<`, `&`, `|`, `~`) build composable expressions
3. **ConfigDict Pattern** - Pydantic-compatible configuration via `model_config = ConfigDict(...)`
4. **Separation of Concerns** - Expressions (logic) separate from Client (execution)

## Coding Style

### Type Hints

-   Always use type hints for function signatures
-   Use `|` for unions (Python 3.10+): `str | None`
-   Use `TYPE_CHECKING` for circular imports
-   Export types in `__all__`

```python
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dynamixe.client import ConfigDict

def func(value: str | None = None) -> dict[str, Any]:
    ...
```

### Naming Conventions

-   **Classes**: PascalCase (`AttrExpression`, `DynamoDBClient`)
-   **Functions/Methods**: snake_case (`get_item`, `not_exists`)
-   **Private helpers**: Leading underscore (`_combine_exprs`, `_get_dynamodb_config`)
-   **Constants**: UPPER_CASE (`Operator`, `EQ`)
-   **Type variables**: Single capital letter (`T = TypeVar("T")`)

### Expression Building

-   Expression methods return new `Expression` instances (immutable)
-   Use f-strings for expression templates
-   Store names/values as dicts for DynamoDB compatibility

```python
def not_exists(self) -> Expression:
    return Expression(f"attribute_not_exists({self.expr})", self.names, None)
```

### Error Handling

-   Custom exception classes inherit from `Exception`
-   Include `reason` dict for debugging
-   Use `TransactionOperationFailed` for conditional failures

```python
class TransactionOperationFailed(Exception):
    def __init__(self, message: str, old_item: dict | None = None) -> None:
        super().__init__(message)
        self.old_item = old_item
        self.reason = {'old_item': old_item} if old_item else {}
```

### Testing

-   Use pytest fixtures (`boto3_dynamodb_client`, `settings`, `dynamodb_table`)
-   Test expression logic separately from integration
-   Use `moto` for DynamoDB mocking
-   Test both `Model` and Pydantic `BaseModel` patterns

```python
class TestExpressionAccess:
    def test_attribute_returns_attr_expression(self):
        expr = User.id
        assert isinstance(expr, AttrExpression)
        assert expr.attr_name == 'id'
```

## Adding New Features

### 1. New Expression Operators

Add to `Operator` enum and implement in `AttrExpression`:

```python
class Operator(Enum):
    # ... existing operators
    CONTAINS = "contains"

def contains(self, value: Any) -> Expression:
    vk = f":{self.attr_name}_contains"
    return Expression(f"contains({self.expr}, {vk})", self.names, {vk: value})
```

### 2. New Model Methods

Add to `Model` base class in `models.py`:

```python
@classmethod
def get_config(cls) -> ConfigDict:
    """Get the full ConfigDict."""
    return cls.model_config
```

### 3. New Client Operations

Add to `DynamoDBClient` in `client.py`:

```python
def query(self, key_expr: Expression, **kwargs: Any) -> list[dict]:
    """Query items with key expression."""
    from .types import deserialize, serialize
    attrs = {
        'TableName': kwargs.get('table_name') or self._table_name,
        'KeyConditionExpression': key_expr.expr,
    }
    if key_expr.names:
        attrs['ExpressionAttributeNames'] = key_expr.names
    if key_expr.values:
        attrs['ExpressionAttributeValues'] = serialize(key_expr.values)
    output = self._client.query(**attrs)
    return [deserialize(item) for item in output.get('Items', [])]
```

### 4. New Tests

Add to `tests/test_expressions.py` following existing patterns:

```python
class TestNewFeature:
    def test_feature_works(self):
        # Arrange
        expr = User.id == 'USER#10'

        # Act
        result = some_operation(expr)

        # Assert
        assert result is not None
        assert '#id' in result.expr
```

## Configuration Patterns

### Model Configuration

```python
class User(Model):
    model_config = ConfigDict(
        table='users',
        partition_key='id',
        sort_key='sk',
    )
    id: str
    sk: str
    name: str
```

### Pydantic Compatibility

```python
from pydantic import BaseModel

class User(BaseModel):
    model_config = ConfigDict(
        table='users',
        partition_key='id',
        sort_key='sk',
    )
    id: str
    sk: str
    name: str
```

### Dataclass Compatibility

```python
from dataclasses import dataclass

@dataclass
class User:
    __dynamodb_config__ = ConfigDict(
        table='users',
        partition_key='id',
        sort_key='sk',
    )
    id: str
    sk: str
    name: str
```

## Expression API Reference

### Comparison Operators

| Operator | Method   | Example                |
| -------- | -------- | ---------------------- |
| `==`     | `__eq__` | `User.id == 'USER#10'` |
| `!=`     | `__ne__` | `User.id != 'USER#10'` |
| `<`      | `__lt__` | `User.sk < '100'`      |
| `<=`     | `__le__` | `User.sk <= '100'`     |
| `>`      | `__gt__` | `User.sk > '0'`        |
| `>=`     | `__ge__` | `User.sk >= '0'`       |

### Condition Helpers

| Method               | DynamoDB Function      | Example                        |
| -------------------- | ---------------------- | ------------------------------ |
| `not_exists()`       | `attribute_not_exists` | `User.sk.not_exists()`         |
| `exists()`           | `attribute_exists`     | `User.sk.exists()`             |
| `begins_with(value)` | `begins_with`          | `User.sk.begins_with('USER#')` |
| `between(low, high)` | `BETWEEN`              | `User.sk.between('0', '100')`  |

### Logical Operators

| Operator | Method       | Example                               |
| -------- | ------------ | ------------------------------------- | ----------------- | ----------------- |
| `&`      | `__and__`    | `(User.id == 'X') & (User.sk == '0')` |
| `        | `            | `__or__`                              | `(User.id == 'X') | (User.id == 'Y')` |
| `~`      | `__invert__` | `~User.sk.not_exists()`               |

## Dependencies

-   **boto3** - AWS SDK for DynamoDB client
-   **pydantic** - Optional, for BaseModel support
-   **moto** - Testing, for DynamoDB mocking
-   **pytest** - Testing framework

## Running Tests

```bash
cd /Users/sergio/Projects/dynamixe
uv run pytest tests/ -v
uv run pytest tests/test_expressions.py -v  # Specific file
uv run pytest tests/ -k "test_equality"     # Specific test
```

## Code Quality

-   Keep functions small and focused
-   Prefer composition over inheritance
-   Use descriptors for expression access
-   Maintain separation: expressions (logic) vs client (execution)
-   Write tests before implementing features (TDD encouraged)
-   Use type hints for all public APIs
-   Document public methods with docstrings

## Influences

-   **SQLAlchemy** - Expression-based query API
-   **Pydantic** - `model_config` pattern, type validation
-   **Django ORM** - Model-based abstraction
-   **Python dataclasses** - Simple data containers

## Future Directions

1. **Query API** - Expression-based `client.query(User.id == 'USER#10')`
2. **Update expressions** - `User.name.set('New')`, `User.count.add(1)`
3. **Batch operations** - `batch_get_item`, `batch_write_item`
4. **Index support** - GSI/LSI query helpers
5. **Pagination** - Cursor-based iteration
6. **Async support** - `AsyncDynamoDBClient`
