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

1.  **Descriptor Pattern** - Model attributes use `AttrDescriptor` to return `AttrExpression` on access
2.  **Expression Algebra** - Operators (`==`, `!=`, `<`, `&`, `|`, `~`) build composable expressions
3.  **ConfigDict Pattern** - Pydantic-compatible configuration via `model_config = ConfigDict(...)`
4.  **Separation of Concerns** - Expressions (logic) separate from Client (execution)

## Coding Style

### Type Hints

-   Always use type hints for function signatures
-   Use `|` for unions (Python 3.10+): `str | None`
-   Use `TYPE_CHECKING` for optional imports (e.g., boto3 stubs)
-   Export types in `__all__`

```python
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.client import DynamoDBClient

def func(value: str | None = None) -> dict[str, Any]:
    ...
```

### Naming Conventions

-   **Classes**: PascalCase (`AttrExpression`, `DynamoDBClient`)
-   **Functions/Methods**: snake_case (`get_item`, `not_exists`)
-   **Private helpers**: Leading underscore (`_combine_exprs`, `_build_condition_attrs`)
-   **Constants**: UPPER_CASE (`Operator`, `EQ`)
-   **Type variables**: Single capital letter (`T = TypeVar("T")`)

### Code Organization

-   Keep functions small and focused
-   Extract repeated logic into private helpers (e.g., `_build_condition_attrs`)
-   Use descriptive variable names (`transact_op`, `batch_items`, `cond_expr_str`)
-   Add docstrings only for larger scope methods (small helpers can be self-documenting)

```python
def _build_condition_attrs(
    cond_expr: str | Expression | None,
    expr_attr_names: dict | None,
    expr_attr_values: dict | None,
) -> dict:
    """Build DynamoDB attributes for condition expressions."""
    ...
```

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
-   Helper functions for exception building (e.g., `_build_tx_exception`)

```python
class TransactionOperationFailed(Exception):
    def __init__(self, message: str, reason: TransactionCanceledReason) -> None:
        super().__init__(message)
        self.reason = reason


def _build_tx_exception(
    exc_cls: type[Exception],
    msg: str,
    reason: TransactionCanceledReason,
) -> Exception:
    """Build an exception with transaction cancellation reason attached."""
    if issubclass(exc_cls, TransactionOperationFailed):
        return exc_cls(msg, reason=reason)
    exc = exc_cls(msg)
    setattr(exc, '__reason__', reason)
    return exc
```

### Testing

-   Use pytest fixtures (`boto3_dynamodb_client`, `settings`, `dynamodb_table`)
-   Test expression logic separately from integration
-   Use `moto` for DynamoDB mocking
-   Test both `Model` and Pydantic `BaseModel` patterns
-   Prefer function-based tests over test classes (avoid wrapping tests in classes)
-   Add comments only when test logic needs explanation (simple tests don't need comments)

```python
def test_attribute_returns_attr_expression():
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

### 3. New Tests

Add to `tests/test_expressions.py` as functions (not classes):

```python
def test_feature_works():
    expr = User.id == 'USER#10'
    result = some_operation(expr)
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
| -------- | ------------ | ------------------------------------- |
| `&`      | `__and__`    | `(User.id == 'X') & (User.sk == '0')` |
| `\|`     | `__or__`     | `(User.id == 'X')`                    |
| `~`      | `__invert__` | `~User.sk.not_exists()`               |

## Dependencies

-   **boto3** - AWS SDK for DynamoDB client
-   **pydantic** - Optional, for BaseModel support
-   **mypy-boto3-dynamodb** - Type stubs for boto3 (optional, via TYPE_CHECKING)
-   **moto** - Testing, for DynamoDB mocking
-   **pytest** - Testing framework
-   **jmespath** - JSON extraction for error parsing

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
-   Add docstrings only for larger scope methods
-   DRY: Extract repeated logic into private helpers

## Influences

-   **SQLAlchemy** - Expression-based query API
-   **Pydantic** - `model_config` pattern, type validation
-   **Django ORM** - Model-based abstraction
-   **Python dataclasses** - Simple data containers

## Future Directions

1.  **Query API** - Expression-based `client.query(User.id == 'USER#10')`
2.  **Update expressions** - `User.name.set('New')`, `User.count.add(1)`
3.  **Batch operations** - `batch_get_item`, `batch_write_item`
4.  **Index support** - GSI/LSI query helpers
5.  **Pagination** - Cursor-based iteration
6.  **Async support** - `AsyncDynamoDBClient`
