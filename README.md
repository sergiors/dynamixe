# Dynamixe

A developer-friendly library for DynamoDB that simplifies single-table design without ORM lock-in.

## Features

-   **Expression-based API** - SQLAlchemy-style attribute access for type-safe queries
-   **Model configuration** - Pydantic-compatible `model_config` pattern
-   **No ORM lock-in** - Works with Pydantic, dataclasses, or plain dicts
-   **Transactional operations** - Full support for transact_get and transact_write
-   **Conditional operations** - Expression-based conditions with custom exceptions

## Installation

```bash
pip install dynamixe
```

## Quick Start

### Define a Model

```python
from dynamixe import Model, ConfigDict

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

### Use Expressions

```python
# SQLAlchemy-style expression access
User.id == 'USER#10'           # Equality
User.sk.not_exists()           # Condition
User.name.begins_with('A')     # Begins with
User.sk.between('0', '100')    # Range

# Combine expressions
(User.id == 'USER#10') & (User.sk == '0')
```

### Conditional Put

```python
from dynamixe import DynamoDBClient

client = DynamoDBClient(table_name='users')

# Put with condition using expressions
client.put_item(
    user,
    cond_expr=User.sk.not_exists(),
    expr_attr_names=User.sk.not_exists().names,
)
```

### Transactional Writes

```python
from dynamixe import TransactWriter, TransactionOperationFailed

class EmailConflictError(TransactionOperationFailed):
    pass

with TransactWriter('users', client=boto3_client) as tx:
    tx.put(
        item={'id': 'USER#1', 'sk': '0', 'name': 'Alice'},
        cond_expr=User.sk.not_exists(),
        exc_cls=EmailConflictError,
    )
```

### Works with Any Model Pattern

**Pydantic:**

```python
from pydantic import BaseModel

class User(BaseModel):
    model_config = ConfigDict(table='users', partition_key='id')
    id: str
    name: str
```

**Dataclass (via `__dynamodb_config__`):**

```python
from dataclasses import dataclass

@dataclass
class User(Model):
    __dynamodb_config__ = ConfigDict(
        table='users',
        partition_key='id',
        sort_key='sk',
    )
    id: str
    sk: str
    name: str
```

**Plain dict:**

```python
# No model needed
client.put_item({'id': 'USER#1', 'sk': '0', 'name': 'Alice'})
```

## API Reference

### DynamoDBClient

-   `get_item(key, ...)` - Get single item
-   `put_item(item, cond_expr=..., exc_cls=...)` - Put with condition
-   `update_item(key, update_expr, ...)` - Update item
-   `delete_item(key, cond_expr=...)` - Delete item
-   `query(key_expr, ...)` - Query items by key condition
-   `scan(filter_expr=...)` - Scan all items with optional filter
-   `transact_get()` - Start transactional read
-   `transact_writer(flush_amount=50)` - Start transactional write

### Model

-   `model_config` - Class attribute for DynamoDB configuration
-   `get_table()` - Get table name
-   `get_partition_key()` - Get partition key attribute
-   `get_sort_key()` - Get sort key attribute

### ConfigDict

```python
ConfigDict(
    table='table-name',
    partition_key='pk',
    sort_key='sk',  # optional
)
```

### Expressions

-   **Comparison**: `==`, `!=`, `<`, `<=`, `>`, `>=`
-   **Conditions**: `not_exists()`, `exists()`, `begins_with()`, `between()`
-   **Logical**: `&` (AND), `|` (OR), `~` (NOT)

## Why Dynamixe?

-   **No ORM lock-in** - Use Pydantic, dataclasses, or plain dicts
-   **Type-safe** - Full type hints for IDE autocomplete
-   **Single-table design** - Built for DynamoDB best practices
-   **Expression API** - Composable, testable, readable
-   **Transactional** - ACID operations with custom exceptions

## License

MIT
