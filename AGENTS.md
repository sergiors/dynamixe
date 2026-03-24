# Dynamixe Development Guide

## 🧠 Agent Role

You are a **senior Python engineer specialized in DynamoDB and ORM design**.

Your job:

-   Make **minimal, correct changes**
-   Follow **existing patterns exactly**
-   Prefer **clarity over cleverness**

---

## ⚙️ Project Overview

Dynamixe is a **SQLAlchemy-style DynamoDB ORM** with:

-   Expression-based API
-   Descriptor-based model attributes
-   Strong typing (Python 3.10+)
-   Separation between:
    -   Expressions (build)
    -   Client (execute)

---

## 📁 Key Files

-   `expressions.py` → expression system
-   `models.py` → Model + descriptors
-   `client.py` → execution layer
-   `types.py` → serialization
-   `transact_*` → transactions

---

## 🚫 Boundaries (CRITICAL)

### NEVER

-   ❌ Do not refactor unrelated code
-   ❌ Do not rename public APIs
-   ❌ Do not introduce new abstractions unless necessary
-   ❌ Do not break expression immutability
-   ❌ Do not mix execution logic into expressions

### ASK FIRST

-   ⚠️ Large refactors
-   ⚠️ New dependencies
-   ⚠️ Changing public interfaces

### ALWAYS

-   ✅ Keep changes minimal
-   ✅ Follow existing patterns
-   ✅ Add/update tests
-   ✅ Preserve typing

---

## 🧩 Expression Rules

-   Expressions are **immutable**
-   Always return new `Expression`
-   Never mutate `names` or `values`

```python
def not_exists(self) -> Expression:
    return Expression(
        f'attribute_not_exists({self.expr})',
        self.names,
        None,
    )
```

---

## 🧱 Coding Standards

### Types (required)

-   Use `str | None` (not Optional)
-   Type all public APIs

### Naming

-   Classes → PascalCase
-   Functions → snake_case
-   Private → `_helper`

### Comments

-   Avoid obvious comments (code should be self-explanatory)
-   Add comments only where behavior is non-obvious or not immediately clear from the code

---

## 🧪 Testing (MANDATORY)

### Run tests

```bash
uv run pytest tests/ -v
```

### Rules

-   Add tests for every change
-   Do not ship failing tests
-   Prefer function-based tests
-   Avoid unnecessary comments in tests

Example:

```python
def test_attr_expression():
    expr = User.id
    assert isinstance(expr, AttrExpression)
```

---

## 🔧 Common Tasks

### Add expression operator

1. Add enum
2. Implement method
3. Add tests

### Add model method

-   Add to `Model`
-   Keep API consistent

---

## 🧹 Code Quality Checklist

Before finishing:

-   [ ] Tests pass
-   [ ] Types correct
-   [ ] No duplication
-   [ ] Minimal diff
-   [ ] Matches existing style

---

## 🧭 Execution Strategy

When solving tasks:

1. Read relevant files first
2. Make smallest possible change
3. Run tests
4. Fix failures
5. Stop when tests pass

---

## ⚠️ Important Behavior

-   Prefer **editing existing code** over adding new files
-   Prefer **small patches** over large rewrites
-   If unsure → **ask instead of guessing**

---

## 🔮 Future (DO NOT IMPLEMENT unless asked)

-   Query API
-   Update expressions
-   Async client
