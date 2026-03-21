from __future__ import annotations

from typing import ClassVar

from .client import ConfigDict, _get_dynamodb_config
from .expressions import expr_field


class Model:
    """Base model class that provides SQLAlchemy-style expression access.

    Usage:
        class User(Model):
            model_config = ConfigDict(table='users', partition_key='id', sort_key='sk')
            id: str
            sk: str
            name: str

        # Access expressions via class attributes
        User.id == 'USER#10'  # AttrExpression
        User.sk.not_exists()  # Condition expression
    """

    model_config: ClassVar[ConfigDict | None] = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Get config from model_config or __dynamodb_config__
        config = _get_dynamodb_config(cls)
        if config:
            cls.model_config = config
        elif not hasattr(cls, 'model_config') or cls.model_config is None:
            cls.model_config = ConfigDict()

        # Set up expression descriptors for type-annotated fields
        if hasattr(cls, '__annotations__'):
            for name in cls.__annotations__.keys():
                if not name.startswith('_'):
                    setattr(cls, name, expr_field(name))

    @classmethod
    def get_table(cls) -> str:
        """Get the DynamoDB table name from config."""
        return cls.model_config.get('table', '') if cls.model_config else ''

    @classmethod
    def get_partition_key(cls) -> str:
        """Get the partition key attribute name."""
        return cls.model_config.get('partition_key') or '' if cls.model_config else ''

    @classmethod
    def get_sort_key(cls) -> str | None:
        """Get the sort key attribute name."""
        return cls.model_config.get('sort_key') if cls.model_config else None


def create_model(
    name: str,
    fields: dict[str, type],
    config: ConfigDict,
    base: type = Model,
) -> type:
    """Programmatically create a model class."""
    annotations = dict(fields)
    cls = type(name, (base,), {'__annotations__': annotations, 'model_config': config})
    return cls
