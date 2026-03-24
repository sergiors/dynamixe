from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, TypedDict

from .expressions import Attr

if TYPE_CHECKING:
    from .expressions import AttrExpression


class ConfigDict(TypedDict, total=False):
    """Configuration for DynamoDB mapping."""

    table: str
    partition_key: str | None
    sort_key: str | None


class _ModelMeta(type):
    def __getattr__(cls, name: str) -> AttrExpression: ...


class Model(metaclass=_ModelMeta):
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

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)

        config = _get_dynamodb_config(cls)
        if config:
            cls.model_config = config
        elif not hasattr(cls, 'model_config') or cls.model_config is None:
            cls.model_config = ConfigDict()

        # Set up expression descriptors for type-annotated fields
        if hasattr(cls, '__annotations__'):
            for name in cls.__annotations__.keys():
                if not name.startswith('_'):
                    setattr(cls, name, Attr(name))

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


def _get_dynamodb_config(obj: Any) -> ConfigDict | None:
    """Extract DynamoDB config from model_config or __dynamodb_config__."""
    model_config = getattr(obj, 'model_config', None)

    if model_config and isinstance(model_config, dict) and 'table' in model_config:
        return ConfigDict(
            table=model_config.get('table', ''),
            partition_key=model_config.get('partition_key'),
            sort_key=model_config.get('sort_key'),
        )

    return getattr(obj, '__dynamodb_config__', None)
