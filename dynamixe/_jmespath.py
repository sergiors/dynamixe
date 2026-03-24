from abc import ABC, abstractmethod
from typing import Any

import jmespath


class JMESPathMixin(ABC):
    def jmespath(self, expr: str) -> Any:
        """Apply JMESPath expression to result list.

        Args:
            expr: JMESPath expression (e.g., '[*].name', '[0]', '[?active == `true`]').

        Returns:
            Transformed result from JMESPath search.
            Returns raw JMESPath output (list, dict, scalar) without wrapping.
        """
        return jmespath.search(expr, self.jmespath_target)

    @property
    @abstractmethod
    def jmespath_target(self) -> Any:
        raise NotImplementedError
