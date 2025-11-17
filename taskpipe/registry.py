from __future__ import annotations

import inspect
from typing import Callable, Dict, Optional, Union, Type

from .runnables import Runnable


class RunnableRegistry:
    """
    Helper utility that stores factories for constructing Runnable instances.
    This stays optional—applications can choose to keep using plain dicts.
    """

    def __init__(self):
        self._registry: Dict[str, Callable[[], Runnable]] = {}

    def register(self, name: str, factory: Union[Callable[[], Runnable], Runnable, Type[Runnable]]) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError("RunnableRegistry.register requires a non-empty string name.")

        if isinstance(factory, Runnable):
            def _factory() -> Runnable:
                return factory.copy()
        elif inspect.isclass(factory) and issubclass(factory, Runnable):
            def _factory() -> Runnable:
                return factory()
        elif callable(factory):
            def _factory() -> Runnable:
                result = factory()
                if not isinstance(result, Runnable):
                    raise TypeError(f"Registered factory '{name}' did not return a Runnable instance.")
                return result
        else:
            raise TypeError("Factory must be a Runnable instance, Runnable subclass, or callable returning a Runnable.")

        if name in self._registry:
            raise ValueError(f"Runnable name '{name}' is already registered.")
        self._registry[name] = _factory

    def register_class(self, cls: Type[Runnable], name: Optional[str] = None) -> Type[Runnable]:
        self.register(name or cls.__name__, cls)
        return cls

    def get(self, name: str) -> Runnable:
        if name not in self._registry:
            raise KeyError(f"Runnable '{name}' is not registered.")
        return self._registry[name]()

    def available(self) -> Dict[str, Callable[[], Runnable]]:
        return dict(self._registry)

