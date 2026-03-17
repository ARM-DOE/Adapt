"""Module registry — discover and instantiate registered modules.

Modules register their class at import time by calling ``registry.register()``.
The controller calls ``registry.create_modules()`` to get instantiated modules
ready for graph construction.

Usage::

    # In a module file (modules/detection/module.py):
    from adapt.execution.module_registry import registry

    class DetectModule(BaseModule):
        name = "detection"
        inputs = ["grid_volume"]
        outputs = ["storm_cells"]
        ...

    registry.register(DetectModule)

    # In the controller:
    modules = registry.create_modules()
    nodes = GraphBuilder(modules).build()
    GraphExecutor(nodes).run(context)
"""

from typing import Dict, List, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from adapt.modules.base import BaseModule


class ModuleRegistry:
    """Central registry for Adapt modules and extensions.

    Stores module *classes*. Instantiates them on demand via
    ``create_modules()``. This allows each module to be configured
    with constructor arguments at runtime while keeping registration
    declarative.

    Raises
    ------
    RuntimeError
        If the same module name is registered twice.
    KeyError
        If ``get()`` is called with an unregistered name.
    """

    def __init__(self) -> None:
        self._modules: Dict[str, Type["BaseModule"]] = {}

    def register(self, module_class: Type["BaseModule"]) -> None:
        """Register a module class by its ``name`` attribute.

        Parameters
        ----------
        module_class : type[BaseModule]
            The module class to register. Must have a non-empty ``name``.

        Raises
        ------
        RuntimeError
            If a module with the same name is already registered.
        ValueError
            If ``module_class.name`` is empty.
        """
        name = module_class.name
        if not name:
            raise ValueError(
                f"Cannot register module with empty name: {module_class}"
            )
        if name in self._modules:
            existing = self._modules[name]
            raise RuntimeError(
                f"Module '{name}' is already registered by {existing}. "
                f"Cannot register {module_class} with the same name."
            )
        self._modules[name] = module_class

    def create_modules(self) -> List["BaseModule"]:
        """Instantiate and return all registered modules.

        Returns
        -------
        list[BaseModule]
            One instance per registered module class, in registration order.
        """
        return [cls() for cls in self._modules.values()]

    def get(self, name: str) -> Type["BaseModule"]:
        """Return the module class registered under ``name``.

        Raises
        ------
        KeyError
            If no module with that name is registered.
        """
        if name not in self._modules:
            raise KeyError(f"Module '{name}' is not registered.")
        return self._modules[name]

    def list_modules(self) -> List[str]:
        """Return names of all registered modules."""
        return list(self._modules.keys())

    def unregister(self, name: str) -> None:
        """Remove a module from the registry (primarily for testing)."""
        self._modules.pop(name, None)

    def clear(self) -> None:
        """Remove all registered modules (primarily for testing)."""
        self._modules.clear()

    def __len__(self) -> int:
        return len(self._modules)

    def __contains__(self, name: str) -> bool:
        return name in self._modules


# Global singleton — modules register here at import time.
registry = ModuleRegistry()
