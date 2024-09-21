from abc import ABC
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import wraps
from typing import Any


class GeneralInterface:
    pass


InterfaceType = type[ABC] | type[GeneralInterface] | str
ConcreteType = type | Callable[..., Any] | object


@dataclass
class DependencyInjectionMap:
    di_map: dict[InterfaceType, Any] = field(default_factory=dict)

    def add_config(
        self,
        interface: InterfaceType,
        concrete: ConcreteType,
    ) -> None:
        self.di_map[interface] = concrete

    def resolve(self, interface: InterfaceType) -> Any:  # noqa: ANN401
        return self.di_map[interface]

    def can_resolve(self, interface: InterfaceType) -> bool:
        return interface in self.di_map

    def retrieve_injectable_parameters(self, injectand: Callable) -> list:
        return list(injectand.__annotations__.items())


dependency = DependencyInjectionMap()


class Injected:
    def __init__(
        self,
        dependency_injection_map: DependencyInjectionMap,
        injectand: Callable[[Any], Any],
    ) -> None:
        @wraps(injectand)
        def injected_function(*args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            injected_parameters: dict = {
                parameter_name: dependency_injection_map.resolve(parameter_type)
                for parameter_name, parameter_type in dependency_injection_map.retrieve_injectable_parameters(
                    injectand=injectand
                )
                if dependency_injection_map.can_resolve(parameter_type)
            }
            residual_kwargs = {
                arg: value
                for arg, value in kwargs.items()
                if arg not in injected_parameters
            }
            residual_args = [arg for arg in args if arg not in injected_parameters]
            return injectand(
                *residual_args, **{**residual_kwargs, **injected_parameters}
            )

        self.injected_function = injected_function

    def __call__(self, *args: Any, **kwds: Any) -> Any:  # noqa: ANN401
        return self.injected_function(*args, **kwds)
