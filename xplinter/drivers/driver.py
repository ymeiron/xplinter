from abc import ABC, abstractmethod
class Driver(ABC):
    @abstractmethod
    def open(self):
        ...

    @abstractmethod
    def is_compatible(self) -> bool:
        ...

    @abstractmethod
    def is_empty(self) -> bool:
        ...

    @abstractmethod
    def reset(self):
        ...

    @abstractmethod
    def write(self):
        ...

    @abstractmethod
    def close(self):
        ...