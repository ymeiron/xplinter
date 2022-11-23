from abc import ABC, abstractmethod
class Driver(ABC):
    @abstractmethod
    def set_record(self, record):
        ...

    @abstractmethod
    def open(self):
        ...

    @abstractmethod
    def is_compatible(self) -> bool:
        ...

    @abstractmethod
    def is_empty(self) -> bool:
        ...

    #TODO rename reset to something else, it's confusing
    @abstractmethod
    def reset(self):
        ...

    @abstractmethod
    def write(self):
        ...

    @abstractmethod
    def flush(self):
        ...

    @abstractmethod
    def close(self):
        ...