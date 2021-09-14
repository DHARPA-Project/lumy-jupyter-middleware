import json
from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, Iterable, List, Optional, TypeVar, Union


class QueryOperator(ABC):
    ...


T = TypeVar('T')


class Eq(QueryOperator, Generic[T]):
    value: T

    def __init__(self, value: T):
        self.value = value


class IsIn(QueryOperator, Generic[T]):
    values: List[T]

    def __init__(self, values: List[T]):
        self.values = values


class Substring(QueryOperator):
    term: str

    def __init__(self, term: str):
        self.term = term


class DataRegistryItem:
    id: str
    label: str
    type: str
    metadata: Dict[str, Any]

    def __init__(
        self,
        id: str,
        label: str,
        type: str,
        metadata: Dict[str, Any] = {}
    ):
        self.id = id
        self.label = label
        self.type = type
        self.metadata = metadata or {}

    def __repr__(self) -> str:
        return json.dumps(dict(self))

    def __iter__(self):
        for k in ['id', 'label', 'type', 'metadata']:
            yield (k, getattr(self, k))


class Batch(ABC):
    @abstractmethod
    def slice(self,
              start: Optional[int] = None,
              stop: Optional[int] = None) -> Iterable[DataRegistryItem]:
        ...

    @abstractmethod
    def __len__(self):
        ...

    def __getitem__(
            self,
            key
    ) -> Union[DataRegistryItem, List[DataRegistryItem]]:
        if isinstance(key, slice):
            assert key.step is None or key.step == 1, \
                f'Step other than 1 is not supported, it is {key.step}'
            assert isinstance(key.start, int), \
                f'Start can only be an integer: {key.start}'
            assert isinstance(key.stop, int) or key.stop is None, \
                f'Stop can only be an integer: {key.stop}'
            return list(self.slice(key.start, key.stop))
        elif isinstance(key, int):
            return list(self.slice(key, key+1))[0]
        else:
            assert False, f'Unexpected argument: {key}'

    def __iter__(self):
        return self.slice()


VT = TypeVar('VT')


class DataRegistry(Generic[VT], ABC):
    '''
    Data registry abstract class.
    '''

    @abstractmethod
    def find(self, **kwargs) -> Batch:
        '''
        Return a filtered subset of data registry items

        kwargs:
            key - field or property to filter by
            value - string or QueryStatement
        '''
        ...

    @abstractmethod
    def get_item_value(self, item_id: str) -> Optional[VT]:
        '''
        Return the value by ID.
        '''
        ...
