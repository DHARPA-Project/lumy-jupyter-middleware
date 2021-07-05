from typing import Any, Callable, Iterable, Optional, Type, cast

from lumy_middleware.context.dataregistry import (Batch, DataRegistry,
                                                  DataRegistryItem, Eq, IsIn,
                                                  QueryOperator, Substring)

from kiara import Kiara
from kiara.data import Value

FilterFn = Callable[[str, QueryOperator, Kiara], bool]


def eq_fn(field: str):
    def fn(id: str, op: Eq, kiara: Kiara):
        item = kiara.data_registry.get_value_item(id)
        return getattr(item, field) == op.value
    return fn


def isin_fn(field: str):
    def fn(id: str, op: IsIn, kiara: Kiara):
        item = kiara.data_registry.get_value_item(id)
        return getattr(item, field) in op.values
    return fn


def substring_fn(field: str):
    def fn(id: str, op: Substring, kiara: Kiara):
        item = kiara.data_registry.get_value_item(id)
        return op.term.lower() in getattr(item, field).lower()
    return fn


FiltersMap: dict[str, dict[Type[QueryOperator], FilterFn]] = {
    'id': {
        Eq: eq_fn('id'),
        IsIn: isin_fn('id'),
    },
    # TODO: use label metadata field value when
    # it is available in Kiara
    'label': {
        Eq: eq_fn('id'),
        IsIn: isin_fn('id'),
        Substring: substring_fn('id'),
    },
    'type': {
        Eq: eq_fn('type_name'),
        IsIn: isin_fn('type_name'),
    }
}


def as_item(id: str, kiara: Kiara) -> DataRegistryItem:
    value = kiara.data_registry.get_value_item(id)
    return DataRegistryItem(
        id=value.id,
        # TODO: use label metadata field value when
        # it is available in Kiara
        label=value.id,
        type=value.type_name,
        metadata=cast(dict[str, Any], value.get_metadata())
    )


class KiaraBatch(Batch):
    _kiara: Kiara
    _ids: list[str]

    def __init__(self, ids: list[str], kiara: Kiara):
        self._ids = ids
        self._kiara = kiara

    def slice(self,
              start: Optional[int] = None,
              stop: Optional[int] = None) -> Iterable[DataRegistryItem]:
        return map(lambda id: as_item(id, self._kiara), self._ids[start:stop])

    def __len__(self):
        return len(self._ids)


class KiaraDataRegistry(DataRegistry[Value]):
    _kiara: Kiara

    def __init__(self, kiara: Kiara):
        self._kiara = kiara

    def get_item_value(self, item_id: str) -> Value:
        return self._kiara.data_registry.get_value_item(item_id)

    def find(self, **kwargs) -> Batch:
        '''
        TODO: Filtering implementation is not efficient at all.
        Waiting for better filtering support in Kiara.
        '''
        ids = list(self._kiara.data_store.value_ids)

        for k, v in kwargs.items():
            filters = FiltersMap.get(k, None)
            assert filters is not None, f'Field "{k}" is not supported'
            if isinstance(v, str):
                v = Eq(v)
            filter_fn = filters[v.__class__]
            assert filter_fn is not None, \
                f'Operator {v.__class__} is not supported for field {k}'
            ids = [
                id for id in ids if filter_fn(id, v, self._kiara)]
        return KiaraBatch(ids, self._kiara)
