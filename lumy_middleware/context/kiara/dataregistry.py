import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Type, cast

from lumy_middleware.context.dataregistry import (Batch, DataRegistry,
                                                  DataRegistryItem, Eq, IsIn,
                                                  QueryOperator, Substring)

from kiara import Kiara
from kiara.data import Value

logger = logging.getLogger(__name__)

FilterFn = Callable[[str, QueryOperator, Kiara], bool]


def eq_fn(field: str):
    def fn(id: str, op: Eq, kiara: Kiara):
        item = kiara.data_store.get_value_obj(id)
        return getattr(item, field) == op.value
    return fn


def isin_fn(field: str):
    def fn(id: str, op: IsIn, kiara: Kiara):
        item = kiara.data_store.get_value_obj(id)
        return getattr(item, field) in op.values
    return fn


def substring_fn(field: str):
    def fn(id: str, op: Substring, kiara: Kiara):
        item = kiara.data_store.get_value_obj(id)
        return op.term.lower() in getattr(item, field).lower()
    return fn


FiltersMap: Dict[str, Dict[Type[QueryOperator], FilterFn]] = {
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


def get_value_label(value: Value, kiara: Kiara) -> str:
    aliases = kiara.data_store.find_aliases_for_value(value)
    return aliases[0].alias if len(aliases) > 0 else value.id


def as_item(id: str, kiara: Kiara) -> Optional[DataRegistryItem]:
    value = kiara.data_store.get_value_obj(id, raise_exception=False)
    if value is None:
        return None
    return DataRegistryItem(
        id=value.id,
        label=get_value_label(value, kiara),
        type=value.type_name,
        metadata=cast(Dict[str, Any], value.get_metadata())
    )


class KiaraBatch(Batch):
    _kiara: Kiara
    _ids: List[str]

    def __init__(self, ids: List[str], kiara: Kiara):
        self._ids = ids
        self._kiara = kiara

    def slice(self,
              start: Optional[int] = None,
              stop: Optional[int] = None) -> Iterable[DataRegistryItem]:
        items = map(lambda id: as_item(id, self._kiara), self._ids[start:stop])
        return filter(
            lambda v: v is not None,
            cast(Iterable[DataRegistryItem], items)
        )

    def __len__(self):
        return len(self._ids)


class KiaraDataRegistry(DataRegistry[Value]):
    _kiara: Kiara

    def __init__(self, kiara: Kiara):
        self._kiara = kiara

    def get_item_value(self, item_id: str) -> Optional[Value]:
        return self._kiara.data_store.get_value_obj(item_id)

    def find(self, **kwargs) -> Batch:
        '''
        TODO: Filtering implementation is not efficient at all.
        Waiting for better filtering support in Kiara.
        '''
        # ids = list(self._kiara.data_store.value_ids)
        # TODO: Not sure if we should use value_ids or aliases.values
        # kiara CLI uses aliases that excludes duplicates
        ids = list(self._kiara.data_store.alias_names)

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
