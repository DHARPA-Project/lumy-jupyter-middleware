import logging
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Type
from uuid import uuid4

import pandas as pd
import pyarrow as pa
from appdirs import user_data_dir
from kiara import Kiara
from kiara.data import Value
from kiara.data.values import ValueSchema
from lumy_middleware.context.dataregistry import (Batch, DataRegistry,
                                                  DataRegistryItem, Eq, IsIn,
                                                  QueryOperator, Substring)
from pydantic import PrivateAttr

APP_NAME = 'Lumy'

DefaultFilesPath = Path(user_data_dir(appname=APP_NAME)) / 'mock-data-registry'

logger = logging.getLogger(__name__)


def file_is_tabular(f: Path):
    return str(f).endswith('csv') or str(f).endswith('tsv')


def to_item(f: Path):
    is_tabular = file_is_tabular(f)
    columns = pd.read_csv(str(f)).columns.tolist(
    ) if file_is_tabular(f) else None

    return {
        'id': str(uuid4()),
        'label': f.name,
        'type': 'table' if is_tabular else 'string',
        'columnNames': columns,
        'columnTypes': ['string' for _ in columns]
        if is_tabular else None
    }


class MockBatch(Batch):
    items: list[DataRegistryItem]

    def __init__(self, items: list[DataRegistryItem]):
        self.items = items

    def slice(self,
              start: Optional[int] = None,
              stop: Optional[int] = None) -> Iterable[DataRegistryItem]:
        return iter(self.items[start:stop])

    def __len__(self):
        return len(self.items)


FilterFn = Callable[[DataRegistryItem, QueryOperator], bool]


def eq_fn(field: str):
    def fn(item: DataRegistryItem, op: Eq):
        return getattr(item, field) == op.value
    return fn


def isin_fn(field: str):
    def fn(item: DataRegistryItem, op: IsIn):
        return getattr(item, field) in op.values
    return fn


def substring_fn(field: str):
    def fn(item: DataRegistryItem, op: Substring):
        return op.term.lower() in getattr(item, field).lower()
    return fn


FiltersMap: dict[str, dict[Type[QueryOperator], FilterFn]] = {
    'id': {
        Eq: eq_fn('id'),
        IsIn: isin_fn('id'),
    },
    'label': {
        Eq: eq_fn('label'),
        IsIn: isin_fn('label'),
        Substring: substring_fn('label'),
    },
    'type': {
        Eq: eq_fn('type'),
        IsIn: isin_fn('type'),
    }
}


class MockValue(Value):
    _data: Any = PrivateAttr()

    def __init__(self,
                 id: str,
                 type: str,
                 data: Any,
                 metadata: dict[str, dict[str, Any]] = {}):
        self._data = data
        super().__init__(
            id=id,
            value_schema=ValueSchema(type=type),
            kiara=Kiara.instance(),
            metadata=metadata
        )

    def get_value_data(self) -> Any:
        return self._data

    def get_value_hash(self) -> str:
        return str(self.value_hash)


class MockDataRegistry(DataRegistry[MockValue]):
    __instance = None

    _files_path: Path
    _file_lookup: Dict[str, Path] = {}

    _items: list[DataRegistryItem]

    def __init__(self, files_location=DefaultFilesPath):
        self._files_path = files_location

        items = [
            (to_item(f), f)
            for f in self._files_path.glob('**/*')
            if f.is_file()
        ]

        self._file_lookup = {i['id']: p for i, p in items}

        self._items = [
            DataRegistryItem(item['id'], item['label'], item['type'], {
                'columnNames': item['columnNames'],
                'columnTypes': item['columnTypes']
            })
            for item, _ in items
        ]

    @staticmethod
    def get_instance():
        if MockDataRegistry.__instance is None:
            MockDataRegistry.__instance = MockDataRegistry()

        return MockDataRegistry.__instance

    def find(self, **kwargs) -> Batch:
        filtered_items = self._items

        for k, v in kwargs.items():
            filters = FiltersMap.get(k, None)
            assert filters is not None, f'Field "{k}" is not supported'
            if isinstance(v, str):
                v = Eq(v)
            filter_fn = filters[v.__class__]
            assert filter_fn is not None, \
                f'Operator {v.__class__} is not supported for field {k}'
            filtered_items = [i for i in filtered_items if filter_fn(i, v)]
        return MockBatch(filtered_items)

    def get_item_value(self, item_id: str, filter: Any = None) -> MockValue:
        assert filter is None, 'Filtering not implemented yet'

        file_path = self._file_lookup[item_id]
        is_tabular = file_is_tabular(file_path)
        if is_tabular:
            df = pd.read_csv(file_path)
            data = pa.Table.from_pandas(df,
                                        preserve_index=False)
            data_type = 'table'
            metadata = {
                'table': {
                    'column_names': df.columns.to_list(),
                    'schema': {
                        k: {'arrow_type_name': str(v)}
                        for k, v in df.dtypes.to_dict().items()
                    }
                }
            }
        else:
            with open(file_path, 'r') as f:
                data = str(f.read())
            data_type = 'string'
            metadata = {}
        return MockValue(item_id, type=data_type, data=data, metadata=metadata)
