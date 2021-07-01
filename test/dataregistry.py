import pathlib
from unittest import TestCase

from lumy_middleware.context.dataregistry import DataRegistryItem
from lumy_middleware.dev.data_registry.mock import MockDataRegistry

current_dir = pathlib.Path(__file__).parent.resolve()


class TestDataRegistry(TestCase):
    registry = MockDataRegistry(files_location=current_dir)

    def test_get_items(self):
        items = list(self.registry.find())
        n = len(self.registry.find())
        assert n > 0 and n == len(items)

    def test_filter_items(self):
        items = list(self.registry.find(label='dataregistry.py'))
        assert len(items) == 1
        item: DataRegistryItem = items[0]
        assert item.label == 'dataregistry.py'

    def test_slice_items(self):
        q = self.registry.find()
        batch = q[0:2]
        assert len(batch) == 2

    def test_get_item_value(self):
        item: DataRegistryItem = self.registry.find(label='dataregistry.py')[0]
        value = self.registry.get_item_value(item.id)
        assert len(value.get_value_data()) > 100
