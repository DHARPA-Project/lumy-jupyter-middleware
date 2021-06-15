from dataclasses import dataclass
from random import random
from typing import List, Mapping

import pyarrow as pa
from kiara.data.values import ValueSchema
from kiara.module import KiaraModule, ValueSet

from .registry import dharpa_module

# NOTE: Fake repository
repository_items = pa.Table.from_pydict({
    'uri': [f'item-{i}' for i in range(10)],
    'metadataA': [int(i) for i in range(10)],
    'metadataB': [random() * i for i in range(10)],
}, pa.schema({
    'uri': pa.utf8(),
    'metadataA': pa.int32(),
    'metadataB': pa.float64()
}))


@dataclass
class Inputs:
    filenames: List[str]
    metadata_sets: List[str]


@dataclass
class Outputs:
    repository_items: 'pa.Table'


@dharpa_module('dataUpload')
def data_upload_process(inputs: Inputs, outputs: Outputs) -> None:
    outputs.repository_items = repository_items


class DataUploadModule(KiaraModule):

    def create_input_schema(self) -> Mapping[str, ValueSchema]:
        return {
            "filenames": ValueSchema(
                type="any", doc="A list of files.", default=[]
            ),
            "metadataSets": ValueSchema(
                type="any", doc="A list of metadata.", default=[]
            ),
        }

    def create_output_schema(self) -> Mapping[str, ValueSchema]:
        return {
            "repositoryItems": ValueSchema(
                type="table",
                doc="Repository items.",
            )
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
        outputs.set_value('repositoryItems', repository_items)
