# -*- coding: utf-8 -*-
import typing

from kiara import KiaraModule
from kiara.module_config import ModuleTypeConfigSchema
from kiara.data import ValueSet
from kiara.data.values import ValueSchema
from pydantic import Field

from kiara_modules.core.metadata_schemas import FileMetadata

#
# NOTE: code backported from:
# https://github.com/DHARPA-Project/kiara_modules.core/tree/main/src/kiara_modules/core
# Will be removed once a stable version of "core" package is released.
#

AVAILABLE_FILE_COLUMNS = [
    "id",
    "rel_path",
    "orig_filename",
    "orig_path",
    "import_time",
    "mime_type",
    "size",
    "content",
    "path",
    "file_name",
]
DEFAULT_COLUMNS = ["id", "rel_path", "content"]


class OnboardFileModule(KiaraModule):
    """Import (copy) a file and its metadata into the internal data store.
    This module is used to import a local file into the *kiara* data store.
    It is necessary, because the file needs to be copied to a different
    location, so we can be sure it isn't modified outside of
    *kiara*.
    """

    _module_type_name = "local_file"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "path": {"type": "string", "doc": "The path to the file."},
            "aliases": {
                "type": "list",
                "doc": "A list of aliases to give the dataset in the " +
                "internal data store.",
                "optional": True,
            },
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "file": {
                "type": "file",
                "doc": "A representation of the original file content in" +
                " the kiara data registry.",
            }
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        path = inputs.get_value_data("path")

        file_model = FileMetadata.load_file(path)
        outputs.set_value("file", file_model)


class CreateTableModuleConfig(ModuleTypeConfigSchema):

    allow_column_filter: bool = Field(
        description="Whether to add an input option to filter columns.",
        default=False
    )


class CreateTableFromFileModule(KiaraModule):
    """Load table-like data from a *kiara* file object (not a path!)."""

    _config_cls = CreateTableModuleConfig
    _module_type_name = "create_from_file"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {
            "file": {
                "type": "file",
                "doc": "The file that contains table data.",
                "optional": False,
            }
        }

        if self.get_config_value("allow_column_filter"):

            inputs["columns"] = {
                "type": "array",
                "doc": "If provided, only import the columns " +
                "that match items in this list.",
                "optional": False,
            }

        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"table": {"type": "table", "doc": "The imported table."}}

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        from pyarrow import csv

        input_file: FileMetadata = inputs.get_value_data("file")
        imported_data = csv.read_csv(input_file.path)

        if self.get_config_value("allow_column_filter"):
            if self.get_config_value("columns"):
                imported_data = imported_data.select(
                    self.get_config_value("only_columns")
                )

        outputs.set_value("table", imported_data)
