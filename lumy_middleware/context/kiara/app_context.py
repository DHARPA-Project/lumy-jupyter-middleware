import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union, cast

from lumy_middleware.context.context import AppContext, UpdatedIO
from lumy_middleware.context.dataregistry import DataRegistry
from lumy_middleware.context.kiara.dataregistry import KiaraDataRegistry
from lumy_middleware.context.kiara.table_utils import filter_table, sort_table
from lumy_middleware.types.generated import (DataTabularDataFilter,
                                             LumyWorkflow, State, TableStats)
from lumy_middleware.utils.dataclasses import from_dict, from_yaml
from pyarrow import Table

from kiara import Kiara
from kiara.data.values import DataValue, PipelineValue, Value
from kiara.defaults import SpecialValue
from kiara.pipeline.controller import BatchController
from kiara.workflow import KiaraWorkflow

if TYPE_CHECKING:
    from kiara.events import StepInputEvent, StepOutputEvent

logger = logging.getLogger(__name__)


def is_default_value_acceptable(value: PipelineValue) -> bool:
    return value.value_schema.default is not None and \
        value.value_schema.default != SpecialValue.NOT_SET


def get_value_data(
    value: Value,
    filter: Optional[DataTabularDataFilter]
) -> Tuple[Any, Any]:
    if not hasattr(value, 'get_value_data'):
        raise Exception(
            f'Don\'t know how to get value from class "{value.__class__}"')
    actual_value = cast(DataValue, value).get_value_data()

    # TODO: Type metadata is not in fully implemented in kiara yet
    # When it is, replace isinstance check with metadata type check
    if isinstance(actual_value, Table):
        table: Table = actual_value
        if filter is not None:
            if filter.full_value:
                table_stats = TableStats(rows_count=table.num_rows)
                return (table, table_stats)
            else:
                filtered_table = filter_table(table, filter.condition)
                sorted_table = sort_table(filtered_table, filter.sorting)
                table_stats = TableStats(rows_count=sorted_table.num_rows)

                offset = filter.offset or 0
                page_size = filter.page_size or 5
                table_page = sorted_table.slice(offset, page_size)
                return (table_page, table_stats)

        table_stats = TableStats(rows_count=table.num_rows)
        return (None, table_stats)
    return (actual_value, None)


def get_pipeline_input_id(ids: List[str]) -> Optional[str]:
    for id in ids:
        parts = id.split('.')

        if parts[0] == '__pipeline__':
            return parts[1]
    return None


def load_lumy_workflow_from_file(path: Path) -> LumyWorkflow:
    if path.suffix in ['.yml', '.yaml']:
        return from_yaml(LumyWorkflow, path.read_text())
    return from_dict(LumyWorkflow, json.loads(path.read_text()))


@dataclass
class ReverseMappingItem:
    page_id: str
    io_id: str


@dataclass
class ReverseIoMappings:
    inputs: dict[str, list[ReverseMappingItem]] = field(
        default_factory=lambda: defaultdict(list))
    outputs: dict[str, list[ReverseMappingItem]] = field(
        default_factory=lambda: defaultdict(list))


PipelineId = '__pipeline__'


def build_reverse_io_mappings(
    workflow: LumyWorkflow
) -> dict[str, ReverseIoMappings]:
    lookup: dict[str, ReverseIoMappings] = defaultdict(ReverseIoMappings)

    for page in (workflow.ui.pages or []):
        mapping = page.mapping
        if mapping is not None:
            for m in (mapping.inputs or []):
                items = lookup[m.workflow_step_id or PipelineId] \
                    .inputs[m.workflow_io_id]
                items.append(ReverseMappingItem(
                    page_id=page.id, io_id=m.page_io_id))
            for m in (mapping.outputs or []):
                items = lookup[m.workflow_step_id or PipelineId] \
                    .outputs[m.workflow_io_id]
                items.append(ReverseMappingItem(
                    page_id=page.id, io_id=m.page_io_id))

    return lookup


class KiaraAppContext(AppContext, BatchController):
    _workflow: Optional[LumyWorkflow]
    _kiara_workflow: Optional[KiaraWorkflow]
    _data_registry: DataRegistry
    _kiara = Kiara.instance()
    # kiara workflow step Id -> mappings
    _reverse_io_mappings: dict[str, ReverseIoMappings]

    def load_workflow(
        self,
        workflow_path_or_content: Union[Path, LumyWorkflow]
    ) -> None:
        '''
        AppContext
        '''

        if isinstance(workflow_path_or_content, Path):
            workflow_path_or_content = load_lumy_workflow_from_file(
                workflow_path_or_content)

        kiara_workflow_name = workflow_path_or_content.processing.workflow.name

        self._kiara_workflow = self._kiara.create_workflow(
            kiara_workflow_name,
            controller=self
        )

        self._data_registry = KiaraDataRegistry(self._kiara)
        # self._data_registry = MockDataRegistry()

        # TODO: access the pipeline here because it is lazily created
        # in the getter. If not done, any code later accessing pipeline in
        # a different way will fail.
        if self._kiara_workflow:
            self._kiara_workflow.pipeline

        self._workflow = workflow_path_or_content
        self._reverse_io_mappings = build_reverse_io_mappings(self._workflow)

        # TODO: executing workflow right away for dev purposes only
        try:
            self.execute_all_steps()
        except Exception:
            logger.debug('Could not execute steps on launch. It is fine.')

    @property
    def current_workflow(self) -> Optional[LumyWorkflow]:
        '''
        AppContext
        '''
        return self._workflow

    def get_step_input_value(
        self,
        step_id: str,  # a page ID
        input_id: str,  # a page input ID
        filter: Optional[DataTabularDataFilter] = None
    ) -> Tuple[Any, Any]:
        workflow_step_id, workflow_input_id = \
            self._get_workflow_input_id_for_page(
                step_id, input_id) or (None, None)
        if workflow_step_id is None or workflow_input_id is None:
            return (None, None)

        inputs = self.get_current_pipeline_state(
        ).step_inputs[workflow_step_id]
        if inputs is None:
            return (None, None)

        if workflow_input_id not in inputs.values:
            return (None, None)

        value = self.get_step_input(workflow_step_id, workflow_input_id)

        return get_value_data(value, filter)

    def get_step_output_value(
        self,
        step_id: str,  # a page ID
        output_id: str,  # a page output ID
        filter: Optional[DataTabularDataFilter] = None
    ) -> Tuple[Any, Any]:
        workflow_step_id, workflow_output_id = \
            self._get_workflow_output_id_for_page(
                step_id, output_id) or (None, None)
        if workflow_step_id is None or workflow_output_id is None:
            return (None, None)

        outputs = self.get_current_pipeline_state(
        ).step_outputs[workflow_step_id]
        if outputs is None:
            return (None, None)

        if workflow_output_id not in outputs.values:
            return (None, None)

        value = self.get_step_output(workflow_step_id, workflow_output_id)

        return get_value_data(value, filter)

    def update_step_input_values(
        self,
        step_id: str,  # a page ID
        input_values: Optional[Dict[str, Any]]  # page input IDs
    ):
        updated_values = {}

        for input_id, value in (input_values or {}).items():
            workflow_step_id, workflow_input_id = \
                self._get_workflow_input_id_for_page(
                    step_id, input_id) or (None, None)
            if workflow_step_id is None or workflow_input_id is None:
                continue

            input_connections = self.get_current_pipeline_state() \
                .structure.steps[workflow_step_id].input_connections

            pipeline_input_id = get_pipeline_input_id(
                input_connections[workflow_input_id])

            if pipeline_input_id is not None and value is not None:
                if self._kiara_workflow is not None:
                    self._kiara_workflow.inputs.set_value(
                        pipeline_input_id, value)
                    updated_values[input_id] = value

    def run_processing(self, step_id: Optional[str] = None):
        try:
            self.processing_state_changed.publish(State.BUSY)
            if step_id is not None:
                self.process_step(step_id)
            else:
                self.execute_all_steps()
        finally:
            self.processing_state_changed.publish(State.IDLE)

    def execute_all_steps(self):
        for stage in self.processing_stages:
            for step_id in stage:
                self.process_step(step_id)

    def set_default_values(self):
        inputs = self.get_current_pipeline_state() \
            .pipeline_inputs.values.items()
        default_pipeline_inputs = {
            key: pipeline_value.value_schema.default
            for key, pipeline_value in inputs
            if is_default_value_acceptable(pipeline_value)
        }
        self.pipeline_inputs = default_pipeline_inputs

    def step_inputs_changed(self, event: "StepInputEvent"):
        '''
        PipelineController
        '''
        super().step_inputs_changed(event)

        page_id_to_input_ids: dict[str, list[str]] = defaultdict(list)

        for step_id, input_ids in event.updated_step_inputs.items():
            for input_id in input_ids:
                for page_id, page_input_id in \
                    self._get_page_input_ids_for_workflow_input_id(
                        step_id, input_id):
                    page_id_to_input_ids[page_id].append(page_input_id)

        for page_id, input_ids in page_id_to_input_ids.items():
            msg = UpdatedIO(step_id=page_id, io_ids=input_ids)
            self.step_input_values_updated.publish(msg)

    def step_outputs_changed(self, event: "StepOutputEvent"):
        '''
        PipelineController
        '''

        page_id_to_output_ids: dict[str, list[str]] = defaultdict(list)

        for step_id, output_ids in event.updated_step_outputs.items():
            for output_id in output_ids:
                for page_id, page_output_id in \
                    self._get_page_output_ids_for_workflow_output_id(
                        step_id, output_id):
                    page_id_to_output_ids[page_id].append(page_output_id)

        for page_id, output_ids in page_id_to_output_ids.items():
            msg = UpdatedIO(step_id=page_id, io_ids=output_ids)
            self.step_output_values_updated.publish(msg)

    @property
    def data_registry(self) -> DataRegistry:
        return self._data_registry

    def _get_workflow_io_id_for_page(
        self,
        page_id: str,
        io_id: str,
        is_input: bool
    ) -> Optional[Tuple[str, str]]:
        if self._workflow is None:
            return None
        matching_pages = [
            p
            for p in (self._workflow.ui.pages or [])
            if p.id == page_id
        ]
        page = matching_pages[0] if len(matching_pages) > 0 else None
        if page is None:
            return None

        mapping = page.mapping
        if mapping is None:
            return None

        items = mapping.inputs if is_input else mapping.outputs
        if items is None:
            return None

        matching_items = [i for i in items if i.page_io_id == io_id]
        item = matching_items[0] if len(matching_items) > 0 else None
        if item is None:
            return None

        return (item.workflow_step_id or PipelineId, item.workflow_io_id)

    def _get_workflow_input_id_for_page(
        self,
        page_id: str,
        input_id: str
    ) -> Optional[Tuple[str, str]]:
        return self._get_workflow_io_id_for_page(page_id, input_id, True)

    def _get_workflow_output_id_for_page(
        self,
        page_id: str,
        output_id: str
    ) -> Optional[Tuple[str, str]]:
        return self._get_workflow_io_id_for_page(page_id, output_id, False)

    def _get_page_io_ids_for_workflow_io_id(
        self,
        step_id: Optional[str],
        io_id: str,
        is_input: bool
    ) -> list[Tuple[str, str]]:
        if self._reverse_io_mappings is None:
            return []

        mappings = self._reverse_io_mappings[step_id or PipelineId]

        io_mapping = mappings.inputs if is_input else mappings.outputs
        items = io_mapping[io_id]

        return [(i.page_id, i.io_id) for i in items]

    def _get_page_input_ids_for_workflow_input_id(
        self,
        step_id: Optional[str],
        input_id: str
    ) -> list[Tuple[str, str]]:
        return self._get_page_io_ids_for_workflow_io_id(
            step_id, input_id, True)

    def _get_page_output_ids_for_workflow_output_id(
        self,
        step_id: Optional[str],
        output_id: str
    ) -> list[Tuple[str, str]]:
        return self._get_page_io_ids_for_workflow_io_id(
            step_id, output_id, False)
