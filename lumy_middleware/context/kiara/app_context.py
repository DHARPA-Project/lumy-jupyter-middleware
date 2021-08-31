import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import (TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple,
                    Union)

from lumy_middleware.context.context import AppContext, UpdatedIO
from lumy_middleware.context.dataregistry import DataRegistry
from lumy_middleware.context.kiara.data_transformation import (
    get_reverse_transformation_method, get_transformation_method,
    transform_value)
from lumy_middleware.context.kiara.dataregistry import KiaraDataRegistry
from lumy_middleware.context.kiara.util.data import get_value_data
from lumy_middleware.types.generated import (
    DataTabularDataFilter, LumyWorkflow, Metadata,
    MsgWorkflowLumyWorkflowLoadProgress,
    MsgWorkflowLumyWorkflowLoadProgressStatus, State, TypeEnum)
from lumy_middleware.utils.lumy import load_lumy_workflow_from_file
from lumy_middleware.utils.workflow import install_dependencies

from kiara import Kiara
from kiara.data.values import PipelineValue
from kiara.defaults import SpecialValue
from kiara.pipeline.controller import PipelineController
from kiara.workflow import KiaraWorkflow

if TYPE_CHECKING:
    from kiara.events import StepInputEvent, StepOutputEvent

logger = logging.getLogger(__name__)


def is_default_value_acceptable(value: PipelineValue) -> bool:
    return value.value_schema.default is not None and \
        value.value_schema.default != SpecialValue.NOT_SET


def get_pipeline_input_id(ids: List[str]) -> Optional[str]:
    for id in ids:
        parts = id.split('.')

        if parts[0] == '__pipeline__':
            return parts[1]
    return None


@dataclass
class ReverseMappingItem:
    page_id: str
    io_id: str


@dataclass
class ReverseIoMappings:
    inputs: Dict[str, List[ReverseMappingItem]] = field(
        default_factory=lambda: defaultdict(list))
    outputs: Dict[str, List[ReverseMappingItem]] = field(
        default_factory=lambda: defaultdict(list))


PipelineId = '__pipeline__'


def build_reverse_io_mappings(
    workflow: LumyWorkflow
) -> Dict[str, ReverseIoMappings]:
    lookup: Dict[str, ReverseIoMappings] = defaultdict(ReverseIoMappings)

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


class KiaraAppContext(AppContext, PipelineController):
    _workflow: Optional[LumyWorkflow] = None
    _workflow_metadata: Optional[Metadata] = None
    _kiara_workflow: Optional[KiaraWorkflow] = None
    _kiara = Kiara.instance()
    _data_registry: DataRegistry = KiaraDataRegistry(_kiara)
    # kiara workflow step Id -> mappings
    _reverse_io_mappings: Dict[str, ReverseIoMappings]
    _is_running = False
    _is_loading_workflow = False

    def load_workflow(
        self,
        workflow_path_or_content: Union[Path, LumyWorkflow],
        workflow_metadata: Optional[Metadata]
    ) -> Iterator[MsgWorkflowLumyWorkflowLoadProgress]:
        '''
        AppContext
        '''
        try:
            if self._is_loading_workflow:
                msg = 'Another workflow is being loaded.'
                yield MsgWorkflowLumyWorkflowLoadProgress(
                    status=MsgWorkflowLumyWorkflowLoadProgressStatus.LOADING,
                    type=TypeEnum.ERROR,
                    message=msg
                )
                raise Exception(msg)

            self._is_loading_workflow = True
            if isinstance(workflow_path_or_content, Path):
                workflow_path_or_content = load_lumy_workflow_from_file(
                    workflow_path_or_content)

            workflow = workflow_path_or_content

            kiara_workflow_name = workflow_path_or_content.processing \
                .workflow.name

            try:
                yield MsgWorkflowLumyWorkflowLoadProgress(
                    status=MsgWorkflowLumyWorkflowLoadProgressStatus
                    .LOADING,
                    type=TypeEnum.INFO,
                    message='Installing dependencies'
                )

                # Install processing dependencies
                if workflow.processing.dependencies is not None:
                    packages = workflow.processing\
                        .dependencies.python_packages or []
                    for installed_dependency in install_dependencies(packages):
                        yield MsgWorkflowLumyWorkflowLoadProgress(
                            status=MsgWorkflowLumyWorkflowLoadProgressStatus
                            .LOADING,
                            type=TypeEnum.INFO,
                            message=(f'Installed processing dependency'
                                     f': {installed_dependency.name}')
                        )

                # Install UI dependencies
                if workflow.ui.dependencies is not None:
                    packages = workflow.ui.dependencies.python_packages or []
                    for installed_dependency in install_dependencies(packages):
                        yield MsgWorkflowLumyWorkflowLoadProgress(
                            status=MsgWorkflowLumyWorkflowLoadProgressStatus
                            .LOADING,
                            type=TypeEnum.INFO,
                            message=(f'Installed UI dependency'
                                     f': {installed_dependency.name}')
                        )
            except Exception as e:
                yield MsgWorkflowLumyWorkflowLoadProgress(
                    status=MsgWorkflowLumyWorkflowLoadProgressStatus.LOADING,
                    type=TypeEnum.ERROR,
                    message=str(e)
                )
                raise e

            self._kiara_workflow = self._kiara.create_workflow(
                kiara_workflow_name,
                controller=self
            )

            yield MsgWorkflowLumyWorkflowLoadProgress(
                status=MsgWorkflowLumyWorkflowLoadProgressStatus.LOADING,
                type=TypeEnum.INFO,
                message='Loaded workflow'
            )

            # TODO: access the pipeline here because it is lazily created
            # in the getter. If not done, any code later accessing pipeline in
            # a different way will fail.
            if self._kiara_workflow:
                try:
                    self._kiara_workflow.pipeline
                except Exception:
                    # TODO: if a new workflow is set, this call
                    # raises an exception. Can we ignore it?
                    pass

            self._workflow = workflow_path_or_content
            self._workflow_metadata = workflow_metadata
            self._reverse_io_mappings = build_reverse_io_mappings(
                self._workflow)

            yield MsgWorkflowLumyWorkflowLoadProgress(
                status=MsgWorkflowLumyWorkflowLoadProgressStatus.LOADING,
                type=TypeEnum.INFO,
                message='Executing workflow'
            )

            # TODO: executing workflow right away for dev purposes only
            try:
                self.run_processing()

                yield MsgWorkflowLumyWorkflowLoadProgress(
                    status=MsgWorkflowLumyWorkflowLoadProgressStatus.LOADED,
                    type=TypeEnum.INFO,
                    message='Executed workflow'
                )
            except Exception:
                logger.debug('Could not execute steps on launch. It is fine.')
                yield MsgWorkflowLumyWorkflowLoadProgress(
                    status=MsgWorkflowLumyWorkflowLoadProgressStatus.LOADED,
                    type=TypeEnum.ERROR,
                    message='Could not execute steps on launch'
                )
        finally:
            self._is_loading_workflow = False

    @property
    def current_workflow(self) -> Optional[LumyWorkflow]:
        '''
        AppContext
        '''
        return self._workflow

    @property
    def current_workflow_metadata(self) -> Optional[Metadata]:
        return self._workflow_metadata

    def get_step_input_value(
        self,
        step_id: str,  # a page ID
        input_id: str,  # a page input ID
        filter: Optional[DataTabularDataFilter] = None
    ) -> Tuple[Any, Any]:
        '''
        Returns value transformed according to the rules.
        '''
        if self._workflow is None:
            return (None, None)

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
        transformation_descriptor = get_transformation_method(
            self._workflow,
            step_id,
            input_id,
            is_input=True,
            value=value
        )
        if transformation_descriptor is not None:
            value = transform_value(
                self._kiara, value, transformation_descriptor)

        return get_value_data(value, filter)

    def get_step_output_value(
        self,
        step_id: str,  # a page ID
        output_id: str,  # a page output ID
        filter: Optional[DataTabularDataFilter] = None
    ) -> Tuple[Any, Any]:
        if self._workflow is None:
            return (None, None)

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
        transformation_descriptor = get_transformation_method(
            self._workflow,
            step_id,
            output_id,
            is_input=False,
            value=value
        )
        if transformation_descriptor is not None:
            value = transform_value(
                self._kiara, value, transformation_descriptor)

        return get_value_data(value, filter)

    def update_step_input_values(
        self,
        step_id: str,  # a page ID
        input_values: Optional[Dict[str, Any]]  # page input IDs
    ):
        if self._workflow is None or self._kiara_workflow is None:
            return

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
                # 1. get reverse transformation descriptor
                # 2. transform value
                transformation_descriptor = get_reverse_transformation_method(
                    self._workflow,
                    step_id, input_id,
                    is_input=True,
                    value=self._kiara_workflow.inputs.get_value_obj(
                        pipeline_input_id)
                )
                if transformation_descriptor is not None:
                    value = transform_value(self._kiara, value,
                                            transformation_descriptor)
                updated_values[pipeline_input_id] = value

        self._kiara_workflow.inputs.set_values(**updated_values)

    def run_processing(self, step_id: Optional[str] = None):
        try:
            self.processing_state_changed.publish(State.BUSY)
            if step_id is not None:
                self.process_step(step_id)
            else:
                self._process_pipeline(self.processing_stages[0] or [])
        finally:
            self.processing_state_changed.publish(State.IDLE)

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
        page_id_to_input_ids: Dict[str, List[str]] = defaultdict(list)

        for step_id, input_ids in event.updated_step_inputs.items():
            self.run_processing(step_id)
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

        if self.pipeline_is_finished():
            self._is_running = False

        page_id_to_output_ids: Dict[str, List[str]] = defaultdict(list)

        for step_id, output_ids in event.updated_step_outputs.items():
            for output_id in output_ids:
                for page_id, page_output_id in \
                    self._get_page_output_ids_for_workflow_output_id(
                        step_id, output_id):
                    page_id_to_output_ids[page_id].append(page_output_id)

        for page_id, output_ids in page_id_to_output_ids.items():
            msg = UpdatedIO(step_id=page_id, io_ids=output_ids)
            self.step_output_values_updated.publish(msg)

    def _process_pipeline(self, steps_ids: List[str]):
        if self._is_running:
            logger.warn(
                "Pipeline running, not starting pipeline processing now.")
            raise Exception("Pipeline already running.")

        if len(steps_ids) == 0:
            return

        self._is_running = True
        try:
            job_ids = [
                self.process_step(step_id)
                for step_id in steps_ids
                if self.can_be_processed(step_id)
                and not self.can_be_skipped(step_id)
            ]
            self._processor.wait_for(*job_ids)
        except Exception:
            logger.exception('Unexpected error while processing steps')
        finally:
            self._is_running = False

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
    ) -> List[Tuple[str, str]]:
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
    ) -> List[Tuple[str, str]]:
        return self._get_page_io_ids_for_workflow_io_id(
            step_id, input_id, True)

    def _get_page_output_ids_for_workflow_output_id(
        self,
        step_id: Optional[str],
        output_id: str
    ) -> List[Tuple[str, str]]:
        return self._get_page_io_ids_for_workflow_io_id(
            step_id, output_id, False)
