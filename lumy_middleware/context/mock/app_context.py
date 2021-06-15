import dataclasses
import importlib.resources as pkg_resources
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

from lumy_middleware.context.context import AppContext, UpdatedIO
from lumy_middleware.context.mock import resources
from lumy_middleware.dev.modules import get_module_processor
from lumy_middleware.types import Workflow, WorkflowStructure
from lumy_middleware.types.generated import (DataTabularDataFilter, State,
                                             WorkflowStep)
from lumy_middleware.utils.dataclasses import from_yaml
from stringcase import snakecase

if TYPE_CHECKING:
    from pyarrow import Table

WorkflowStructureUpdated = Callable[[WorkflowStructure], None]

# !!! NOTE
# The mock context is out of sync with the most recent version of the context.
# Consider removing if it is not used for a while.


class MockAppContext(AppContext):
    _current_workflow: Optional[Workflow] = None

    # steps i/o values: step_id -> { io_id -> value (if exists) }
    _steps_input_values: Dict[str, Dict[str, Any]] = defaultdict(dict)
    _steps_output_values: Dict[str, Dict[str, Any]] = defaultdict(dict)

    def __init__(self):
        with pkg_resources.path(resources, 'sample_workflow_1.yml') as path:
            self.load_workflow(path)

    def load_workflow(self, workflow_file_or_name: Union[Path, str]) -> None:
        assert isinstance(workflow_file_or_name, Path), \
            'Only Path is supported'

        with open(workflow_file_or_name, 'r') as f:
            workflow = from_yaml(Workflow, f.read())
            self._current_workflow = workflow
        self.workflow_structure_updated.publish(
            self._current_workflow)

        self._process_all_steps()

    @property
    def current_workflow_structure(self):
        # NOTE: returns old mock structure
        return self._current_workflow

    def _get_step(self, step_id: str) -> Optional[WorkflowStep]:
        if self._current_workflow is None:
            return None
        return next((
            x
            for x in self._current_workflow.structure.steps
            if x.id == step_id
        ), None)

    def _process_step(self, step_id: str) -> None:
        step = self._get_step(step_id)
        if step is None:
            return

        fn = get_module_processor(step.module_id)
        if fn is not None:
            inputs = self.get_step_input_values(step_id, None, True)
            if inputs is not None:
                inputs_keys = list(step.inputs.keys())
                inputs_cls = dataclasses.make_dataclass(
                    'Inputs', inputs_keys)
                inputs = inputs_cls(**{k: inputs.get(k, None)
                                       for k in inputs_keys})

            outputs = self._get_step_output_values(step_id)
            if outputs is not None:
                outputs_keys = list(step.outputs.keys())
                outputs_cls = dataclasses.make_dataclass(
                    'Outputs', outputs_keys)
                outputs = outputs_cls(
                    **{k: outputs.get(k, None) for k in outputs_keys})

            if inputs is not None and outputs is not None:
                fn(inputs, outputs)

                updated_inputs: Dict[str, List[str]] = defaultdict(list)

                items = dataclasses.asdict(outputs).items()
                for output_id, output_value in items:
                    io = step.outputs.get(output_id)
                    if io is not None and io.connection is not None:
                        conn_step_id = io.connection.step_id
                        conn_input_id = snakecase(io.connection.io_id)
                        self._steps_input_values[
                            conn_step_id][conn_input_id] = output_value
                        updated_inputs[conn_step_id].append(conn_input_id)
                    else:
                        self._steps_output_values[step_id][output_id] = \
                            output_value

                for step_id, input_ids in updated_inputs.items():
                    self.step_input_values_updated.publish(
                        UpdatedIO(step_id, io_ids=input_ids))

    def _process_all_steps(self) -> None:
        if self._current_workflow is None:
            return None

        for s in self._current_workflow.structure.steps:
            self._process_step(s.id)

    def get_step_input_values(
        self,
        step_id: str,
        input_ids: Optional[List[str]] = None,
        include_tabular: Optional[bool] = None
    ) -> Optional[Dict]:
        step = self._get_step(step_id)
        if step is None:
            return None

        def get_value(input_id):
            if self.is_tabular_input(step_id, input_id) \
                    and include_tabular is not True:
                return None
            if input_id in self._steps_input_values[step_id]:
                return self._steps_input_values[step_id][input_id]
            else:
                i = step.inputs.get(input_id, None)
                if i is not None and i.default_value is not None:
                    return i.default_value
            return None

        returned_inputs_ids = input_ids or list(step.inputs.keys())
        values = {
            i: get_value(i)
            for i in returned_inputs_ids
        }

        return {k: v for k, v in values.items() if v is not None}

    def update_step_input_values(
        self,
        step_id: str,
        input_values: Optional[Dict]
    ) -> Optional[Dict]:
        for k, v in (input_values or {}).items():
            self._steps_input_values[step_id][k] = v
        return input_values

    def get_step_tabular_input_value(
        self,
        step_id: str,
        input_id: str,
        filter: Optional[DataTabularDataFilter] = None
    ) -> Optional['Table']:
        if self._current_workflow is None:
            return None

        if input_id not in self._steps_input_values[step_id]:
            return None

        table: 'Table' = self._steps_input_values[step_id][input_id]

        if filter:
            return table.slice(filter.offset or 0, filter.page_size)
        else:
            return table

    def is_tabular_input(self, step_id: str, input_id: str) -> bool:
        if self._current_workflow is None:
            return False

        step = self._get_step(step_id)

        if step is not None and input_id in step.inputs:
            return step.inputs[input_id].is_tabular or False
        return True

    def _get_step_output_values(
        self,
        step_id: str,
        output_ids: Optional[List[str]] = None
    ) -> Optional[Dict]:
        step = self._get_step(step_id)
        if step is None:
            return None

        def get_value(output_id):
            if output_id in self._steps_output_values[step_id]:
                return self._steps_output_values[step_id][output_id]
            else:
                io = step.outputs.get(output_id)
                if io is not None and io.connection is not None:
                    connection_step_id = io.connection.step_id
                    connection_input_id = snakecase(io.connection.io_id)
                    v = self._steps_input_values[connection_step_id].get(
                        connection_input_id, None)
                    if v is not None:
                        return v
                elif io is not None and io.default_value is not None:
                    return io.default_value
            return None

        returned_outputs_ids = output_ids or list(step.outputs.keys())
        values = {
            i: get_value(i)
            for i in returned_outputs_ids
        }

        return {k: v for k, v in values.items() if v is not None}

    def run_processing(self, step_id: Optional[str] = None):
        try:
            self.processing_state_changed.publish(State.BUSY)
            if step_id is not None:
                self._process_step(step_id)
            else:
                self._process_all_steps()
        finally:
            self.processing_state_changed.publish(State.IDLE)
