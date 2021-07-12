from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from lumy_middleware.context.dataregistry import DataRegistry
from lumy_middleware.types import State
from lumy_middleware.types.generated import DataTabularDataFilter, LumyWorkflow
from tinypubsub.simple import SimplePublisher


@dataclass
class UpdatedIO:
    step_id: str
    io_ids: List[str]


class AppContext(ABC):
    '''
    Application context interface that needs to be implemented for
    a particular backend. There are likely only two backends:
    "kiara" and "mock".
    '''

    _event_workflow_updated = SimplePublisher[LumyWorkflow]()
    _event_step_input_values_updated = SimplePublisher[UpdatedIO]()
    _event_step_output_values_updated = SimplePublisher[UpdatedIO]()
    _event_processing_state_changed = SimplePublisher[State]()

    @abstractmethod
    def load_workflow(
        self,
        workflow_path_or_content: Union[Path, LumyWorkflow]
    ) -> None:
        '''
        Load workflow and set it as the current workflow.
        A synchronous method which should raise an exception if
        something goes wrong. When the method returns - the workflow
        is ready to use.
        '''
        ...

    @property
    @abstractmethod
    def current_workflow(self) -> Optional[LumyWorkflow]:
        '''
        Returns current workflow structure or `None` if no
        workflow has been loaded.
        '''
        ...

    @property
    def workflow_updated(self) -> SimplePublisher[LumyWorkflow]:
        '''
        Event fired whenever current workflow structure is updated.
        This happens either when the user changes the structure or when
        the workflow is loaded.
        '''
        return self._event_workflow_updated

    @abstractmethod
    def get_step_input_value(
        self,
        step_id: str,
        input_id: str,
        filter: Optional[DataTabularDataFilter] = None
    ) -> Tuple[Any, Any]:
        '''
        Return value for a step input along with its stats:
        [value, stats]

        NOTE: There are 2 types of input values: simple (scalar) and complex.

        For simple values the first item (value) is always the ful actual value
        and the stats is `None`.

        For complex types the first item is a filtered value if filter if
        provided, otherwise it is `None`. Stats are always returned
        for complex values.
        '''
        ...

    @abstractmethod
    def get_step_output_value(
        self,
        step_id: str,
        output_id: str,
        filter: Optional[DataTabularDataFilter] = None
    ) -> Tuple[Any, Any]:
        '''
        Return value for a step input along with its stats:
        [value, stats]

        NOTE: There are 2 types of input values: simple (scalar) and complex.

        For simple values the first item (value) is always the ful actual value
        and the stats is `None`.

        For complex types the first item is a filtered value if filter if
        provided, otherwise it is `None`. Stats are always returned
        for complex values.
        '''
        ...

    @abstractmethod
    def update_step_input_values(
        self,
        step_id: str,
        input_values: Optional[Dict[str, Any]]
    ):
        '''
        Update input values for a step. The values dict may not contain
        all the values to be updated.

        Connected values should not be updated.
        '''
        ...

    @property
    def step_input_values_updated(self) -> SimplePublisher[UpdatedIO]:
        '''
        Event fired when input values have been updated.
        The payload contains only input ids without values.
        '''
        return self._event_step_input_values_updated

    @property
    def step_output_values_updated(self) -> SimplePublisher[UpdatedIO]:
        '''
        Event fired when output values have been updated.
        The payload contains only output ids without values.
        '''
        return self._event_step_output_values_updated

    @abstractmethod
    def run_processing(self, step_id: Optional[str] = None):
        '''
        Run processing of data through the whole workflow.
        '''
        ...

    @property
    def processing_state_changed(self) -> SimplePublisher[State]:
        '''
        Fired when processing state is changed.
        '''
        return self._event_processing_state_changed

    @property
    @abstractmethod
    def data_registry(self) -> DataRegistry:
        '''
        Returns data registry.
        '''
        ...
