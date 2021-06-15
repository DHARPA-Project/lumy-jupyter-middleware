# flake8: noqa
from dataclasses import dataclass
from typing import Optional, List, Any, Dict, Union
from enum import Enum


@dataclass
class MsgError:
    """Target: "activity"
    Message type: "Error"
    
    Indicates that an error occured and contains error details.
    """
    """Unique ID of the error, for traceability."""
    id: str
    """User friendly error message."""
    message: str
    """A less user friendly error message. Optional."""
    extended_message: Optional[str] = None


class State(Enum):
    """Current state."""
    BUSY = "busy"
    IDLE = "idle"


@dataclass
class MsgExecutionState:
    """Target: "activity"
    Message type: "ExecutionState"
    
    Announces current state of the backend. Useful for letting the user know if they need to
    wait.
    """
    """Current state."""
    state: State


@dataclass
class MsgProgress:
    """Target: "activity"
    Message type: "Progress"
    
    Announces progress of current operation to the frontend.
    """
    """Progress in percents."""
    progress: float


@dataclass
class MsgDataRepositoryCreateSubset:
    """Target: "dataRepository"
    Message type: "CreateSubset"
    
    Request to create a subset of items
    """
    """List of items IDs to add to the subset"""
    items_ids: List[str]
    """Label of the subset"""
    label: str


@dataclass
class DataRepositoryItemsFilter:
    """Filter to apply to items"""
    """Start from item"""
    offset: Optional[int] = None
    """Number of items to return"""
    page_size: Optional[int] = None
    """Limit the result to these data types."""
    types: Optional[List[str]] = None


@dataclass
class MsgDataRepositoryFindItems:
    """Target: "dataRepository"
    Message type: "FindItems"
    
    Request to find items in data repository
    """
    filter: DataRepositoryItemsFilter


@dataclass
class MsgDataRepositoryItems:
    """Target: "dataRepository"
    Message type: "Items"
    
    Items from data repository.
    """
    filter: DataRepositoryItemsFilter
    """Serialized table with items as rows."""
    items: Any
    """Stats of the data repository."""
    stats: Dict[str, Any]


@dataclass
class MsgDataRepositorySubset:
    """Target: "dataRepository"
    Message type: "Subset"
    
    A subset of items
    """
    """Unique ID of the subset"""
    id: str
    """List of items IDs to add to the subset"""
    items_ids: List[str]
    """Label of the subset"""
    label: str


@dataclass
class MsgModuleIOExecute:
    """Target: "moduleIO"
    Message type: "Execute"
    
    Run this step with the latest used parameters on all data (not preview only).
    """
    """Unique ID of the step within the workflow."""
    id: str


@dataclass
class DataTabularDataFilterItem:
    """Filter condition item"""
    """Id of the column to filter"""
    column: str
    """Filter operator"""
    operator: str
    """Value for the operator"""
    value: Any


class Operator(Enum):
    """Operator used to combine items"""
    AND = "and"
    OR = "or"


@dataclass
class DataTabularDataFilterCondition:
    """Condition items"""
    items: List[DataTabularDataFilterItem]
    """Operator used to combine items"""
    operator: Operator


class Direction(Enum):
    """sorting direction"""
    ASC = "asc"
    DEFAULT = "default"
    DESC = "desc"


@dataclass
class DataTabularDataSortingMethod:
    """Sorting method"""
    """Id of the column to filter"""
    column: str
    """sorting direction"""
    direction: Optional[Direction] = None


@dataclass
class DataTabularDataFilter:
    """Filter for tabular data"""
    condition: Optional[DataTabularDataFilterCondition] = None
    """Whether to ignore other filter items and return full value."""
    full_value: Optional[bool] = None
    """Offset of the page"""
    offset: Optional[int] = None
    """Size of the page"""
    page_size: Optional[int] = None
    sorting: Optional[DataTabularDataSortingMethod] = None


@dataclass
class MsgModuleIOGetInputValue:
    """Target: "moduleIO"
    Message type: "GetInputValue"
    
    Get value of a step input from the current workflow.
    This is a 'pull' request meaning that a synchronous response will be returned. The
    behaviour of the response is different depending on whether it is a simple or complex
    value.
    For simple values the filter is ignored and full value is always returned.
    For complex values only stats are returned unless 'filter' is set and is not empty.
    """
    """ID of the input"""
    input_id: str
    """Unique ID of the step within the workflow that we are getting parameters for."""
    step_id: str
    filter: Optional[DataTabularDataFilter] = None


@dataclass
class MsgModuleIOGetOutputValue:
    """Target: "moduleIO"
    Message type: "GetOutputValue"
    
    Get value of a step output from the current workflow.
    This is a 'pull' request meaning that a synchronous response will be returned. The
    behaviour of the response is different depending on whether it is a simple or complex
    value.
    For simple values the filter is ignored and full value is always returned.
    For complex values only stats are returned unless 'filter' is set and is not empty.
    """
    """ID of the output"""
    output_id: str
    """Unique ID of the step within the workflow that we are getting parameters for."""
    step_id: str
    filter: Optional[DataTabularDataFilter] = None


@dataclass
class MsgModuleIOGetPreview:
    """Target: "moduleIO"
    Message type: "GetPreview"
    
    Get preview of I/O data of a step from the current workflow.
    """
    """Unique ID of the step within the workflow that we are getting preview for."""
    id: str


@dataclass
class MsgModuleIOInputValue:
    """Target: "moduleIO"
    Message type: "InputValue"
    
    Response to GetInputValue 'pull' request.
    Contains value and stats for an input.
    """
    """ID of the input"""
    input_id: str
    """Unique ID of the step within the workflow that we are getting parameters for."""
    step_id: str
    """Type of the input value"""
    type: str
    """Actual serialized value.
    It may be undefined if not set. It may be a filtered value in case of a complex value.
    Filter is also returned if the value is filtered.
    """
    value: Any
    filter: Optional[DataTabularDataFilter] = None
    """Stats of the value if applicable. Simple types usually do not include stats.
    Complex ones like table do.
    """
    stats: Optional[Dict[str, Any]] = None


@dataclass
class MsgModuleIOInputValuesUpdated:
    """Target: "moduleIO"
    Message type: "InputValuesUpdated"
    
    Input IDs of a step in the current workflow that had their values updated.
    """
    """IDs of inputs that had their values updated."""
    input_ids: List[str]
    """Unique ID of the step within the workflow."""
    step_id: str


@dataclass
class MsgModuleIOOutputValue:
    """Target: "moduleIO"
    Message type: "OutputValue"
    
    Response to GetOutputValue 'pull' request.
    Contains value and stats for an output.
    """
    """ID of the output"""
    output_id: str
    """Unique ID of the step within the workflow that we are getting parameters for."""
    step_id: str
    """Type of the output value"""
    type: str
    """Actual serialized value.
    It may be undefined if not set. It may be a filtered value in case of a complex value.
    Filter is also returned if the value is filtered.
    """
    value: Any
    filter: Optional[DataTabularDataFilter] = None
    """Stats of the value if applicable. Simple types usually do not include stats.
    Complex ones like table do.
    """
    stats: Optional[Dict[str, Any]] = None


@dataclass
class MsgModuleIOOutputValuesUpdated:
    """Target: "moduleIO"
    Message type: "OutputValuesUpdated"
    
    Output IDs of a step in the current workflow that had their values updated.
    """
    """IDs of outputs that had their values updated."""
    output_ids: List[str]
    """Unique ID of the step within the workflow."""
    step_id: str


@dataclass
class MsgModuleIOPreviewUpdated:
    """Target: "moduleIO"
    Message type: "PreviewUpdated"
    
    Contains preview of I/O data of a step from the current workflow.
    """
    """Unique ID of the step within the workflow that the preview is for."""
    id: str
    """Input data of the module. Key is input Id."""
    inputs: Dict[str, Any]
    """Output data of the module. Key is input Id."""
    outputs: Dict[str, Any]


class DataType(Enum):
    """Type of the data value."""
    SIMPLE = "simple"
    TABLE = "table"


@dataclass
class DataValueContainer:
    """Used to send serialized data from front end to back end."""
    """Type of the data value."""
    data_type: DataType
    """Actual serialized value."""
    value: Any


@dataclass
class MsgModuleIOUpdateInputValues:
    """Target: "moduleIO"
    Message type: "UpdateInputValues"
    
    Update input values of a step in the current workflow.
    Only disconnected values can be updated.
    """
    """Unique ID of the step within the workflow."""
    step_id: str
    """Input values."""
    input_values: Optional[Dict[str, DataValueContainer]] = None


@dataclass
class MsgModuleIOUpdatePreviewParameters:
    """Target: "moduleIO"
    Message type: "UpdatePreviewParameters"
    
    Update preview parameters (or preview filters) for the current workflow.
    """
    """Size of the preview"""
    size: Optional[int] = None


@dataclass
class Note:
    """Represents a step note."""
    """Textual content of the note."""
    content: str
    """When the note was created. Must be an ISO string."""
    created_at: str
    """Unique ID of the note."""
    id: str
    """Optional title of the note"""
    title: Optional[str] = None


@dataclass
class MsgNotesAdd:
    """Target: "notes"
    Message type: "Add"
    
    Add a note for a workflow step.
    """
    note: Note
    """Workflow step Id."""
    step_id: str


@dataclass
class MsgNotesDelete:
    """Target: "notes"
    Message type: "Delete"
    
    Delete a note by Id.
    """
    """Note Id."""
    note_id: str
    """Workflow step Id."""
    step_id: str


@dataclass
class MsgNotesGetNotes:
    """Target: "notes"
    Message type: "GetNotes"
    
    Get list of notes for a workflow step.
    """
    """Workflow step Id."""
    step_id: str


@dataclass
class MsgNotesNotes:
    """Target: "notes"
    Message type: "Notes"
    
    Contains list of notes for a workflow step.
    """
    notes: List[Note]
    """Workflow step Id."""
    step_id: str


@dataclass
class MsgNotesUpdate:
    """Target: "notes"
    Message type: "Update"
    
    Update a note for a workflow step.
    """
    note: Note
    """Workflow step Id."""
    step_id: str


@dataclass
class MsgParametersCreateSnapshot:
    """Target: "parameters"
    Message type: "CreateSnapshot"
    
    Create snapshot of parameters of a step from the current workflow.
    """
    """Optional parameters of the step."""
    parameters: Dict[str, Any]
    """Unique ID of the step within the workflow."""
    step_id: str


@dataclass
class MsgParametersSnapshots:
    """Target: "parameters"
    Message type: "Snapshots"
    
    List of snapshots for a step from the current workflow.
    """
    """List of snapshots."""
    snapshots: List[Any]
    """Unique ID of the step within the workflow."""
    step_id: str


@dataclass
class MsgWorkflowUpdated:
    """Target: "workflow"
    Message type: "Updated"
    
    Contains current workflow.
    """
    """Current workflow state. Type: PipelineState from
    https://dharpa.org/kiara/development/entities/modules/PipelineState.json .
    Not using it as a reference because of a code generation bug.
    """
    workflow: Optional[Dict[str, Any]] = None


@dataclass
class TableStats:
    """Stats object for arrow table"""
    """Number of rows."""
    rows_count: int


@dataclass
class IOStateConnection:
    """Incoming or outgoing connection of a module"""
    """ID of the input or output"""
    io_id: str
    """ID of the step"""
    step_id: str


@dataclass
class WorkflowIOState:
    """State of a single input or output."""
    """Optional default value"""
    default_value: Union[List[Any], bool, float, int, Dict[str, Any], None, str]
    connection: Optional[IOStateConnection] = None
    """Indicates whether the value is tabular. This field will likely be gone in real backend."""
    is_tabular: Optional[bool] = None


@dataclass
class WorkflowStep:
    """A single Workflow step."""
    """Unique ID of the step within the workflow."""
    id: str
    """State of module inputs of the step. Key is stepId."""
    inputs: Dict[str, WorkflowIOState]
    """ID of the module that is used in this step."""
    module_id: str
    """State of module outputs of the step. Key is stepId."""
    outputs: Dict[str, WorkflowIOState]


@dataclass
class WorkflowStructure:
    """Modular structure of the workflow.
    
    Workflow structure. Contains all modules that are a part of the workflow.
    """
    """Steps of the workflow."""
    steps: List[WorkflowStep]


@dataclass
class Workflow:
    """Represents a workflow."""
    """Unique ID of the workflow."""
    id: str
    """Human readable name of the workflow."""
    label: str
    """Modular structure of the workflow."""
    structure: WorkflowStructure
