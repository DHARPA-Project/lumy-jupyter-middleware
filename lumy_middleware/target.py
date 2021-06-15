from enum import Enum


class Target(Enum):
    Activity = 'activity'
    Workflow = 'workflow'
    ModuleIO = 'module_io'
    DataRepository = 'data_repository'
    Parameters = 'parameters'
    Notes = 'notes'
