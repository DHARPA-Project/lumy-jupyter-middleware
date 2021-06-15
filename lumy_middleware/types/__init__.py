from collections import defaultdict
from dataclasses import is_dataclass
from typing import Dict, Type

from dataclasses_json import LetterCase, dataclass_json

from ..target import Target
from .generated import *  # noqa

target_action_mapping: Dict[Target, Dict[str, Type]] = defaultdict(dict)

# Adding "action" and "target" message class metdata
# to make it easier to deal with publishing of the messages

for k, v in list(globals().items()):
    if is_dataclass(v):
        dataclass_json(v, letter_case=LetterCase.CAMEL)
    if k.startswith('Msg'):
        if k.startswith('MsgModuleIO'):
            v._action = k.replace('MsgModuleIO', '')
            v._target = Target.ModuleIO
        elif k.startswith('MsgWorkflow'):
            v._action = k.replace('MsgWorkflow', '')
            v._target = Target.Workflow
        elif k.startswith('MsgDataRepository'):
            v._action = k.replace('MsgDataRepository', '')
            v._target = Target.DataRepository
        elif k.startswith('MsgParameters'):
            v._action = k.replace('MsgParameters', '')
            v._target = Target.Parameters
        elif k.startswith('MsgNotes'):
            v._action = k.replace('MsgNotes', '')
            v._target = Target.Notes
        else:
            v._action = k.replace('Msg', '')
            v._target = Target.Activity

        target_action_mapping[v._target][v._action] = v
