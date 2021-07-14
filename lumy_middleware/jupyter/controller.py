import json
import logging
import pathlib
import sys
from enum import Enum
from typing import Any, Dict, Optional
from uuid import uuid4

import pyarrow as pa
from ipykernel.comm import Comm
from IPython import get_ipython
from lumy_middleware import dev as dev_pkg
from lumy_middleware.context.context import AppContext
from lumy_middleware.context.kiara.app_context import KiaraAppContext
from lumy_middleware.jupyter.base import (MessageEnvelope, MessageHandler,
                                          Target, TargetPublisher)
from lumy_middleware.jupyter.message_handlers import (ActivityHandler,
                                                      DataRepositoryHandler,
                                                      ModuleIOHandler,
                                                      NotesHandler,
                                                      WorkflowMessageHandler)
from lumy_middleware.types.generated import MsgError
from lumy_middleware.utils.codec import serialize_table
from lumy_middleware.utils.dataclasses import to_dict
from lumy_middleware.utils.json import object_as_json

logger = logging.getLogger(__name__)

logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def preprocess_dict(d):
    if d is None:
        return d

    def val(v):
        if isinstance(v, dict):
            return preprocess_dict(v)
        elif isinstance(v, pa.Table):
            return serialize_table(v)
        elif isinstance(v, list):
            return [val(i) for i in v]
        else:
            if isinstance(v, Enum):
                return v.value
            return v

    return {
        k: val(v)
        for k, v in d.items()
    }


def get_dev_workflow_path():
    return pathlib.Path(dev_pkg.__file__).parent / "resources" / \
        "networkAnalysisWorkflowNew.yml"


class IpythonKernelController(TargetPublisher):
    __instance = None

    _comms: Dict[Target, Comm] = {}
    _module_id: str
    _is_ready = False

    _context: AppContext

    _handlers: Dict[Target, MessageHandler] = {}

    @staticmethod
    def start():
        if IpythonKernelController.get_instance() is None:
            IpythonKernelController()

    @staticmethod
    def get_instance():
        return IpythonKernelController.__instance

    def __init__(self):
        super().__init__()
        context = KiaraAppContext()
        context.load_workflow(get_dev_workflow_path())
        self._context = context

        self._handlers = {
            Target.Workflow: WorkflowMessageHandler(
                self._context, self, Target.Workflow),
            Target.ModuleIO: ModuleIOHandler(
                self._context, self, Target.ModuleIO),
            Target.Activity: ActivityHandler(
                self._context, self, Target.Activity),
            Target.DataRepository: DataRepositoryHandler(
                self._context, self, Target.DataRepository),
            Target.Notes: NotesHandler(
                self._context, self, Target.Notes),
        }

        def _open_handle_factory(target: Target):
            def _open_handle(comm: Comm, open_msg):
                self._comms[target] = comm

                def _recv(msg):
                    response = self._handle_message(target, msg)
                    if response is not None:
                        data = preprocess_dict(to_dict(response))
                        logger.debug(
                            f'Sending response on {target.value} {data}')
                        comm.send(data)

                _recv(open_msg)
                comm.on_msg(_recv)

            return _open_handle

        get_ipython().kernel.comm_manager.register_target(
            Target.Activity.value,
            _open_handle_factory(Target.Activity)
        )

        get_ipython().kernel.comm_manager.register_target(
            Target.Workflow.value,
            _open_handle_factory(Target.Workflow)
        )

        get_ipython().kernel.comm_manager.register_target(
            Target.ModuleIO.value,
            _open_handle_factory(Target.ModuleIO)
        )

        get_ipython().kernel.comm_manager.register_target(
            Target.DataRepository.value,
            _open_handle_factory(Target.DataRepository)
        )

        get_ipython().kernel.comm_manager.register_target(
            Target.Notes.value,
            _open_handle_factory(Target.Notes)
        )

        self._is_ready = True
        IpythonKernelController.__instance = self

    @property
    def is_ready(self):
        return self._is_ready

    def publish_on_target(self, target: Target, msg: MessageEnvelope) -> None:
        comm = self._comms[target]
        ready_msg = preprocess_dict(to_dict(msg))
        msg_str = json.dumps(ready_msg)
        logger.debug(
            f'Message published on "{target}": {msg_str}')
        comm.send(ready_msg)

    def _handle_message(self, target: Target, message: Dict) -> Optional[Any]:
        message_data: Dict = message.get('content', {}).get('data', {})
        msg_str = json.dumps(message_data)
        logger.debug(
            f'Message received on "{target}": {msg_str}')

        if message_data.get('action', None) is None:
            logger.warn(
                f'Received a message with no "action" field on \
                    target "{target}": {object_as_json(message)}'
            )
            return None

        try:
            msg = MessageEnvelope(**message_data)

            handler = self._handlers[target]

            if handler is None:
                logger.warn(f'No handler found for target "{target}"')
            else:
                return handler(msg)
        except Exception as e:
            error_id = str(uuid4())
            logger.exception(
                f'''{error_id}: Error occured while executing a message
                handler for target "{target}" and message
                {json.dumps(message_data)}'''
            )
            self.publish(MsgError(
                id=error_id,
                message=f'Error occured while executing a message \
                        handler for target "{target}": {str(e)}'
            ))
        return None
