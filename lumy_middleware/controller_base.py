import json
import logging
import traceback
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Optional
from uuid import uuid4

from lumy_middleware.context.context import AppContext
from lumy_middleware.jupyter.base import (MessageEnvelope, MessageHandler,
                                          Target, TargetPublisher)
from lumy_middleware.jupyter.message_handlers import (ActivityHandler,
                                                      DataRepositoryHandler,
                                                      ModuleIOHandler,
                                                      NotesHandler,
                                                      WorkflowMessageHandler)
from lumy_middleware.types.generated import MsgError
from lumy_middleware.utils.dataclasses import to_dict
from lumy_middleware.utils.json import object_as_json

logger = logging.getLogger(__name__)


def preprocess_dict(d):
    if d is None:
        return d

    def val(v):
        if isinstance(v, dict):
            return preprocess_dict(v)
        elif isinstance(v, list):
            return [val(i) for i in v]
        elif isinstance(v, Enum):
            return v.value
        else:
            return v

    return {
        k: val(v)
        for k, v in d.items()
    }


class ControllerBase(TargetPublisher, ABC):
    '''
    Base class for controllers that sit between the transport
    to/from middleware client and the business logic of the middleware.

    Concrete subclasses only need to provide implementation of transport
    oriented methods like conversion between Lumy message envelope
    and the transport message, publishing messages to client and subscribing
    to client messages.
    '''
    _context: AppContext
    _handlers: Dict[Target, MessageHandler] = {}

    def __init__(self, context: AppContext):
        super().__init__()

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

        for target in self._handlers.keys():
            self.subscribe_to_client(target)

    @abstractmethod
    def as_transport_message(self, msg_envelope: Dict) -> Any:
        '''
        Wrap preprocessed message envelope into a transport envelope.
        '''
        ...

    @abstractmethod
    def from_transport_message(self, msg: Any) -> Optional[MessageEnvelope]:
        '''
        Get actual message from a transport envelope.
        '''
        ...

    @abstractmethod
    def publish_to_client(self, target: Target, transport_msg: Any) -> None:
        '''
        Publish a transport message to client.
        '''
        ...

    @abstractmethod
    def subscribe_to_client(self, target: Target):
        '''
        Subscribe to client messages.
        '''
        ...

    def publish_on_target(self, target: Target, msg: MessageEnvelope) -> None:
        '''
        Publish on target.
        '''
        msg_envelope = preprocess_dict(to_dict(msg))

        if logger.getEffectiveLevel() <= logging.DEBUG:
            # do not serialise message if debug logging is not enabled
            msg_str = json.dumps(msg_envelope)
            if len(msg_str) > 1000:
                msg_str = msg_str[0:997] + '...'
            logger.debug(
                f'Message published on "{target}": {msg_str}')

        transport_msg = self.as_transport_message(msg_envelope)
        self.publish_to_client(target, transport_msg)

    def handle_client_message(self,
                              target: Target,
                              transport_msg: Any) -> Optional[Any]:
        msg_envelope = None
        try:
            msg_envelope = self.from_transport_message(transport_msg)

            if msg_envelope is None:
                logger.warn(
                    'Received a message that cannot be parsed on ' +
                    f'target "{target}": {object_as_json(transport_msg)}'
                )
                return None

            if logger.getEffectiveLevel() <= logging.DEBUG:
                # do not serialise message if debug logging is not enabled
                msg_str = json.dumps(to_dict(msg_envelope))
                logger.debug(
                    f'Message received on "{target}": {msg_str}')

            handler = self._handlers[target]

            if handler is None:
                logger.warn(f'No handler found for target "{target}"')
            else:
                response_msg = handler(msg_envelope)
                if response_msg is not None:
                    data = preprocess_dict(to_dict(response_msg))
                    logger.debug(
                        f'Sending response on {target.value} {data}')
                    self.publish_on_target(target, data)

        except Exception as e:
            stack = '\n'.join(traceback.format_exception(
                None, e, e.__traceback__))
            error_id = str(uuid4())
            msg_obj = to_dict(msg_envelope) \
                if msg_envelope is not None \
                else object_as_json(transport_msg)
            logger.exception(
                f'''{error_id}: Error occured while processing a message
                handler for target "{target}" and message
                {json.dumps(msg_obj)}'''
            )
            self.publish(MsgError(
                id=error_id,
                message=f'Error occured while executing a message \
                        handler for target "{target}": {str(e)}',
                extended_message=stack
            ))
        return None
