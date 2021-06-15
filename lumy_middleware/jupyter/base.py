import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from inspect import signature
from typing import Any, Dict, Optional

from lumy_middleware.context.context import AppContext
from lumy_middleware.target import Target
from lumy_middleware.types import target_action_mapping
from lumy_middleware.utils.dataclasses import from_dict, to_dict

logger = logging.getLogger(__name__)


@dataclass
class MessageEnvelope:
    action: str
    content: Optional[Dict] = None


def prepare_result(message: Any) -> Optional[MessageEnvelope]:
    if message is None:
        return None
    action = getattr(message.__class__, '_action', None)
    assert action is not None, f'Message of class {message.__class__} \
        does not have "_action" property'
    return MessageEnvelope(
        action=action,
        content=to_dict(message)
    )


class TargetPublisher(ABC):
    @abstractmethod
    def publish_on_target(self, target: Target, msg: MessageEnvelope) -> None:
        ...

    def publish(self, message: Any) -> None:
        '''A convenience method that picks action and target
        from the "message" class. See `types.__init__` for more
        details on how metadata is assigned.'''

        action = getattr(message.__class__, '_action', None)
        target = getattr(message.__class__, '_target', None)
        assert action is not None, f'Message of class {message.__class__} \
            does not have "_action" property'
        assert target is not None, f'Message of class {message.__class__} \
            does not have "_target" property'

        self.publish_on_target(
            target,
            MessageEnvelope(
                action=action,
                content=to_dict(message)
            )
        )


class MessageHandler(ABC):
    _context: AppContext
    _publisher: TargetPublisher
    _target: Target

    # for performance reasons to avoid introspecting all the time
    # we store handler arguments count in this dict.
    _handler_method_args_count_cache: Dict[Any, int] = {}

    def __init__(self,
                 context: AppContext,
                 publisher: TargetPublisher,
                 target: Target):
        self._context = context
        self._publisher = publisher
        self._target = target

        self.initialize()

    def initialize(self):
        pass

    @property
    def publisher(self):
        return self._publisher

    @property
    def context(self):
        return self._context

    def __handler_needs_message(self, handler: Any):
        if handler not in self._handler_method_args_count_cache:
            sig = signature(handler)
            self._handler_method_args_count_cache[handler] = len(
                sig.parameters)
        return self._handler_method_args_count_cache[handler] > 0

    def handle_message(self, msg: MessageEnvelope):
        handler = getattr(self, f'_handle_{msg.action}', None)
        if handler:
            message_class = target_action_mapping[self._target].get(
                msg.action, None)
            if message_class is None:
                # If no message class has been found it might be that
                # the handler does not need a message.
                if not self.__handler_needs_message(handler):
                    result = handler()
                    return prepare_result(result)
                else:
                    logger.warn(
                        f'{self.__class__}: \
                            No message type class found for message \
                                action: {msg.action}')
            else:
                message = from_dict(message_class, msg.content)
                result = handler(message)
                return prepare_result(result)
        else:
            logger.warn(
                f'{self.__class__}: \
                    Unknown message type received: {msg.action}')

    def __call__(self, msg: MessageEnvelope):
        return self.handle_message(msg)
