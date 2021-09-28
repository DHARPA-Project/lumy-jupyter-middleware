import logging
from enum import Enum
from typing import Any, Callable, Dict, Optional

from lumy_middleware.context.context import AppContext
from lumy_middleware.context.kiara.app_context import KiaraAppContext
from lumy_middleware.controller_base import ControllerBase, preprocess_dict
from lumy_middleware.jupyter.base import MessageEnvelope, Target
from lumy_middleware.utils.dataclasses import to_dict
from tinypubsub import Subscription
from tinypubsub.simple import SimplePublisher

logger = logging.getLogger(__name__)


class Sender(Enum):
    Client = 'client'
    Server = 'server'


SENDER_FIELD = 'sender'
CONTENT_FIELD = 'content'


class StandaloneControllerClient:
    _controller: 'StandaloneController'

    def __init__(self, controller: 'StandaloneController'):
        self._controller = controller

    def subscribe(self,
                  target: Target,
                  handler: Callable[[Any], None]) -> Subscription:
        def _handler(transport_msg: Any):
            if transport_msg.get(SENDER_FIELD, None) != Sender.Server.value:
                return

            msg = self._controller.from_transport_message(transport_msg)
            handler(msg)

        return self._controller._channels[target].subscribe(_handler)

    def publish(self, target: Target, msg: MessageEnvelope):
        msg_envelope = preprocess_dict(to_dict(msg))
        transport_msg = self._controller.as_transport_message(msg_envelope)
        transport_msg[SENDER_FIELD] = Sender.Client.value
        self._controller._channels[target].publish(transport_msg)


class StandaloneController(ControllerBase):
    '''
    Controller that uses in-memory pub/sub channels based on
    tinypubsub.

    This controller is used in unit tests where we do not need
    to set up IPython transport.
    '''
    _channels: Dict[Target, SimplePublisher] = {}
    _client: StandaloneControllerClient

    def __init__(self, context: Optional[AppContext] = None):
        if context is None:
            context = KiaraAppContext()
        super().__init__(context)
        self._client = StandaloneControllerClient(self)

    def as_transport_message(self, msg_envelope: Dict) -> Any:
        return {
            SENDER_FIELD: Sender.Server.value,
            CONTENT_FIELD: msg_envelope
        }

    def from_transport_message(self, msg: Any) -> Optional[MessageEnvelope]:
        content = msg.get(CONTENT_FIELD, None)
        if content is None:
            return None
        return MessageEnvelope(**content)

    def publish_to_client(self, target: Target, transport_msg: Any) -> None:
        if target not in self._channels:
            logger.warning('Cannot publish to client. ' +
                           f'No channel found for target "{target.value}".')
            return None
        self._channels[target].publish(transport_msg)

    def subscribe_to_client(self, target: Target):
        if target not in self._channels:
            self._channels[target] = SimplePublisher()

        def _handler(transport_msg: Any):
            if transport_msg.get(SENDER_FIELD, None) != Sender.Server.value:
                self.handle_client_message(target, transport_msg)

        self._channels[target].subscribe(_handler)

    @property
    def client(self):
        return self._client
