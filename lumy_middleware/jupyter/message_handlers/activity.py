import logging

from lumy_middleware.jupyter.base import MessageHandler
from lumy_middleware.types.generated import MsgExecutionState, State

logger = logging.getLogger(__name__)


class ActivityHandler(MessageHandler):

    def initialize(self):
        self.context.processing_state_changed.subscribe(self._on_state_changed)

    def _on_state_changed(self, state: State):
        self.publisher.publish(MsgExecutionState(state))
