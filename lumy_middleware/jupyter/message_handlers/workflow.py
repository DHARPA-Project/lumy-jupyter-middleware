import logging

from lumy_middleware.jupyter.base import MessageHandler
from lumy_middleware.types import MsgWorkflowUpdated
from lumy_middleware.utils.dataclasses import to_dict

logger = logging.getLogger(__name__)


class WorkflowMessageHandler(MessageHandler):

    def _handle_GetCurrent(self):
        '''
        Return current workflow.
        '''
        self.publisher.publish(MsgWorkflowUpdated(
            None if self._context.current_workflow_structure is None
            else to_dict(self._context.current_workflow_structure)
        ))
