import logging
import sys
from typing import Any, Dict, Optional

from ipykernel.comm import Comm
from IPython import get_ipython
from lumy_middleware.context.context import AppContext
from lumy_middleware.context.kiara.app_context import KiaraAppContext
from lumy_middleware.controller_base import ControllerBase
from lumy_middleware.jupyter.base import (
    MessageEnvelope, MessageHandler, Target)

logger = logging.getLogger(__name__)


class IpythonKernelController(ControllerBase):
    '''
    IPython Kernel Comm Manager based controller implementation.

    This is the controller used by Lumy web app.

    **NOTE**:

    The name and location of this class is used in Lumy web app
    to refer to the controller at the very beginning when the application
    is started. If it is changed, it should also be changed in
    "jupyter-support" package in Lumy web app.

    The web app uses the following methods of this class/instance:
     - get_instance()
     - is_ready()
     - start()
    '''
    __instance = None

    _comms: Dict[Target, Comm] = {}
    _is_ready = False

    _context: AppContext

    _handlers: Dict[Target, MessageHandler] = {}

    @ staticmethod
    def start():
        if IpythonKernelController.get_instance() is None:
            # Enable debug messages for Lumy web app
            logging.basicConfig(level=logging.DEBUG, handlers=[
                                logging.StreamHandler(sys.stdout)])

            IpythonKernelController.__instance = IpythonKernelController()

    @ staticmethod
    def get_instance():
        return IpythonKernelController.__instance

    @ property
    def is_ready(self):
        return self._is_ready

    def __init__(self, context: Optional[AppContext] = None):
        if context is None:
            context = KiaraAppContext()
        super().__init__(context)
        self._is_ready = True

    def as_transport_message(self, msg_envelope: Dict) -> Any:
        # IPython kernel CommManager will do the job of
        # creating a transport level message out of this message
        return msg_envelope

    def from_transport_message(self, msg: Any) -> Optional[MessageEnvelope]:
        if msg is None:
            return None
        content = msg.get('content', {}).get('data', {})
        if content is None:
            return None
        if content.get('action') is None:
            return None
        return MessageEnvelope(**content)

    def publish_to_client(self, target: Target, transport_msg: Any) -> None:
        if target not in self._comms:
            logger.warning('Cannot publish to client. ' +
                           f'No channel found for target "{target.value}".')
            return None
        self._comms[target].send(transport_msg)

    def subscribe_to_client(self, target: Target):
        def _open_handler(comm: Comm, open_msg: Any):
            self._comms[target] = comm

            def _recv(transport_msg: Any):
                self.handle_client_message(target, transport_msg)

            _recv(open_msg)
            comm.on_msg(_recv)

        get_ipython().kernel.comm_manager.register_target(
            target.value,
            _open_handler
        )
