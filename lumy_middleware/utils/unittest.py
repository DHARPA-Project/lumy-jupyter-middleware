import logging
import unittest

from lumy_middleware.standalone.controller import (StandaloneController,
                                                   StandaloneControllerClient)


class ControllerTestCase(unittest.TestCase):
    controller: StandaloneController

    def setUp(self):
        self._old_log_level = logging.getLogger().getEffectiveLevel()
        logging.getLogger().setLevel(logging.WARN)

        self.controller = StandaloneController()

    def tearDown(self):
        del self.controller
        logging.getLogger().setLevel(self._old_log_level)

    @property
    def client(self) -> StandaloneControllerClient:
        return self.controller.client
