import os
from pathlib import Path
import asyncio

from lumy_middleware.jupyter.base import MessageEnvelope
from lumy_middleware.target import Target
from lumy_middleware.types.generated import (
    MsgWorkflowLoadLumyWorkflow, MsgWorkflowLumyWorkflowLoadProgress,
    MsgWorkflowUpdated, MsgWorkflowLumyWorkflowLoadProgressStatus)
from lumy_middleware.utils.dataclasses import from_dict
from lumy_middleware.utils.unittest import ControllerTestCase

TEST_WORKFLOW_DIR = Path(__file__).parent.parent / 'resources'
LOGIC_TEST_WORKFLOW_PATH = TEST_WORKFLOW_DIR / 'LogicXorWorkflow.yml'

TIMEOUT = 3  # sec


class TestCurrentWorkflow(ControllerTestCase):

    def setUp(self):
        super().setUp()
        os.environ['LUMY_WORKFLOW_DIR'] = str(TEST_WORKFLOW_DIR)

    def tearDown(self):
        super().tearDown()
        del os.environ['LUMY_WORKFLOW_DIR']

    async def test_aaaa_no_workflow_by_default(self):
        '''
        *TODO* 'aaaa' in the function name is to make it the first
        test to run before the workflow is loaded. Change this when
        a "unload current workflow" method is implemented and a workflow
        can be unloaded in "tearDown".
        '''
        response_received = asyncio.get_event_loop().create_future()

        def handler(msg: MessageEnvelope):
            self.assertEqual(msg.action, 'Updated')

            content = from_dict(MsgWorkflowUpdated, msg.content)
            self.assertIsNone(content.workflow)
            self.assertIsNone(content.metadata)
            response_received.set_result(True)

        with self.client.subscribe(Target.Workflow, handler):
            self.client.publish(
                Target.Workflow,
                MessageEnvelope(action='GetCurrent')
            )

        await response_received

    async def test_load_workflow(self):
        '''
        1. Load test workflow
        2. Make sure it loaded without errors
        3. Make sure current workflow is now the requested workflow
        '''
        loaded_status_seen = asyncio.get_event_loop().create_future()
        current_workflow_updated = asyncio.get_event_loop().create_future()

        def handler(msg: MessageEnvelope):
            self.assertTrue(
                msg.action in ['LumyWorkflowLoadProgress', 'Updated'])

            if msg.action == 'LumyWorkflowLoadProgress':
                content = from_dict(
                    MsgWorkflowLumyWorkflowLoadProgress, msg.content)

                self.assertNotEqual(
                    content.status,
                    MsgWorkflowLumyWorkflowLoadProgressStatus.NOT_LOADED
                )

                if content.status == \
                        MsgWorkflowLumyWorkflowLoadProgressStatus.LOADED:
                    self.assertFalse(loaded_status_seen.done(),
                                     'Saw LOADED status more than once')
                    loaded_status_seen.set_result(True)
            if msg.action == 'Updated':
                updated = from_dict(MsgWorkflowUpdated, msg.content)
                self.assertIsNotNone(updated.workflow)
                self.assertIsNotNone(updated.metadata)
                if updated.workflow is not None:
                    self.assertEqual(updated.workflow.meta.label,
                                     'Xor Logic workflow')
                self.assertTrue(loaded_status_seen.result())
                current_workflow_updated.set_result(True)

        with self.client.subscribe(Target.Workflow, handler):
            self.client.publish(
                Target.Workflow,
                MessageEnvelope(
                    action='LoadLumyWorkflow',
                    content=MsgWorkflowLoadLumyWorkflow(
                        workflow=str(LOGIC_TEST_WORKFLOW_PATH)
                    )
                )
            )
        await asyncio.wait_for(loaded_status_seen, timeout=TIMEOUT)
        await asyncio.wait_for(current_workflow_updated, timeout=TIMEOUT)
