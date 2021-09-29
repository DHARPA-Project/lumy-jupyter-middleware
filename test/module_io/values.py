import asyncio
import os
from pathlib import Path

from lumy_middleware.jupyter.base import MessageEnvelope
from lumy_middleware.target import Target
from lumy_middleware.types.generated import (DataValueContainer,
                                             MsgModuleIOUpdateInputValues,
                                             MsgWorkflowLoadLumyWorkflow,
                                             MsgModuleIOGetOutputValue,
                                             DataTabularDataFilter,
                                             MsgModuleIOOutputValue)
from lumy_middleware.utils.dataclasses import from_dict, to_dict
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

    async def load_test_workflow(self):
        current_workflow_updated = asyncio.get_event_loop().create_future()

        def handler(msg: MessageEnvelope):
            if msg.action == 'Updated':
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
        await asyncio.wait_for(current_workflow_updated, timeout=TIMEOUT)

    async def test_workflow_outputs_updated_after_inputs_change(self):
        '''
        1. Load test workflow
        2. Set inputs
        3. See that output values update notification received
        4. Request output value
        5. Receive and validate output value
        '''
        await self.load_test_workflow()

        outputs_updated = asyncio.get_event_loop().create_future()

        def outputs_updated_handler(msg: MessageEnvelope):
            if msg.action == 'OutputValuesUpdated':
                self.client.publish(
                    Target.ModuleIO,
                    MessageEnvelope(
                        action='GetOutputValue',
                        content=to_dict(MsgModuleIOGetOutputValue(
                            step_id='setBAndSeeY',
                            output_id='y',
                            filter=DataTabularDataFilter(full_value=True)
                        ))
                    )
                )
            if msg.action == 'OutputValue':
                content = from_dict(MsgModuleIOOutputValue, msg.content)
                self.assertTrue(content.value)
                if not outputs_updated.done():
                    outputs_updated.set_result(content.value)

        with self.client.subscribe(Target.ModuleIO, outputs_updated_handler):
            self.client.publish(
                Target.ModuleIO,
                MessageEnvelope(
                    action='UpdateInputValues',
                    content=to_dict(MsgModuleIOUpdateInputValues(
                        step_id='setA',
                        input_values={
                            'a': DataValueContainer('simple', True)
                        }
                    ))
                )
            )
            self.client.publish(
                Target.ModuleIO,
                MessageEnvelope(
                    action='UpdateInputValues',
                    content=to_dict(MsgModuleIOUpdateInputValues(
                        step_id='setBAndSeeY',
                        input_values={
                            'b': DataValueContainer('simple', False)
                        }
                    ))
                )
            )

        await asyncio.wait_for(outputs_updated, timeout=TIMEOUT)
