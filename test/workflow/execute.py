from lumy_middleware.jupyter.base import MessageEnvelope
from lumy_middleware.target import Target
from lumy_middleware.types.generated import (MsgWorkflowExecute,
                                             MsgWorkflowExecutionResult)
from lumy_middleware.types.generated import \
    MsgWorkflowExecutionResultStatus as Status
from lumy_middleware.utils.dataclasses import from_dict
from lumy_middleware.utils.unittest import ControllerTestCase

TEST_REQUEST_ID = 'test-id'


class TestExecuteWorkflow(ControllerTestCase):

    def test_execute_kiara_workflow(self):
        def handler(msg: MessageEnvelope):
            self.assertEqual(msg.action, 'ExecutionResult')

            content = from_dict(MsgWorkflowExecutionResult, msg.content)
            self.assertEqual(content.request_id, TEST_REQUEST_ID)
            self.assertEqual(content.status, Status.OK)

        with self.client.subscribe(Target.Workflow, handler):
            self.client.publish(
                Target.Workflow,
                MessageEnvelope(
                    action='Execute',
                    content=MsgWorkflowExecute(
                        module_name='logic.and',
                        request_id=TEST_REQUEST_ID,
                        inputs={'a': True, 'b': True}
                    ))
            )
