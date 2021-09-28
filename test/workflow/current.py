from lumy_middleware.utils.unittest import ControllerTestCase
from lumy_middleware.jupyter.base import MessageEnvelope
from lumy_middleware.target import Target
from lumy_middleware.types.generated import MsgWorkflowUpdated


class TestCurrentWorkflow(ControllerTestCase):

    def test_no_workflow_by_default(self):
        def handler(msg: MessageEnvelope):
            self.assertEqual(msg.action, 'Updated')

            content = MsgWorkflowUpdated(**(msg.content or {}))
            self.assertIsNone(content.workflow)
            self.assertIsNone(content.metadata)

        sub = self.client.subscribe(Target.Workflow, handler)
        self.client.publish(
            Target.Workflow,
            MessageEnvelope(action='GetCurrent')
        )
        sub.unsubscribe()
