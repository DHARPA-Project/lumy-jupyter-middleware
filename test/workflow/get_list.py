from lumy_middleware.jupyter.base import MessageEnvelope
from lumy_middleware.target import Target
from lumy_middleware.types.generated import (MsgWorkflowGetWorkflowList,
                                             MsgWorkflowWorkflowList)
from lumy_middleware.utils.unittest import ControllerTestCase
from lumy_middleware.utils.dataclasses import from_dict


class TestGetWorkflowList(ControllerTestCase):

    def test_get_list_of_workflows(self):
        def handler(msg: MessageEnvelope):
            self.assertEqual(msg.action, 'WorkflowList')

            content = from_dict(MsgWorkflowWorkflowList, msg.content)
            self.assertGreater(len(content.workflows), 0)
            first_workflow = content.workflows[0]
            self.assertIsNotNone(first_workflow.body)

        sub = self.client.subscribe(Target.Workflow, handler)
        self.client.publish(
            Target.Workflow,
            MessageEnvelope(action='GetWorkflowList',
                            content=MsgWorkflowGetWorkflowList(
                                include_workflow=True
                            ))
        )
        sub.unsubscribe()
