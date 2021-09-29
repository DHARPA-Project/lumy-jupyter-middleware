import os
from pathlib import Path

from lumy_middleware.jupyter.base import MessageEnvelope
from lumy_middleware.target import Target
from lumy_middleware.types.generated import (MsgWorkflowGetWorkflowList,
                                             MsgWorkflowWorkflowList)
from lumy_middleware.utils.dataclasses import from_dict
from lumy_middleware.utils.unittest import ControllerTestCase

TEST_WORKFLOW_DIR = Path(__file__).parent.parent / 'resources'


class TestGetWorkflowList(ControllerTestCase):
    def setUp(self):
        super().setUp()
        os.environ['LUMY_WORKFLOW_DIR'] = str(TEST_WORKFLOW_DIR)

    def tearDown(self):
        super().tearDown()
        del os.environ['LUMY_WORKFLOW_DIR']

    def test_get_list_of_workflows(self):
        def handler(msg: MessageEnvelope):
            self.assertEqual(msg.action, 'WorkflowList')

            content = from_dict(MsgWorkflowWorkflowList, msg.content)
            self.assertGreater(len(content.workflows), 0)
            first_workflow = content.workflows[0]
            self.assertIsNotNone(first_workflow.body)

            workflow_names = [w.name for w in content.workflows]
            self.assertTrue('Xor Logic workflow' in workflow_names,
                            'A Unit Test workflow missing in the ' +
                            'list of workflow')

        with self.client.subscribe(Target.Workflow, handler):
            self.client.publish(
                Target.Workflow,
                MessageEnvelope(
                    action='GetWorkflowList',
                    content=MsgWorkflowGetWorkflowList(
                        include_workflow=True
                    ))
            )
