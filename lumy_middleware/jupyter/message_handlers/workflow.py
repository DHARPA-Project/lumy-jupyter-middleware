import logging
from typing import Dict, cast

from kiara.kiara import Kiara
from lumy_middleware.context.kiara.page_components_code import \
    get_specific_components_code
from lumy_middleware.jupyter.base import MessageHandler
from lumy_middleware.types import MsgWorkflowUpdated
from lumy_middleware.types.generated import (LumyWorkflow, MsgWorkflowExecute,
                                             MsgWorkflowExecutionResult)
from lumy_middleware.types.generated import \
    MsgWorkflowExecutionResultStatus as Status
from lumy_middleware.types.generated import (MsgWorkflowGetWorkflowList,
                                             MsgWorkflowLoadLumyWorkflow,
                                             MsgWorkflowPageComponentsCode,
                                             MsgWorkflowWorkflowList)
from lumy_middleware.utils.dataclasses import from_dict
from lumy_middleware.utils.lumy import get_workflows
from uuid import uuid4

logger = logging.getLogger(__name__)


class WorkflowMessageHandler(MessageHandler):

    def _handle_GetCurrent(self):
        '''
        Return current workflow.
        '''
        self.publisher.publish(MsgWorkflowUpdated(
            None if self._context.current_workflow is None
            else self._context.current_workflow
        ))

    def _handle_LoadLumyWorkflow(self, msg: MsgWorkflowLoadLumyWorkflow):
        '''
        Load a workflow:
            - install dependencies
            - set workflow as current
        '''
        workflow = msg.workflow
        if not isinstance(workflow, str):
            workflow = from_dict(LumyWorkflow, cast(Dict, workflow))
        for status_update in self.context.load_workflow(workflow):
            self.publisher.publish(status_update)

    def _handle_GetWorkflowList(self, msg: MsgWorkflowGetWorkflowList):
        self.publisher.publish(MsgWorkflowWorkflowList(
            workflows=list(get_workflows(msg.include_workflow or False))
        ))

    def _handle_Execute(self, msg: MsgWorkflowExecute):
        # TODO: This can be better encapsulated into context
        kiara: Kiara = getattr(self.context, '_kiara', None)
        if kiara is None:
            logger.warn('No kiara is available')
            response = MsgWorkflowExecutionResult(
                request_id=msg.request_id,
                status=Status.ERROR,
                error_message='No kiara is available'
            )
        else:
            try:
                workflow = kiara.create_workflow(
                    msg.module_name, workflow_id=msg.workflow_id)
                inputs = msg.inputs or {}
                workflow.inputs.set_values(**inputs)

                outputs_ids = {}

                if msg.save:
                    for field, value \
                            in workflow.outputs.items():  # type: ignore
                        try:
                            meta = value.save(
                                [msg.workflow_id or str(uuid4())])
                            outputs_ids[field] = meta.value_id
                        except Exception:
                            # Kiara does not provide a way to detect
                            # whether the value can be saved.
                            # Using internal API is unreliable.
                            pass

                response = MsgWorkflowExecutionResult(
                    request_id=msg.request_id,
                    status=Status.OK,
                    result={
                        'outputs': outputs_ids
                    }
                )
            except Exception as e:
                logger.exception(f'Could not execute workflow: {str(e)}')
                response = MsgWorkflowExecutionResult(
                    request_id=msg.request_id,
                    status=Status.ERROR,
                    error_message=str(e)
                )

        self.publisher.publish(response)

    def _handle_GetPageComponentsCode(self):
        code = [] if self._context.current_workflow is None \
            else get_specific_components_code(self._context.current_workflow)
        response = MsgWorkflowPageComponentsCode(code=code)
        self.publisher.publish(response)
