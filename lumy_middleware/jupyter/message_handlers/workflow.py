import json
import logging
from pathlib import Path
from typing import Dict, Optional, Union, cast

from kiara.kiara import Kiara
from lumy_middleware.context.kiara.page_components_code import \
    get_specific_components_code
from lumy_middleware.jupyter.base import MessageHandler
from lumy_middleware.types import MsgWorkflowUpdated
from lumy_middleware.types.generated import (LumyWorkflow, Metadata,
                                             MsgWorkflowExecute,
                                             MsgWorkflowExecutionResult)
from lumy_middleware.types.generated import \
    MsgWorkflowExecutionResultStatus as Status
from lumy_middleware.types.generated import (
    MsgWorkflowGetWorkflowList, MsgWorkflowLoadLumyWorkflow,
    MsgWorkflowLumyWorkflowLoadProgress,
    MsgWorkflowLumyWorkflowLoadProgressStatus, MsgWorkflowPageComponentsCode,
    MsgWorkflowWorkflowList)
from lumy_middleware.utils.dataclasses import (EnhancedJSONEncoder, from_dict,
                                               to_dict)
from lumy_middleware.utils.lumy import get_workflows

logger = logging.getLogger(__name__)


class WorkflowMessageHandler(MessageHandler):

    def _handle_GetCurrent(self):
        '''
        Return current workflow.
        '''
        self.publisher.publish(MsgWorkflowUpdated(
            workflow=None if self._context.current_workflow is None
            else self._context.current_workflow,
            metadata=self._context.current_workflow_metadata
        ))

    def _handle_LoadLumyWorkflow(self, msg: MsgWorkflowLoadLumyWorkflow):
        '''
        Load a workflow:
            - install dependencies
            - set workflow as current
        '''
        workflow: Union[Path, LumyWorkflow]
        if not isinstance(msg.workflow, str):
            workflow = from_dict(LumyWorkflow, cast(Dict, msg.workflow))
        else:
            workflow = Path(msg.workflow)

        metadata: Optional[Metadata] = None
        if isinstance(msg.workflow, str):
            metadata = Metadata(uri=msg.workflow)
        else:
            # find the workflow
            workflows = list(get_workflows(include_body=True))
            workflow_str = json.dumps(to_dict(msg.workflow))
            for w in workflows:
                w_str = json.dumps(to_dict(w.body), cls=EnhancedJSONEncoder)
                if w_str == workflow_str:
                    metadata = Metadata(uri=w.uri)
                    break

        last_status_update: Optional[MsgWorkflowLumyWorkflowLoadProgress] = \
            None
        for status_update in self.context.load_workflow(workflow, metadata):
            last_status_update = status_update
            self.publisher.publish(status_update)

        if last_status_update is not None and \
                last_status_update.status == \
                MsgWorkflowLumyWorkflowLoadProgressStatus.LOADED:
            self._handle_GetCurrent()

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
                        value_type = value.type_obj
                        save_config = value_type.save_config()

                        # values without 'save_config' cannot be saved
                        # https://github.com/DHARPA-Project/kiara/blob/4263687ebd1c0749a719fb489f004bce46935780/src/kiara/data/persistence.py#L83
                        if save_config is not None:
                            value_id = value.save()
                            outputs_ids[field] = value_id

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
