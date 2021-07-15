from typing import Callable, List, Optional

from lumy_middleware.types.generated import (DataTransformationDescriptor,
                                             LumyWorkflow, WorkflowPageDetails,
                                             WorkflowPageMapping)

PageFilter = Callable[[WorkflowPageDetails], bool]
PageMappingFilter = Callable[[WorkflowPageMapping], bool]


def get_page(
    workflow: LumyWorkflow,
    page_id: str
) -> Optional[WorkflowPageDetails]:
    if workflow is None or workflow.ui is None:
        return None
    page_id_match: PageFilter = lambda page: page.id == page_id
    try:
        return next(filter(page_id_match, workflow.ui.pages or []))
    except StopIteration:
        return None


def get_mapping(
    page_details: WorkflowPageDetails,
    page_io_id: str,
    is_input: bool
) -> Optional[WorkflowPageMapping]:
    if page_details is None or page_details.mapping is None:
        return None

    mappings = page_details.mapping.inputs \
        if is_input \
        else page_details.mapping.outputs
    mapping_id_match: PageMappingFilter = \
        lambda mapping: mapping.page_io_id == page_io_id
    try:
        return next(filter(mapping_id_match, mappings or []))
    except StopIteration:
        return None


def get_data_transformations_from_type(
    workflow: LumyWorkflow,
    type: str
) -> List[DataTransformationDescriptor]:
    if workflow is None \
        or workflow.processing is None \
            or workflow.processing.data is None:
        return []

    transformations = workflow.processing.data.transformations or []

    return [
        t
        for t in transformations
        if t.source_type == type
    ]
