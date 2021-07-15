import logging
from typing import Callable, Optional

from lumy_middleware.context.kiara.util.data import is_lumy_supported_type
from lumy_middleware.context.kiara.util.workflow import (
    get_data_transformations_from_type, get_mapping, get_page)
from lumy_middleware.types.generated import (DataTransformationDescriptor,
                                             LumyWorkflow)

from kiara.data.values import Value
from kiara.kiara import Kiara

logger = logging.getLogger(__name__)

TransformationFilter = Callable[[DataTransformationDescriptor], bool]
is_default_transformation: TransformationFilter = lambda t: bool(t.default)


def get_transformation_method(
    workflow: LumyWorkflow,
    page_id: str,
    io_id: str,
    is_input: bool,
    value: Value
) -> Optional[DataTransformationDescriptor]:
    page = get_page(workflow, page_id)
    mapping = get_mapping(page, io_id, is_input) if page is not None else None
    if mapping is None:
        return None

    value_type = value.type_name
    is_lumy_supported = is_lumy_supported_type(value_type)

    if mapping.type is None and is_lumy_supported:
        return None

    transformations = get_data_transformations_from_type(workflow, value_type)

    if len(transformations) == 0 \
            and not is_lumy_supported:
        logger.warn(f'Could not find transformation from type "{value_type}"')
        return None

    if mapping.type is not None:
        # filter transformations further to find the ones
        # specific for requested types
        transformations = [
            t
            for t in transformations
            if t.target_type == mapping.type
        ]

        if len(transformations) == 0:
            logger.warn(
                'Could not find transformation from type ' +
                f'"{value_type}" to type "{mapping.type}"'
            )
            return None

        if mapping.view is not None:
            # filter transformations further to find the ones
            # specific for requested view
            transformations = [
                t
                for t in transformations
                if t.view == mapping.view
            ]

            if len(transformations) == 0:
                logger.warn(
                    'Could not find transformation from type ' +
                    f'"{value_type}" to type "{mapping.type}"' +
                    f' with view "{mapping.view}"'
                )
                return None

    if len(transformations) > 1:
        # Try to find default
        try:
            return next(filter(is_default_transformation, transformations))
        except StopIteration:
            logger.warn(
                f'More than one transformation ({len(transformations)})' +
                f' found for type "{value_type}". Using the first one.'
            )

    return transformations[0]


def transform_value(
    kiara: Kiara,
    value: Value,
    transformation: DataTransformationDescriptor
) -> Value:
    workflow = kiara.create_workflow(
        config=transformation.pipeline.name
    )
    workflow.inputs.set_value('source', value)
    return workflow.outputs.get_value_obj('target')
