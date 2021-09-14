from kiara import (KiaraEntryPointItem, find_pipeline_base_path_for_module)

pipelines: KiaraEntryPointItem = (
    find_pipeline_base_path_for_module,
    ["lumy_middleware.kiara_modules.pipelines"],
)

KIARA_METADATA = {"tags": ["pipeline"], "labels": {"pipeline": "true"}}
