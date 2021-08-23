from kiara import (KiaraEntryPointItem, find_kiara_modules_under,
                   find_pipeline_base_path_for_module)

modules: KiaraEntryPointItem = (find_kiara_modules_under, [
                                "lumy_middleware.dev.modules"])
pipelines: KiaraEntryPointItem = (
    find_pipeline_base_path_for_module,
    ["lumy_middleware.dev"],
)
