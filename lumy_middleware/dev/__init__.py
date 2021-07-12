from kiara import (KiaraEntryPointItem, find_kiara_modules_under,
                   find_kiara_pipelines_under)

modules: KiaraEntryPointItem = (find_kiara_modules_under, [
                                "lumy_middleware.dev.modules"])
pipelines: KiaraEntryPointItem = (
    find_kiara_pipelines_under,
    ["lumy_middleware.dev"],
)
