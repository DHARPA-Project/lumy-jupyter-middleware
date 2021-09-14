from kiara import (KiaraEntryPointItem, find_kiara_modules_under)

modules: KiaraEntryPointItem = (find_kiara_modules_under, [
                                "lumy_middleware.kiara_modules"])
