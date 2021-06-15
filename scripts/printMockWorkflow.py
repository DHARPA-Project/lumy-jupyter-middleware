import yaml
from lumy_middleware.utils.dataclasses import to_dict
from kiara import Kiara
from kiara.pipeline.pipeline import StepStatus


def step_status_representer(dumper, status: StepStatus):
    return dumper.represent_scalar('tag:yaml.org,2002:str', str(status.value))


yaml.add_representer(StepStatus, step_status_representer)

kiara = Kiara.instance()

wf = kiara.create_workflow("networkAnalysisDev")
state = wf.get_current_state()

print(yaml.dump(to_dict(state), default_flow_style=False))
