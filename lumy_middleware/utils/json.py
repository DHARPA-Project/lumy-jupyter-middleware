import datetime
import json
from typing import Any


def _datetime_aware_converter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()


def object_as_json(j: Any) -> str:
    return json.dumps(j, default=_datetime_aware_converter)
