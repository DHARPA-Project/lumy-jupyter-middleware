from dataclasses import is_dataclass
from typing import IO, Any, Dict, Optional, Type, TypeVar, Union, cast

import yaml
from dataclasses_json import LetterCase, dataclass_json
from pydantic import BaseModel  # pylint: disable=no-name-in-module
from stringcase import camelcase

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader  # type: ignore


T = TypeVar('T')


def is_dataclass_json(data):
    return hasattr(data, 'to_dict')


def from_yaml(
    data_class: Type[T],
    yaml_content: Union[bytes, IO[bytes], str, IO[str]]
) -> T:
    content: Dict = yaml.load(yaml_content, Loader=Loader)
    return from_dict(data_class, content)


def __ensure_dataclass_json_instance(obj: T) -> T:
    assert is_dataclass(obj), f'Expected {obj} to be a dataclass'
    if isinstance(obj, type):
        return dataclass_json(obj, letter_case=LetterCase.CAMEL)
    else:
        dataclass_json(obj.__class__, letter_case=LetterCase.CAMEL)
        return obj


def is_pydantic(obj: Any):
    return isinstance(obj, BaseModel)  # pytype: disable=wrong-arg-types


def pydantic_to_dict(obj: Any):
    if not is_pydantic(obj):
        if isinstance(obj, dict):
            return dict({k: pydantic_to_dict(v) for k, v in obj.items()})
        if isinstance(obj, list):
            return [pydantic_to_dict(v) for v in obj]
        return obj

    def iter(obj):
        for k, v in BaseModel._iter(obj, to_dict=False):
            yield camelcase(k), pydantic_to_dict(v)
    return dict(iter(obj))


def to_dict(data: Any) -> Optional[Dict]:
    if data is None:
        return None
    if is_dataclass(data):
        if is_dataclass_json(data):
            return data.to_dict()
        else:
            return __ensure_dataclass_json_instance(data).to_dict()
    elif is_pydantic(data):
        return cast(Dict, pydantic_to_dict(data))
    return dict(data)


def from_dict(
    data_class: Type[T],
    data: Optional[Dict],
) -> T:
    assert is_dataclass(
        data_class), f'Expected type {data_class} to be a dataclass'

    if not is_dataclass_json(data):
        cls = __ensure_dataclass_json_instance(data_class)
    else:
        cls = data_class
    return cast(Any, cls).from_dict(data)
