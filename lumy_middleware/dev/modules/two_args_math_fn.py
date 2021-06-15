from dataclasses import dataclass
from typing import Mapping, Optional

from kiara.data.values import ValueSchema
from kiara.module import KiaraModule, ValueSet

from .registry import dharpa_module


@dataclass
class Inputs:
    a: Optional[float] = None
    b: Optional[float] = None
    operator: Optional[str] = None


@dataclass
class Outputs:
    c: float


FUNCTIONS = {
    'add': lambda a, b: a + b,
    'sub': lambda a, b: a - b,
    'mul': lambda a, b: a * b,
    'div': lambda a, b: a / b,
    'pow': lambda a, b: a ** b,
}


@dharpa_module('twoArgsMathFunction')
def two_args_math_fn(inputs: Inputs, outputs: Outputs) -> None:
    fn = FUNCTIONS.get(inputs.operator or 'add')
    if fn is not None:
        outputs.c = fn(inputs.a or 0, inputs.b or 0)


class TwoArgsMathFnModule(KiaraModule):

    def create_input_schema(self) -> Mapping[str, ValueSchema]:
        return {
            "a": ValueSchema(
                type="any", doc="a.", default=1
            ),
            "b": ValueSchema(
                type="any", doc="b.", default=1
            ),
            "operator": ValueSchema(
                type="any", doc="operator.", default='add'
            )
        }

    def create_output_schema(self) -> Mapping[str, ValueSchema]:
        return {
            "c": ValueSchema(
                type="any",
                doc="c.",
            )
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
        operator = inputs.get_value_data('operator') or 'add'
        a = inputs.get_value_data('a')
        b = inputs.get_value_data('b')
        fn = FUNCTIONS.get(operator)
        if fn is not None:
            c = fn(a or 0, b or 0)
            outputs.set_value('c', c)
