from typing import List

from yate.protocol import Message

from ywsd.routing_tree import CallTarget, IntermediateRoutingResult


def encode_routing_result(msg: Message, result: IntermediateRoutingResult):
    global_parameters = result.target.parameters
    msg.params.update(global_parameters)
    if result.is_simple:
        msg.return_value = result.target.target
    else:
        msg.return_value = "fork"
        fork_parameters = calltargets_to_callfork_params(
            result.fork_targets, global_parameters
        )
        msg.params.update(fork_parameters)
    return msg


def calltargets_to_callfork_params(
    call_targets: List["CallTarget"], global_params: dict
):
    index = 1
    params = {}
    for target in call_targets:
        current_prefix = "callto." + str(index)
        params[current_prefix] = target.target
        for key, value in target.parameters.items():
            if key not in global_params or global_params[key] != value:
                params[current_prefix + "." + key] = value
        index += 1
    return params
