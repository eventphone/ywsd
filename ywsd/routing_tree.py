import logging
from enum import Enum
from typing import List, Dict, Tuple, Optional
import os.path
import uuid

from ywsd.objects import Extension, ForkRank, Yate, DoesNotExist


class RoutingError(Exception):
    def __init__(self, error_code, message):
        self.error_code = error_code
        self.message = message


class RoutingTree:
    def __init__(self, source, target, source_params, settings):
        self.source_extension = source
        self.target_extension = target
        self.source = None
        self.target = None
        self._settings = settings
        self._source_params = source_params

        self.routing_result = None
        self.new_routing_cache_content = {}
        self.all_routing_results = {}

    async def discover_tree(
        self, db_connection
    ) -> Optional["RoutingTreeDiscoveryVisitor"]:
        await self._load_source_and_target(db_connection)
        if self.target.type != Extension.Type.TRUNK:
            visitor = RoutingTreeDiscoveryVisitor(self.target, [self.source.extension])
            await visitor.discover_tree(db_connection)
            return visitor

    def calculate_routing(
        self, local_yate: int, yates_dict: Dict[int, Yate]
    ) -> Tuple["IntermediateRoutingResult", Dict[str, "IntermediateRoutingResult"]]:
        self._calculate_main_routing(local_yate, yates_dict)
        self._provide_ringback()
        self._populate_source_and_target_parameters()

        return self.routing_result, self.new_routing_cache_content

    def serialized_tree(self):
        return self.target.serialize() if self.target else None

    def _calculate_main_routing(self, local_yate, yates_dict):
        visitor = YateRoutingGenerationVisitor(self, local_yate, yates_dict)
        if self.target.type != Extension.Type.TRUNK:
            result = visitor.calculate_routing()
        else:
            result = visitor.generate_trunk_routing(self.target, self.target_extension)
        self.routing_result = result
        self.all_routing_results = visitor.get_routing_results()
        self.new_routing_cache_content = visitor.get_routing_cache_content()
        if not result.is_valid:
            raise RoutingError("noroute", "The main routing target returned NO_ROUTE.")

    def _provide_ringback(self):
        if self.target.ringback is not None:
            ringback_path = (
                os.path.join(
                    self._settings.RINGBACK_TOP_DIRECTORY, self.target.ringback
                )
                + ".slin"
            )
            if os.path.isfile(ringback_path):
                if self.routing_result.is_simple:
                    # we need to convert routing result into a simple fork
                    self.routing_result = IntermediateRoutingResult(
                        target=CallTarget(
                            "fork", self.routing_result.target.parameters
                        ),
                        fork_targets=[
                            self._make_ringback_target(ringback_path),
                            self.routing_result.target,
                        ],
                    )
                else:
                    # if the routing target is already a callfork, just prepend the ringback target to the first group
                    self.routing_result.fork_targets.insert(
                        0, self._make_ringback_target(ringback_path)
                    )

    def _populate_parameters_global(self, parameters):
        self.routing_result.target.parameters.update(parameters)
        for entry in self.new_routing_cache_content.values():
            entry.target.parameters.update(parameters)

    def _populate_source_and_target_parameters(self):
        parameters = self._source_params
        if self.target.name is not None:
            parameters["calledname"] = self.target.name

        if (
            self.target.type == Extension.Type.GROUP
            and self.target.short_name is not None
        ):
            # Callername should always be populated by source parameters, otherwise, default to source name
            callername = self._source_params.get("callername", self.source.name)
            parameters["callername"] = "[{}] {}".format(
                self.target.short_name, callername
            )

        self._populate_parameters_global(parameters)

    @staticmethod
    def _make_ringback_target(path):
        return CallTarget(
            "wave/play/" + path,
            {
                "fork.calltype": "persistent",
                "fork.autoring": "true",
                "fork.automessage": "call.progress",
            },
        )

    async def _load_source_and_target(self, db_connection):
        try:
            if isinstance(self.source_extension, Extension):
                self.source = self.source_extension
            else:
                self.source = await Extension.load_extension(
                    self.source_extension, db_connection
                )
        except DoesNotExist:
            self.source = Extension.create_unknown(self.source_extension)
        try:
            self.target = await Extension.load_extension(
                self.target_extension, db_connection
            )
            self.target.tree_identifier = str(self.target.id)
        except DoesNotExist:
            # we give this one rescue attempt by trying to load a trunk
            try:
                self.target = await Extension.load_trunk_extension(
                    self.target_extension, db_connection
                )
                self.target.tree_identifier = str(self.target.id)
                return
            except DoesNotExist:
                pass
            raise RoutingError("noroute", "Routing target was not found")


class RoutingTreeDiscoveryVisitor:
    def __init__(self, root_node, excluded_targets, max_depth=10):
        self._root_node = root_node
        self._excluded_targets = set(excluded_targets)
        self._max_depth = max_depth
        self._failed = False
        self._pruned = False

    @property
    def failed(self):
        return self._failed

    @property
    def pruned(self):
        return self._pruned

    async def discover_tree(self, db_connection):
        await self._visit(
            self._root_node, 0, list(self._excluded_targets), db_connection
        )

    async def _visit(
        self, node: Extension, depth: int, path_extensions: list, db_connection
    ):
        if depth >= self._max_depth:
            node.routing_log(
                "Routing aborted due to depth limit at {}".format(node), "ERROR"
            )
            self._failed = True
            return

        path_extensions_local = path_extensions.copy()
        path_extensions_local.append(node.extension)

        if (
            node.type != Extension.Type.EXTERNAL
            and node.forwarding_mode != Extension.ForwardingMode.DISABLED
        ):
            await node.load_forwarding_extension(db_connection)
        if node.type in (Extension.Type.GROUP, Extension.Type.MULTIRING) and (
            node.forwarding_mode != Extension.ForwardingMode.ENABLED
            or node.forwarding_delay > 0
        ):
            # we discover group members if there is no immediate forward
            await node.populate_fork_ranks(db_connection)
        # now we visit the populated children if they haven't been already discovered
        if node.forwarding_extension is not None:
            # TODO: We might want to avoid following forwards if this is discovered as a MULTIRING child?
            fwd = node.forwarding_extension
            if fwd.extension not in path_extensions_local:
                await self._visit(fwd, depth + 1, path_extensions_local, db_connection)
            else:
                self._pruned = True
                node.routing_log(
                    "Discovery aborted for forward to {}, was already present.\n"
                    "Disabling Forward".format(fwd),
                    "WARN",
                    related_node=node.forwarding_extension,
                )
                node.forwarding_mode = Extension.ForwardingMode.DISABLED
        for fork_rank in node.fork_ranks:
            for member in fork_rank.members:
                # do not discover inactive members
                if not member.active:
                    continue
                ext = member.extension
                if ext.extension not in path_extensions_local:
                    await self._visit(
                        ext, depth + 1, path_extensions_local, db_connection
                    )
                else:
                    self._pruned = True
                    fork_rank.routing_log(
                        "Discovery aborted for {} in {}, was already present.\n"
                        "Temporarily disable membership for this routing.".format(
                            ext, fork_rank, path_extensions_local
                        ),
                        "WARN",
                        related_node=member.extension,
                    )
                    member.active = False


class CallTarget:
    target = ""
    parameters = {}

    def __init__(self, target, parameters=None):
        self.target = target
        self.parameters = parameters if parameters is not None else {}

    def serialize(self):
        return {"target": self.target, "parameters": self.parameters}

    @classmethod
    def deserialize(cls, data):
        target = data["target"]
        parameters = data.get("parameters")
        return cls(target, parameters=parameters)

    @property
    def is_separator(self):
        return self.target.startswith("|")

    def __repr__(self):
        return "<CallTarget {}, params={}>".format(self.target, self.parameters)


class IntermediateRoutingResult:
    class Type(Enum):
        SIMPLE = 0
        FORK = 1
        NO_ROUTE = 99

    def __init__(
        self, target: CallTarget = None, fork_targets: List["CallTarget"] = None
    ):
        if fork_targets is not None:
            if len(fork_targets) > 0:
                self.type = IntermediateRoutingResult.Type.FORK
                self.fork_targets = fork_targets
                self.target = target
            else:
                self.type = IntermediateRoutingResult.Type.NO_ROUTE
                self.target = None
                self.fork_targets = []
        elif target is not None:
            self.type = IntermediateRoutingResult.Type.SIMPLE
            self.target = target
            self.fork_targets = []
        else:
            self.type = IntermediateRoutingResult.Type.NO_ROUTE
            self.target = None
            self.fork_targets = []

    def serialize(self):
        result = {
            "type": str(self.type),
            "target": self.target.serialize() if self.target is not None else str(None),
        }
        if self.fork_targets:
            result["fork_targets"] = [
                target.serialize() for target in self.fork_targets
            ]
        return result

    @classmethod
    def deserialize(cls, data):
        target = data.get("target")
        if target is not None:
            target = CallTarget.deserialize(target)
        fork_targets = [
            CallTarget.deserialize(target) for target in data.get("fork_targets", [])
        ]
        return cls(target=target, fork_targets=fork_targets)

    def __repr__(self):
        fork_targets_str = "\n\t\t".join([repr(targ) for targ in self.fork_targets])
        return "<IntermediateRoutingResult\n\ttarget={}\n\tfork_targets=\n\t\t{}\n>".format(
            self.target, fork_targets_str
        )

    @property
    def is_simple(self):
        return self.type == IntermediateRoutingResult.Type.SIMPLE

    @property
    def is_valid(self):
        return self.type != IntermediateRoutingResult.Type.NO_ROUTE


class YateRoutingGenerationVisitor:
    def __init__(
        self, routing_tree: RoutingTree, local_yate_id: int, yates_dict: Dict[int, Yate]
    ):
        self._routing_tree = routing_tree
        self._local_yate_id = local_yate_id
        self._yates_dict = yates_dict
        self._lateroute_cache: Dict[str, IntermediateRoutingResult] = {}
        self._x_eventphone_id = uuid.uuid4().hex
        self._routing_results: Dict[str, IntermediateRoutingResult] = {}

    def get_routing_cache_content(self):
        return self._lateroute_cache

    def get_routing_results(self):
        return self._routing_results

    def _make_intermediate_result(self, target: CallTarget = None, fork_targets=None):
        return IntermediateRoutingResult(target=target, fork_targets=fork_targets)

    def _make_calltarget(self, target: str, parameters: dict = None):
        # write default parameters into the calltarget
        if parameters is None:
            parameters = {}
        parameters["x_eventphone_id"] = self._x_eventphone_id
        parameters["osip_X-Eventphone-Id"] = self._x_eventphone_id
        return CallTarget(target=target, parameters=parameters)

    def _cache_intermediate_result(self, result: IntermediateRoutingResult):
        if not result.is_simple:
            self._lateroute_cache[result.target.target] = result

    def calculate_routing(self):
        return self._visit(self._routing_tree.target, [])

    def _visit(self, node: Extension, path: list) -> IntermediateRoutingResult:
        result = self._visit_for_route_calculation(node, path)
        self._routing_results[node.tree_identifier] = result
        return result

    def _visit_for_route_calculation(
        self, node: Extension, path: list
    ) -> IntermediateRoutingResult:
        local_path = path.copy()
        local_path.append(node.id)

        # first we check if this node has an immediate forward. If yes, we defer routing there.
        if node.immediate_forward:
            return self._visit(node.forwarding_extension, local_path)

        if YateRoutingGenerationVisitor.node_has_simple_routing(node):
            return self._make_intermediate_result(
                target=self.generate_simple_routing_target(node)
            )
        else:
            # this will require a fork
            # go through the callgroup ranks to issue the groups of the fork
            fork_targets = []
            accumulated_delay = 0
            for rank in node.fork_ranks:
                if fork_targets:
                    # this is not the first rank, so we need to generate a separator
                    if rank.mode == ForkRank.Mode.DROP:
                        separator = "|drop={}".format(rank.delay)
                        accumulated_delay += rank.delay
                    elif rank.mode == ForkRank.Mode.NEXT:
                        separator = "|next={}".format(rank.delay)
                        accumulated_delay += rank.delay
                    else:
                        separator = "|"
                        # If we see an untimed separator, any time-based forward is not possible anymore
                        if node.forwarding_mode == Extension.ForwardingMode.ENABLED:
                            node.routing_log(
                                "Non time-based fork rank is incompatible with time-based forward. "
                                "Disabling the forward.",
                                "WARN",
                                rank,
                            )
                            node.forwarding_mode = Extension.ForwardingMode.DISABLED

                    if (
                        node.forwarding_mode == Extension.ForwardingMode.ENABLED
                        and accumulated_delay >= node.forwarding_delay
                    ):
                        # all of those will not be called, as the forward takes effect now
                        node.routing_log(
                            "Fork rank (and following) are ignored due to time-based forward.",
                            "WARN",
                            rank,
                        )
                        break
                    # Do not generate default params on pseudo targets
                    fork_targets.append(CallTarget(separator))
                for member in rank.members:
                    # do not route inactive members
                    if not member.active:
                        continue
                    member_route = self._visit(member.extension, local_path)
                    if not member_route.is_valid:
                        rank.routing_log(
                            "Extension has no valid (non-empty) routing and is thus ignored.",
                            "WARN",
                            member.extension,
                        )
                        continue
                    if member.type.is_special_calltype:
                        member_route.target.parameters[
                            "fork.calltype"
                        ] = member.type.fork_calltype
                    # please note that we ignore the member modes for the time being
                    fork_targets.append(member_route.target)
                    self._cache_intermediate_result(member_route)

                if fork_targets and fork_targets[-1].target == "|":
                    # We just created an empty default rank. This will cause the call to hang
                    del fork_targets[-1]
                    rank.routing_log(
                        "This created an empty default rank. It will be removed to prevent call hang.",
                        "WARN",
                    )

            # if this is a MULTIRING or (SIMPLE with forward), the extension itself needs to be part of the first group
            if node.type in (Extension.Type.MULTIRING, Extension.Type.SIMPLE):
                # in difference to groups, the first ForkRank can have type NEXT or DROP and we should respect it
                if len(node.fork_ranks) > 0:
                    first_fork_rank = node.fork_ranks[0]
                    if first_fork_rank.mode == ForkRank.Mode.NEXT:
                        fork_targets.insert(
                            0, CallTarget("|next={}".format(first_fork_rank.delay))
                        )
                    elif first_fork_rank.mode == ForkRank.Mode.DROP:
                        fork_targets.insert(
                            0, CallTarget("|drop={}".format(first_fork_rank.delay))
                        )
                    # If the fork rank is default, we assume that multiring should start with the main extension
                    # and do nothing here
                fork_targets.insert(0, self.generate_simple_routing_target(node))

            # Handle forwards
            if node.forwarding_mode == Extension.ForwardingMode.ON_BUSY:
                # There should be no call waiting on all previous call legs.
                for target in fork_targets:
                    if not target.is_separator:
                        target.parameters["osip_X-No-Call-Wait"] = "1"

            if node.forwarding_mode in (
                Extension.ForwardingMode.ENABLED,
                Extension.ForwardingMode.ON_BUSY,
                Extension.ForwardingMode.ON_UNAVAILABLE,
            ):
                # this is non-immediate forward
                forwarding_route = self._visit(node.forwarding_extension, local_path)
                if node.forwarding_mode == Extension.ForwardingMode.ENABLED:
                    fwd_delay = node.forwarding_delay - accumulated_delay
                    fork_targets.append(CallTarget("|drop={}".format(fwd_delay)))
                else:
                    # Add a default rank, call will progress to next rang when all previous calls failed
                    fork_targets.append(CallTarget("|"))
                fork_targets.append(forwarding_route.target)
                self._cache_intermediate_result(forwarding_route)

            return self._make_intermediate_result(
                fork_targets=fork_targets,
                target=self._make_calltarget(
                    self.generate_deferred_routestring(local_path)
                ),
            )

    @staticmethod
    def node_has_simple_routing(node: Extension):
        if node.type == Extension.Type.EXTERNAL:
            return True
        # if the node is set to immediate forward, it will be ignored for routing and we calculate the routing for
        # the forwarded node instead
        if node.immediate_forward:
            return YateRoutingGenerationVisitor.node_has_simple_routing(
                node.forwarding_extension
            )

        if node.type == Extension.Type.SIMPLE:
            if node.forwarding_mode == Extension.ForwardingMode.DISABLED:
                return True
            # Forwarding with delay or ON_BUSY/UNAVAILABLE requires a callfork
            return False
        # multiring is simple, if there are no active multiring participants configured
        if node.type == Extension.Type.MULTIRING:
            if node.has_active_group_members:
                return False
            # ok, nothing to multiring, so look at the forwarding mode
            if node.forwarding_mode == Extension.ForwardingMode.DISABLED:
                return True
            # Forwarding with delay or ON_BUSY/UNAVAILABLE requires a callfork
            return False
        # groups might have a simple routing if they have exactly one participant. We will ignore this possibility
        # for the moment being. We could do this by introducing an optimizer stage that reshapes the tree :P
        return False

    def generate_simple_routing_target(self, node: Extension):
        if node.type == Extension.Type.EXTERNAL:
            # External things are handled by regexroute, just issue a lateroute that triggers this
            return self._make_calltarget(
                "lateroute/{}".format(node.extension), {"eventphone_stage2": "1"}
            )
        if node.yate_id is None:
            raise RoutingError(
                "failure",
                "Extension {} is misconfigured - yate_id is NULL.".format(node),
            )
        if node.yate_id == self._local_yate_id:
            return self._make_calltarget(
                "lateroute/{}".format(node.extension), {"eventphone_stage2": "1"}
            )
        else:
            return self._make_calltarget(
                "sip/sip:{}@{}".format(
                    node.extension, self._yates_dict[node.yate_id].hostname
                ),
                {
                    "oconnection_id": self._yates_dict[node.yate_id].voip_listener,
                },
            )

    def generate_trunk_routing(self, trunk: Extension, dialed_number: str):
        if trunk.yate_id is None:
            raise RoutingError(
                "failure",
                "Trunk-Extension {} is misconfigured - yate_id is NULL.".format(trunk),
            )
        if trunk.yate_id == self._local_yate_id:
            return IntermediateRoutingResult(
                target=self._make_calltarget(
                    "lateroute/{}".format(dialed_number), {"eventphone_stage2": "1"}
                )
            )
        else:
            return IntermediateRoutingResult(
                target=self._make_calltarget(
                    "sip/sip:{}@{}".format(
                        dialed_number, self._yates_dict[trunk.yate_id].hostname
                    ),
                    {
                        "oconnection_id": self._yates_dict[trunk.yate_id].voip_listener,
                    },
                )
            )

    def generate_deferred_routestring(self, path):
        return "lateroute/" + self.generate_node_route_string(path)

    def generate_node_route_string(self, path):
        joined_path = "-".join(map(str, path))
        return "stage1-{}-{}".format(self._x_eventphone_id, joined_path)
