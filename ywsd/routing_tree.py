from enum import Enum
import uuid

from ywsd.objects import Yate, Extension, CallgroupRank


class RoutingTree:
    def __init__(self, source, target):
        self.source_extension = source
        self.target_extension = target
        self.source = None
        self.target = None

        self.routing_result = None
        self.new_routing_cache_content = {}

    async def discover_tree(self, db_connection):
        await self._load_source_and_target(db_connection)
        visitor = RoutingTreeDiscoveryVisitor(self.target, [self.source_extension])
        await visitor.discover_tree(db_connection)
        return visitor

    def calculate_routing(self, local_yate, yates_dict):
        visitor = YateRoutingGenerationVisitor(self, local_yate, yates_dict)
        result = visitor.calculate_routing()
        self.routing_result = result
        self.new_routing_cache_content = visitor.get_routing_cache_content()

    async def _load_source_and_target(self, db_connection):
        self.source = await Extension.load_extension(self.source_extension, db_connection)
        self.target = await Extension.load_extension(self.target_extension, db_connection)


class RoutingTreeDiscoveryVisitor:
    def __init__(self, root_node, excluded_targets, max_depth=10):
        self._root_node = root_node
        self._excluded_targets = set(excluded_targets)
        self._discovery_log = []
        self._max_depth = max_depth
        self._failed = False
        self._pruned = False

    @property
    def failed(self):
        return self._failed

    @property
    def pruned(self):
        return self._pruned

    def get_log(self):
        return self._discovery_log

    def _log(self, msg, level=None):
        self._discovery_log.append(msg)

    async def discover_tree(self, db_connection):
        await self._visit(self._root_node, 0, list(self._excluded_targets), db_connection)

    async def _visit(self, node: Extension, depth: int, path_extensions: list, db_connection):
        if depth >= self._max_depth:
            self._log("Routing aborted due to depth limit at {}".format(node))
            self._failed = True
            return

        path_extensions_local = path_extensions.copy()
        path_extensions_local.append(node.extension)

        if node.type != Extension.Type.EXTERNAL and node.forwarding_mode != Extension.ForwardingMode.DISABLED:
            await node.load_forwarding_extension(db_connection)
        if node.type in (Extension.Type.GROUP, Extension.Type.MULTIRING) \
                and (node.forwarding_mode != Extension.ForwardingMode.ENABLED or node.forwarding_delay > 0):
            # we discover group members if there is no immediate forward
            await node.populate_callgroup_ranks(db_connection)
        # now we visit the populated children if they haven't been already discovered
        if node.forwarding_extension is not None:
            fwd = node.forwarding_extension
            if fwd.extension not in path_extensions_local:
                await self._visit(fwd, depth+1, path_extensions_local, db_connection)
            else:
                self._pruned = True
                self._log("Discovery aborted for forward to {}, was already present. Discovery state: {}\n"
                          "Disabling Forward".format(fwd, path_extensions_local))
                node.forwarding_mode = Extension.ForwardingMode.DISABLED
        for callgroup_rank in node.callgroup_ranks:
            for member in callgroup_rank.members:
                # do not discover inactive members
                if not member.active:
                    continue
                ext = member.extension
                if ext.extension not in path_extensions_local:
                    await self._visit(ext, depth+1, path_extensions_local, db_connection)
                else:
                    self._pruned = True
                    self._log("Discovery aborted for {} in {}, was already present. Discovery state: {}\n"
                              "Temporarily disable membership for this routing."
                              .format(ext, callgroup_rank, path_extensions_local))
                    member.active = False


class YateRoutingGenerationVisitor:
    class IntermediateRoutingResult:
        class Type(Enum):
            SIMPLE = 0
            FORK = 1

        def __init__(self, target=None, fork_targets=None):
            if fork_targets is not None:
                self.type = YateRoutingGenerationVisitor.IntermediateRoutingResult.Type.FORK
                self.fork_targets = fork_targets
                self.target = target
            else:
                self.type = YateRoutingGenerationVisitor.IntermediateRoutingResult.Type.SIMPLE
                self.target = target

        @property
        def is_simple(self):
            return self.type == YateRoutingGenerationVisitor.IntermediateRoutingResult.Type.SIMPLE

    def __init__(self, routing_tree: RoutingTree, local_yate_id: int, yates_dict: dict):
        self._routing_tree = routing_tree
        self._local_yate_id = local_yate_id
        self._yates_dict = yates_dict
        self._lateroute_cache = {}
        self._x_eventphone_id = uuid.uuid4().hex

    def get_routing_cache_content(self):
        return self._lateroute_cache

    def calculate_routing(self):
        result = self._visit_for_route_calculation(self._routing_tree.target, [])
        if result.is_simple:
            return result.target, {}
        else:
            return "fork", YateRoutingGenerationVisitor.generate_yate_callfork_params(result.fork_targets)

    def _visit_for_route_calculation(self, node: Extension, path: list):
        local_path = path.copy()
        local_path.append(node.id)

        # first we check if this node has an immediate forward. If yes, we defer routing there.
        if node.immediate_forward:
            return self._visit_for_route_calculation(node.forwarding_extension, local_path)

        if YateRoutingGenerationVisitor.node_has_simple_routing(node):
            print("Node {} has simple routing".format(node))
            return YateRoutingGenerationVisitor.IntermediateRoutingResult(
                target=self.generate_simple_routing_string(node))
        else:
            print("Node {} requires complex routing".format(node))
            # this will require a fork

            # go through the callgroup ranks to issue the groups of the fork
            fork_targets = []
            accumulated_delay = 0
            for rank in node.callgroup_ranks:
                if fork_targets:
                    # this is not the first rank, so we need to generate a separator
                    if rank.mode == CallgroupRank.Mode.DROP:
                        separator = "|drop={}".format(rank.delay)
                        accumulated_delay += rank.delay
                    elif rank.mode == CallgroupRank.Mode.NEXT:
                        separator = "|next={}".format(rank.delay)
                        accumulated_delay += rank.delay
                    else:
                        separator = "|"
                    if accumulated_delay >= self.forwarding_delay:
                        # all of those will not be called, as the forward takes effect now
                        break
                    fork_targets.append(separator)
                for member in rank.members:
                    # do not route inactive members
                    if not member.active:
                        continue
                    member_route = self._visit_for_route_calculation(member.extension, local_path)
                    # please note that we ignore the member modes for the time being
                    fork_targets.append(member_route.target)
            # if this is a MULTIRING, the extension itself needs to be part of the first group
            if node.type == Extension.Type.MULTIRING:
                fork_targets.insert(0, self.generate_simple_routing_string(node))
            # we might need to issue a delayed forward
            if node.forwarding_mode == Extension.ForwardingMode.ENABLED:
                # this is forward with a delay. We want to know how to route there...
                forwarding_route = self._visit_for_route_calculation(node.forwarding_extension, local_path)
                fwd_delay = node.forwarding_delay - accumulated_delay
                fork_targets.append("|drop={}".format(fwd_delay))
                fork_targets.append(forwarding_route.target)
            
            lateroute_address = self.generate_deferred_routestring(local_path)
            self._lateroute_cache[lateroute_address] = fork_targets
            return YateRoutingGenerationVisitor.IntermediateRoutingResult(fork_targets=fork_targets,
                                                                          target=lateroute_address)

    @staticmethod
    def node_has_simple_routing(node: Extension):
        if node.type == Extension.Type.EXTERNAL:
            return True
        # if the node is set to immediate forward, it will be ignored for routing and we calculate the routing for
        # the forwarded node instead
        if node.immediate_forward:
            return YateRoutingGenerationVisitor.node_has_simple_routing(node.forwarding_extension)

        if node.type == Extension.Type.SIMPLE:
            if node.forwarding_mode == Extension.ForwardingMode.DISABLED:
                return True
            # Forwarding with delay or ON_BUSY requires a callfork
            return False
        # multiring is simple, if there are no active multiring participants configured
        if node.type == Extension.Type.MULTIRING:
            if node.has_active_group_members:
                return False
            # ok, nothing to multiring, so look at the forwarding mode
            if node.forwarding_mode == Extension.ForwardingMode.DISABLED:
                return True
            # Forwarding with delay or ON_BUSY requires a callfork
            return False
        # groups might have a simple routing if they have exactly one participant. We will ignore this possibility
        # for the moment being. We could do this by introducing an optimizer stage that reshapes the tree :P
        return False

    def generate_simple_routing_string(self, node: Extension):
        if node.yate_id == self._local_yate_id:
            return "lateroute/stage2-{}".format(node.extension)
        else:
            return "sip/sip:{}@{}".format(node.extension, self._yates_dict[node.yate_id])

    def generate_deferred_routestring(self, path):
        return "lateroute/" + self.generate_node_route_string(path)

    def generate_node_route_string(self, path):
        joined_path = "-".join(map(str, path))
        return "stage1-{}-{}".format(self._x_eventphone_id, joined_path)

    @staticmethod
    def generate_yate_callfork_params(fork_targets):
        index = 1
        params = {}
        for target in fork_targets:
            current_prefix = "callto." + str(index)
            params[current_prefix] = target
            index += 1
        return params
