from ywsd.objects import Yate, Extension, CallgroupRank


class RoutingTree:
    def __init__(self, source, target):
        self.source_extension = source
        self.target_extension = target
        self.source = None
        self.target = None

    async def discover_tree(self, db_connection):
        await self._load_source_and_target(db_connection)
        visitor = RoutingTreeDiscoveryVisitor(self.target, [self.source_extension])
        await visitor.discover_tree(db_connection)
        return visitor

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
    def __init__(self, routing_tree: RoutingTree, local_yate_id: int, yates_dict: dict):
        self._routing_tree = routing_tree
        self._local_yate_id = local_yate_id
        self._yates_dict = yates_dict
        self._calcuates_routing_msgs = []

    def calculate_routing(self):
        pass

    def _visit(self, node: Extension, path: list):
        pass

    @staticmethod
    def check_node_for_simple_routing(node: Extension):
        if node.type == Extension.Type.EXTERNAL:
            return True
        if node.type == Extension.Type.SIMPLE:
            if node.forwarding_mode == Extension.ForwardingMode.DISABLED:
                return True
            elif node.immediate_forward:
                # this depends on the forward target
                return YateRoutingGenerationVisitor.check_node_for_simple_routing(node.forwarding_extension)
            else:
                # Forwarding with delay or ON_BUSY requires a callfork
                return False
        # multiring is simple, if there are no active multiring participants configured
        if node.type == Extension.Type.MULTIRING:
            for rank in node.callgroup_ranks:
                if any([m[1] for m in rank.members]):
                    return False
            # ok, nothing to multiring, so look at the forwarding mode
            if (node.forwarding_mode == Extension.ForwardingMode.DISABLED or
                    (node.forwarding_mode == Extension.ForwardingMode.ENABLED and node.forwarding_delay == 0)):
                return True
            else:
                # Forwarding with delay or ON_BUSY requires a callfork
                return False
