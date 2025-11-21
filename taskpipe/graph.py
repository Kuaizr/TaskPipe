import logging
import asyncio # Added for CompiledGraph.invoke_async
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Dict, List, Tuple, Set, Union
from collections import deque

# Assuming runnables.py is in the same directory or accessible via PYTHONPATH
from .runnables import Runnable, ExecutionContext, InMemoryExecutionContext, NO_INPUT, BaseModel
# To allow CompiledGraph to be an AsyncRunnable if desired, or to call invoke_async
from .async_runnables import AsyncRunnable 
from .registry import RunnableRegistry


logger = logging.getLogger(__name__)
if not logger.hasHandlers(): # Avoid duplicate handlers if already configured
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


@dataclass
class EdgeDefinition:
    source: str
    dest: str
    data_mapping: Dict[str, str] = field(default_factory=dict)
    static_inputs: Dict[str, Any] = field(default_factory=dict)
    branch: Optional[str] = None
    edge_id: str = ""

class WorkflowGraph:
    """
    A builder class for defining graph nodes (Runnable instances) and edges
    (dependencies and data mapping between nodes).
    """
    def __init__(self, name: Optional[str] = None, use_cache: bool = False):
        self.name: str = name or f"WorkflowGraph_{id(self)}"
        self.nodes: Dict[str, Runnable] = {}
        self.edges: Dict[str, List[EdgeDefinition]] = {}
        self._adj: Dict[str, List[str]] = {} 
        self._in_degree: Dict[str, int] = {} 
        self._edge_lookup: Dict[Tuple[str, str, Optional[str]], EdgeDefinition] = {}
        self.entry_points: List[str] = []
        self.output_node_names: List[str] = []
        self.use_cache = use_cache

    def add_node(self, runnable: Runnable, node_name: Optional[str] = None) -> str:
        if not isinstance(runnable, Runnable):
            raise TypeError("Node must be a Runnable instance.")

        name_to_use = node_name or runnable.name
        if not name_to_use: 
            raise ValueError("Runnable must have a name, or node_name must be provided.")

        if name_to_use in self.nodes:
            raise ValueError(f"Node with name '{name_to_use}' already exists in graph '{self.name}'. Node names must be unique.")

        node_instance = runnable.copy() 
        node_instance.name = name_to_use 

        self.nodes[name_to_use] = node_instance
        self._adj.setdefault(name_to_use, [])
        self._in_degree.setdefault(name_to_use, 0)
        return name_to_use

    def add_edge(self,
                 source_node_name: str,
                 dest_node_name: str,
                 data_mapping: Optional[Dict[str, str]] = None,
                 static_inputs: Optional[Dict[str, Any]] = None,
                 branch: Optional[str] = None) -> EdgeDefinition:
        if source_node_name != "__static__" and source_node_name not in self.nodes:
            raise ValueError(f"Source node '{source_node_name}' not found in graph '{self.name}'.")
        if dest_node_name not in self.nodes:
            raise ValueError(f"Destination node '{dest_node_name}' not found in graph '{self.name}'.")

        key = (source_node_name, dest_node_name, branch)
        existing_edge = self._edge_lookup.get(key)
        if existing_edge:
            if data_mapping:
                existing_edge.data_mapping.update(data_mapping)
            if static_inputs:
                existing_edge.static_inputs.update(static_inputs)
            return existing_edge

        edge = EdgeDefinition(
            source=source_node_name,
            dest=dest_node_name,
            data_mapping=dict(data_mapping or {}),
            static_inputs=dict(static_inputs or {}),
            branch=branch,
            edge_id=f"{source_node_name}->{dest_node_name}#{len(self.edges.get(source_node_name, []))}"
        )
        self.edges.setdefault(source_node_name, []).append(edge)
        self._edge_lookup[key] = edge

        if source_node_name != "__static__":
            self._adj.setdefault(source_node_name, []).append(dest_node_name)
            self._in_degree[dest_node_name] = self._in_degree.get(dest_node_name, 0) + 1
        return edge

    def set_entry_point(self, node_name: str) -> 'WorkflowGraph':
        if node_name not in self.nodes:
            raise ValueError(f"Node '{node_name}' not found, cannot set as entry point in graph '{self.name}'.")
        if node_name not in self.entry_points:
            self.entry_points.append(node_name)
        return self

    def set_output_nodes(self, node_names: List[str]) -> 'WorkflowGraph':
        for name in node_names:
            if name not in self.nodes:
                raise ValueError(f"Node '{name}' not found, cannot set as output node in graph '{self.name}'.")
        self.output_node_names = list(node_names) 
        return self

    def to_json(self) -> Dict[str, Any]:
        nodes_payload = []
        for node_name, runnable in self.nodes.items():
            nodes_payload.append(
                {
                    "id": node_name,
                    "name": node_name,
                    "ref": node_name,
                    "type": runnable.__class__.__name__,
                    "input_schema": runnable.describe_input_schema(),
                    "output_schema": runnable.describe_output_schema(),
                }
            )

        edges_payload: List[Dict[str, Any]] = []
        for source, edges in self.edges.items():
            for edge in edges:
                edges_payload.append(
                    {
                        "source": edge.source,
                        "dest": edge.dest,
                        "data_mapping": edge.data_mapping,
                        "static_inputs": edge.static_inputs,
                        "branch": edge.branch,
                    }
                )

        return {
            "name": self.name,
            "nodes": nodes_payload,
            "edges": edges_payload,
            "entry_points": list(self.entry_points),
            "output_nodes": list(self.output_node_names),
        }

    @classmethod
    def from_json(cls, json_data: Dict[str, Any], registry: Union[Dict[str, Union[Runnable, Callable[[], Runnable]]], RunnableRegistry]) -> 'WorkflowGraph':
        graph = cls(name=json_data.get("name"))

        for node_spec in json_data.get("nodes", []):
            node_name = node_spec.get("id") or node_spec.get("name")
            registry_key = node_spec.get("ref", node_name)
            if isinstance(registry, RunnableRegistry):
                runnable_instance = registry.get(registry_key)
            else:
                if registry_key not in registry:
                    raise KeyError(f"Node reference '{registry_key}' not found in registry.")
                runnable_or_factory = registry[registry_key]
                if callable(runnable_or_factory) and not isinstance(runnable_or_factory, Runnable):
                    runnable_instance = runnable_or_factory()
                else:
                    runnable_instance = runnable_or_factory
            if not isinstance(runnable_instance, Runnable):
                raise KeyError(f"Node reference '{registry_key}' not found in registry.")
            graph.add_node(runnable_instance, node_name=node_name)

        for edge_spec in json_data.get("edges", []):
            graph.add_edge(
                edge_spec.get("source", "__static__"),
                edge_spec["dest"],
                data_mapping=edge_spec.get("data_mapping") or {},
                static_inputs=edge_spec.get("static_inputs") or {},
                branch=edge_spec.get("branch")
            )

        for entry_name in json_data.get("entry_points", []):
            graph.set_entry_point(entry_name)

        output_nodes = json_data.get("output_nodes")
        if output_nodes:
            graph.set_output_nodes(output_nodes)

        return graph

    def _get_topological_sort(self) -> List[str]:
        adj = {k: list(v) for k, v in self._adj.items()}
        in_degree = self._in_degree.copy()
        
        queue = deque([node for node, degree in in_degree.items() if degree == 0])
        sorted_nodes: List[str] = []

        while queue:
            u = queue.popleft()
            sorted_nodes.append(u)
            for v_name in adj.get(u, []): 
                in_degree[v_name] -= 1
                if in_degree[v_name] == 0:
                    queue.append(v_name)

        if len(sorted_nodes) != len(self.nodes):
            cycle_nodes_detail = [name for name, deg in in_degree.items() if deg > 0]
            raise Exception(
                f"Graph '{self.name}' has a cycle. Topological sort failed. "
                f"Nodes involved or dependent on cycle: {cycle_nodes_detail}. "
                f"Sorted {len(sorted_nodes)} of {len(self.nodes)} nodes."
            )
        return sorted_nodes

    def _get_execution_stages(self, sorted_nodes: List[str]) -> List[List[str]]:
        in_degree = {node: self._in_degree.get(node, 0) for node in self.nodes.keys()}
        ready = [node for node in sorted_nodes if in_degree.get(node, 0) == 0]
        stages: List[List[str]] = []
        visited = set()
        while ready:
            stage_nodes = sorted(ready, key=lambda n: sorted_nodes.index(n))
            stages.append(stage_nodes)
            next_ready: List[str] = []
            for node in stage_nodes:
                visited.add(node)
                for edge in self.edges.get(node, []):
                    child = edge.dest
                    in_degree[child] = max(0, in_degree.get(child, 0) - 1)
                    if in_degree[child] == 0:
                        next_ready.append(child)
            ready = next_ready
        if len(visited) != len(self.nodes):
            missing = set(self.nodes.keys()) - visited
            raise Exception(f"Failed to build execution stages, unvisited nodes: {missing}")
        return stages

    def compile(self, enable_gc: bool = True) -> 'CompiledGraph':
        if not self.nodes:
            raise ValueError(f"Graph '{self.name}' has no nodes, cannot compile.")

        sorted_node_names = self._get_topological_sort()

        effective_entry_points = self.entry_points
        if not effective_entry_points: 
            effective_entry_points = [node for node, degree in self._in_degree.items() if degree == 0]
            if not effective_entry_points and self.nodes:
                raise ValueError(
                    f"Graph '{self.name}' has no explicit entry points and no nodes with in-degree 0. "
                    "Cannot determine starting points for execution."
                )
        
        effective_output_nodes = self.output_node_names
        if not effective_output_nodes: 
            effective_output_nodes = [node for node in sorted_node_names if not self._adj.get(node)]
            if not effective_output_nodes and self.nodes: 
                effective_output_nodes = [sorted_node_names[-1]] if sorted_node_names else []

        # Calculate Initial Reference Counts for GC
        # ref_count[u] = (number of downstream nodes depending on u) + (1 if u is an output node)
        ref_counts = {node: 0 for node in self.nodes}
        
        for u, neighbors in self._adj.items():
            # Note: use set to de-duplicate. If A->B has multiple edges (mapping multiple fields),
            # B still counts as only 1 consumer.
            unique_consumers = set(neighbors)
            ref_counts[u] += len(unique_consumers)
            
        for out_node in effective_output_nodes:
            ref_counts[out_node] += 1

        return CompiledGraph(
            name=f"Compiled_{self.name}",
            nodes_map=self.nodes, 
            edges_map=self.edges,
            sorted_node_names=sorted_node_names,
            entry_point_names=effective_entry_points,
            output_node_names=effective_output_nodes,
            graph_definition_name=self.name,
            use_cache = self.use_cache,
            execution_stages=self._get_execution_stages(sorted_node_names),
            initial_ref_counts=ref_counts,
            enable_gc=enable_gc
        )


class CompiledGraph(Runnable):
    class InputModel(BaseModel):
        payload: Any | None = None

    class OutputModel(BaseModel):
        payload: Any | None = None
    """
    Runtime representation that executes WorkflowGraph using explicit data mappings
    and an activation queue capable of handling control-flow branches.
    """

    def __init__(self,
                 name: str,
                 nodes_map: Dict[str, Runnable],
                 edges_map: Dict[str, List[EdgeDefinition]],
                 sorted_node_names: List[str],
                 entry_point_names: List[str],
                 output_node_names: List[str],
                 graph_definition_name: str,
                 execution_stages: Optional[List[List[str]]] = None,
                 initial_ref_counts: Optional[Dict[str, int]] = None,
                 enable_gc: bool = True,
                 **kwargs):
        super().__init__(name=name, **kwargs)
        self.nodes: Dict[str, Runnable] = nodes_map
        self.edges: Dict[str, List[EdgeDefinition]] = edges_map
        self.sorted_nodes: List[str] = sorted_node_names
        self.entry_points: List[str] = entry_point_names
        self.output_nodes: List[str] = output_node_names
        self.graph_def_name: str = graph_definition_name
        self.execution_stages: List[List[str]] = execution_stages or [sorted_node_names]
        self.initial_ref_counts: Dict[str, int] = initial_ref_counts or {}
        self.enable_gc = enable_gc

        self._incoming_edges: Dict[str, List[EdgeDefinition]] = {n: [] for n in self.nodes}
        self._branch_edges: Dict[str, List[EdgeDefinition]] = {n: [] for n in self.nodes}
        self._static_inputs: Dict[str, Dict[str, Any]] = {n: {} for n in self.nodes}
        for source_name, edges in self.edges.items():
            for edge in edges:
                if edge.source == "__static__":
                    self._static_inputs[edge.dest].update(edge.static_inputs)
                    continue
                self._incoming_edges.setdefault(edge.dest, []).append(edge)
                if edge.static_inputs:
                    self._static_inputs[edge.dest].update(edge.static_inputs)
                if edge.branch:
                    self._branch_edges.setdefault(edge.dest, []).append(edge)

    def _apply_input_model(self, payload: Any) -> Any:
        return payload

    def _apply_output_model(self, result: Any) -> Any:
        return result

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        logger.info(f"CompiledGraph '{self.name}' (from '{self.graph_def_name}'): Starting SYNC execution. GC Enabled: {self.enable_gc}")
        internal_context = InMemoryExecutionContext(initial_input=input_data, parent_context=context)
        internal_context.log_event(f"Graph '{self.name}': SYNC internal context created. Initial input type: {type(input_data).__name__}")
        self._execute_sync(input_data, internal_context)
        if context:
            context.node_status_events.extend(getattr(internal_context, "node_status_events", []))
        return self._collect_final_results(internal_context)

    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Any:
        logger.info(f"CompiledGraph '{self.name}' (from '{self.graph_def_name}'): Starting ASYNC execution. GC Enabled: {self.enable_gc}")
        internal_context = InMemoryExecutionContext(initial_input=input_data, parent_context=context)
        internal_context.log_event(f"Graph '{self.name}': ASYNC internal context created. Initial input type: {type(input_data).__name__}")
        await self._execute_async(input_data, internal_context)
        if context:
            context.node_status_events.extend(getattr(internal_context, "node_status_events", []))
        return self._collect_final_results(internal_context)

    def _execute_sync(self, graph_input: Any, internal_context: ExecutionContext) -> None:
        state = self._build_initial_state(graph_input)
        # Add runtime reference counts to state for GC
        state["runtime_ref_counts"] = self.initial_ref_counts.copy()
        
        queue = state["queue"]
        while queue:
            node = queue.popleft()
            if state["node_states"][node] != "pending":
                continue
            payload = state["pending_payloads"][node]
            node_input = payload if payload else NO_INPUT
            try:
                result = self.nodes[node].invoke(node_input, internal_context)
            except Exception as exc:
                internal_context.log_event(f"Graph '{self.name}', Node '{node}': FAILED with {type(exc).__name__}: {str(exc)[:120]}")
                raise
            state["node_states"][node] = "completed"
            internal_context.add_output(node, result)
            
            # Trigger GC for inputs of this node (since it has finished consuming them)
            self._perform_garbage_collection(node, state, internal_context)

            result_plain = Runnable._model_to_plain(result)
            self._propagate_output(node, result_plain, state, queue, internal_context)

    async def _execute_async(self, graph_input: Any, internal_context: ExecutionContext) -> None:
        state = self._build_initial_state(graph_input)
        # Add runtime reference counts to state for GC
        state["runtime_ref_counts"] = self.initial_ref_counts.copy()

        queue = state["queue"]
        while queue:
            node = queue.popleft()
            if state["node_states"][node] != "pending":
                continue
            payload = state["pending_payloads"][node]
            node_input = payload if payload else NO_INPUT
            try:
                result = await self.nodes[node].invoke_async(node_input, internal_context)
            except Exception as exc:
                internal_context.log_event(f"Graph '{self.name}', Node '{node}': ASYNC FAILED with {type(exc).__name__}: {str(exc)[:120]}")
                raise
            state["node_states"][node] = "completed"
            internal_context.add_output(node, result)
            
            # Trigger GC for inputs of this node
            self._perform_garbage_collection(node, state, internal_context)

            result_plain = Runnable._model_to_plain(result)
            self._propagate_output(node, result_plain, state, queue, internal_context)

    def _perform_garbage_collection(self, 
                                    finished_node: str, 
                                    state: Dict[str, Any], 
                                    context: ExecutionContext) -> None:
        """
        Garbage Collection Logic:
        When 'finished_node' completes (or is skipped), it has consumed its inputs.
        We decrement the reference count of all its upstream dependencies (parents).
        If a parent's ref count drops to 0 and GC is enabled, we remove its output from context.
        """
        if not self.enable_gc:
            return

        runtime_ref_counts = state.get("runtime_ref_counts")
        if not runtime_ref_counts:
            return

        # Identify unique parents of this node
        incoming_edges = self._incoming_edges.get(finished_node, [])
        unique_parents = set()
        for edge in incoming_edges:
            if edge.source != "__static__":
                unique_parents.add(edge.source)
        
        for parent in unique_parents:
            if parent in runtime_ref_counts:
                runtime_ref_counts[parent] -= 1
                if runtime_ref_counts[parent] <= 0:
                    context.remove_output(parent)
                    # Optional debug log:
                    # logger.debug(f"GC: Node '{parent}' output removed (all consumers finished).")

    def _build_initial_state(self, graph_input: Any) -> Dict[str, Any]:
        queue = deque()
        node_states = {node: "pending" for node in self.nodes}
        pending_payloads = {node: dict(self._static_inputs.get(node, {})) for node in self.nodes}
        incoming_edge_state: Dict[str, Dict[str, str]] = {
            node: {edge.edge_id: "waiting" for edge in self._incoming_edges.get(node, [])}
            for node in self.nodes
        }
        graph_input_payload = self._convert_to_payload(graph_input)

        for node in self.nodes:
            if not self._incoming_edges.get(node):
                if node in self.entry_points and graph_input_payload:
                    pending_payloads[node].update(graph_input_payload)
                queue.append(node)

        return {
            "queue": queue,
            "node_states": node_states,
            "pending_payloads": pending_payloads,
            "incoming_edge_state": incoming_edge_state,
        }

    def _propagate_output(self,
                          node: str,
                          parent_dict: Dict[str, Any],
                          state: Dict[str, Any],
                          queue: deque,
                          internal_context: ExecutionContext) -> None:
        pending_payloads = state["pending_payloads"]
        incoming_edge_state = state["incoming_edge_state"]

        for edge in self.edges.get(node, []):
            if edge.source == "__static__":
                continue
            dest = edge.dest
            if state["node_states"][dest] == "skipped":
                continue

            if not self._edge_activated(edge, parent_dict):
                incoming_edge_state[dest][edge.edge_id] = "inactive"
                self._maybe_schedule_node(dest, state, queue, internal_context)
                continue

            mapped = self._apply_mapping(parent_dict, edge.data_mapping)
            pending_payloads[dest].update(mapped)
            incoming_edge_state[dest][edge.edge_id] = "provided"
            self._maybe_schedule_node(dest, state, queue, internal_context)

    def _edge_activated(self, edge: EdgeDefinition, parent_dict: Dict[str, Any]) -> bool:
        if edge.branch is None:
            return True
        decision = parent_dict.get("decision")
        if decision is None:
            return False
        value = str(decision).lower()
        return value == str(edge.branch).lower()

    def _maybe_schedule_node(self,
                             node: str,
                             state: Dict[str, Any],
                             queue: deque,
                             internal_context: ExecutionContext) -> None:
        if state["node_states"][node] != "pending":
            return
        incoming = state["incoming_edge_state"].get(node, {})
        if any(status == "waiting" for status in incoming.values()):
            return

        has_data = any(status == "provided" for status in incoming.values()) or bool(state["pending_payloads"][node])
        if not incoming and not state["pending_payloads"][node] and node not in self.entry_points:
            has_data = False

        branch_edges = self._branch_edges.get(node, [])
        if branch_edges:
            branch_provided = any(incoming.get(edge.edge_id) == "provided" for edge in branch_edges)
            if not branch_provided:
                self._mark_node_skipped(node, state, queue, internal_context)
                return

        if has_data or not incoming:
            queue.append(node)
            return

        self._mark_node_skipped(node, state, queue, internal_context)

    def _mark_node_skipped(self,
                           node: str,
                           state: Dict[str, Any],
                           queue: deque,
                           internal_context: ExecutionContext) -> None:
        if state["node_states"][node] != "pending":
            return
        state["node_states"][node] = "skipped"
        internal_context.notify_status(node, "skipped", {})
        
        # If a node is skipped, it is "finished" in terms of dependency consumption.
        # Its parents are no longer needed by it.
        self._perform_garbage_collection(node, state, internal_context)

        for edge in self.edges.get(node, []):
            if edge.source == "__static__":
                continue
            dest = edge.dest
            if state["node_states"][dest] == "pending":
                state["incoming_edge_state"][dest][edge.edge_id] = "inactive"
                self._maybe_schedule_node(dest, state, queue, internal_context)

    @staticmethod
    def _convert_to_payload(value: Any) -> Dict[str, Any]:
        return Runnable._model_to_plain(value)

    @staticmethod
    def _apply_mapping(parent_dict: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
        if not mapping or mapping == {"*": "*"}:
            return dict(parent_dict)
        projected: Dict[str, Any] = {}
        for target_field, source_field in mapping.items():
            if source_field == "*":
                projected[target_field] = parent_dict
            else:
                projected[target_field] = parent_dict.get(source_field)
        return projected

    def _collect_final_results(self, internal_graph_context: ExecutionContext) -> Any:
        if not self.output_nodes:
            internal_graph_context.log_event(f"Graph '{self.name}': Execution finished. No output nodes specified.")
            return None

        final_results: Dict[str, Any] = {}
        for out_name in self.output_nodes:
            if out_name in internal_graph_context.node_outputs:
                final_results[out_name] = internal_graph_context.get_output(out_name)
            else:
                final_results[out_name] = None 

        internal_graph_context.log_event(f"Graph '{self.name}': Execution finished. Returning outputs for: {list(final_results.keys())}.")
        
        if len(final_results) == 1:
            return list(final_results.values())[0]
        return final_results

    # To make CompiledGraph fully act like an AsyncRunnable, it would also need invoke_async at the top level
    # And _default_check_async etc. if it's to be a full AsyncRunnable citizen.
    # For now, _internal_invoke calls sync logic, and we add a top-level invoke_async.

    async def invoke_async(self, input_data: Any = NO_INPUT, context: Optional[ExecutionContext] = None) -> Any:
        # This is the public async interface, similar to Runnable.invoke_async
        # It wraps _internal_invoke_async and handles top-level context, caching, retries for the graph itself.
        # For simplicity in this modification, we'll directly call _internal_invoke_async.
        # A full implementation would mirror Runnable.invoke_async's features for the graph as a whole.
        
        effective_context = context if context is not None else InMemoryExecutionContext(
            initial_input=input_data if input_data is not NO_INPUT else None
        )
        
        # Graph-level caching, retry, etc. could be implemented here, similar to Runnable.invoke_async
        # For now, a direct call to the internal async logic:
        return await self._internal_invoke_async(input_data, effective_context)


    def clear_cache(self, cache_name: str = 'all') -> 'CompiledGraph':
        super().clear_cache(cache_name) 
        for node_name, node_runnable in self.nodes.items():
            if hasattr(node_runnable, 'clear_cache'):
                node_runnable.clear_cache(cache_name)
        return self

    def __repr__(self) -> str:
        return f"<CompiledGraph name='{self.name}' (from_def='{self.graph_def_name}') nodes={len(self.nodes)}>"
