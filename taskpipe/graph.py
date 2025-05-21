import logging
import asyncio # Added for CompiledGraph.invoke_async
from typing import Any, Callable, Optional, Dict, List, Tuple, Set, Union
from collections import deque

# Assuming runnables.py is in the same directory or accessible via PYTHONPATH
from .runnables import Runnable, ExecutionContext, NO_INPUT, SimpleTask
# To allow CompiledGraph to be an AsyncRunnable if desired, or to call invoke_async
from .async_runnables import AsyncRunnable 


logger = logging.getLogger(__name__)
if not logger.hasHandlers(): # Avoid duplicate handlers if already configured
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class WorkflowGraph:
    """
    A builder class for defining graph nodes (Runnable instances) and edges
    (dependencies and data mapping between nodes).
    """
    def __init__(self, name: Optional[str] = None, use_cache: bool = False):
        self.name: str = name or f"WorkflowGraph_{id(self)}"
        self.nodes: Dict[str, Runnable] = {}
        self.edges: Dict[str, List[Tuple[str, Optional[Callable[[Any, Dict[str, Any]], Any]]]]] = {}
        self._adj: Dict[str, List[str]] = {} 
        self._in_degree: Dict[str, int] = {} 
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
                 input_mapper: Optional[Callable[[Any, Dict[str, Any]], Any]] = None):
        if source_node_name not in self.nodes:
            raise ValueError(f"Source node '{source_node_name}' not found in graph '{self.name}'.")
        if dest_node_name not in self.nodes:
            raise ValueError(f"Destination node '{dest_node_name}' not found in graph '{self.name}'.")

        self.edges.setdefault(source_node_name, []).append((dest_node_name, input_mapper))
        self._adj.setdefault(source_node_name, []).append(dest_node_name) 
        self._in_degree[dest_node_name] = self._in_degree.get(dest_node_name, 0) + 1 
        return self # Allow chaining

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

    def compile(self) -> 'CompiledGraph':
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

        return CompiledGraph(
            name=f"Compiled_{self.name}",
            nodes_map=self.nodes, 
            edges_map=self.edges,
            sorted_node_names=sorted_node_names,
            entry_point_names=effective_entry_points,
            output_node_names=effective_output_nodes,
            graph_definition_name=self.name,
            use_cache = self.use_cache
        )


class CompiledGraph(Runnable): # Can also inherit AsyncRunnable if it implements all its abstract methods
    """
    The runnable runtime representation of a WorkflowGraph.
    Now supports both synchronous and asynchronous execution.
    """
    def __init__(self,
                 name: str,
                 nodes_map: Dict[str, Runnable],
                 edges_map: Dict[str, List[Tuple[str, Optional[Callable]]]],
                 sorted_node_names: List[str],
                 entry_point_names: List[str],
                 output_node_names: List[str],
                 graph_definition_name: str,
                 **kwargs): 
        super().__init__(name=name, **kwargs) # Runnable's __init__
        self.nodes: Dict[str, Runnable] = nodes_map
        self.edges: Dict[str, List[Tuple[str, Optional[Callable]]]] = edges_map
        self.sorted_nodes: List[str] = sorted_node_names
        self.entry_points: List[str] = entry_point_names
        self.output_nodes: List[str] = output_node_names
        self.graph_def_name: str = graph_definition_name 

        self._node_parents_info: Dict[str, List[Tuple[str, Optional[Callable]]]] = {n: [] for n in self.nodes}
        for source_name, destinations in self.edges.items():
            for dest_name, mapper_fn in destinations:
                self._node_parents_info[dest_name].append((source_name, mapper_fn))

    def _prepare_node_input(self, node_name: str, graph_input_data: Any, current_context: ExecutionContext) -> Any:
        node_runnable = self.nodes[node_name]
        current_node_input = NO_INPUT

        if node_runnable.input_declaration:
            current_context.log_event(f"Graph '{self.name}', Node '{node_name}': Has input_declaration. Will fetch from context.")
            # Input will be resolved by the node's invoke/invoke_async method itself from context
        else:
            parent_edges_to_this_node = self._node_parents_info.get(node_name, [])
            
            if node_name in self.entry_points and not parent_edges_to_this_node:
                current_node_input = graph_input_data
                current_context.log_event(f"Graph '{self.name}', Node '{node_name}': Is entry point, using graph's input_data.")
            elif len(parent_edges_to_this_node) == 1:
                source_parent_name, input_mapper_fn = parent_edges_to_this_node[0]
                parent_output = current_context.get_output(source_parent_name)
                if input_mapper_fn:
                    # input_mapper_fn could be async, but current signature is sync.
                    # If mapper needs to be async, this part requires more changes.
                    current_node_input = input_mapper_fn(parent_output, current_context.node_outputs.copy())
                    current_context.log_event(f"Graph '{self.name}', Node '{node_name}': Input from parent '{source_parent_name}' via mapper.")
                else:
                    current_node_input = parent_output
                    current_context.log_event(f"Graph '{self.name}', Node '{node_name}': Input from parent '{source_parent_name}' (direct).")
            elif len(parent_edges_to_this_node) > 1:
                current_context.log_event(
                    f"Graph '{self.name}', Node '{node_name}': Has multiple parents and no input_declaration. "
                    "Input remains NO_INPUT. Node must handle this or declare inputs."
                )
                logger.warning(
                    f"Node '{node_name}' in graph '{self.name}' has multiple incoming edges but no input_declaration. "
                    "It will receive NO_INPUT directly."
                )
        return current_node_input

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        # This is the synchronous execution path
        logger.info(f"CompiledGraph '{self.name}' (from '{self.graph_def_name}'): Starting SYNC execution.")
        internal_graph_context = ExecutionContext(initial_input=input_data, parent_context=context)
        internal_graph_context.log_event(f"Graph '{self.name}': SYNC internal context created. Initial input type: {type(input_data).__name__}")

        for node_name in self.sorted_nodes:
            node_runnable = self.nodes[node_name]
            internal_graph_context.log_event(f"Graph '{self.name}': SYNC Processing node '{node_name}' (type: {type(node_runnable).__name__}).")
            
            current_node_input = self._prepare_node_input(node_name, input_data, internal_graph_context)

            try:
                node_runnable.invoke(current_node_input, internal_graph_context) # SYNC invoke
            except Exception as e:
                logger.error(f"CompiledGraph '{self.name}': SYNC Error executing node '{node_name}': {e}", exc_info=True)
                internal_graph_context.log_event(f"Graph '{self.name}', Node '{node_name}': SYNC FAILED with {type(e).__name__}: {str(e)[:100]}.")
                raise e

        return self._collect_final_results(internal_graph_context)

    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Any:
        # This is the asynchronous execution path
        logger.info(f"CompiledGraph '{self.name}' (from '{self.graph_def_name}'): Starting ASYNC execution.")
        internal_graph_context = ExecutionContext(initial_input=input_data, parent_context=context)
        internal_graph_context.log_event(f"Graph '{self.name}': ASYNC internal context created. Initial input type: {type(input_data).__name__}")

        for node_name in self.sorted_nodes:
            node_runnable = self.nodes[node_name]
            internal_graph_context.log_event(f"Graph '{self.name}': ASYNC Processing node '{node_name}' (type: {type(node_runnable).__name__}).")

            current_node_input = self._prepare_node_input(node_name, input_data, internal_graph_context)
            
            try:
                await node_runnable.invoke_async(current_node_input, internal_graph_context) # ASYNC invoke_async
            except Exception as e:
                logger.error(f"CompiledGraph '{self.name}': ASYNC Error executing node '{node_name}': {e}", exc_info=True)
                internal_graph_context.log_event(f"Graph '{self.name}', Node '{node_name}': ASYNC FAILED with {type(e).__name__}: {str(e)[:100]}.")
                raise
        
        return self._collect_final_results(internal_graph_context)

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
        
        effective_context = context if context is not None else ExecutionContext(
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