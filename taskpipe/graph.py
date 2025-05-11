import logging
from typing import Any, Callable, Optional, Dict, List, Tuple, Set, Union
from collections import deque

# Assuming runnables.py is in the same directory or accessible via PYTHONPATH
from .runnables import Runnable, ExecutionContext, NO_INPUT, SimpleTask

logger = logging.getLogger(__name__)
if not logger.hasHandlers(): # Avoid duplicate handlers if already configured
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class WorkflowGraph:
    """
    A builder class for defining graph nodes (Runnable instances) and edges
    (dependencies and data mapping between nodes).
    """
    def __init__(self, name: Optional[str] = None):
        self.name: str = name or f"WorkflowGraph_{id(self)}"
        self.nodes: Dict[str, Runnable] = {}
        # edges: source_name -> list of (dest_name, input_mapper_func)
        # input_mapper_func: (source_output, all_context_outputs_dict) -> input_for_dest_node
        self.edges: Dict[str, List[Tuple[str, Optional[Callable[[Any, Dict[str, Any]], Any]]]]] = {}
        
        # For topological sort and graph traversal
        self._adj: Dict[str, List[str]] = {} # Adjacency list: node_name -> list of child_node_names
        self._in_degree: Dict[str, int] = {} # node_name -> count of incoming edges
        
        self.entry_points: List[str] = []
        self.output_node_names: List[str] = []

    def add_node(self, runnable: Runnable, node_name: Optional[str] = None) -> str:
        """
        Adds a node to the graph. Uses a copy of the runnable.
        Ensures the node name is unique within the graph.

        Args:
            runnable: The Runnable instance to add.
            node_name: Optional explicit name for the node in the graph.
                       If None, uses runnable.name.

        Returns:
            The name of the node in the graph.
        
        Raises:
            TypeError: If runnable is not a Runnable instance.
            ValueError: If node name is not provided or already exists.
        """
        if not isinstance(runnable, Runnable):
            raise TypeError("Node must be a Runnable instance.")

        name_to_use = node_name or runnable.name
        if not name_to_use: # Should be handled by Runnable's default naming
            raise ValueError("Runnable must have a name, or node_name must be provided.")

        if name_to_use in self.nodes:
            raise ValueError(f"Node with name '{name_to_use}' already exists in graph '{self.name}'. Node names must be unique.")

        # Use a copy to ensure graph nodes are distinct even if the same runnable object is conceptually added multiple times.
        node_instance = runnable.copy() 
        node_instance.name = name_to_use # Assign the graph-specific unique name to the copy

        self.nodes[name_to_use] = node_instance
        self._adj.setdefault(name_to_use, [])
        self._in_degree.setdefault(name_to_use, 0)
        # logger.debug(f"Graph '{self.name}': Node '{name_to_use}' (type: {type(node_instance).__name__}) added.")
        return name_to_use

    def add_edge(self,
                 source_node_name: str,
                 dest_node_name: str,
                 input_mapper: Optional[Callable[[Any, Dict[str, Any]], Any]] = None):
        """
        Adds a directed edge from source_node to dest_node.

        Args:
            source_node_name: Name of the source node.
            dest_node_name: Name of the destination node.
            input_mapper: Optional function to transform the source_node's output
                          before it's used by the dest_node (or its input_declaration).
                          Signature: mapper(source_output, all_outputs_in_context) -> input_for_dest
        
        Raises:
            ValueError: If source or destination node not found.
        """
        if source_node_name not in self.nodes:
            raise ValueError(f"Source node '{source_node_name}' not found in graph '{self.name}'.")
        if dest_node_name not in self.nodes:
            raise ValueError(f"Destination node '{dest_node_name}' not found in graph '{self.name}'.")

        self.edges.setdefault(source_node_name, []).append((dest_node_name, input_mapper))
        self._adj.setdefault(source_node_name, []).append(dest_node_name) # Update adjacency list
        self._in_degree[dest_node_name] = self._in_degree.get(dest_node_name, 0) + 1 # Update in-degree
        # logger.debug(f"Graph '{self.name}': Edge added from '{source_node_name}' to '{dest_node_name}'. Mapper: {'Yes' if input_mapper else 'No'}.")

    def set_entry_point(self, node_name: str) -> 'WorkflowGraph':
        """Sets a node as an entry point for the graph."""
        if node_name not in self.nodes:
            raise ValueError(f"Node '{node_name}' not found, cannot set as entry point in graph '{self.name}'.")
        if node_name not in self.entry_points:
            self.entry_points.append(node_name)
            # logger.debug(f"Graph '{self.name}': Node '{node_name}' set as an entry point.")
        return self

    def set_output_nodes(self, node_names: List[str]) -> 'WorkflowGraph':
        """Sets the designated output nodes for the graph."""
        for name in node_names:
            if name not in self.nodes:
                raise ValueError(f"Node '{name}' not found, cannot set as output node in graph '{self.name}'.")
        self.output_node_names = list(node_names) # Store a copy
        # logger.debug(f"Graph '{self.name}': Output nodes set to: {node_names}.")
        return self

    def _get_topological_sort(self) -> List[str]:
        """
        Performs a topological sort of the graph nodes using Kahn's algorithm.
        
        Returns:
            A list of node names in topological order.
        
        Raises:
            Exception: If the graph contains a cycle.
        """
        # Make copies for processing
        adj = {k: list(v) for k, v in self._adj.items()}
        in_degree = self._in_degree.copy()
        
        queue = deque([node for node, degree in in_degree.items() if degree == 0])
        sorted_nodes: List[str] = []

        while queue:
            u = queue.popleft()
            sorted_nodes.append(u)
            for v_name in adj.get(u, []): # Iterate over neighbors of u
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
        
        # logger.info(f"Graph '{self.name}': Topological sort successful: {sorted_nodes}")
        return sorted_nodes

    def compile(self) -> 'CompiledGraph':
        """
        Analyzes the graph structure, performs topological sort, and returns a CompiledGraph instance.
        """
        # logger.info(f"Graph '{self.name}': Compiling...")
        if not self.nodes:
            raise ValueError(f"Graph '{self.name}' has no nodes, cannot compile.")

        sorted_node_names = self._get_topological_sort()

        effective_entry_points = self.entry_points
        if not effective_entry_points: # If no explicit entry points, use nodes with in-degree 0
            effective_entry_points = [node for node, degree in self._in_degree.items() if degree == 0]
            if not effective_entry_points and self.nodes:
                raise ValueError(
                    f"Graph '{self.name}' has no explicit entry points and no nodes with in-degree 0. "
                    "Cannot determine starting points for execution."
                )
            # logger.info(f"Graph '{self.name}': No explicit entry points, using in-degree 0 nodes: {effective_entry_points}")
        
        effective_output_nodes = self.output_node_names
        if not effective_output_nodes: # Default to leaf nodes (no outgoing edges)
            effective_output_nodes = [node for node in sorted_node_names if not self._adj.get(node)]
            if not effective_output_nodes and self.nodes: # e.g., single node graph or all nodes in a cycle (already caught by topo sort)
                effective_output_nodes = [sorted_node_names[-1]] if sorted_node_names else []
            # logger.info(f"Graph '{self.name}': No explicit output nodes, defaulting to leaf/last nodes: {effective_output_nodes}")

        # logger.info(f"Graph '{self.name}': Compilation successful.")
        return CompiledGraph(
            name=f"Compiled_{self.name}",
            nodes_map=self.nodes, # Pass the dict of node instances
            edges_map=self.edges,
            sorted_node_names=sorted_node_names,
            entry_point_names=effective_entry_points,
            output_node_names=effective_output_nodes,
            graph_definition_name=self.name
        )


class CompiledGraph(Runnable):
    """
    The runnable runtime representation of a WorkflowGraph.
    """
    def __init__(self,
                 name: str,
                 nodes_map: Dict[str, Runnable],
                 edges_map: Dict[str, List[Tuple[str, Optional[Callable]]]],
                 sorted_node_names: List[str],
                 entry_point_names: List[str],
                 output_node_names: List[str],
                 graph_definition_name: str,
                 **kwargs): # For Runnable base class args
        super().__init__(name=name, **kwargs)
        self.nodes: Dict[str, Runnable] = nodes_map
        self.edges: Dict[str, List[Tuple[str, Optional[Callable]]]] = edges_map
        self.sorted_nodes: List[str] = sorted_node_names
        self.entry_points: List[str] = entry_point_names
        self.output_nodes: List[str] = output_node_names
        self.graph_def_name: str = graph_definition_name # For logging/reference

        # Pre-calculate parent information for easier input determination (if needed)
        self._node_parents_info: Dict[str, List[Tuple[str, Optional[Callable]]]] = {n: [] for n in self.nodes}
        for source_name, destinations in self.edges.items():
            for dest_name, mapper_fn in destinations:
                self._node_parents_info[dest_name].append((source_name, mapper_fn))

    def _internal_invoke(self, input_data: Any, context: ExecutionContext) -> Any:
        # logger.info(f"CompiledGraph '{self.name}' (from '{self.graph_def_name}'): Starting execution.")
        
        # Create an internal context for this graph run, with parent linkage
        internal_graph_context = ExecutionContext(initial_input=input_data, parent_context=context)
        internal_graph_context.log_event(f"Graph '{self.name}': Internal context created. Initial input type: {type(input_data).__name__}")

        for node_name in self.sorted_nodes:
            node_runnable = self.nodes[node_name]
            internal_graph_context.log_event(f"Graph '{self.name}': Processing node '{node_name}' (type: {type(node_runnable).__name__}).")

            current_node_input = NO_INPUT # Default input to node's invoke

            if node_runnable.input_declaration:
                # If node declares its inputs, Runnable.invoke will handle fetching from internal_graph_context.
                # We pass NO_INPUT, and the Runnable's invoke method uses its declaration.
                internal_graph_context.log_event(f"Graph '{self.name}', Node '{node_name}': Has input_declaration. Will fetch from context.")
                # current_node_input remains NO_INPUT
            else:
                # Node has no input_declaration. It relies on direct input.
                parent_edges_to_this_node = self._node_parents_info.get(node_name, [])
                
                if node_name in self.entry_points and not parent_edges_to_this_node:
                    # This is a true entry point (no graph parents feeding it).
                    # It receives the graph's overall input_data.
                    current_node_input = input_data
                    internal_graph_context.log_event(f"Graph '{self.name}', Node '{node_name}': Is entry point, using graph's input_data.")
                elif len(parent_edges_to_this_node) == 1:
                    source_parent_name, input_mapper_fn = parent_edges_to_this_node[0]
                    parent_output = internal_graph_context.get_output(source_parent_name)
                    if input_mapper_fn:
                        # Apply mapper: mapper(source_output, all_outputs_in_current_context)
                        current_node_input = input_mapper_fn(parent_output, internal_graph_context.node_outputs.copy())
                        internal_graph_context.log_event(f"Graph '{self.name}', Node '{node_name}': Input from parent '{source_parent_name}' via mapper.")
                    else:
                        current_node_input = parent_output # Direct pass
                        internal_graph_context.log_event(f"Graph '{self.name}', Node '{node_name}': Input from parent '{source_parent_name}' (direct).")
                elif len(parent_edges_to_this_node) > 1:
                    # Multiple parents and no input_declaration. This is ambiguous.
                    # The node *must* use input_declaration to specify how to combine/select inputs.
                    # Or it must be a special node type that inherently handles multiple inputs (e.g. a list).
                    internal_graph_context.log_event(
                        f"Graph '{self.name}', Node '{node_name}': Has multiple parents and no input_declaration. "
                        "Input remains NO_INPUT. Node must handle this or declare inputs."
                    )
                    logger.warning(
                        f"Node '{node_name}' in graph '{self.name}' has multiple incoming edges but no input_declaration. "
                        "It will receive NO_INPUT directly. Ensure the node is designed to handle this (e.g., by fetching all its inputs from context via a custom function in its own logic if not using standard input_declaration)."
                    )
                    # current_node_input remains NO_INPUT
                # If no parents and not an entry point (shouldn't happen in a valid sorted graph unless it's an isolated node not part of entry/output paths)
                # current_node_input remains NO_INPUT

            try:
                # Node's invoke method handles its input_declaration using internal_graph_context
                node_runnable.invoke(current_node_input, internal_graph_context)
            except Exception as e:
                logger.error(f"CompiledGraph '{self.name}': Error executing node '{node_name}': {e}", exc_info=True)
                internal_graph_context.log_event(f"Graph '{self.name}', Node '{node_name}': FAILED with {type(e).__name__}: {str(e)[:100]}.")
                raise # Propagate error

        # Collect final outputs
        if not self.output_nodes:
            # logger.warning(f"CompiledGraph '{self.name}': No output nodes specified. Returning None.")
            internal_graph_context.log_event(f"Graph '{self.name}': Execution finished. No output nodes specified.")
            return None

        final_results: Dict[str, Any] = {}
        for out_name in self.output_nodes:
            if out_name in internal_graph_context.node_outputs:
                final_results[out_name] = internal_graph_context.get_output(out_name)
            else:
                # logger.warning(f"CompiledGraph '{self.name}': Output node '{out_name}' not found in final context or produced no output.")
                final_results[out_name] = None # Or raise an error, or skip

        # logger.info(f"CompiledGraph '{self.name}': Execution finished. Returning outputs for: {list(final_results.keys())}.")
        internal_graph_context.log_event(f"Graph '{self.name}': Execution finished. Returning outputs for: {list(final_results.keys())}.")
        
        if len(final_results) == 1:
            return list(final_results.values())[0]
        return final_results

    def clear_cache(self, cache_name: str = 'all') -> 'CompiledGraph':
        """
        Clears the cache for this CompiledGraph and all its internal nodes.
        """
        super().clear_cache(cache_name) # Clear this graph's own invoke/check cache
        # logger.debug(f"CompiledGraph '{self.name}': Cleared its own cache(s).")
        for node_name, node_runnable in self.nodes.items():
            if hasattr(node_runnable, 'clear_cache'):
                # logger.debug(f"CompiledGraph '{self.name}': Clearing cache for internal node '{node_name}'.")
                node_runnable.clear_cache(cache_name)
        # logger.info(f"CompiledGraph '{self.name}': Cache cleared for self and all internal nodes.")
        return self

    def __repr__(self) -> str:
        return f"<CompiledGraph name='{self.name}' (from_def='{self.graph_def_name}') nodes={len(self.nodes)}>"