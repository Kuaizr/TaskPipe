import logging
import asyncio 
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Dict, List, Tuple, Union, Set
from collections import deque

from .runnables import (
    Runnable, ExecutionContext, InMemoryExecutionContext, NO_INPUT, 
    BaseModel, START, END, _deep_get, SuspendExecution
)
from .async_runnables import AsyncRunnable 
from .registry import RunnableRegistry

logger = logging.getLogger(__name__)

@dataclass
class EdgeDefinition:
    source: str; dest: str
    data_mapping: Dict[str, str] = field(default_factory=dict)
    static_inputs: Dict[str, Any] = field(default_factory=dict)
    branch: Optional[str] = None
    edge_id: str = ""

class WorkflowGraph:
    def __init__(self, name=None, use_cache=False):
        self.name = name or f"WorkflowGraph_{id(self)}"
        self.nodes = {}; self.edges = {}; self._adj = {}; self._in_degree = {}
        self.entry_points = []; self.output_node_names = []; self.use_cache = use_cache

    def add_node(self, runnable, node_name=None):
        name = node_name or runnable.name
        if name in self.nodes: raise ValueError(f"Node {name} exists")
        self.nodes[name] = runnable.copy(); self.nodes[name].name = name
        self._adj.setdefault(name, []); self._in_degree.setdefault(name, 0)
        return name

    def add_edge(self, source, dest, data_mapping=None, static_inputs=None, branch=None):
        edge = EdgeDefinition(source, dest, dict(data_mapping or {}), dict(static_inputs or {}), branch, f"{source}->{dest}#{len(self.edges.get(source, []))}")
        self.edges.setdefault(source, []).append(edge)
        if source != "__static__":
            self._adj.setdefault(source, []).append(dest)
            self._in_degree[dest] = self._in_degree.get(dest, 0) + 1
        return edge

    def set_entry_point(self, name): self.entry_points.append(name); return self
    def set_output_nodes(self, names): self.output_node_names = list(names); return self

    def _get_topological_sort(self):
        ind = self._in_degree.copy(); q = deque([n for n, d in ind.items() if d == 0])
        res = []
        while q:
            u = q.popleft(); res.append(u)
            for v in self._adj.get(u, []):
                ind[v] -= 1; 
                if ind[v] == 0: q.append(v)
        if len(res) != len(self.nodes): raise Exception("Cycle")
        return res

    def _get_execution_stages(self, sorted_nodes):
        ind = {n: self._in_degree.get(n, 0) for n in self.nodes}; ready = [n for n in sorted_nodes if ind.get(n, 0) == 0]
        stages = []
        while ready:
            stages.append(ready); next_r = []
            for n in ready:
                for e in self.edges.get(n, []):
                    child = e.dest; ind[child] = max(0, ind[child]-1)
                    if ind[child] == 0: next_r.append(child)
            ready = next_r
        return stages

    def compile(self, enable_gc=True):
        sorted_nodes = self._get_topological_sort()
        
        start_nodes = [n for n in self.nodes.values() if isinstance(n, START)]
        end_nodes = [n for n in self.nodes.values() if isinstance(n, END)]

        graph_input_model = None
        if len(start_nodes) > 1:
            raise ValueError(f"Graph '{self.name}' defines multiple START nodes.")
        elif len(start_nodes) == 1:
            graph_input_model = start_nodes[0].OutputModel

        graph_output_model = None
        effective_output_names = list(self.output_node_names)
        
        if end_nodes:
            first_schema = end_nodes[0].InputModel
            for node in end_nodes[1:]:
                if node.InputModel != first_schema:
                    raise TypeError(f"Graph '{self.name}' END nodes have conflicting schemas.")
            graph_output_model = first_schema
            if not effective_output_names:
                effective_output_names = [n.name for n in end_nodes]
        else:
            if not effective_output_names and sorted_nodes:
                effective_output_names = [sorted_nodes[-1]]

        natural_roots = [n for n, d in self._in_degree.items() if d == 0]
        effective_entry = list(self.entry_points) if self.entry_points else list(natural_roots)
        
        if start_nodes:
            s_name = start_nodes[0].name
            if s_name not in effective_entry:
                effective_entry.append(s_name)

        ref_counts = {n: 0 for n in self.nodes}
        for u, neighbors in self._adj.items():
            ref_counts[u] += len(set(neighbors))
        for out_node in effective_output_names:
            ref_counts[out_node] += 1

        compiled = CompiledGraph(
            name=f"Compiled_{self.name}",
            nodes=self.nodes,
            edges=self.edges,
            sorted=sorted_nodes,
            entry=effective_entry,
            output=effective_output_names,
            def_name=self.name,
            stages=self._get_execution_stages(sorted_nodes),
            refs=ref_counts,
            gc=enable_gc,
            use_cache=self.use_cache
        )
        
        if graph_input_model: compiled.InputModel = graph_input_model
        if graph_output_model: compiled.OutputModel = graph_output_model
            
        return compiled

    def to_json(self): 
        return {
            "name": self.name,
            "nodes": [
                {"id": k, **v.to_dict()} 
                for k, v in self.nodes.items()
            ],
            "edges": [vars(e) for es in self.edges.values() for e in es],
            "entry_points": self.entry_points,
            "output_nodes": self.output_node_names
        }

    @classmethod
    def from_json(cls, data, registry):
        g = cls(data.get("name")); reg = registry if isinstance(registry, RunnableRegistry) else registry
        for n in data.get("nodes", []): g.add_node(reg.get(n.get("ref", n.get("id"))) if hasattr(reg, "get") else reg[n.get("ref", n.get("id"))], n.get("id"))
        for e in data.get("edges", []): g.add_edge(e.get("source", "__static__"), e["dest"], e.get("data_mapping"), e.get("static_inputs"), e.get("branch"))
        if data.get("entry_points"): g.entry_points = data["entry_points"]
        if data.get("output_nodes"): g.output_node_names = data["output_nodes"]
        return g

class CompiledGraph(Runnable):
    def __init__(self, name, nodes, edges, sorted, entry, output, def_name, stages, refs, gc, **kw):
        super().__init__(name, **kw); self.nodes = nodes; self.edges = edges; self.sorted_nodes = sorted
        self.entry_points = entry; self.output_nodes = output; self.graph_def_name = def_name
        self.execution_stages = stages; self.initial_ref_counts = refs; self.enable_gc = gc
        self._incoming = {n: [] for n in nodes}; self._static = {n: {} for n in nodes}
        self.InputModel = None; self.OutputModel = None
        
        for s, es in edges.items():
            for e in es:
                if e.source == "__static__": self._static[e.dest].update(e.static_inputs)
                else: self._incoming[e.dest].append(e)

    def _internal_invoke(self, input_data, context, resume_state=None):
        state = resume_state if resume_state else self._init_state(input_data)
        i_ctx = InMemoryExecutionContext(input_data, context)
        if resume_state and "ctx_data" in resume_state:
            i_ctx.node_outputs = resume_state["ctx_data"].copy()
            
        self._run_sync(state, i_ctx)
        return self._collect_results(i_ctx)

    async def _internal_invoke_async(self, input_data, context, resume_state=None):
        state = resume_state if resume_state else self._init_state(input_data)
        i_ctx = InMemoryExecutionContext(input_data, context)
        if resume_state and "ctx_data" in resume_state:
            i_ctx.node_outputs = resume_state["ctx_data"].copy()
            
        await self._run_async(state, i_ctx)
        return self._collect_results(i_ctx)

    def _run_sync(self, state, ctx):
        queue = state["queue"]
        while queue:
            node = queue.popleft()
            if state["node_states"][node] != "pending": continue
            
            inp = state["payloads"][node]
            child_resume = state.get("child_states", {}).get(node)
            
            try:
                res = self.nodes[node].invoke(inp or NO_INPUT, ctx, resume_state=child_resume)
                state["node_states"][node] = "completed"
                if "child_states" in state and node in state["child_states"]: del state["child_states"][node]
                ctx.add_output(node, res)
                self._gc(node, state, ctx)
                self._propagate(node, self._model_to_plain(res), state, ctx)
            except SuspendExecution as e:
                state["node_states"][node] = "pending"
                state["queue"].appendleft(node)
                if "child_states" not in state: state["child_states"] = {}
                state["child_states"][node] = e.snapshot
                state["ctx_data"] = ctx.node_outputs.copy()
                raise SuspendExecution(state)
            except Exception as e: raise e

    async def _run_async(self, state, ctx):
        queue = state["queue"]
        while queue:
            node = queue.popleft()
            if state["node_states"][node] != "pending": continue
            
            inp = state["payloads"][node]
            child_resume = state.get("child_states", {}).get(node)
            
            try:
                res = await self.nodes[node].invoke_async(inp or NO_INPUT, ctx, resume_state=child_resume)
                state["node_states"][node] = "completed"
                if "child_states" in state and node in state["child_states"]: del state["child_states"][node]
                ctx.add_output(node, res)
                self._gc(node, state, ctx)
                self._propagate(node, self._model_to_plain(res), state, ctx)
            except SuspendExecution as e:
                state["node_states"][node] = "pending"
                state["queue"].appendleft(node)
                if "child_states" not in state: state["child_states"] = {}
                state["child_states"][node] = e.snapshot
                state["ctx_data"] = ctx.node_outputs.copy()
                raise SuspendExecution(state)
            except Exception as e: raise e

    def _init_state(self, input_data):
        payload = self._convert_to_payload(input_data)
        state = {
            "queue": deque(), 
            "node_states": {n: "pending" for n in self.nodes},
            "payloads": {n: dict(self._static.get(n, {})) for n in self.nodes},
            "edge_states": {n: {e.edge_id: "waiting" for e in self._incoming[n]} for n in self.nodes},
            "refs": self.initial_ref_counts.copy(),
            "child_states": {} 
        }
        
        for n in self.nodes:
            if isinstance(self.nodes[n], START) and n in self.entry_points:
                state["payloads"][n].update(payload)
                
        for n in self.nodes:
            if not self._incoming[n]:
                if n in self.entry_points:
                    state["queue"].append(n)
        return state

    def _propagate(self, node, result, state, ctx):
        for edge in self.edges.get(node, []):
            if edge.source == "__static__": continue
            dest = edge.dest
            if state["node_states"][dest] == "skipped": continue
            
            decision = result.get("decision")
            activated = True
            if edge.branch is not None:
                activated = str(decision).lower() == str(edge.branch).lower() if decision is not None else False
            
            if activated:
                mapped = self._apply_mapping(result, edge.data_mapping)
                state["payloads"][dest].update(mapped)
                state["edge_states"][dest][edge.edge_id] = "provided"
            else:
                state["edge_states"][dest][edge.edge_id] = "inactive"
            
            self._check_ready(dest, state, ctx)

    def _check_ready(self, node, state, ctx):
        inc = state["edge_states"][node]
        if any(s == "waiting" for s in inc.values()): return
        
        incoming_edges = self._incoming[node]
        should_skip_by_gate = False
        for edge in incoming_edges:
            if edge.branch is not None: 
                if inc[edge.edge_id] == "inactive":
                    should_skip_by_gate = True
                    break
        
        if should_skip_by_gate:
            has_data = False
        else:
            has_data = any(s == "provided" for s in inc.values()) or bool(state["payloads"][node])
            
            if not inc and not state["payloads"][node] and node not in self.entry_points: 
                has_data = False
            elif not inc and node in self.entry_points:
                has_data = True

        if has_data: state["queue"].append(node)
        else: 
            state["node_states"][node] = "skipped"
            ctx.notify_status(node, "skipped")
            self._gc(node, state, ctx)
            for e in self.edges.get(node, []):
                state["edge_states"][e.dest][e.edge_id] = "inactive"
                self._check_ready(e.dest, state, ctx)

    def _gc(self, node, state, ctx):
        if not self.enable_gc: return
        for e in self._incoming[node]:
            p = e.source
            if p != "__static__" and p in state["refs"]:
                state["refs"][p] -= 1
                if state["refs"][p] <= 0: ctx.remove_output(p)

    @staticmethod
    def _apply_mapping(parent, mapping):
        if not mapping or mapping == {"*": "*"}: return dict(parent)
        return {t: parent if s == "*" else _deep_get(parent, s) for t, s in mapping.items()}

    def _collect_results(self, ctx):
        res = {k: ctx.get_output(k) for k in self.output_nodes if k in ctx.node_outputs}
        vals = [v for v in res.values() if v is not None]
        if len(vals) == 1: return vals[0]
        return res if vals else None
    
    async def invoke_async(self, input_data=NO_INPUT, context=None, resume_state=None):
        return await self._internal_invoke_async(input_data, context, resume_state)
    
    def clear_cache(self, c="all"): 
        for n in self.nodes.values(): 
            if hasattr(n, 'clear_cache'): n.clear_cache(c)
    
    @staticmethod
    def _convert_to_payload(input_data):
        return Runnable._model_to_plain(input_data)

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["graph"] = {
            "name": self.graph_def_name,
            "nodes": [{"id": k, **v.to_dict()} for k, v in self.nodes.items()],
            "edges": [vars(e) for es in self.edges.values() for e in es],
            "entry_points": self.entry_points,
            "output_nodes": self.output_nodes
        }
        return data

    @staticmethod
    def prepare_resume(snapshot: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        辅助方法：根据快照生成恢复状态 (Resume State)。
        自动查找快照中挂起的节点，并将 inputs 注入其中。
        """
        import copy
        new_snapshot = copy.deepcopy(snapshot)
        
        def _inject(current_state):
            # 1. 遍历当前层级的 child_states (用于 Graph/Loop/Map)
            if "child_states" in current_state:
                for child_name, child_state in current_state["child_states"].items():
                    # 如果当前子节点在 inputs 中，说明就是要恢复的目标
                    if child_name in inputs:
                        # 注入数据 (覆盖原有状态或作为新状态)
                        current_state["child_states"][child_name] = inputs[child_name]
                        # 标记已处理，但继续遍历（可能同时恢复多个）
                    
                    # 递归深入 (如果子状态也是复合结构)
                    if isinstance(child_state, dict):
                        _inject(child_state)
            
            # 2. 处理 Loop 的特殊结构 (child_state 是单个对象而非字典)
            if "child_state" in current_state and isinstance(current_state["child_state"], dict):
                 _inject(current_state["child_state"])

        _inject(new_snapshot)
        return new_snapshot

class _GraphExportHelper:
    def __init__(self):
        self._name_counter: Dict[str, int] = {}
        self._node_name_to_runnable: Dict[str, Runnable] = {}
        self._instance_to_name: Dict[Runnable, str] = {}
        self._static_attached: Set[str] = set()

    def _unique_name(self, base_name: Optional[str]) -> str:
        sanitized = base_name or "Node"
        count = self._name_counter.get(sanitized, 0)
        unique = sanitized if count == 0 else f"{sanitized}_{count}"
        self._name_counter[sanitized] = count + 1
        return unique

    def add_node(self, graph: WorkflowGraph, runnable: Runnable) -> str:
        node_name = self._unique_name(runnable.name)
        graph.add_node(runnable, node_name=node_name)
        self._node_name_to_runnable[node_name] = runnable
        self._instance_to_name[runnable] = node_name
        self._attach_static_inputs(graph, runnable, node_name)
        
        # [New] Process implicit data dependencies (Side Edges)
        self._connect_data_dependencies(graph, runnable, node_name)
        
        return node_name

    def _connect_data_dependencies(self, graph, runnable, node_name):
        source_map = {}
        for target, binding in runnable._input_bindings.items():
            if binding.is_dynamic() and binding.source:
                source_map.setdefault(binding.source, []).append(target)
        
        for source_runnable, targets in source_map.items():
            if source_runnable in self._instance_to_name:
                source_name = self._instance_to_name[source_runnable]
                mapping = {}
                for t in targets:
                    binding = runnable._input_bindings[t]
                    mapping[t] = ".".join(binding.source_path)
                
                # Add implicit data edge
                graph.add_edge(source_name, node_name, data_mapping=mapping)

    def connect_by_name(self,
                        graph: WorkflowGraph,
                        parent_node_name: str,
                        child_node_name: str,
                        branch: Optional[str] = None) -> None:
        parent_runnable = self._node_name_to_runnable.get(parent_node_name)
        child_runnable = self._node_name_to_runnable.get(child_node_name)
        if parent_runnable is None or child_runnable is None:
            raise ValueError("Graph export helper lost track.")

        data_mapping = child_runnable._dynamic_mapping_for_source(parent_runnable)
        graph.add_edge(parent_node_name, child_node_name, data_mapping=data_mapping, branch=branch)
        self._attach_static_inputs(graph, child_runnable, child_node_name)

    def ensure_static_inputs(self, graph: WorkflowGraph, node_name: str) -> None:
        runnable = self._node_name_to_runnable.get(node_name)
        if runnable:
            self._attach_static_inputs(graph, runnable, node_name)

    def _attach_static_inputs(self, graph: WorkflowGraph, runnable: Runnable, node_name: str) -> None:
        if node_name in self._static_attached:
            return
        static_values = runnable._static_binding_values()
        if static_values:
            graph.add_edge("__static__", node_name, static_inputs=static_values)
            self._static_attached.add(node_name)