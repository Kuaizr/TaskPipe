import asyncio
import time
import unittest
import logging
from typing import Any
from unittest.mock import MagicMock, ANY

# Assuming runnables.py and graph.py are accessible
from taskpipe import Runnable, SimpleTask, AsyncRunnable, ExecutionContext, InMemoryExecutionContext, NO_INPUT, MergeInputs
from taskpipe import WorkflowGraph, CompiledGraph, RunnableRegistry

# Suppress most logging output during tests
# Configure specific loggers if needed for debugging
graph_logger = logging.getLogger('core.graph') # Updated logger name
runnables_logger = logging.getLogger('core.runnables') # Updated logger name
graph_logger.setLevel(logging.CRITICAL) 
runnables_logger.setLevel(logging.CRITICAL)

class GraphTestMockRunnable(Runnable):
    def __init__(self, name="mock_runnable", invoke_side_effect=None, **kwargs):
        super().__init__(name=name, **kwargs)
        self.invoke_call_count = 0
        self.last_input_to_internal_invoke = None
        self.last_effective_input = None
        self.last_context_in_internal_invoke = None
        self._invoke_side_effect = invoke_side_effect

    def _internal_invoke(self, input_data, context):
        self.invoke_call_count += 1
        self.last_input_to_internal_invoke = input_data
        effective_input = self._normalize_input(input_data)
        self.last_effective_input = effective_input
        self.last_context_in_internal_invoke = context
        if self._invoke_side_effect:
            if isinstance(self._invoke_side_effect, Exception):
                raise self._invoke_side_effect
            # If callable, pass input_data and context
            if callable(self._invoke_side_effect):
                return self._invoke_side_effect(effective_input, context)
            return self._invoke_side_effect # If it's a direct value
        return f"invoked_{self.name}_{str(effective_input)[:20]}" # Make output somewhat dependent on input for clarity

    def _normalize_input(self, input_data: Any) -> Any:
        if isinstance(input_data, dict):
            if not input_data:
                return NO_INPUT
            if set(input_data.keys()) == {"_input"}:
                return input_data["_input"]
        return input_data

class TestWorkflowGraph(unittest.TestCase):
    def test_add_node(self):
        graph = WorkflowGraph()
        r1 = GraphTestMockRunnable(name="nodeA")
        
        added_name = graph.add_node(r1)
        self.assertEqual(added_name, "nodeA")
        self.assertIn("nodeA", graph.nodes)
        self.assertIsInstance(graph.nodes["nodeA"], GraphTestMockRunnable)
        self.assertNotEqual(id(r1), id(graph.nodes["nodeA"])) 

        r2 = GraphTestMockRunnable(name="original_r2")
        graph.add_node(r2, node_name="graph_node_B")
        self.assertIn("graph_node_B", graph.nodes)
        self.assertEqual(graph.nodes["graph_node_B"].name, "graph_node_B")

        with self.assertRaises(ValueError): 
            graph.add_node(GraphTestMockRunnable(name="nodeA"))

    def test_add_edge(self):
        graph = WorkflowGraph()
        graph.add_node(GraphTestMockRunnable(name="src"))
        graph.add_node(GraphTestMockRunnable(name="dest"))
        
        graph.add_edge("src", "dest")
        self.assertEqual(graph.edges["src"][0][:2], ("dest", None)) # Check dest_name and mapper
        self.assertEqual(graph._in_degree["dest"], 1)

        def my_mapper(data, ctx_outputs): return str(data) + "_mapped"
        graph.add_edge("src", "dest", input_mapper=my_mapper)
        self.assertEqual(graph.edges["src"][1][1], my_mapper)

        with self.assertRaises(ValueError): graph.add_edge("non_existent_src", "dest")
        with self.assertRaises(ValueError): graph.add_edge("src", "non_existent_dest")

    def test_compile_simple_linear_graph(self):
        graph = WorkflowGraph(name="LinearTest")
        graph.add_node(GraphTestMockRunnable(name="A")); graph.add_node(GraphTestMockRunnable(name="B")); graph.add_node(GraphTestMockRunnable(name="C"))
        graph.add_edge("A", "B"); graph.add_edge("B", "C")

        compiled_graph = graph.compile()
        self.assertEqual(compiled_graph.sorted_nodes, ["A", "B", "C"])
        self.assertEqual(compiled_graph.entry_points, ["A"])
        self.assertEqual(compiled_graph.output_nodes, ["C"])

    def test_compile_graph_with_cycle(self):
        graph = WorkflowGraph()
        graph.add_node(GraphTestMockRunnable(name="A")); graph.add_node(GraphTestMockRunnable(name="B"))
        graph.add_edge("A", "B"); graph.add_edge("B", "A") 
        with self.assertRaisesRegex(Exception, "Graph '.*' has a cycle"):
            graph.compile()

    def test_compile_infer_entry_and_output_nodes(self):
        graph = WorkflowGraph()
        graph.add_node(GraphTestMockRunnable(name="A")); graph.add_node(GraphTestMockRunnable(name="B")); graph.add_node(GraphTestMockRunnable(name="C"))
        graph.add_node(GraphTestMockRunnable(name="D")); graph.add_node(GraphTestMockRunnable(name="E"))
        graph.add_edge("A", "B"); graph.add_edge("D", "C"); graph.add_edge("B", "C")
        
        compiled_graph = graph.compile()
        self.assertCountEqual(compiled_graph.entry_points, ["A", "D", "E"])
        self.assertCountEqual(compiled_graph.output_nodes, ["C", "E"])

    def test_compile_with_explicit_entry_output(self):
        graph = WorkflowGraph()
        graph.add_node(GraphTestMockRunnable(name="N1")); graph.add_node(GraphTestMockRunnable(name="N2")); graph.add_node(GraphTestMockRunnable(name="N3"))
        graph.add_edge("N1", "N2"); graph.add_edge("N2", "N3")
        graph.set_entry_point("N1"); graph.set_output_nodes(["N2", "N3"])
        
        compiled_graph = graph.compile()
        self.assertEqual(compiled_graph.entry_points, ["N1"])
        self.assertCountEqual(compiled_graph.output_nodes, ["N2", "N3"])

class TestCompiledGraph(unittest.TestCase):
    def test_execute_linear_graph(self):
        graph = WorkflowGraph(name="ExecLinear")
        node_a_run = GraphTestMockRunnable(name="A", invoke_side_effect=lambda d,c: f"A_proc_{d}")
        node_b_run = GraphTestMockRunnable(name="B", invoke_side_effect=lambda d,c: f"B_proc_{d}")
        graph.add_node(node_a_run); graph.add_node(node_b_run)
        graph.add_edge("A", "B")
        
        compiled_graph = graph.compile()
        result = compiled_graph.invoke("start_val")
        
        self.assertEqual(result, "B_proc_A_proc_start_val")
        # Access the nodes from the compiled graph to check their state
        graph_node_a = compiled_graph.nodes["A"]
        graph_node_b = compiled_graph.nodes["B"]
        self.assertEqual(graph_node_a.last_effective_input, "start_val")
        self.assertEqual(graph_node_b.last_effective_input, "A_proc_start_val")

        # Test context propagation
        outer_ctx = InMemoryExecutionContext()
        # Create a new graph and compiled graph for this part of the test to ensure clean state
        graph2 = WorkflowGraph(name="ExecLinearCtx2")
        node_a_orig_for_ctx_test = GraphTestMockRunnable(name="A", invoke_side_effect=lambda d,c: f"A_proc_{d}")
        node_b_orig_for_ctx_test = GraphTestMockRunnable(name="B", invoke_side_effect=lambda d,c: f"B_proc_{d}")
        graph2.add_node(node_a_orig_for_ctx_test); graph2.add_node(node_b_orig_for_ctx_test)
        graph2.add_edge("A", "B")
        compiled_graph_2 = graph2.compile() # Use a different compiled_graph instance

        compiled_graph_2.invoke("ctx_val", outer_ctx)
        
        # Access the node *copies* from the compiled graph to check their state
        graph2_node_a_copy = compiled_graph_2.nodes["A"]
        graph2_node_b_copy = compiled_graph_2.nodes["B"]

        self.assertEqual(graph2_node_a_copy.last_effective_input, "ctx_val")
        self.assertEqual(graph2_node_b_copy.last_effective_input, "A_proc_ctx_val")
        
        internal_ctx_seen_by_b = graph2_node_b_copy.last_context_in_internal_invoke
        self.assertIsNotNone(internal_ctx_seen_by_b)
        self.assertEqual(internal_ctx_seen_by_b.get_output("A"), "A_proc_ctx_val")
        self.assertEqual(internal_ctx_seen_by_b.get_output("B"), "B_proc_A_proc_ctx_val")
        self.assertEqual(outer_ctx.get_output(compiled_graph_2.name), "B_proc_A_proc_ctx_val")

    def test_execute_graph_with_mapper(self):
        graph = WorkflowGraph()
        node_a = GraphTestMockRunnable(name="A", invoke_side_effect=lambda d,c: 10)
        node_b = GraphTestMockRunnable(name="B")
        def mapper(src_out, ctx_outs): return src_out * 2
        graph.add_node(node_a); graph.add_node(node_b)
        graph.add_edge("A", "B", input_mapper=mapper)
        
        compiled_graph = graph.compile()
        result = compiled_graph.invoke("start")
        # Access the node *copy* from the compiled graph
        graph_node_b_copy = compiled_graph.nodes["B"]
        self.assertEqual(graph_node_b_copy.last_effective_input, 20)
        self.assertEqual(result, "invoked_B_20")

    def test_execute_graph_multiple_outputs(self):
        graph = WorkflowGraph()
        node_a = GraphTestMockRunnable(name="A", invoke_side_effect=lambda d,c: "out_A")
        node_b = GraphTestMockRunnable(name="B", invoke_side_effect=lambda d,c: "out_B") # B is independent entry
        node_c = GraphTestMockRunnable(name="C", invoke_side_effect=lambda d,c: f"out_C_from_{d}")
        graph.add_node(node_a); graph.add_node(node_b); graph.add_node(node_c)
        graph.add_edge("A", "C")
        graph.set_output_nodes(["B", "C"])
        
        compiled_graph = graph.compile()
        result = compiled_graph.invoke("graph_input") # Goes to A and B (if entry)
        
        # A gets "graph_input", C gets "out_A"
        # B gets "graph_input" (but its mock doesn't use it for output string)
        expected = {"B": "out_B", "C": "out_C_from_out_A"}
        self.assertEqual(result, expected)

    def test_execute_graph_node_with_input_declaration(self):
        graph = WorkflowGraph()
        s1 = GraphTestMockRunnable(name="S1", invoke_side_effect=lambda d,c: "data_s1")
        s2 = GraphTestMockRunnable(name="S2", invoke_side_effect=lambda d,c: "data_s2")
        def merger(in_a, in_b): return f"{in_a}_{in_b}"
        node_m = MergeInputs({"in_a": "S1", "in_b": "S2"}, merger, name="Merger")
            
        graph.add_node(s1); graph.add_node(s2); graph.add_node(node_m)
        graph.add_edge("S1", "Merger"); graph.add_edge("S2", "Merger")
        
        compiled_graph = graph.compile()
        result = compiled_graph.invoke("graph_start")
        self.assertEqual(result, "data_s1_data_s2")

    def test_execute_graph_error_propagation(self):
        graph = WorkflowGraph()
        node_ok = GraphTestMockRunnable(name="OK", invoke_side_effect=lambda d,c: "ok")
        node_fail = GraphTestMockRunnable(name="FAIL", invoke_side_effect=RuntimeError("NodeFailed"))
        graph.add_node(node_ok); graph.add_node(node_fail)
        graph.add_edge("OK", "FAIL")
    
        compiled_graph = graph.compile()
        try:
            compiled_graph.invoke("start")
        except RuntimeError as e:
            self.assertEqual(str(e), "NodeFailed")
        else:
            self.fail("RuntimeError not raised")

    def test_compiled_graph_caching(self):
        graph = WorkflowGraph(name="CacheTestG",use_cache = True)
        
        # Use a list to track calls to the side effect, as MagicMock identity changes with deepcopy
        call_tracker = []
        def side_effect_fn(d, c):
            call_tracker.append(d)
            return f"g_out_{d}"

        # The GraphTestMockRunnable will be deepcopied when added to the graph.
        # Its _invoke_side_effect (our side_effect_fn) will also be deepcopied if possible,
        # or the reference copied if not (like for simple lambdas).
        # For this test to work reliably across different copy behaviors of callables,
        # the side effect should modify a list in the outer scope.
        inner_task_original = GraphTestMockRunnable(name="InnerT", invoke_side_effect=side_effect_fn)
        graph.add_node(inner_task_original)
        
        compiled_graph = graph.compile()
        
        # First call, should execute the side effect
        res1 = compiled_graph.invoke("in1")
        self.assertEqual(res1, "g_out_in1")
        self.assertEqual(len(call_tracker), 1)
        self.assertEqual(call_tracker[0], "in1")

        # Second call with same input, should be cached, side effect not called again
        res2 = compiled_graph.invoke("in1")
        self.assertEqual(res2, "g_out_in1")
        self.assertEqual(len(call_tracker), 1) # Still 1 call

        # Clear cache
        # Note: clear_cache on CompiledGraph should ideally clear caches of its internal nodes.
        # For this test, we assume compiled_graph.clear_cache() clears its own invoke cache,
        # and the node's cache is relevant if the node itself is invoked multiple times *within* a single graph.invoke.
        # The test is more about the CompiledGraph's own caching of its entire execution for a given input.
        # Let's refine this if CompiledGraph is meant to have its own invoke cache separate from node caches.
        # The current Runnable.invoke cache is per Runnable instance.
        # A CompiledGraph is a Runnable. Its invoke result can be cached.
        # If we want to test the *internal node's* cache, the test structure would be different.
        # This test, as written, tests if the *CompiledGraph's* result is cached.
        
        # To test the node's cache being cleared via the graph, we'd need:
        # compiled_graph.nodes["InnerT"].clear_cache() or a graph-level clear that propagates.
        # For now, let's assume compiled_graph.clear_cache() clears the graph's own invoke cache.
        compiled_graph.clear_cache() # Clears CompiledGraph's own _invoke_cache

        # Third call, cache cleared, should execute side effect again
        res3 = compiled_graph.invoke("in1")
        self.assertEqual(res3, "g_out_in1")
        self.assertEqual(len(call_tracker), 2) # Now 2 calls
        self.assertEqual(call_tracker[1], "in1")

    def test_graph_with_multiple_parents_no_declaration(self):
        graph = WorkflowGraph()
        node_a = GraphTestMockRunnable(name="A", invoke_side_effect="out_A")
        node_b = GraphTestMockRunnable(name="B", invoke_side_effect="out_B")
        node_c = GraphTestMockRunnable(name="C", invoke_side_effect="C_fixed_out") # Doesn't use input
        graph.add_node(node_a); graph.add_node(node_b); graph.add_node(node_c)
        graph.add_edge("A", "C"); graph.add_edge("B", "C")

        compiled_graph = graph.compile()
        
        # Set graph_logger level to WARNING to catch the specific log message
        # Use the updated logger name for 'core.graph'
        logger_to_check = logging.getLogger('taskpipe.graph')
        original_level = logger_to_check.level
        logger_to_check.setLevel(logging.WARNING)
        try:
            with self.assertLogs(logger='taskpipe.graph', level='WARNING') as cm:
                 result = compiled_graph.invoke("start_input")
            self.assertTrue(any("Node 'C'" in log_msg and "multiple incoming edges but no input_declaration" in log_msg for log_msg in cm.output))
        finally:
            logger_to_check.setLevel(original_level) # Reset logger level
        
        # Access the node *copy* from the compiled graph
        graph_node_c_copy = compiled_graph.nodes["C"]
        self.assertEqual(graph_node_c_copy.last_effective_input, NO_INPUT)
        self.assertEqual(result, "C_fixed_out")


class TestGraphSerialization(unittest.TestCase):
    def test_pipeline_to_graph_roundtrip(self):
        add_one = SimpleTask(lambda x: x + 1, name="AddOne")
        mul_two = SimpleTask(lambda x: x * 2, name="MulTwo")
        workflow = add_one | mul_two

        graph = workflow.to_graph("PipelineGraph")
        compiled = graph.compile()
        self.assertEqual(compiled.invoke(3), 8)

    def test_workflow_graph_json_roundtrip(self):
        graph = WorkflowGraph(name="SerializableGraph")
        node_a = graph.add_node(SimpleTask(lambda x: x + 1, name="OpA"))
        node_b = graph.add_node(SimpleTask(lambda x: x * 5, name="OpB"))
        graph.add_edge(node_a, node_b)
        graph.set_entry_point(node_a).set_output_nodes([node_b])

        data = graph.to_json()
        registry = {
            "OpA": lambda: SimpleTask(lambda x: x + 1, name="OpA"),
            "OpB": lambda: SimpleTask(lambda x: x * 5, name="OpB"),
        }
        rebuilt = WorkflowGraph.from_json(data, registry)
        compiled = rebuilt.compile()
        self.assertEqual(compiled.invoke(2), (2 + 1) * 5)

    def test_compiled_graph_execution_stages_and_parallel_async(self):
        tracker = []

        class RecorderTask(AsyncRunnable):
            def __init__(self, name: str, delay: float = 0.05):
                super().__init__(name=name)
                self.delay = delay

            async def _internal_invoke_async(self, input_data, context):
                tracker.append((self.name, "start", time.perf_counter()))
                await asyncio.sleep(self.delay)
                tracker.append((self.name, "end", time.perf_counter()))
                return self.name

        graph = WorkflowGraph(name="AsyncParallel")
        start = SimpleTask(lambda: "start", name="Start")
        branch_a = RecorderTask("BranchA")
        branch_b = RecorderTask("BranchB")
        joiner = MergeInputs({"first": "BranchA", "second": "BranchB"}, lambda first, second: f"{first}+{second}", name="Joiner")

        graph.add_node(start)
        graph.add_node(branch_a)
        graph.add_node(branch_b)
        graph.add_node(joiner)
        graph.add_edge("Start", "BranchA")
        graph.add_edge("Start", "BranchB")
        graph.add_edge("BranchA", "Joiner")
        graph.add_edge("BranchB", "Joiner")

        compiled = graph.compile()
        self.assertGreaterEqual(len(compiled.execution_stages), 3)

        asyncio.run(compiled.invoke_async(NO_INPUT))

        starts = [event for event in tracker if event[1] == "start"]
        self.assertEqual(len(starts), 2)
        diff = abs(starts[0][2] - starts[1][2])
        self.assertLess(diff, 0.2)  # Should start nearly simultaneously

    def test_workflow_graph_from_json_with_registry(self):
        registry = RunnableRegistry()
        registry.register("Adder", lambda: SimpleTask(lambda x: x + 1, name="Adder"))
        registry.register("Multiplier", lambda: SimpleTask(lambda x: x * 3, name="Multiplier"))

        graph_json = {
            "name": "RegistryGraph",
            "nodes": [
                {"name": "AdderNode", "ref": "Adder"},
                {"name": "MultiplierNode", "ref": "Multiplier"}
            ],
            "edges": [
                {"source": "AdderNode", "dest": "MultiplierNode"}
            ],
            "entry_points": ["AdderNode"],
            "output_nodes": ["MultiplierNode"]
        }

        graph = WorkflowGraph.from_json(graph_json, registry)
        self.assertEqual(graph.compile().invoke(1), (1 + 1) * 3)

if __name__ == '__main__':
    unittest.main(verbosity=2)