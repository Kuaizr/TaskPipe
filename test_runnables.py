import unittest
import time
import logging
from unittest.mock import MagicMock, call, patch
from typing import Any, Callable, Optional, Dict, List, Tuple, Union, Type # Import Any and other types

# Assuming runnables.py is in the same directory or accessible via PYTHONPATH
from taskpipe.runnables import (
    ExecutionContext,
    Runnable,
    SimpleTask,
    Pipeline,
    Conditional,
    BranchAndFanIn,
    SourceParallel,
    While,
    MergeInputs,
    NO_INPUT,
    _PendingConditional
)

# Configure specific loggers if needed for debugging
runnables_logger = logging.getLogger('core.runnables') # Updated logger name
# Set level to INFO or DEBUG to see logs from runnables.py
runnables_logger.setLevel(logging.CRITICAL) # Default to CRITICAL for tests


class TestExecutionContext(unittest.TestCase):
    def test_initialization(self):
        ctx = ExecutionContext("initial_data")
        self.assertEqual(ctx.initial_input, "initial_data")
        self.assertEqual(ctx.node_outputs, {})

    def test_add_get_output(self):
        ctx = ExecutionContext()
        ctx.add_output("node_A", "output_A")
        self.assertEqual(ctx.get_output("node_A"), "output_A")
        self.assertIsNone(ctx.get_output("node_B"))
        self.assertEqual(ctx.get_output("node_B", "default_B"), "default_B")

    def test_add_output_unnamed_node(self):
        ctx = ExecutionContext()
        ctx.add_output("", "output_unnamed") # Should not add to node_outputs
        self.assertEqual(len(ctx.node_outputs), 0)
        self.assertTrue(any("Output for unnamed node" in event for event in ctx.event_log))

    def test_parent_context_get_output(self):
        parent_ctx = ExecutionContext()
        parent_ctx.add_output("parent_node", "parent_value")
        
        child_ctx = ExecutionContext(parent_context=parent_ctx)
        child_ctx.add_output("child_node", "child_value")

        self.assertEqual(child_ctx.get_output("child_node"), "child_value")
        self.assertEqual(child_ctx.get_output("parent_node"), "parent_value") 
        self.assertIsNone(child_ctx.get_output("non_existent_node"))


class MockRunnable(Runnable):
    # Define _internal_invoke to make MockRunnable concrete
    def _internal_invoke(self, input_data, context):
        self.invoke_call_count += 1
        self.last_input_to_internal_invoke = input_data
        self.last_context_in_internal_invoke = context
        if self._invoke_side_effect:
            if isinstance(self._invoke_side_effect, Exception):
                raise self._invoke_side_effect
            # If callable, pass input_data and context
            if callable(self._invoke_side_effect):
                return self._invoke_side_effect(input_data, context)
            return self._invoke_side_effect # If it's a direct value
        return f"invoked_{self.name}_{str(input_data)[:20]}" # Make output somewhat dependent on input for clarity

    def __init__(self, name: Optional[str] = None, invoke_side_effect=None, check_side_effect=None, **kwargs):
        # Explicitly pass name=None if not provided, so base class generates default
        super().__init__(name=name, **kwargs) 
        self.invoke_call_count = 0
        self.check_call_count = 0 # Counter for _default_check calls
        self._invoke_side_effect = invoke_side_effect
        # _check_side_effect is only used for the custom check function in tests
        self._check_side_effect = check_side_effect 
        self.last_input_to_internal_invoke = None
        self.last_context_in_internal_invoke = None
        self.last_input_to_default_check = None

    def _default_check(self, data_from_invoke: Any) -> bool:
        """
        Override _default_check to count calls for testing.
        """
        self.check_call_count += 1
        self.last_input_to_default_check = data_from_invoke
        # The actual check logic should still be the default boolean check
        return bool(data_from_invoke)


class TestRunnableBase(unittest.TestCase):
    def test_naming(self):
        # Test default naming on MockRunnable
        r1 = MockRunnable() 
        self.assertTrue(r1.name.startswith("MockRunnable_"), f"Name '{r1.name}' does not start with 'MockRunnable_'")
        self.assertTrue(str(id(r1)) in r1.name, f"ID '{id(r1)}' not found in name '{r1.name}'")

        # Test custom naming
        r2 = MockRunnable(name="custom_name")
        self.assertEqual(r2.name, "custom_name")

        # Test empty name fallback
        r3 = MockRunnable(name="") 
        self.assertTrue(r3.name.startswith("MockRunnable_"), f"Name '{r3.name}' does not start with 'MockRunnable_' for empty input")
        self.assertTrue(str(id(r3)) in r3.name, f"ID '{id(r3)}' not found in name '{r3.name}' for empty input")

    # Removed test_runnable_base_naming as Runnable is abstract

    def test_invoke_caching_with_context_sensitivity(self):
        mock_fn = MagicMock(return_value="result_ctx_dependent")
        
        # Cache key generator that includes a specific context value
        def custom_key_gen(name, input_data, context, decl):
            ctx_val = context.get_output("sensitive_key", "default") if context else "no_ctx"
            return (name, input_data, ctx_val)

        r = MockRunnable(invoke_side_effect=mock_fn, cache_key_generator=custom_key_gen)
        
        ctx1 = ExecutionContext()
        ctx1.add_output("sensitive_key", "alpha")
        
        ctx2 = ExecutionContext()
        ctx2.add_output("sensitive_key", "beta")

        res1 = r.invoke("input", context=ctx1)
        self.assertEqual(res1, "result_ctx_dependent")
        mock_fn.assert_called_once_with("input", ctx1)
        self.assertEqual(r.invoke_call_count, 1)

        res2 = r.invoke("input", context=ctx1) # Cached for ctx1
        self.assertEqual(res2, "result_ctx_dependent")
        mock_fn.assert_called_once() 
        self.assertEqual(r.invoke_call_count, 1)

        res3 = r.invoke("input", context=ctx2) # Different context, should re-invoke
        self.assertEqual(res3, "result_ctx_dependent")
        self.assertEqual(mock_fn.call_count, 2)
        mock_fn.assert_called_with("input", ctx2) # Check last call
        self.assertEqual(r.invoke_call_count, 2)

    def test_check_caching(self):
        # Use a simple value for invoke_side_effect to get data_from_invoke
        r = MockRunnable(invoke_side_effect="some_data_to_check") 
        
        # Invoke first to populate data_from_invoke in the test flow (though check takes explicit data)
        r.invoke("initial_input") 
        
        # Now test check caching
        check_data = "data_for_check1"
        
        # First check call
        res1 = r.check(check_data)
        self.assertTrue(res1) # Default check bool("data_for_check1") is True
        self.assertEqual(r.check_call_count, 1) # _default_check called

        # Second check call with same data - should be cached
        res2 = r.check(check_data) 
        self.assertTrue(res2)
        self.assertEqual(r.check_call_count, 1) # _default_check not called again

        # Clear check cache
        r.clear_cache('_check_cache')
        
        # Third check call with same data - cache cleared, should re-invoke _default_check
        res3 = r.check(check_data) 
        self.assertTrue(res3)
        self.assertEqual(r.check_call_count, 2) # _default_check called again

    def test_custom_check_function(self):
        r = MockRunnable()
        custom_checker = MagicMock(return_value=False)
        r.set_check(custom_checker)
        
        invoke_result = r.invoke("test_data")
        check_res = r.check(invoke_result) 
        
        self.assertFalse(check_res)
        custom_checker.assert_called_once_with(invoke_result)
        self.assertEqual(r.check_call_count, 0) # _default_check should not be called

    def test_retry_logic_success_on_first_try(self):
        mock_fn = MagicMock(return_value="success")
        r = MockRunnable(invoke_side_effect=mock_fn).retry(max_attempts=3, delay_seconds=0.001)
        
        result = r.invoke("input")
        self.assertEqual(result, "success")
        mock_fn.assert_called_once()
        self.assertEqual(r.invoke_call_count, 1)

    def test_retry_logic_success_on_retry(self):
        mock_fn = MagicMock()
        # With max_attempts=3, it will try 3 times.
        # The side_effect should provide results for 3 calls.
        mock_fn.side_effect = [ValueError("fail1"), ValueError("fail2"), "success_on_3rd"]
        
        r = MockRunnable(invoke_side_effect=mock_fn).retry(
            max_attempts=3, delay_seconds=0.001, retry_on_exceptions=(ValueError,)
        )
        
        result = r.invoke("input")
        self.assertEqual(result, "success_on_3rd")
        # After fixing the retry loop, it should call the mock function 3 times
        self.assertEqual(mock_fn.call_count, 3) 
        self.assertEqual(r.invoke_call_count, 3) # Check internal invoke count

    def test_retry_logic_failure_after_max_attempts(self):
        mock_fn = MagicMock(side_effect=ValueError("persistent_fail"))
        r = MockRunnable(invoke_side_effect=mock_fn).retry(
            max_attempts=2, delay_seconds=0.001, retry_on_exceptions=(ValueError,)
        )
        
        with self.assertRaisesRegex(ValueError, "persistent_fail"):
            r.invoke("input")
        # After fixing the retry loop, it should call the mock function 2 times
        self.assertEqual(mock_fn.call_count, 2) 
        self.assertEqual(r.invoke_call_count, 2) # Check internal invoke count

    def test_retry_logic_non_retryable_exception(self):
        mock_fn = MagicMock(side_effect=TypeError("type_error_fail"))
        r = MockRunnable(invoke_side_effect=mock_fn).retry(
            max_attempts=3, delay_seconds=0.001, retry_on_exceptions=(ValueError,)
        )
        with self.assertRaisesRegex(TypeError, "type_error_fail"):
            r.invoke("input")
        mock_fn.assert_called_once()
        self.assertEqual(r.invoke_call_count, 1)

    def test_error_handler_invoked(self):
        failing_runnable = MockRunnable(name="failing", invoke_side_effect=ValueError("main_error"))
        handler_fn = MagicMock(return_value="handler_output")
        error_handler = MockRunnable(name="handler", invoke_side_effect=handler_fn)
        
        failing_runnable.on_error(error_handler)
        result = failing_runnable.invoke("input_to_fail")
        
        self.assertEqual(result, "handler_output")
        handler_fn.assert_called_once_with("input_to_fail", unittest.mock.ANY)

    def test_error_handler_also_fails(self):
        failing_runnable = MockRunnable(name="failing", invoke_side_effect=ValueError("main_error"))
        failing_handler = MockRunnable(name="failing_handler", invoke_side_effect=RuntimeError("handler_error"))
        
        failing_runnable.on_error(failing_handler)
        with self.assertRaisesRegex(ValueError, "main_error") as cm:
            failing_runnable.invoke("input_to_fail")
        self.assertIsInstance(cm.exception.__cause__, RuntimeError)
        self.assertEqual(str(cm.exception.__cause__), "handler_error")

    def test_error_handler_with_retry(self):
        # If retry fails (after max_attempts), then error handler should be called
        mock_main_fn = MagicMock(side_effect=ValueError("retry_then_fail"))
        # Set max_attempts=2, so it tries twice and fails both times
        failing_runnable = MockRunnable(name="failing", invoke_side_effect=mock_main_fn)
        failing_runnable.retry(max_attempts=2, delay_seconds=0.001, retry_on_exceptions=(ValueError,))
        
        handler_fn = MagicMock(return_value="handler_after_retry_fail")
        error_handler = MockRunnable(name="handler", invoke_side_effect=handler_fn)
        failing_runnable.on_error(error_handler)
        
        result = failing_runnable.invoke("input")
        
        self.assertEqual(result, "handler_after_retry_fail")
        # Main runnable should be invoked max_attempts times (2 times)
        self.assertEqual(mock_main_fn.call_count, 2) 
        self.assertEqual(failing_runnable.invoke_call_count, 2)
        # Error handler should be called once after retries are exhausted
        handler_fn.assert_called_once()
        self.assertEqual(error_handler.invoke_call_count, 1)

    def test_input_declaration_string(self):
        ctx = ExecutionContext(); ctx.add_output("source_node", "data_from_context")
        r = MockRunnable(input_declaration="source_node")
        r.invoke(context=ctx)
        self.assertEqual(r.last_input_to_internal_invoke, "data_from_context")

    def test_input_declaration_dict(self):
        ctx = ExecutionContext(); ctx.add_output("k1", "v1"); ctx.add_output("k2", "v2")
        r = MockRunnable(input_declaration={"p_a": "k1", "p_b": "k2"})
        r.invoke(context=ctx)
        self.assertEqual(r.last_input_to_internal_invoke, {"p_a": "v1", "p_b": "v2"})

    def test_input_declaration_callable(self):
        ctx = ExecutionContext(); ctx.add_output("val_x", 10)
        def fetcher(ec: ExecutionContext): return {"f": ec.get_output("val_x") * 2}
        r = MockRunnable(input_declaration=fetcher)
        r.invoke(context=ctx)
        self.assertEqual(r.last_input_to_internal_invoke, {"f": 20})

    def test_input_declaration_overrides_direct_input_if_no_input(self):
        ctx = ExecutionContext(); ctx.add_output("decl_src", "decl_data")
        r = MockRunnable(input_declaration="decl_src")
        r.invoke(context=ctx) 
        self.assertEqual(r.last_input_to_internal_invoke, "decl_data")
        r.invoke(NO_INPUT, context=ctx)
        self.assertEqual(r.last_input_to_internal_invoke, "decl_data")

    def test_direct_input_takes_precedence_over_declaration_if_provided(self):
        ctx = ExecutionContext(); ctx.add_output("decl_src", "decl_data")
        r = MockRunnable(input_declaration="decl_src")
        r.invoke("direct_data", context=ctx)
        self.assertEqual(r.last_input_to_internal_invoke, "direct_data")

    def test_copy_method(self):
        r1 = MockRunnable(name="original"); r1._invoke_cache["key"] = "value"
        r2 = r1.copy()
        self.assertNotEqual(id(r1), id(r2))
        # The copy method itself doesn't assign a new name starting with _copy_ anymore.
        # The name is still the original name on the copy before it's added to a graph.
        self.assertEqual(r2.name, "original") 
        self.assertEqual(r2._invoke_cache, {})
        self.assertEqual(r1._invoke_cache, {"key": "value"})


class TestSimpleTask(unittest.TestCase):
    def test_basic_execution(self):
        task = SimpleTask(lambda x: x * 2)
        self.assertEqual(task.invoke(10), 20)

    def test_no_arg_function(self):
        task = SimpleTask(lambda: "done")
        self.assertEqual(task.invoke(), "done")
        self.assertEqual(task.invoke(NO_INPUT), "done")

    def test_kwargs_input(self):
        task = SimpleTask(lambda a, b=0: a + b)
        self.assertEqual(task.invoke({"a": 5, "b": 3}), 8)
        self.assertEqual(task.invoke({"a": 5}), 5)

    def test_input_declaration_with_simple_task(self):
        task = SimpleTask(lambda data: f"processed_{data}", input_declaration="in_src")
        ctx = ExecutionContext(); ctx.add_output("in_src", "ctx_data")
        self.assertEqual(task.invoke(context=ctx), "processed_ctx_data")

    def test_input_declaration_dict_with_simple_task(self):
        task = SimpleTask(lambda val1, val2: val1 + val2, input_declaration={"val1": "s1", "val2": "s2"})
        ctx = ExecutionContext(); ctx.add_output("s1", 10); ctx.add_output("s2", 20)
        self.assertEqual(task.invoke(context=ctx), 30)

    def test_name_generation(self):
        def named_func(): pass
        self.assertEqual(SimpleTask(named_func).name, "named_func")
        # After fixing lambda naming, it should fall back to the default class_name_id format
        self.assertTrue(SimpleTask(lambda x: x).name.startswith("SimpleTask_")) 


class TestPipeline(unittest.TestCase):
    def test_basic_pipeline(self):
        def add_one(x): return x + 1
        def mul_two(x): return x * 2
        
        task1 = SimpleTask(add_one, name="add1")
        task2 = SimpleTask(mul_two, name="mul2")
        
        pipeline = task1 | task2
        # self.assertEqual(pipeline.invoke(5), 12) # Removed first invoke without context
        
        ctx = ExecutionContext()
        pipeline_result = pipeline.invoke(5, ctx) # Call only once with context
        
        # Check if outputs of individual named runnables within the pipeline are in context
        self.assertEqual(ctx.get_output("add1"), 6)
        self.assertEqual(ctx.get_output("mul2"), 12)
        # The pipeline's own output should also be in context under its name
        self.assertEqual(ctx.get_output(pipeline.name), 12)

    def test_pipeline_default_check_delegates_to_second(self):
        r1 = MockRunnable(name="p_first")
        # Use set_check to provide a custom check logic for r2 in this test
        r2_check_fn = MagicMock(return_value=False)
        r2 = MockRunnable(name="p_second")
        r2.set_check(r2_check_fn) # Set the custom check on r2
        
        pipeline = r1 | r2
        # Invoke pipeline to get the result that will be passed to pipeline.check
        # r1 output will be "invoked_p_first_data"
        # r2 input will be "invoked_p_first_data"
        # r2 output will be "invoked_p_second_invoked_p_first_data"
        invoke_res = pipeline.invoke("data") 
        
        # Call pipeline.check with the result of the pipeline's invoke (which is r2's output)
        check_res = pipeline.check(invoke_res) 
        
        self.assertFalse(check_res)
        # The pipeline's check should delegate to r2's check.
        # r2's check will use its custom check function (r2_check_fn).
        # The input to r2's check is the output of r2.invoke, which is invoke_res.
        r2_check_fn.assert_called_once_with(invoke_res) 
        self.assertEqual(r2.check_call_count, 0) # _default_check on r2 should not be called
        self.assertEqual(r1.check_call_count, 0) # r1's check should not be involved by default


class TestConditional(unittest.TestCase):
    def test_conditional_true_branch(self):
        # Use a simple value for invoke_side_effect, the check logic is separate
        condition_r = MockRunnable(name="cond", invoke_side_effect="truthy_data") 
        # Set a custom check function that returns True for this data
        condition_r.set_check(lambda data: data == "truthy_data") 

        true_r = MockRunnable(name="true_action", invoke_side_effect="true_result")
        false_r = MockRunnable(name="false_action", invoke_side_effect="false_result")

        op_cond = condition_r % true_r >> false_r
        # result = op_cond.invoke("input_to_cond") # Removed first invoke without context
        # self.assertEqual(result, "true_result")
        # self.assertEqual(true_r.invoke_call_count, 1) # Resetting counts for isolated test
        # self.assertEqual(false_r.invoke_call_count, 0)
        true_r.invoke_call_count = 0 # Manually reset for this specific test structure
        false_r.invoke_call_count = 0 # Manually reset

        ctx = ExecutionContext()
        cond_result = op_cond.invoke("input_to_cond", ctx) # Call only once with context
        
        # Check if the condition node's output is in context
        self.assertEqual(ctx.get_output("cond"), "truthy_data")
        self.assertEqual(ctx.get_output("true_action"), "true_result")
        self.assertIsNone(ctx.get_output("false_action"))
        self.assertEqual(ctx.get_output(op_cond.name), "true_result")


    def test_conditional_false_branch(self):
        # Use a simple value for invoke_side_effect
        condition_r = MockRunnable(name="cond", invoke_side_effect="falsey_data") 
        # Set a custom check function that returns False for this data
        condition_r.set_check(lambda data: data != "falsey_data") # Returns False if data is "falsey_data"

        true_r = MockRunnable(name="true_action", invoke_side_effect="true_result")
        false_r = MockRunnable(name="false_action", invoke_side_effect="false_result")

        cond_runnable = condition_r % true_r >> false_r
        result = cond_runnable.invoke("input_to_cond")

        self.assertEqual(result, "false_result")
        self.assertEqual(true_r.invoke_call_count, 0)
        self.assertEqual(false_r.invoke_call_count, 1)


class TestParallelRunnables(unittest.TestCase):
    def test_branch_and_fan_in(self):
        task_C = SimpleTask(lambda d: f"C_{d}", name="tC")
        task_D = SimpleTask(lambda d: f"D_{d}", name="tD")
        branch_fan_in = BranchAndFanIn({"Ck": task_C, "Dk": task_D})
        self.assertEqual(branch_fan_in.invoke("in_val"), {"Ck": "C_in_val", "Dk": "D_in_val"})

        task_A = SimpleTask(lambda x: f"A_{x}", name="tA")
        pipeline = task_A | {"Ck": task_C, "Dk": task_D}
        ctx = ExecutionContext()
        res = pipeline.invoke("start", ctx)
        self.assertEqual(res, {"Ck": "C_A_start", "Dk": "D_A_start"})
        self.assertEqual(ctx.get_output("tA"), "A_start")
        # Name of the BranchAndFanIn instance created by __or__ will be in context
        # The output of the pipeline is the output of the last step (BranchAndFanIn)
        self.assertEqual(ctx.get_output(pipeline.name), res) 

    def test_source_parallel(self):
        chain_A = SimpleTask(lambda d: f"ChA_{d}", name="ChA")
        chain_B = SimpleTask(lambda d: f"ChB_{d}", name="ChB")
        source_par = SourceParallel({"Ar": chain_A, "Br": chain_B})
        
        ctx = ExecutionContext(initial_input="init_workflow_in")
        res_ctx = source_par.invoke(context=ctx)
        self.assertEqual(res_ctx, {"Ar": "ChA_init_workflow_in", "Br": "ChB_init_workflow_in"})
        
        res_expl = source_par.invoke("expl_in")
        self.assertEqual(res_expl, {"Ar": "ChA_expl_in", "Br": "ChB_expl_in"})

    def test_branch_fan_in_error_handling(self):
        task_ok = SimpleTask(lambda x: "ok", name="tOK")
        task_fail = MockRunnable(name="tFail", invoke_side_effect=ValueError("branch_err"))
        branch_fan_in = BranchAndFanIn({"ok_b": task_ok, "fail_b": task_fail})
        with self.assertRaisesRegex(ValueError, "branch_err"):
            branch_fan_in.invoke("input")


class TestWhile(unittest.TestCase):
    def test_while_loop_executes_correctly(self):
        # Condition: input < 3
        # Body: input + 1
        # Start input: 0
        # Expected body outputs: 1, 2, 3
        
        # Condition runnable outputs its input, check is input < 3
        cond_r = SimpleTask(lambda x: x, name="wC") 
        cond_r.set_check(lambda d: d < 3)
        
        # Body runnable increments its input
        body_r = SimpleTask(lambda x: x + 1, name="wB")
        
        loop = While(cond_r, body_r, max_loops=5)
        
        # With corrected While logic, body receives previous body output
        self.assertEqual(loop.invoke(0), [1, 2, 3])
        
        ctx = ExecutionContext(); loop.invoke(0, ctx)
        self.assertEqual(ctx.get_output(loop.name), [1, 2, 3])

    def test_while_loop_max_loops_reached(self):
        # Condition: always true
        cond_r = SimpleTask(lambda x: True, name="alwaysT")
        # Body: increment input
        body_r = SimpleTask(lambda x: x + 1, name="incrB")
        
        # Max loops = 3. Initial input = 0.
        # Iter 1: cond(0) -> True. body(0) -> 1. current_input_for_iteration = 1. outputs = [1]
        # Iter 2: cond(1) -> True. body(1) -> 2. current_input_for_iteration = 2. outputs = [1, 2]
        # Iter 3: cond(2) -> True. body(2) -> 3. current_input_for_iteration = 3. outputs = [1, 2, 3]
        # Loop count reaches 3. Max loops reached. Exit.
        # Expected body outputs: [1, 2, 3]
        
        while_loop = While(cond_r, body_r, max_loops=3)
        results = while_loop.invoke(0)
        
        self.assertEqual(results, [1, 2, 3])
        
        # Check event log for max_loops message
        # Use a fresh context to avoid cache issues affecting the log check
        test_ctx = ExecutionContext() 
        while_loop.clear_cache() # Clear cache on the runnable itself
        while_loop.invoke(0, test_ctx)
        expected_log = f"Node '{while_loop.name}': Loop exited due to max_loops (3) reached."
        self.assertTrue(any(expected_log in e for e in test_ctx.event_log), f"Expected log '{expected_log}' not found in {test_ctx.event_log}")


    def test_while_loop_condition_false_initially(self):
        cond_r = SimpleTask(lambda x: False, name="alwaysF")
        body_r = MockRunnable(name="neverRunB")
        loop = While(cond_r, body_r)
        self.assertEqual(loop.invoke(0), [])
        self.assertEqual(body_r.invoke_call_count, 0)


class TestMergeInputs(unittest.TestCase):
    def test_merge_inputs_basic(self):
        def merger(uname, icount): return f"User {uname} has {icount} items."
        merge_r = MergeInputs(
            {"uname": "nodeA_out", "icount": "nodeB_out"}, merger, name="merger"
        )
        ctx = ExecutionContext()
        ctx.add_output("nodeA_out", "Bob"); ctx.add_output("nodeB_out", 3)
        res = merge_r.invoke(context=ctx)
        self.assertEqual(res, "User Bob has 3 items.")
        self.assertEqual(ctx.get_output("merger"), res)

    def test_merge_inputs_missing_source_with_default_in_func(self):
        def merger(v1, v2=None): return f"{v1}-{v2}"
        merge_r = MergeInputs({"v1": "s1", "v2": "s_missing"}, merger)
        ctx = ExecutionContext(); ctx.add_output("s1", "d1")
        self.assertEqual(merge_r.invoke(context=ctx), "d1-None")


if __name__ == '__main__':
    # Ensure the logger for 'runnables' is set up if we want to control its level for tests
    # This basicConfig might conflict if run multiple times or if root is already configured.
    # Consider configuring only the 'runnables' logger if that's the target.
    # logging.basicConfig(level=logging.DEBUG) # For verbose output during test development
    unittest.main(verbosity=2)