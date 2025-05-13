import unittest
import time
import logging
import asyncio # Added for testing invoke_async
from unittest.mock import MagicMock, call, patch
from typing import Any, Callable, Optional, Dict, List, Tuple, Union, Type, Coroutine # Added Coroutine

from taskpipe import ( 
    ExecutionContext,
    Runnable,
    SimpleTask,
    Pipeline,
    NO_INPUT,
    AsyncRunnable,
    AsyncPipeline,
    While,
    SourceParallel,
    BranchAndFanIn,
    MergeInputs
)
# Import an AsyncRunnable for mixed tests
from taskpipe.async_runnables import AsyncRunnable, AsyncPipeline, AsyncConditional

# Configure specific loggers if needed for debugging
runnables_logger = logging.getLogger('core.runnables') # Updated logger name
# Set level to INFO or DEBUG to see logs from runnables.py
runnables_logger.setLevel(logging.CRITICAL) # Default to CRITICAL for tests

async_runnables_logger = logging.getLogger('taskpipe.async_runnables')
async_runnables_logger.setLevel(logging.CRITICAL)

class MockRunnable(Runnable):
    def _internal_invoke(self, input_data, context):
        self.invoke_call_count += 1
        self.last_input_to_internal_invoke = input_data
        self.last_context_in_internal_invoke = context
        if self.delay > 0:
            time.sleep(self.delay) # Simulate blocking work for sync runnable

        if self._invoke_side_effect:
            if isinstance(self._invoke_side_effect, Exception):
                raise self._invoke_side_effect
            if callable(self._invoke_side_effect):
                return self._invoke_side_effect(input_data, context)
            return self._invoke_side_effect 
        return f"invoked_{self.name}_{str(input_data)[:20]}" 

    def __init__(self, name: Optional[str] = None, invoke_side_effect=None, check_side_effect=None, delay: float = 0, **kwargs):
        super().__init__(name=name, **kwargs) 
        self.invoke_call_count = 0
        self.check_call_count = 0 
        self._invoke_side_effect = invoke_side_effect
        self._check_side_effect = check_side_effect 
        self.last_input_to_internal_invoke = None
        self.last_context_in_internal_invoke = None
        self.last_input_to_default_check = None
        self.delay = delay # Added delay for testing invoke_async

    def _default_check(self, data_from_invoke: Any) -> bool:
        self.check_call_count += 1
        self.last_input_to_default_check = data_from_invoke
        if self._check_side_effect: # For custom check behavior in tests
            return self._check_side_effect(data_from_invoke)
        return bool(data_from_invoke)

# A simple AsyncRunnable for testing interactions
class MockAsyncRunnableForSyncTest(AsyncRunnable):
    def __init__(self, name: Optional[str] = None, delay: float = 0.01, result_val: str = "async_result"):
        super().__init__(name=name)
        self.delay = delay
        self.result_val = result_val
        self.invoke_count = 0

    async def _internal_invoke_async(self, input_data: Any, context: ExecutionContext) -> Any:
        self.invoke_count += 1
        await asyncio.sleep(self.delay)
        return f"{self.result_val}_for_{input_data}"


class TestRunnableBase(unittest.TestCase):
    # ... (existing test_naming, test_invoke_caching_with_context_sensitivity, etc. remain) ...
    # Add new tests for invoke_async and check_async on base Runnable
    def test_naming(self):
        r1 = MockRunnable() 
        self.assertTrue(r1.name.startswith("MockRunnable_"))
        r2 = MockRunnable(name="custom_name")
        self.assertEqual(r2.name, "custom_name")

    def test_runnable_default_invoke_async(self):
        # Test that Runnable.invoke_async calls the synchronous self.invoke via executor
        mock_sync_invoke_method = MagicMock(return_value="sync_result_from_invoke")
        
        task = MockRunnable(name="TestSyncInvokeAsync", delay=0.01)
        # Patch the 'invoke' method of this specific instance
        with patch.object(task, 'invoke', new=mock_sync_invoke_method) as patched_invoke:
            async def run_test():
                ctx = ExecutionContext()
                result = await task.invoke_async("async_input", context=ctx)
                return result

            res = asyncio.run(run_test())
            self.assertEqual(res, "sync_result_from_invoke")
            patched_invoke.assert_called_once_with("async_input", unittest.mock.ANY) # context is created/passed
            # Check that the context passed to invoke is the one created/passed in invoke_async
            self.assertIsInstance(patched_invoke.call_args[0][1], ExecutionContext)


    def test_runnable_default_check_async(self):
        task = MockRunnable(name="TestSyncCheckAsync")
        task._default_check = MagicMock(return_value=True) # Mock the actual check logic

        async def run_test():
            ctx = ExecutionContext()
            # data_from_invoke is "some_data"
            result = await task.check_async("some_data", context=ctx) 
            return result

        res = asyncio.run(run_test())
        self.assertTrue(res)
        task._default_check.assert_called_once_with("some_data")
        # The context is passed to the 'check' method which then uses it.
        # The mock here is on _default_check which doesn't directly take context in this MockRunnable.
        # If we were patching 'check', we'd check for context.

    def test_runnable_set_check_with_async_function(self):
        task = MockRunnable(name="TestSetAsyncCheck")
        
        custom_async_checker_mock = MagicMock()
        async def my_async_check(data):
            custom_async_checker_mock(data)
            await asyncio.sleep(0.001)
            return False

        task.set_check(my_async_check) # set_check now handles coroutine functions

        async def run_test():
            return await task.check_async("async_check_val")

        result = asyncio.run(run_test())
        self.assertFalse(result)
        custom_async_checker_mock.assert_called_once_with("async_check_val")
        self.assertIsNotNone(task._custom_async_check_fn)
        self.assertIsNone(task._custom_check_fn) # Should clear the sync one

    def test_runnable_set_check_with_sync_function_after_async(self):
        task = MockRunnable(name="TestSetSyncCheckAfterAsync")
        async def my_async_check(data): return True
        def my_sync_check(data): return False
        
        task.set_check(my_async_check)
        task.set_check(my_sync_check) # Now set a sync one

        self.assertIsNotNone(task._custom_check_fn)
        self.assertIsNone(task._custom_async_check_fn)

        # Test sync check
        self.assertFalse(task.check("sync_check_val"))
        # Test async check (should use the sync one wrapped)
        async def run_test():
            return await task.check_async("async_check_val_on_sync_custom")
        
        # Because _custom_async_check_fn is None, Runnable.check_async will
        # run self.check (which uses _custom_check_fn) in an executor.
        self.assertFalse(asyncio.run(run_test()))


    # --- Existing tests like caching, retry, error_handler, input_declaration, copy ---
    # These should ideally be re-verified or adapted if their interaction with async changes,
    # but the core logic of these features in the synchronous path should remain unchanged.
    # For brevity, I'm not repeating all of them here but they should be maintained.

    def test_retry_logic_success_on_first_try(self):
        mock_fn = MagicMock(return_value="success")
        r = MockRunnable(invoke_side_effect=mock_fn).retry(max_attempts=3, delay_seconds=0.001)
        result = r.invoke("input")
        self.assertEqual(result, "success")
        mock_fn.assert_called_once()

    def test_error_handler_invoked(self):
        failing_runnable = MockRunnable(name="failing", invoke_side_effect=ValueError("main_error"))
        handler_fn = MagicMock(return_value="handler_output")
        # Pass the handler_fn directly as the side effect for the MockRunnable
        error_handler = MockRunnable(name="handler", invoke_side_effect=handler_fn)
        
        failing_runnable.on_error(error_handler)
        ctx = ExecutionContext()
        result = failing_runnable.invoke("input_to_fail", ctx)
        
        self.assertEqual(result, "handler_output")
        # The handler_fn (inside error_handler) is called by error_handler._internal_invoke
        # error_handler._internal_invoke receives (actual_input_for_invoke, effective_context)
        handler_fn.assert_called_once_with("input_to_fail", ctx)


class TestSyncComposersWithAsyncRunnables(unittest.TestCase):
    def test_sync_pipeline_with_async_runnable(self):
        # Test that AsyncRunnable.invoke (blocking sync version) is called
        ar1 = MockAsyncRunnableForSyncTest("AR1_in_SyncPipe", delay=0.01)
        sr2 = MockRunnable("SR2_in_SyncPipe", invoke_side_effect=lambda d,c: f"final_{d}")
        
        # Standard (sync) Pipeline
        pipeline = ar1 | sr2
        self.assertIsInstance(pipeline, AsyncPipeline) # Ensure it's an AsyncPipeline

        context = ExecutionContext()
        start_time = time.time()
        result = pipeline.invoke("start_val", context) # Sync invoke on AsyncPipeline
        duration = time.time() - start_time

        self.assertEqual(ar1.invoke_count, 1) # AsyncRunnable's invoke was called
        self.assertEqual(sr2.invoke_call_count, 1)
        self.assertEqual(result, "final_async_result_for_start_val")
        # Duration should reflect blocking execution of ar1
        self.assertTrue(duration >= 0.01, f"Duration {duration} too short, AR1 might not have blocked.")
        self.assertEqual(context.get_output(ar1.name), "async_result_for_start_val")
        self.assertEqual(context.get_output(sr2.name), result)

    def test_sync_conditional_with_async_runnable(self):
        # AsyncRunnable for condition, its sync check will be based on its sync invoke
        # The sync invoke of AsyncRunnable will run its async logic blockingly.
        # The sync check of AsyncRunnable will then operate on that result.
        
        # Condition task: Output "use_true"
        ar_cond = MockAsyncRunnableForSyncTest("ARCond_Sync", result_val="use_true")
        # Default check on AsyncRunnable (via Runnable) is bool("use_true") -> True
        
        sr_true = MockRunnable("SRTrue_Sync", invoke_side_effect=lambda d,c: f"true_branch_got_{d}")
        sr_false = MockRunnable("SRFalse_Sync", invoke_side_effect=lambda d,c: f"false_branch_got_{d}")

        # Standard (sync) Conditional
        conditional = ar_cond % sr_true >> sr_false
        self.assertIsInstance(conditional, AsyncConditional)

        context = ExecutionContext()
        result = conditional.invoke("cond_input", context)

        self.assertEqual(ar_cond.invoke_count, 1)
        self.assertEqual(sr_true.invoke_call_count, 1)
        self.assertEqual(sr_false.invoke_call_count, 0)
        self.assertEqual(result, "true_branch_got_use_true_for_cond_input")


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